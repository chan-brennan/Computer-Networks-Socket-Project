# Socket Project Milestone
# Group 1
# Giovanni Khoshaba - 1221672569
# Brennan Chan - 1219829962
# Harsheeth Aggarwal -1222892931
# Ashmit Arya - 1225249415
# 3/22/2025

import socket
import sys
import json
import random
from enum import Enum

# Define constants and enums
class PeerState(Enum):
    FREE = 1
    LEADER = 2
    IN_DHT = 3
    
# Define return codes
class ReturnCode(Enum):
    SUCCESS = 100
    FAILURE = 200
    
# Define the DHTManager class
# This class manages the DHT and handles peer registrations and DHT setup
# It uses UDP sockets for communication with peers
class DHTManager:
    def __init__(self, port):
        self.port = port
        self.peers = {}  # {peer_name: (ip, m_port, p_port, state)}
        self.dht_exists = False
        self.dht_leader = None
        self.dht_peers = []  # List of peer names in DHT
        self.waiting_for_dht_complete = False
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('0.0.0.0', port))
        print(f"DHT Manager started on port {port}")

    def run(self):
        while True:
            data, addr = self.sock.recvfrom(4096)
            try:
                message = json.loads(data.decode())
                command = message.get('command')
                print(f"Received command: {command} from {addr}")
                
                if command == 'register':
                    self.handle_register(message, addr)
                elif command == 'setup-dht':
                    self.handle_setup_dht(message, addr)
                elif command == 'dht-complete':
                    self.handle_dht_complete(message, addr)
                elif command == 'query-dht':
                    self.handle_query_dht(message, addr)
                elif command == 'leave-dht':
                    self.handle_leave_dht(message, addr)
                elif command == 'join-dht':
                    self.handle_join_dht(message, addr)
                elif command == 'dht-rebuilt':
                    self.handle_dht_rebuilt(message, addr)
                elif command == 'deregister':
                    self.handle_deregister(message, addr)
                elif command == 'teardown-dht':
                    self.handle_teardown_dht(message, addr)
                elif command == 'teardown-complete':
                    self.handle_teardown_complete(message, addr)
                elif command == 'find-event':
                    self.handle_find_event(message, addr)
                else:
                    self.send_response(ReturnCode.FAILURE.value, "Unsupported command", addr)
            except json.JSONDecodeError:
                print(f"Error decoding JSON from {addr}")
                self.send_response(ReturnCode.FAILURE.value, "Invalid message format", addr)
            except Exception as e:
                print(f"Error processing message: {e}")
                self.send_response(ReturnCode.FAILURE.value, f"Internal error: {str(e)}", addr)
                
# Handle incoming messages and process commands
# The DHTManager listens for incoming messages and processes them based on the command type 
    def handle_register(self, message, addr):
        """Handle register command"""
        peer_name = message.get('peer_name')
        ip_addr = message.get('ip_addr')
        m_port = message.get('m_port')
        p_port = message.get('p_port')
        
        # Validate ports are within our range [1500, 1999]
        if not (1500 <= m_port <= 1999 and 1500 <= p_port <= 1999):
            self.send_response(ReturnCode.FAILURE.value, "Ports must be in range [1500, 1999]", addr)
            return
        
        # Check for duplicate registrations
        if peer_name in self.peers:
            self.send_response(ReturnCode.FAILURE.value, "Peer name already registered", addr)
            return
        
        # Check for duplicate ports
        for peer, details in self.peers.items():
            if details[0] == ip_addr and (details[1] == m_port or details[2] == p_port):
                self.send_response(ReturnCode.FAILURE.value, "IP and port combination must be unique", addr)
                return
        
        # Register the peer
        self.peers[peer_name] = (ip_addr, m_port, p_port, PeerState.FREE)
        print(f"Registered peer {peer_name} at {ip_addr}:{m_port},{p_port}")
        self.send_response(ReturnCode.SUCCESS.value, "Registration successful", addr)
        
# Handle setup-dht command
# This method handles the setup-dht command, which initiates the DHT setup process
    def handle_setup_dht(self, message, addr):
        """Handle setup-dht command"""
        if self.waiting_for_dht_complete:
            self.send_response(ReturnCode.FAILURE.value, "Waiting for dht-complete", addr)
            return
        
        if self.dht_exists:
            self.send_response(ReturnCode.FAILURE.value, "DHT already exists", addr)
            return
        
        peer_name = message.get('peer_name')
        n = message.get('n')
        year = message.get('year')
        
        # Validate parameters
        if peer_name not in self.peers:
            self.send_response(ReturnCode.FAILURE.value, "Peer not registered", addr)
            return
        
        if n < 3:
            self.send_response(ReturnCode.FAILURE.value, "n must be at least 3", addr)
            return
        
        if len(self.peers) < n:
            self.send_response(ReturnCode.FAILURE.value, f"Not enough peers registered. Need {n}, have {len(self.peers)}", addr)
            return
        
        # Select n-1 random peers to join the DHT
        available_peers = [p for p in self.peers if p != peer_name and self.peers[p][3] == PeerState.FREE]
        if len(available_peers) < n - 1:
            self.send_response(ReturnCode.FAILURE.value, "Not enough FREE peers available", addr)
            return
        
        selected_peers = random.sample(available_peers, n - 1)
        
        # Update state of selected peers and leader
        self.peers[peer_name] = (self.peers[peer_name][0], self.peers[peer_name][1], 
                              self.peers[peer_name][2], PeerState.LEADER)
        
        for peer in selected_peers:
            self.peers[peer] = (self.peers[peer][0], self.peers[peer][1], 
                             self.peers[peer][2], PeerState.IN_DHT)
        
        # Set DHT information
        self.dht_exists = True
        self.dht_leader = peer_name
        self.dht_peers = [peer_name] + selected_peers
        self.waiting_for_dht_complete = True
        
        # Create response with the list of peers
        peers_info = []
        # Add leader first
        peers_info.append({
            'peer_name': peer_name,
            'ip_addr': self.peers[peer_name][0],
            'p_port': self.peers[peer_name][2]
        })
        
        # Add other peers
        for peer in selected_peers:
            peers_info.append({
                'peer_name': peer,
                'ip_addr': self.peers[peer][0],
                'p_port': self.peers[peer][2]
            })
        
        print(f"Setting up DHT with leader {peer_name} and peers {selected_peers}")
        self.send_response(ReturnCode.SUCCESS.value, "DHT setup initiated", addr, peers_info=peers_info)
        
# Handle dht-complete command
# This method handles the dht-complete command, which finalizes the DHT setup process
    def handle_dht_complete(self, message, addr):
        """Handle dht-complete command"""
        if not self.waiting_for_dht_complete:
            self.send_response(ReturnCode.FAILURE.value, "Not waiting for dht-complete", addr)
            return
        
        peer_name = message.get('peer_name')
        
        if peer_name != self.dht_leader:
            self.send_response(ReturnCode.FAILURE.value, "Only the leader can complete DHT setup", addr)
            return
        
        self.waiting_for_dht_complete = False
        print(f"DHT setup completed by leader {peer_name}")
        self.send_response(ReturnCode.SUCCESS.value, "DHT setup completed", addr)
        
    def handle_query_dht(self, message, addr):
        """Handle query-dht command"""
        if not self.dht_exists:
            self.send_response(ReturnCode.FAILURE.value, "No DHT exists", addr)
            return
        
        peer_name = message.get('peer_name')
        
        # Validate parameters
        if peer_name not in self.peers:
            self.send_response(ReturnCode.FAILURE.value, "Peer not registered", addr)
            return
        
        # Check if peer is FREE (not in the DHT)
        if peer_name in self.dht_peers:
            self.send_response(ReturnCode.FAILURE.value, "Peer is in DHT, must be FREE to query", addr)
            return
        
        # Randomly select a peer from the DHT
        random_peer_name = random.choice(self.dht_peers)
        
        # Get the selected peer's information
        peer_info = {
            'peer_name': random_peer_name,
            'ip_addr': self.peers[random_peer_name][0],
            'p_port': self.peers[random_peer_name][2]
        }
        
        print(f"Query DHT initiated by FREE peer {peer_name}, randomly selected {random_peer_name}")
        self.send_response(ReturnCode.SUCCESS.value, "Query DHT initiated", addr, peer_info=peer_info)

    def handle_leave_dht(self, message, addr):
        """Handle leave-dht command"""
        if not self.dht_exists:
            self.send_response(ReturnCode.FAILURE.value, "No DHT exists", addr)
            return
        
        # Cannot leave if we're waiting for DHT operations to complete
        if self.waiting_for_dht_complete:
            self.send_response(ReturnCode.FAILURE.value, "DHT operation in progress", addr)
            return
        
        peer_name = message.get('peer_name')
        
        # Validate parameters
        if peer_name not in self.peers:
            self.send_response(ReturnCode.FAILURE.value, "Peer not registered", addr)
            return
        
        # Check if peer is part of the DHT
        if peer_name not in self.dht_peers:
            self.send_response(ReturnCode.FAILURE.value, "Peer is not in DHT", addr)
            return
        
        # Cannot leave if the peer is the leader
        if peer_name == self.dht_leader:
            self.send_response(ReturnCode.FAILURE.value, "Leaders cannot leave DHT", addr)
            return
        
        # Update peer state
        self.peers[peer_name] = (self.peers[peer_name][0], self.peers[peer_name][1], 
                               self.peers[peer_name][2], PeerState.FREE)
        
        # Get leader info to send to leaving peer
        leader_name = self.dht_leader
        leader_info = {
            'peer_name': leader_name,
            'ip_addr': self.peers[leader_name][0],
            'p_port': self.peers[leader_name][2]
        }
        
        # Remove peer from DHT peers list
        self.dht_peers.remove(peer_name)
        
        # Set waiting for DHT rebuild
        self.waiting_for_dht_complete = True
        
        print(f"Peer {peer_name} leaving DHT")
        self.send_response(ReturnCode.SUCCESS.value, "Leave DHT initiated", addr, leader_info=leader_info)

    def handle_join_dht(self, message, addr):
        """Handle join-dht command"""
        if not self.dht_exists:
            self.send_response(ReturnCode.FAILURE.value, "No DHT exists", addr)
            return
        
        # Cannot join if we're waiting for DHT operations to complete
        if self.waiting_for_dht_complete:
            self.send_response(ReturnCode.FAILURE.value, "DHT operation in progress", addr)
            return
        
        peer_name = message.get('peer_name')
        
        # Validate parameters
        if peer_name not in self.peers:
            self.send_response(ReturnCode.FAILURE.value, "Peer not registered", addr)
            return
        
        # Check if peer is already part of the DHT
        if peer_name in self.dht_peers:
            self.send_response(ReturnCode.FAILURE.value, "Peer is already in DHT", addr)
            return
        
        # Check if peer is in FREE state
        if self.peers[peer_name][3] != PeerState.FREE:
            self.send_response(ReturnCode.FAILURE.value, "Peer is not in FREE state", addr)
            return
        
        # Update peer state
        self.peers[peer_name] = (self.peers[peer_name][0], self.peers[peer_name][1], 
                               self.peers[peer_name][2], PeerState.IN_DHT)
        
        # Get leader info to send to joining peer
        leader_name = self.dht_leader
        leader_info = {
            'peer_name': leader_name,
            'ip_addr': self.peers[leader_name][0],
            'p_port': self.peers[leader_name][2]
        }
        
        # Add peer to DHT peers list
        self.dht_peers.append(peer_name)
        
        # Set waiting for DHT rebuild
        self.waiting_for_dht_complete = True
        
        print(f"Peer {peer_name} joining DHT")
        self.send_response(ReturnCode.SUCCESS.value, "Join DHT initiated", addr, leader_info=leader_info)

    def handle_dht_rebuilt(self, message, addr):
        """Handle dht-rebuilt command"""
        if not self.waiting_for_dht_complete:
            self.send_response(ReturnCode.FAILURE.value, "Not waiting for dht-rebuilt", addr)
            return
        
        peer_name = message.get('peer_name')
        new_leader = message.get('new_leader')
        
        # Only the leader can signal DHT rebuild completion
        if peer_name != self.dht_leader:
            self.send_response(ReturnCode.FAILURE.value, "Only the leader can complete DHT rebuild", addr)
            return
        
        # Update leader if specified
        if new_leader and new_leader != self.dht_leader and new_leader in self.dht_peers:
            old_leader = self.dht_leader
            self.dht_leader = new_leader
            
            # Update states
            if old_leader in self.peers:
                self.peers[old_leader] = (self.peers[old_leader][0], self.peers[old_leader][1], 
                                       self.peers[old_leader][2], PeerState.IN_DHT)
            if new_leader in self.peers:
                self.peers[new_leader] = (self.peers[new_leader][0], self.peers[new_leader][1], 
                                       self.peers[new_leader][2], PeerState.LEADER)
            
            print(f"Leadership transferred from {old_leader} to {new_leader}")
        
        self.waiting_for_dht_complete = False
        print(f"DHT rebuild completed by leader {peer_name}")
        self.send_response(ReturnCode.SUCCESS.value, "DHT rebuild completed", addr)

    def handle_deregister(self, message, addr):
        """Handle deregister command"""
        peer_name = message.get('peer_name')
        
        # Validate parameters
        if peer_name not in self.peers:
            self.send_response(ReturnCode.FAILURE.value, "Peer not registered", addr)
            return
        
        # Can only deregister if in FREE state
        if self.peers[peer_name][3] != PeerState.FREE:
            self.send_response(ReturnCode.FAILURE.value, "Peer is not in FREE state", addr)
            return
        
        # Remove peer from peers dictionary
        del self.peers[peer_name]
        
        print(f"Deregistered peer {peer_name}")
        self.send_response(ReturnCode.SUCCESS.value, "Deregistration successful", addr)

    def handle_teardown_dht(self, message, addr):
        """Handle teardown-dht command"""
        if not self.dht_exists:
            self.send_response(ReturnCode.FAILURE.value, "No DHT exists", addr)
            return
        
        # Cannot teardown if we're waiting for DHT operations to complete
        if self.waiting_for_dht_complete:
            self.send_response(ReturnCode.FAILURE.value, "DHT operation in progress", addr)
            return
        
        peer_name = message.get('peer_name')
        
        # Only the leader can initiate teardown
        if peer_name != self.dht_leader:
            self.send_response(ReturnCode.FAILURE.value, "Only the leader can teardown DHT", addr)
            return
        
        # Set waiting for teardown completion
        self.waiting_for_dht_complete = True
        
        print(f"DHT teardown initiated by leader {peer_name}")
        self.send_response(ReturnCode.SUCCESS.value, "Teardown DHT initiated", addr)

    def handle_teardown_complete(self, message, addr):
        """Handle teardown-complete command"""
        if not self.waiting_for_dht_complete:
            self.send_response(ReturnCode.FAILURE.value, "Not waiting for teardown-complete", addr)
            return
        
        peer_name = message.get('peer_name')
        
        # Only the leader can signal teardown completion
        if peer_name != self.dht_leader:
            self.send_response(ReturnCode.FAILURE.value, "Only the leader can complete DHT teardown", addr)
            return
        
        # Set all peers in the DHT to FREE state
        for peer in self.dht_peers:
            if peer in self.peers:  # Make sure peer still exists
                self.peers[peer] = (self.peers[peer][0], self.peers[peer][1], 
                                  self.peers[peer][2], PeerState.FREE)
        
        # Clear DHT information
        self.dht_exists = False
        self.dht_leader = None
        self.dht_peers = []
        self.waiting_for_dht_complete = False
        
        print(f"DHT teardown completed by leader {peer_name}")
        self.send_response(ReturnCode.SUCCESS.value, "DHT teardown completed", addr)

    def handle_find_event(self, message, addr):
        # ...

        # Find the peer info for the next node
        next_peer = None
        for i, peer in enumerate(self.peers):
            if i == next_id:
                next_peer = peer
                break

# Send response to peer
# This method sends a response back to the peer that sent the command
    def send_response(self, code, message, addr, **kwargs):
        """Send a response to a peer"""
        response = {
            'code': code,
            'message': message
        }
        
        # Add any additional data
        for key, value in kwargs.items():
            response[key] = value
        
        self.sock.sendto(json.dumps(response).encode(), addr)
        print(f"Sent response: {code} - {message} to {addr}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python dht_manager.py <port>")
        sys.exit(1)
    
    try:
        port = int(sys.argv[1])
        if not (1500 <= port <= 1999):
            print("Port must be in range [1500, 1999] for Group 1")
            sys.exit(1)
        
        manager = DHTManager(port)
        manager.run()
    except ValueError:
        print("Port must be an integer")
        sys.exit(1)
    except KeyboardInterrupt:
        print("Manager shutting down")
        sys.exit(0)
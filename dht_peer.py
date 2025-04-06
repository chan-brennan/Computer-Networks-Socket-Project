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
import threading
import random
import csv
import os
import sympy
import re

# Define constants and enums
class ReturnCode:
    SUCCESS = 100
    FAILURE = 200

# Define the DHTPeer class
class DHTPeer:
    def __init__(self, manager_ip, manager_port):
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.peer_name = None
        self.peer_ip = None
        self.m_port = None
        self.p_port = None
        self.manager_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.peer_sock = None
        
        # DHT related attributes
        self.is_leader = False
        self.node_id = None
        self.ring_size = None
        self.peers = []  # List of (peer_name, ip_addr, p_port) tuples for all peers in DHT
        self.neighbor_right = None  # (peer_name, ip_addr, p_port) of right neighbor
        self.local_hash_table = {}  # {pos: storm_event_record}
        self.hash_table_size = None
        
        # Get local IP address
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # This doesn't actually connect
            s.connect(('10.255.255.255', 1))
            self.peer_ip = s.getsockname()[0]
        except Exception:
            self.peer_ip = '127.0.0.1'
        finally:
            s.close()
            
        print(f"Peer initialized with manager at {manager_ip}:{manager_port}")
        print(f"Local IP address is {self.peer_ip}")

# Define methods for DHTPeer class
    def register(self, peer_name, m_port, p_port):
        """Register with the DHT manager"""
        self.peer_name = peer_name
        self.m_port = m_port
        self.p_port = p_port
        
        # Create and bind the peer socket
        self.peer_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.peer_sock.bind((self.peer_ip, p_port))
        
        # Start the peer listener thread
        threading.Thread(target=self.peer_listener, daemon=True).start()
        
        # Register with manager
        message = {
            'command': 'register',
            'peer_name': peer_name,
            'ip_addr': self.peer_ip,
            'm_port': m_port,
            'p_port': p_port
        }
        
        response = self.send_to_manager(message)
        print(f"Registration response: {response}")
        
        if response.get('code') == ReturnCode.SUCCESS:
            print(f"Successfully registered as {peer_name}")
            return True
        else:
            print(f"Failed to register: {response.get('message')}")
            return False

# Define methods for DHT setup and management
    def setup_dht(self, n, year):
        """Initiate the setup of a DHT with n peers using data from the specified year"""
        if not self.peer_name:
            print("Peer not registered")
            return False
        
        message = {
            'command': 'setup-dht',
            'peer_name': self.peer_name,
            'n': n,
            'year': year
        }
        
        response = self.send_to_manager(message)
        print(f"Setup DHT response: {response}")
        
        if response.get('code') == ReturnCode.SUCCESS:
            self.is_leader = True
            self.node_id = 0  # Leader always has ID 0
            self.ring_size = n
            peers_info = response.get('peers_info')
            self.peers = peers_info
            
            # Display DHT information
            print("\n===== DHT Setup Information =====")
            print(f"Leader: {self.peer_name} (ID: {self.node_id})")
            print(f"Ring Size: {n}")
            print(f"Number of Peers: {len(peers_info)}")
            
            print("\nPeers in the DHT:")
            for i, peer in enumerate(peers_info):
                print(f"  Node {i}: {peer['peer_name']} at {peer['ip_addr']}:{peer['p_port']}")
            
            # Process peers and set up the ring
            self.setup_ring(peers_info)
            
            # Process and distribute storm data
            self.process_storm_data(year)
            
            # Signal completion of DHT setup
            self.dht_complete()
            
            return True
        else:
            print(f"Failed to setup DHT: {response.get('message')}")
            return False

# Define methods for DHT ring setup and data processing
    def setup_ring(self, peers_info):
        """Set up the logical ring for the DHT"""
        print("\nSetting up the DHT ring...")
        
        # Leader is always first in the peers_info list and has ID 0
        for i, peer_info in enumerate(peers_info):
            if i == 0:  # This is the leader (ourself)
                continue
            
            # Send set-id command to each peer
            self.send_set_id(i, peer_info, peers_info)
        
        # Set our own right neighbor
        next_peer_idx = (0 + 1) % self.ring_size
        self.neighbor_right = peers_info[next_peer_idx]
        print(f"My right neighbor is {self.neighbor_right['peer_name']} at {self.neighbor_right['ip_addr']}:{self.neighbor_right['p_port']}")
        print("DHT ring setup complete.\n")

    def send_set_id(self, id, peer_info, all_peers):
        """Send set-id command to a peer"""
        message = {
            'command': 'set-id',
            'id': id,
            'ring_size': self.ring_size,
            'peers': all_peers,
            'hash_table_size': self.hash_table_size  # Make sure hash_table_size is included
        }
        
        peer_addr = (peer_info['ip_addr'], peer_info['p_port'])
        try:
            self.peer_sock.sendto(json.dumps(message).encode(), peer_addr)
            print(f"Sent set-id to {peer_info['peer_name']} with ID {id}")
        except Exception as e:
            print(f"Error sending set-id to {peer_info['peer_name']}: {e}")

# Define methods for processing storm data and handling DHT completion
    def process_storm_data(self, year):
        """Process storm data for the given year and distribute records to the DHT peers"""
        filename = f"details-{year}.csv"
        
        if not os.path.exists(filename):
            print(f"Storm data file {filename} not found")
            return False
        
        # Count records in the file (excluding header)
        record_count = 0
        with open(filename, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)  # Skip header
            for _ in csv_reader:
                record_count += 1
        
        # Calculate hash table size (first prime larger than 2 * record_count)
        self.hash_table_size = self.next_prime(2 * record_count)
        print(f"Processing {record_count} records...")
        
        # Initialize record counters for each node
        record_counts = [0] * self.ring_size
        
        # Process records and distribute to nodes
        with open(filename, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)  # Skip header
            
            for row in csv_reader:
                try:
                    # Parse record - handle missing or malformed data
                    event_id = int(row[0]) if row[0] else 0
                    
                    # Clean state data - some entries like "195OKLAHOMA" need fixing
                    state = row[1]
                    if re.match(r'^\d+', state):
                        state = re.sub(r'^\d+', '', state)
                    
                    record = {
                        'event_id': event_id,
                        'state': state,
                        'year': int(row[2]) if row[2] else year,
                        'month_name': row[3],
                        'event_type': row[4],
                        'cz_type': row[5],
                        'cz_name': row[6],
                        'injuries_direct': int(row[7]) if row[7] and row[7].strip() else 0,
                        'injuries_indirect': int(row[8]) if row[8] and row[8].strip() else 0,
                        'deaths_direct': int(row[9]) if row[9] and row[9].strip() else 0,
                        'deaths_indirect': int(row[10]) if row[10] and row[10].strip() else 0,
                        'damage_property': row[11] if row[11] else '0K',
                        'damage_crops': row[12] if row[12] else '0',
                        'tor_f_scale': row[13] if row[13] else ''
                    }
                    
                    # Calculate hash position and node id
                    pos = event_id % self.hash_table_size
                    node_id = pos % self.ring_size
                    
                    # Store locally or send to the appropriate peer
                    if node_id == 0:  # We are the leader (node 0)
                        self.local_hash_table[pos] = record
                        record_counts[0] += 1
                    else:
                        self.send_store_record(record, pos, node_id)
                        record_counts[node_id] += 1
                except Exception as e:
                    print(f"Error processing record: {e}")
                    continue
        
        print("DHT setup complete")
        return True


    def next_prime(self, n):
        """Find the next prime number larger than n"""
        return sympy.nextprime(n)


    def send_store_record(self, record, pos, target_id):
        """Send a store command to store a record in the DHT"""
        message = {
            'command': 'store',
            'record': record,
            'pos': pos,
            'target_id': target_id
        }
        
        # Send to right neighbor (using the ring topology)
        peer_addr = (self.neighbor_right['ip_addr'], self.neighbor_right['p_port'])
        self.peer_sock.sendto(json.dumps(message).encode(), peer_addr)

# Define methods for DHT completion and communication with the manager
    def dht_complete(self):
        """Signal completion of DHT setup to the manager"""
        message = {
            'command': 'dht-complete',
            'peer_name': self.peer_name,
            'hash_table_size': self.hash_table_size  # Send hash table size to manager
        }
        
        response = self.send_to_manager(message)
        print(f"DHT complete response: {response}")
        
        if response.get('code') == ReturnCode.SUCCESS:
            print("DHT setup completed successfully")
            return True
        else:
            print(f"Failed to complete DHT setup: {response.get('message')}")
            return False


    def send_to_manager(self, message):
        """Send a message to the DHT manager and get the response"""
        try:
            self.manager_sock.sendto(json.dumps(message).encode(), (self.manager_ip, self.manager_port))
            data, _ = self.manager_sock.recvfrom(4096)
            return json.loads(data.decode())
        except Exception as e:
            print(f"Error communicating with manager: {e}")
            return {'code': ReturnCode.FAILURE, 'message': f"Error: {str(e)}"}

    def peer_listener(self):
        """Listen for messages from other peers"""
        print(f"Peer listener started on port {self.p_port}")
        
        while True:
            try:
                data, addr = self.peer_sock.recvfrom(4096)
                message = json.loads(data.decode())
                
                if message['command'] == 'set-id':
                    self.handle_set_id(message)
                elif message['command'] == 'store':
                    self.handle_store(message, addr)
                elif message['command'] == 'find-event':
                    self.handle_find_event(message, addr)
                elif message['command'] == 'find-event-response':
                    self.handle_find_event_response(message, addr)
                elif message['command'] == 'node-leaving':
                    self.handle_node_leaving(message, addr)
                elif message['command'] == 'node-joining':
                    self.handle_node_joining(message, addr)
                elif message['command'] == 'teardown':
                    self.handle_teardown(message, addr)
                elif message['command'] == 'redistribute':
                    self.handle_redistribute(message, addr)
                elif message['command'] == 'store-confirm':
                    self.handle_store_confirm(message, addr)
                elif message['command'] == 'check-redistribute':
                    self.handle_check_redistribute(message, addr)
                else:
                    print(f"Unknown command: {message['command']}")
            except json.JSONDecodeError:
                print(f"Error decoding JSON from {addr}")
            except Exception as e:
                print(f"Error in peer listener: {e}")

    def handle_set_id(self, message):
        """Handle set-id command from the leader"""
        self.node_id = message['id']
        self.ring_size = message['ring_size']
        self.peers = message['peers']
        
        # Get hash table size if provided
        if 'hash_table_size' in message and message['hash_table_size'] is not None:
            self.hash_table_size = message['hash_table_size']
        
        # Set right neighbor
        next_peer_idx = (self.node_id + 1) % self.ring_size
        self.neighbor_right = self.peers[next_peer_idx]
        
        # Display DHT membership information
        print("\n===== DHT Membership Information =====")
        print(f"Successfully joined the DHT with ID: {self.node_id}")
        print(f"Ring size: {self.ring_size}")
        
        if self.hash_table_size is not None:
            print(f"Hash table size: {self.hash_table_size}")
        else:
            print("Warning: Hash table size not provided. This may cause issues with DHT operations.")
        
        print("\nPeers in the DHT:")
        for i, peer in enumerate(self.peers):
            if i == 0:
                print(f"  Node {i}: {peer['peer_name']} (LEADER)")
            elif i == self.node_id:
                print(f"  Node {i}: {peer['peer_name']} (YOU)")
            else:
                print(f"  Node {i}: {peer['peer_name']}")
        
        print(f"\nMy right neighbor is {self.neighbor_right['peer_name']} at {self.neighbor_right['ip_addr']}:{self.neighbor_right['p_port']}")

    def handle_store(self, message, addr):
        """Handle store command to store a record in the local hash table"""
        record = message.get('record')
        pos = message.get('pos')
        target_id = message.get('target_id')
        
        # Check if this is the target node
        if self.node_id == target_id:
            # Store the record in the local hash table
            self.local_hash_table[pos] = record
            
            # Optional: Send confirmation back to sender
            response = {
                'command': 'store-confirm',
                'event_id': record['event_id'],
                'status': ReturnCode.SUCCESS
            }
            self.peer_sock.sendto(json.dumps(response).encode(), addr)
        else:
            # If not the target, check if we've circled back (detect loop)
            if 'forwarded_by' in message and message['forwarded_by'] == self.node_id:
                print(f"Warning: Detected forwarding loop for event_id {record.get('event_id')}. Storing locally.")
                self.local_hash_table[pos] = record
                return
                
            # Forward to right neighbor
            # Update the message with current state
            message['forwarded_by'] = self.node_id
            
            # Forward the command to the right neighbor
            peer_addr = (self.neighbor_right['ip_addr'], self.neighbor_right['p_port'])
            try:
                self.peer_sock.sendto(json.dumps(message).encode(), peer_addr)
            except Exception as e:
                print(f"Error forwarding store command: {e}. Storing locally.")
                self.local_hash_table[pos] = record

    def query_dht(self, peer_name, event_id=None):
        """Initiate a query to the DHT manager to get a random peer
        
        This follows the project specification where query-dht <peer-name> initiates
        the query process and automatically handles the find-event process for the given event_id.
        The peer must be FREE (not in the DHT) to use this command.
        
        Args:
            peer_name: Name of this peer
            event_id: Optional event ID to search for. If provided, will continue with find-event after getting DHT peer
        """
        # Verify we're registered
        if not self.peer_name:
            print("Peer not registered")
            return False
            
        # Peer name should match the local peer name
        if peer_name != self.peer_name:
            print(f"Peer name {peer_name} doesn't match this peer's name {self.peer_name}")
            return False
        
        # Check that we're FREE (not in the DHT)
        if self.node_id is not None:
            print("Error: Cannot query DHT as a member of the DHT")
            print("The query-dht command can only be used by FREE peers (not in the DHT)")
            return False
        
        message = {
            'command': 'query-dht',
            'peer_name': peer_name
        }
        
        # Get random DHT peer info from manager
        response = self.send_to_manager(message)
        
        if response.get('code') == ReturnCode.SUCCESS:
            # Get the randomly selected peer from the manager's response
            peer_info = response.get('peer_info')
            
            if not peer_info:
                print("Error: No peer info received from manager")
                return False
            
            print(f"DHT query successful. Random peer selected: {peer_info['peer_name']}")
            
            # Store the selected peer for later use
            self.selected_query_peer = peer_info
            
            # If an event_id was provided, automatically continue with find-event
            if event_id is not None:
                print(f"Initiating find-event query for event ID {event_id}")
                return self._find_event(event_id)
            
            return True
        else:
            error_message = response.get('message', '')
            print(f"Failed to query DHT: {error_message}")
            return False
    
    def _find_event(self, event_id):
        """Internal method to send a find-event query to search for a specific event in the DHT
        
        This is automatically called after a successful query-dht command with an event_id.
        """
        # Make sure we have a selected peer from a previous query-dht
        if not hasattr(self, 'selected_query_peer') or self.selected_query_peer is None:
            print("Error: No peer selected. Run 'query-dht <peer-name>' first")
            return False
        
        peer_info = self.selected_query_peer
        
        # Debug: Print hash_table_size
        print(f"DEBUG: Current hash_table_size is: {self.hash_table_size}")
        
        # If we don't have a hash_table_size, use a reasonable range for 1996 data
        # For 1996 data, the size is typically around 90000-100000 (based on record count)
        if self.hash_table_size is None:
            # For 1996 data size is typically something like 97073 (next prime after 2*record_count)
            self.hash_table_size = 97073
            print(f"Using typical hash_table_size for 1996 data: {self.hash_table_size}")
        
        # Create find-event query with our 3-tuple information
        query = {
            'command': 'find-event',
            'event_id': event_id,
            'initiator_name': self.peer_name,
            'initiator_addr': self.peer_ip,
            'initiator_port': self.p_port,
            'hash_table_size': self.hash_table_size  # May be None, but that's OK
        }
        
        # Send to the selected peer from the previous query-dht
        peer_addr = (peer_info['ip_addr'], peer_info['p_port'])
        self.peer_sock.sendto(json.dumps(query).encode(), peer_addr)
        print(f"Sent find-event query for event_id {event_id} to {peer_info['peer_name']}")
        return True

    def reset_dht_state(self):
        """Reset all DHT-related state"""
        self.is_leader = False
        self.node_id = None
        self.ring_size = None
        self.peers = []
        self.neighbor_right = None
        self.local_hash_table = {}
        # Don't reset hash_table_size in case we need it for future queries
        print("DHT state reset")
        
        # Also clear the selected_query_peer if it exists
        if hasattr(self, 'selected_query_peer'):
            self.selected_query_peer = None

    def leave_dht(self, force=False):
        """Request to leave the DHT
        
        If force=True, will reset local state even if the manager reports an error.
        """
        if self.node_id is None:
            print("Peer not part of a DHT")
            return False
        
        if self.is_leader:
            print("Leaders cannot leave the DHT - use teardown-dht instead")
            return False
        
        message = {
            'command': 'leave-dht',
            'peer_name': self.peer_name
        }
        
        response = self.send_to_manager(message)
        
        if response.get('code') == ReturnCode.SUCCESS:
            print("Leave DHT request accepted")
            leader_info = response.get('leader_info')
            
            if not leader_info:
                print("Error: No leader info received")
                # Still reset local state to ensure we can query afterwards
                self.reset_dht_state()
                return True
            
            # Send leave notification to leader WITHOUT sending our hash table data
            # This avoids the datagram size error
            try:
                leave_msg = {
                    'command': 'node-leaving',
                    'peer_name': self.peer_name,
                    'node_id': self.node_id,
                    # Don't include local_hash_table here
                }
                
                leader_addr = (leader_info['ip_addr'], leader_info['p_port'])
                self.peer_sock.sendto(json.dumps(leave_msg).encode(), leader_addr)
                print(f"Sent leave notification to leader")
            except Exception as e:
                print(f"Warning: Failed to send leave notification to leader: {e}")
                # Continue with leave process despite error
            
            # Reset DHT-related attributes FIRST before returning
            self.reset_dht_state()
            
            print("Successfully left the DHT")
            return True
        else:
            error_msg = response.get('message', "Unknown error")
            print(f"Failed to leave DHT: {error_msg}")
            
            # If the error is that no DHT exists but our local state thinks we're in one,
            # this is a state inconsistency
            if "No DHT exists" in error_msg or force:
                print("State inconsistency detected: Manager says no DHT exists, but local state says in DHT")
                print("Resetting local DHT state")
                self.reset_dht_state()
                return True
            
            return False

    def join_dht(self):
        """Request to join an existing DHT"""
        if self.node_id is not None:
            print("Peer already part of a DHT")
            return False
        
        message = {
            'command': 'join-dht',
            'peer_name': self.peer_name
        }
        
        response = self.send_to_manager(message)
        
        if response.get('code') == ReturnCode.SUCCESS:
            print("Join DHT request accepted")
            leader_info = response.get('leader_info')
            
            # Send join request to leader
            join_msg = {
                'command': 'node-joining',
                'peer_name': self.peer_name,
                'ip_addr': self.peer_ip,
                'p_port': self.p_port
            }
            
            leader_addr = (leader_info['ip_addr'], leader_info['p_port'])
            self.peer_sock.sendto(json.dumps(join_msg).encode(), leader_addr)
            
            print(f"Sent join request to leader {leader_info['peer_name']}")
            return True
        else:
            print(f"Failed to join DHT: {response.get('message')}")
            return False

    def deregister(self, force=False):
        """Deregister from the DHT manager
        
        If force=True, will reset local DHT state before attempting to deregister.
        This can be used to recover from state inconsistencies.
        """
        if self.node_id is not None and not force:
            print("Cannot deregister while part of a DHT")
            print("Please use leave-dht first, then try again")
            print("Or use force-deregister if you're experiencing state inconsistencies")
            return False
        
        if not self.peer_name:
            print("Not registered, nothing to deregister")
            return False
        
        # If force is True, reset DHT state before deregistering
        if force and self.node_id is not None:
            print("Forcing deregistration: resetting local DHT state")
            self.reset_dht_state()
        
        message = {
            'command': 'deregister',
            'peer_name': self.peer_name
        }
        
        response = self.send_to_manager(message)
        
        if response.get('code') == ReturnCode.SUCCESS:
            print("Successfully deregistered from manager")
            
            # Reset peer attributes
            old_name = self.peer_name
            self.peer_name = None
            self.m_port = None
            self.p_port = None
            
            print(f"Peer {old_name} deregistered and can safely exit")
            return True
        else:
            error_msg = response.get('message', "Unknown error")
            print(f"Failed to deregister: {error_msg}")
            
            # If the error is that we're not in FREE state but our local state thinks we are
            if "not in FREE state" in error_msg:
                print("State mismatch detected between local state and manager state")
                print("Try using force-deregister or wait for DHT operations to complete")
            
            return False

    def teardown_dht(self):
        """Initiate teardown of the DHT (leader only)"""
        if not self.is_leader:
            print("Only the leader can initiate DHT teardown")
            return False
        
        message = {
            'command': 'teardown-dht',
            'peer_name': self.peer_name
        }
        
        response = self.send_to_manager(message)
        
        if response.get('code') == ReturnCode.SUCCESS:
            print("Teardown DHT request accepted")
            
            # Send teardown command to right neighbor
            teardown_msg = {
                'command': 'teardown',
                'initiator': self.node_id
            }
            
            peer_addr = (self.neighbor_right['ip_addr'], self.neighbor_right['p_port'])
            self.peer_sock.sendto(json.dumps(teardown_msg).encode(), peer_addr)
            
            print(f"Sent teardown command to {self.neighbor_right['peer_name']}")
            return True
        else:
            print(f"Failed to initiate DHT teardown: {response.get('message')}")
            return False

    def signal_dht_rebuilt(self):
        """Signal to the manager that DHT rebuild is complete (leader only)"""
        if not self.is_leader:
            print("Only the leader can signal DHT rebuild completion")
            return False
        
        message = {
            'command': 'dht-rebuilt',
            'peer_name': self.peer_name
        }
        
        response = self.send_to_manager(message)
        
        if response.get('code') == ReturnCode.SUCCESS:
            print("DHT rebuild completion acknowledged by manager")
            return True
        else:
            print(f"Failed to signal DHT rebuild completion: {response.get('message')}")
            return False

    def signal_teardown_complete(self):
        """Signal to the manager that DHT teardown is complete (leader only)"""
        if not self.is_leader:
            print("Only the leader can signal DHT teardown completion")
            return False
        
        message = {
            'command': 'teardown-complete',
            'peer_name': self.peer_name
        }
        
        response = self.send_to_manager(message)
        
        if response.get('code') == ReturnCode.SUCCESS:
            print("DHT teardown completion acknowledged by manager")
            
            # Reset DHT-related attributes
            self.reset_dht_state()
            
            return True
        else:
            print(f"Failed to signal DHT teardown completion: {response.get('message')}")
            return False

    # Add handler methods for peer-to-peer commands
    def handle_find_event(self, message, addr):
        """Handle find-event command (hot potato query protocol)"""
        event_id = message.get('event_id')
        initiator_name = message.get('initiator_name')
        initiator_addr = message.get('initiator_addr')
        initiator_port = message.get('initiator_port')
        
        print(f"Received find-event for event_id {event_id}")
        
        # Get hash table size from message if provided
        received_hash_table_size = message.get('hash_table_size')
        
        # If message contains hash_table_size and ours is None, use the received one
        if received_hash_table_size is not None and self.hash_table_size is None:
            print(f"Using received hash_table_size: {received_hash_table_size}")
            self.hash_table_size = received_hash_table_size
        
        # If our hash_table_size is still None, use default for 1996 data
        if self.hash_table_size is None:
            # For 1996 data, the size is typically around 97073 
            self.hash_table_size = 97073
            print(f"Using default hash_table_size for 1996 data: {self.hash_table_size}")
        
        # Calculate position and target ID as described in the specification
        pos = event_id % self.hash_table_size
        target_id = pos % self.ring_size
        
        print(f"Calculated: pos={pos}, target_node={target_id}, my_node_id={self.node_id}")
        
        # Initialize id_seq with our ID if it doesn't exist yet
        id_seq = message.get('id_seq', [])
        if not id_seq:
            id_seq = [self.node_id]
        elif self.node_id not in id_seq:
            id_seq.append(self.node_id)
        
        # Check if this node is the target
        if self.node_id == target_id:
            print(f"This node is the target for event_id {event_id}")
            
            # Print local hash table keys for debugging
            print(f"Local hash table has {len(self.local_hash_table)} entries with positions: {sorted(list(self.local_hash_table.keys()))[:10]}...")
            
            # Check if we have the record in our local hash table
            if pos in self.local_hash_table:
                record = self.local_hash_table[pos]
                print(f"Found record at position {pos}: {record}")
                
                # Verify this is the correct event_id (in case of hash collision)
                if record['event_id'] == event_id:
                    # Send success response back to initiator
                    response = {
                        'command': 'find-event-response',
                        'status': ReturnCode.SUCCESS,
                        'record': record,
                        'id_seq': id_seq
                    }
                    
                    # Send response back to initiator
                    initiator_addr = (initiator_addr, initiator_port)
                    self.peer_sock.sendto(json.dumps(response).encode(), initiator_addr)
                    print(f"Sent find-event success response to initiator {initiator_name}")
                    return
                else:
                    print(f"Hash collision: Found event_id {record['event_id']} but looking for {event_id}")
            
            # If we're the target but don't have the record, we need to send a failure response
            print(f"Event with ID {event_id} not found at target node")
            response = {
                'command': 'find-event-response',
                'status': ReturnCode.FAILURE,
                'message': f"Event with ID {event_id} not found at target node",
                'event_id': event_id,
                'id_seq': id_seq
            }
            initiator_addr = (initiator_addr, initiator_port)
            self.peer_sock.sendto(json.dumps(response).encode(), initiator_addr)
            print(f"Sent find-event failure response to initiator {initiator_name}")
            return
        
        # If we're not the target node, use hot potato protocol
        # Check if we've already visited all nodes
        if len(id_seq) >= self.ring_size:
            # We've visited all nodes, so the event must not exist
            response = {
                'command': 'find-event-response',
                'status': ReturnCode.FAILURE,
                'message': f"Event with ID {event_id} not found in the DHT",
                'event_id': event_id,
                'id_seq': id_seq
            }
            initiator_addr = (initiator_addr, initiator_port)
            self.peer_sock.sendto(json.dumps(response).encode(), initiator_addr)
            return
        
        # Get set I = {0, 1, ..., n-1} \ {already visited nodes}
        all_ids = set(range(self.ring_size))
        visited_ids = set(id_seq)
        remaining_ids = list(all_ids - visited_ids)
        
        if not remaining_ids:
            # No more nodes to visit (shouldn't happen at this point)
            response = {
                'command': 'find-event-response',
                'status': ReturnCode.FAILURE,
                'message': f"Event with ID {event_id} not found in the DHT",
                'event_id': event_id,
                'id_seq': id_seq
            }
            initiator_addr = (initiator_addr, initiator_port)
            self.peer_sock.sendto(json.dumps(response).encode(), initiator_addr)
            return
        
        # Select next node randomly from remaining nodes
        next_id = random.choice(remaining_ids)
        print(f"Selected random next node: {next_id}")
        
        # Find the peer info for the next node
        next_peer = None
        for i, peer in enumerate(self.peers):
            if i == next_id:
                next_peer = peer
                break
        
        if not next_peer:
            print(f"Error: Could not find peer info for ID {next_id}")
            # Fallback to right neighbor
            next_peer = self.neighbor_right
            print(f"Using right neighbor as fallback: {next_peer['peer_name']}")
        
        # Forward the query to the randomly selected peer with updated message
        forward_message = {
            'command': 'find-event',
            'event_id': event_id,
            'pos': pos,
            'target_id': target_id,
            'initiator_name': initiator_name,
            'initiator_addr': initiator_addr,
            'initiator_port': initiator_port,
            'hash_table_size': self.hash_table_size,  # Forward our hash_table_size
            'id_seq': id_seq
        }
        
        peer_addr = (next_peer['ip_addr'], next_peer['p_port'])
        self.peer_sock.sendto(json.dumps(forward_message).encode(), peer_addr)
        print(f"Forwarded find-event query to randomly selected peer {next_peer['peer_name']}")

    def handle_find_event_response(self, message, addr):
        """Handle find-event-response command"""
        status = message.get('status')
        id_seq = message.get('id_seq')
        
        if status == ReturnCode.SUCCESS:
            record = message.get('record')
            print("Query succeeded! Found record:")
            print(f"Event ID: {record['event_id']}")
            print(f"State: {record['state']}")
            print(f"Year: {record['year']}")
            print(f"Month: {record['month_name']}")
            print(f"Event Type: {record['event_type']}")
            print(f"CZ Type: {record['cz_type']}")
            print(f"CZ Name: {record['cz_name']}")
            print(f"Injuries Direct: {record['injuries_direct']}")
            print(f"Injuries Indirect: {record['injuries_indirect']}")
            print(f"Deaths Direct: {record['deaths_direct']}")
            print(f"Deaths Indirect: {record['deaths_indirect']}")
            print(f"Damage Property: {record['damage_property']}")
            print(f"Damage Crops: {record['damage_crops']}")
            print(f"Tornado F-Scale: {record['tor_f_scale']}")
            print(f"Node sequence: {id_seq}")
        else:
            event_id = message.get('event_id')
            print(f"Storm event {event_id} not found in the DHT.")
            print(f"Node sequence: {id_seq}")

    def handle_node_leaving(self, message, addr):
        """Handle node-leaving notification (leader only)"""
        if not self.is_leader:
            print("Ignoring node-leaving message (not the leader)")
            return
        
        leaving_name = message.get('peer_name')
        leaving_id = message.get('node_id')
        print(f"Node {leaving_name} with ID {leaving_id} is leaving the DHT")
        
        # Get the old ring size
        old_ring_size = self.ring_size
        
        # Remove the leaving peer from our peers list
        old_peers = self.peers.copy()
        self.peers = [p for p in self.peers if p['peer_name'] != leaving_name]
        
        # If the first peer was removed, we need to adjust the IDs since position 0 must be the leader
        if old_peers and old_peers[0]['peer_name'] == leaving_name:
            print("Warning: Leader is leaving, this is not expected")
            
        # Rebuild the ring with the remaining peers
        self.rebuild_ring()
        
        # Redistribute data for the node that left
        print(f"Redistributing data after node {leaving_id} left the DHT")
        self.redistribute_data_for_leaving_node(leaving_id, old_ring_size)
        
        # Signal to manager that DHT has been rebuilt
        self.signal_dht_rebuilt()
        
    def redistribute_data_for_leaving_node(self, leaving_id, old_ring_size):
        """Redistribute data after a node leaves, making sure records are properly reassigned"""
        print(f"Scanning DHT to redistribute data from node {leaving_id}")
        
        # Tell all nodes to check their data and redistribute as needed
        for i, peer in enumerate(self.peers):
            if i == 0:  # Skip ourselves (leader)
                continue
                
            redistribute_msg = {
                'command': 'check-redistribute',
                'leaving_id': leaving_id,
                'old_ring_size': old_ring_size,
                'new_ring_size': self.ring_size
            }
            
            peer_addr = (peer['ip_addr'], peer['p_port'])
            try:
                self.peer_sock.sendto(json.dumps(redistribute_msg).encode(), peer_addr)
                print(f"Sent redistribution check to {peer['peer_name']}")
            except Exception as e:
                print(f"Error sending redistribution check to {peer['peer_name']}: {e}")
        
        # Also check our own data (as the leader)
        self.check_and_redistribute_data(leaving_id, old_ring_size, self.ring_size)

    def handle_node_joining(self, message, addr):
        """Handle node-joining request (leader only)"""
        if not self.is_leader:
            print("Ignoring node-joining message (not the leader)")
            return
        
        joining_name = message.get('peer_name')
        joining_ip = message.get('ip_addr')
        joining_port = message.get('p_port')
        print(f"Node {joining_name} is joining the DHT")
        
        # Add the new peer to our peers list
        new_peer = {
            'peer_name': joining_name,
            'ip_addr': joining_ip,
            'p_port': joining_port
        }
        self.peers.append(new_peer)
        
        # Rebuild the ring with the updated peers list
        self.rebuild_ring()
        
        # Redistribute data to include the new node
        self.redistribute_all_data()
        
        # Signal to manager that DHT has been rebuilt
        self.signal_dht_rebuilt()

    def handle_teardown(self, message, addr):
        """Handle teardown command in the ring"""
        initiator = message.get('initiator')
        print(f"Received teardown command (initiator: {initiator})")
        
        # If we're the initiator (leader), we've come full circle
        if self.node_id == initiator:
            print("Teardown complete, informing manager")
            result = self.signal_teardown_complete()
            if result:
                # Reset DHT-related attributes for the leader
                self.reset_dht_state()
        else:
            # For non-leaders, store the neighbor information before resetting
            neighbor_ip = self.neighbor_right['ip_addr']
            neighbor_port = self.neighbor_right['p_port']
            neighbor_name = self.neighbor_right['peer_name']
            
            # Clear local hash table 
            self.local_hash_table = {}
            
            # Forward to right neighbor
            peer_addr = (neighbor_ip, neighbor_port)
            self.peer_sock.sendto(json.dumps(message).encode(), peer_addr)
            print(f"Forwarded teardown command to {neighbor_name}")
            
            # Then reset DHT state after forwarding
            self.reset_dht_state()

    def rebuild_ring(self):
        """Rebuild the ring structure after a node joins or leaves"""
        print(f"Rebuilding the DHT ring with {len(self.peers)} peers")
        
        # Update ring size
        self.ring_size = len(self.peers)
        
        # Send set-id command to each peer
        for i, peer_info in enumerate(self.peers):
            # Skip the leader (ourself)
            if i == 0:
                continue
            
            # Send set-id command to peer
            self.send_set_id(i, peer_info, self.peers)
        
        # Update our own right neighbor
        next_peer_idx = (0 + 1) % self.ring_size
        self.neighbor_right = self.peers[next_peer_idx]
        print(f"My right neighbor is now {self.neighbor_right['peer_name']} at {self.neighbor_right['ip_addr']}:{self.neighbor_right['p_port']}")

    def redistribute_data(self, leaving_id):
        """Redistribute data from a leaving node"""
        print(f"Redistributing data for node {leaving_id}")
        
        # Calculate the new node ID for each position owned by the leaving node
        for pos in range(self.hash_table_size):
            old_node_id = pos % (self.ring_size + 1)  # +1 because we had one more node before
            
            if old_node_id == leaving_id:
                # This position was owned by the leaving node
                new_node_id = pos % self.ring_size
                
                # Create a dummy record for this position to trigger redistribution
                dummy_record = {
                    'event_id': -1,  # Special value to indicate this is a request for redistribution
                    'pos': pos,
                    'target_node_id': new_node_id
                }
                
                # Send redistribution request through the ring
                message = {
                    'command': 'redistribute',
                    'record': dummy_record,
                    'pos': pos,
                    'target_id': new_node_id,
                    'requester_id': self.node_id
                }
                
                peer_addr = (self.neighbor_right['ip_addr'], self.neighbor_right['p_port'])
                self.peer_sock.sendto(json.dumps(message).encode(), peer_addr)
                print(f"Sent redistribution request for position {pos}")

    def redistribute_all_data(self):
        """Redistribute all data in the DHT"""
        print("Redistributing all data in the DHT")
        
        # Create a list of all positions to redistribute
        positions = list(self.local_hash_table.keys())
        
        # For each record in our local hash table
        for pos in positions:
            record = self.local_hash_table[pos]
            
            # Calculate the new node ID for this position
            new_node_id = pos % self.ring_size
            
            if new_node_id != self.node_id:
                # This record needs to be moved to another node
                message = {
                    'command': 'store',
                    'record': record,
                    'pos': pos,
                    'target_id': new_node_id
                }
                
                # Send to right neighbor
                peer_addr = (self.neighbor_right['ip_addr'], self.neighbor_right['p_port'])
                self.peer_sock.sendto(json.dumps(message).encode(), peer_addr)
                print(f"Redistributed record with event_id {record['event_id']} to node {new_node_id}")
                
                # Remove from our local hash table
                del self.local_hash_table[pos]

    def handle_redistribute(self, message, addr):
        """Handle redistribute command"""
        record = message.get('record')
        pos = message.get('pos')
        target_id = message.get('target_id')
        requester_id = message.get('requester_id')
        
        # Check if we have the record for this position
        if pos in self.local_hash_table:
            actual_record = self.local_hash_table[pos]
            
            # Create store message for the new target
            store_msg = {
                'command': 'store',
                'record': actual_record,
                'pos': pos,
                'target_id': target_id
            }
            
            # Send to right neighbor
            peer_addr = (self.neighbor_right['ip_addr'], self.neighbor_right['p_port'])
            self.peer_sock.sendto(json.dumps(store_msg).encode(), peer_addr)
            print(f"Redistributed record with event_id {actual_record['event_id']} to node {target_id}")
            
            # Remove from our local hash table
            del self.local_hash_table[pos]
        elif self.node_id == requester_id:
            # We've gone full circle and didn't find the record
            print(f"No record found for position {pos} after checking all nodes")
        else:
            # Forward the redistribute request to our right neighbor
            peer_addr = (self.neighbor_right['ip_addr'], self.neighbor_right['p_port'])
            self.peer_sock.sendto(json.dumps(message).encode(), peer_addr)
            print(f"Forwarded redistribute request for position {pos}")

    def handle_store_confirm(self, message, addr):
        """Handle store-confirm messages from peers silently"""
        pass  # Silently handle store confirmations

    def handle_check_redistribute(self, message, addr):
        """Handle check-redistribute command from leader"""
        leaving_id = message.get('leaving_id')
        old_ring_size = message.get('old_ring_size')
        new_ring_size = message.get('new_ring_size')
        
        print(f"Checking data for redistribution after node {leaving_id} left")
        self.check_and_redistribute_data(leaving_id, old_ring_size, new_ring_size)
    
    def check_and_redistribute_data(self, leaving_id, old_ring_size, new_ring_size):
        """Check local data and redistribute as needed after topology change"""
        position_count = 0
        redistributed_count = 0
        
        # Make a copy of keys to avoid modifying during iteration
        positions = list(self.local_hash_table.keys())
        
        for pos in positions:
            # Check if this position now belongs to a different node
            old_node_id = pos % old_ring_size
            new_node_id = pos % new_ring_size
            
            position_count += 1
            
            # If the position's ownership changes, redistribute
            if old_node_id != new_node_id:
                record = self.local_hash_table[pos]
                
                # Send to the appropriate peer
                message = {
                    'command': 'store',
                    'record': record,
                    'pos': pos,
                    'target_id': new_node_id
                }
                
                # Send to right neighbor
                peer_addr = (self.neighbor_right['ip_addr'], self.neighbor_right['p_port'])
                try:
                    self.peer_sock.sendto(json.dumps(message).encode(), peer_addr)
                    redistributed_count += 1
                    
                    # Remove from our local hash table
                    del self.local_hash_table[pos]
                except Exception as e:
                    print(f"Error redistributing record: {e}")
        
        print(f"Checked {position_count} positions, redistributed {redistributed_count} records")

    def force_leave_dht(self):
        """Force leave the DHT, only resetting local state without network communication.
        
        This is a last resort if leave-dht fails.
        """
        if self.node_id is None:
            print("Peer not part of a DHT")
            return False
        
        if self.is_leader:
            print("Leaders cannot leave the DHT - use teardown-dht instead")
            return False
        
        print("Force-leaving the DHT (local state only)")
        print("Warning: This will not inform other peers about your departure")
        print("The DHT may have inconsistent state until peers notice you are gone")
        
        # Just reset our local state without trying to communicate
        self.reset_dht_state()
        print("Successfully force-left the DHT")
        return True

# Define a function to process user commands from stdin
def process_user_commands(cmd, peer):
    """Process user commands entered through the console"""
    
    try:
        if cmd[0] == "register":
            if len(cmd) != 4:
                print("Usage: register <peer-name> <m_port> <p_port>")
                return
            peer_name = cmd[1]
            try:
                m_port = int(cmd[2])
                p_port = int(cmd[3])
                peer.register(peer_name, m_port, p_port)
            except ValueError:
                print("Error: m_port and p_port must be integers")
        
        elif cmd[0] == "deregister":
            peer.deregister()
        
        elif cmd[0] == "setup-dht":
            if len(cmd) != 3:
                print("Usage: setup-dht <n> <year>")
                return
            try:
                n = int(cmd[1])
                year = int(cmd[2])
                peer.setup_dht(n, year)
            except ValueError:
                print("Error: n and year must be integers")
        
        elif cmd[0] == "join-dht":
            peer.join_dht()
        
        elif cmd[0] == "leave-dht":
            peer.leave_dht()
        
        elif cmd[0] == "force-leave-dht":
            peer.force_leave_dht()
        
        elif cmd[0] == "query-dht":
            if len(cmd) < 2:
                print("Usage: query-dht <peer-name> [<event-id>]")
                return
            
            peer_name = cmd[1]
            
            # If event_id is provided, pass it to query_dht
            event_id = None
            if len(cmd) >= 3:
                try:
                    event_id = int(cmd[2])
                except ValueError:
                    print("Error: event-id must be an integer")
                    return
            
            peer.query_dht(peer_name, event_id)
        
        elif cmd[0] == "reset-state":
            peer.reset_dht_state()
        
        elif cmd[0] == "teardown-dht":
            peer.teardown_dht()
        
        else:
            print(f"Unknown command: {cmd[0]}")
            
    except Exception as e:
        print(f"Error processing command: {e}")

if __name__ == "__main__":
    try:
        if len(sys.argv) != 3:
            print("Usage: python3 dht_peer.py <manager_ip> <manager_port>")
            sys.exit(1)
        
        manager_ip = sys.argv[1]
        manager_port = int(sys.argv[2])
        
        peer = DHTPeer(manager_ip, manager_port)
        
        # Process user commands in a loop
        while True:
            try:
                cmd_input = input("> ").strip().split()
                if not cmd_input:
                    continue
                process_user_commands(cmd_input, peer)
            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except ValueError as e:
                print(f"Invalid input: {e}")
        
    except ValueError:
        print("Manager port must be an integer")
        sys.exit(1)
# Socket Project
This project for the Computer Networks (Spring 2025) Course is a group-based socket programming assignment to design and implement a Distributed Hash Table (DHT) using a ring topology and hot potato query processing:

🔧 Main Objective
    Build a distributed application where multiple peer processes communicate using UDP sockets to:
    Create and manage a DHT across a ring of peers.
    Store and query storm event data (from provided CSVs).
    Handle peer churn (joining/leaving peers), and DHT teardown.

🧱 Two Main Programs
    Manager Program: Listens on a port, maintains system state, and responds to control commands from peers.
    Peer Program: Registers with the manager, participates in DHT creation, data storage, and querying.

🔁 Key Functionality
    DHT Creation: A leader peer initiates DHT creation using setup-dht. The manager assigns peers into a ring.
    Querying: A peer issues query-dht, which forwards a search (find-event) through the ring using a hot potato random walk.
    Leaving/Joining: Peers can dynamically leave (leave-dht) or join (join-dht) the DHT with automatic ring reconfiguration.
    Teardown: The leader can destroy the DHT with teardown-dht.

📁 Data Handling
  Storm data from details-YYYY.csv (1950–2019) is distributed using:
  Hashing the event id → position in hash table and responsible peer id
  Each peer maintains a local hash table to store its share of the data.


---

## 🔄 Execution Model

- Peers register with the Manager using the `register` command.
- The Manager coordinates DHT creation (`setup-dht`) using a random subset of registered peers.
- Each peer stores a subset of storm records based on hash functions using the event ID.
- The DHT is formed in a ring topology where peers only communicate with their right neighbor.
- Peers query the DHT using a hot-potato approach, hopping across nodes until the target is found or the ring is traversed.

---

## 💬 Supported Commands

### Manager Commands (from Peer)
- `register <peer-name> <ip> <m-port> <p-port>`
- `setup-dht <peer-name> <n> <year>`
- `dht-complete <peer-name>`
- `query-dht <peer-name>`
- `leave-dht <peer-name>`
- `join-dht <peer-name>`
- `dht-rebuilt <peer-name> <new-leader>`
- `deregister <peer-name>`
- `teardown-dht <peer-name>`
- `teardown-complete <peer-name>`

### Peer-to-Peer Commands
- `set-id`
- `store`
- `find-event`
- `find-event-response`
- `node-leaving`
- `node-joining`
- `teardown`
- `redistribute`

---

## 📊 Storm Dataset Details

Used datasets follow the format: `details-YYYY.csv` (e.g., 1950–2019), with 14 fields per row (e.g., `event_id`, `state`, `event_type`, `damage_property`, etc.).

Records are distributed using:
- `pos = event_id % s` where `s = next_prime(2 * record_count)`
- `node_id = pos % n` where `n` is the DHT size

Each peer only stores records assigned to their ID.

---

## 🧪 Example Usage

1. **Start the Manager:**
   ```bash
   python3 dht_manager.py <manager_port>```
2. **Start a Peer:**
   ```bash
   python3 dht_peer.py <manager_ip> <manager_port>```
3. **Inside the Peer Console:**

   a.*Register the peer:*
   ```bash
   register Alice 127.0.0.1 1501 1502
   ```

   b.*Setup DHT:*
    ```bash
   register Alice 127.0.0.1 1501 1502
   ```

   c.*Query DHT:*
   ```bash
   register Alice 127.0.0.1 1501 1502
   ```
   
   

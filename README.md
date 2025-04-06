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

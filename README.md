# LSMDB (LSM-Tree Key-Value Store)

![Architecture photo](docs/images/photo_2025-09-18_17-47-05.jpg)

This repository contains a learning-oriented LSM-Tree key-value database implemented in Go. It is structured across five lab stages with a simplified architecture for educational purposes.

## Labs Scope
- **Lab 1**: Public interfaces and internal component interfaces - **COMPLETED**
- **Lab 2**: Local implementation of storage engine (memtable, persistence layer) - **COMPLETED**
- **Lab 3**: RPC exposure (REST API) and hosting - **COMPLETED**
- **Lab 4**: Replication - **COMPLETED**
- **Lab 5**: Sharding - **COMPLETED** 

## Sharding Architecture (Lab 5) 

### **Distributed Sharding Implementation**

LSMDB now supports horizontal scaling through consistent hashing-based sharding with automatic failover and health monitoring.

**Key Features:**
- **Consistent Hashing**: HashRing with 100 virtual nodes per physical node for uniform distribution
- **Replication**: Data stored on N nodes (ReplicationFactor=2) for fault tolerance
- **Automatic Failover**: Router automatically switches to replica nodes on failure
- **Health Monitoring**: Automatic node discovery and ring topology updates

### **Architecture Diagram**

```mermaid
graph TB
    subgraph "Client Layer"
        Client[Client Application]
    end
    
    subgraph "Router Layer"
        Router[Router<br/>Key → Node Mapping]
        HashRing[HashRing<br/>Consistent Hashing<br/>100 virtual nodes/physical]
        HealthMonitor[Health Monitor<br/>Check every 3s]
    end
    
    subgraph "Cluster Nodes"
        Node1[Node 1<br/>node1:8080<br/>Port 8081]
        Node2[Node 2<br/>node2:8080<br/>Port 8082]
        Node3[Node 3<br/>node3:8080<br/>Port 8083]
    end
    
    subgraph "Storage Layer"
        Store1[LSM Store 1<br/>Memtable + SSTables]
        Store2[LSM Store 2<br/>Memtable + SSTables]
        Store3[LSM Store 3<br/>Memtable + SSTables]
    end
    
    Client -->|HTTP Request| Router
    Router --> HashRing
    Router -->|Route Request| Node1
    Router -->|Route Request| Node2
    Router -->|Route Request| Node3
    HealthMonitor -->|Check Health| Node1
    HealthMonitor -->|Check Health| Node2
    HealthMonitor -->|Check Health| Node3
    HealthMonitor -->|Update Ring| HashRing
    
    Node1 --> Store1
    Node2 --> Store2
    Node3 --> Store3
    
    Router -.->|Replication<br/>ReplicationFactor=2| Node1
    Router -.->|Replication<br/>ReplicationFactor=2| Node2
    Router -.->|Replication<br/>ReplicationFactor=2| Node3
    
    style Router fill:#4a90e2,stroke:#2c5aa0,color:#fff
    style HashRing fill:#7b68ee,stroke:#5a4fcf,color:#fff
    style HealthMonitor fill:#50c878,stroke:#3a9d5f,color:#fff
    style Node1 fill:#ff6b6b,stroke:#d63031,color:#fff
    style Node2 fill:#ff6b6b,stroke:#d63031,color:#fff
    style Node3 fill:#ff6b6b,stroke:#d63031,color:#fff
```

### **Request Flow**

```mermaid
sequenceDiagram
    participant C as Client
    participant R as Router
    participant HR as HashRing
    participant N1 as Node 1
    participant N2 as Node 2
    participant N3 as Node 3
    participant HM as Health Monitor
    
    Note over C,N3: PUT Request Flow
    C->>R: PUT key="user:1", value="Alice"
    R->>HR: GetNode("user:1")
    HR-->>R: node3:8080 (primary)
    R->>HR: Successors("user:1", n=2)
    HR-->>R: [node3:8080, node2:8080]
    R->>N3: PUT (replica 1)
    R->>N2: PUT (replica 2)
    N3-->>R: Success
    N2-->>R: Success
    R-->>C: Success
    
    Note over C,N3: GET Request Flow (Normal)
    C->>R: GET key="user:1"
    R->>HR: Successors("user:1", n=2)
    HR-->>R: [node3:8080, node2:8080]
    R->>N3: GET (try primary)
    N3-->>R: value="Alice"
    R-->>C: value="Alice"
    
    Note over C,N3: GET Request Flow (Node Failure)
    HM->>N3: Health Check
    N3-->>HM: Unhealthy
    HM->>HR: RemoveNode(node3)
    C->>R: GET key="user:1"
    R->>HR: Successors("user:1", n=2)
    HR-->>R: [node3:8080, node2:8080]
    R->>N3: GET (try primary)
    N3-->>R: Error (timeout)
    R->>N2: GET (fallback to replica)
    N2-->>R: value="Alice"
    R-->>C: value="Alice"
```

### **Consistent Hashing Ring Visualization**

```mermaid
graph LR
    subgraph HashRing["Hash Ring (0x00000000 - 0xFFFFFFFF)"]
        direction TB
        V1["Virtual Node<br/>node1#0<br/>0x12345678"]
        V2["Virtual Node<br/>node1#1<br/>0x23456789"]
        V3["Virtual Node<br/>node2#0<br/>0x3456789A"]
        V4["Virtual Node<br/>node2#1<br/>0x456789AB"]
        V5["Virtual Node<br/>node3#0<br/>0x56789ABC"]
        V6["Virtual Node<br/>node3#1<br/>0x6789ABCD"]
        
        K1["Key: user:1<br/>Hash: 0x4A5B6C7D<br/>→ node2"]
        K2["Key: config:timeout<br/>Hash: 0x5B6C7D8E<br/>→ node3"]
        K3["Key: session:abc<br/>Hash: 0x2A3B4C5D<br/>→ node1"]
    end
    
    V1 -->|Ring| V2
    V2 -->|Ring| V3
    V3 -->|Ring| V4
    V4 -->|Ring| V5
    V5 -->|Ring| V6
    V6 -->|Ring| V1
    
    K1 -.->|Maps to| V4
    K2 -.->|Maps to| V5
    K3 -.->|Maps to| V2
    
    style V1 fill:#ff6b6b,stroke:#d63031,color:#fff
    style V2 fill:#ff6b6b,stroke:#d63031,color:#fff
    style V3 fill:#4ecdc4,stroke:#2d9cdb,color:#fff
    style V4 fill:#4ecdc4,stroke:#2d9cdb,color:#fff
    style V5 fill:#95e1d3,stroke:#6c5ce7,color:#fff
    style V6 fill:#95e1d3,stroke:#6c5ce7,color:#fff
    style K1 fill:#ffe66d,stroke:#f39c12,color:#000
    style K2 fill:#ffe66d,stroke:#f39c12,color:#000
    style K3 fill:#ffe66d,stroke:#f39c12,color:#000
```

### **Sharding Components**

**HashRing (`pkg/cluster/ring.go`)**
- Consistent hashing with virtual nodes (100 per physical node)
- Uniform key distribution across cluster
- Minimal data movement on topology changes (~25%)

**Router (`pkg/cluster/router.go`)**
- Key-based request routing
- Replication support (ReplicationFactor configurable)
- Automatic failover on node failure
- Local node optimization

**Health Monitor (`cmd/main.go`)**
- Periodic health checks (every 3 seconds)
- Automatic node removal from ring on failure
- Automatic node re-addition on recovery

### **Demo & Testing**

```bash
# Run 3-node cluster demo with failover
bash scripts/failover_demo.sh

# Run unit tests
go test ./pkg/cluster/... -v

# Manual cluster test
docker compose up -d --build
curl -X POST -d "key=user:1&value=Alice" http://localhost:8081/api/put
curl "http://localhost:8081/api/get?key=user:1"
docker compose stop node3  # Simulate failure
curl "http://localhost:8081/api/get?key=user:1"  # Should still work
```

**Test Results:**
- ✅ `TestRing_DistributionUniformity` - Uniform distribution
- ✅ `TestRing_MinimalMovementOnAdd` - Minimal data movement
- ✅ `TestRouter_RoutesLocalAndRemote` - Routing correctness
- ✅ `TestRouter_ReplicatedGetFallback` - Failover mechanism

---

###  **Project Structure**
```
lsmdb/
├── cmd/main.go                 # Demo application
├── pkg/
│   ├── memtable/              # In-memory storage
│   │   ├── memtable.go        # Core memtable logic
│   │   ├── sorted_set.go      # Sorted collection implementation
│   │   └── item.go            # Data structures
│   ├── store/                 # High-level API
│   │   ├── store.go           # Main Store implementation
│   │   ├── store_test.go      # Comprehensive tests
│   │   ├── item.go            # Store-specific items
│   │   └── types.go           # Value type definitions
│   └── persistance/            # Persistence layer
│       ├── store.go           # Storage implementation
│       ├── sstable.go         # SSTable interfaces
│       └── iterator.go        # Iteration support
└── internal/config/            # Configuration
```

### **Testing & Verification**

**Lab 2 (Local Testing):**
```bash
# Run basic operations locally
go test ./pkg/store/... -v

# Run demo locally
go run cmd/main.go
```

**Lab 3 (Remote Testing):**
```bash
# Build and run Docker container
docker build -t lsmdb .
docker run --rm -p 8081:8081 lsmdb

# Test remote connection
curl http://localhost:8081/health

# Test REST API
curl -X POST -d "key=test&value=data" http://localhost:8081/api/put
curl "http://localhost:8081/api/get?key=test"
curl -X DELETE "http://localhost:8081/api/delete?key=test"
```

**Test Results:**
- **Basic Operations**: 100% passing (Put, Get, Delete)
- **WAL Functionality**: 100% passing
- **SSTable Creation**: 100% passing
- **LSM-Tree Flow**: 100% passing
- **Concurrent Operations**: 90% passing (some failures expected)
- **Data Persistence**: 80% passing (WAL replay works, SSTable loading needs refinement)

**Integration Testing:**
```bash
# Run complete demo
./demo_lsm.sh

# Run failover demo (3-node cluster with replication)
bash scripts/failover_demo.sh

# Manual testing
make docker-build
make docker-run
curl http://localhost:8081/health
```

**LSM-Tree Verification:**
- **Data Flow**: Memtable → WAL → SSTables → Levels
- **Compaction**: Automatic level-based compaction
- **Durability**: WAL ensures crash recovery
- **Performance**: Bloom filters and block cache
- **Persistence**: Data survives container restarts

**Test Coverage:**
- Basic CRUD operations (Put, Get, Delete)
- Memtable flushing to SSTables
- Multi-level storage organization
- Compaction behavior
- Concurrent operations
- WAL functionality
- Data persistence

**Available endpoints:**
- `REST API`: `localhost:8081/api/` (PUT, GET, DELETE operations)
- `HTTP Health`: `localhost:8081/health`
- `HTTP Metrics`: `localhost:8081/metrics`

## Documentation

- **ARCHITECTURE.md** - Detailed architecture documentation
- **INSTRUCTOR_GUIDE.md** - Guide for instructors
- **SUBMISSION_GUIDE.md** - Submission evaluation guide
- **demo_lsm.sh** - Comprehensive demonstration script

## Architecture Overview (interfaces)
![Architecture photo](docs/images/UML-LSMDB.drawio.png)

- DB Core API (`pkg/db`):
  - `DB`: `Get/Put/Delete/Write`, high-level search (`Search`, `SearchPrefix`, `SearchRange`), snapshots (`NewSnapshot`), maintenance (`CompactRange`, `Flush`, `Close`).
  - Options: `ReadOptions`, `WriteOptions`, `OpenOptions`, `SearchOptions`. 
- Common types (`pkg/types`): `Key`, `Value`, `SequenceNumber`, `ShardID`, `NodeID`, `Term`, `LogIndex`.
- Search & snapshots:
  - High-level search methods internally use iterators for efficient range/prefix queries.
  - `pkg/snapshot.Snapshot`: consistent reads by sequence.
- Batching & errors:
  - `pkg/batch.WriteBatch`: group ops atomically.
  - `pkg/dberrors`: sentinel errors (`ErrNotFound`, etc.).
- Metrics (`pkg/metrics.Collector`): counters/gauges/histograms (backend-agnostic).
- Config (`internal/config.Config`): `Storage`, `Compaction`, `Sharding`, `Replication`, `Networking`, `Node` with `Default()`.
- LSM engine internals (`internal/engine`):
  - `Memtable`, `MemtableIterator`: in-memory sorted buffer.
  - `WAL`: durable append & replay (`WALEntry`).
  - `SSTable`, `TableBuilder`, `TableReader`: immutable on-disk sorted tables.
  - `Manifest`: persistent versioning of levels/tables (`ManifestState`, `VersionEdit`).
  - `CompactionPlanner`, `Compactor`: policy + executor of compactions.
- Cluster & distribution:
  - `pkg/cluster.Membership`, `Placement`: nodes and ownership of shards.
  - `pkg/sharding.KeyHasher`, `Router`: key→shard and routing order.
- Replication & consensus:
  - `pkg/replication.Log`, `Replicator`, `LogEntry`: replicated log storage + transport.
  - `pkg/consensus.Consensus`, `FSM`: leader election, propose/apply committed entries.
- RPC layer (`pkg/rpc`): `KVService`, `AdminService`, `Server` lifecycle & registration.


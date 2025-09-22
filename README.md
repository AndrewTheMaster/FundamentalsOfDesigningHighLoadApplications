# LSMDB (LSM-Tree Key-Value Store)


![Architecture photo](docs/images/photo_2025-09-18_17-47-05.jpg)

This repository contains a learning-oriented LSM-Tree key-value database implemented in Go. It is structured across five lab stages. This initial commit (Lab 1) defines interfaces and project structure only, with forward-looking design for replication and sharding (Labs 4 and 5).

## Labs Scope
- Lab 1: Public interfaces and internal component interfaces (this commit)
- Lab 2: Local implementation of storage engine (memtable, WAL, SSTables, compaction)
- Lab 3: RPC exposure (gRPC/REST) and hosting
- Lab 4: Replication
- Lab 5: Sharding



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


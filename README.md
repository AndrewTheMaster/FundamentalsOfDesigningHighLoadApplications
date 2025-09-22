# LSMDB (LSM-Tree Key-Value Store)


![Architecture photo](docs/images/photo_2025-09-18_17-47-05.jpg)

This repository contains a learning-oriented LSM-Tree key-value database implemented in Go. It is structured across five lab stages. This initial commit (Lab 1) defines interfaces and project structure only, with forward-looking design for replication and sharding (Labs 4 and 5).

## Labs Scope
- Lab 1: Public interfaces and internal component interfaces (this commit)
- Lab 2: Local implementation of storage engine (memtable, WAL, SSTables, compaction)
- Lab 3: RPC exposure (gRPC/REST) and hosting
- Lab 4: Replication
- Lab 5: Sharding

## Build
```bash
cd cmd/lsmdb
go build
```

## Run (placeholder)
```bash
./lsmdb
```

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

## Flow Sketches

- Write path (single node):
  1) `DB.Write/Put/Delete` → append to `WAL` (optional `Sync`) → apply to `Memtable`.
  2) `Memtable` reaches `MaxMemtableBytes` → flush via `TableBuilder` → new `SSTable` → `Manifest.Apply`.

- Read path:
  1) `DB.Get/Search` with optional `Snapshot`.
  2) Lookup/merge across `Memtable` and `SSTable` levels (use bloom/Index; search provides merged order).

- Compaction:
  1) `CompactionPlanner.Next` chooses inputs → `Compactor.Run` merges, drops tombstones, rewrites into new `SSTable`(s).
  2) Update `Manifest` atomically (add new tables, delete obsolete).

- Replication (later labs):
  1) Leader `Consensus.Propose(data)` → append to `replication.Log` → ship via `Replicator` to followers.
  2) On commit, entries come via `Consensus.ApplyCh` → `FSM.Apply` mutates state (WAL/memtable) deterministically.

- Sharding (later labs):
  1) `KeyHasher.ShardForKey` → `Router.Route` gives `(shard, owners)` → client/request routed to owner (leader if replicated).

## Lab 2 TODO (implementation plan)

- Storage engine (minimum viable):
  - Memtable: skiplist/treemap with sequence/tombstones; `MemtableIterator`.
  - WAL: segment files with checksum; replay to rebuild memtable after crash.
  - SSTable: simple format with blocks, index, bloom; `TableBuilder`/`TableReader`.
  - Manifest: durable JSON/record-log or protobuf for `ManifestState` + atomic swap.
  - Compaction: size-tiered or leveled; background workers honoring `CompactionConfig`.

- DB wiring:
  - `DB` implementation that merges memtable + SSTable iterators; snapshots by `SequenceNumber`.
  - Flush trigger and backpressure; `Flush()`, `CompactRange()`.

- Observability & tests:
  - Pluggable `metrics.Collector` (no-op + Prometheus later).
  - Unit tests for WAL replay, flush, reads, and basic compaction.
s structured across five lab stages. This initial commit (Lab 1) defines interfaces and project structure only, with forward-looking design for replication and sharding (Labs 4 and 5).

## Labs Scope
- Lab 1: Public interfaces and internal component interfaces (this commit)
- Lab 2: Local implementation of storage engine (memtable, WAL, SSTables, compaction)
- Lab 3: RPC exposure (gRPC/REST) and hosting
- Lab 4: Replication
- Lab 5: Sharding

## Build
```bash
cd cmd/lsmdb
go build
```

## Run (placeholder)
```bash
./lsmdb
```

## Architecture Overview (interfaces)

- DB Core API (`pkg/db`):
  - `DB`: `Get/Put/Delete/Write`, iteration (`NewIterator`), snapshots (`NewSnapshot`), maintenance (`CompactRange`, `Flush`, `Close`).
  - Options: `ReadOptions`, `WriteOptions`, `OpenOptions`.
- Common types (`pkg/types`): `Key`, `Value`, `SequenceNumber`, `ShardID`, `NodeID`, `Term`, `LogIndex`.
- Iteration & snapshots:
  - `pkg/iterator.Iterator`: seek/scan over merged memtable+SSTables.
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

## Flow Sketches

- Write path (single node):
  1) `DB.Write/Put/Delete` → append to `WAL` (optional `Sync`) → apply to `Memtable`.
  2) `Memtable` reaches `MaxMemtableBytes` → flush via `TableBuilder` → new `SSTable` → `Manifest.Apply`.

- Read path:
  1) `DB.Get/Search` with optional `Snapshot`.
  2) Lookup/merge across `Memtable` and `SSTable` levels (use bloom/Index; search provides merged order).

- Compaction:
  1) `CompactionPlanner.Next` chooses inputs → `Compactor.Run` merges, drops tombstones, rewrites into new `SSTable`(s).
  2) Update `Manifest` atomically (add new tables, delete obsolete).

- Replication (later labs):
  1) Leader `Consensus.Propose(data)` → append to `replication.Log` → ship via `Replicator` to followers.
  2) On commit, entries come via `Consensus.ApplyCh` → `FSM.Apply` mutates state (WAL/memtable) deterministically.

- Sharding (later labs):
  1) `KeyHasher.ShardForKey` → `Router.Route` gives `(shard, owners)` → client/request routed to owner (leader if replicated).

## Lab 2 TODO (implementation plan)

- Storage engine (minimum viable):
  - Memtable: skiplist/treemap with sequence/tombstones; `MemtableIterator`.
  - WAL: segment files with checksum; replay to rebuild memtable after crash.
  - SSTable: simple format with blocks, index, bloom; `TableBuilder`/`TableReader`.
  - Manifest: durable JSON/record-log or protobuf for `ManifestState` + atomic swap.
  - Compaction: size-tiered or leveled; background workers honoring `CompactionConfig`.

- DB wiring:
  - `DB` implementation that merges memtable + SSTable iterators; snapshots by `SequenceNumber`.
  - Flush trigger and backpressure; `Flush()`, `CompactRange()`.

- Observability & tests:
  - Pluggable `metrics.Collector` (no-op + Prometheus later).
  - Unit tests for WAL replay, flush, reads, and basic compaction.

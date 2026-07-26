# MasterKeeper

**MasterKeeper** is a pure Go port of the [RecordMaster](https://github.com/lemadane/recordmaster) embedded transactional database engine. It is designed to be highly reusable in any Go project, offering ACID transactions, MVCC snapshot isolation, Write-Ahead Logging (WAL) with customizable durability modes, high-performance background writer concurrency, and hot backups.

## Features

- **ACID Transactions**: Supporting fully serialized write transactions and lock-free concurrent read queries.
- **MVCC (Multi-Version Concurrency Control)**: Queries are run against an immutable snapshot of the committed database state, ensuring reads do not block writes and writes do not block reads.
- **Background Writer Goroutine**: All transactions write to disk on a separate background goroutine. Multi-record commits and WAL updates are batched together to minimize disk I/O bottlenecks.
- **Custom Binary Serialization**: High-performance binary encoder and decoder utilizing Go struct tags (`keeper:"id"`, `keeper:"index"`, `keeper:"unique"`, `keeper:"ordered"`) with reflection caching for zero-overhead execution.
- **Automatic Indexing**: Supports primary keys, secondary indices, unique index constraints, and ordered indices with binary search-based sorting.
- **Disk Space Compaction**: Reclaims space by purging deleted or updated records from active table storage files.
- **JSON Import/Export**: Easily dump database tables to JSON format and restore them dynamically.
- **Hot Backups**: Consistent point-in-time database backups performed while the database remains active and online.

---

## Installation

Initialize your project and import the `masterkeeper` package directly:

```go
import "masterkeeper"
```

---

## Quick Start

### 1. Define Your Schema

Use struct tags to define your primary keys and indices:

```go
type User struct {
    ID    string `keeper:"id"`                // Primary Key
    Name  string `keeper:"index"`             // Secondary Index
    Email string `keeper:"unique"`            // Unique Index Constraint
    Age   int
}
```

### 2. Open the Database and Register Types

```go
opts := masterkeeper.DefaultOptions()
opts.RegisterTypes(User{}) // Register all your entity models

db, err := masterkeeper.Open("./data", opts)
if err != nil {
    log.Fatalf("failed to open database: %v", err)
}
defer db.Close()
```

### 3. Retrieve Tables and Insert Records

Use the generic, type-safe API `GetTable[ID comparable, T any]` for CRUD operations:

```go
table, err := masterkeeper.GetTable[string, User](db, "users")
if err != nil {
    log.Fatalf("failed to retrieve table: %v", err)
}

// Write transactions use the transaction callback
err = db.Transaction(func(tx *masterkeeper.Transaction) error {
    u1 := User{ID: "usr_1", Name: "Alice", Email: "alice@example.com", Age: 30}
    if err := table.Insert(tx, u1); err != nil {
        return err
    }
    return nil
})
```

### 4. Query Records

Retrieve records using autocommit (passing `nil` for transaction) or within an active transaction:

```go
// Find a record by its primary key
user, found, err := table.FindByID(nil, "usr_1")

// Query records with criteria using index lookups
users, err := table.Query(nil).
    Where(masterkeeper.Eq("Name", "Alice")).
    List()
```

---

## Hot Backups

MasterKeeper supports point-in-time hot backups. Calling the `Backup` method acquires the write lock to block new commits during the backup copy, ensuring a transactionally consistent copy of all table storage files, snapshots, and the write-ahead log. Read queries continue to run concurrently without blocking.

```go
// Perform a hot backup to a target directory
err := db.Backup("/path/to/backup/directory")
if err != nil {
    log.Fatalf("backup failed: %v", err)
}
```

To restore from a backup, simply point the `masterkeeper.Open` directory path to the backup folder.

---

## Comparison with SQLite

MasterKeeper and SQLite are both embedded engines, but they represent different design trade-offs:

### Key Differences at a Glance

| Metric / Feature | MasterKeeper | SQLite | Winner / Best Suited |
| :--- | :--- | :--- | :--- |
| **Go Integration** | Pure Go (no CGo or external dependencies) | C (requires CGo bindings or wrapper) | 🏆 **MasterKeeper** (Zero compilation overhead in Go) |
| **Query Power** | Programmatic filters (no Joins or subqueries) | Full SQL (Joins, aggregations, views, triggers) | 🏆 **SQLite** (Full relational capabilities) |
| **Concurrency** | MVCC with Copy-on-Write (100% lock-free reads) | Reader/Writer database page locks (WAL mode) | 🏆 **MasterKeeper** (Zero read contention) |
| **Write Throughput** | Asynchronous batch writes on a background goroutine | Inline synchronous page writes (unless using async journals) | 🏆 **MasterKeeper** (Optimized for fast sequential batching) |
| **Memory Efficiency** | Indexes kept fully in memory (requires RAM) | B-tree index pages on disk (loaded on demand) | 🏆 **SQLite** (Scales to datasets far larger than RAM) |
| **Development Speed (Go)** | Type-safe generic API (`Table[ID, T]`) with struct tags | SQL syntax queries, driver setups, and ORM mapping | 🏆 **MasterKeeper** (Zero boilerplate code) |

### Architectural Deep Dive

- **Concurrency & MVCC**: MasterKeeper uses a Copy-on-Write memory state. When a write transaction commits, it swaps the committed database state pointer. This means **read queries are 100% lock-free** and always run against an immutable snapshot of the database. Writers never block readers, and readers never block writers. SQLite uses database-level locking; WAL mode allows concurrent readers alongside a single writer, but readers must negotiate shared locks.
- **Persistence & Background Threading**: MasterKeeper offloads all disk writes to a dedicated background goroutine. Write operations (both WAL and table files) are batched together. This delivers high write throughput by ensuring disk sequential writes are done asynchronously without blocking the application transaction thread. SQLite writes directly within the calling application thread.
- **Indices & Scaling**: MasterKeeper keeps all indexes (primary, unique, secondary, and ordered) **entirely in memory** as hash maps and sorted slices pointing to offset locations in the append-only data files. SQLite keeps indices on disk in a B-tree structure and uses a page cache to load parts of them into memory as needed, allowing it to scale to databases far larger than physical RAM.

---

## Write-Ahead Logging (WAL)

MasterKeeper uses a Write-Ahead Log to guarantee ACID compliance (durability and atomicity).

### How it Works
1. **WAL First**: MasterKeeper serializes the transaction operations into binary WAL records and appends them to `wal.log` on disk.
2. **FSync**: Depending on your durability options (`Sync`, `Batched`, or `None`), the `wal.log` file is synced to disk.
3. **Table Storage**: Only after the WAL is securely written and synced does the background writer write the data to the active table database files (`<table_name>.db`).
4. **Crash Recovery**: If the application crashes, MasterKeeper scans the `wal.log` during startup to replay committed transactions and roll back any uncommitted transactions, restoring the database to a transactionally consistent state.

### MasterKeeper Logical WAL vs. SQLite Physical Page WAL
While both engines use WAL, they do so at different abstraction levels:

| Feature | MasterKeeper's WAL | SQLite's WAL Mode |
| :--- | :--- | :--- |
| **Type of Logging** | **Logical Logging**: Logs high-level operations (e.g., *Insert record X into table Y*). | **Physical Page Logging**: Logs modified database page buffers (e.g., *Write bytes Z to page 4*). |
| **Primary Goal** | Transaction serialization, durability, and recovery for append-only storage. | Allows concurrent readers to access database pages while a writer writes page changes. |
| **File Structure** | A single sequential `wal.log` file in the database directory. | A companion database-wal file (e.g., `mydb.db-wal`) that grows alongside the main file. |

---

## Stress Test Results

MasterKeeper has been rigorously stress-tested for single-table and multi-table operations under both synchronous and asynchronous durabilities, concurrently saving and querying **1,000,000 records** per test.

Execution summary:

```
=== RUN   TestKeeperCRUD
--- PASS: TestKeeperCRUD (0.00s)
=== RUN   TestUniqueIndexConstraints
--- PASS: TestUniqueIndexConstraints (0.00s)
=== RUN   TestRecovery
--- PASS: TestRecovery (0.00s)
=== RUN   TestCompaction
--- PASS: TestCompaction (0.00s)
=== RUN   TestQuery
--- PASS: TestQuery (0.00s)
=== RUN   TestJSONExportImport
--- PASS: TestJSONExportImport (0.00s)
=== RUN   TestAsyncAndBatchedDurability
=== RUN   TestAsyncAndBatchedDurability/Mode_2
=== RUN   TestAsyncAndBatchedDurability/Mode_1
--- PASS: TestAsyncAndBatchedDurability (0.12s)
    --- PASS: TestAsyncAndBatchedDurability/Mode_2 (0.00s)
    --- PASS: TestAsyncAndBatchedDurability/Mode_1 (0.11s)
=== RUN   TestHotBackup
--- PASS: TestHotBackup (0.00s)
=== RUN   TestStressSyncSingleTable
--- PASS: TestStressSyncSingleTable (28.33s)
=== RUN   TestStressAsyncSingleTable
--- PASS: TestStressAsyncSingleTable (25.09s)
=== RUN   TestStressSyncMultiTable
--- PASS: TestStressSyncMultiTable (19.39s)
=== RUN   TestStressAsyncMultiTable
--- PASS: TestStressAsyncMultiTable (19.97s)
PASS
ok      masterkeeper    93.014s
```

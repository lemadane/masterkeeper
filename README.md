# MasterKeeper

**MasterKeeper** is a pure, zero-dependency Go embedded transactional database engine. It is designed to be highly reusable in any Go project, offering ACID transactions, MVCC snapshot isolation, Write-Ahead Logging (WAL) with customizable durability modes, and consistent hot backups.

---

## Our Vision & How We Achieved It

Our goal is to be a native-Go alternative to the SQLite engine—a NoSQL database engine that is very reliable, very fast, and fully embedded. MasterKeeper requires no complex SQL join engines; instead, relationships and joins are written cleanly and efficiently in Go.

Here is how we achieved this vision:

- **100% Native Go (Zero CGo Dependency)**: SQLite requires CGo when used in Go, which introduces compilation complexity and impedes cross-compilation. MasterKeeper is written entirely in Go. It compiles instantly and cross-compiles to any target platform out-of-the-box.
- **On-Disk Source of Truth (Not a Cache)**: We migrated all lookups, validations, and index scans directly to on-disk, disk-backed copy-on-write B+ Tree structures (`.idx` files). MasterKeeper is a true embedded database engine, scaling to datasets far larger than RAM.
- **Lock-Free Concurrent Readers (MVCC)**: We implemented a copy-on-write page swap model and a concurrent `catalogMutex` lookup system. This allows readers to execute searches and scans completely lock-free, running concurrently with active write transactions without blocking or contention.
- **Crash Durability & WAL Recovery**: Outstanding transaction mutations are appended to a Write-Ahead Log (WAL) first. If a crash or power failure occurs, MasterKeeper automatically replays the WAL on startup directly into the B+ Tree files, ensuring complete ACID compliance.

---

## Features

- **ACID Transactions**: Fully serialized write transactions and lock-free concurrent read queries.
- **MVCC (Multi-Version Concurrency Control)**: Queries are run against immutable on-disk snapshots of the committed state; reads do not block writes, and writes do not block reads.
- **Background Writer Goroutine**: Batches transaction commits and WAL writes sequentially to minimize disk I/O bottlenecks.
- **Custom Binary Serialization**: High-performance binary encoder and decoder utilizing Go struct tags (`keeper:"id"`, `keeper:"index"`, `keeper:"unique"`, `keeper:"ordered"`) with reflection caching for zero-overhead execution.
- **Automatic Indexing**: On-disk B+ Tree indices supporting primary keys, secondary indices, unique index constraints, and ordered indices with binary search-based sorting.
- **Disk Space Compaction**: Reclaims space by purging deleted or updated records from active table storage files.
- **JSON Import/Export**: Easily dump database tables to JSON format and restore them dynamically.
- **Hot Backups**: Consistent point-in-time database backups performed while the database remains active and online.
- **SQL Migrator**: Supports one-click, zero-dependency schema, index, and record migration to **PostgreSQL**, **MySQL**, **MariaDB**, **SQLite**, and **Microsoft SQL Server**.

---

## Installation

Initialize your project and import the `masterkeeper` package via its GitHub module path:

```go
import "github.com/lemadane/masterkeeper"
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
    Where(masterkeeper.Equal("Name", "Alice")).
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

## SQL Migration

MasterKeeper includes a Go-native `SQLMigrator` module that allows developers to easily migrate schemas, indices, and records from a MasterKeeper embedded database to a relational SQL database.

It supports:
- **PostgreSQL**
- **MySQL / MariaDB**
- **SQLite**
- **Microsoft SQL Server (MSSQL)**

### Usage

Open a standard `database/sql` connection to your target relational database, choose the corresponding dialect, and call `keeper.Migrate`:

```go
import (
    "database/sql"
    _ "modernc.org/sqlite" // or any other driver
    keeper "github.com/lemadane/masterkeeper"
)

// Open target SQL database
sqlDB, err := sql.Open("sqlite", "./target.db")
if err != nil {
    log.Fatal(err)
}
defer sqlDB.Close()

// Migrate schemas, indices, and all records from MasterKeeper
err = keeper.Migrate(db, sqlDB, keeper.DialectSQLite)
if err != nil {
    log.Fatalf("migration failed: %v", err)
}
```

The migrator will automatically:
1. Map Go datatypes to dialect-specific SQL datatypes (e.g. mapping `time.Time` to `DATETIME`/`TIMESTAMPTZ`, mapping nested structs to `TEXT` JSON).
2. Generate and run `CREATE TABLE IF NOT EXISTS` commands (using conditional check blocks for MSSQL).
3. Generate and run index creation scripts (respecting `unique` and `ordered` constraints).
4. Perform batch insertions of existing records inside SQL transactions.

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
| **Memory Efficiency** | B+ Tree index pages on disk (loaded on demand) | B-tree index pages on disk (loaded on demand) | 🤝 **Tie** (Both scale to datasets far larger than RAM) |
| **Development Speed (Go)** | Type-safe generic API (`Table[ID, T]`) with struct tags | SQL syntax queries, driver setups, and ORM mapping | 🏆 **MasterKeeper** (Zero boilerplate code) |

### Architectural Deep Dive

- **Concurrency & MVCC**: MasterKeeper uses a Copy-on-Write page swapping structure. When a write transaction commits, it swaps the catalog mapping. This means **read queries are 100% lock-free** and always run against an immutable snapshot of the database. Writers never block readers, and readers never block writers. SQLite uses database-level locking; WAL mode allows concurrent readers alongside a single writer, but readers must negotiate shared locks.
- **Persistence & Background Threading**: MasterKeeper offloads all disk writes to a dedicated background goroutine. Write operations (both WAL and table files) are batched together. This delivers high write throughput by ensuring disk sequential writes are done asynchronously without blocking the application transaction thread. SQLite writes directly within the calling application thread.
- **Indices & Scaling**: MasterKeeper keeps all indexes (primary, unique, secondary, and ordered) **directly on disk** inside B+ tree `.idx` files. Pages are loaded on demand and cached in memory using a memory-capped cache. This allows it to scale to datasets far larger than physical RAM, just like SQLite.

---

## Write-Ahead Logging (WAL)

MasterKeeper uses a Write-Ahead Log to guarantee ACID compliance (durability and atomicity).

### How it Works
1. **WAL First**: MasterKeeper serializes the transaction operations into binary WAL records and appends them to `wal.log` on disk.
2. **FSync**: Depending on your durability options (`Sync`, `Batched`, or `None`), the `wal.log` file is synced to disk.
3. **Table Storage**: Only after the WAL is securely written and synced does the background writer write the data to the active table database files (`<table_name>.db`).
4. **Crash Recovery**: If the application crashes, MasterKeeper scans the `wal.log` during startup to replay committed transactions and roll back any uncommitted transactions, restoring the database to a transactionally consistent state.

---

## Transactions Guidelines

To maintain safe concurrency and prevent transaction writer lock starvation or deadlocks, observe the following rules:

- **`Transaction()` is intended for ordinary single-goroutine transactions**: The legacy `Transaction()` method is context-free. By default, it waits indefinitely for lock acquisition. 
- **Applications that want bounded legacy waits must explicitly configure `TransactionWaitTimeout`**: Set `TransactionWaitTimeout` to a positive duration in `Options` to make legacy transactions time out after a specified duration, returning `TransactionWaitTimeoutError` (wrapping `context.DeadlineExceeded`).
- **`TransactionContext()` is required for cancellation, deadlines, and related goroutines**: Use `TransactionContext()` if you need explicit context propagation, cancellation, or custom timeouts.
- **Always propagate the transaction context (`txCtx`)**: When launching child goroutines or starting related transactional units of work from within an active transaction, pass the propagated `txCtx` context:
  ```go
  db.TransactionContext(ctx, func(txCtx context.Context, outer *Transaction) error {
      return db.TransactionContext(txCtx, func(nestedCtx context.Context, inner *Transaction) error {
          return nil
      })
  })
  ```
  This will immediately return `NestedTransactionNotSupportedError` instead of deadlocking.
- **Never start a legacy nested transaction on a spawned goroutine**: Never launch a child transaction via `Transaction()` and block waiting for it inside an active transaction.

---

## Production Readiness & Checklist

MasterKeeper is optimized for embedded use cases requiring strict write-ahead logging (WAL), atomicity, and schema safety. Core concurrency, durability, and recovery guarantees have been rigorously verified under high-concurrency race-enabled stress tests.

Before deploying MasterKeeper to critical production environments, it is recommended to review the following checklist:

1. **Staging Validation**: Always validate database performance, locking patterns, and memory footprints in a staging environment simulating your actual application workload and access patterns.
2. **Monitoring & Telemetry**: Configure alerts on file write operations and database errors, paying special attention to WAL and snapshot flush operations.
3. **Backup Strategy**: Implement a regular database backup routine using the built-in hot backup API (`Backup()`) and periodically verify restore integrity from those backups.
4. **Dependency Locking**: Pin MasterKeeper to a specific commit or release tag in your `go.mod` to ensure you maintain these stability, transaction locking, and recovery fixes.

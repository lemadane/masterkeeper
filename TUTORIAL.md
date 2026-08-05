# MasterKeeper Tutorial

MasterKeeper is a type-safe, lightweight, embedded key-value and document database for Go. It features strict write-ahead logging (WAL) for durability, transactional consistency, indexes, and atomic recovery.

This tutorial guides you through installing MasterKeeper, configuring options, executing transactions safely, performing CRUD and query operations, and managing database maintenance.

---

## 1. Installation

Add MasterKeeper to your Go project:

```bash
go get github.com/lemadane/masterkeeper
```

Ensure your project imports `"github.com/lemadane/masterkeeper"` and `"context"`.

---

## 2. Defining Schemas

MasterKeeper maps Go structs directly to table schemas. The generic parameter of your table requires a comparable ID constraint and a concrete struct type. 

### Struct Tag Rules
- Identify your primary key field using the struct tag ``keeper:"id"``.
- Identify fields that require unique indexes using the struct tag ``keeper:"unique"``.
- **Important**: Do not use pointer types for either the primary key or the main entity type when calling `GetTable`.

```go
package main

import "time"

type User struct {
	ID        string    `keeper:"id"`
	Email     string    `keeper:"unique"`
	Name      string
	Age       int
	CreatedAt time.Time
}
```

---

## 3. Initializing and Opening the Database

To open a database, configure your `Options`, register your schemas, and open the storage directory:

```go
package main

import (
	"log"
	"os"
	"time"

	"github.com/lemadane/masterkeeper"
)

func main() {
	// Configure options
	options := masterkeeper.DefaultOptions()
	
	// Register your entity types
	options.RegisterTypes(User{})
	
	// Optional: Configure legacy lock wait timeout (0 = wait indefinitely)
	options.TransactionWaitTimeout = 5 * time.Second

	// Open the database in the target directory
	database, databaseError := masterkeeper.Open("./data", options)
	if databaseError != nil {
		log.Fatalf("failed to open database: %v", databaseError)
	}
	defer database.Close()
}
```

---

## 4. Retrieving a Table

Retrieve a table using the generic type-safe API. Specify your ID type (must match the ``keeper:"id"`` field's type) and entity type:

```go
table, tableError := masterkeeper.GetTable[string, User](database, "users")
if tableError != nil {
	log.Fatalf("failed to get users table: %v", tableError)
}
```

---

## 5. Working with Transactions

All writes inside MasterKeeper must happen within a transaction callback. MasterKeeper offers two APIs depending on your concurrency model:

### A. Simple Single-Goroutine Transactions (`Transaction`)
Use `Transaction` for simple work units where context propagation across goroutines is not required:

```go
transactionError := database.Transaction(func(transaction *masterkeeper.Transaction) error {
	newUser := User{
		ID:        "usr_100",
		Email:     "alice@example.com",
		Name:      "Alice Smith",
		Age:       30,
		CreatedAt: time.Now(),
	}
	
	// Perform table operations using the active transaction
	return table.Insert(transaction, newUser)
})
if transactionError != nil {
	log.Printf("transaction failed: %v", transactionError)
}
```

### B. Concurrent & Cancellable Transactions (`TransactionContext`)
If you need deadline cancellation, context timeouts, or spin off child goroutines, use `TransactionContext()` and pass the parent context:

```go
parentContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

transactionError := database.TransactionContext(parentContext, func(transactionContext context.Context, transaction *masterkeeper.Transaction) error {
	// If you spin up related transactional tasks, you must pass transactionContext (txCtx)
	// to prevent deadlocks:
	newUser := User{ID: "usr_101", Email: "bob@example.com", Name: "Bob Jones", Age: 25}
	return table.Insert(transaction, newUser)
})
```

### Transaction Safety Rules
1. **Always propagate the transaction context**: Pass the context provided by `TransactionContext` down.
2. **Never nest `Transaction()`**: Doing so will return `NestedTransactionNotSupportedError` immediately if on the same goroutine, or timeout if on separate goroutines.
3. **Keep callbacks focused**: Minimize blocking I/O inside transaction callbacks to prevent lock starvation.

---

## 6. CRUD Operations

CRUD operations are direct and type-safe:

### Insert
Inserts a new record. Fails with `DuplicateIndexError` if the ID or any unique index fields already exist.
```go
insertError := table.Insert(transaction, User{ID: "usr_1", Email: "c@example.com", Name: "Charlie"})
```

### Find by ID
Look up a record. Pass `nil` for the transaction argument to use auto-commit read.
```go
user, found, lookupError := table.FindByID(nil, "usr_1")
```

### Update
Modifies an existing record.
```go
user.Name = "Charlie Brown"
updateError := table.Update(transaction, user)
```

### Delete
Removes a record by its ID.
```go
deleteError := table.Delete(transaction, "usr_1")
```

---

## 7. Querying Data

MasterKeeper supports index-driven queries using comparison operators:

- `masterkeeper.Equal(fieldName, value)`
- `masterkeeper.Greater(fieldName, value)`
- `masterkeeper.GreaterOrEqual(fieldName, value)`
- `masterkeeper.Less(fieldName, value)`
- `masterkeeper.LessOrEqual(fieldName, value)`

### Running a Query
Queries can be run with or without an active transaction:

```go
// Fetch all users aged 25 or older
users, queryError := table.Query(nil).
	Where(masterkeeper.GreaterOrEqual("Age", 25)).
	List()
if queryError != nil {
	log.Fatalf("query failed: %v", queryError)
}

for _, user := range users {
	log.Printf("User: %s, Age: %d", user.Name, user.Age)
}
```

---

## 8. Modeling and CRUD for Relationships

MasterKeeper allows you to implement common database relationships (One-to-One, One-to-Many, and Many-to-Many) by utilizing unique index constraints, foreign keys, and junction tables.

### A. One-to-One Relationships
In a One-to-One relationship (e.g., `User` and `Profile`), you store the reference ID on one of the structs and define a unique constraint on it to prevent duplicates.

```go
type Profile struct {
	ID     string `keeper:"id"`
	UserID string `keeper:"unique"` // Ensures One-to-One relationship
	Bio    string
}
```

#### CRUD Operations:
```go
// 1. Create User and Profile atomically
relationshipError := database.Transaction(func(transaction *masterkeeper.Transaction) error {
	newUser := User{ID: "usr_1", Name: "Alice"}
	newProfile := Profile{ID: "prof_1", UserID: "usr_1", Bio: "Developer"}
	
	if insertError := userTable.Insert(transaction, newUser); insertError != nil {
		return insertError
	}
	return profileTable.Insert(transaction, newProfile)
})

// 2. Read (Query Profile by User ID)
profiles, _ := profileTable.Query(nil).
	Where(masterkeeper.Equal("UserID", "usr_1")).
	List()
if len(profiles) > 0 {
	log.Printf("Alice's Bio: %s", profiles[0].Bio)
}

// 3. Delete Profile and User atomically (Cascade Delete)
relationshipError = database.Transaction(func(transaction *masterkeeper.Transaction) error {
	if deleteError := profileTable.Delete(transaction, "prof_1"); deleteError != nil {
		return deleteError
	}
	return userTable.Delete(transaction, "usr_1")
})
```

---

### B. One-to-Many Relationships
In a One-to-Many relationship (e.g., `User` and `Posts`), the child records reference the parent's ID.

```go
type Post struct {
	ID       string `keeper:"id"`
	AuthorID string // Reference to User.ID (no unique constraint)
	Title    string
}
```

#### CRUD Operations:
```go
// 1. Create a Post
postError := database.Transaction(func(transaction *masterkeeper.Transaction) error {
	newPost := Post{ID: "post_1", AuthorID: "usr_1", Title: "MasterKeeper Guide"}
	return postTable.Insert(transaction, newPost)
})

// 2. Read all Posts for a User
userPosts, _ := postTable.Query(nil).
	Where(masterkeeper.Equal("AuthorID", "usr_1")).
	List()

// 3. Cascade Delete all user Posts and User atomically
cascadeError := database.Transaction(func(transaction *masterkeeper.Transaction) error {
	posts, _ := postTable.Query(transaction).
		Where(masterkeeper.Equal("AuthorID", "usr_1")).
		List()
	for _, post := range posts {
		if deleteError := postTable.Delete(transaction, post.ID); deleteError != nil {
			return deleteError
		}
	}
	return userTable.Delete(transaction, "usr_1")
})
```

---

### C. Many-to-Many Relationships
In a Many-to-Many relationship (e.g., `User` and `Groups`), define a junction table struct (e.g., `UserGroupJoin`) that maps foreign key references.

```go
type Group struct {
	ID   string `keeper:"id"`
	Name string
}

type UserGroupJoin struct {
	ID      string `keeper:"id"` // Typically composite/unique string "userID_groupID"
	UserID  string
	GroupID string
}
```

#### CRUD Operations:
```go
// 1. Associate a User with a Group
associationError := database.Transaction(func(transaction *masterkeeper.Transaction) error {
	association := UserGroupJoin{
		ID:      "usr_1_grp_5",
		UserID:  "usr_1",
		GroupID: "grp_5",
	}
	return joinTable.Insert(transaction, association)
})

// 2. Query all Groups for a User
joins, _ := joinTable.Query(nil).
	Where(masterkeeper.Equal("UserID", "usr_1")).
	List()
for _, join := range joins {
	group, found, _ := groupTable.FindByID(nil, join.GroupID)
	if found {
		log.Printf("User belongs to group: %s", group.Name)
	}
}

// 3. Dissociate a User from a Group
dissociationError := database.Transaction(func(transaction *masterkeeper.Transaction) error {
	return joinTable.Delete(transaction, "usr_1_grp_5")
})
```

---

## 9. Database Administration & Maintenance

### Compaction
Over time, updates and deletes create overhead in the write-ahead log. Use `Compact` to clean up old WAL logs and write a consolidated snapshot of the current state:

```go
if compactionError := database.Compact(); compactionError != nil {
	log.Fatalf("compaction failed: %v", compactionError)
}
```

### Hot Backups
You can create consistent copies of your database directory without stopping the application using `Backup`:

```go
if backupError := database.Backup("./backups/2026-08-04"); backupError != nil {
	log.Fatalf("backup failed: %v", backupError)
}
```

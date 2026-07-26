package masterkeeper_test

import (
	"fmt"
	keeper "github.com/lemadane/masterkeeper"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

type Customer struct {
	ID     string `keeper:"id"`
	Name   string `keeper:"index"`
	Email  string `keeper:"unique"`
	Age    int
	Joined time.Time
}

func TestKeeperCRUD(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	opts := keeper.DefaultOptions()
	opts.RegisterTypes(Customer{})

	db, err := keeper.Open(tempDir, opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	table, err := keeper.GetTable[string, Customer](db, "customers")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	joinedTime := time.Now().Truncate(time.Second)
	c1 := Customer{
		ID:     "cust_1",
		Name:   "Alice",
		Email:  "alice@example.com",
		Age:    30,
		Joined: joinedTime,
	}

	// 1. Autocommit Insert
	err = table.Insert(nil, c1)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// 2. Autocommit Find
	got, found, err := table.FindByID(nil, "cust_1")
	if err != nil {
		t.Fatalf("failed to find: %v", err)
	}
	if !found {
		t.Fatalf("record not found")
	}
	if got.Name != "Alice" || got.Age != 30 || !got.Joined.Equal(joinedTime) {
		t.Errorf("unexpected record retrieved: %+v", got)
	}

	// 3. Autocommit Update
	c1.Age = 31
	err = table.Update(nil, c1)
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	got, found, err = table.FindByID(nil, "cust_1")
	if err != nil {
		t.Fatalf("failed to find after update: %v", err)
	}
	if !found || got.Age != 31 {
		t.Errorf("update was not reflected: found=%v, age=%d", found, got.Age)
	}

	// 4. Transaction with Rollback
	err = db.Transaction(func(tx *keeper.Transaction) error {
		c2 := Customer{
			ID:    "cust_2",
			Name:  "Bob",
			Email: "bob@example.com",
			Age:   25,
		}
		if err := table.Insert(tx, c2); err != nil {
			return err
		}

		// Read your own writes inside transaction
		r, foundInside, err := table.FindByID(tx, "cust_2")
		if err != nil {
			return err
		}
		if !foundInside || r.Name != "Bob" {
			return fmt.Errorf("bob not found inside transaction")
		}

		// Fail to trigger rollback
		return fmt.Errorf("simulated error")
	})

	if err == nil {
		t.Fatalf("expected transaction to fail")
	}

	// Bob should not be found in the database
	_, found, err = table.FindByID(nil, "cust_2")
	if err != nil {
		t.Fatalf("error finding bob: %v", err)
	}
	if found {
		t.Fatalf("bob was found even though transaction rolled back")
	}

	// 5. Transaction with Commit
	err = db.Transaction(func(tx *keeper.Transaction) error {
		c3 := Customer{
			ID:    "cust_3",
			Name:  "Charlie",
			Email: "charlie@example.com",
			Age:   40,
		}
		return table.Insert(tx, c3)
	})

	if err != nil {
		t.Fatalf("transaction commit failed: %v", err)
	}

	got, found, err = table.FindByID(nil, "cust_3")
	if err != nil {
		t.Fatalf("error finding Charlie: %v", err)
	}
	if !found || got.Name != "Charlie" {
		t.Fatalf("charlie was not found after commit")
	}
}

func TestUniqueIndexConstraints(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-test-unique-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	opts := keeper.DefaultOptions()
	opts.RegisterTypes(Customer{})

	db, err := keeper.Open(tempDir, opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	table, err := keeper.GetTable[string, Customer](db, "customers")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	c1 := Customer{
		ID:    "cust_1",
		Name:  "Alice",
		Email: "dup@example.com",
		Age:   30,
	}
	c2 := Customer{
		ID:    "cust_2",
		Name:  "Bob",
		Email: "dup@example.com", // duplicate email
		Age:   25,
	}

	err = table.Insert(nil, c1)
	if err != nil {
		t.Fatalf("failed to insert c1: %v", err)
	}

	// Attempting to insert c2 with duplicate email must fail
	err = table.Insert(nil, c2)
	if err == nil {
		t.Fatalf("expected duplicate unique index violation, but it succeeded")
	}

	// Ensure error is type of ErrDuplicateIndex
	var dupErr *keeper.ErrDuplicateIndex
	if !errorsAs(err, &dupErr) {
		t.Errorf("expected ErrDuplicateIndex, got: %T (%v)", err, err)
	}
}

func TestRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-test-recovery-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	opts := keeper.DefaultOptions()
	opts.RegisterTypes(Customer{})

	// 1. Populate and close db
	db, err := keeper.Open(tempDir, opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	table, err := keeper.GetTable[string, Customer](db, "customers")
	if err != nil {
		db.Close()
		t.Fatalf("failed to get table: %v", err)
	}

	c1 := Customer{
		ID:    "cust_1",
		Name:  "Alice",
		Email: "alice@example.com",
		Age:   30,
	}
	if err := table.Insert(nil, c1); err != nil {
		db.Close()
		t.Fatalf("failed to insert: %v", err)
	}

	db.Close()

	// 2. Reopen and verify data recovered from WAL
	db2, err := keeper.Open(tempDir, opts)
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer db2.Close()

	table2, err := keeper.GetTable[string, Customer](db2, "customers")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	got, found, err := table2.FindByID(nil, "cust_1")
	if err != nil {
		t.Fatalf("failed to find recovered record: %v", err)
	}
	if !found {
		t.Fatalf("recovered record not found")
	}
	if got.Name != "Alice" || got.Age != 30 {
		t.Errorf("incorrect recovered record: %+v", got)
	}
}

func TestCompaction(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-test-compaction-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	opts := keeper.DefaultOptions()
	opts.RegisterTypes(Customer{})

	db, err := keeper.Open(tempDir, opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	table, err := keeper.GetTable[string, Customer](db, "customers")
	if err != nil {
		db.Close()
		t.Fatalf("failed to get table: %v", err)
	}

	c1 := Customer{ID: "c1", Name: "Alice", Email: "alice@example.com"}
	_ = table.Insert(nil, c1)

	// Update c1 multiple times to generate stale records
	for i := 0; i < 5; i++ {
		c1.Age = 20 + i
		_ = table.Update(nil, c1)
	}

	// compact
	if err := db.Compact(); err != nil {
		db.Close()
		t.Fatalf("compaction failed: %v", err)
	}

	db.Close()

	// Reopen and check
	db2, err := keeper.Open(tempDir, opts)
	if err != nil {
		t.Fatalf("failed to reopen after compaction: %v", err)
	}
	defer db2.Close()

	table2, err := keeper.GetTable[string, Customer](db2, "customers")
	if err != nil {
		t.Fatalf("failed to get table after compaction: %v", err)
	}

	got, found, err := table2.FindByID(nil, "c1")
	if err != nil {
		t.Fatalf("failed to find after compaction: %v", err)
	}
	if !found || got.Age != 24 {
		t.Fatalf("record not found or incorrect state: found=%v, age=%d", found, got.Age)
	}
}

func TestQuery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-test-query-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	opts := keeper.DefaultOptions()
	opts.RegisterTypes(Customer{})

	db, err := keeper.Open(tempDir, opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	table, err := keeper.GetTable[string, Customer](db, "customers")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	_ = table.Insert(nil, Customer{ID: "c1", Name: "Alice", Email: "alice@example.com", Age: 30})
	_ = table.Insert(nil, Customer{ID: "c2", Name: "Bob", Email: "bob@example.com", Age: 25})
	_ = table.Insert(nil, Customer{ID: "c3", Name: "Charlie", Email: "charlie@example.com", Age: 35})

	// 1. Where Age >= 30, Sort by Age Desc
	q := table.Query(nil).
		Where(keeper.Ge("Age", 30)).
		OrderBy(keeper.Desc("Age"))

	res, err := q.List()
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 records, got %d", len(res))
	}
	if res[0].ID != "c3" || res[1].ID != "c1" {
		t.Errorf("incorrect sort or filter: %+v", res)
	}

	// Explain
	plan := q.Explain()
	if plan.Strategy == "" {
		t.Errorf("explain strategy is empty")
	}

	// 2. Limit and Offset
	res2, err := table.Query(nil).
		OrderBy(keeper.Asc("Age")).
		Offset(1).
		Limit(1).
		List()

	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(res2) != 1 || res2[0].ID != "c1" {
		t.Errorf("limit/offset failed, got: %+v", res2)
	}
}

func TestJSONExportImport(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-test-json-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	opts := keeper.DefaultOptions()
	opts.RegisterTypes(Customer{})

	db, err := keeper.Open(tempDir, opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	table, err := keeper.GetTable[string, Customer](db, "customers")
	if err != nil {
		db.Close()
		t.Fatalf("failed to get table: %v", err)
	}

	_ = table.Insert(nil, Customer{ID: "c1", Name: "Alice", Email: "alice@example.com", Age: 30})
	_ = table.Insert(nil, Customer{ID: "c2", Name: "Bob", Email: "bob@example.com", Age: 25})

	jsonPath := filepath.Join(tempDir, "export.json")
	if err := db.ExportJSON(jsonPath); err != nil {
		db.Close()
		t.Fatalf("failed to export JSON: %v", err)
	}

	db.Close()

	// Reopen a fresh database and import JSON
	tempDir2, _ := os.MkdirTemp("", "keeper-test-json-import-*")
	defer os.RemoveAll(tempDir2)

	db2, err := keeper.Open(tempDir2, opts)
	if err != nil {
		t.Fatalf("failed to open db2: %v", err)
	}
	defer db2.Close()

	// Get table registers metadata
	table2, _ := keeper.GetTable[string, Customer](db2, "customers")

	if err := db2.ImportJSON(jsonPath); err != nil {
		t.Fatalf("failed to import JSON: %v", err)
	}

	// Verify imported data
	got, found, _ := table2.FindByID(nil, "c1")
	if !found || got.Name != "Alice" {
		t.Fatalf("imported record c1 not found or invalid: %+v", got)
	}
}

func TestAsyncAndBatchedDurability(t *testing.T) {
	for _, mode := range []keeper.DurabilityMode{keeper.DurabilityAsync, keeper.DurabilityBatched} {
		t.Run(fmt.Sprintf("Mode_%d", mode), func(t *testing.T) {
			tempDir, err := os.MkdirTemp("", "keeper-test-durability-*")
			if err != nil {
				t.Fatalf("failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tempDir)

			opts := keeper.DefaultOptions()
			opts.Durability = mode
			opts.RegisterTypes(Customer{})

			db, err := keeper.Open(tempDir, opts)
			if err != nil {
				t.Fatalf("failed to open database: %v", err)
			}

			table, err := keeper.GetTable[string, Customer](db, "customers")
			if err != nil {
				db.Close()
				t.Fatalf("failed to get table: %v", err)
			}

			// Concurrent inserts
			const numGoroutines = 10
			var wg syncWaitGroup
			wg.Add(numGoroutines)
			for i := 0; i < numGoroutines; i++ {
				go func(idx int) {
					defer wg.Done()
					c := Customer{
						ID:    fmt.Sprintf("c_%d", idx),
						Name:  fmt.Sprintf("User_%d", idx),
						Email: fmt.Sprintf("user_%d@example.com", idx),
						Age:   20 + idx,
					}
					_ = table.Insert(nil, c)
				}(i)
			}
			wg.Wait()

			// Check all records exist
			for i := 0; i < numGoroutines; i++ {
				_, found, err := table.FindByID(nil, fmt.Sprintf("c_%d", i))
				if err != nil || !found {
					db.Close()
					t.Fatalf("expected record c_%d to be found (err=%v)", i, err)
				}
			}

			db.Close()
		})
	}
}

// Helpers for testing

type syncWaitGroup struct {
	wg sync.WaitGroup
}

func (s *syncWaitGroup) Add(delta int) { s.wg.Add(delta) }
func (s *syncWaitGroup) Done()         { s.wg.Done() }
func (s *syncWaitGroup) Wait()         { s.wg.Wait() }

// Custom implementation of errors.As to avoid standard errors package mismatch
func errorsAs(err error, target any) bool {
	if err == nil {
		return false
	}
	val := reflect.ValueOf(target)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		panic("errorsAs target must be a non-nil pointer")
	}
	targetType := val.Type().Elem()
	if targetType.Kind() != reflect.Ptr && targetType.Kind() != reflect.Interface {
		panic("errorsAs target must be a pointer to an interface or a pointer to a struct implementing error")
	}
	
	// Direct type assertion check
	errVal := reflect.ValueOf(err)
	if errVal.Type().AssignableTo(targetType) {
		val.Elem().Set(errVal)
		return true
	}
	
	return false
}

func TestHotBackup(t *testing.T) {
	dir, err := os.MkdirTemp("", "keeper-backup-src-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := keeper.DefaultOptions()
	opts.RegisterTypes(Customer{})
	db, err := keeper.Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	table, err := keeper.GetTable[string, Customer](db, "customers")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	// Insert some records
	err = db.Transaction(func(tx *keeper.Transaction) error {
		c1 := Customer{ID: "c1", Name: "Alice", Email: "alice@example.com", Age: 30}
		c2 := Customer{ID: "c2", Name: "Bob", Email: "bob@example.com", Age: 35}
		if err := table.Insert(tx, c1); err != nil {
			return err
		}
		if err := table.Insert(tx, c2); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed transaction: %v", err)
	}

	// Create backup dir
	backupDir, err := os.MkdirTemp("", "keeper-backup-dest-*")
	if err != nil {
		t.Fatalf("failed to create backup temp dir: %v", err)
	}
	defer os.RemoveAll(backupDir)

	// Perform backup
	if err := db.Backup(backupDir); err != nil {
		t.Fatalf("failed to backup database: %v", err)
	}

	// Open the backed up database to verify consistency
	backupDB, err := keeper.Open(backupDir, opts)
	if err != nil {
		t.Fatalf("failed to open backed up database: %v", err)
	}
	defer backupDB.Close()

	backupTable, err := keeper.GetTable[string, Customer](backupDB, "customers")
	if err != nil {
		t.Fatalf("failed to get table in backup DB: %v", err)
	}

	// Verify records are present
	c1Back, found, err := backupTable.FindByID(nil, "c1")
	if err != nil {
		t.Fatalf("failed query on backup table: %v", err)
	}
	if !found {
		t.Fatalf("customer c1 not found in backup table")
	}
	if c1Back.Name != "Alice" || c1Back.Age != 30 {
		t.Errorf("incorrect data in backup customer c1: %+v", c1Back)
	}

	c2Back, found, err := backupTable.FindByID(nil, "c2")
	if err != nil {
		t.Fatalf("failed query on backup table: %v", err)
	}
	if !found {
		t.Fatalf("customer c2 not found in backup table")
	}
	if c2Back.Name != "Bob" || c2Back.Age != 35 {
		t.Errorf("incorrect data in backup customer c2: %+v", c2Back)
	}
}

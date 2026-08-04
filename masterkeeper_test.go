package masterkeeper_test

import (
	"fmt"
	keeper "github.com/lemadane/masterkeeper"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

func TestKeeperCRUD(testingT *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-test-*")
	if err != nil {
		testingT.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	database, err := keeper.Open(tempDir, options)
	if err != nil {
		testingT.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	customerTable, err := keeper.GetTable[string, Customer](database, "customers")
	if err != nil {
		testingT.Fatalf("failed to get table: %v", err)
	}

	joinedTime := time.Now().Truncate(time.Second)
	customer1 := Customer{
		ID:     "cust_1",
		Name:   "Alice",
		Email:  "alice@example.com",
		Age:    30,
		Joined: joinedTime,
	}

	// 1. Autocommit Insert
	err = customerTable.Insert(nil, customer1)
	if err != nil {
		testingT.Fatalf("failed to insert: %v", err)
	}

	// 2. Autocommit Find
	gotRecord, found, err := customerTable.FindByID(nil, "cust_1")
	if err != nil {
		testingT.Fatalf("failed to find: %v", err)
	}
	if !found {
		testingT.Fatalf("record not found")
	}
	if gotRecord.Name != "Alice" || gotRecord.Age != 30 || !gotRecord.Joined.Equal(joinedTime) {
		testingT.Errorf("unexpected record retrieved: %+v", gotRecord)
	}

	// 3. Autocommit Update
	customer1.Age = 31
	err = customerTable.Update(nil, customer1)
	if err != nil {
		testingT.Fatalf("failed to update: %v", err)
	}

	gotRecord, found, err = customerTable.FindByID(nil, "cust_1")
	if err != nil {
		testingT.Fatalf("failed to find after update: %v", err)
	}
	if !found || gotRecord.Age != 31 {
		testingT.Errorf("update was not reflected: found=%v, age=%d", found, gotRecord.Age)
	}

	// 4. Transaction with Rollback
	err = database.Transaction(func(transaction *keeper.Transaction) error {
		customer2 := Customer{
			ID:    "cust_2",
			Name:  "Bob",
			Email: "bob@example.com",
			Age:   25,
		}
		if err := customerTable.Insert(transaction, customer2); err != nil {
			return err
		}

		// Read your own writes inside transaction
		record, foundInside, err := customerTable.FindByID(transaction, "cust_2")
		if err != nil {
			return err
		}
		if !foundInside || record.Name != "Bob" {
			return fmt.Errorf("bob not found inside transaction")
		}

		// Fail to trigger rollback
		return fmt.Errorf("simulated error")
	})

	if err == nil {
		testingT.Fatalf("expected transaction to fail")
	}

	// Bob should not be found in the database
	_, found, err = customerTable.FindByID(nil, "cust_2")
	if err != nil {
		testingT.Fatalf("error finding bob: %v", err)
	}
	if found {
		testingT.Fatalf("bob was found even though transaction rolled back")
	}

	// 5. Transaction with Commit
	err = database.Transaction(func(transaction *keeper.Transaction) error {
		customer3 := Customer{
			ID:    "cust_3",
			Name:  "Charlie",
			Email: "charlie@example.com",
			Age:   40,
		}
		return customerTable.Insert(transaction, customer3)
	})

	if err != nil {
		testingT.Fatalf("transaction commit failed: %v", err)
	}

	gotRecord, found, err = customerTable.FindByID(nil, "cust_3")
	if err != nil {
		testingT.Fatalf("error finding Charlie: %v", err)
	}
	if !found || gotRecord.Name != "Charlie" {
		testingT.Fatalf("charlie was not found after commit")
	}
}

func TestUniqueIndexConstraints(testingT *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-test-unique-*")
	if err != nil {
		testingT.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	database, err := keeper.Open(tempDir, options)
	if err != nil {
		testingT.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	customerTable, err := keeper.GetTable[string, Customer](database, "customers")
	if err != nil {
		testingT.Fatalf("failed to get table: %v", err)
	}

	customer1 := Customer{
		ID:    "cust_1",
		Name:  "Alice",
		Email: "dup@example.com",
		Age:   30,
	}
	customer2 := Customer{
		ID:    "cust_2",
		Name:  "Bob",
		Email: "dup@example.com", // duplicate email
		Age:   25,
	}

	err = customerTable.Insert(nil, customer1)
	if err != nil {
		testingT.Fatalf("failed to insert customer1: %v", err)
	}

	// Attempting to insert customer2 with duplicate email must fail
	err = customerTable.Insert(nil, customer2)
	if err == nil {
		testingT.Fatalf("expected duplicate unique index violation, but it succeeded")
	}

	// Ensure error is type of ErrDuplicateIndex
	var dupErr *keeper.ErrDuplicateIndex
	if !errorsAs(err, &dupErr) {
		testingT.Errorf("expected ErrDuplicateIndex, got: %T (%v)", err, err)
	}
}

func TestRecovery(testingT *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-test-recovery-*")
	if err != nil {
		testingT.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	// 1. Populate and close database
	database, err := keeper.Open(tempDir, options)
	if err != nil {
		testingT.Fatalf("failed to open database: %v", err)
	}

	customerTable, err := keeper.GetTable[string, Customer](database, "customers")
	if err != nil {
		database.Close()
		testingT.Fatalf("failed to get table: %v", err)
	}

	customer1 := Customer{
		ID:    "cust_1",
		Name:  "Alice",
		Email: "alice@example.com",
		Age:   30,
	}
	if err := customerTable.Insert(nil, customer1); err != nil {
		database.Close()
		testingT.Fatalf("failed to insert: %v", err)
	}

	database.Close()

	// 2. Reopen and verify data recovered from WAL
	database2, err := keeper.Open(tempDir, options)
	if err != nil {
		testingT.Fatalf("failed to reopen database: %v", err)
	}
	defer database2.Close()

	customerTable2, err := keeper.GetTable[string, Customer](database2, "customers")
	if err != nil {
		testingT.Fatalf("failed to get table: %v", err)
	}

	gotRecord, found, err := customerTable2.FindByID(nil, "cust_1")
	if err != nil {
		testingT.Fatalf("failed to find recovered record: %v", err)
	}
	if !found {
		testingT.Fatalf("recovered record not found")
	}
	if gotRecord.Name != "Alice" || gotRecord.Age != 30 {
		testingT.Errorf("incorrect recovered record: %+v", gotRecord)
	}
}

func TestCompaction(testingT *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-test-compaction-*")
	if err != nil {
		testingT.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	database, err := keeper.Open(tempDir, options)
	if err != nil {
		testingT.Fatalf("failed to open database: %v", err)
	}

	customerTable, err := keeper.GetTable[string, Customer](database, "customers")
	if err != nil {
		database.Close()
		testingT.Fatalf("failed to get table: %v", err)
	}

	customer1 := Customer{ID: "c1", Name: "Alice", Email: "alice@example.com"}
	_ = customerTable.Insert(nil, customer1)

	// Update customer1 multiple times to generate stale records
	for index := 0; index < 5; index++ {
		customer1.Age = 20 + index
		_ = customerTable.Update(nil, customer1)
	}

	// compact
	if err := database.Compact(); err != nil {
		database.Close()
		testingT.Fatalf("compaction failed: %v", err)
	}

	database.Close()

	// Reopen and check
	database2, err := keeper.Open(tempDir, options)
	if err != nil {
		testingT.Fatalf("failed to reopen after compaction: %v", err)
	}
	defer database2.Close()

	customerTable2, err := keeper.GetTable[string, Customer](database2, "customers")
	if err != nil {
		testingT.Fatalf("failed to get table after compaction: %v", err)
	}

	gotRecord, found, err := customerTable2.FindByID(nil, "c1")
	if err != nil {
		testingT.Fatalf("failed to find after compaction: %v", err)
	}
	if !found || gotRecord.Age != 24 {
		testingT.Fatalf("record not found or incorrect state: found=%v, age=%d", found, gotRecord.Age)
	}
}

func TestQuery(testingT *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-test-query-*")
	if err != nil {
		testingT.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	database, err := keeper.Open(tempDir, options)
	if err != nil {
		testingT.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	customerTable, err := keeper.GetTable[string, Customer](database, "customers")
	if err != nil {
		testingT.Fatalf("failed to get table: %v", err)
	}

	_ = customerTable.Insert(nil, Customer{ID: "c1", Name: "Alice", Email: "alice@example.com", Age: 30})
	_ = customerTable.Insert(nil, Customer{ID: "c2", Name: "Bob", Email: "bob@example.com", Age: 25})
	_ = customerTable.Insert(nil, Customer{ID: "c3", Name: "Charlie", Email: "charlie@example.com", Age: 35})

	// 1. Where Age >= 30, Sort by Age Desc
	queryVal := customerTable.Query(nil).
		Where(keeper.Ge("Age", 30)).
		OrderBy(keeper.Desc("Age"))

	results, err := queryVal.List()
	if err != nil {
		testingT.Fatalf("query failed: %v", err)
	}
	if len(results) != 2 {
		testingT.Fatalf("expected 2 records, got %d", len(results))
	}
	if results[0].ID != "c3" || results[1].ID != "c1" {
		testingT.Errorf("incorrect sort or filter: %+v", results)
	}

	// Explain
	plan := queryVal.Explain()
	if plan.Strategy == "" {
		testingT.Errorf("explain strategy is empty")
	}

	// 2. Limit and Offset
	results2, err := customerTable.Query(nil).
		OrderBy(keeper.Asc("Age")).
		Offset(1).
		Limit(1).
		List()

	if err != nil {
		testingT.Fatalf("query failed: %v", err)
	}
	if len(results2) != 1 || results2[0].ID != "c1" {
		testingT.Errorf("limit/offset failed, got: %+v", results2)
	}
}

func TestJSONExportImport(testingT *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-test-json-*")
	if err != nil {
		testingT.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	database, err := keeper.Open(tempDir, options)
	if err != nil {
		testingT.Fatalf("failed to open database: %v", err)
	}

	customerTable, err := keeper.GetTable[string, Customer](database, "customers")
	if err != nil {
		database.Close()
		testingT.Fatalf("failed to get table: %v", err)
	}

	_ = customerTable.Insert(nil, Customer{ID: "c1", Name: "Alice", Email: "alice@example.com", Age: 30})
	_ = customerTable.Insert(nil, Customer{ID: "c2", Name: "Bob", Email: "bob@example.com", Age: 25})

	jsonPath := filepath.Join(tempDir, "export.json")
	if err := database.ExportJSON(jsonPath); err != nil {
		database.Close()
		testingT.Fatalf("failed to export JSON: %v", err)
	}

	database.Close()

	// Reopen a fresh database and import JSON
	tempDir2, _ := os.MkdirTemp("", "keeper-test-json-import-*")
	defer os.RemoveAll(tempDir2)

	database2, err := keeper.Open(tempDir2, options)
	if err != nil {
		testingT.Fatalf("failed to open database2: %v", err)
	}
	defer database2.Close()

	// Get table registers metadata
	customerTable2, _ := keeper.GetTable[string, Customer](database2, "customers")

	if err := database2.ImportJSON(jsonPath); err != nil {
		testingT.Fatalf("failed to import JSON: %v", err)
	}

	// Verify imported data
	gotRecord, found, _ := customerTable2.FindByID(nil, "c1")
	if !found || gotRecord.Name != "Alice" {
		testingT.Fatalf("imported record c1 not found or invalid: %+v", gotRecord)
	}
}

func TestAsyncAndBatchedDurability(testingT *testing.T) {
	for _, mode := range []keeper.DurabilityMode{keeper.DurabilityAsync, keeper.DurabilityBatched} {
		testingT.Run(fmt.Sprintf("Mode_%d", mode), func(t *testing.T) {
			tempDir, err := os.MkdirTemp("", "keeper-test-durability-*")
			if err != nil {
				testingT.Fatalf("failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tempDir)

			options := keeper.DefaultOptions()
			options.Durability = mode
			options.RegisterTypes(Customer{})

			database, err := keeper.Open(tempDir, options)
			if err != nil {
				testingT.Fatalf("failed to open database: %v", err)
			}

			customerTable, err := keeper.GetTable[string, Customer](database, "customers")
			if err != nil {
				database.Close()
				testingT.Fatalf("failed to get table: %v", err)
			}

			// Concurrent inserts
			const numGoroutines = 10
			var syncWaitGroup syncWaitGroup
			syncWaitGroup.Add(numGoroutines)
			for index := 0; index < numGoroutines; index++ {
				go func(idx int) {
					defer syncWaitGroup.Done()
					c := Customer{
						ID:    fmt.Sprintf("c_%d", idx),
						Name:  fmt.Sprintf("User_%d", idx),
						Email: fmt.Sprintf("user_%d@example.com", idx),
						Age:   20 + idx,
					}
					_ = customerTable.Insert(nil, c)
				}(index)
			}
			syncWaitGroup.Wait()

			// Check all records exist
			for index := 0; index < numGoroutines; index++ {
				_, found, err := customerTable.FindByID(nil, fmt.Sprintf("c_%d", index))
				if err != nil || !found {
					database.Close()
					testingT.Fatalf("expected record c_%d to be found (err=%v)", index, err)
				}
			}

			database.Close()
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
	reflectValue := reflect.ValueOf(target)
	if reflectValue.Kind() != reflect.Ptr || reflectValue.IsNil() {
		panic("errorsAs target must be a non-nil pointer")
	}
	targetType := reflectValue.Type().Elem()
	if targetType.Kind() != reflect.Ptr && targetType.Kind() != reflect.Interface {
		panic("errorsAs target must be a pointer to an interface or a pointer to a struct implementing error")
	}
	
	// Direct type assertion check
	errVal := reflect.ValueOf(err)
	if errVal.Type().AssignableTo(targetType) {
		reflectValue.Elem().Set(errVal)
		return true
	}
	
	return false
}

func TestHotBackup(testingT *testing.T) {
	dir, err := os.MkdirTemp("", "keeper-backup-src-*")
	if err != nil {
		testingT.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})
	database, err := keeper.Open(dir, options)
	if err != nil {
		testingT.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	customerTable, err := keeper.GetTable[string, Customer](database, "customers")
	if err != nil {
		testingT.Fatalf("failed to get table: %v", err)
	}

	// Insert some records
	err = database.Transaction(func(transaction *keeper.Transaction) error {
		customer1 := Customer{ID: "c1", Name: "Alice", Email: "alice@example.com", Age: 30}
		customer2 := Customer{ID: "c2", Name: "Bob", Email: "bob@example.com", Age: 35}
		if err := customerTable.Insert(transaction, customer1); err != nil {
			return err
		}
		if err := customerTable.Insert(transaction, customer2); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		testingT.Fatalf("failed transaction: %v", err)
	}

	// Create backup dir
	backupDir, err := os.MkdirTemp("", "keeper-backup-dest-*")
	if err != nil {
		testingT.Fatalf("failed to create backup temp dir: %v", err)
	}
	defer os.RemoveAll(backupDir)

	// Perform backup
	if err := database.Backup(backupDir); err != nil {
		testingT.Fatalf("failed to backup database: %v", err)
	}

	// Open the backed up database to verify consistency
	backupDatabase, err := keeper.Open(backupDir, options)
	if err != nil {
		testingT.Fatalf("failed to open backed up database: %v", err)
	}
	defer backupDatabase.Close()

	backupTable, err := keeper.GetTable[string, Customer](backupDatabase, "customers")
	if err != nil {
		testingT.Fatalf("failed to get table in backup database: %v", err)
	}

	// Verify records are present
	customer1Back, found, err := backupTable.FindByID(nil, "c1")
	if err != nil {
		testingT.Fatalf("failed query on backup table: %v", err)
	}
	if !found {
		testingT.Fatalf("customer c1 not found in backup table")
	}
	if customer1Back.Name != "Alice" || customer1Back.Age != 30 {
		testingT.Errorf("incorrect data in backup customer c1: %+v", customer1Back)
	}

	customer2Back, found, err := backupTable.FindByID(nil, "c2")
	if err != nil {
		testingT.Fatalf("failed query on backup table: %v", err)
	}
	if !found {
		testingT.Fatalf("customer c2 not found in backup table")
	}
	if customer2Back.Name != "Bob" || customer2Back.Age != 35 {
		testingT.Errorf("incorrect data in backup customer c2: %+v", customer2Back)
	}
}

type BigIntIDRecord struct {
	ID   int `keeper:"id"`
	Name string
}

func TestInt64SerializationRegression(testingT *testing.T) {
	testValues := []int{
		math.MaxInt,
		math.MinInt,
		int(1 << 40),
		int(-(1 << 40)),
		math.MaxInt32,
		math.MinInt32,
	}

	for _, val := range testValues {
		testingT.Run(fmt.Sprintf("val_%d", val), func(t *testing.T) {
			tempDir, err := os.MkdirTemp("", "keeper-test-int64-*")
			if err != nil {
				testingT.Fatalf("failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tempDir)

			options := keeper.DefaultOptions()
			options.RegisterTypes(BigIntIDRecord{})

			// 1. Open Database
			database, err := keeper.Open(tempDir, options)
			if err != nil {
				testingT.Fatalf("failed to open database: %v", err)
			}

			table, err := keeper.GetTable[int, BigIntIDRecord](database, "big_ints")
			if err != nil {
				database.Close()
				testingT.Fatalf("failed to get table: %v", err)
			}

			// 2. Insert record
			record := BigIntIDRecord{
				ID:   val,
				Name: "TestRecord",
			}
			err = table.Insert(nil, record)
			if err != nil {
				database.Close()
				testingT.Fatalf("failed to insert: %v", err)
			}

			// 3. Close database
			database.Close()

			// 4. Reopen database
			database2, err := keeper.Open(tempDir, options)
			if err != nil {
				testingT.Fatalf("failed to reopen database: %v", err)
			}
			defer database2.Close()

			table2, err := keeper.GetTable[int, BigIntIDRecord](database2, "big_ints")
			if err != nil {
				testingT.Fatalf("failed to get table: %v", err)
			}

			// 5. Find by original ID
			gotRecord, found, err := table2.FindByID(nil, val)
			if err != nil {
				testingT.Fatalf("failed to find record: %v", err)
			}
			if !found {
				testingT.Fatalf("record not found")
			}

			// Verify decoded ID is unchanged
			if gotRecord.ID != val {
				testingT.Errorf("expected ID %d, got %d", val, gotRecord.ID)
			}

			// 6. Test update and deletion using that ID
			gotRecord.Name = "UpdatedRecord"
			err = table2.Update(nil, gotRecord)
			if err != nil {
				testingT.Fatalf("failed to update record: %v", err)
			}

			gotUpdated, foundUpdated, err := table2.FindByID(nil, val)
			if err != nil {
				testingT.Fatalf("failed to find updated record: %v", err)
			}
			if !foundUpdated || gotUpdated.Name != "UpdatedRecord" {
				testingT.Fatalf("update not reflected or record lost: found=%v, name=%s", foundUpdated, gotUpdated.Name)
			}

			deleted, err := table2.DeleteByID(nil, val)
			if err != nil {
				testingT.Fatalf("failed to delete record: %v", err)
			}
			if !deleted {
				testingT.Fatalf("expected delete to return true")
			}

			_, foundDeleted, err := table2.FindByID(nil, val)
			if err != nil {
				testingT.Fatalf("failed to find deleted record: %v", err)
			}
			if foundDeleted {
				testingT.Fatalf("record still exists after deletion")
			}

			// 7. Confirm overflow into int8, int16, or int32 returns an error
			type LargeValStruct struct {
				Val int
			}
			data, err := keeper.Marshal(LargeValStruct{Val: val})
			if err != nil {
				testingT.Fatalf("failed to marshal large val struct: %v", err)
			}

			// Check int8 overflow
			if val < math.MinInt8 || val > math.MaxInt8 {
				var t8 struct{ Val int8 }
				err8 := keeper.Unmarshal(data, &t8)
				if err8 == nil {
					testingT.Errorf("expected overflow error casting %d to int8, but got nil", val)
				} else if !strings.Contains(err8.Error(), "overflows") {
					testingT.Errorf("expected error message to contain 'overflows', got: %v", err8)
				}
			}

			// Check int16 overflow
			if val < math.MinInt16 || val > math.MaxInt16 {
				var t16 struct{ Val int16 }
				err16 := keeper.Unmarshal(data, &t16)
				if err16 == nil {
					testingT.Errorf("expected overflow error casting %d to int16, but got nil", val)
				} else if !strings.Contains(err16.Error(), "overflows") {
					testingT.Errorf("expected error message to contain 'overflows', got: %v", err16)
				}
			}

			// Check int32 overflow
			if val < math.MinInt32 || val > math.MaxInt32 {
				var t32 struct{ Val int32 }
				err32 := keeper.Unmarshal(data, &t32)
				if err32 == nil {
					testingT.Errorf("expected overflow error casting %d to int32, but got nil", val)
				} else if !strings.Contains(err32.Error(), "overflows") {
					testingT.Errorf("expected error message to contain 'overflows', got: %v", err32)
				}
			}
		})
	}
}

type UserID int

type UserRecord struct {
	ID   UserID `keeper:"id"`
	Name string
}

func TestInt64E2E(testingT *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-test-e2e-*")
	if err != nil {
		testingT.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := keeper.DefaultOptions()
	options.RegisterTypes(UserRecord{})

	database, err := keeper.Open(tempDir, options)
	if err != nil {
		testingT.Fatalf("failed to open database: %v", err)
	}

	table, err := keeper.GetTable[UserID, UserRecord](database, "users")
	if err != nil {
		database.Close()
		testingT.Fatalf("failed to get table: %v", err)
	}

	// Test named type values
	val := UserID(1 << 40)
	err = table.Insert(nil, UserRecord{ID: val, Name: "Alice"})
	if err != nil {
		database.Close()
		testingT.Fatalf("failed to insert: %v", err)
	}

	database.Close()

	// Reopen
	database2, err := keeper.Open(tempDir, options)
	if err != nil {
		testingT.Fatalf("failed to reopen database: %v", err)
	}
	defer database2.Close()

	table2, err := keeper.GetTable[UserID, UserRecord](database2, "users")
	if err != nil {
		testingT.Fatalf("failed to get table: %v", err)
	}

	gotRecord, found, err := table2.FindByID(nil, val)
	if err != nil {
		testingT.Fatalf("failed to find: %v", err)
	}
	if !found {
		testingT.Fatalf("not found")
	}
	if gotRecord.ID != val || gotRecord.Name != "Alice" {
		testingT.Errorf("unexpected record: %+v", gotRecord)
	}

	// Update
	gotRecord.Name = "Bob"
	err = table2.Update(nil, gotRecord)
	if err != nil {
		testingT.Fatalf("failed to update: %v", err)
	}

	gotRecord2, found2, _ := table2.FindByID(nil, val)
	if !found2 || gotRecord2.Name != "Bob" {
		testingT.Errorf("update failed")
	}

	// Delete
	deleted, err := table2.DeleteByID(nil, val)
	if err != nil || !deleted {
		testingT.Fatalf("failed to delete")
	}

	_, found3, _ := table2.FindByID(nil, val)
	if found3 {
		testingT.Errorf("record still exists after delete")
	}
}

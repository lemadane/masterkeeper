package masterkeeper_test

import (
	"errors"
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

func TestKeeperCRUD(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	customerTable, testError := keeper.GetTable[string, Customer](database, "customers")
	if testError != nil {
		test.Fatalf("failed to get table: %v", testError)
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
	testError = customerTable.Insert(nil, customer1)
	if testError != nil {
		test.Fatalf("failed to insert: %v", testError)
	}

	// 2. Autocommit Find
	gotRecord, found, testError := customerTable.FindByID(nil, "cust_1")
	if testError != nil {
		test.Fatalf("failed to find: %v", testError)
	}
	if !found {
		test.Fatalf("record not found")
	}
	if gotRecord.Name != "Alice" || gotRecord.Age != 30 || !gotRecord.Joined.Equal(joinedTime) {
		test.Errorf("unexpected record retrieved: %+v", gotRecord)
	}

	// 3. Autocommit Update
	customer1.Age = 31
	testError = customerTable.Update(nil, customer1)
	if testError != nil {
		test.Fatalf("failed to update: %v", testError)
	}

	gotRecord, found, testError = customerTable.FindByID(nil, "cust_1")
	if testError != nil {
		test.Fatalf("failed to find after update: %v", testError)
	}
	if !found || gotRecord.Age != 31 {
		test.Errorf("update was not reflected: found=%v, age=%d", found, gotRecord.Age)
	}

	// 4. Transaction with Rollback
	testError = database.Transaction(func(transaction *keeper.Transaction) error {
		customer2 := Customer{
			ID:    "cust_2",
			Name:  "Bob",
			Email: "bob@example.com",
			Age:   25,
		}
		if testError := customerTable.Insert(transaction, customer2); testError != nil {
			return testError
		}

		// Read your own writes inside transaction
		record, foundInside, testError := customerTable.FindByID(transaction, "cust_2")
		if testError != nil {
			return testError
		}
		if !foundInside || record.Name != "Bob" {
			return fmt.Errorf("bob not found inside transaction")
		}

		// Fail to trigger rollback
		return fmt.Errorf("simulated testError")
	})

	if testError == nil {
		test.Fatalf("expected transaction to fail")
	}

	// Bob should not be found in the database
	_, found, testError = customerTable.FindByID(nil, "cust_2")
	if testError != nil {
		test.Fatalf("testError finding bob: %v", testError)
	}
	if found {
		test.Fatalf("bob was found even though transaction rolled back")
	}

	// 5. Transaction with Commit
	testError = database.Transaction(func(transaction *keeper.Transaction) error {
		customer3 := Customer{
			ID:    "cust_3",
			Name:  "Charlie",
			Email: "charlie@example.com",
			Age:   40,
		}
		return customerTable.Insert(transaction, customer3)
	})

	if testError != nil {
		test.Fatalf("transaction commit failed: %v", testError)
	}

	gotRecord, found, testError = customerTable.FindByID(nil, "cust_3")
	if testError != nil {
		test.Fatalf("testError finding Charlie: %v", testError)
	}
	if !found || gotRecord.Name != "Charlie" {
		test.Fatalf("charlie was not found after commit")
	}
}

func TestUniqueIndexConstraints(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-unique-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	customerTable, testError := keeper.GetTable[string, Customer](database, "customers")
	if testError != nil {
		test.Fatalf("failed to get table: %v", testError)
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

	testError = customerTable.Insert(nil, customer1)
	if testError != nil {
		test.Fatalf("failed to insert customer1: %v", testError)
	}

	// Attempting to insert customer2 with duplicate email must fail
	testError = customerTable.Insert(nil, customer2)
	if testError == nil {
		test.Fatalf("expected duplicate unique index violation, but it succeeded")
	}

	// Ensure testError is type of DuplicateIndexError
	var dupErr *keeper.DuplicateIndexError
	if !errorsAs(testError, &dupErr) {
		test.Errorf("expected DuplicateIndexError, got: %T (%v)", testError, testError)
	}
}

func TestRecovery(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-recovery-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	// 1. Populate and close database
	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}

	customerTable, testError := keeper.GetTable[string, Customer](database, "customers")
	if testError != nil {
		database.Close()
		test.Fatalf("failed to get table: %v", testError)
	}

	customer1 := Customer{
		ID:    "cust_1",
		Name:  "Alice",
		Email: "alice@example.com",
		Age:   30,
	}
	if testError := customerTable.Insert(nil, customer1); testError != nil {
		database.Close()
		test.Fatalf("failed to insert: %v", testError)
	}

	database.Close()

	// 2. Reopen and verify data recovered from WAL
	database2, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to reopen database: %v", testError)
	}
	defer database2.Close()

	customerTable2, testError := keeper.GetTable[string, Customer](database2, "customers")
	if testError != nil {
		test.Fatalf("failed to get table: %v", testError)
	}

	gotRecord, found, testError := customerTable2.FindByID(nil, "cust_1")
	if testError != nil {
		test.Fatalf("failed to find recovered record: %v", testError)
	}
	if !found {
		test.Fatalf("recovered record not found")
	}
	if gotRecord.Name != "Alice" || gotRecord.Age != 30 {
		test.Errorf("incorrect recovered record: %+v", gotRecord)
	}
}

func TestCompaction(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-compaction-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}

	customerTable, testError := keeper.GetTable[string, Customer](database, "customers")
	if testError != nil {
		database.Close()
		test.Fatalf("failed to get table: %v", testError)
	}

	customer1 := Customer{ID: "c1", Name: "Alice", Email: "alice@example.com"}
	_ = customerTable.Insert(nil, customer1)

	// Update customer1 multiple times to generate stale records
	for index := 0; index < 5; index++ {
		customer1.Age = 20 + index
		_ = customerTable.Update(nil, customer1)
	}

	// compact
	if testError := database.Compact(); testError != nil {
		database.Close()
		test.Fatalf("compaction failed: %v", testError)
	}

	database.Close()

	// Reopen and check
	database2, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to reopen after compaction: %v", testError)
	}
	defer database2.Close()

	customerTable2, testError := keeper.GetTable[string, Customer](database2, "customers")
	if testError != nil {
		test.Fatalf("failed to get table after compaction: %v", testError)
	}

	gotRecord, found, testError := customerTable2.FindByID(nil, "c1")
	if testError != nil {
		test.Fatalf("failed to find after compaction: %v", testError)
	}
	if !found || gotRecord.Age != 24 {
		test.Fatalf("record not found or incorrect state: found=%v, age=%d", found, gotRecord.Age)
	}
}

func TestQuery(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-query-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	customerTable, testError := keeper.GetTable[string, Customer](database, "customers")
	if testError != nil {
		test.Fatalf("failed to get table: %v", testError)
	}

	_ = customerTable.Insert(nil, Customer{ID: "c1", Name: "Alice", Email: "alice@example.com", Age: 30})
	_ = customerTable.Insert(nil, Customer{ID: "c2", Name: "Bob", Email: "bob@example.com", Age: 25})
	_ = customerTable.Insert(nil, Customer{ID: "c3", Name: "Charlie", Email: "charlie@example.com", Age: 35})

	// 1. Where Age >= 30, Sort by Age Descending
	queryValue := customerTable.Query(nil).
		Where(keeper.GreaterThanOrEqual("Age", 30)).
		OrderBy(keeper.Descending("Age"))

	results, testError := queryValue.List()
	if testError != nil {
		test.Fatalf("query failed: %v", testError)
	}
	if len(results) != 2 {
		test.Fatalf("expected 2 records, got %d", len(results))
	}
	if results[0].ID != "c3" || results[1].ID != "c1" {
		test.Errorf("incorrect sort or filter: %+v", results)
	}

	// Explain
	plan := queryValue.Explain()
	if plan.Strategy == "" {
		test.Errorf("explain strategy is empty")
	}

	// 2. Limit and Offset
	results2, testError := customerTable.Query(nil).
		OrderBy(keeper.Ascending("Age")).
		Offset(1).
		Limit(1).
		List()

	if testError != nil {
		test.Fatalf("query failed: %v", testError)
	}
	if len(results2) != 1 || results2[0].ID != "c1" {
		test.Errorf("limit/offset failed, got: %+v", results2)
	}
}

func TestJSONExportImport(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-json-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}

	customerTable, testError := keeper.GetTable[string, Customer](database, "customers")
	if testError != nil {
		database.Close()
		test.Fatalf("failed to get table: %v", testError)
	}

	_ = customerTable.Insert(nil, Customer{ID: "c1", Name: "Alice", Email: "alice@example.com", Age: 30})
	_ = customerTable.Insert(nil, Customer{ID: "c2", Name: "Bob", Email: "bob@example.com", Age: 25})

	jsonPath := filepath.Join(tempDirectory, "export.json")
	if testError := database.ExportJSON(jsonPath); testError != nil {
		database.Close()
		test.Fatalf("failed to export JSON: %v", testError)
	}

	database.Close()

	// Reopen a fresh database and import JSON
	tempDir2, _ := os.MkdirTemp("", "keeper-test-json-import-*")
	defer os.RemoveAll(tempDir2)

	database2, testError := keeper.Open(tempDir2, options)
	if testError != nil {
		test.Fatalf("failed to open database2: %v", testError)
	}
	defer database2.Close()

	// Get table registers metadata
	customerTable2, _ := keeper.GetTable[string, Customer](database2, "customers")

	if testError := database2.ImportJSON(jsonPath); testError != nil {
		test.Fatalf("failed to import JSON: %v", testError)
	}

	// Verify imported data
	gotRecord, found, _ := customerTable2.FindByID(nil, "c1")
	if !found || gotRecord.Name != "Alice" {
		test.Fatalf("imported record c1 not found or invalid: %+v", gotRecord)
	}
}

func TestAsyncAndBatchedDurability(test *testing.T) {
	for _, mode := range []keeper.DurabilityMode{keeper.DurabilityAsync, keeper.DurabilityBatched} {
		test.Run(fmt.Sprintf("Mode_%d", mode), func(t *testing.T) {
			tempDirectory, testError := os.MkdirTemp("", "keeper-test-durability-*")
			if testError != nil {
				test.Fatalf("failed to create temp directory: %v", testError)
			}
			defer os.RemoveAll(tempDirectory)

			options := keeper.DefaultOptions()
			options.Durability = mode
			options.RegisterTypes(Customer{})

			database, testError := keeper.Open(tempDirectory, options)
			if testError != nil {
				test.Fatalf("failed to open database: %v", testError)
			}

			customerTable, testError := keeper.GetTable[string, Customer](database, "customers")
			if testError != nil {
				database.Close()
				test.Fatalf("failed to get table: %v", testError)
			}

			// Concurrent inserts
			const numGoroutines = 10
			var syncWaitGroup syncWaitGroup
			syncWaitGroup.Add(numGoroutines)
			for index := 0; index < numGoroutines; index++ {
				go func(index int) {
					defer syncWaitGroup.Done()
					c := Customer{
						ID:    fmt.Sprintf("c_%d", index),
						Name:  fmt.Sprintf("User_%d", index),
						Email: fmt.Sprintf("user_%d@example.com", index),
						Age:   20 + index,
					}
					_ = customerTable.Insert(nil, c)
				}(index)
			}
			syncWaitGroup.Wait()

			// Check all records exist
			for index := 0; index < numGoroutines; index++ {
				_, found, testError := customerTable.FindByID(nil, fmt.Sprintf("c_%d", index))
				if testError != nil || !found {
					database.Close()
					test.Fatalf("expected record c_%d to be found (testError =%v)", index, testError)
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
func errorsAs(actualError error, target any) bool {
	if actualError == nil {
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
	errorValue := reflect.ValueOf(actualError)
	if errorValue.Type().AssignableTo(targetType) {
		reflectValue.Elem().Set(errorValue)
		return true
	}
	
	return false
}

func TestHotBackup(test *testing.T) {
	directory, testError := os.MkdirTemp("", "keeper-backup-src-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(directory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})
	database, testError := keeper.Open(directory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	customerTable, testError := keeper.GetTable[string, Customer](database, "customers")
	if testError != nil {
		test.Fatalf("failed to get table: %v", testError)
	}

	// Insert some records
	testError = database.Transaction(func(transaction *keeper.Transaction) error {
		customer1 := Customer{ID: "c1", Name: "Alice", Email: "alice@example.com", Age: 30}
		customer2 := Customer{ID: "c2", Name: "Bob", Email: "bob@example.com", Age: 35}
		if testError := customerTable.Insert(transaction, customer1); testError != nil {
			return testError
		}
		if testError := customerTable.Insert(transaction, customer2); testError != nil {
			return testError
		}
		return nil
	})
	if testError != nil {
		test.Fatalf("failed transaction: %v", testError)
	}

	// Create backup directory
	backupDirectory, testError := os.MkdirTemp("", "keeper-backup-dest-*")
	if testError != nil {
		test.Fatalf("failed to create backup temp directory: %v", testError)
	}
	defer os.RemoveAll(backupDirectory)

	// Perform backup
	if testError := database.Backup(backupDirectory); testError != nil {
		test.Fatalf("failed to backup database: %v", testError)
	}

	// Open the backed up database to verify consistency
	backupDatabase, testError := keeper.Open(backupDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open backed up database: %v", testError)
	}
	defer backupDatabase.Close()

	backupTable, testError := keeper.GetTable[string, Customer](backupDatabase, "customers")
	if testError != nil {
		test.Fatalf("failed to get table in backup database: %v", testError)
	}

	// Verify records are present
	customer1Back, found, testError := backupTable.FindByID(nil, "c1")
	if testError != nil {
		test.Fatalf("failed query on backup table: %v", testError)
	}
	if !found {
		test.Fatalf("customer c1 not found in backup table")
	}
	if customer1Back.Name != "Alice" || customer1Back.Age != 30 {
		test.Errorf("incorrect data in backup customer c1: %+v", customer1Back)
	}

	customer2Back, found, testError := backupTable.FindByID(nil, "c2")
	if testError != nil {
		test.Fatalf("failed query on backup table: %v", testError)
	}
	if !found {
		test.Fatalf("customer c2 not found in backup table")
	}
	if customer2Back.Name != "Bob" || customer2Back.Age != 35 {
		test.Errorf("incorrect data in backup customer c2: %+v", customer2Back)
	}
}

type BigIntIDRecord struct {
	ID   int `keeper:"id"`
	Name string
}

func TestInt64SerializationRegression(test *testing.T) {
	testValues := []int{
		math.MaxInt,
		math.MinInt,
		int(1 << 40),
		int(-(1 << 40)),
		math.MaxInt32,
		math.MinInt32,
	}

	for _, val := range testValues {
		test.Run(fmt.Sprintf("val_%d", val), func(t *testing.T) {
			tempDirectory, testError := os.MkdirTemp("", "keeper-test-int64-*")
			if testError != nil {
				test.Fatalf("failed to create temp directory: %v", testError)
			}
			defer os.RemoveAll(tempDirectory)

			options := keeper.DefaultOptions()
			options.RegisterTypes(BigIntIDRecord{})

			// 1. Open Database
			database, testError := keeper.Open(tempDirectory, options)
			if testError != nil {
				test.Fatalf("failed to open database: %v", testError)
			}

			table, testError := keeper.GetTable[int, BigIntIDRecord](database, "big_ints")
			if testError != nil {
				database.Close()
				test.Fatalf("failed to get table: %v", testError)
			}

			// 2. Insert record
			record := BigIntIDRecord{
				ID:   val,
				Name: "TestRecord",
			}
			testError = table.Insert(nil, record)
			if testError != nil {
				database.Close()
				test.Fatalf("failed to insert: %v", testError)
			}

			// 3. Close database
			database.Close()

			// 4. Reopen database
			database2, testError := keeper.Open(tempDirectory, options)
			if testError != nil {
				test.Fatalf("failed to reopen database: %v", testError)
			}
			defer database2.Close()

			table2, testError := keeper.GetTable[int, BigIntIDRecord](database2, "big_ints")
			if testError != nil {
				test.Fatalf("failed to get table: %v", testError)
			}

			// 5. Find by original ID
			gotRecord, found, testError := table2.FindByID(nil, val)
			if testError != nil {
				test.Fatalf("failed to find record: %v", testError)
			}
			if !found {
				test.Fatalf("record not found")
			}

			// Verify decoded ID is unchanged
			if gotRecord.ID != val {
				test.Errorf("expected ID %d, got %d", val, gotRecord.ID)
			}

			// 6. Test update and deletion using that ID
			gotRecord.Name = "UpdatedRecord"
			testError = table2.Update(nil, gotRecord)
			if testError != nil {
				test.Fatalf("failed to update record: %v", testError)
			}

			gotUpdated, foundUpdated, testError := table2.FindByID(nil, val)
			if testError != nil {
				test.Fatalf("failed to find updated record: %v", testError)
			}
			if !foundUpdated || gotUpdated.Name != "UpdatedRecord" {
				test.Fatalf("update not reflected or record lost: found=%v, name=%s", foundUpdated, gotUpdated.Name)
			}

			deleted, testError := table2.DeleteByID(nil, val)
			if testError != nil {
				test.Fatalf("failed to delete record: %v", testError)
			}
			if !deleted {
				test.Fatalf("expected delete to return true")
			}

			_, foundDeleted, testError := table2.FindByID(nil, val)
			if testError != nil {
				test.Fatalf("failed to find deleted record: %v", testError)
			}
			if foundDeleted {
				test.Fatalf("record still exists after deletion")
			}

			// 7. Confirm overflow into int8, int16, or int32 returns an testError
			type LargeValStruct struct {
				Val int
			}
			data, testError := keeper.Marshal(LargeValStruct{Val: val})
			if testError != nil {
				test.Fatalf("failed to marshal large val struct: %v", testError)
			}

			// Check int8 overflow
			if val < math.MinInt8 || val > math.MaxInt8 {
				var t8 struct{ Val int8 }
				err8 := keeper.Unmarshal(data, &t8)
				if err8 == nil {
					test.Errorf("expected overflow testError casting %d to int8, but got nil", val)
				} else if !strings.Contains(err8.Error(), "overflows") {
					test.Errorf("expected testError message to contain 'overflows', got: %v", err8)
				}
			}

			// Check int16 overflow
			if val < math.MinInt16 || val > math.MaxInt16 {
				var t16 struct{ Val int16 }
				err16 := keeper.Unmarshal(data, &t16)
				if err16 == nil {
					test.Errorf("expected overflow testError casting %d to int16, but got nil", val)
				} else if !strings.Contains(err16.Error(), "overflows") {
					test.Errorf("expected testError message to contain 'overflows', got: %v", err16)
				}
			}

			// Check int32 overflow
			if val < math.MinInt32 || val > math.MaxInt32 {
				var t32 struct{ Val int32 }
				err32 := keeper.Unmarshal(data, &t32)
				if err32 == nil {
					test.Errorf("expected overflow testError casting %d to int32, but got nil", val)
				} else if !strings.Contains(err32.Error(), "overflows") {
					test.Errorf("expected testError message to contain 'overflows', got: %v", err32)
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

func TestInt64E2E(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-e2e-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(UserRecord{})

	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}

	table, testError := keeper.GetTable[UserID, UserRecord](database, "users")
	if testError != nil {
		database.Close()
		test.Fatalf("failed to get table: %v", testError)
	}

	// Test named type values
	val := UserID(1 << 40)
	testError = table.Insert(nil, UserRecord{ID: val, Name: "Alice"})
	if testError != nil {
		database.Close()
		test.Fatalf("failed to insert: %v", testError)
	}

	database.Close()

	// Reopen
	database2, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to reopen database: %v", testError)
	}
	defer database2.Close()

	table2, testError := keeper.GetTable[UserID, UserRecord](database2, "users")
	if testError != nil {
		test.Fatalf("failed to get table: %v", testError)
	}

	gotRecord, found, testError := table2.FindByID(nil, val)
	if testError != nil {
		test.Fatalf("failed to find: %v", testError)
	}
	if !found {
		test.Fatalf("not found")
	}
	if gotRecord.ID != val || gotRecord.Name != "Alice" {
		test.Errorf("unexpected record: %+v", gotRecord)
	}

	// Update
	gotRecord.Name = "Bob"
	testError = table2.Update(nil, gotRecord)
	if testError != nil {
		test.Fatalf("failed to update: %v", testError)
	}

	gotRecord2, found2, _ := table2.FindByID(nil, val)
	if !found2 || gotRecord2.Name != "Bob" {
		test.Errorf("update failed")
	}

	// Delete
	deleted, testError := table2.DeleteByID(nil, val)
	if testError != nil || !deleted {
		test.Fatalf("failed to delete")
	}

	_, found3, _ := table2.FindByID(nil, val)
	if found3 {
		test.Errorf("record still exists after delete")
	}
}

func TestTransactionAfterCloseDeadlock(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-deadlock-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(UserRecord{})

	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}

	table, testError := keeper.GetTable[UserID, UserRecord](database, "users")
	if testError != nil {
		database.Close()
		test.Fatalf("failed to get table: %v", testError)
	}

	// Close database
	if testError := database.Close(); testError != nil {
		test.Fatalf("failed to close database: %v", testError)
	}

	// 1. Transaction should return DatabaseClosedError
	testError = database.Transaction(func(transaction *keeper.Transaction) error {
		return nil
	})
	if testError != keeper.DatabaseClosedError {
		test.Errorf("expected DatabaseClosedError on Transaction, got: %v", testError)
	}

	// 2. Insert should return DatabaseClosedError
	testError = table.Insert(nil, UserRecord{ID: 1, Name: "Alice"})
	if testError != keeper.DatabaseClosedError {
		test.Errorf("expected DatabaseClosedError on Insert, got: %v", testError)
	}

	// 3. Update should return DatabaseClosedError
	testError = table.Update(nil, UserRecord{ID: 1, Name: "Bob"})
	if testError != keeper.DatabaseClosedError {
		test.Errorf("expected DatabaseClosedError on Update, got: %v", testError)
	}

	// 4. DeleteByID should return DatabaseClosedError
	_, testError = table.DeleteByID(nil, UserID(1))
	if testError != keeper.DatabaseClosedError {
		test.Errorf("expected DatabaseClosedError on DeleteByID, got: %v", testError)
	}

	// 5. Compact should return DatabaseClosedError
	testError = database.Compact()
	if testError != keeper.DatabaseClosedError {
		test.Errorf("expected DatabaseClosedError on Compact, got: %v", testError)
	}

	// 6. Backup should return DatabaseClosedError
	testError = database.Backup(filepath.Join(tempDirectory, "backup"))
	if testError != keeper.DatabaseClosedError {
		test.Errorf("expected DatabaseClosedError on Backup, got: %v", testError)
	}
}

func TestPathTraversalE2E(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-traversal-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(UserRecord{})

	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	// 1. Attempts to get table with traversal should fail
	_, testError = keeper.GetTable[UserID, UserRecord](database, "../escaped")
	if testError != keeper.InvalidTableNameError {
		test.Errorf("expected InvalidTableNameError, got: %v", testError)
	}

	// 2. Attempts to drop table with traversal should fail
	_, testError = database.DropTable("../escaped")
	if testError != keeper.InvalidTableNameError {
		test.Errorf("expected InvalidTableNameError on DropTable, got: %v", testError)
	}

	// 3. Confirm that no file escaped.db exists outside/above the db directory
	parentDirectory := filepath.Dir(tempDirectory)
	escapedDBPath := filepath.Join(parentDirectory, "escaped.db")
	if _, testError := os.Stat(escapedDBPath); testError == nil {
		test.Errorf("vulnerability check failed: escaped.db was created outside database directory at %s", escapedDBPath)
		_ = os.Remove(escapedDBPath)
	}
}

func TestDeadlockE2E(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-deadlock-e2e-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(UserRecord{})

	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}

	table, testError := keeper.GetTable[UserID, UserRecord](database, "users")
	if testError != nil {
		database.Close()
		test.Fatalf("failed to get table: %v", testError)
	}

	// Start a transaction that does some work
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		testError := database.Transaction(func(transaction *keeper.Transaction) error {
			time.Sleep(50 * time.Millisecond) // hold the lock
			return table.Insert(transaction, UserRecord{ID: 1, Name: "Alice"})
		})
		if testError != nil {
			test.Errorf("unexpected testError in concurrent transaction: %v", testError)
		}
	}()

	// Wait a bit to let the transaction start and acquire the lock
	time.Sleep(10 * time.Millisecond)

	// Close the database concurrently. It should wait for the transaction to finish and not deadlock.
	closeChan := make(chan struct{})
	go func() {
		_ = database.Close()
		close(closeChan)
	}()

	select {
	case <-closeChan:
		// Database closed successfully
	case <-time.After(1 * time.Second):
		test.Fatal("deadlock detected: Close did not return within 1 second")
	}

	waitGroup.Wait()

	// Subsequent operations should fail with DatabaseClosedError
	testError = database.Transaction(func(transaction *keeper.Transaction) error {
		return nil
	})
	if testError != keeper.DatabaseClosedError {
		test.Errorf("expected DatabaseClosedError, got %v", testError)
	}
}

func TestRecoveryNoDuplication(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-duplication-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(UserRecord{})

	// 1. Open database and insert record
	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}

	table, testError := keeper.GetTable[UserID, UserRecord](database, "users")
	if testError != nil {
		database.Close()
		test.Fatalf("failed to get table: %v", testError)
	}

	testError = table.Insert(nil, UserRecord{ID: 1, Name: "Alice"})
	if testError != nil {
		database.Close()
		test.Fatalf("failed to insert record: %v", testError)
	}

	database.Close()

	// 2. Measure table storage file size
	dbFilePath := filepath.Join(tempDirectory, "users.db")
	fileInfo1, testError := os.Stat(dbFilePath)
	if testError != nil {
		test.Fatalf("failed to stat users.db: %v", testError)
	}
	initialSize := fileInfo1.Size()
	if initialSize == 0 {
		test.Fatalf("expected users.db size to be greater than 0")
	}

	// 3. Reopen database (triggers recovery replay) and close immediately
	database2, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to reopen database: %v", testError)
	}
	database2.Close()

	// 4. Measure table storage file size again
	fileInfo2, testError := os.Stat(dbFilePath)
	if testError != nil {
		test.Fatalf("failed to stat users.db after reopen: %v", testError)
	}
	reopenedSize := fileInfo2.Size()

	if reopenedSize != initialSize {
		test.Errorf("record duplication detected: file size grew from %d bytes to %d bytes on database reopen", initialSize, reopenedSize)
	}

	// 5. Reopen and verify data is still correct
	database3, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to reopen database for read: %v", testError)
	}
	defer database3.Close()

	table3, testError := keeper.GetTable[UserID, UserRecord](database3, "users")
	if testError != nil {
		test.Fatalf("failed to get table on third open: %v", testError)
	}

	record, found, testError := table3.FindByID(nil, UserID(1))
	if testError != nil {
		test.Fatalf("failed to find record after recovery: %v", testError)
	}
	if !found {
		test.Fatalf("record not found after recovery")
	}
	if record.Name != "Alice" {
		test.Errorf("expected record name Alice, got %s", record.Name)
	}
}

func TestIncompatibleTypesPrevention(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-incompatible-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(UserRecord{})

	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	// 1. First registration should succeed
	_, testError = keeper.GetTable[UserID, UserRecord](database, "users")
	if testError != nil {
		test.Fatalf("failed to register table first time: %v", testError)
	}

	// 2. Registering with incompatible ID type should fail
	_, testError = keeper.GetTable[string, UserRecord](database, "users")
	if testError == nil {
		test.Errorf("expected testError when registering with incompatible ID type, got nil")
	} else if !errors.Is(testError, keeper.IncompatibleTypesError) {
		test.Errorf("expected errors.Is(testError, IncompatibleTypesError), got: %v", testError)
	}

	// 3. Registering with incompatible Entity type should fail
	type DifferentRecord struct {
		ID   UserID `keeper:"id"`
		Age  int
	}
	_, testError = keeper.GetTable[UserID, DifferentRecord](database, "users")
	if testError == nil {
		test.Errorf("expected testError when registering with incompatible Entity type, got nil")
	} else if !errors.Is(testError, keeper.IncompatibleTypesError) {
		test.Errorf("expected errors.Is(testError, IncompatibleTypesError), got: %v", testError)
	}

	// 4. Registering with correct types again should succeed
	_, testError = keeper.GetTable[UserID, UserRecord](database, "users")
	if testError != nil {
		test.Errorf("failed to register table again with correct types: %v", testError)
	}
}

func TestQueryAfterClearE2E(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-query-clear-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	customerTable, testError := keeper.GetTable[string, Customer](database, "customers")
	if testError != nil {
		test.Fatalf("failed to get table: %v", testError)
	}

	// 1. Insert initial record
	testError = customerTable.Insert(nil, Customer{ID: "cust_1", Name: "Alice", Email: "alice@example.com", Age: 30})
	if testError != nil {
		test.Fatalf("failed to insert initial: %v", testError)
	}

	// 2. Start transaction, clear table, insert new record, and query it
	testError = database.Transaction(func(transaction *keeper.Transaction) error {
		testError := customerTable.Clear(transaction)
		if testError != nil {
			return testError
		}

		testError = customerTable.Insert(transaction, Customer{ID: "cust_new", Name: "NewAlice", Email: "new@example.com", Age: 25})
		if testError != nil {
			return testError
		}

		results, testError := customerTable.Query(transaction).
			Where(keeper.Equal("Email", "new@example.com")).
			List()
		if testError != nil {
			return testError
		}

		if len(results) != 1 {
			test.Errorf("expected 1 record from query after clear, got: %d", len(results))
			return nil
		}

		if results[0].ID != "cust_new" {
			test.Errorf("expected customer ID cust_new, got: %s", results[0].ID)
		}

		return nil
	})
	if testError != nil {
		test.Fatalf("transaction failed: %v", testError)
	}
}

func TestNegativeOffsetPrevention(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-offset-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	customerTable, testError := keeper.GetTable[string, Customer](database, "customers")
	if testError != nil {
		test.Fatalf("failed to get table: %v", testError)
	}

	// Insert test records
	_ = customerTable.Insert(nil, Customer{ID: "cust_1", Name: "Alice", Email: "alice@example.com", Age: 30})
	_ = customerTable.Insert(nil, Customer{ID: "cust_2", Name: "Bob", Email: "bob@example.com", Age: 25})

	// Query with negative offset
	results, testError := customerTable.Query(nil).
		Offset(-1).
		List()
	if testError != nil {
		test.Fatalf("query failed: %v", testError)
	}

	if len(results) != 2 {
		test.Errorf("expected 2 records, got: %d", len(results))
	}
}

func TestConcurrentReadClose(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-read-close-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(Customer{})

	database, testError := keeper.Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}

	customerTable, testError := keeper.GetTable[string, Customer](database, "customers")
	if testError != nil {
		database.Close()
		test.Fatalf("failed to get table: %v", testError)
	}

	// Insert initial record
	_ = customerTable.Insert(nil, Customer{ID: "c1", Name: "Alice", Age: 30})

	var wg sync.WaitGroup
	concurrencyLimit := 10

	// Spawn multiple concurrent reader goroutines
	for i := 0; i < concurrencyLimit; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, _, testError := customerTable.FindByID(nil, "c1")
				if testError != nil {
					if testError == keeper.DatabaseClosedError {
						return // database closed successfully
					}
				}
			}
		}()
	}

	// Spawn multiple concurrent writer goroutines
	for i := 0; i < concurrencyLimit; i++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()
			for j := 0; ; j++ {
				testError := database.Transaction(func(tx *keeper.Transaction) error {
					return customerTable.Insert(tx, Customer{
						ID:   fmt.Sprintf("w_%d_%d", workerIndex, j),
						Name: "ActiveUser",
						Age:  20 + j,
					})
				})
				if testError != nil {
					if testError == keeper.DatabaseClosedError || strings.Contains(testError.Error(), "closed") {
						return // database closed successfully
					}
				}
			}
		}(i)
	}

	// Let readers and writers saturate the database, then close it
	time.Sleep(10 * time.Millisecond)
	if testError := database.Close(); testError != nil {
		test.Errorf("failed to close database: %v", testError)
	}

	wg.Wait()
}








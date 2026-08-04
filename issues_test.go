package masterkeeper

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type IssueCustomer struct {
	ID   string `keeper:"id"`
	Name string `keeper:"index"`
	Age  int
}

type Float32Record struct {
	ID    string  `keeper:"id"`
	Score float32 `keeper:"index"`
	Age8  int8    `keeper:"index"`
	Age16 int16   `keeper:"index"`
}

type BytesRecord struct {
	ID   string `keeper:"id"`
	Hash []byte `keeper:"unique"`
}

type MultiIDRecord struct {
	ID1 string `keeper:"id"`
	ID2 string `keeper:"id"`
}

type NoIDRecord struct {
	Name string
}

func TestWALChecksumCorruption(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-corrupt-wal-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(IssueCustomer{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}

	customerTable, testError := GetTable[string, IssueCustomer](database, "customers")
	if testError != nil {
		database.Close()
		test.Fatalf("failed to get table: %v", testError)
	}

	_ = customerTable.Insert(nil, IssueCustomer{ID: "c1", Name: "Alice", Age: 30})
	database.Close()

	// Corrupt final WAL record checksum
	walPath := filepath.Join(tempDirectory, "wal.log")
	walFile, err := os.OpenFile(walPath, os.O_RDWR, 0644)
	if err != nil {
		test.Fatalf("failed to open WAL: %v", err)
	}
	fileInfo, _ := walFile.Stat()
	if fileInfo.Size() >= 4 {
		_, _ = walFile.WriteAt([]byte{0xde, 0xad, 0xbe, 0xef}, fileInfo.Size()-4)
	}
	walFile.Close()

	// Reopen database should return a checksum corruption error!
	_, testError = Open(tempDirectory, options)
	if testError == nil {
		test.Errorf("expected WAL checksum corruption error, got nil")
	} else if !strings.Contains(testError.Error(), "checksum mismatch") {
		test.Errorf("expected checksum mismatch error, got: %v", testError)
	}
}

func TestWALCommitWriteFailure(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-commit-fail-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(IssueCustomer{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}

	customerTable, testError := GetTable[string, IssueCustomer](database, "customers")
	if testError != nil {
		database.Close()
		test.Fatalf("failed to get table: %v", testError)
	}

	// Force write failure by closing the table storage's file
	tableStorageVal, err := database.getTableStorage("customers")
	if err != nil {
		database.Close()
		test.Fatalf("failed to get table storage: %v", err)
	}
	tableStorageVal.mu.Lock()
	if tableStorageVal.file != nil {
		tableStorageVal.file.Close()
	}
	tableStorageVal.mu.Unlock()

	// Attempt to commit a transaction (should fail)
	testError = database.Transaction(func(tx *Transaction) error {
		return customerTable.Insert(tx, IssueCustomer{ID: "c1", Name: "Alice", Age: 30})
	})
	if testError == nil {
		test.Errorf("expected commit write failure, got nil")
	}

	database.Close()

	// Reopen database and verify the record is NOT present!
	database2, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to reopen database: %v", testError)
	}
	defer database2.Close()

	customerTable2, _ := GetTable[string, IssueCustomer](database2, "customers")
	_, found, _ := customerTable2.FindByID(nil, "c1")
	if found {
		test.Errorf("failed transaction record should not have been committed or recovered")
	}
}

func TestCorruptSnapshotValidation(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-corrupt-snap-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(IssueCustomer{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}

	customerTable, testError := GetTable[string, IssueCustomer](database, "customers")
	if testError != nil {
		database.Close()
		test.Fatalf("failed to get table: %v", testError)
	}

	_ = customerTable.Insert(nil, IssueCustomer{ID: "c1", Name: "Alice", Age: 30})
	if testError := database.Compact(); testError != nil {
		database.Close()
		test.Fatalf("failed to compact: %v", testError)
	}
	database.Close()

	// Corrupt snapshot file checksum at the end of the file
	files, _ := os.ReadDir(tempDirectory)
	var snapPath string
	for _, entry := range files {
		if strings.HasPrefix(entry.Name(), "snapshot.") {
			snapPath = filepath.Join(tempDirectory, entry.Name())
			break
		}
	}
	if snapPath != "" {
		snapFile, err := os.OpenFile(snapPath, os.O_RDWR, 0644)
		if err == nil {
			fileInfo, _ := snapFile.Stat()
			if fileInfo.Size() >= 8 {
				_, _ = snapFile.WriteAt([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, fileInfo.Size()-8)
			}
			snapFile.Close()
		}
	} else {
		test.Fatalf("snapshot file not found in %s", tempDirectory)
	}

	// Reopen database should fail during snapshot read, but table storage file must NOT be deleted or modified!
	_, testError = Open(tempDirectory, options)
	if testError == nil {
		test.Errorf("expected snapshot validation error, got nil")
	}

	// Verify that customers.db still exists on disk
	if _, err := os.Stat(filepath.Join(tempDirectory, "customers.db")); os.IsNotExist(err) {
		test.Errorf("customers.db was deleted or modified upon snapshot validation failure")
	}
}

func TestGetTableSchemaValidation(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-schema-val-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(IssueCustomer{}, Float32Record{}, BytesRecord{}, MultiIDRecord{}, NoIDRecord{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	// 1. Generic ID mismatch validation
	_, testError = GetTable[int, IssueCustomer](database, "customers")
	if testError == nil || !errors.Is(testError, IncompatibleTypesError) {
		test.Errorf("expected IncompatibleTypesError for ID mismatch, got: %v", testError)
	}

	// 2. T is not a struct validation
	_, testError = GetTable[string, string](database, "strings")
	if testError == nil || !errors.Is(testError, IncompatibleTypesError) {
		test.Errorf("expected IncompatibleTypesError for non-struct type, got: %v", testError)
	}

	// 3. Entity has no ID field validation
	_, testError = GetTable[string, NoIDRecord](database, "no_id")
	if testError == nil || !errors.Is(testError, IncompatibleTypesError) {
		test.Errorf("expected IncompatibleTypesError for entity with no ID, got: %v", testError)
	}

	// 4. Entity has multiple ID fields validation
	_, testError = GetTable[string, MultiIDRecord](database, "multi_id")
	if testError == nil || !errors.Is(testError, IncompatibleTypesError) {
		test.Errorf("expected IncompatibleTypesError for entity with multiple IDs, got: %v", testError)
	}
}

func TestFloat32QueryComparison(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-float32-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(Float32Record{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	table, testError := GetTable[string, Float32Record](database, "floats")
	if testError != nil {
		test.Fatalf("failed to get table: %v", testError)
	}

	_ = table.Insert(nil, Float32Record{ID: "r1", Score: 2.0, Age8: 2, Age16: 2})
	_ = table.Insert(nil, Float32Record{ID: "r2", Score: 10.0, Age8: 10, Age16: 10})

	// Float32 comparison check
	results, testError := table.Query(nil).
		Where(GreaterThan("Score", float32(5.0))).
		List()
	if testError != nil {
		test.Fatalf("query failed: %v", testError)
	}
	if len(results) != 1 || results[0].ID != "r2" {
		test.Errorf("expected 1 record with ID r2 for float32 query, got: %+v", results)
	}

	// Int8 comparison check
	results, testError = table.Query(nil).
		Where(GreaterThan("Age8", int8(5))).
		List()
	if testError != nil {
		test.Fatalf("query failed: %v", testError)
	}
	if len(results) != 1 || results[0].ID != "r2" {
		test.Errorf("expected 1 record with ID r2 for int8 query, got: %+v", results)
	}

	// Int16 comparison check
	results, testError = table.Query(nil).
		Where(GreaterThan("Age16", int16(5))).
		List()
	if testError != nil {
		test.Fatalf("query failed: %v", testError)
	}
	if len(results) != 1 || results[0].ID != "r2" {
		test.Errorf("expected 1 record with ID r2 for int16 query, got: %+v", results)
	}
}

func TestSliceByteUniqueIndex(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-bytes-unique-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(BytesRecord{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	table, testError := GetTable[string, BytesRecord](database, "documents")
	if testError != nil {
		test.Fatalf("failed to get table: %v", testError)
	}

	// Insert distinct byte slices (should succeed)
	if testError := table.Insert(nil, BytesRecord{ID: "d1", Hash: []byte("hello")}); testError != nil {
		test.Fatalf("failed to insert document: %v", testError)
	}
	if testError := table.Insert(nil, BytesRecord{ID: "d2", Hash: []byte("world")}); testError != nil {
		test.Fatalf("failed to insert document: %v", testError)
	}

	// Insert duplicate byte slice (should fail unique constraint validation)
	testError = table.Insert(nil, BytesRecord{ID: "d3", Hash: []byte("hello")})
	if testError == nil {
		test.Errorf("expected duplicate unique index conflict for byte slice, got nil")
	}
}

func TestNestedTransactionRejection(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-nested-tx-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(IssueCustomer{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	// Nested transaction attempt should return NestedTransactionNotSupportedError immediately
	testError = database.Transaction(func(outer *Transaction) error {
		return database.Transaction(func(inner *Transaction) error {
			return nil
		})
	})

	if !errors.Is(testError, NestedTransactionNotSupportedError) {
		test.Errorf("expected NestedTransactionNotSupportedError, got: %v", testError)
	}
}

func TestSemanticInvalidSnapshotCorruptsDecodable(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-semantic-snap-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(IssueCustomer{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}

	customerTable, testError := GetTable[string, IssueCustomer](database, "customers")
	if testError != nil {
		database.Close()
		test.Fatalf("failed to get table: %v", testError)
	}

	_ = customerTable.Insert(nil, IssueCustomer{ID: "c1", Name: "Alice", Age: 30})
	if testError := database.Compact(); testError != nil {
		database.Close()
		test.Fatalf("failed to compact: %v", testError)
	}
	database.Close()

	// Locate the snapshot file and corrupt a record's tag or make it fail unmarshal
	files, _ := os.ReadDir(tempDirectory)
	var snapshotPath string
	for _, entry := range files {
		if strings.HasPrefix(entry.Name(), "snapshot.") {
			snapshotPath = filepath.Join(tempDirectory, entry.Name())
			break
		}
	}
	if snapshotPath != "" {
		snapshotBytes, operationError := os.ReadFile(snapshotPath)
		if operationError == nil {
			// Corrupt last 12 bytes of the payload (just before the 8-byte checksum)
			if len(snapshotBytes) > 30 {
				for i := len(snapshotBytes) - 20; i < len(snapshotBytes) - 8; i++ {
					snapshotBytes[i] = 0xff
				}
				
				// Recompute CRC32
				checksumTable := crc32.MakeTable(crc32.Castagnoli)
				hash := crc32.New(checksumTable)
				_, _ = hash.Write(snapshotBytes[:len(snapshotBytes)-8])
				checksum := hash.Sum32()
				
				binary.BigEndian.PutUint64(snapshotBytes[len(snapshotBytes)-8:], uint64(checksum))
				_ = os.WriteFile(snapshotPath, snapshotBytes, 0644)
			}
		}
	}

	// Reopen database should fail during snapshot unmarshal validation, leaving active db files intact
	_, testError = Open(tempDirectory, options)
	if testError == nil {
		test.Errorf("expected snapshot record unmarshal validation to fail, got nil error")
	}

	// Verify that customers.db still exists on disk and is unchanged
	if _, error := os.Stat(filepath.Join(tempDirectory, "customers.db")); os.IsNotExist(error) {
		test.Errorf("customers.db was deleted or modified upon snapshot validation failure")
	}
}

func TestNegativeWALAndSnapshotLengths(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-neg-wal-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(IssueCustomer{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	customerTable, _ := GetTable[string, IssueCustomer](database, "customers")
	_ = customerTable.Insert(nil, IssueCustomer{ID: "c1", Name: "Alice", Age: 30})
	database.Close()

	// Modify WAL payload length to -1 (0xffffffff)
	walPath := filepath.Join(tempDirectory, "wal.log")
	walBytes, err := os.ReadFile(walPath)
	if err == nil && len(walBytes) >= 20 {
		binary.BigEndian.PutUint32(walBytes[len(walBytes)-12:len(walBytes)-8], 0xffffffff)
		_ = os.WriteFile(walPath, walBytes, 0644)
	}

	// Reopen database should return a corruption error instead of panicking
	_, testError = Open(tempDirectory, options)
	if testError == nil || !strings.Contains(testError.Error(), "corrupt") {
		test.Errorf("expected corrupt WAL error for negative length, got: %v", testError)
	}
}

func TestPointerGenericIDValidation(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-ptr-id-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(IssueCustomer{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	_, testError = GetTable[*string, IssueCustomer](database, "customers")
	if testError == nil || !errors.Is(testError, IncompatibleTypesError) {
		test.Errorf("expected IncompatibleTypesError for pointer generic ID, got: %v", testError)
	}
}

type PtrEntityRecord struct {
	ID string `keeper:"id"`
}

func TestPointerEntityValidation(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-ptr-entity-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(PtrEntityRecord{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	_, testError = GetTable[string, *PtrEntityRecord](database, "ptr_entities")
	if testError == nil || !errors.Is(testError, IncompatibleTypesError) {
		test.Errorf("expected IncompatibleTypesError for pointer entity type, got: %v", testError)
	}
}

func TestCrossGoroutineNestedTransactionRejection(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-cross-tx-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(IssueCustomer{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	testError = database.TransactionContext(context.Background(), func(contextValue context.Context, outer *Transaction) error {
		resultChan := make(chan error)
		go func() {
			resultChan <- database.TransactionContext(contextValue, func(contextValue context.Context, inner *Transaction) error {
				return nil
			})
		}()
		return <-resultChan
	})

	if !errors.Is(testError, NestedTransactionNotSupportedError) {
		test.Errorf("expected NestedTransactionNotSupportedError for cross-goroutine nested tx, got: %v", testError)
	}
}

func TestLegacySnapshotMagicCompatibility(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-legacy-snap-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(IssueCustomer{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	customerTable, _ := GetTable[string, IssueCustomer](database, "customers")
	_ = customerTable.Insert(nil, IssueCustomer{ID: "c1", Name: "Alice", Age: 30})
	_ = database.Compact()
	database.Close()

	// Locate snapshot and change magic to legacy SnapshotMagic (0x524d534e)
	files, _ := os.ReadDir(tempDirectory)
	var snapshotPath string
	for _, entry := range files {
		if strings.HasPrefix(entry.Name(), "snapshot.") {
			snapshotPath = filepath.Join(tempDirectory, entry.Name())
			break
		}
	}
	if snapshotPath != "" {
		snapshotBytes, operationError := os.ReadFile(snapshotPath)
		if operationError == nil && len(snapshotBytes) >= 20 {
			binary.BigEndian.PutUint32(snapshotBytes[0:4], 0x524d534e)

			// Recompute CRC32
			checksumTable := crc32.MakeTable(crc32.Castagnoli)
			hash := crc32.New(checksumTable)
			_, _ = hash.Write(snapshotBytes[:len(snapshotBytes)-8])
			checksum := hash.Sum32()
			binary.BigEndian.PutUint64(snapshotBytes[len(snapshotBytes)-8:], uint64(checksum))

			_ = os.WriteFile(snapshotPath, snapshotBytes, 0644)
		}
	}

	database2, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to reopen database with legacy magic snapshot: %v", testError)
	}
	defer database2.Close()

	customerTable2, _ := GetTable[string, IssueCustomer](database2, "customers")
	customer, found, _ := customerTable2.FindByID(nil, "c1")
	if !found || customer.Name != "Alice" {
		test.Errorf("failed to recover customer from legacy magic snapshot")
	}
}

func TestLegacyTransactionTimeout(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-legacy-timeout-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.TransactionWaitTimeout = 50 * time.Millisecond
	options.RegisterTypes(IssueCustomer{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	testError = database.Transaction(func(outer *Transaction) error {
		resultChan := make(chan error)
		go func() {
			resultChan <- database.Transaction(func(inner *Transaction) error {
				return nil
			})
		}()
		return <-resultChan
	})

	if !errors.Is(testError, TransactionWaitTimeoutError) {
		test.Errorf("expected errors.Is(err, TransactionWaitTimeoutError) to be true, got %v", testError)
	}
	if !errors.Is(testError, context.DeadlineExceeded) {
		test.Errorf("expected errors.Is(err, context.DeadlineExceeded) to be true, got %v", testError)
	}
}

func TestRegisterTypesNilValidation(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-register-nil-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(nil)

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()
}

func TestUnknownWALOperationRejection(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-unknown-wal-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(IssueCustomer{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	customerTable, _ := GetTable[string, IssueCustomer](database, "customers")
	_ = customerTable.Insert(nil, IssueCustomer{ID: "c1", Name: "Alice", Age: 30})
	database.Close()

	// Modify the operation type byte in the WAL to 0xFE (unknown type)
	walPath := filepath.Join(tempDirectory, "wal.log")
	walFileBytes, operationError := os.ReadFile(walPath)
	if operationError == nil && len(walFileBytes) >= 30 {
		walFileBytes[4] = 0xFE

		// Recompute CRC32 checksum for the record
		payloadLength := int32(binary.BigEndian.Uint32(walFileBytes[21:25]))
		checksumTable := crc32.MakeTable(crc32.Castagnoli)
		hash := crc32.New(checksumTable)
		_, _ = hash.Write(walFileBytes[4:21]) // type, transactionID, generation
		if payloadLength > 0 {
			_, _ = hash.Write(walFileBytes[29 : 29+payloadLength])
		}
		checksum := hash.Sum32()
		binary.BigEndian.PutUint32(walFileBytes[25:29], checksum)

		_ = os.WriteFile(walPath, walFileBytes, 0644)
	}

	// Reopen database should return a corruption/unknown operation type error!
	_, testError = Open(tempDirectory, options)
	if testError == nil || !strings.Contains(testError.Error(), "unknown operation type") {
		test.Errorf("expected unknown operation type error, got: %v", testError)
	}
}

func TestNilInterfaceEntityValidation(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-nil-interface-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	_, testError = GetTable[string, any](database, "any_table")
	if testError == nil || !errors.Is(testError, IncompatibleTypesError) {
		test.Errorf("expected IncompatibleTypesError for interface entity type, got: %v", testError)
	}
}

func TestAlreadyCancelledContext(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-cancelled-context-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	cancelledContext, cancelFunction := context.WithCancel(context.Background())
	cancelFunction()

	runCount := 0
	testError = database.TransactionContext(cancelledContext, func(contextValue context.Context, transaction *Transaction) error {
		runCount++
		return nil
	})

	if !errors.Is(testError, context.Canceled) {
		test.Errorf("expected context.Canceled, got: %v", testError)
	}
	if runCount != 0 {
		test.Errorf("expected callback not to run, but it ran %d times", runCount)
	}
}

func TestNegativeTimeout(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-negative-timeout-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.TransactionWaitTimeout = -1 * time.Second

	_, testError = Open(tempDirectory, options)
	if !errors.Is(testError, InvalidTransactionWaitTimeoutError) {
		test.Errorf("expected InvalidTransactionWaitTimeoutError, got: %v", testError)
	}
}

func TestZeroTimeout(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-zero-timeout-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.TransactionWaitTimeout = 0 // Wait indefinitely

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	// Acquire lock and hold it for a short duration in a transaction
	lockAcquired := make(chan struct{})
	doneChannel := make(chan error)
	go func() {
		doneChannel <- database.Transaction(func(outer *Transaction) error {
			close(lockAcquired)
			time.Sleep(100 * time.Millisecond)
			return nil
		})
	}()

	<-lockAcquired
	// The next transaction should block until the first one releases the lock
	testError = database.Transaction(func(inner *Transaction) error {
		return nil
	})

	if testError != nil {
		test.Errorf("expected second transaction to succeed after first releases lock, got: %v", testError)
	}
	if firstError := <-doneChannel; firstError != nil {
		test.Errorf("expected first transaction to succeed, got: %v", firstError)
	}
}

func TestTimedOutWaiterLockIntegrity(test *testing.T) {
	tempDirectory, testError := os.MkdirTemp("", "keeper-test-waiter-integrity-*")
	if testError != nil {
		test.Fatalf("failed to create temp directory: %v", testError)
	}
	defer os.RemoveAll(tempDirectory)

	options := DefaultOptions()
	options.RegisterTypes(IssueCustomer{})

	database, testError := Open(tempDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	customerTable, _ := GetTable[string, IssueCustomer](database, "customers")

	// 1. Acquire the lock and hold it
	lockAcquired := make(chan struct{})
	releaseLock := make(chan struct{})
	doneChannel := make(chan error)
	go func() {
		doneChannel <- database.Transaction(func(outer *Transaction) error {
			close(lockAcquired)
			<-releaseLock
			return customerTable.Insert(outer, IssueCustomer{ID: "c1", Name: "Alice"})
		})
	}()

	<-lockAcquired

	// 2. Start a transaction that will time out waiting for the lock
	timeoutContext, cancelFunction := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelFunction()

	timedOutError := database.TransactionContext(timeoutContext, func(contextValue context.Context, inner *Transaction) error {
		return nil
	})

	if !errors.Is(timedOutError, context.DeadlineExceeded) {
		test.Errorf("expected context.DeadlineExceeded, got: %v", timedOutError)
	}

	// 3. Release the lock and verify that the original transaction completes successfully
	close(releaseLock)
	if firstError := <-doneChannel; firstError != nil {
		test.Errorf("expected original transaction to succeed, got: %v", firstError)
	}

	// 4. Verify that data inserted by the original transaction is visible and intact
	customer, found, findError := customerTable.FindByID(nil, "c1")
	if findError != nil {
		test.Fatalf("failed to query customer: %v", findError)
	}
	if !found || customer.Name != "Alice" {
		test.Errorf("expected customer to be Alice, found: %v, customer: %v", found, customer)
	}
}

package masterkeeper

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const SnapshotMagic int32 = 0x524d524d

type Options struct {
	Durability DurabilityMode
	Types      []any
}

func DefaultOptions() Options {
	return Options{
		Durability: DurabilitySync,
	}
}

func (options *Options) RegisterTypes(types ...any) {
	for _, typeValue := range types {
		RegisterType(typeValue)
	}
}

type TableMetadata struct {
	TableName string
	IdType    reflect.Type
	Type      reflect.Type
}

type Database struct {
	directory            string
	durability           DurabilityMode
	walManager           *WalManager
	writeLock            chan struct{}
	activeGoroutineID    atomic.Int64
	committedState       atomic.Pointer[DatabaseState]
	tableMetadataMap     sync.Map // tableName -> TableMetadata
	tableStorageMap      sync.Map // tableName -> *TableStorage
	closed               atomic.Bool
	databaseID           int64
	lastFlushError       error
	lastFlushErrorMutex  sync.RWMutex
}

func Open(directory string, options Options) (*Database, error) {
	// Register all types specified in options
	for _, typeValue := range options.Types {
		RegisterType(typeValue)
	}

	database := &Database{
		directory:  directory,
		durability: options.Durability,
		databaseID: rand.Int63() & 0x7fffffffffffffff,
		writeLock:  make(chan struct{}, 1),
	}
	database.writeLock <- struct{}{}

	// Resolve table storage resolver function
	tableStorageResolver := func(tableName string) (*TableStorage, error) {
		return database.getTableStorage(tableName)
	}

	// 1. Read Snapshot
	snapshotState, error := readSnapshot(directory, database)
	if error != nil {
		return nil, fmt.Errorf("database snapshot read failed: %w", error)
	}

	if snapshotState == nil {
		snapshotState = NewDatabaseState(0)
	}

	// 2. Open WAL Manager
	walManagerValue, error := NewWalManager(directory, options.Durability, tableStorageResolver)
	if error != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", error)
	}
	database.walManager = walManagerValue

	// 3. Read and Replay WAL
	walRecords, error := walManagerValue.ReadAllRecords()
	if error != nil {
		walManagerValue.Close()
		return nil, fmt.Errorf("failed to read WAL records: %w", error)
	}

	recoveredState, error := database.recover(snapshotState, walRecords)
	if error != nil {
		walManagerValue.Close()
		return nil, fmt.Errorf("database recovery failed: %w", error)
	}

	database.committedState.Store(recoveredState)

	// Register metadata for recovered tables
	for _, tableStateValue := range recoveredState.Tables {
		database.tableMetadataMap.Store(tableStateValue.TableName, TableMetadata{
			TableName: tableStateValue.TableName,
			IdType:    tableStateValue.IdType,
			Type:      tableStateValue.EntityType,
		})
	}

	return database, nil
}

func (database *Database) getCommittedState() *DatabaseState {
	return database.committedState.Load()
}

func (database *Database) currentGeneration() int64 {
	return database.getCommittedState().Generation
}

func (database *Database) getTableStorage(tableName string) (*TableStorage, error) {
	if database.closed.Load() {
		return nil, DatabaseClosedError
	}

	if val, ok := database.tableStorageMap.Load(tableName); ok {
		return val.(*TableStorage), nil
	}

	tableStorageValue, error := NewTableStorage(database.directory, tableName)
	if error != nil {
		return nil, error
	}

	actual, loaded := database.tableStorageMap.LoadOrStore(tableName, tableStorageValue)
	if loaded {
		tableStorageValue.Close()
		return actual.(*TableStorage), nil
	}

	return tableStorageValue, nil
}

func (database *Database) lock(ctx context.Context) error {
	select {
	case <-database.writeLock:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (database *Database) unlock() {
	select {
	case database.writeLock <- struct{}{}:
	default:
	}
}

func (database *Database) releaseWriterLock() {
	database.activeGoroutineID.Store(0)
	database.unlock()
}

func (database *Database) publish(nextState *DatabaseState) {
	database.committedState.Store(nextState)
}

func (database *Database) registerTableMetadata(tableName string, idType reflect.Type, entityType reflect.Type) error {
	if error := validateSchema(idType, entityType); error != nil {
		return error
	}

	if !isValidTableName(tableName) {
		return InvalidTableNameError
	}

	if val, found := database.tableMetadataMap.Load(tableName); found {
		meta := val.(TableMetadata)
		if meta.IdType != idType || meta.Type != entityType {
			return fmt.Errorf("%w: expected ID %s and entity %s, got ID %s and entity %s",
				IncompatibleTypesError, meta.IdType.String(), meta.Type.String(), idType.String(), entityType.String())
		}
		return nil
	}

	if err := database.lock(context.Background()); err != nil {
		return err
	}
	defer database.unlock()

	if database.closed.Load() {
		return DatabaseClosedError
	}

	// Double check
	if val, found := database.tableMetadataMap.Load(tableName); found {
		meta := val.(TableMetadata)
		if meta.IdType != idType || meta.Type != entityType {
			return fmt.Errorf("%w: expected ID %s and entity %s, got ID %s and entity %s",
				IncompatibleTypesError, meta.IdType.String(), meta.Type.String(), idType.String(), entityType.String())
		}
		return nil
	}

	RegisterType(reflect.New(entityType).Elem().Interface())
	RegisterType(reflect.New(idType).Elem().Interface())

	// Scan index tags
	var indexMetadataList []IndexMetadata
	for index := 0; index < entityType.NumField(); index++ {
		structField := entityType.Field(index)
		fieldMetadata := parseFieldTag(structField)
		if fieldMetadata.IsIndex {
			indexName := tableName + "_" + fieldMetadata.FieldName + "_idx"
			indexMetadataList = append(indexMetadataList, IndexMetadata{
				IndexName: indexName,
				FieldName: fieldMetadata.FieldName,
				Unique:    fieldMetadata.IsUnique,
				Ordered:   fieldMetadata.IsOrdered,
			})
		}
	}

	nextGenerationeration := database.currentGeneration() + 1
	var walRecords []WalRecord

	// CREATE TABLE WAL record
	var buffer bytes.Buffer
	_ = writeString(&buffer, tableName)
	_ = writeString(&buffer, idType.String())
	_ = writeString(&buffer, entityType.String())
	walRecords = append(walRecords, WalRecord{
		Type:          OpCreateTable,
		TransactionID: database.databaseID,
		Generation:    nextGenerationeration,
		Payload:       buffer.Bytes(),
	})

	// CREATE INDEX WAL records
	for _, indexMetadata := range indexMetadataList {
		var indexBuf bytes.Buffer
		_ = writeString(&indexBuf, tableName)
		_ = writeString(&indexBuf, indexMetadata.IndexName)
		_ = binary.Write(&indexBuf, binary.BigEndian, indexMetadata.Unique)
		_ = binary.Write(&indexBuf, binary.BigEndian, indexMetadata.Ordered)

		walRecords = append(walRecords, WalRecord{
			Type:          OpCreateIndex,
			TransactionID: database.databaseID,
			Generation:    nextGenerationeration,
			Payload:       indexBuf.Bytes(),
		})
	}

	// Submit schema transaction to background writer
	doneChan := make(chan WriteResult, 1)
	database.walManager.Submit(&WriteTask{
		TxID:         database.databaseID,
		Generation:   nextGenerationeration,
		WalRecords:   walRecords,
		TableAppends: nil,
		Done:         doneChan,
	})

	res := <-doneChan
	if res.Error != nil {
		return fmt.Errorf("schema modification failed: %w", res.Error)
	}

	// Publish state update
	newTableState := NewTableState(tableName, idType, entityType, indexMetadataList)
	nextState := database.getCommittedState().Copy(nextGenerationeration)
	nextState.Tables[tableName] = newTableState
	database.publish(nextState)

	database.tableMetadataMap.Store(tableName, TableMetadata{
		TableName: tableName,
		IdType:    idType,
		Type:      entityType,
	})

	return nil
}

func (database *Database) TableNames() []string {
	var names []string
	for tableName := range database.getCommittedState().Tables {
		names = append(names, tableName)
	}
	return names
}

func (database *Database) ContainsTable(tableName string) bool {
	_, found := database.getCommittedState().Tables[tableName]
	return found
}

func (database *Database) DropTable(tableName string) (bool, error) {
	if err := database.lock(context.Background()); err != nil {
		return false, err
	}
	defer database.unlock()

	if database.closed.Load() {
		return false, DatabaseClosedError
	}

	if !isValidTableName(tableName) {
		return false, InvalidTableNameError
	}

	committed := database.getCommittedState()
	if _, found := committed.Tables[tableName]; !found {
		return false, nil
	}

	nextGenerationeration := database.currentGeneration() + 1
	var buffer bytes.Buffer
	_ = writeString(&buffer, tableName)

	doneChan := make(chan WriteResult, 1)
	database.walManager.Submit(&WriteTask{
		TxID:       database.databaseID,
		Generation: nextGenerationeration,
		WalRecords: []WalRecord{
			{
				Type:          OpDropTable,
				TransactionID: database.databaseID,
				Generation:    nextGenerationeration,
				Payload:       buffer.Bytes(),
			},
		},
		TableAppends: nil,
		Done:         doneChan,
	})

	res := <-doneChan
	if res.Error != nil {
		return false, res.Error
	}

	nextState := committed.Copy(nextGenerationeration)
	delete(nextState.Tables, tableName)
	database.publish(nextState)

	database.tableMetadataMap.Delete(tableName)

	// Close and delete table file
	if val, ok := database.tableStorageMap.LoadAndDelete(tableName); ok {
		tableStorageValue := val.(*TableStorage)
		tableStorageValue.Close()
	}
	_ = os.Remove(filepath.Join(database.directory, tableName+".db"))

	return true, nil
}

type txContextKey struct{}

func (database *Database) Transaction(callback func(transaction *Transaction) error) error {
	return database.TransactionContext(context.Background(), func(ctx context.Context, tx *Transaction) error {
		return callback(tx)
	})
}

func (database *Database) TransactionContext(ctx context.Context, callback func(ctx context.Context, transaction *Transaction) error) error {
	if ctx.Value(txContextKey{}) != nil {
		return NestedTransactionNotSupportedError
	}

	currentGoroutineID := getGoroutineID()
	if database.activeGoroutineID.Load() == currentGoroutineID {
		return NestedTransactionNotSupportedError
	}

	if err := database.lock(ctx); err != nil {
		return err
	}
	if database.closed.Load() {
		database.unlock()
		return DatabaseClosedError
	}
	database.activeGoroutineID.Store(currentGoroutineID)
	transactionID := rand.Int63()
	transaction := NewTransaction(transactionID, database, database.getCommittedState())

	defer func() {
		if transaction.IsActive() {
			_ = transaction.Rollback()
		}
	}()

	txCtx := context.WithValue(ctx, txContextKey{}, transaction)
	error := callback(txCtx, transaction)
	if error != nil {
		// Rollback on callback error
		if transaction.IsActive() {
			_ = transaction.Rollback()
		}
		return error
	}

	if transaction.IsActive() {
		return transaction.Commit()
	}

	return nil
}

func (database *Database) Close() error {
	if err := database.lock(context.Background()); err != nil {
		return err
	}
	defer database.unlock()

	if database.closed.Swap(true) {
		return nil
	}

	database.tableStorageMap.Range(func(key, value any) bool {
		tableStorageValue := value.(*TableStorage)
		tableStorageValue.Close()
		return true
	})

	return database.walManager.Close()
}

func (database *Database) Compact() error {
	if err := database.lock(context.Background()); err != nil {
		return err
	}
	defer database.unlock()

	if database.closed.Load() {
		return DatabaseClosedError
	}

	committed := database.getCommittedState()
	// 1. Write snapshot
	if error := writeSnapshot(database.directory, committed, database); error != nil {
		return fmt.Errorf("SNAPSHOT failed during compaction: %w", error)
	}

	// 2. Compact table files
	for tableName, tableStateValue := range committed.Tables {
		tableStorageValue, error := database.getTableStorage(tableName)
		if error != nil {
			return error
		}
		if error := tableStorageValue.Compact(tableStateValue.RecordPointers); error != nil {
			return fmt.Errorf("compaction of table %s failed: %w", tableName, error)
		}
	}

	// 3. Truncate WAL log
	if error := database.walManager.Truncate(); error != nil {
		return fmt.Errorf("WAL truncate failed: %w", error)
	}

	return nil
}

func (database *Database) recover(initialState *DatabaseState, walRecords []WalRecord) (*DatabaseState, error) {
	if len(walRecords) == 0 {
		if initialState != nil {
			return initialState, nil
		}
		return NewDatabaseState(0), nil
	}

	transactionGroups := make(map[int64][]WalRecord)
	var committedTransactionIDs []int64
	rolledBackTransactionIDs := make(map[int64]struct{})

	for _, walRecord := range walRecords {
		transactionGroups[walRecord.TransactionID] = append(transactionGroups[walRecord.TransactionID], walRecord)
		if walRecord.Type == OpCommitTransaction {
			committedTransactionIDs = append(committedTransactionIDs, walRecord.TransactionID)
		} else if walRecord.Type == OpRollbackTransaction {
			rolledBackTransactionIDs[walRecord.TransactionID] = struct{}{}
		}
	}

	// Filter out rolled back or incomplete transactions
	var activeCommits []int64
	for _, transactionID := range committedTransactionIDs {
		if _, rolled := rolledBackTransactionIDs[transactionID]; !rolled {
			activeCommits = append(activeCommits, transactionID)
		}
	}

	// Sort active commits by generation
	sort.Slice(activeCommits, func(indexLeft, indexRight int) bool {
		recordsLeft := transactionGroups[activeCommits[indexLeft]]
		recordsRight := transactionGroups[activeCommits[indexRight]]
		generationLeft := int64(0)
		generationRight := int64(0)
		if len(recordsLeft) > 0 {
			generationLeft = recordsLeft[0].Generation
		}
		if len(recordsRight) > 0 {
			generationRight = recordsRight[0].Generation
		}
		return generationLeft < generationRight
	})

	var databaseState *DatabaseState
	if initialState != nil {
		databaseState = initialState.Copy(initialState.Generation)
	} else {
		databaseState = NewDatabaseState(0)
	}
	currentGen := databaseState.Generation

	clearedTables := make(map[string]bool)
	if initialState != nil {
		for tableName := range initialState.Tables {
			clearedTables[tableName] = true
		}
	}

	for _, transactionID := range activeCommits {
		walRecordsGroup := transactionGroups[transactionID]
		for _, walRecord := range walRecordsGroup {
			if walRecord.Generation > currentGen {
				currentGen = walRecord.Generation
			}

			if walRecord.Type == OpBeginTransaction || walRecord.Type == OpCommitTransaction || walRecord.Type == OpRollbackTransaction {
				continue
			}

			payloadReader := bytes.NewReader(walRecord.Payload)
			switch walRecord.Type {
			case OpInsert, OpUpsert, OpUpdate:
				tableName, error := readString(payloadReader)
				if error != nil {
					return nil, error
				}
				entityClassName, error := readString(payloadReader)
				if error != nil {
					return nil, error
				}
				var payloadLength int32
				if error := binary.Read(payloadReader, binary.BigEndian, &payloadLength); error != nil {
					return nil, error
				}
				recordBytes := make([]byte, payloadLength)
				if _, error := io.ReadFull(payloadReader, recordBytes); error != nil {
					return nil, error
				}

				entityType := getRegisteredType(entityClassName)
				if entityType == nil {
					return nil, fmt.Errorf("recovery failed: type %s not registered", entityClassName)
				}

				newRecordValue := reflect.New(entityType)
				if error := Unmarshal(recordBytes, newRecordValue.Interface()); error != nil {
					return nil, error
				}
				record := newRecordValue.Elem().Interface()

				tableState := databaseState.Tables[tableName]
				if tableState == nil {
					// Table creation fallback if not in state
					tableState = NewTableState(tableName, reflect.TypeOf(""), entityType, nil)
					databaseState.Tables[tableName] = tableState
				}

				tableStorageValue, error := database.getTableStorage(tableName)
				if error != nil {
					return nil, error
				}

				if !clearedTables[tableName] {
					if error := tableStorageValue.Reset(); error != nil {
						return nil, error
					}
					clearedTables[tableName] = true
				}

				id := getPrimaryKey(record)
				oldRecordPointer, found := tableState.RecordPointers[id]
				var oldRecord any
				if found {
					oldRecordBytes, error := tableStorageValue.ReadRecord(oldRecordPointer)
					if error == nil {
						oldRecordReflectValue := reflect.New(entityType)
						if Unmarshal(oldRecordBytes, oldRecordReflectValue.Interface()) == nil {
							oldRecord = oldRecordReflectValue.Elem().Interface()
						}
					}
				}

				recordPointer, error := tableStorageValue.AppendRecord(recordBytes)
				if error != nil {
					return nil, error
				}

				if walRecord.Type == OpInsert {
					tableState.Insert(record, recordPointer)
				} else {
					tableState.Update(record, oldRecord, recordPointer)
				}

			case OpDelete:
				tableName, error := readString(payloadReader)
				if error != nil {
					return nil, error
				}
				_, error = readString(payloadReader) // skip idClassName
				if error != nil {
					return nil, error
				}
				primaryKey, error := readValue(payloadReader)
				if error != nil {
					return nil, error
				}

				tableState := databaseState.Tables[tableName]
				if tableState != nil {
					tableStorageValue, error := database.getTableStorage(tableName)
					if error != nil {
						return nil, error
					}

					oldRecordPointer, found := tableState.RecordPointers[primaryKey]
					var oldRecord any
					if found {
						oldRecordBytes, error := tableStorageValue.ReadRecord(oldRecordPointer)
						if error == nil {
							oldRecordReflectValue := reflect.New(tableState.EntityType)
							if Unmarshal(oldRecordBytes, oldRecordReflectValue.Interface()) == nil {
								oldRecord = oldRecordReflectValue.Elem().Interface()
							}
						}
					}
					tableState.Delete(primaryKey, oldRecord)
				}

			case OpClearTable:
				tableName, error := readString(payloadReader)
				if error != nil {
					return nil, error
				}
				tableState := databaseState.Tables[tableName]
				if tableState != nil {
					tableState.Clear()
				}

			case OpCreateTable:
				tableName, error := readString(payloadReader)
				if error != nil {
					return nil, error
				}
				idClassName, error := readString(payloadReader)
				if error != nil {
					return nil, error
				}
				entityClassName, error := readString(payloadReader)
				if error != nil {
					return nil, error
				}

				idType := getRegisteredType(idClassName)
				if idType == nil {
					idType = reflect.TypeOf("")
				}
				entityType := getRegisteredType(entityClassName)
				if entityType == nil {
					return nil, fmt.Errorf("recovery failed: type %s not registered", entityClassName)
				}

				tableState := NewTableState(tableName, idType, entityType, nil)
				databaseState.Tables[tableName] = tableState

			case OpDropTable:
				tableName, error := readString(payloadReader)
				if error != nil {
					return nil, error
				}
				delete(databaseState.Tables, tableName)

			case OpCreateIndex:
				tableName, error := readString(payloadReader)
				if error != nil {
					return nil, error
				}
				indexName, error := readString(payloadReader)
				if error != nil {
					return nil, error
				}
				var unique bool
				if error := binary.Read(payloadReader, binary.BigEndian, &unique); error != nil {
					return nil, error
				}
				var ordered bool
				if error := binary.Read(payloadReader, binary.BigEndian, &ordered); error != nil {
					return nil, error
				}

				tableState := databaseState.Tables[tableName]
				if tableState != nil {
					// Re-derive FieldName from IndexName
					// e.g. tableName_FieldName_idx
					prefix := tableName + "_"
					suffix := "_idx"
					fieldName := indexName
					if strings.HasPrefix(indexName, prefix) && strings.HasSuffix(indexName, suffix) {
						fieldName = indexName[len(prefix) : len(indexName)-len(suffix)]
					}

					indexMetadata := IndexMetadata{
						IndexName: indexName,
						FieldName: fieldName,
						Unique:    unique,
						Ordered:   ordered,
					}

					tableState.IndexMetadataList = append(tableState.IndexMetadataList, indexMetadata)
					indexState := NewIndexState(indexMetadata)
					tableState.Indexes[indexName] = indexState

					// Populate
					tableStorageValue, error := database.getTableStorage(tableName)
					if error != nil {
						return nil, error
					}

					for primaryKey, recordPointer := range tableState.RecordPointers {
						recordBytes, error := tableStorageValue.ReadRecord(recordPointer)
						if error == nil {
							newRecordReflectValue := reflect.New(tableState.EntityType)
							if Unmarshal(recordBytes, newRecordReflectValue.Interface()) == nil {
								record := newRecordReflectValue.Elem().Interface()
								indexValue := getFieldValue(record, fieldName)
								indexState.Add(indexValue, primaryKey)
							}
						}
					}
				}

			case OpDropIndex:
				tableName, error := readString(payloadReader)
				if error != nil {
					return nil, error
				}
				indexName, error := readString(payloadReader)
				if error != nil {
					return nil, error
				}

				tableState := databaseState.Tables[tableName]
				if tableState != nil {
					delete(tableState.Indexes, indexName)
					var newIndexMetadataList []IndexMetadata
					for _, indexMetadata := range tableState.IndexMetadataList {
						if indexMetadata.IndexName != indexName {
							newIndexMetadataList = append(newIndexMetadataList, indexMetadata)
						}
					}
					tableState.IndexMetadataList = newIndexMetadataList
				}
			default:
				return nil, fmt.Errorf("corrupt WAL: unknown operation type 0x%X", walRecord.Type)
			}
		}
	}

	return databaseState.Copy(currentGen), nil
}

type typeRegistryStruct struct {
	mu sync.RWMutex
	m  map[string]reflect.Type
}

var typeRegistry = typeRegistryStruct{
	m: make(map[string]reflect.Type),
}

func RegisterType(value any) {
	if value == nil {
		return
	}
	typeRegistry.mu.Lock()
	defer typeRegistry.mu.Unlock()
	reflectType := reflect.TypeOf(value)
	for reflectType.Kind() == reflect.Ptr {
		reflectType = reflectType.Elem()
	}
	typeRegistry.m[reflectType.String()] = reflectType
	// Also register by short name
	parts := strings.Split(reflectType.String(), ".")
	typeRegistry.m[parts[len(parts)-1]] = reflectType
}

func getRegisteredType(name string) reflect.Type {
	typeRegistry.mu.RLock()
	defer typeRegistry.mu.RUnlock()
	reflectType, found := typeRegistry.m[name]
	if !found {
		for keyName, valueType := range typeRegistry.m {
			if strings.EqualFold(keyName, name) {
				return valueType
			}
		}
	}
	return reflectType
}

func writeSnapshot(directory string, state *DatabaseState, database *Database) error {
	generation := state.Generation
	temporaryPath := filepath.Join(directory, fmt.Sprintf("snapshot.%d.tmp", generation))
	finalPath := filepath.Join(directory, fmt.Sprintf("snapshot.%d", generation))

	snapshotFile, error := os.OpenFile(temporaryPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if error != nil {
		return error
	}
	defer func() {
		if snapshotFile != nil {
			snapshotFile.Close()
			_ = os.Remove(temporaryPath)
		}
	}()

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	hash := crc32.New(crcTable)
	writer := io.MultiWriter(snapshotFile, hash)

	if error := binary.Write(writer, binary.BigEndian, SnapshotMagic); error != nil {
		return error
	}
	if error := binary.Write(writer, binary.BigEndian, generation); error != nil {
		return error
	}
	if error := binary.Write(writer, binary.BigEndian, int32(len(state.Tables))); error != nil {
		return error
	}

	for _, tableState := range state.Tables {
		if error := writeString(writer, tableState.TableName); error != nil {
			return error
		}
		if error := writeString(writer, tableState.IdType.String()); error != nil {
			return error
		}
		if error := writeString(writer, tableState.EntityType.String()); error != nil {
			return error
		}

		if error := binary.Write(writer, binary.BigEndian, int32(len(tableState.IndexMetadataList))); error != nil {
			return error
		}
		for _, indexMetadata := range tableState.IndexMetadataList {
			if error := writeString(writer, indexMetadata.IndexName); error != nil {
				return error
			}
			if error := writeString(writer, indexMetadata.FieldName); error != nil {
				return error
			}
			if error := binary.Write(writer, binary.BigEndian, indexMetadata.Unique); error != nil {
				return error
			}
			if error := binary.Write(writer, binary.BigEndian, indexMetadata.Ordered); error != nil {
				return error
			}
		}

		tableStorageValue, error := database.getTableStorage(tableState.TableName)
		if error != nil {
			return error
		}

		if error := binary.Write(writer, binary.BigEndian, int32(len(tableState.RecordPointers))); error != nil {
			return error
		}
		for _, recordPointer := range tableState.RecordPointers {
			recordBytes, error := tableStorageValue.ReadRecord(recordPointer)
			if error != nil {
				return error
			}
			if error := binary.Write(writer, binary.BigEndian, int32(len(recordBytes))); error != nil {
				return error
			}
			if _, error := writer.Write(recordBytes); error != nil {
				return error
			}
		}
	}

	checksum := hash.Sum32()
	if error := binary.Write(snapshotFile, binary.BigEndian, int64(checksum)); error != nil {
		return error
	}

	if error := snapshotFile.Sync(); error != nil {
		return error
	}
	if error := snapshotFile.Close(); error != nil {
		return error
	}
	snapshotFile = nil

	return os.Rename(temporaryPath, finalPath)
}

func readSnapshot(directory string, database *Database) (*DatabaseState, error) {
	snapshotPath, _, error := findLatestSnapshot(directory)
	if error != nil || snapshotPath == "" {
		return nil, nil
	}

	snapshotFile, error := os.Open(snapshotPath)
	if error != nil {
		return nil, error
	}
	defer snapshotFile.Close()

	fileInfo, error := snapshotFile.Stat()
	if error != nil {
		return nil, error
	}
	if fileInfo.Size() < 20 {
		return nil, fmt.Errorf("corrupt snapshot: file too small")
	}

	dataLength := fileInfo.Size() - 8
	limitReader := io.LimitReader(snapshotFile, dataLength)

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	hash := crc32.New(crcTable)
	teeReader := io.TeeReader(limitReader, hash)

	var magic int32
	if error := binary.Read(teeReader, binary.BigEndian, &magic); error != nil {
		return nil, error
	}
	if magic != SnapshotMagic && magic != 0x524d534e {
		return nil, fmt.Errorf("corrupt snapshot: magic mismatch")
	}

	var snapshotGen int64
	if error := binary.Read(teeReader, binary.BigEndian, &snapshotGen); error != nil {
		return nil, error
	}
	var tableCount int32
	if error := binary.Read(teeReader, binary.BigEndian, &tableCount); error != nil {
		return nil, error
	}
	if tableCount < 0 || tableCount > 10000 {
		return nil, fmt.Errorf("corrupt snapshot: invalid table count %d", tableCount)
	}

	type tableSnapshotData struct {
		tableName   string
		idType      reflect.Type
		entityType  reflect.Type
		indexMetas  []IndexMetadata
		recordBytes [][]byte
	}

	var tableSnapshots []tableSnapshotData

	for tableIndex := 0; tableIndex < int(tableCount); tableIndex++ {
		tableName, error := readString(teeReader)
		if error != nil {
			return nil, error
		}
		idClassName, error := readString(teeReader)
		if error != nil {
			return nil, error
		}
		entityClassName, error := readString(teeReader)
		if error != nil {
			return nil, error
		}

		entityType := getRegisteredType(entityClassName)
		if entityType == nil {
			return nil, fmt.Errorf("type %s not registered in options", entityClassName)
		}
		idType := getRegisteredType(idClassName)
		if idType == nil {
			idType = reflect.TypeOf("")
		}

		var indexCount int32
		if error := binary.Read(teeReader, binary.BigEndian, &indexCount); error != nil {
			return nil, error
		}
		if indexCount < 0 || indexCount > 1000 {
			return nil, fmt.Errorf("corrupt snapshot: invalid index count %d", indexCount)
		}

		var indexMetas []IndexMetadata
		for index := 0; index < int(indexCount); index++ {
			indexName, error := readString(teeReader)
			if error != nil {
				return nil, error
			}
			fieldName, error := readString(teeReader)
			if error != nil {
				return nil, error
			}
			var unique bool
			if error := binary.Read(teeReader, binary.BigEndian, &unique); error != nil {
				return nil, error
			}
			var ordered bool
			if error := binary.Read(teeReader, binary.BigEndian, &ordered); error != nil {
				return nil, error
			}
			indexMetas = append(indexMetas, IndexMetadata{
				IndexName: indexName,
				FieldName: fieldName,
				Unique:    unique,
				Ordered:   ordered,
			})
		}

		var recordCount int32
		if error := binary.Read(teeReader, binary.BigEndian, &recordCount); error != nil {
			return nil, error
		}
		if recordCount < 0 {
			return nil, fmt.Errorf("corrupt snapshot: invalid record count %d", recordCount)
		}

		var records [][]byte
		for recordIndex := 0; recordIndex < int(recordCount); recordIndex++ {
			var recordLength int32
			if error := binary.Read(teeReader, binary.BigEndian, &recordLength); error != nil {
				return nil, error
			}
			if recordLength < 0 || recordLength > 1024*1024*64 {
				return nil, fmt.Errorf("corrupt snapshot: invalid record length %d", recordLength)
			}
			recordBytes := make([]byte, recordLength)
			if _, error := io.ReadFull(teeReader, recordBytes); error != nil {
				return nil, error
			}
			// Dry-run decode/validation
			newRecordValue := reflect.New(entityType)
			if error := Unmarshal(recordBytes, newRecordValue.Interface()); error != nil {
				return nil, error
			}
			records = append(records, recordBytes)
		}

		tableSnapshots = append(tableSnapshots, tableSnapshotData{
			tableName:   tableName,
			idType:      idType,
			entityType:  entityType,
			indexMetas:  indexMetas,
			recordBytes: records,
		})
	}

	var buffer [1024]byte
	for {
		_, error := teeReader.Read(buffer[:])
		if error == io.EOF {
			break
		}
		if error != nil {
			return nil, error
		}
	}

	if _, error := snapshotFile.Seek(dataLength, io.SeekStart); error != nil {
		return nil, error
	}
	var fileChecksum int64
	if error := binary.Read(snapshotFile, binary.BigEndian, &fileChecksum); error != nil {
		return nil, error
	}

	computedChecksum := int64(hash.Sum32())
	if computedChecksum != fileChecksum {
		return nil, fmt.Errorf("corrupt snapshot: checksum failure")
	}

	// ONLY AFTER checksum and metadata validation succeeds, we overwrite table storage files
	var renamedTables []string
	var databaseState *DatabaseState
	var restoreSuccess bool

	defer func() {
		if !restoreSuccess {
			// Rollback: Close and remove newly created files, restore .old files
			for _, snap := range tableSnapshots {
				database.tableStorageMap.Delete(snap.tableName)
			}
			for _, tableName := range renamedTables {
				oldPath := filepath.Join(directory, tableName+".db.old")
				newPath := filepath.Join(directory, tableName+".db")
				_ = os.Remove(newPath)
				_ = os.Rename(oldPath, newPath)
			}
		} else {
			// Success: Clean up .old files
			for _, tableName := range renamedTables {
				oldPath := filepath.Join(directory, tableName+".db.old")
				_ = os.Remove(oldPath)
			}
		}
	}()

	databaseState = NewDatabaseState(snapshotGen)
	for _, snap := range tableSnapshots {
		storagePath := filepath.Join(directory, snap.tableName+".db")
		oldPath := filepath.Join(directory, snap.tableName+".db.old")
		if _, err := os.Stat(storagePath); err == nil {
			_ = os.Remove(oldPath)
			if err := os.Rename(storagePath, oldPath); err == nil {
				renamedTables = append(renamedTables, snap.tableName)
			}
		}
	}

	for _, snap := range tableSnapshots {
		tableStorageValue, error := database.getTableStorage(snap.tableName)
		if error != nil {
			return nil, error
		}

		tableState := NewTableState(snap.tableName, snap.idType, snap.entityType, snap.indexMetas)

		for _, recordBytes := range snap.recordBytes {
			newRecordValue := reflect.New(snap.entityType)
			if error := Unmarshal(recordBytes, newRecordValue.Interface()); error != nil {
				return nil, error
			}
			record := newRecordValue.Elem().Interface()

			recordPointer, error := tableStorageValue.AppendRecord(recordBytes)
			if error != nil {
				return nil, error
			}
			tableState.Insert(record, recordPointer)
		}

		databaseState.Tables[snap.tableName] = tableState
	}

	restoreSuccess = true
	return databaseState, nil
}

func findLatestSnapshot(directory string) (string, int64, error) {
	files, error := os.ReadDir(directory)
	if error != nil {
		if os.IsNotExist(error) {
			return "", 0, nil
		}
		return "", 0, error
	}

	var latestPath string
	var maxGeneration int64 = -1

	for _, fileEntry := range files {
		name := fileEntry.Name()
		if strings.HasPrefix(name, "snapshot.") && !strings.HasSuffix(name, ".tmp") {
			var generation int64
			_, error := fmt.Sscanf(name, "snapshot.%d", &generation)
			if error == nil {
				if generation > maxGeneration {
					maxGeneration = generation
					latestPath = filepath.Join(directory, name)
				}
			}
		}
	}

	if maxGeneration == -1 {
		return "", 0, nil
	}
	return latestPath, maxGeneration, nil
}

func (database *Database) ExportJSON(destinationPath string) error {
	committed := database.getCommittedState()
	jsonDb := make(map[string][]any)

	for tableName, tableState := range committed.Tables {
		tableStorageValue, error := database.getTableStorage(tableName)
		if error != nil {
			return error
		}

		var records []any
		for _, recordPointer := range tableState.RecordPointers {
			bytesValue, error := tableStorageValue.ReadRecord(recordPointer)
			if error != nil {
				return error
			}
			newRecordValue := reflect.New(tableState.EntityType)
			if error := Unmarshal(bytesValue, newRecordValue.Interface()); error != nil {
				return error
			}
			records = append(records, newRecordValue.Elem().Interface())
		}
		jsonDb[tableName] = records
	}

	importExportJSON, error := json.MarshalIndent(jsonDb, "", "  ")
	if error != nil {
		return error
	}

	return os.WriteFile(destinationPath, importExportJSON, 0644)
}

func (database *Database) ImportJSON(sourcePath string) error {
	data, readError := os.ReadFile(sourcePath)
	if readError != nil {
		return readError
	}

	var rawMap map[string][]json.RawMessage
	if error := json.Unmarshal(data, &rawMap); error != nil {
		return error
	}

	return database.Transaction(func(transaction *Transaction) error {
		for tableName, jsonRows := range rawMap {
			committedTable := transaction.committedState.Tables[tableName]
			if committedTable == nil {
				continue
			}

			for _, rawJsonRow := range jsonRows {
				newRecordValue := reflect.New(committedTable.EntityType)
				if error := json.Unmarshal(rawJsonRow, newRecordValue.Interface()); error != nil {
					return error
				}
				
				error := transaction.InsertDynamic(tableName, newRecordValue.Elem().Interface())
				if error != nil {
					return error
				}
			}
		}
		return nil
	})
}

func (database *Database) Backup(backupDirectory string) error {
	if err := database.lock(context.Background()); err != nil {
		return err
	}
	defer database.unlock()

	if database.closed.Load() {
		return DatabaseClosedError
	}

	if error := os.MkdirAll(backupDirectory, 0755); error != nil {
		return fmt.Errorf("failed to create backup directory: %w", error)
	}

	walPath := filepath.Join(database.directory, "wal.log")
	if _, error := os.Stat(walPath); error == nil {
		destinationWalPath := filepath.Join(backupDirectory, "wal.log")
		if error := copyFile(walPath, destinationWalPath); error != nil {
			return fmt.Errorf("failed to backup WAL log: %w", error)
		}
	}

	committed := database.getCommittedState()
	for tableName := range committed.Tables {
		sourcePath := filepath.Join(database.directory, tableName+".db")
		if _, error := os.Stat(sourcePath); error == nil {
			destinationPath := filepath.Join(backupDirectory, tableName+".db")
			if error := copyFile(sourcePath, destinationPath); error != nil {
				return fmt.Errorf("failed to backup table storage for %s: %w", tableName, error)
			}
		}
	}

	snapshotPath, _, error := findLatestSnapshot(database.directory)
	if error == nil && snapshotPath != "" {
		destinationSnapshotPath := filepath.Join(backupDirectory, filepath.Base(snapshotPath))
		if error := copyFile(snapshotPath, destinationSnapshotPath); error != nil {
			return fmt.Errorf("failed to backup latest snapshot: %w", error)
		}
	}

	return nil
}

func copyFile(src, dst string) error {
	in, error := os.Open(src)
	if error != nil {
		return error
	}
	defer in.Close()

	out, error := os.Create(dst)
	if error != nil {
		return error
	}
	defer out.Close()

	if _, error = io.Copy(out, in); error != nil {
		return error
	}
	return out.Sync()
}

func getGoroutineID() int64 {
	var buffer [64]byte
	bytesWritten := runtime.Stack(buffer[:], false)
	stackSlice := buffer[:bytesWritten]
	prefix := []byte("goroutine ")
	if !bytes.HasPrefix(stackSlice, prefix) {
		return 0
	}
	stackSlice = stackSlice[len(prefix):]
	endIndex := bytes.IndexByte(stackSlice, ' ')
	if endIndex == -1 {
		return 0
	}
	id, error := strconv.ParseInt(string(stackSlice[:endIndex]), 10, 64)
	if error != nil {
		return 0
	}
	return id
}

func validateSchema(idType reflect.Type, entityType reflect.Type) error {
	if entityType == nil {
		return fmt.Errorf("%w: entity type must be a struct, got nil", IncompatibleTypesError)
	}
	if idType == nil {
		return fmt.Errorf("%w: ID type must not be nil", IncompatibleTypesError)
	}
	if entityType.Kind() == reflect.Ptr {
		return fmt.Errorf("%w: pointer entity types are not supported", IncompatibleTypesError)
	}
	if entityType.Kind() != reflect.Struct {
		return fmt.Errorf("%w: entity type must be a struct, got %s", IncompatibleTypesError, entityType.Kind().String())
	}
	if idType.Kind() == reflect.Ptr {
		return fmt.Errorf("%w: pointer ID types are not supported", IncompatibleTypesError)
	}

	// Scan primary key ID fields in entity
	idFieldCount := 0
	var actualIdType reflect.Type
	for index := 0; index < entityType.NumField(); index++ {
		structField := entityType.Field(index)
		if structField.PkgPath != "" {
			continue // unexported
		}
		tag := structField.Tag.Get("keeper")
		parts := strings.Split(tag, ",")
		isID := false
		if len(parts) > 0 && strings.TrimSpace(parts[0]) == "id" {
			isID = true
		}
		for _, part := range parts {
			if strings.TrimSpace(part) == "id" {
				isID = true
			}
		}
		if isID || strings.ToLower(structField.Name) == "id" {
			idFieldCount++
			actualIdType = structField.Type
		}
	}

	if idFieldCount == 0 {
		return fmt.Errorf("%w: entity must have exactly one ID field (tagged with `keeper:\"id\"` or named 'id')", IncompatibleTypesError)
	}
	if idFieldCount > 1 {
		return fmt.Errorf("%w: entity has multiple ID fields, exactly one is required", IncompatibleTypesError)
	}

	if idType != actualIdType {
		return fmt.Errorf("%w: generic ID type %s does not match entity ID field type %s", IncompatibleTypesError, idType.String(), actualIdType.String())
	}

	return nil
}

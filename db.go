package masterkeeper

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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
	writeLock            sync.Mutex
	committedState       atomic.Pointer[DatabaseState]
	tableMetadataMap     sync.Map // tableName -> TableMetadata
	tableStorageMap      sync.Map // tableName -> *TableStorage
	closed               bool
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
	}

	// Resolve table storage resolver function
	tableStorageResolver := func(tableName string) (*TableStorage, error) {
		return database.getTableStorage(tableName)
	}

	// 1. Read Snapshot
	snapshotState, err := readSnapshot(directory, database)
	if err != nil {
		return nil, fmt.Errorf("database snapshot read failed: %w", err)
	}

	if snapshotState == nil {
		snapshotState = NewDatabaseState(0)
	}

	// 2. Open WAL Manager
	walManagerVal, err := NewWalManager(directory, options.Durability, tableStorageResolver)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	database.walManager = walManagerVal

	// 3. Read and Replay WAL
	walRecords, err := walManagerVal.ReadAllRecords()
	if err != nil {
		walManagerVal.Close()
		return nil, fmt.Errorf("failed to read WAL records: %w", err)
	}

	recoveredState, err := database.recover(snapshotState, walRecords)
	if err != nil {
		walManagerVal.Close()
		return nil, fmt.Errorf("database recovery failed: %w", err)
	}

	database.committedState.Store(recoveredState)

	// Register metadata for recovered tables
	for _, tableStateVal := range recoveredState.Tables {
		database.tableMetadataMap.Store(tableStateVal.TableName, TableMetadata{
			TableName: tableStateVal.TableName,
			IdType:    tableStateVal.IdType,
			Type:      tableStateVal.EntityType,
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
	if database.closed {
		return nil, ErrClosed
	}

	if val, ok := database.tableStorageMap.Load(tableName); ok {
		return val.(*TableStorage), nil
	}

	tableStorageVal, err := NewTableStorage(database.directory, tableName)
	if err != nil {
		return nil, err
	}

	actual, loaded := database.tableStorageMap.LoadOrStore(tableName, tableStorageVal)
	if loaded {
		tableStorageVal.Close()
		return actual.(*TableStorage), nil
	}

	return tableStorageVal, nil
}

func (database *Database) releaseWriterLock() {
	database.writeLock.Unlock()
}

func (database *Database) publish(nextState *DatabaseState) {
	database.committedState.Store(nextState)
}

func (database *Database) registerTableMetadata(tableName string, idType reflect.Type, entityType reflect.Type) error {
	if _, found := database.tableMetadataMap.Load(tableName); found {
		return nil
	}

	database.writeLock.Lock()
	defer database.writeLock.Unlock()

	if database.closed {
		return ErrClosed
	}

	// Double check
	if _, found := database.tableMetadataMap.Load(tableName); found {
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

	nextGen := database.currentGeneration() + 1
	var walRecords []WalRecord

	// CREATE TABLE WAL record
	var buffer bytes.Buffer
	_ = writeString(&buffer, tableName)
	_ = writeString(&buffer, idType.String())
	_ = writeString(&buffer, entityType.String())
	walRecords = append(walRecords, WalRecord{
		Type:          OpCreateTable,
		TransactionID: database.databaseID,
		Generation:    nextGen,
		Payload:       buffer.Bytes(),
	})

	// CREATE INDEX WAL records
	for _, indexMetadata := range indexMetadataList {
		var idxBuf bytes.Buffer
		_ = writeString(&idxBuf, tableName)
		_ = writeString(&idxBuf, indexMetadata.IndexName)
		_ = binary.Write(&idxBuf, binary.BigEndian, indexMetadata.Unique)
		_ = binary.Write(&idxBuf, binary.BigEndian, indexMetadata.Ordered)

		walRecords = append(walRecords, WalRecord{
			Type:          OpCreateIndex,
			TransactionID: database.databaseID,
			Generation:    nextGen,
			Payload:       idxBuf.Bytes(),
		})
	}

	// Submit schema transaction to background writer
	doneChan := make(chan WriteResult, 1)
	database.walManager.Submit(&WriteTask{
		TxID:         database.databaseID,
		Generation:   nextGen,
		WalRecords:   walRecords,
		TableAppends: nil,
		Done:         doneChan,
	})

	res := <-doneChan
	if res.Err != nil {
		return fmt.Errorf("schema modification failed: %w", res.Err)
	}

	// Publish state update
	newTableState := NewTableState(tableName, idType, entityType, indexMetadataList)
	nextState := database.getCommittedState().Copy(nextGen)
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
	database.writeLock.Lock()
	defer database.writeLock.Unlock()

	if database.closed {
		return false, ErrClosed
	}

	committed := database.getCommittedState()
	if _, found := committed.Tables[tableName]; !found {
		return false, nil
	}

	nextGen := database.currentGeneration() + 1
	var buffer bytes.Buffer
	_ = writeString(&buffer, tableName)

	doneChan := make(chan WriteResult, 1)
	database.walManager.Submit(&WriteTask{
		TxID:       database.databaseID,
		Generation: nextGen,
		WalRecords: []WalRecord{
			{
				Type:          OpDropTable,
				TransactionID: database.databaseID,
				Generation:    nextGen,
				Payload:       buffer.Bytes(),
			},
		},
		TableAppends: nil,
		Done:         doneChan,
	})

	res := <-doneChan
	if res.Err != nil {
		return false, res.Err
	}

	nextState := committed.Copy(nextGen)
	delete(nextState.Tables, tableName)
	database.publish(nextState)

	database.tableMetadataMap.Delete(tableName)

	// Close and delete table file
	if val, ok := database.tableStorageMap.LoadAndDelete(tableName); ok {
		tableStorageVal := val.(*TableStorage)
		tableStorageVal.Close()
	}
	_ = os.Remove(filepath.Join(database.directory, tableName+".db"))

	return true, nil
}

func (database *Database) Transaction(callback func(transaction *Transaction) error) error {
	database.writeLock.Lock()
	if database.closed {
		database.writeLock.Unlock()
		return ErrClosed
	}
	txID := rand.Int63()
	transaction := NewTransaction(txID, database, database.getCommittedState())

	defer func() {
		if transaction.IsActive() {
			_ = transaction.Rollback()
		}
	}()

	err := callback(transaction)
	if err != nil {
		// Rollback on callback error
		if transaction.IsActive() {
			_ = transaction.Rollback()
		}
		return err
	}

	if transaction.IsActive() {
		return transaction.Commit()
	}

	return nil
}

func (database *Database) Close() error {
	database.writeLock.Lock()
	defer database.writeLock.Unlock()

	if database.closed {
		return nil
	}
	database.closed = true

	database.tableStorageMap.Range(func(key, value any) bool {
		tableStorageVal := value.(*TableStorage)
		tableStorageVal.Close()
		return true
	})

	return database.walManager.Close()
}

func (database *Database) Compact() error {
	database.writeLock.Lock()
	defer database.writeLock.Unlock()

	if database.closed {
		return ErrClosed
	}

	committed := database.getCommittedState()
	// 1. Write snapshot
	if err := writeSnapshot(database.directory, committed, database); err != nil {
		return fmt.Errorf("SNAPSHOT failed during compaction: %w", err)
	}

	// 2. Compact table files
	for tableName, tableStateVal := range committed.Tables {
		tableStorageVal, err := database.getTableStorage(tableName)
		if err != nil {
			return err
		}
		if err := tableStorageVal.Compact(tableStateVal.RecordPointers); err != nil {
			return fmt.Errorf("compaction of table %s failed: %w", tableName, err)
		}
	}

	// 3. Truncate WAL log
	if err := database.walManager.Truncate(); err != nil {
		return fmt.Errorf("WAL truncate failed: %w", err)
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

	txGroups := make(map[int64][]WalRecord)
	var committedTxIDs []int64
	rolledBackTxIDs := make(map[int64]struct{})

	for _, walRecord := range walRecords {
		txGroups[walRecord.TransactionID] = append(txGroups[walRecord.TransactionID], walRecord)
		if walRecord.Type == OpCommitTransaction {
			committedTxIDs = append(committedTxIDs, walRecord.TransactionID)
		} else if walRecord.Type == OpRollbackTransaction {
			rolledBackTxIDs[walRecord.TransactionID] = struct{}{}
		}
	}

	// Filter out rolled back or incomplete transactions
	var activeCommits []int64
	for _, txID := range committedTxIDs {
		if _, rolled := rolledBackTxIDs[txID]; !rolled {
			activeCommits = append(activeCommits, txID)
		}
	}

	// Sort active commits by generation
	sort.Slice(activeCommits, func(indexLeft, indexRight int) bool {
		recordsLeft := txGroups[activeCommits[indexLeft]]
		recordsRight := txGroups[activeCommits[indexRight]]
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

	for _, txID := range activeCommits {
		walRecordsGroup := txGroups[txID]
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
				tableName, err := readString(payloadReader)
				if err != nil {
					return nil, err
				}
				entityClassName, err := readString(payloadReader)
				if err != nil {
					return nil, err
				}
				var payloadLength int32
				if err := binary.Read(payloadReader, binary.BigEndian, &payloadLength); err != nil {
					return nil, err
				}
				recordBytes := make([]byte, payloadLength)
				if _, err := io.ReadFull(payloadReader, recordBytes); err != nil {
					return nil, err
				}

				entityType := getRegisteredType(entityClassName)
				if entityType == nil {
					return nil, fmt.Errorf("recovery failed: type %s not registered", entityClassName)
				}

				newRecordValue := reflect.New(entityType)
				if err := Unmarshal(recordBytes, newRecordValue.Interface()); err != nil {
					return nil, err
				}
				record := newRecordValue.Elem().Interface()

				tableState := databaseState.Tables[tableName]
				if tableState == nil {
					// Table creation fallback if not in state
					tableState = NewTableState(tableName, reflect.TypeOf(""), entityType, nil)
					databaseState.Tables[tableName] = tableState
				}

				tableStorageVal, err := database.getTableStorage(tableName)
				if err != nil {
					return nil, err
				}

				id := getPrimaryKey(record)
				oldRecordPointer, found := tableState.RecordPointers[id]
				var oldRecord any
				if found {
					oldRecordBytes, err := tableStorageVal.ReadRecord(oldRecordPointer)
					if err == nil {
						oldRecordReflectValue := reflect.New(entityType)
						if Unmarshal(oldRecordBytes, oldRecordReflectValue.Interface()) == nil {
							oldRecord = oldRecordReflectValue.Elem().Interface()
						}
					}
				}

				recordPointer, err := tableStorageVal.AppendRecord(recordBytes)
				if err != nil {
					return nil, err
				}

				if walRecord.Type == OpInsert {
					tableState.Insert(record, recordPointer)
				} else {
					tableState.Update(record, oldRecord, recordPointer)
				}

			case OpDelete:
				tableName, err := readString(payloadReader)
				if err != nil {
					return nil, err
				}
				_, err = readString(payloadReader) // skip idClassName
				if err != nil {
					return nil, err
				}
				primaryKey, err := readValue(payloadReader)
				if err != nil {
					return nil, err
				}

				tableState := databaseState.Tables[tableName]
				if tableState != nil {
					tableStorageVal, err := database.getTableStorage(tableName)
					if err != nil {
						return nil, err
					}

					oldRecordPointer, found := tableState.RecordPointers[primaryKey]
					var oldRecord any
					if found {
						oldRecordBytes, err := tableStorageVal.ReadRecord(oldRecordPointer)
						if err == nil {
							oldRecordReflectValue := reflect.New(tableState.EntityType)
							if Unmarshal(oldRecordBytes, oldRecordReflectValue.Interface()) == nil {
								oldRecord = oldRecordReflectValue.Elem().Interface()
							}
						}
					}
					tableState.Delete(primaryKey, oldRecord)
				}

			case OpClearTable:
				tableName, err := readString(payloadReader)
				if err != nil {
					return nil, err
				}
				tableState := databaseState.Tables[tableName]
				if tableState != nil {
					tableState.Clear()
				}

			case OpCreateTable:
				tableName, err := readString(payloadReader)
				if err != nil {
					return nil, err
				}
				idClassName, err := readString(payloadReader)
				if err != nil {
					return nil, err
				}
				entityClassName, err := readString(payloadReader)
				if err != nil {
					return nil, err
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
				tableName, err := readString(payloadReader)
				if err != nil {
					return nil, err
				}
				delete(databaseState.Tables, tableName)

			case OpCreateIndex:
				tableName, err := readString(payloadReader)
				if err != nil {
					return nil, err
				}
				indexName, err := readString(payloadReader)
				if err != nil {
					return nil, err
				}
				var unique bool
				if err := binary.Read(payloadReader, binary.BigEndian, &unique); err != nil {
					return nil, err
				}
				var ordered bool
				if err := binary.Read(payloadReader, binary.BigEndian, &ordered); err != nil {
					return nil, err
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
					tableStorageVal, err := database.getTableStorage(tableName)
					if err != nil {
						return nil, err
					}

					for primaryKey, recordPointer := range tableState.RecordPointers {
						recordBytes, err := tableStorageVal.ReadRecord(recordPointer)
						if err == nil {
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
				tableName, err := readString(payloadReader)
				if err != nil {
					return nil, err
				}
				indexName, err := readString(payloadReader)
				if err != nil {
					return nil, err
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
		for keyName, valType := range typeRegistry.m {
			if strings.EqualFold(keyName, name) {
				return valType
			}
		}
	}
	return reflectType
}

func writeSnapshot(directory string, state *DatabaseState, database *Database) error {
	generation := state.Generation
	tmpPath := filepath.Join(directory, fmt.Sprintf("snapshot.%d.tmp", generation))
	finalPath := filepath.Join(directory, fmt.Sprintf("snapshot.%d", generation))

	snapshotFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if snapshotFile != nil {
			snapshotFile.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	hash := crc32.New(crcTable)
	writer := io.MultiWriter(snapshotFile, hash)

	if err := binary.Write(writer, binary.BigEndian, SnapshotMagic); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, generation); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, int32(len(state.Tables))); err != nil {
		return err
	}

	for _, tableState := range state.Tables {
		if err := writeString(writer, tableState.TableName); err != nil {
			return err
		}
		if err := writeString(writer, tableState.IdType.String()); err != nil {
			return err
		}
		if err := writeString(writer, tableState.EntityType.String()); err != nil {
			return err
		}

		if err := binary.Write(writer, binary.BigEndian, int32(len(tableState.IndexMetadataList))); err != nil {
			return err
		}
		for _, indexMetadata := range tableState.IndexMetadataList {
			if err := writeString(writer, indexMetadata.IndexName); err != nil {
				return err
			}
			if err := writeString(writer, indexMetadata.FieldName); err != nil {
				return err
			}
			if err := binary.Write(writer, binary.BigEndian, indexMetadata.Unique); err != nil {
				return err
			}
			if err := binary.Write(writer, binary.BigEndian, indexMetadata.Ordered); err != nil {
				return err
			}
		}

		tableStorageVal, err := database.getTableStorage(tableState.TableName)
		if err != nil {
			return err
		}

		if err := binary.Write(writer, binary.BigEndian, int32(len(tableState.RecordPointers))); err != nil {
			return err
		}
		for _, recordPointer := range tableState.RecordPointers {
			recordBytes, err := tableStorageVal.ReadRecord(recordPointer)
			if err != nil {
				return err
			}
			if err := binary.Write(writer, binary.BigEndian, int32(len(recordBytes))); err != nil {
				return err
			}
			if _, err := writer.Write(recordBytes); err != nil {
				return err
			}
		}
	}

	checksum := hash.Sum32()
	if err := binary.Write(snapshotFile, binary.BigEndian, int64(checksum)); err != nil {
		return err
	}

	if err := snapshotFile.Sync(); err != nil {
		return err
	}
	if err := snapshotFile.Close(); err != nil {
		return err
	}
	snapshotFile = nil

	return os.Rename(tmpPath, finalPath)
}

func readSnapshot(directory string, database *Database) (*DatabaseState, error) {
	snapshotPath, _, err := findLatestSnapshot(directory)
	if err != nil || snapshotPath == "" {
		return nil, nil
	}

	snapshotFile, err := os.Open(snapshotPath)
	if err != nil {
		return nil, err
	}
	defer snapshotFile.Close()

	fileInfo, err := snapshotFile.Stat()
	if err != nil {
		return nil, err
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
	if err := binary.Read(teeReader, binary.BigEndian, &magic); err != nil {
		return nil, err
	}
	if magic != SnapshotMagic {
		return nil, fmt.Errorf("corrupt snapshot: magic mismatch")
	}

	var snapshotGen int64
	if err := binary.Read(teeReader, binary.BigEndian, &snapshotGen); err != nil {
		return nil, err
	}
	var tableCount int32
	if err := binary.Read(teeReader, binary.BigEndian, &tableCount); err != nil {
		return nil, err
	}

	databaseState := NewDatabaseState(snapshotGen)

	for tableIndex := 0; tableIndex < int(tableCount); tableIndex++ {
		tableName, err := readString(teeReader)
		if err != nil {
			return nil, err
		}
		idClassName, err := readString(teeReader)
		if err != nil {
			return nil, err
		}
		entityClassName, err := readString(teeReader)
		if err != nil {
			return nil, err
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
		if err := binary.Read(teeReader, binary.BigEndian, &indexCount); err != nil {
			return nil, err
		}

		var indexMetas []IndexMetadata
		for index := 0; index < int(indexCount); index++ {
			indexName, err := readString(teeReader)
			if err != nil {
				return nil, err
			}
			fieldName, err := readString(teeReader)
			if err != nil {
				return nil, err
			}
			var unique bool
			if err := binary.Read(teeReader, binary.BigEndian, &unique); err != nil {
				return nil, err
			}
			var ordered bool
			if err := binary.Read(teeReader, binary.BigEndian, &ordered); err != nil {
				return nil, err
			}
			indexMetas = append(indexMetas, IndexMetadata{
				IndexName: indexName,
				FieldName: fieldName,
				Unique:    unique,
				Ordered:   ordered,
			})
		}

		tableState := NewTableState(tableName, idType, entityType, indexMetas)

		var recordCount int32
		if err := binary.Read(teeReader, binary.BigEndian, &recordCount); err != nil {
			return nil, err
		}

		storagePath := filepath.Join(directory, tableName+".db")
		_ = os.Remove(storagePath)
		tableStorageVal, err := database.getTableStorage(tableName)
		if err != nil {
			return nil, err
		}

		for recordIndex := 0; recordIndex < int(recordCount); recordIndex++ {
			var recordLength int32
			if err := binary.Read(teeReader, binary.BigEndian, &recordLength); err != nil {
				return nil, err
			}
			recordBytes := make([]byte, recordLength)
			if _, err := io.ReadFull(teeReader, recordBytes); err != nil {
				return nil, err
			}

			newRecordValue := reflect.New(entityType)
			if err := Unmarshal(recordBytes, newRecordValue.Interface()); err != nil {
				return nil, err
			}
			record := newRecordValue.Elem().Interface()

			recordPointer, err := tableStorageVal.AppendRecord(recordBytes)
			if err != nil {
				return nil, err
			}
			tableState.Insert(record, recordPointer)
		}

		databaseState.Tables[tableName] = tableState
	}

	var buffer [1024]byte
	for {
		_, err := teeReader.Read(buffer[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	if _, err := snapshotFile.Seek(dataLength, io.SeekStart); err != nil {
		return nil, err
	}
	var fileChecksum int64
	if err := binary.Read(snapshotFile, binary.BigEndian, &fileChecksum); err != nil {
		return nil, err
	}

	computedChecksum := int64(hash.Sum32())
	if computedChecksum != fileChecksum {
		return nil, fmt.Errorf("corrupt snapshot: checksum failure")
	}

	return databaseState, nil
}

func findLatestSnapshot(directory string) (string, int64, error) {
	files, err := os.ReadDir(directory)
	if err != nil {
		if os.IsNotExist(err) {
			return "", 0, nil
		}
		return "", 0, err
	}

	var latestPath string
	var maxGeneration int64 = -1

	for _, fileEntry := range files {
		name := fileEntry.Name()
		if strings.HasPrefix(name, "snapshot.") && !strings.HasSuffix(name, ".tmp") {
			var generation int64
			_, err := fmt.Sscanf(name, "snapshot.%d", &generation)
			if err == nil {
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
		tableStorageVal, err := database.getTableStorage(tableName)
		if err != nil {
			return err
		}

		var records []any
		for _, recordPointer := range tableState.RecordPointers {
			bytesValue, err := tableStorageVal.ReadRecord(recordPointer)
			if err != nil {
				return err
			}
			newRecordValue := reflect.New(tableState.EntityType)
			if err := Unmarshal(bytesValue, newRecordValue.Interface()); err != nil {
				return err
			}
			records = append(records, newRecordValue.Elem().Interface())
		}
		jsonDb[tableName] = records
	}

	importExportJSON, err := json.MarshalIndent(jsonDb, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(destinationPath, importExportJSON, 0644)
}

func (database *Database) ImportJSON(sourcePath string) error {
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}

	var rawMap map[string][]json.RawMessage
	if err := json.Unmarshal(data, &rawMap); err != nil {
		return err
	}

	return database.Transaction(func(transaction *Transaction) error {
		for tableName, jsonRows := range rawMap {
			committedTable := transaction.committedState.Tables[tableName]
			if committedTable == nil {
				continue
			}

			for _, rawJsonRow := range jsonRows {
				newRecordValue := reflect.New(committedTable.EntityType)
				if err := json.Unmarshal(rawJsonRow, newRecordValue.Interface()); err != nil {
					return err
				}
				
				err := transaction.InsertDynamic(tableName, newRecordValue.Elem().Interface())
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (database *Database) Backup(backupDirectory string) error {
	database.writeLock.Lock()
	defer database.writeLock.Unlock()

	if database.closed {
		return ErrClosed
	}

	if err := os.MkdirAll(backupDirectory, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	walPath := filepath.Join(database.directory, "wal.log")
	if _, err := os.Stat(walPath); err == nil {
		destWalPath := filepath.Join(backupDirectory, "wal.log")
		if err := copyFile(walPath, destWalPath); err != nil {
			return fmt.Errorf("failed to backup WAL log: %w", err)
		}
	}

	committed := database.getCommittedState()
	for tableName := range committed.Tables {
		srcPath := filepath.Join(database.directory, tableName+".db")
		if _, err := os.Stat(srcPath); err == nil {
			destPath := filepath.Join(backupDirectory, tableName+".db")
			if err := copyFile(srcPath, destPath); err != nil {
				return fmt.Errorf("failed to backup table storage for %s: %w", tableName, err)
			}
		}
	}

	snapPath, _, err := findLatestSnapshot(database.directory)
	if err == nil && snapPath != "" {
		destSnapPath := filepath.Join(backupDirectory, filepath.Base(snapPath))
		if err := copyFile(snapPath, destSnapPath); err != nil {
			return fmt.Errorf("failed to backup latest snapshot: %w", err)
		}
	}

	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

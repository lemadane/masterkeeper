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

const SnapshotMagic int32 = 0x524d534e

type Options struct {
	Durability DurabilityMode
	Types      []any
}

func DefaultOptions() Options {
	return Options{
		Durability: DurabilitySync,
	}
}

func (o *Options) RegisterTypes(types ...any) {
	for _, t := range types {
		RegisterType(t)
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

func Open(directory string, opts Options) (*Database, error) {
	// Register all types specified in options
	for _, t := range opts.Types {
		RegisterType(t)
	}

	db := &Database{
		directory:  directory,
		durability: opts.Durability,
		databaseID: rand.Int63() & 0x7fffffffffffffff,
	}

	// Resolve table storage resolver function
	tsResolver := func(tableName string) (*TableStorage, error) {
		return db.getTableStorage(tableName)
	}

	// 1. Read Snapshot
	snapState, err := readSnapshot(directory, db)
	if err != nil {
		return nil, fmt.Errorf("database snapshot read failed: %w", err)
	}

	if snapState == nil {
		snapState = NewDatabaseState(0)
	}

	// 2. Open WAL Manager
	walManager, err := NewWalManager(directory, opts.Durability, tsResolver)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	db.walManager = walManager

	// 3. Read and Replay WAL
	walRecords, err := walManager.ReadAllRecords()
	if err != nil {
		walManager.Close()
		return nil, fmt.Errorf("failed to read WAL records: %w", err)
	}

	recoveredState, err := db.recover(snapState, walRecords)
	if err != nil {
		walManager.Close()
		return nil, fmt.Errorf("database recovery failed: %w", err)
	}

	db.committedState.Store(recoveredState)

	// Register metadata for recovered tables
	for _, ts := range recoveredState.Tables {
		db.tableMetadataMap.Store(ts.TableName, TableMetadata{
			TableName: ts.TableName,
			IdType:    ts.IdType,
			Type:      ts.EntityType,
		})
	}

	return db, nil
}

func (db *Database) getCommittedState() *DatabaseState {
	return db.committedState.Load()
}

func (db *Database) currentGeneration() int64 {
	return db.getCommittedState().Generation
}

func (db *Database) getTableStorage(tableName string) (*TableStorage, error) {
	if db.closed {
		return nil, ErrClosed
	}

	if val, ok := db.tableStorageMap.Load(tableName); ok {
		return val.(*TableStorage), nil
	}

	storage, err := NewTableStorage(db.directory, tableName)
	if err != nil {
		return nil, err
	}

	actual, loaded := db.tableStorageMap.LoadOrStore(tableName, storage)
	if loaded {
		storage.Close()
		return actual.(*TableStorage), nil
	}

	return storage, nil
}

func (db *Database) releaseWriterLock() {
	db.writeLock.Unlock()
}

func (db *Database) publish(nextState *DatabaseState) {
	db.committedState.Store(nextState)
}

func (db *Database) registerTableMetadata(tableName string, idType reflect.Type, entityType reflect.Type) error {
	if _, ok := db.tableMetadataMap.Load(tableName); ok {
		return nil
	}

	db.writeLock.Lock()
	defer db.writeLock.Unlock()

	// Double check
	if _, ok := db.tableMetadataMap.Load(tableName); ok {
		return nil
	}

	RegisterType(reflect.New(entityType).Elem().Interface())
	RegisterType(reflect.New(idType).Elem().Interface())

	// Scan index tags
	var indexMetadataList []IndexMetadata
	for i := 0; i < entityType.NumField(); i++ {
		f := entityType.Field(i)
		meta := parseFieldTag(f)
		if meta.IsIndex {
			idxName := tableName + "_" + meta.FieldName + "_idx"
			indexMetadataList = append(indexMetadataList, IndexMetadata{
				IndexName: idxName,
				FieldName: meta.FieldName,
				Unique:    meta.IsUnique,
				Ordered:   meta.IsOrdered,
			})
		}
	}

	nextGen := db.currentGeneration() + 1
	var walRecords []WalRecord

	// CREATE TABLE WAL record
	var buf bytes.Buffer
	_ = writeString(&buf, tableName)
	_ = writeString(&buf, idType.String())
	_ = writeString(&buf, entityType.String())
	walRecords = append(walRecords, WalRecord{
		Type:          OpCreateTable,
		TransactionID: db.databaseID,
		Generation:    nextGen,
		Payload:       buf.Bytes(),
	})

	// CREATE INDEX WAL records
	for _, idx := range indexMetadataList {
		var idxBuf bytes.Buffer
		_ = writeString(&idxBuf, tableName)
		_ = writeString(&idxBuf, idx.IndexName)
		_ = binary.Write(&idxBuf, binary.BigEndian, idx.Unique)
		_ = binary.Write(&idxBuf, binary.BigEndian, idx.Ordered)

		walRecords = append(walRecords, WalRecord{
			Type:          OpCreateIndex,
			TransactionID: db.databaseID,
			Generation:    nextGen,
			Payload:       idxBuf.Bytes(),
		})
	}

	// Submit schema transaction to background writer
	doneChan := make(chan WriteResult, 1)
	db.walManager.Submit(&WriteTask{
		TxID:         db.databaseID,
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
	nextState := db.getCommittedState().Copy(nextGen)
	nextState.Tables[tableName] = newTableState
	db.publish(nextState)

	db.tableMetadataMap.Store(tableName, TableMetadata{
		TableName: tableName,
		IdType:    idType,
		Type:      entityType,
	})

	return nil
}

func (db *Database) TableNames() []string {
	var names []string
	for k := range db.getCommittedState().Tables {
		names = append(names, k)
	}
	return names
}

func (db *Database) ContainsTable(tableName string) bool {
	_, ok := db.getCommittedState().Tables[tableName]
	return ok
}

func (db *Database) DropTable(tableName string) (bool, error) {
	db.writeLock.Lock()
	defer db.writeLock.Unlock()

	committed := db.getCommittedState()
	if _, ok := committed.Tables[tableName]; !ok {
		return false, nil
	}

	nextGen := db.currentGeneration() + 1
	var buf bytes.Buffer
	_ = writeString(&buf, tableName)

	doneChan := make(chan WriteResult, 1)
	db.walManager.Submit(&WriteTask{
		TxID:       db.databaseID,
		Generation: nextGen,
		WalRecords: []WalRecord{
			{
				Type:          OpDropTable,
				TransactionID: db.databaseID,
				Generation:    nextGen,
				Payload:       buf.Bytes(),
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
	db.publish(nextState)

	db.tableMetadataMap.Delete(tableName)

	// Close and delete table file
	if val, ok := db.tableStorageMap.LoadAndDelete(tableName); ok {
		storage := val.(*TableStorage)
		storage.Close()
	}
	_ = os.Remove(filepath.Join(db.directory, tableName+".db"))

	return true, nil
}

func (db *Database) Transaction(fn func(tx *Transaction) error) error {
	db.writeLock.Lock()
	txID := rand.Int63()
	tx := NewTransaction(txID, db, db.getCommittedState())

	defer func() {
		if tx.IsActive() {
			_ = tx.Rollback()
		}
	}()

	err := fn(tx)
	if err != nil {
		// Rollback on callback error
		if tx.IsActive() {
			_ = tx.Rollback()
		}
		return err
	}

	if tx.IsActive() {
		return tx.Commit()
	}

	return nil
}

func (db *Database) Close() error {
	if db.closed {
		return nil
	}
	db.closed = true

	db.tableStorageMap.Range(func(key, value any) bool {
		storage := value.(*TableStorage)
		storage.Close()
		return true
	})

	return db.walManager.Close()
}

func (db *Database) Compact() error {
	db.writeLock.Lock()
	defer db.writeLock.Unlock()

	committed := db.getCommittedState()
	// 1. Write snapshot
	if err := writeSnapshot(db.directory, committed, db); err != nil {
		return fmt.Errorf("SNAPSHOT failed during compaction: %w", err)
	}

	// 2. Compact table files
	for tableName, ts := range committed.Tables {
		storage, err := db.getTableStorage(tableName)
		if err != nil {
			return err
		}
		if err := storage.Compact(ts.RecordPointers); err != nil {
			return fmt.Errorf("compaction of table %s failed: %w", tableName, err)
		}
	}

	// 3. Truncate WAL log
	if err := db.walManager.Truncate(); err != nil {
		return fmt.Errorf("WAL truncate failed: %w", err)
	}

	return nil
}

func (db *Database) recover(initialState *DatabaseState, walRecords []WalRecord) (*DatabaseState, error) {
	if len(walRecords) == 0 {
		if initialState != nil {
			return initialState, nil
		}
		return NewDatabaseState(0), nil
	}

	txGroups := make(map[int64][]WalRecord)
	var committedTxIDs []int64
	rolledBackTxIDs := make(map[int64]struct{})

	for _, rec := range walRecords {
		txGroups[rec.TransactionID] = append(txGroups[rec.TransactionID], rec)
		if rec.Type == OpCommitTransaction {
			committedTxIDs = append(committedTxIDs, rec.TransactionID)
		} else if rec.Type == OpRollbackTransaction {
			rolledBackTxIDs[rec.TransactionID] = struct{}{}
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
	sort.Slice(activeCommits, func(i, j int) bool {
		recsI := txGroups[activeCommits[i]]
		recsJ := txGroups[activeCommits[j]]
		genI := int64(0)
		genJ := int64(0)
		if len(recsI) > 0 {
			genI = recsI[0].Generation
		}
		if len(recsJ) > 0 {
			genJ = recsJ[0].Generation
		}
		return genI < genJ
	})

	var dbState *DatabaseState
	if initialState != nil {
		dbState = initialState.Copy(initialState.Generation)
	} else {
		dbState = NewDatabaseState(0)
	}
	currentGen := dbState.Generation

	for _, txID := range activeCommits {
		recs := txGroups[txID]
		for _, rec := range recs {
			if rec.Generation > currentGen {
				currentGen = rec.Generation
			}

			if rec.Type == OpBeginTransaction || rec.Type == OpCommitTransaction || rec.Type == OpRollbackTransaction {
				continue
			}

			r := bytes.NewReader(rec.Payload)
			switch rec.Type {
			case OpInsert, OpUpsert, OpUpdate:
				tableName, err := readString(r)
				if err != nil {
					return nil, err
				}
				entityClassName, err := readString(r)
				if err != nil {
					return nil, err
				}
				var lenVal int32
				if err := binary.Read(r, binary.BigEndian, &lenVal); err != nil {
					return nil, err
				}
				recBytes := make([]byte, lenVal)
				if _, err := io.ReadFull(r, recBytes); err != nil {
					return nil, err
				}

				entityType := getRegisteredType(entityClassName)
				if entityType == nil {
					return nil, fmt.Errorf("recovery failed: type %s not registered", entityClassName)
				}

				newRecordVal := reflect.New(entityType)
				if err := Unmarshal(recBytes, newRecordVal.Interface()); err != nil {
					return nil, err
				}
				record := newRecordVal.Elem().Interface()

				ts := dbState.Tables[tableName]
				if ts == nil {
					// Table creation fallback if not in state
					ts = NewTableState(tableName, reflect.TypeOf(""), entityType, nil)
					dbState.Tables[tableName] = ts
				}

				storage, err := db.getTableStorage(tableName)
				if err != nil {
					return nil, err
				}

				id := getPrimaryKey(record)
				oldPtr, ok := ts.RecordPointers[id]
				var oldRecord any
				if ok {
					oldBytes, err := storage.ReadRecord(oldPtr)
					if err == nil {
						oldVal := reflect.New(entityType)
						if Unmarshal(oldBytes, oldVal.Interface()) == nil {
							oldRecord = oldVal.Elem().Interface()
						}
					}
				}

				ptr, err := storage.AppendRecord(recBytes)
				if err != nil {
					return nil, err
				}

				if rec.Type == OpInsert {
					ts.Insert(record, ptr)
				} else {
					ts.Update(record, oldRecord, ptr)
				}

			case OpDelete:
				tableName, err := readString(r)
				if err != nil {
					return nil, err
				}
				_, err = readString(r) // skip idClassName
				if err != nil {
					return nil, err
				}
				key, err := readValue(r)
				if err != nil {
					return nil, err
				}

				ts := dbState.Tables[tableName]
				if ts != nil {
					storage, err := db.getTableStorage(tableName)
					if err != nil {
						return nil, err
					}

					oldPtr, ok := ts.RecordPointers[key]
					var oldRecord any
					if ok {
						oldBytes, err := storage.ReadRecord(oldPtr)
						if err == nil {
							oldVal := reflect.New(ts.EntityType)
							if Unmarshal(oldBytes, oldVal.Interface()) == nil {
								oldRecord = oldVal.Elem().Interface()
							}
						}
					}
					ts.Delete(key, oldRecord)
				}

			case OpClearTable:
				tableName, err := readString(r)
				if err != nil {
					return nil, err
				}
				ts := dbState.Tables[tableName]
				if ts != nil {
					ts.Clear()
				}

			case OpCreateTable:
				tableName, err := readString(r)
				if err != nil {
					return nil, err
				}
				idClassName, err := readString(r)
				if err != nil {
					return nil, err
				}
				entityClassName, err := readString(r)
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

				ts := NewTableState(tableName, idType, entityType, nil)
				dbState.Tables[tableName] = ts

			case OpDropTable:
				tableName, err := readString(r)
				if err != nil {
					return nil, err
				}
				delete(dbState.Tables, tableName)

			case OpCreateIndex:
				tableName, err := readString(r)
				if err != nil {
					return nil, err
				}
				idxName, err := readString(r)
				if err != nil {
					return nil, err
				}
				var unique bool
				if err := binary.Read(r, binary.BigEndian, &unique); err != nil {
					return nil, err
				}
				var ordered bool
				if err := binary.Read(r, binary.BigEndian, &ordered); err != nil {
					return nil, err
				}

				ts := dbState.Tables[tableName]
				if ts != nil {
					// Re-derive FieldName from IndexName
					// e.g. tableName_FieldName_idx
					prefix := tableName + "_"
					suffix := "_idx"
					fieldName := idxName
					if strings.HasPrefix(idxName, prefix) && strings.HasSuffix(idxName, suffix) {
						fieldName = idxName[len(prefix) : len(idxName)-len(suffix)]
					}

					meta := IndexMetadata{
						IndexName: idxName,
						FieldName: fieldName,
						Unique:    unique,
						Ordered:   ordered,
					}

					ts.IndexMetadataList = append(ts.IndexMetadataList, meta)
					idxState := NewIndexState(meta)
					ts.Indexes[idxName] = idxState

					// Populate
					storage, err := db.getTableStorage(tableName)
					if err != nil {
						return nil, err
					}

					for pKey, ptr := range ts.RecordPointers {
						recBytes, err := storage.ReadRecord(ptr)
						if err == nil {
							newVal := reflect.New(ts.EntityType)
							if Unmarshal(recBytes, newVal.Interface()) == nil {
								rec := newVal.Elem().Interface()
								val := getFieldValue(rec, fieldName)
								idxState.Add(val, pKey)
							}
						}
					}
				}

			case OpDropIndex:
				tableName, err := readString(r)
				if err != nil {
					return nil, err
				}
				idxName, err := readString(r)
				if err != nil {
					return nil, err
				}

				ts := dbState.Tables[tableName]
				if ts != nil {
					delete(ts.Indexes, idxName)
					var newList []IndexMetadata
					for _, m := range ts.IndexMetadataList {
						if m.IndexName != idxName {
							newList = append(newList, m)
						}
					}
					ts.IndexMetadataList = newList
				}
			}
		}
	}

	return dbState.Copy(currentGen), nil
}

type typeRegistryStruct struct {
	mu sync.RWMutex
	m  map[string]reflect.Type
}

var typeRegistry = typeRegistryStruct{
	m: make(map[string]reflect.Type),
}

func RegisterType(v any) {
	typeRegistry.mu.Lock()
	defer typeRegistry.mu.Unlock()
	t := reflect.TypeOf(v)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	typeRegistry.m[t.String()] = t
	// Also register by short name
	parts := strings.Split(t.String(), ".")
	typeRegistry.m[parts[len(parts)-1]] = t
}

func getRegisteredType(name string) reflect.Type {
	typeRegistry.mu.RLock()
	defer typeRegistry.mu.RUnlock()
	t, ok := typeRegistry.m[name]
	if !ok {
		for k, v := range typeRegistry.m {
			if strings.EqualFold(k, name) {
				return v
			}
		}
	}
	return t
}

func writeSnapshot(dir string, state *DatabaseState, db *Database) error {
	gen := state.Generation
	tmpPath := filepath.Join(dir, fmt.Sprintf("snapshot.%d.tmp", gen))
	finalPath := filepath.Join(dir, fmt.Sprintf("snapshot.%d", gen))

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if f != nil {
			f.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	hash := crc32.New(crcTable)
	w := io.MultiWriter(f, hash)

	if err := binary.Write(w, binary.BigEndian, SnapshotMagic); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, gen); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, int32(len(state.Tables))); err != nil {
		return err
	}

	for _, ts := range state.Tables {
		if err := writeString(w, ts.TableName); err != nil {
			return err
		}
		if err := writeString(w, ts.IdType.String()); err != nil {
			return err
		}
		if err := writeString(w, ts.EntityType.String()); err != nil {
			return err
		}

		if err := binary.Write(w, binary.BigEndian, int32(len(ts.IndexMetadataList))); err != nil {
			return err
		}
		for _, meta := range ts.IndexMetadataList {
			if err := writeString(w, meta.IndexName); err != nil {
				return err
			}
			if err := writeString(w, meta.FieldName); err != nil {
				return err
			}
			if err := binary.Write(w, binary.BigEndian, meta.Unique); err != nil {
				return err
			}
			if err := binary.Write(w, binary.BigEndian, meta.Ordered); err != nil {
				return err
			}
		}

		storage, err := db.getTableStorage(ts.TableName)
		if err != nil {
			return err
		}

		if err := binary.Write(w, binary.BigEndian, int32(len(ts.RecordPointers))); err != nil {
			return err
		}
		for _, ptr := range ts.RecordPointers {
			recBytes, err := storage.ReadRecord(ptr)
			if err != nil {
				return err
			}
			if err := binary.Write(w, binary.BigEndian, int32(len(recBytes))); err != nil {
				return err
			}
			if _, err := w.Write(recBytes); err != nil {
				return err
			}
		}
	}

	checksum := hash.Sum32()
	if err := binary.Write(f, binary.BigEndian, int64(checksum)); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	f = nil

	return os.Rename(tmpPath, finalPath)
}

func readSnapshot(dir string, db *Database) (*DatabaseState, error) {
	snapshotPath, _, err := findLatestSnapshot(dir)
	if err != nil || snapshotPath == "" {
		return nil, nil
	}

	f, err := os.Open(snapshotPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < 20 {
		return nil, fmt.Errorf("corrupt snapshot: file too small")
	}

	dataLen := info.Size() - 8
	r := io.LimitReader(f, dataLen)

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	hash := crc32.New(crcTable)
	tr := io.TeeReader(r, hash)

	var magic int32
	if err := binary.Read(tr, binary.BigEndian, &magic); err != nil {
		return nil, err
	}
	if magic != SnapshotMagic {
		return nil, fmt.Errorf("corrupt snapshot: magic mismatch")
	}

	var snapshotGen int64
	if err := binary.Read(tr, binary.BigEndian, &snapshotGen); err != nil {
		return nil, err
	}
	var tableCount int32
	if err := binary.Read(tr, binary.BigEndian, &tableCount); err != nil {
		return nil, err
	}

	state := NewDatabaseState(snapshotGen)

	for t := 0; t < int(tableCount); t++ {
		tableName, err := readString(tr)
		if err != nil {
			return nil, err
		}
		idClassName, err := readString(tr)
		if err != nil {
			return nil, err
		}
		entityClassName, err := readString(tr)
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
		if err := binary.Read(tr, binary.BigEndian, &indexCount); err != nil {
			return nil, err
		}

		var indexMetas []IndexMetadata
		for i := 0; i < int(indexCount); i++ {
			idxName, err := readString(tr)
			if err != nil {
				return nil, err
			}
			fieldName, err := readString(tr)
			if err != nil {
				return nil, err
			}
			var unique bool
			if err := binary.Read(tr, binary.BigEndian, &unique); err != nil {
				return nil, err
			}
			var ordered bool
			if err := binary.Read(tr, binary.BigEndian, &ordered); err != nil {
				return nil, err
			}
			indexMetas = append(indexMetas, IndexMetadata{
				IndexName: idxName,
				FieldName: fieldName,
				Unique:    unique,
				Ordered:   ordered,
			})
		}

		ts := NewTableState(tableName, idType, entityType, indexMetas)

		var recordCount int32
		if err := binary.Read(tr, binary.BigEndian, &recordCount); err != nil {
			return nil, err
		}

		storagePath := filepath.Join(dir, tableName+".db")
		_ = os.Remove(storagePath)
		storage, err := db.getTableStorage(tableName)
		if err != nil {
			return nil, err
		}

		for rIdx := 0; rIdx < int(recordCount); rIdx++ {
			var recLen int32
			if err := binary.Read(tr, binary.BigEndian, &recLen); err != nil {
				return nil, err
			}
			recBytes := make([]byte, recLen)
			if _, err := io.ReadFull(tr, recBytes); err != nil {
				return nil, err
			}

			newRecordVal := reflect.New(entityType)
			if err := Unmarshal(recBytes, newRecordVal.Interface()); err != nil {
				return nil, err
			}
			record := newRecordVal.Elem().Interface()

			ptr, err := storage.AppendRecord(recBytes)
			if err != nil {
				return nil, err
			}
			ts.Insert(record, ptr)
		}

		state.Tables[tableName] = ts
	}

	var buf [1024]byte
	for {
		_, err := tr.Read(buf[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	if _, err := f.Seek(dataLen, io.SeekStart); err != nil {
		return nil, err
	}
	var fileChecksum int64
	if err := binary.Read(f, binary.BigEndian, &fileChecksum); err != nil {
		return nil, err
	}

	computedChecksum := int64(hash.Sum32())
	if computedChecksum != fileChecksum {
		return nil, fmt.Errorf("corrupt snapshot: checksum failure")
	}

	return state, nil
}

func findLatestSnapshot(dir string) (string, int64, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", 0, nil
		}
		return "", 0, err
	}

	var latestPath string
	var maxGen int64 = -1

	for _, file := range files {
		name := file.Name()
		if strings.HasPrefix(name, "snapshot.") && !strings.HasSuffix(name, ".tmp") {
			var gen int64
			_, err := fmt.Sscanf(name, "snapshot.%d", &gen)
			if err == nil {
				if gen > maxGen {
					maxGen = gen
					latestPath = filepath.Join(dir, name)
				}
			}
		}
	}

	if maxGen == -1 {
		return "", 0, nil
	}
	return latestPath, maxGen, nil
}

func (db *Database) ExportJSON(destinationPath string) error {
	committed := db.getCommittedState()
	jsonDb := make(map[string][]any)

	for tableName, ts := range committed.Tables {
		storage, err := db.getTableStorage(tableName)
		if err != nil {
			return err
		}

		var records []any
		for _, ptr := range ts.RecordPointers {
			bytes, err := storage.ReadRecord(ptr)
			if err != nil {
				return err
			}
			newRecordVal := reflect.New(ts.EntityType)
			if err := Unmarshal(bytes, newRecordVal.Interface()); err != nil {
				return err
			}
			records = append(records, newRecordVal.Elem().Interface())
		}
		jsonDb[tableName] = records
	}

	importExportJSON, err := json.MarshalIndent(jsonDb, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(destinationPath, importExportJSON, 0644)
}

func (db *Database) ImportJSON(sourcePath string) error {
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}

	var rawMap map[string][]json.RawMessage
	if err := json.Unmarshal(data, &rawMap); err != nil {
		return err
	}

	return db.Transaction(func(tx *Transaction) error {
		for tableName, rows := range rawMap {
			committedTable := tx.committedState.Tables[tableName]
			if committedTable == nil {
				continue
			}

			for _, rowRaw := range rows {
				newRecordVal := reflect.New(committedTable.EntityType)
				if err := json.Unmarshal(rowRaw, newRecordVal.Interface()); err != nil {
					return err
				}
				
				err := tx.InsertDynamic(tableName, newRecordVal.Elem().Interface())
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (db *Database) Backup(backupDirectory string) error {
	db.writeLock.Lock()
	defer db.writeLock.Unlock()

	if err := os.MkdirAll(backupDirectory, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	walPath := filepath.Join(db.directory, "wal.log")
	if _, err := os.Stat(walPath); err == nil {
		destWalPath := filepath.Join(backupDirectory, "wal.log")
		if err := copyFile(walPath, destWalPath); err != nil {
			return fmt.Errorf("failed to backup WAL log: %w", err)
		}
	}

	committed := db.getCommittedState()
	for tableName := range committed.Tables {
		srcPath := filepath.Join(db.directory, tableName+".db")
		if _, err := os.Stat(srcPath); err == nil {
			destPath := filepath.Join(backupDirectory, tableName+".db")
			if err := copyFile(srcPath, destPath); err != nil {
				return fmt.Errorf("failed to backup table storage for %s: %w", tableName, err)
			}
		}
	}

	snapPath, _, err := findLatestSnapshot(db.directory)
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

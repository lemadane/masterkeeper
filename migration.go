package masterkeeper

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type SQLDialect int

const (
	DialectPostgreSQL SQLDialect = iota
	DialectMySQL
	DialectSQLite
	DialectMSSQL
)

// Migrate exports all tables, schemas, and records from the Database to a SQL database.
func Migrate(database *Database, sqlDatabase *sql.DB, dialect SQLDialect) error {
	if database == nil {
		return fmt.Errorf("masterkeeper database cannot be nil")
	}
	if sqlDatabase == nil {
		return fmt.Errorf("sql database connection cannot be nil")
	}

	committed := database.getCommittedState()
	for tableName, tableState := range committed.Tables {
		tableStorageValue, error := database.getTableStorage(tableName)
		if error != nil {
			return error
		}

		// 1. Reflect EntityType fields to construct column definitions
		entityType := tableState.EntityType
		if entityType.Kind() != reflect.Struct {
			return fmt.Errorf("entity type for table %s must be a struct", tableName)
		}

		var columns []string
		var quotedColumns []string
		var columnDefs []string
		var fieldNames []string

		for index := 0; index < entityType.NumField(); index++ {
			structField := entityType.Field(index)
			fieldName := getFieldName(structField)
			fieldMetadata := parseFieldTag(structField)

			colType := mapGoTypeToSQL(structField.Type, dialect)
			colDef := quoteIdentifier(fieldName, dialect) + " " + colType

			if fieldMetadata.IsID {
				colDef += " PRIMARY KEY"
			}

			columnDefs = append(columnDefs, colDef)
			columns = append(columns, fieldName)
			quotedColumns = append(quotedColumns, quoteIdentifier(fieldName, dialect))
			fieldNames = append(fieldNames, structField.Name)
		}

		// 2. Create the Table
		var createTableSQL string
		if dialect == DialectMSSQL {
			createTableSQL = fmt.Sprintf(`IF OBJECT_ID('%s', 'U') IS NULL
BEGIN
	CREATE TABLE %s (
		%s
	);
END`, tableName, quoteIdentifier(tableName, dialect), strings.Join(columnDefs, ",\n\t\t"))
		} else {
			createTableSQL = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\t%s\n)",
				quoteIdentifier(tableName, dialect),
				strings.Join(columnDefs, ",\n\t"),
			)
		}

		if _, error := sqlDatabase.Exec(createTableSQL); error != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, error)
		}

		// 3. Create Indexes
		for _, indexMetadata := range tableState.IndexMetadataList {
			// Primary key does not need secondary index
			if strings.ToLower(indexMetadata.FieldName) == "id" {
				continue
			}

			indexName := indexMetadata.IndexName
			var indexSQL string
			uniqueKeyword := ""
			if indexMetadata.Unique {
				uniqueKeyword = "UNIQUE "
			}

			switch dialect {
			case DialectMSSQL:
				indexSQL = fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '%s' AND object_id = OBJECT_ID('%s'))
BEGIN
	CREATE %sINDEX %s ON %s (%s);
END`,
					indexName,
					tableName,
					uniqueKeyword,
					quoteIdentifier(indexName, dialect),
					quoteIdentifier(tableName, dialect),
					quoteIdentifier(indexMetadata.FieldName, dialect),
				)
			case DialectPostgreSQL, DialectSQLite:
				indexSQL = fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
					uniqueKeyword,
					quoteIdentifier(indexName, dialect),
					quoteIdentifier(tableName, dialect),
					quoteIdentifier(indexMetadata.FieldName, dialect),
				)
			default: // DialectMySQL
				indexSQL = fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
					uniqueKeyword,
					quoteIdentifier(indexName, dialect),
					quoteIdentifier(tableName, dialect),
					quoteIdentifier(indexMetadata.FieldName, dialect),
				)
			}

			_, error := sqlDatabase.Exec(indexSQL)
			if error != nil {
				// For MySQL, catch "Duplicate key name" error (ErrorCode 1061)
				if dialect == DialectMySQL && (strings.Contains(error.Error(), "1061") || strings.Contains(strings.ToLower(error.Error()), "duplicate key name")) {
					// Already exists, ignore
				} else {
					return fmt.Errorf("failed to create index %s on table %s: %w", indexName, tableName, error)
				}
			}
		}

		// 4. Retrieve and Insert Records
		var records []any
		namedIndex, loadError := database.getShadowIndex(tableName)
		if loadError != nil {
			return fmt.Errorf("failed to load shadow index for table %s: %w", tableName, loadError)
		}
		rangeError := namedIndex.Range(nil, nil, uint64(database.getCommittedState().Generation), func(keyBytes []byte, recordPointer RecordPointer) bool {
			bytesValue, error := tableStorageValue.ReadRecord(recordPointer)
			if error != nil {
				return false
			}
			newRecordValue := reflect.New(tableState.EntityType)
			if error := Unmarshal(bytesValue, newRecordValue.Interface()); error != nil {
				return false
			}
			records = append(records, newRecordValue.Elem().Interface())
			return true
		})
		if rangeError != nil {
			return fmt.Errorf("failed to scan B+ tree for table %s: %w", tableName, rangeError)
		}

		if len(records) == 0 {
			continue
		}

		// Prepare placeholders
		var placeholders []string
		for index := range columns {
			placeholders = append(placeholders, getPlaceholder(dialect, index+1))
		}

		// Execute insertions in a transaction
		sqlTransaction, error := sqlDatabase.Begin()
		if error != nil {
			return fmt.Errorf("failed to begin SQL transaction for table %s: %w", tableName, error)
		}

		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			quoteIdentifier(tableName, dialect),
			strings.Join(quotedColumns, ", "),
			strings.Join(placeholders, ", "),
		)

		sqlStatement, error := sqlTransaction.Prepare(insertSQL)
		if error != nil {
			sqlTransaction.Rollback()
			return fmt.Errorf("failed to prepare insert statement for table %s: %w", tableName, error)
		}

		for _, record := range records {
			reflectValue := reflect.ValueOf(record)
			var args []any
			for _, fieldName := range fieldNames {
				fieldValue := reflectValue.FieldByName(fieldName)
				boundValue, error := bindValue(fieldValue, dialect)
				if error != nil {
					sqlStatement.Close()
					sqlTransaction.Rollback()
					return fmt.Errorf("failed to bind field value in table %s: %w", tableName, error)
				}
				args = append(args, boundValue)
			}
			if _, error := sqlStatement.Exec(args...); error != nil {
				sqlStatement.Close()
				sqlTransaction.Rollback()
				return fmt.Errorf("failed to insert record into table %s: %w", tableName, error)
			}
		}

		sqlStatement.Close()
		if error := sqlTransaction.Commit(); error != nil {
			return fmt.Errorf("failed to commit SQL transaction for table %s: %w", tableName, error)
		}
	}

	return nil
}

func quoteIdentifier(name string, dialect SQLDialect) string {
	switch dialect {
	case DialectPostgreSQL, DialectSQLite:
		return `"` + name + `"`
	case DialectMSSQL:
		return `[` + name + `]`
	default: // DialectMySQL
		return "`" + name + "`"
	}
}

func getPlaceholder(dialect SQLDialect, index int) string {
	switch dialect {
	case DialectPostgreSQL:
		return fmt.Sprintf("$%d", index)
	case DialectMSSQL:
		return fmt.Sprintf("@p%d", index)
	default: // DialectSQLite, DialectMySQL
		return "?"
	}
}

func mapGoTypeToSQL(reflectType reflect.Type, dialect SQLDialect) string {
	for reflectType.Kind() == reflect.Ptr {
		reflectType = reflectType.Elem()
	}

	switch reflectType.Kind() {
	case reflect.String:
		if dialect == DialectSQLite {
			return "TEXT"
		}
		if dialect == DialectMSSQL {
			return "NVARCHAR(255)"
		}
		return "VARCHAR(255)"

	case reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
		return "INT"

	case reflect.Int64:
		if dialect == DialectSQLite {
			return "INTEGER"
		}
		return "BIGINT"

	case reflect.Float32, reflect.Float64:
		switch dialect {
		case DialectSQLite:
			return "REAL"
		case DialectPostgreSQL:
			return "DOUBLE PRECISION"
		case DialectMSSQL:
			return "FLOAT"
		default: // DialectMySQL
			return "DOUBLE"
		}

	case reflect.Bool:
		switch dialect {
		case DialectPostgreSQL:
			return "BOOLEAN"
		case DialectSQLite:
			return "INTEGER"
		case DialectMSSQL:
			return "BIT"
		default:
			return "TINYINT(1)"
		}

	case reflect.Struct:
		if reflectType.String() == "time.Time" {
			switch dialect {
			case DialectPostgreSQL:
				return "TIMESTAMPTZ"
			case DialectSQLite:
				return "TEXT"
			case DialectMSSQL:
				return "DATETIMEOFFSET"
			default:
				return "DATETIME(6)"
			}
		}
		if dialect == DialectMSSQL {
			return "NVARCHAR(MAX)"
		}
		return "TEXT"

	case reflect.Slice:
		if reflectType.Elem().Kind() == reflect.Uint8 {
			switch dialect {
			case DialectPostgreSQL:
				return "BYTEA"
			case DialectMSSQL:
				return "VARBINARY(MAX)"
			case DialectMySQL:
				return "LONGBLOB"
			default:
				return "BLOB"
			}
		}
		if dialect == DialectMSSQL {
			return "NVARCHAR(MAX)"
		}
		return "TEXT"

	default:
		if dialect == DialectMSSQL {
			return "NVARCHAR(MAX)"
		}
		return "TEXT"
	}
}

func bindValue(reflectValue reflect.Value, dialect SQLDialect) (any, error) {
	if !reflectValue.IsValid() {
		return nil, nil
	}

	for reflectValue.Kind() == reflect.Ptr {
		if reflectValue.IsNil() {
			return nil, nil
		}
		reflectValue = reflectValue.Elem()
	}

	switch reflectValue.Kind() {
	case reflect.String:
		return reflectValue.String(), nil
	case reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
		return reflectValue.Int(), nil
	case reflect.Int64:
		return reflectValue.Int(), nil
	case reflect.Float32, reflect.Float64:
		return reflectValue.Float(), nil
	case reflect.Bool:
		boolValue := reflectValue.Bool()
		if dialect == DialectPostgreSQL {
			return boolValue, nil
		}
		if boolValue {
			return 1, nil
		}
		return 0, nil
	case reflect.Struct:
		if reflectValue.Type().String() == "time.Time" {
			timeValue := reflectValue.Interface().(time.Time)
			switch dialect {
			case DialectSQLite:
				return timeValue.Format(time.RFC3339Nano), nil
			default:
				return timeValue, nil
			}
		}
		jsonBytes, error := json.Marshal(reflectValue.Interface())
		if error != nil {
			return nil, error
		}
		return string(jsonBytes), nil

	case reflect.Slice:
		if reflectValue.Type().Elem().Kind() == reflect.Uint8 {
			return reflectValue.Bytes(), nil
		}
		jsonBytes, error := json.Marshal(reflectValue.Interface())
		if error != nil {
			return nil, error
		}
		return string(jsonBytes), nil

	default:
		jsonBytes, error := json.Marshal(reflectValue.Interface())
		if error != nil {
			return nil, error
		}
		return string(jsonBytes), nil
	}
}

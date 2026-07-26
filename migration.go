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
func Migrate(db *Database, sqlDB *sql.DB, dialect SQLDialect) error {
	if db == nil {
		return fmt.Errorf("masterkeeper database cannot be nil")
	}
	if sqlDB == nil {
		return fmt.Errorf("sql database connection cannot be nil")
	}

	committed := db.getCommittedState()
	for tableName, ts := range committed.Tables {
		storage, err := db.getTableStorage(tableName)
		if err != nil {
			return err
		}

		// 1. Reflect EntityType fields to construct column definitions
		entityType := ts.EntityType
		if entityType.Kind() != reflect.Struct {
			return fmt.Errorf("entity type for table %s must be a struct", tableName)
		}

		var columns []string
		var quotedColumns []string
		var columnDefs []string
		var fieldNames []string

		for i := 0; i < entityType.NumField(); i++ {
			field := entityType.Field(i)
			fieldName := getFieldName(field)
			meta := parseFieldTag(field)

			colType := mapGoTypeToSQL(field.Type, dialect)
			colDef := quoteIdentifier(fieldName, dialect) + " " + colType

			if meta.IsID {
				colDef += " PRIMARY KEY"
			}

			columnDefs = append(columnDefs, colDef)
			columns = append(columns, fieldName)
			quotedColumns = append(quotedColumns, quoteIdentifier(fieldName, dialect))
			fieldNames = append(fieldNames, field.Name)
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

		if _, err := sqlDB.Exec(createTableSQL); err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}

		// 3. Create Indexes
		for _, meta := range ts.IndexMetadataList {
			// Primary key does not need secondary index
			if strings.ToLower(meta.FieldName) == "id" {
				continue
			}

			indexName := meta.IndexName
			var indexSQL string
			uniqueKeyword := ""
			if meta.Unique {
				uniqueKeyword = "UNIQUE "
			}

			if dialect == DialectMSSQL {
				indexSQL = fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '%s' AND object_id = OBJECT_ID('%s'))
BEGIN
	CREATE %sINDEX %s ON %s (%s);
END`,
					indexName,
					tableName,
					uniqueKeyword,
					quoteIdentifier(indexName, dialect),
					quoteIdentifier(tableName, dialect),
					quoteIdentifier(meta.FieldName, dialect),
				)
			} else if dialect == DialectPostgreSQL || dialect == DialectSQLite {
				indexSQL = fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
					uniqueKeyword,
					quoteIdentifier(indexName, dialect),
					quoteIdentifier(tableName, dialect),
					quoteIdentifier(meta.FieldName, dialect),
				)
			} else { // DialectMySQL
				indexSQL = fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
					uniqueKeyword,
					quoteIdentifier(indexName, dialect),
					quoteIdentifier(tableName, dialect),
					quoteIdentifier(meta.FieldName, dialect),
				)
			}

			_, err := sqlDB.Exec(indexSQL)
			if err != nil {
				// For MySQL, catch "Duplicate key name" error (ErrorCode 1061)
				if dialect == DialectMySQL && (strings.Contains(err.Error(), "1061") || strings.Contains(strings.ToLower(err.Error()), "duplicate key name")) {
					// Already exists, ignore
				} else {
					return fmt.Errorf("failed to create index %s on table %s: %w", indexName, tableName, err)
				}
			}
		}

		// 4. Retrieve and Insert Records
		var records []any
		for _, ptr := range ts.RecordPointers {
			bytes, err := storage.ReadRecord(ptr)
			if err != nil {
				return fmt.Errorf("failed to read record from table %s: %w", tableName, err)
			}
			newRecordVal := reflect.New(ts.EntityType)
			if err := Unmarshal(bytes, newRecordVal.Interface()); err != nil {
				return fmt.Errorf("failed to unmarshal record from table %s: %w", tableName, err)
			}
			records = append(records, newRecordVal.Elem().Interface())
		}

		if len(records) == 0 {
			continue
		}

		// Prepare placeholders
		var placeholders []string
		for i := range columns {
			placeholders = append(placeholders, getPlaceholder(dialect, i+1))
		}

		// Execute insertions in a transaction
		tx, err := sqlDB.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin SQL transaction for table %s: %w", tableName, err)
		}

		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			quoteIdentifier(tableName, dialect),
			strings.Join(quotedColumns, ", "),
			strings.Join(placeholders, ", "),
		)

		stmt, err := tx.Prepare(insertSQL)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to prepare insert statement for table %s: %w", tableName, err)
		}

		for _, record := range records {
			val := reflect.ValueOf(record)
			var args []any
			for _, fName := range fieldNames {
				fieldVal := val.FieldByName(fName)
				boundVal, err := bindValue(fieldVal, dialect)
				if err != nil {
					stmt.Close()
					tx.Rollback()
					return fmt.Errorf("failed to bind field value in table %s: %w", tableName, err)
				}
				args = append(args, boundVal)
			}
			if _, err := stmt.Exec(args...); err != nil {
				stmt.Close()
				tx.Rollback()
				return fmt.Errorf("failed to insert record into table %s: %w", tableName, err)
			}
		}

		stmt.Close()
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit SQL transaction for table %s: %w", tableName, err)
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

func mapGoTypeToSQL(t reflect.Type, dialect SQLDialect) string {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
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
		if t.String() == "time.Time" {
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
		if t.Elem().Kind() == reflect.Uint8 {
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

func bindValue(val reflect.Value, dialect SQLDialect) (any, error) {
	if !val.IsValid() {
		return nil, nil
	}

	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, nil
		}
		val = val.Elem()
	}

	switch val.Kind() {
	case reflect.String:
		return val.String(), nil
	case reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
		return val.Int(), nil
	case reflect.Int64:
		return val.Int(), nil
	case reflect.Float32, reflect.Float64:
		return val.Float(), nil
	case reflect.Bool:
		b := val.Bool()
		if dialect == DialectPostgreSQL {
			return b, nil
		}
		if b {
			return 1, nil
		}
		return 0, nil
	case reflect.Struct:
		if val.Type().String() == "time.Time" {
			t := val.Interface().(time.Time)
			switch dialect {
			case DialectSQLite:
				return t.Format(time.RFC3339Nano), nil
			default:
				return t, nil
			}
		}
		jsonBytes, err := json.Marshal(val.Interface())
		if err != nil {
			return nil, err
		}
		return string(jsonBytes), nil

	case reflect.Slice:
		if val.Type().Elem().Kind() == reflect.Uint8 {
			return val.Bytes(), nil
		}
		jsonBytes, err := json.Marshal(val.Interface())
		if err != nil {
			return nil, err
		}
		return string(jsonBytes), nil

	default:
		jsonBytes, err := json.Marshal(val.Interface())
		if err != nil {
			return nil, err
		}
		return string(jsonBytes), nil
	}
}

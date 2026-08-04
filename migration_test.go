package masterkeeper_test

import (
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync"
	"testing"
	"time"

	keeper "github.com/lemadane/masterkeeper"
)

var (
	queriesMu sync.Mutex
	queries   []string
)

func recordQuery(queryString string) {
	queriesMu.Lock()
	defer queriesMu.Unlock()
	queries = append(queries, queryString)
}

func getRecordedQueries() []string {
	queriesMu.Lock()
	defer queriesMu.Unlock()
	results := make([]string, len(queries))
	copy(results, queries)
	return results
}

func clearRecordedQueries() {
	queriesMu.Lock()
	defer queriesMu.Unlock()
	queries = nil
}

type mockDriver struct{}

func (driverValue *mockDriver) Open(name string) (driver.Conn, error) {
	return &mockConnection{}, nil
}

type mockConnection struct{}

func (connection *mockConnection) Prepare(query string) (driver.Stmt, error) {
	recordQuery(query)
	return &mockStatement{query: query}, nil
}

func (connection *mockConnection) Close() error { return nil }

func (connection *mockConnection) Begin() (driver.Tx, error) {
	return &mockTransaction{}, nil
}

func (connection *mockConnection) Exec(query string, args []driver.Value) (driver.Result, error) {
	recordQuery(query)
	return &mockResult{}, nil
}

type mockStatement struct {
	query string
}

func (statement *mockStatement) Close() error { return nil }

func (statement *mockStatement) NumInput() int { return -1 }

func (statement *mockStatement) Exec(args []driver.Value) (driver.Result, error) {
	return &mockResult{}, nil
}

func (statement *mockStatement) Query(args []driver.Value) (driver.Rows, error) {
	return nil, nil
}

type mockTransaction struct{}

func (transaction *mockTransaction) Commit() error   { return nil }
func (transaction *mockTransaction) Rollback() error { return nil }

type mockResult struct{}

func (result *mockResult) LastInsertId() (int64, error) { return 0, nil }
func (result *mockResult) RowsAffected() (int64, error) { return 0, nil }

func init() {
	sql.Register("mock_driver", &mockDriver{})
}

type MigrationNestedStruct struct {
	Tag   string
	Score int
}

type MigrationUser struct {
	ID       string `keeper:"id"`
	Name     string `keeper:"index"`
	Email    string `keeper:"unique"`
	Age      int
	Score    float64
	Active   bool
	Created  time.Time
	Blob     []byte
	Metadata MigrationNestedStruct
}

func TestMigrateAllDialects(test *testing.T) {
	options := keeper.DefaultOptions()
	options.RegisterTypes(MigrationUser{})

	database, testError := keeper.Open(test.TempDir(), options)
	if testError != nil {
		test.Fatalf("failed to open database: %v", testError)
	}
	defer database.Close()

	userTable, testError := keeper.GetTable[string, MigrationUser](database, "users")
	if testError != nil {
		test.Fatalf("failed to retrieve table: %v", testError)
	}

	testError = database.Transaction(func(transaction *keeper.Transaction) error {
		return userTable.Insert(transaction, MigrationUser{
			ID:      "usr_1",
			Name:    "Alice",
			Email:   "alice@example.com",
			Age:     30,
			Score:   95.5,
			Active:  true,
			Created: time.Now(),
			Blob:    []byte("hello bytes"),
			Metadata: MigrationNestedStruct{
				Tag:   "vip",
				Score: 10,
			},
		})
	})
	if testError != nil {
		test.Fatalf("failed to insert test record: %v", testError)
	}

	sqlDatabase, testError := sql.Open("mock_driver", "dummy_source")
	if testError != nil {
		test.Fatalf("failed to open mock db: %v", testError)
	}
	defer sqlDatabase.Close()

	tests := []struct {
		dialect          keeper.SQLDialect
		name             string
		expectedTableSQL string
		expectedIndexSQL []string
		expectedInsert   string
	}{
		{
			dialect: keeper.DialectPostgreSQL,
			name:    "PostgreSQL",
			expectedTableSQL: `CREATE TABLE IF NOT EXISTS "users" (
	"ID" VARCHAR(255) PRIMARY KEY,
	"Name" VARCHAR(255),
	"Email" VARCHAR(255),
	"Age" INT,
	"Score" DOUBLE PRECISION,
	"Active" BOOLEAN,
	"Created" TIMESTAMPTZ,
	"Blob" BYTEA,
	"Metadata" TEXT
)`,
			expectedIndexSQL: []string{
				`CREATE INDEX IF NOT EXISTS "users_Name_idx" ON "users" ("Name")`,
				`CREATE UNIQUE INDEX IF NOT EXISTS "users_Email_idx" ON "users" ("Email")`,
			},
			expectedInsert: `INSERT INTO "users" ("ID", "Name", "Email", "Age", "Score", "Active", "Created", "Blob", "Metadata") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		},
		{
			dialect: keeper.DialectMySQL,
			name:    "MySQL",
			expectedTableSQL: "CREATE TABLE IF NOT EXISTS `users` (\n\t`ID` VARCHAR(255) PRIMARY KEY,\n\t`Name` VARCHAR(255),\n\t`Email` VARCHAR(255),\n\t`Age` INT,\n\t`Score` DOUBLE,\n\t`Active` TINYINT(1),\n\t`Created` DATETIME(6),\n\t`Blob` LONGBLOB,\n\t`Metadata` TEXT\n)",
			expectedIndexSQL: []string{
				"CREATE INDEX `users_Name_idx` ON `users` (`Name`)",
				"CREATE UNIQUE INDEX `users_Email_idx` ON `users` (`Email`)",
			},
			expectedInsert: "INSERT INTO `users` (`ID`, `Name`, `Email`, `Age`, `Score`, `Active`, `Created`, `Blob`, `Metadata`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		},
		{
			dialect: keeper.DialectSQLite,
			name:    "SQLite",
			expectedTableSQL: `CREATE TABLE IF NOT EXISTS "users" (
	"ID" TEXT PRIMARY KEY,
	"Name" TEXT,
	"Email" TEXT,
	"Age" INT,
	"Score" REAL,
	"Active" INTEGER,
	"Created" TEXT,
	"Blob" BLOB,
	"Metadata" TEXT
)`,
			expectedIndexSQL: []string{
				`CREATE INDEX IF NOT EXISTS "users_Name_idx" ON "users" ("Name")`,
				`CREATE UNIQUE INDEX IF NOT EXISTS "users_Email_idx" ON "users" ("Email")`,
			},
			expectedInsert: `INSERT INTO "users" ("ID", "Name", "Email", "Age", "Score", "Active", "Created", "Blob", "Metadata") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		},
		{
			dialect: keeper.DialectMSSQL,
			name:    "MSSQL",
			expectedTableSQL: `IF OBJECT_ID('users', 'U') IS NULL
BEGIN
	CREATE TABLE [users] (
		[ID] NVARCHAR(255) PRIMARY KEY,
		[Name] NVARCHAR(255),
		[Email] NVARCHAR(255),
		[Age] INT,
		[Score] FLOAT,
		[Active] BIT,
		[Created] DATETIMEOFFSET,
		[Blob] VARBINARY(MAX),
		[Metadata] NVARCHAR(MAX)
	);
END`,
			expectedIndexSQL: []string{
				`IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'users_Name_idx' AND object_id = OBJECT_ID('users'))
BEGIN
	CREATE INDEX [users_Name_idx] ON [users] ([Name]);
END`,
				`IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'users_Email_idx' AND object_id = OBJECT_ID('users'))
BEGIN
	CREATE UNIQUE INDEX [users_Email_idx] ON [users] ([Email]);
END`,
			},
			expectedInsert: `INSERT INTO [users] ([ID], [Name], [Email], [Age], [Score], [Active], [Created], [Blob], [Metadata]) VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9)`,
		},
	}

	for _, testCase := range tests {
		test.Run(testCase.name, func(subtestT *testing.T) {
			clearRecordedQueries()
			testError := keeper.Migrate(database, sqlDatabase, testCase.dialect)
			if testError != nil {
				subtestT.Fatalf("failed to migrate: %v", testError)
			}

			recorded := getRecordedQueries()

			// Check table creation query
			foundTableSQL := false
			for _, queryString := range recorded {
				if strings.Contains(strings.ReplaceAll(queryString, "\r\n", "\n"), strings.ReplaceAll(testCase.expectedTableSQL, "\r\n", "\n")) {
					foundTableSQL = true
					break
				}
			}
			if !foundTableSQL {
				subtestT.Errorf("expected table SQL not found.\nExpected:\n%s\nRecorded:\n%v", testCase.expectedTableSQL, recorded)
			}

			// Check index creation queries
			for _, indexSQLQuery := range testCase.expectedIndexSQL {
				foundIndex := false
				for _, queryString := range recorded {
					if strings.Contains(strings.ReplaceAll(queryString, "\r\n", "\n"), strings.ReplaceAll(indexSQLQuery, "\r\n", "\n")) {
						foundIndex = true
						break
					}
				}
				if !foundIndex {
					subtestT.Errorf("expected index SQL not found: %s", indexSQLQuery)
				}
			}

			// Check insert statement
			foundInsert := false
			for _, queryString := range recorded {
				if strings.Contains(queryString, testCase.expectedInsert) {
					foundInsert = true
					break
				}
			}
			if !foundInsert {
				subtestT.Errorf("expected insert statement not found: %s", testCase.expectedInsert)
			}
		})
	}
}

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

func recordQuery(q string) {
	queriesMu.Lock()
	defer queriesMu.Unlock()
	queries = append(queries, q)
}

func getRecordedQueries() []string {
	queriesMu.Lock()
	defer queriesMu.Unlock()
	res := make([]string, len(queries))
	copy(res, queries)
	return res
}

func clearRecordedQueries() {
	queriesMu.Lock()
	defer queriesMu.Unlock()
	queries = nil
}

type mockDriver struct{}

func (d *mockDriver) Open(name string) (driver.Conn, error) {
	return &mockConn{}, nil
}

type mockConn struct{}

func (c *mockConn) Prepare(query string) (driver.Stmt, error) {
	recordQuery(query)
	return &mockStmt{query: query}, nil
}

func (c *mockConn) Close() error { return nil }

func (c *mockConn) Begin() (driver.Tx, error) {
	return &mockTx{}, nil
}

func (c *mockConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	recordQuery(query)
	return &mockResult{}, nil
}

type mockStmt struct {
	query string
}

func (s *mockStmt) Close() error { return nil }

func (s *mockStmt) NumInput() int { return -1 }

func (s *mockStmt) Exec(args []driver.Value) (driver.Result, error) {
	return &mockResult{}, nil
}

func (s *mockStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, nil
}

type mockTx struct{}

func (t *mockTx) Commit() error   { return nil }
func (t *mockTx) Rollback() error { return nil }

type mockResult struct{}

func (r *mockResult) LastInsertId() (int64, error) { return 0, nil }
func (r *mockResult) RowsAffected() (int64, error) { return 0, nil }

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

func TestMigrateAllDialects(t *testing.T) {
	opts := keeper.DefaultOptions()
	opts.RegisterTypes(MigrationUser{})

	db, err := keeper.Open(t.TempDir(), opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	table, err := keeper.GetTable[string, MigrationUser](db, "users")
	if err != nil {
		t.Fatalf("failed to retrieve table: %v", err)
	}

	err = db.Transaction(func(tx *keeper.Transaction) error {
		return table.Insert(tx, MigrationUser{
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
	if err != nil {
		t.Fatalf("failed to insert test record: %v", err)
	}

	sqlDB, err := sql.Open("mock_driver", "dummy_source")
	if err != nil {
		t.Fatalf("failed to open mock db: %v", err)
	}
	defer sqlDB.Close()

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

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearRecordedQueries()
			err := keeper.Migrate(db, sqlDB, tc.dialect)
			if err != nil {
				t.Fatalf("failed to migrate: %v", err)
			}

			recorded := getRecordedQueries()

			// Check table creation query
			foundTableSQL := false
			for _, q := range recorded {
				if strings.Contains(strings.ReplaceAll(q, "\r\n", "\n"), strings.ReplaceAll(tc.expectedTableSQL, "\r\n", "\n")) {
					foundTableSQL = true
					break
				}
			}
			if !foundTableSQL {
				t.Errorf("expected table SQL not found.\nExpected:\n%s\nRecorded:\n%v", tc.expectedTableSQL, recorded)
			}

			// Check index creation queries
			for _, idxSQL := range tc.expectedIndexSQL {
				foundIdx := false
				for _, q := range recorded {
					if strings.Contains(strings.ReplaceAll(q, "\r\n", "\n"), strings.ReplaceAll(idxSQL, "\r\n", "\n")) {
						foundIdx = true
						break
					}
				}
				if !foundIdx {
					t.Errorf("expected index SQL not found: %s", idxSQL)
				}
			}

			// Check insert statement
			foundInsert := false
			for _, q := range recorded {
				if strings.Contains(q, tc.expectedInsert) {
					foundInsert = true
					break
				}
			}
			if !foundInsert {
				t.Errorf("expected insert statement not found: %s", tc.expectedInsert)
			}
		})
	}
}

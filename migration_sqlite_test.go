package masterkeeper_test

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	keeper "github.com/lemadane/masterkeeper"
)

func TestSQLiteMigrationIntegration(test *testing.T) {
	// 1. Setup masterkeeper database
	options := keeper.DefaultOptions()
	options.RegisterTypes(MigrationUser{})

	dbDirectory := test.TempDir()
	database, testError := keeper.Open(dbDirectory, options)
	if testError != nil {
		test.Fatalf("failed to open masterkeeper: %v", testError)
	}

	userTable, testError := keeper.GetTable[string, MigrationUser](database, "users")
	if testError != nil {
		database.Close()
		test.Fatalf("failed to get table: %v", testError)
	}

	// 2. Insert test data in masterkeeper
	testUser1 := MigrationUser{
		ID:      "usr_100",
		Name:    "Bob Smith",
		Email:   "bob@example.com",
		Age:     42,
		Score:   88.2,
		Active:  true,
		Created: time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC),
		Blob:    []byte("sqlite integration test"),
		Metadata: MigrationNestedStruct{
			Tag:   "moderator",
			Score: 100,
		},
	}

	testUser2 := MigrationUser{
		ID:      "usr_200",
		Name:    "Charlie Brown",
		Email:   "charlie@example.com",
		Age:     28,
		Score:   73.4,
		Active:  false,
		Created: time.Date(2026, 7, 26, 13, 0, 0, 0, time.UTC),
		Blob:    []byte("another sqlite user"),
		Metadata: MigrationNestedStruct{
			Tag:   "user",
			Score: 10,
		},
	}

	testError = database.Transaction(func(transaction *keeper.Transaction) error {
		if testError := userTable.Insert(transaction, testUser1); testError != nil {
			return testError
		}
		if testError := userTable.Insert(transaction, testUser2); testError != nil {
			return testError
		}
		return nil
	})
	if testError != nil {
		database.Close()
		test.Fatalf("failed to insert data: %v", testError)
	}

	// 3. Open SQLite Database
	sqlitePath := filepath.Join(test.TempDir(), "test.db")
	sqlDatabase, testError := sql.Open("sqlite", sqlitePath)
	if testError != nil {
		database.Close()
		test.Fatalf("failed to open SQLite connection: %v", testError)
	}
	defer sqlDatabase.Close()

	// 4. Run Migration
	testError = keeper.Migrate(database, sqlDatabase, keeper.DialectSQLite)
	if testError != nil {
		database.Close()
		test.Fatalf("migration to SQLite failed: %v", testError)
	}

	// Close masterkeeper
	database.Close()

	// 5. Query and Assert data from SQLite
	rows, testError := sqlDatabase.Query(`SELECT "ID", "Name", "Email", "Age", "Score", "Active", "Created", "Blob", "Metadata" FROM "users" ORDER BY "ID" ASC`)
	if testError != nil {
		test.Fatalf("failed to query sqlite database: %v", testError)
	}
	defer rows.Close()

	var migratedUsers []MigrationUser
	for rows.Next() {
		var user MigrationUser
		var createdString string
		var blob []byte
		var metadataJSON string
		var activeInteger int

		testError := rows.Scan(
			&user.ID,
			&user.Name,
			&user.Email,
			&user.Age,
			&user.Score,
			&activeInteger,
			&createdString,
			&blob,
			&metadataJSON,
		)
		if testError != nil {
			test.Fatalf("failed to scan sqlite row: %v", testError)
		}

		user.Active = activeInteger != 0
		user.Blob = blob

		// Parse created time
		user.Created, testError = time.Parse(time.RFC3339Nano, createdString)
		if testError != nil {
			test.Fatalf("failed to parse created time: %v", testError)
		}

		// Parse nested struct
		testError = json.Unmarshal([]byte(metadataJSON), &user.Metadata)
		if testError != nil {
			test.Fatalf("failed to unmarshal nested struct metadata: %v", testError)
		}

		migratedUsers = append(migratedUsers, user)
	}

	if len(migratedUsers) != 2 {
		test.Fatalf("expected 2 users in sqlite, got %d", len(migratedUsers))
	}

	// Assert User 1
	if migratedUsers[0].ID != testUser1.ID ||
		migratedUsers[0].Name != testUser1.Name ||
		migratedUsers[0].Email != testUser1.Email ||
		migratedUsers[0].Age != testUser1.Age ||
		migratedUsers[0].Score != testUser1.Score ||
		migratedUsers[0].Active != testUser1.Active ||
		!migratedUsers[0].Created.Equal(testUser1.Created) ||
		string(migratedUsers[0].Blob) != string(testUser1.Blob) ||
		migratedUsers[0].Metadata.Tag != testUser1.Metadata.Tag ||
		migratedUsers[0].Metadata.Score != testUser1.Metadata.Score {
		test.Errorf("mismatch on user 1. Got: %+v, Want: %+v", migratedUsers[0], testUser1)
	}

	// Assert User 2
	if migratedUsers[1].ID != testUser2.ID ||
		migratedUsers[1].Name != testUser2.Name ||
		migratedUsers[1].Email != testUser2.Email ||
		migratedUsers[1].Age != testUser2.Age ||
		migratedUsers[1].Score != testUser2.Score ||
		migratedUsers[1].Active != testUser2.Active ||
		!migratedUsers[1].Created.Equal(testUser2.Created) ||
		string(migratedUsers[1].Blob) != string(testUser2.Blob) ||
		migratedUsers[1].Metadata.Tag != testUser2.Metadata.Tag ||
		migratedUsers[1].Metadata.Score != testUser2.Metadata.Score {
		test.Errorf("mismatch on user 2. Got: %+v, Want: %+v", migratedUsers[1], testUser2)
	}
}

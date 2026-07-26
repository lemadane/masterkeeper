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

func TestSQLiteMigrationIntegration(t *testing.T) {
	// 1. Setup masterkeeper database
	opts := keeper.DefaultOptions()
	opts.RegisterTypes(MigrationUser{})

	dbDir := t.TempDir()
	db, err := keeper.Open(dbDir, opts)
	if err != nil {
		t.Fatalf("failed to open masterkeeper: %v", err)
	}

	table, err := keeper.GetTable[string, MigrationUser](db, "users")
	if err != nil {
		db.Close()
		t.Fatalf("failed to get table: %v", err)
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

	err = db.Transaction(func(tx *keeper.Transaction) error {
		if err := table.Insert(tx, testUser1); err != nil {
			return err
		}
		if err := table.Insert(tx, testUser2); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		db.Close()
		t.Fatalf("failed to insert data: %v", err)
	}

	// 3. Open SQLite Database
	sqlitePath := filepath.Join(t.TempDir(), "test.db")
	sqlDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		db.Close()
		t.Fatalf("failed to open SQLite connection: %v", err)
	}
	defer sqlDB.Close()

	// 4. Run Migration
	err = keeper.Migrate(db, sqlDB, keeper.DialectSQLite)
	if err != nil {
		db.Close()
		t.Fatalf("migration to SQLite failed: %v", err)
	}

	// Close masterkeeper
	db.Close()

	// 5. Query and Assert data from SQLite
	rows, err := sqlDB.Query(`SELECT "ID", "Name", "Email", "Age", "Score", "Active", "Created", "Blob", "Metadata" FROM "users" ORDER BY "ID" ASC`)
	if err != nil {
		t.Fatalf("failed to query sqlite database: %v", err)
	}
	defer rows.Close()

	var migratedUsers []MigrationUser
	for rows.Next() {
		var u MigrationUser
		var createdStr string
		var blob []byte
		var metadataJSON string
		var activeInt int

		err := rows.Scan(
			&u.ID,
			&u.Name,
			&u.Email,
			&u.Age,
			&u.Score,
			&activeInt,
			&createdStr,
			&blob,
			&metadataJSON,
		)
		if err != nil {
			t.Fatalf("failed to scan sqlite row: %v", err)
		}

		u.Active = activeInt != 0
		u.Blob = blob

		// Parse created time
		u.Created, err = time.Parse(time.RFC3339Nano, createdStr)
		if err != nil {
			t.Fatalf("failed to parse created time: %v", err)
		}

		// Parse nested struct
		err = json.Unmarshal([]byte(metadataJSON), &u.Metadata)
		if err != nil {
			t.Fatalf("failed to unmarshal nested struct metadata: %v", err)
		}

		migratedUsers = append(migratedUsers, u)
	}

	if len(migratedUsers) != 2 {
		t.Fatalf("expected 2 users in sqlite, got %d", len(migratedUsers))
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
		t.Errorf("mismatch on user 1. Got: %+v, Want: %+v", migratedUsers[0], testUser1)
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
		t.Errorf("mismatch on user 2. Got: %+v, Want: %+v", migratedUsers[1], testUser2)
	}
}

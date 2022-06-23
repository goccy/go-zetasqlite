package zetasqlite_test

import (
	"database/sql"
	"testing"

	"github.com/goccy/go-zetasqlite"
	_ "github.com/goccy/go-zetasqlite"
)

func TestDriver(t *testing.T) {
	db, err := sql.Open("zetasqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS Singers (
  SingerId   INT64 NOT NULL,
  FirstName  STRING(1024),
  LastName   STRING(1024),
  SingerInfo BYTES(MAX)
)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT Singers (SingerId, FirstName, LastName) VALUES (1, 'John', 'Titor')`); err != nil {
		t.Fatal(err)
	}
	row := db.QueryRow("SELECT SingerID, FirstName, LastName FROM Singers WHERE SingerId = @id", 1)
	if row.Err() != nil {
		t.Fatal(row.Err())
	}
	var (
		singerID  int64
		firstName string
		lastName  string
	)
	if err := row.Scan(&singerID, &firstName, &lastName); err != nil {
		t.Fatal(err)
	}
	if singerID != 1 || firstName != "John" || lastName != "Titor" {
		t.Fatalf("failed to find row %v %v %v", singerID, firstName, lastName)
	}
}

func TestRegisterCustomDriver(t *testing.T) {
	sql.Register("zetasqlite-custom", &zetasqlite.ZetaSQLiteDriver{
		ConnectHook: func(conn *zetasqlite.ZetaSQLiteConn) error {
			conn.SetNamePath([]string{"projectID", "datasetID"})
			return nil
		},
	})
	db, err := sql.Open("zetasqlite-custom", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS Samples (Id INT64 NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT datasetID.Samples (Id) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	row := db.QueryRow("SELECT * FROM Samples WHERE Id = @id", 1)
	if row.Err() != nil {
		t.Fatal(row.Err())
	}
	var id int64
	if err := row.Scan(&id); err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("failed to find row %v", id)
	}
}

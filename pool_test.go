package sqlpool

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestPool(t *testing.T) {
	pool := NewPool(Opts{
		Max:         10,
		IdleTimeout: 30,

		PreInit:  nil,
		PostInit: nil,
	})

	// Should be able to open
	dbPath := "/tmp/sqlpool_test.db"
	os.Remove(dbPath)
	r, err := pool.Acquire("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Error opening tmp database: %s", err)
	}
	if r == nil {
		t.Fatalf("Resource should not be nil ...")
	}

	t.Log(r)

	// Check stats
	if !(pool.Stats().Total == 1 && pool.Stats().Active == 1) {
		t.Errorf("Total databases open should be 1")
	}

	// Do SQL stuff
	if err := sqlTest(r.DB, t); err != nil {
		t.Errorf("Failed SQL: %s", err)
	}

	// Release
	if err := pool.Release(r); err != nil {
		t.Errorf("Error releasing resource: %s", err)
	}

	// Check stats
	if !(pool.Stats().Total == 1 && pool.Stats().Inactive == 1) {
		t.Errorf("Total databases open should be 1 (inactive)")
	}

	// Close
	if err := pool.Close(); err != nil {
		t.Errorf("Failed to close pool: %s", err)
	}

	// Closed stats
	if !(pool.Stats().Total == 0 && pool.Stats().Active == 0 && pool.Stats().Inactive == 0) {
		t.Errorf("Not correctly closed / cleaned up")
	}
}

func sqlTest(db *sql.DB, t *testing.T) error {
	sqlStmt := `
	create table foo (id integer not null primary key, name text);
	delete from foo;
	`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		t.Logf("%q: %s\n", err, sqlStmt)
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("insert into foo(id, name) values(?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i := 0; i < 100; i++ {
		_, err = stmt.Exec(i, fmt.Sprintf("こんにちわ世界%03d", i))
		if err != nil {
			return err
		}
	}
	tx.Commit()

	rows, err := db.Query("select id, name from foo")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		t.Log(id, name)
	}

	stmt, err = db.Prepare("select name from foo where id = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()
	var name string
	err = stmt.QueryRow("3").Scan(&name)
	if err != nil {
		return err
	}
	t.Log(name)

	_, err = db.Exec("delete from foo")
	if err != nil {
		return err
	}

	_, err = db.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		return err
	}

	rows, err = db.Query("select id, name from foo")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		t.Log(id, name)
	}

	return nil
}

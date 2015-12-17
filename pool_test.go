package sqlpool

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
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

func TestPoolParallel(t *testing.T) {
	m := 10
	n := 50
	resources := make([]*Resource, n*m)
	wg := sync.WaitGroup{}

	// Pool
	pool := NewPool(Opts{
		Max:         10,
		IdleTimeout: 30,

		PreInit:  nil,
		PostInit: nil,
	})
	// Build a group of DBs
	dbs := make([]string, m)
	for i, _ := range dbs {
		dbs[i] = fmt.Sprintf("/tmp/sqlpool_test_%d.db", i)
	}
	// Remove DBs
	for _, dbPath := range dbs {
		os.Remove(dbPath)
	}

	// Get connections to same DB
	for i := 0; i < n*m; i++ {
		wg.Add(1)
		go func(x int) {
			defer wg.Done()

			// Pick db
			dbPath := dbs[x%m]

			// Open DB
			r, err := pool.Acquire("sqlite3", dbPath)
			if err != nil {
				t.Errorf("Failed to acquire DB: %s", err)
			} else if r == nil {
				t.Errorf("Resource should not be nil if err is nil")
			} else {
				pool.Release(r)
			}
			resources[x] = r
		}(i)
	}

	wg.Wait()

	// Ensure there are exactly M unique databases open
	uniqueDBs := map[*Resource]bool{}
	for _, resource := range resources {
		uniqueDBs[resource] = true
	}
	if len(uniqueDBs) != m {
		for resource, _ := range uniqueDBs {
			t.Log(*resource)
		}
		t.Log(pool.Stats())
		t.Errorf("Expected %d unique resources, instead have %d", m, len(uniqueDBs))
	}

	// Close
	if err := pool.Close(); err != nil {
		t.Errorf("Failed to close parallel pool: %s", err)
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

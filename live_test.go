package opengauss

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"
)

// TestLiveFirstMilestone runs the first-milestone acceptance steps against a
// real openGauss server. It is skipped unless OPENGAUSS_DSN is set, because a
// live result cannot be produced offline. The DSN must point at a disposable
// database whose account may create temporary tables.
func TestLiveFirstMilestone(t *testing.T) {
	dsn := os.Getenv("OPENGAUSS_DSN")
	if dsn == "" {
		t.Skip("set OPENGAUSS_DSN to run against a live openGauss server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	connector, err := NewConnector(dsn)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}

	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatalf("SELECT version(): %v", err)
	}
	if version == "" {
		t.Fatal("server returned an empty version banner")
	}
	t.Logf("server version: %s", version)

	var bound int
	if err := db.QueryRowContext(ctx, "SELECT $1::integer", 42).Scan(&bound); err != nil {
		t.Fatalf("bound integer: %v", err)
	}
	if bound != 42 {
		t.Errorf("bound integer = %d, want 42", bound)
	}

	// Keep the temporary table and the transaction on one pinned connection so
	// a pool switch cannot hide session or rollback errors.
	pinned, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	defer func() { _ = pinned.Close() }()

	if _, err := pinned.ExecContext(ctx, "CREATE TEMPORARY TABLE opengauss_milestone (v integer)"); err != nil {
		t.Fatalf("CREATE TEMPORARY TABLE: %v", err)
	}
	tx, err := pinned.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO opengauss_milestone (v) VALUES ($1)", 7); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	var count int
	if err := pinned.QueryRowContext(ctx, "SELECT count(*) FROM opengauss_milestone").Scan(&count); err != nil {
		t.Fatalf("count after rollback: %v", err)
	}
	if count != 0 {
		t.Errorf("count after rollback = %d, want 0", count)
	}
}

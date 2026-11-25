package opengauss

import (
	"context"
	"database/sql"
	"fmt"
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

// TestLiveNotifications runs the F09 acceptance steps against a real server: a
// listener session and a separate publishing session, delivery only at commit,
// rollback, self-notification through the listener session and UNLISTEN. It is
// skipped unless OPENGAUSS_DSN is set.
func TestLiveNotifications(t *testing.T) {
	dsn := os.Getenv("OPENGAUSS_DSN")
	if dsn == "" {
		t.Skip("set OPENGAUSS_DSN to run against a live openGauss server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	listener, err := NewListener(dsn, time.Second, time.Minute)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	defer func() { _ = listener.Close() }()

	channel := fmt.Sprintf("opengauss_live_%d", os.Getpid())
	if err := listener.Listen(channel); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	publish := func(payload string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, "SELECT pg_notify($1, $2)", channel, payload); err != nil {
			t.Fatalf("pg_notify(%q): %v", payload, err)
		}
	}
	expectNotification := func(want string) {
		t.Helper()
		select {
		case notification, ok := <-listener.NotificationChannel():
			if !ok {
				t.Fatal("the notification channel was closed")
			}
			if notification.Channel != channel || notification.Extra != want {
				t.Fatalf("notification = %+v, want payload %q on %q", notification, want, channel)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("no notification with payload %q arrived", want)
		}
	}
	expectNoNotification := func(why string) {
		t.Helper()
		select {
		case notification := <-listener.NotificationChannel():
			t.Fatalf("unexpected notification %+v: %s", notification, why)
		case <-time.After(500 * time.Millisecond):
		}
	}

	// A committed NOTIFY from another session reaches the listener.
	publish("committed")
	expectNotification("committed")

	// A listener can notify itself; the session is autocommit, so this commits
	// immediately.
	if err := listener.Notify(channel, "self"); err != nil {
		t.Fatalf("Listener.Notify: %v", err)
	}
	expectNotification("self")

	// A NOTIFY inside a rolled-back transaction is never delivered.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SELECT pg_notify($1, $2)", channel, "rolled back"); err != nil {
		t.Fatalf("pg_notify in a transaction: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	expectNoNotification("a rolled-back NOTIFY must not be delivered")

	// A NOTIFY inside a committed transaction is delivered at commit.
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SELECT pg_notify($1, $2)", channel, "committed in tx"); err != nil {
		t.Fatalf("pg_notify in a transaction: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	expectNotification("committed in tx")

	// After UNLISTEN the session receives nothing more.
	if err := listener.Unlisten(channel); err != nil {
		t.Fatalf("Unlisten: %v", err)
	}
	publish("after unlisten")
	expectNoNotification("the channel was unlistened")
}

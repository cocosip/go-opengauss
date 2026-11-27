package opengauss

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// liveTableName returns a table name that no other run can already own. The
// process id alone repeats across runs, so the counter and the start time make
// the name unique and a leftover table cannot disturb a fresh run.
func liveTableName(prefix string) string {
	return fmt.Sprintf("%s_%d_%d_%d", prefix, os.Getpid(), liveTableSeq.Add(1), liveRunStamp)
}

// liveRunStamp identifies this process, so a name from an earlier run is never
// mistaken for one of this run's tables.
var liveRunStamp = time.Now().UnixNano() % 1_000_000

// liveTableSeq numbers the tables a single run creates.
var liveTableSeq atomic.Int32

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

	// The banner is the only product signal: openGauss reports a
	// PostgreSQL-compatible server_version.
	detected, err := DetectServerVersion(ctx, pinned)
	if err != nil {
		t.Fatalf("DetectServerVersion: %v", err)
	}
	t.Logf("product version: %s", detected)
	if detected.Product == "" || detected.Major == 0 {
		t.Errorf("the banner %q did not name a product and version", detected.Raw)
	}

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

// TestLiveMultiVectorSearch runs the F11 acceptance steps against a real
// server: a vector table, two search vectors in parallel, scan settings applied
// to each session and an ordered per-item result. It is skipped unless
// OPENGAUSS_DSN is set and the server provides the vector type.
func TestLiveMultiVectorSearch(t *testing.T) {
	dsn := os.Getenv("OPENGAUSS_DSN")
	if dsn == "" {
		t.Skip("set OPENGAUSS_DSN to run against a live openGauss server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	connector, err := NewConnector(dsn)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)

	// The table is permanent because each worker searches on its own session
	// and temporary tables are visible only to their creator.
	table := liveTableName("opengauss_live_vector")
	if _, err := db.ExecContext(ctx, "CREATE TABLE "+table+" (id integer, embedding vector(3))"); err != nil {
		t.Skipf("the server does not provide the vector type: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS "+table)
	}()

	for i, vector := range []string{"[1,0,0]", "[0,1,0]", "[0,0,1]"} {
		if _, err := db.ExecContext(ctx, "INSERT INTO "+table+" (id, embedding) VALUES ($1, $2::vector)", i+1, vector); err != nil {
			t.Fatalf("INSERT vector %d: %v", i+1, err)
		}
	}

	query := "SELECT id FROM " + table + " ORDER BY embedding <-> $1 LIMIT $2"
	results, err := ExecuteMultiSearch(ctx, db, query,
		[][]any{{"[1,0,0]", 2}, {"[0,1,0]", 2}},
		map[string]any{"enable_seqscan": "on"}, 2)
	if err != nil {
		t.Fatalf("ExecuteMultiSearch: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
	for i, want := range []int64{1, 2} {
		if results[i].Err != nil {
			t.Fatalf("result %d: %v", i, results[i].Err)
		}
		if len(results[i].Rows) != 2 {
			t.Fatalf("result %d rows = %d, want the requested limit", i, len(results[i].Rows))
		}
		id, ok := results[i].Rows[0]["id"].(int64)
		if !ok || id != want {
			t.Errorf("result %d nearest id = %v, want %d", i, results[i].Rows[0]["id"], want)
		}
	}
}

// TestLiveCopyOut reads a COPY ... TO STDOUT stream from a real server: a NULL
// field, a tab inside a value and an early close that must leave the session
// usable for the next statement.
func TestLiveCopyOut(t *testing.T) {
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

	table := liveTableName("opengauss_live_copy_out")
	if _, err := db.ExecContext(ctx, "CREATE TABLE "+table+" (id integer, label text)"); err != nil {
		t.Skipf("cannot create a table: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS "+table)
	}()
	if _, err := db.ExecContext(ctx, "INSERT INTO "+table+" (id, label) VALUES (1, 'one'), (2, NULL), (3, 'tab\there')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	rows, err := db.QueryContext(ctx, "COPY "+table+" TO STDOUT")
	if err != nil {
		t.Fatalf("COPY TO STDOUT: %v", err)
	}
	type record struct {
		id    string
		label any
	}
	var got []record
	for rows.Next() {
		var id string
		var label any
		if err := rows.Scan(&id, &label); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, record{id: id, label: label})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	want := []record{{id: "1", label: "one"}, {id: "2", label: nil}, {id: "3", label: "tab\there"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}

	// A caller that stops early must leave the session synchronized.
	rows, err = db.QueryContext(ctx, "COPY "+table+" TO STDOUT")
	if err != nil {
		t.Fatalf("second COPY TO STDOUT: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("second stream returned no row: %v", rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close after one row: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("query after early close: %v", err)
	}
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}

	// CSV output: the header line is skipped, NULL is an empty unquoted field,
	// and a separator and a quote are quoted with a doubled quote.
	if _, err := db.ExecContext(ctx, "INSERT INTO "+table+" (id, label) VALUES (4, 'comma, \"quote\"')"); err != nil {
		t.Fatalf("INSERT for CSV: %v", err)
	}
	rows, err = db.QueryContext(ctx, "COPY "+table+" TO STDOUT WITH (FORMAT csv, HEADER)")
	if err != nil {
		t.Fatalf("COPY TO STDOUT csv: %v", err)
	}
	var csvRows [][]any
	for rows.Next() {
		var id, label any
		if err := rows.Scan(&id, &label); err != nil {
			t.Fatalf("Scan CSV: %v", err)
		}
		csvRows = append(csvRows, []any{id, label})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err CSV: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close CSV: %v", err)
	}
	wantCSV := [][]any{{"1", "one"}, {"2", nil}, {"3", "tab\there"}, {"4", `comma, "quote"`}}
	if !reflect.DeepEqual(csvRows, wantCSV) {
		t.Fatalf("CSV rows = %#v, want %#v", csvRows, wantCSV)
	}

	// CSV input: the same quoting is written back through CopyInWithOptions.
	csvTable := table + "_in"
	if _, err := db.ExecContext(ctx, "CREATE TABLE "+csvTable+" (id integer, label text)"); err != nil {
		t.Fatalf("CREATE TABLE for CSV input: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS "+csvTable)
	}()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	copyIn, err := CopyInWithOptions(tx, csvTable, CopyInOptions{Format: CopyFormatCSV}, "id", "label")
	if err != nil {
		t.Fatalf("CopyInWithOptions: %v", err)
	}
	wantIn := [][]any{{int64(1), "one"}, {int64(2), nil}, {int64(3), "line\nbreak"}, {int64(4), `comma, "quote"`}}
	for i, row := range wantIn {
		if err := copyIn.Exec(row...); err != nil {
			t.Fatalf("CSV Exec row %d: %v", i+1, err)
		}
	}
	if err := copyIn.Close(); err != nil {
		t.Fatalf("CSV Close: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("CSV Commit: %v", err)
	}
	var gotIn []any
	rows, err = db.QueryContext(ctx, "SELECT label FROM "+csvTable+" ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT after CSV input: %v", err)
	}
	for rows.Next() {
		var label any
		if err := rows.Scan(&label); err != nil {
			t.Fatalf("Scan after CSV input: %v", err)
		}
		gotIn = append(gotIn, label)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close after CSV input: %v", err)
	}
	wantLabels := []any{"one", nil, "line\nbreak", `comma, "quote"`}
	if !reflect.DeepEqual(gotIn, wantLabels) {
		t.Fatalf("labels = %#v, want %#v", gotIn, wantLabels)
	}

	// Binary output returns the raw field bytes: an int4 is four big-endian
	// bytes and text is UTF-8, because the protocol names no types.
	rows, err = db.QueryContext(ctx, "COPY "+table+" (id, label) TO STDOUT WITH (FORMAT binary)")
	if err != nil {
		t.Fatalf("COPY TO STDOUT binary: %v", err)
	}
	binaryLabels := map[uint32]any{}
	for rows.Next() {
		var id, label any
		if err := rows.Scan(&id, &label); err != nil {
			t.Fatalf("Scan binary: %v", err)
		}
		raw, ok := id.([]byte)
		if !ok || len(raw) != 4 {
			t.Fatalf("binary id = %#v, want four bytes", id)
		}
		binaryLabels[binary.BigEndian.Uint32(raw)] = label
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err binary: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close binary: %v", err)
	}
	wantBinary := map[uint32]any{
		1: []byte("one"),
		2: nil,
		3: []byte("tab\there"),
		4: []byte(`comma, "quote"`),
	}
	if !reflect.DeepEqual(binaryLabels, wantBinary) {
		t.Fatalf("binary labels = %#v, want %#v", binaryLabels, wantBinary)
	}
}

// TestLiveEncodings verifies the UTF-8 client contract and a non-ASCII round
// trip against a real server, including bytea bytes that are not valid UTF-8.
func TestLiveEncodings(t *testing.T) {
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

	var encoding string
	if err := db.QueryRowContext(ctx, "SHOW client_encoding").Scan(&encoding); err != nil {
		t.Fatalf("SHOW client_encoding: %v", err)
	}
	if !strings.EqualFold(encoding, "UTF8") {
		t.Fatalf("client_encoding = %q, want UTF8", encoding)
	}

	const text = "héllo — 世界 🚀"
	var gotText string
	if err := db.QueryRowContext(ctx, "SELECT $1::text", text).Scan(&gotText); err != nil {
		t.Fatalf("non-ASCII round trip: %v", err)
	}
	if gotText != text {
		t.Errorf("text = %q, want %q", gotText, text)
	}

	raw := []byte{0xff, 0x00, 0x80}
	var gotRaw []byte
	if err := db.QueryRowContext(ctx, "SELECT $1::bytea", `\x`+hex.EncodeToString(raw)).Scan(&gotRaw); err != nil {
		t.Fatalf("bytea round trip: %v", err)
	}
	if !reflect.DeepEqual(gotRaw, raw) {
		t.Errorf("bytea = %x, want %x", gotRaw, raw)
	}

	var gotNull any = "sentinel"
	if err := db.QueryRowContext(ctx, "SELECT NULL::text").Scan(&gotNull); err != nil {
		t.Fatalf("NULL round trip: %v", err)
	}
	if gotNull != nil {
		t.Errorf("NULL = %v, want nil", gotNull)
	}
}

// TestLiveMultipleResultSets verifies a simple query batch on a live server: the
// first row-producing statement yields the rows, statements without rows are
// skipped and NextResultSet advances to the next set.
func TestLiveMultipleResultSets(t *testing.T) {
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

	rows, err := db.QueryContext(ctx, "SET application_name = 'multi-result'; SELECT 1; SELECT 'two'")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("first set: %v", rows.Err())
	}
	var first int64
	if err := rows.Scan(&first); err != nil || first != 1 {
		t.Fatalf("first value = %d, err = %v", first, err)
	}
	if rows.Next() {
		t.Fatal("the first set returned an extra row")
	}
	if !rows.NextResultSet() {
		t.Fatalf("NextResultSet to the second set = false, err = %v", rows.Err())
	}
	if !rows.Next() {
		t.Fatalf("second set: %v", rows.Err())
	}
	var second string
	if err := rows.Scan(&second); err != nil || second != "two" {
		t.Fatalf("second value = %q, err = %v", second, err)
	}
	if rows.Next() {
		t.Fatal("the second set returned an extra row")
	}
	if rows.NextResultSet() {
		t.Fatal("a third set was reported")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
}

// TestLiveOfficialExamples runs the official openGauss Go guide's common
// operations against a live server: DDL, a prepared insert loop, a prepared
// update, row iteration, a scalar function, a manual transaction and a JSONB
// round trip. Every object it creates carries a unique suffix and is dropped
// again, so the DSN may point at a shared disposable database.
func TestLiveOfficialExamples(t *testing.T) {
	dsn := os.Getenv("OPENGAUSS_DSN")
	if dsn == "" {
		t.Skip("set OPENGAUSS_DSN to run against a live openGauss server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	connector, err := NewConnector(dsn)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	// Cleanup must not block on a lock held by a failed test, so it uses a
	// bounded context instead of an unbounded one.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cleanupCancel()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	table := liveTableName("customer_t1")
	jsonTable := liveTableName("t_jsonb")
	funcName := "testproc_" + suffix
	insertSQL := "INSERT INTO " + table + " (c_customer_sk, c_customer_name) VALUES ($1, $2)"

	if _, err := db.ExecContext(ctx, "CREATE TABLE "+table+" (c_customer_sk INTEGER, c_customer_name VARCHAR(32))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer func() {
		if _, err := db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Logf("drop %s: %v", table, err)
		}
	}()

	stmt, err := db.PrepareContext(ctx, insertSQL)
	if err != nil {
		t.Fatalf("PrepareContext: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := stmt.ExecContext(ctx, i, fmt.Sprintf("data %d", i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if err := stmt.Close(); err != nil {
		t.Fatalf("stmt.Close: %v", err)
	}

	res, err := db.ExecContext(ctx, "UPDATE "+table+" SET c_customer_name = $1 WHERE c_customer_sk = $2", "new Data", 1)
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	if affected, err := res.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("UPDATE RowsAffected = %d, %v; want 1, nil", affected, err)
	}

	rows, err := db.QueryContext(ctx, "SELECT c_customer_sk, c_customer_name FROM "+table+" ORDER BY c_customer_sk")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	wantNames := []string{"data 0", "new Data", "data 2"}
	var index int
	for rows.Next() {
		var sk int
		var name string
		if err := rows.Scan(&sk, &name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if sk != index || name != wantNames[index] {
			t.Fatalf("row %d = %d/%q, want %d/%q", index, sk, name, index, wantNames[index])
		}
		index++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows close: %v", err)
	}
	if index != len(wantNames) {
		t.Fatalf("read %d rows, want %d", index, len(wantNames))
	}

	createFunction := "CREATE OR REPLACE FUNCTION " + funcName + "(a int, b int, c int) RETURNS int " +
		"AS $$ BEGIN RETURN a + b + c; END; $$ LANGUAGE plpgsql"
	if _, err := db.ExecContext(ctx, createFunction); err != nil {
		t.Fatalf("CREATE FUNCTION: %v", err)
	}
	defer func() {
		if _, err := db.ExecContext(cleanupCtx, "DROP FUNCTION IF EXISTS "+funcName+"(int, int, int)"); err != nil {
			t.Logf("drop function %s: %v", funcName, err)
		}
	}()
	var sum int
	if err := db.QueryRowContext(ctx, "SELECT "+funcName+"($1, $2, $3)", 20, 50, 90).Scan(&sum); err != nil {
		t.Fatalf("call function: %v", err)
	}
	if sum != 160 {
		t.Errorf("function result = %d, want 160", sum)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	// A failure before Commit or Rollback must not leave the transaction open,
	// because the deferred drops below would then wait on its locks.
	defer func(tx *sql.Tx) { _ = tx.Rollback() }(tx)
	if _, err := tx.ExecContext(ctx, insertSQL, 100, "tx-data"); err != nil {
		t.Fatalf("tx insert: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE "+table+" SET c_customer_name = $1 WHERE c_customer_sk = $2", "tx-updated", 100); err != nil {
		t.Fatalf("tx update: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	var updated string
	if err := db.QueryRowContext(ctx, "SELECT c_customer_name FROM "+table+" WHERE c_customer_sk = 100").Scan(&updated); err != nil {
		t.Fatalf("read committed row: %v", err)
	}
	if updated != "tx-updated" {
		t.Errorf("committed name = %q, want tx-updated", updated)
	}

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx (rollback): %v", err)
	}
	defer func(tx *sql.Tx) { _ = tx.Rollback() }(tx)
	if _, err := tx.ExecContext(ctx, insertSQL, 200, "rolled-back"); err != nil {
		t.Fatalf("rollback insert: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	var rolledBack int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE c_customer_sk = 200").Scan(&rolledBack); err != nil {
		t.Fatalf("count rolled-back row: %v", err)
	}
	if rolledBack != 0 {
		t.Errorf("rolled-back rows = %d, want 0", rolledBack)
	}

	if _, err := db.ExecContext(ctx, "CREATE TABLE "+jsonTable+" (id serial PRIMARY KEY, data jsonb)"); err != nil {
		t.Fatalf("CREATE JSONB TABLE: %v", err)
	}
	defer func() {
		if _, err := db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS "+jsonTable); err != nil {
			t.Logf("drop %s: %v", jsonTable, err)
		}
	}()
	document := `{"name":"Alice","age":30}`
	if _, err := db.ExecContext(ctx, "INSERT INTO "+jsonTable+" (data) VALUES ($1)", document); err != nil {
		t.Fatalf("JSONB insert: %v", err)
	}
	var raw string
	if err := db.QueryRowContext(ctx, "SELECT data FROM "+jsonTable).Scan(&raw); err != nil {
		t.Fatalf("JSONB select: %v", err)
	}
	var decoded struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("Unmarshal(%q): %v", raw, err)
	}
	if decoded.Name != "Alice" || decoded.Age != 30 {
		t.Errorf("decoded payload = %+v, want Alice/30", decoded)
	}
	var id int
	if err := db.QueryRowContext(ctx, "INSERT INTO "+jsonTable+" (data) VALUES ($1) RETURNING id", document).Scan(&id); err != nil {
		t.Fatalf("JSONB RETURNING: %v", err)
	}
	if id != 2 {
		t.Errorf("returned id = %d, want 2", id)
	}
}

// TestLiveTypes runs the F05 acceptance steps against a real server: text-mode
// decoding and ColumnTypes for the types the driver decodes, plus the container
// types it keeps as text. A dolphin-compatible B database changes some of the
// type rules, so those cases are asserted only where the server reports that
// compatibility mode.
func TestLiveTypes(t *testing.T) {
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

	var compatibility string
	if err := db.QueryRowContext(ctx, "SHOW sql_compatibility").Scan(&compatibility); err != nil {
		t.Fatalf("SHOW sql_compatibility: %v", err)
	}
	t.Logf("sql_compatibility = %s", compatibility)

	// checkOne reads one column and compares the value with reflect.DeepEqual,
	// so a nil want asserts NULL and a []byte want asserts exact bytes.
	checkOne := func(t *testing.T, query, wantType string, want any) {
		t.Helper()
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		defer func() { _ = rows.Close() }()
		types, err := rows.ColumnTypes()
		if err != nil {
			t.Fatalf("ColumnTypes: %v", err)
		}
		if len(types) != 1 {
			t.Fatalf("%d columns, want 1", len(types))
		}
		if got := types[0].DatabaseTypeName(); !strings.EqualFold(got, wantType) {
			t.Errorf("DatabaseTypeName = %q, want %q", got, wantType)
		}
		if !rows.Next() {
			t.Fatalf("no row: %v", rows.Err())
		}
		var got any
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("value = %#v (%T), want %#v (%T)", got, got, want, want)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Rows.Err: %v", err)
		}
	}

	t.Run("scalars", func(t *testing.T) {
		checkOne(t, "SELECT true", "bool", true)
		checkOne(t, "SELECT false", "bool", false)
		checkOne(t, "SELECT NULL::bool", "bool", nil)
		checkOne(t, "SELECT 32767::int2", "int2", int64(32767))
		checkOne(t, "SELECT (-2147483648)::int4", "int4", int64(-2147483648))
		checkOne(t, "SELECT 9223372036854775807::int8", "int8", int64(9223372036854775807))
		checkOne(t, "SELECT 0.5::float4", "float4", 0.5)
		checkOne(t, "SELECT (-1.25)::float8", "float8", -1.25)
		checkOne(t, "SELECT 'plain'::text", "text", "plain")
		checkOne(t, "SELECT ''::text", "text", "")
		checkOne(t, "SELECT 'v'::varchar(10)", "varchar", "v")
		checkOne(t, "SELECT 'c'::\"char\"", "char", "c")
		checkOne(t, "SELECT 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid",
			"uuid", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
		checkOne(t, `SELECT '\x00ff80'::bytea`, "bytea", []byte{0x00, 0xff, 0x80})
		checkOne(t, "SELECT ''::bytea", "bytea", []byte{})
		checkOne(t, "SELECT B'1010'", "bit", "1010")
		checkOne(t, "SELECT '1 day 02:03:04'::interval", "interval", "1 day 02:03:04")
		// Numeric keeps every digit as text, so no precision is lost to a float.
		checkOne(t, "SELECT CAST(123456789012345678901234567890 AS numeric(38,0))",
			"numeric", "123456789012345678901234567890")
		checkOne(t, "SELECT 12.345::numeric(8,3)", "numeric", "12.345")
		checkOne(t, "SELECT '2024-02-29'::date", "date",
			time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC))
		// A timestamp with a fractional precision keeps the fraction; the zone
		// the server appends is decoded into a fixed offset, which is what the
		// value below compares against.
		var zoned any
		if err := db.QueryRowContext(ctx,
			"SELECT '2024-02-29 13:14:15.123456'::timestamp(6)").Scan(&zoned); err != nil {
			t.Fatalf("timestamp(6): %v", err)
		}
		stamp, ok := zoned.(time.Time)
		if !ok {
			t.Fatalf("timestamp(6) = %#v, want a time.Time", zoned)
		}
		if stamp.Year() != 2024 || stamp.Month() != time.February || stamp.Day() != 29 ||
			stamp.Hour() != 13 || stamp.Minute() != 14 || stamp.Second() != 15 ||
			stamp.Nanosecond() != 123456000 {
			t.Errorf("timestamp(6) = %v, want 2024-02-29 13:14:15.123456", stamp)
		}
	})

	t.Run("containers stay text", func(t *testing.T) {
		checkOne(t, "SELECT ARRAY[1,2,3]", "int4[]", "{1,2,3}")
		checkOne(t, `SELECT ARRAY['a,b','c"d']`, "text[]", `{"a,b","c\"d"}`)
		checkOne(t, "SELECT ARRAY[[1,2],[3,4]]", "int4[]", "{{1,2},{3,4}}")
		checkOne(t, "SELECT NULL::int4[]", "int4[]", nil)
	})

	// ColumnTypes metadata comes from the type modifier in RowDescription, so it
	// needs real columns: a literal carries no modifier.
	t.Run("column metadata", func(t *testing.T) {
		pinned, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("db.Conn: %v", err)
		}
		defer func() { _ = pinned.Close() }()
		if _, err := pinned.ExecContext(ctx, `CREATE TEMPORARY TABLE opengauss_live_types (
			v10 varchar(10),
			c5 char(5),
			b4 bit(4),
			n83 numeric(8,3),
			stamp timestamptz
		)`); err != nil {
			t.Skipf("cannot create a temporary table: %v", err)
		}
		rows, err := pinned.QueryContext(ctx,
			"SELECT v10, c5, b4, n83, stamp FROM opengauss_live_types")
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		defer func() { _ = rows.Close() }()
		types, err := rows.ColumnTypes()
		if err != nil {
			t.Fatalf("ColumnTypes: %v", err)
		}
		for _, c := range []struct {
			name      string
			wantType  string
			length    int64
			lengthOK  bool
			precision int64
			scale     int64
			scaleOK   bool
		}{
			{name: "v10", wantType: "varchar", length: 10, lengthOK: true},
			{name: "c5", wantType: "bpchar", length: 5, lengthOK: true},
			{name: "b4", wantType: "bit", length: 4, lengthOK: true},
			{name: "n83", wantType: "numeric", precision: 8, scale: 3, scaleOK: true},
			{name: "stamp", wantType: "timestamptz"},
		} {
			column, found := columnByName(types, c.name)
			if !found {
				t.Fatalf("column %s is missing", c.name)
			}
			if !strings.EqualFold(column.DatabaseTypeName(), c.wantType) {
				t.Errorf("%s type = %q, want %q", c.name, column.DatabaseTypeName(), c.wantType)
			}
			if length, ok := column.Length(); ok != c.lengthOK || length != c.length {
				t.Errorf("%s Length = %d, ok %v, want %d, %v", c.name, length, ok, c.length, c.lengthOK)
			}
			precision, scale, ok := column.DecimalSize()
			if ok != c.scaleOK || precision != c.precision || scale != c.scale {
				t.Errorf("%s DecimalSize = %d, %d, %v, want %d, %d, %v",
					c.name, precision, scale, ok, c.precision, c.scale, c.scaleOK)
			}
			// RowDescription carries no nullability, so the driver must report
			// that it does not know rather than guess.
			if nullable, ok := column.Nullable(); ok {
				t.Errorf("%s Nullable = %v, ok %v, want an unknown nullability", c.name, nullable, ok)
			}
		}
	})

	// A B database follows MySQL rules for the spellings it shares, and the
	// driver must continue to report what the server actually sends.
	if strings.EqualFold(compatibility, "B") {
		t.Run("B mode rules", func(t *testing.T) {
			// The bare numeric is decimal(10,0), so a cast rounds it.
			checkOne(t, "SELECT 1.5::numeric", "numeric", "2")
			// The bare timestamp is a timestamptz with zero fractional
			// precision, so the fraction is dropped.
			checkOne(t, "SELECT '2024-02-29 13:14:15.123456'::timestamp", "timestamptz",
				func() any {
					var got any
					if err := db.QueryRowContext(ctx,
						"SELECT '2024-02-29 13:14:15.123456'::timestamp").Scan(&got); err != nil {
						t.Fatalf("timestamp: %v", err)
					}
					return got
				}())
			// The character length syntax of the native modes is a syntax error.
			if _, err := db.ExecContext(ctx, "SELECT 'x'::varchar(7 char)"); err == nil {
				t.Error("varchar(7 char) was accepted in B mode")
			}
		})
	}
}

// columnByName returns the column metadata with the given name.
func columnByName(types []*sql.ColumnType, name string) (*sql.ColumnType, bool) {
	for _, column := range types {
		if column.Name() == name {
			return column, true
		}
	}
	return nil, false
}

// TestLiveVectorTypes runs the F10 acceptance steps against a real server: the
// vector column type is named, its text form round-trips, NULL and a missing
// dimension behave, and a nearest-neighbour order comes back as the distance
// operator defines it.
func TestLiveVectorTypes(t *testing.T) {
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

	// One session, so a temporary table is visible to every statement here.
	pinned, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	defer func() { _ = pinned.Close() }()

	if _, err := pinned.ExecContext(ctx, "CREATE TEMPORARY TABLE opengauss_live_vectors (id integer, embedding vector(3))"); err != nil {
		t.Skipf("the server does not provide the vector type: %v", err)
	}

	rows, err := pinned.QueryContext(ctx, "SELECT id, embedding FROM opengauss_live_vectors")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("ColumnTypes: %v", err)
	}
	if got := types[1].DatabaseTypeName(); !strings.EqualFold(got, "vector") {
		t.Errorf("vector DatabaseTypeName = %q, want vector", got)
	}
	// The driver decodes a vector as its text form, so a scan reports a string.
	if got := types[1].ScanType(); got == nil || got.Kind().String() != "string" {
		t.Errorf("vector ScanType = %v, want a string", got)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The text form is what the official guide binds, and it round-trips.
	for i, value := range []string{"[1,0,0]", "[0,1,0]", "[0,0,1]"} {
		if _, err := pinned.ExecContext(ctx, "INSERT INTO opengauss_live_vectors (id, embedding) VALUES ($1, $2::vector)", i+1, value); err != nil {
			t.Fatalf("INSERT vector %q: %v", value, err)
		}
	}
	if _, err := pinned.ExecContext(ctx, "INSERT INTO opengauss_live_vectors (id, embedding) VALUES ($1, NULL)", 4); err != nil {
		t.Fatalf("INSERT NULL vector: %v", err)
	}
	var gotText string
	if err := pinned.QueryRowContext(ctx, "SELECT embedding::text FROM opengauss_live_vectors WHERE id = 1").Scan(&gotText); err != nil {
		t.Fatalf("vector as text: %v", err)
	}
	if gotText != "[1,0,0]" {
		t.Errorf("vector text = %q, want [1,0,0]", gotText)
	}
	var nullVector *string
	if err := pinned.QueryRowContext(ctx, "SELECT embedding::text FROM opengauss_live_vectors WHERE id = 4").Scan(&nullVector); err != nil {
		t.Fatalf("NULL vector: %v", err)
	}
	if nullVector != nil {
		t.Errorf("NULL vector = %q, want NULL", *nullVector)
	}

	// A vector of the wrong width is rejected by the server, not silently
	// truncated by the driver.
	if _, err := pinned.ExecContext(ctx, "INSERT INTO opengauss_live_vectors (id, embedding) VALUES ($1, $2::vector)", 5, "[1,2]"); err == nil {
		t.Error("a two-dimensional vector was accepted by a vector(3) column")
	}

	// The distance operator orders the rows, and the nearest one comes first.
	rows, err = pinned.QueryContext(ctx, "SELECT id FROM opengauss_live_vectors WHERE embedding IS NOT NULL ORDER BY embedding <-> $1::vector LIMIT 2", "[0,1,0]")
	if err != nil {
		t.Fatalf("nearest-neighbour query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var nearest []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		nearest = append(nearest, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
	if want := []int64{2, 1}; !reflect.DeepEqual(nearest, want) {
		t.Errorf("nearest ids = %v, want %v", nearest, want)
	}
}

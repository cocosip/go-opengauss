package opengauss

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cocosip/go-opengauss/internal/proto"
)

func TestCopyInSQL(t *testing.T) {
	tests := []struct {
		table   string
		columns []string
		want    string
	}{
		{"logs", nil, `COPY "logs" FROM STDIN`},
		{"logs", []string{"id", "msg"}, `COPY "logs" ("id", "msg") FROM STDIN`},
		{"public.logs", []string{"id"}, `COPY "public"."logs" ("id") FROM STDIN`},
		{`we"ird`, nil, `COPY "we""ird" FROM STDIN`},
	}
	for _, tc := range tests {
		if got := copyInSQL(tc.table, tc.columns); got != tc.want {
			t.Errorf("copyInSQL(%q, %v) = %q, want %q", tc.table, tc.columns, got, tc.want)
		}
	}
}

func TestIsCopyFromStdin(t *testing.T) {
	tests := []struct {
		statement string
		want      bool
	}{
		{"COPY logs FROM STDIN", true},
		{"  copy logs (a, b) from stdin", true},
		{"COPY logs TO STDOUT", false},
		{"COPY logs FROM 'file.txt'", false},
		{"INSERT INTO logs VALUES ($1)", false},
		{"copycat", false},
	}
	for _, tc := range tests {
		if got := isCopyFromStdin(tc.statement); got != tc.want {
			t.Errorf("isCopyFromStdin(%q) = %v, want %v", tc.statement, got, tc.want)
		}
	}
}

func TestEncodeCopyRow(t *testing.T) {
	row, err := encodeCopyRow(nil, namedValues([]driver.Value{
		nil,
		"plain",
		"tab\there\nnewline\\backslash\r",
		int64(42),
		true,
	}))
	if err != nil {
		t.Fatalf("encodeCopyRow: %v", err)
	}
	want := "\\N\tplain\ttab\\there\\nnewline\\\\backslash\\r\t42\t1\n"
	if string(row) != want {
		t.Errorf("row = %q, want %q", row, want)
	}

	if _, err := encodeCopyRow(nil, namedValues([]driver.Value{struct{}{}})); err == nil {
		t.Error("expected an unsupported value type to fail")
	}
}

func TestCopyInStreamsRows(t *testing.T) {
	rowsCh := make(chan []string, 1)
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		if query := s.expectSimpleQuery(); query != "BEGIN" {
			s.fail("begin query = %q", query)
		}
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		if query := s.expectSimpleQuery(); query != `COPY "logs" ("id", "msg") FROM STDIN` {
			s.fail("copy query = %q", query)
		}
		s.sendCopyInResponse(0, 2)
		rowsCh <- s.readCopyRows()
		s.sendCommandComplete("COPY 2")
		s.sendReady(proto.TxnInTransaction)

		if query := s.expectSimpleQuery(); query != "COMMIT" {
			s.fail("commit query = %q", query)
		}
		s.sendCommandComplete("COMMIT")
		s.sendReady(proto.TxnIdle)

		// The committed session returns to the pool unchanged.
		if query := s.expectSimpleQuery(); query != "" {
			s.fail("reuse query = %q", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	defer func() { _ = db.Close() }()
	ctx := testContext(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	copyIn, err := CopyIn(tx, "logs", "id", "msg")
	if err != nil {
		t.Fatalf("CopyIn: %v", err)
	}
	for i, message := range []string{"hello", "world"} {
		if err := copyIn.Exec(int64(i+1), message); err != nil {
			t.Fatalf("Exec row %d: %v", i+1, err)
		}
	}
	if err := copyIn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}

	rows := <-rowsCh
	want := []string{"1\thello\n", "2\tworld\n"}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("rows = %q, want %q", rows, want)
	}
	if server.connections() != 1 {
		t.Errorf("connections = %d, want the stream to reuse one session", server.connections())
	}
}

func TestCopyInFinishesWithExecWithoutValues(t *testing.T) {
	rowsCh := make(chan []string, 1)
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCopyInResponse(0, 1)
		rowsCh <- s.readCopyRows()
		s.sendCommandComplete("COPY 1")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	defer func() { _ = db.Close() }()
	ctx := testContext(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	copyIn, err := CopyIn(tx, "plain", "v")
	if err != nil {
		t.Fatalf("CopyIn: %v", err)
	}
	if err := copyIn.Exec("value"); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := copyIn.Exec(); err != nil {
		t.Fatalf("finishing Exec: %v", err)
	}
	if err := copyIn.Close(); err != nil {
		t.Fatalf("Close after finishing: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if rows := <-rowsCh; !reflect.DeepEqual(rows, []string{"value\n"}) {
		t.Errorf("rows = %q", rows)
	}
}

func TestCopyInRejectsColumnCountMismatch(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCopyInResponse(0, 2)
		s.readCopyRows() // nothing was written
		s.sendCommandComplete("COPY 0")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	defer func() { _ = db.Close() }()
	ctx := testContext(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	copyIn, err := CopyIn(tx, "logs", "id", "msg")
	if err != nil {
		t.Fatalf("CopyIn: %v", err)
	}
	err = copyIn.Exec(1, "a", "extra")
	if err == nil {
		t.Fatal("expected the row width to be rejected")
	}
	if !strings.Contains(err.Error(), "2 columns but the row has 3") {
		t.Errorf("error = %v, want the column count", err)
	}
	// The stream is still synchronized and can be finished with no rows.
	if err := copyIn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
}

func TestCopyInReportsServerErrorAndKeepsSession(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCopyInResponse(0, 1)
		// The constraint fails on the first row, while the client may still be
		// streaming: the server discards the rest until CopyDone.
		if typ, _ := s.readMessage(); typ != proto.MsgCopyData {
			s.fail("expected the first row, got %q", typ)
			return
		}
		s.sendError("23505", `duplicate key value violates unique constraint "logs_pkey"`)
		for {
			typ, _ := s.readMessage()
			if s.err != nil {
				return
			}
			if typ == proto.MsgCopyDone {
				break
			}
			if typ != proto.MsgCopyData {
				s.fail("unexpected message %q after the COPY error", typ)
				return
			}
		}
		s.sendReady(proto.TxnFailed)

		s.expectSimpleQuery()
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)

		// The synchronized session is reusable after the rollback.
		s.expectSimpleQuery()
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	defer func() { _ = db.Close() }()
	ctx := testContext(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	copyIn, err := CopyIn(tx, "logs", "id")
	if err != nil {
		t.Fatalf("CopyIn: %v", err)
	}
	if err := copyIn.Exec(1); err != nil {
		t.Fatalf("first row: %v", err)
	}
	if err := copyIn.Exec(1); err != nil {
		t.Fatalf("second row: %v", err)
	}
	err = copyIn.Close()
	if err == nil {
		t.Fatal("expected the duplicate key error")
	}
	var serverErr *Error
	if !errors.As(err, &serverErr) {
		t.Fatalf("error = %v, want *Error", err)
	}
	if serverErr.Code != "23505" {
		t.Errorf("SQLSTATE = %q, want 23505", serverErr.Code)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if server.connections() != 1 {
		t.Errorf("connections = %d, want the session to stay usable", server.connections())
	}
}

func TestCopyInCanceledStreamStaysSynchronized(t *testing.T) {
	rowsReceived := make(chan struct{})
	var server *fakeServer
	server = newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCopyInResponse(0, 1)
		if typ, _ := s.readMessage(); typ != proto.MsgCopyData {
			s.fail("expected one row before the cancellation, got %q", typ)
			return
		}
		close(rowsReceived)

		record := server.waitCancel(t)
		if record.pid != 4242 || record.key != 9001 {
			s.fail("cancel identity = pid %d key %d", record.pid, record.key)
		}
		// The server aborts the COPY and discards the rest of the stream.
		s.sendError("57014", "canceling statement due to user request")
		for {
			typ, _ := s.readMessage()
			if s.err != nil {
				return
			}
			if typ == proto.MsgCopyDone {
				break
			}
		}
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	copyIn, err := CopyIn(tx, "logs", "id")
	if err != nil {
		t.Fatalf("CopyIn: %v", err)
	}
	if err := copyIn.ExecContext(ctx, 1); err != nil {
		t.Fatalf("first row: %v", err)
	}
	<-rowsReceived
	cancel()

	err = copyIn.Close()
	if err == nil {
		t.Fatal("expected the canceled stream to fail")
	}
	var serverErr *Error
	if errors.As(err, &serverErr) && serverErr.Code != "57014" {
		t.Errorf("SQLSTATE = %q, want 57014", serverErr.Code)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if server.connections() != 1 {
		t.Errorf("connections = %d, want the synchronized session reused", server.connections())
	}
}

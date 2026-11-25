package opengauss

import (
	"bytes"
	"encoding/hex"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cocosip/go-opengauss/internal/proto"
)

func TestSupportedClientEncoding(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"UTF8", true},
		{"utf8", true},
		{"SQL_ASCII", true},
		{"sql_ascii", true},
		{"GBK", false},
		{"LATIN1", false},
		{"UNICODE", false},
		{"", false},
	}
	for _, tc := range tests {
		if got := supportedClientEncoding(tc.name); got != tc.want {
			t.Errorf("supportedClientEncoding(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestStartupRejectsUnsupportedClientEncoding(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartupEncoding("GBK", nil)
	})

	connector, err := NewConnector(testDSN(server, ""))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	raw, err := connector.Connect(testContext(t))
	if err == nil {
		_ = raw.Close()
		t.Fatal("connect succeeded with client_encoding GBK")
	}
	if !strings.Contains(err.Error(), "client_encoding") || !strings.Contains(err.Error(), "GBK") {
		t.Fatalf("connect error = %v, want the unsupported client encoding", err)
	}
}

func TestStartupAcceptsSQLASCIIClientEncoding(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartupEncoding("SQL_ASCII", nil)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestRuntimeClientEncodingChangeIsRejected(t *testing.T) {
	var connections atomic.Int32
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if connections.Add(1) == 1 {
			if query := s.expectSimpleQuery(); query != "SET client_encoding TO 'GBK'" {
				s.fail("query = %q", query)
				return
			}
			s.sendCommandComplete("SET")
			s.sendParameterStatus("client_encoding", "GBK")
			s.sendReady(proto.TxnIdle)
			return
		}
		if query := s.expectSimpleQuery(); query != "SELECT 1" {
			s.fail("query = %q", query)
			return
		}
		s.sendRowDescription(fakeColumn{name: "?column?", oid: 23})
		s.sendDataRow("1")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	if _, err := db.ExecContext(ctx, "SET client_encoding TO 'GBK'"); err == nil ||
		!strings.Contains(err.Error(), "client_encoding") {
		t.Fatalf("SET client_encoding = %v, want an encoding error", err)
	}
	// The session is discarded, so the next statement runs fresh with UTF8.
	if _, err := db.ExecContext(ctx, "SELECT 1"); err != nil {
		t.Fatalf("Exec after the encoding change: %v", err)
	}
	if connections.Load() != 2 {
		t.Errorf("connections = %d, want the changed session discarded", connections.Load())
	}
}

// TestQueryRejectsRuntimeClientEncodingChange covers the Query path: the server
// sends CommandComplete before the ParameterStatus of a SET, so the change is
// consumed while the empty result is finished.
func TestQueryRejectsRuntimeClientEncodingChange(t *testing.T) {
	var connections atomic.Int32
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if connections.Add(1) == 1 {
			if query := s.expectSimpleQuery(); query != "SET client_encoding TO 'GBK'" {
				s.fail("query = %q", query)
				return
			}
			s.sendCommandComplete("SET")
			s.sendParameterStatus("client_encoding", "GBK")
			s.sendReady(proto.TxnIdle)
			return
		}
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, "SET client_encoding TO 'GBK'")
	if err == nil {
		_ = rows.Close()
		t.Fatal("Query succeeded with a changed client encoding")
	}
	if !strings.Contains(err.Error(), "client_encoding") {
		t.Fatalf("Query error = %v, want the unsupported client encoding", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping after the encoding change: %v", err)
	}
	if connections.Load() != 2 {
		t.Errorf("connections = %d, want the changed session discarded", connections.Load())
	}
}

// TestRowsAbortOnRuntimeClientEncodingChange proves that a change arriving in
// the middle of a streaming result stops the rows instead of returning text
// decoded under the wrong encoding.
func TestRowsAbortOnRuntimeClientEncodingChange(t *testing.T) {
	var connections atomic.Int32
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if connections.Add(1) == 1 {
			if query := s.expectSimpleQuery(); query != "SELECT v FROM t" {
				s.fail("query = %q", query)
				return
			}
			s.sendRowDescription(fakeColumn{name: "v", oid: 25})
			s.sendDataRow("héllo")
			s.sendParameterStatus("client_encoding", "GBK")
			// The client aborts the stream on the parameter status, so nothing
			// further is written; the remaining text must never be decoded.
			return
		}
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, "SELECT v FROM t")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("first row missing, err = %v", rows.Err())
	}
	var first string
	if err := rows.Scan(&first); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if first != "héllo" {
		t.Errorf("first row = %q", first)
	}
	if rows.Next() {
		t.Fatal("a row decoded from the changed encoding was returned")
	}
	if err := rows.Err(); err == nil || !strings.Contains(err.Error(), "client_encoding") {
		t.Fatalf("Rows.Err = %v, want the unsupported client encoding", err)
	}
	_ = rows.Close()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping after the encoding change: %v", err)
	}
	if connections.Load() != 2 {
		t.Errorf("connections = %d, want the changed session discarded", connections.Load())
	}
}

// TestCopyOutAbortsOnRuntimeClientEncodingChange covers the COPY OUT stream: the
// remaining copy data must not be decoded after the encoding changed.
func TestCopyOutAbortsOnRuntimeClientEncodingChange(t *testing.T) {
	var connections atomic.Int32
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if connections.Add(1) == 1 {
			if query := s.expectSimpleQuery(); query != "COPY t TO STDOUT" {
				s.fail("query = %q", query)
				return
			}
			s.sendCopyOutResponse(0, 1)
			s.sendCopyData("ok\n")
			s.sendParameterStatus("client_encoding", "GBK")
			s.sendCopyData("\xd6\xd0\n")
			s.sendCopyDone()
			s.sendCommandComplete("COPY 2")
			s.sendReady(proto.TxnIdle)
			return
		}
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, "COPY t TO STDOUT")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("first row missing, err = %v", rows.Err())
	}
	var first string
	if err := rows.Scan(&first); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if first != "ok" {
		t.Errorf("first row = %q", first)
	}
	if rows.Next() {
		t.Fatal("a copy row decoded from the changed encoding was returned")
	}
	if err := rows.Err(); err == nil || !strings.Contains(err.Error(), "client_encoding") {
		t.Fatalf("Rows.Err = %v, want the unsupported client encoding", err)
	}
	_ = rows.Close()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping after the encoding change: %v", err)
	}
	if connections.Load() != 2 {
		t.Errorf("connections = %d, want the changed session discarded", connections.Load())
	}
}

// TestSQLASCIITextBytesPassThrough proves that a SQL_ASCII session returns the
// stored bytes unchanged, even when they are not valid UTF-8.
func TestSQLASCIITextBytesPassThrough(t *testing.T) {
	raw := []byte{0xd6, 0xd0}
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartupEncoding("SQL_ASCII", nil)
		if query := s.expectSimpleQuery(); query != "SELECT v FROM t" {
			s.fail("query = %q", query)
			return
		}
		s.sendRowDescription(fakeColumn{name: "v", oid: 25})
		s.sendDataRow(string(raw))
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	var got string
	if err := db.QueryRowContext(testContext(t), "SELECT v FROM t").Scan(&got); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !bytes.Equal([]byte(got), raw) {
		t.Errorf("text = %x, want %x", got, raw)
	}
}

// TestRowsCloseReportsRuntimeClientEncodingChange covers the drain path: the
// change is consumed while unread rows are discarded, so Close must report it.
func TestRowsCloseReportsRuntimeClientEncodingChange(t *testing.T) {
	var connections atomic.Int32
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if connections.Add(1) == 1 {
			if query := s.expectSimpleQuery(); query != "SELECT v FROM t" {
				s.fail("query = %q", query)
				return
			}
			s.sendRowDescription(fakeColumn{name: "v", oid: 25})
			s.sendDataRow("héllo")
			s.sendParameterStatus("client_encoding", "GBK")
			s.sendReady(proto.TxnIdle)
			return
		}
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, "SELECT v FROM t")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	if err := rows.Close(); err == nil || !strings.Contains(err.Error(), "client_encoding") {
		t.Fatalf("Rows.Close = %v, want the unsupported client encoding", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping after the encoding change: %v", err)
	}
	if connections.Load() != 2 {
		t.Errorf("connections = %d, want the changed session discarded", connections.Load())
	}
}

// TestTransactionKeepsTheEncodingCause proves the encoding error survives the
// transaction wrappers instead of degrading to driver.ErrBadConn.
func TestTransactionKeepsTheEncodingCause(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "BEGIN" {
			s.fail("query = %q", query)
			return
		}
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)
		if query := s.expectSimpleQuery(); query != "SET client_encoding TO 'GBK'" {
			s.fail("query = %q", query)
			return
		}
		s.sendCommandComplete("SET")
		s.sendParameterStatus("client_encoding", "GBK")
		s.sendReady(proto.TxnInTransaction)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SET client_encoding TO 'GBK'"); err == nil ||
		!strings.Contains(err.Error(), "client_encoding") {
		t.Fatalf("Exec = %v, want the unsupported client encoding", err)
	}
	if err := tx.Commit(); err == nil || !strings.Contains(err.Error(), "client_encoding") {
		t.Fatalf("Commit = %v, want the unsupported client encoding", err)
	}
}

// TestUnsupportedClientEncodingIsNotRetriedOverTLS proves sslmode=allow does
// not replay the startup over TLS once the password already reached the server.
func TestUnsupportedClientEncodingIsNotRetriedOverTLS(t *testing.T) {
	var connections atomic.Int32
	server := newFakeServer(t, func(s *fakeSession) {
		connections.Add(1)
		s.completeStartupEncoding("GBK", nil)
	})

	connector, err := NewConnector(testDSN(server, "sslmode=allow"))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	raw, err := connector.Connect(testContext(t))
	if err == nil {
		_ = raw.Close()
		t.Fatal("connect succeeded with client_encoding GBK")
	}
	if connections.Load() != 1 {
		t.Errorf("connections = %d, want no TLS retry after authentication", connections.Load())
	}
	if got := strings.Count(err.Error(), "GBK"); got != 1 {
		t.Errorf("the encoding error appears %d times in %v, want once", got, err)
	}
}

// TestListenerReportsUnsupportedClientEncoding proves a listener session that
// receives a changed client encoding disconnects with that cause instead of
// reporting a malformed message.
func TestListenerReportsUnsupportedClientEncoding(t *testing.T) {
	var connections atomic.Int32
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		switch connections.Add(1) {
		case 1:
			s.expectQuery(`LISTEN "events"`)
			s.sendParameterStatus("client_encoding", "GBK")
		default:
			s.expectQuery(`LISTEN "events"`)
			s.expectType(proto.MsgTerminate)
		}
	})

	callback, events := recordEvents()
	listener := newTestListener(t, testDSN(server, ""), callback)
	if err := listener.Listen("events"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	waitEvent(t, events, ListenerEventConnected)
	event := waitEvent(t, events, ListenerEventDisconnected)
	if event.err == nil || !strings.Contains(event.err.Error(), "client_encoding") {
		t.Fatalf("disconnect error = %v, want the unsupported client encoding", event.err)
	}
	waitEvent(t, events, ListenerEventReconnected)
}

func TestNonASCIITextByteaAndNullRoundTrip(t *testing.T) {
	text := "héllo — 世界 🚀"
	raw := []byte{0xff, 0x00, 0x80}
	const query = "SELECT $1::text, $2::bytea, $3::text"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		gotQuery, args := s.expectExtended()
		if gotQuery != query || len(args) != 3 {
			s.fail("query = %q, args = %+v", gotQuery, args)
			return
		}
		if args[0].text != text {
			s.fail("text parameter = %q", args[0].text)
			return
		}
		if !bytes.Equal([]byte(args[1].text), raw) {
			s.fail("bytea parameter = %x", args[1].text)
			return
		}
		if !args[2].isNull {
			s.fail("third parameter is not NULL")
			return
		}
		s.sendRowDescription(
			fakeColumn{name: "t", oid: 25},
			fakeColumn{name: "b", oid: 17},
			fakeColumn{name: "n", oid: 25},
		)
		s.sendDataRow(text, `\x`+hex.EncodeToString(raw), nil)
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), query, text, raw, nil)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("Next = false, err = %v", rows.Err())
	}
	var (
		gotText  string
		gotBytes []byte
		gotNull  any
	)
	if err := rows.Scan(&gotText, &gotBytes, &gotNull); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if gotText != text {
		t.Errorf("text = %q, want %q", gotText, text)
	}
	if !bytes.Equal(gotBytes, raw) {
		t.Errorf("bytea = %x, want %x", gotBytes, raw)
	}
	if gotNull != nil {
		t.Errorf("NULL = %v, want nil", gotNull)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
}

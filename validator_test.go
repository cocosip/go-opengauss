package opengauss

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/cocosip/go-opengauss/internal/oid"
	"github.com/cocosip/go-opengauss/internal/proto"
)

// TestIsValidRejectsUnusableSessions checks the decision database/sql makes
// before returning a connection to the idle pool: only a healthy, idle session
// may be reused, and ResetSession agrees with it.
func TestIsValidRejectsUnusableSessions(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	healthy := func() *conn {
		return &conn{netConn: client, txnStatus: proto.TxnIdle}
	}

	tests := []struct {
		name   string
		mutate func(*conn)
		want   bool
	}{
		{"healthy idle session", func(*conn) {}, true},
		{"protocol failure", func(c *conn) { c.bad = true }, false},
		{"undecodable client encoding", func(c *conn) { c.encodingErr = errUnsupportedClientEncoding }, false},
		{"open transaction", func(c *conn) { c.txnStatus = proto.TxnInTransaction }, false},
		{"aborted transaction", func(c *conn) { c.txnStatus = proto.TxnFailed }, false},
		{"open COPY stream", func(c *conn) { c.copying = true }, false},
		{"closed socket", func(c *conn) { c.netConn = nil }, false},
	}
	for _, tc := range tests {
		c := healthy()
		tc.mutate(c)
		if got := c.IsValid(); got != tc.want {
			t.Errorf("%s: IsValid() = %v, want %v", tc.name, got, tc.want)
		}
		err := c.ResetSession(context.Background())
		if reset := err == nil; reset != tc.want {
			t.Errorf("%s: ResetSession() = %v, want validity %v", tc.name, err, tc.want)
		}
	}
}

// TestCanceledTransactionRollsBackAndKeepsSession covers the reuse path that
// database/sql takes because the driver implements both SessionResetter and
// Validator: when the transaction context is canceled, the driver rolls the
// transaction back and the session returns to the pool instead of being
// discarded.
func TestCanceledTransactionRollsBackAndKeepsSession(t *testing.T) {
	var served atomic.Int32
	server := newFakeServer(t, func(s *fakeSession) {
		served.Add(1)
		s.completeStartup(nil)
		if got := s.expectSimpleQuery(); got != "BEGIN" {
			s.fail("query = %q, want BEGIN", got)
		}
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)
		if got := s.expectSimpleQuery(); got != "ROLLBACK" {
			s.fail("query = %q, want ROLLBACK", got)
		}
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)
		if got := s.expectSimpleQuery(); got != "SELECT 1" {
			s.fail("query = %q, want SELECT 1", got)
		}
		s.sendRowDescription(fakeColumn{name: "one", oid: oid.Int4})
		s.sendDataRow("1")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	db.SetMaxOpenConns(1)

	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	if _, err := db.BeginTx(ctx, nil); err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	cancel()

	// The single pooled connection is held by the transaction until its
	// rollback finishes, so this query waits for the rollback and then reuses
	// the same session.
	var one int
	if err := db.QueryRowContext(testContext(t), "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("query after canceled transaction: %v", err)
	}
	if one != 1 {
		t.Fatalf("value = %d, want 1", one)
	}
	if got := served.Load(); got != 1 {
		t.Fatalf("server sessions = %d, want 1 (the rolled-back session must be reused)", got)
	}
}

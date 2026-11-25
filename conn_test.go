package opengauss

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"database/sql/driver"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/cocosip/go-opengauss/internal/auth"
	"github.com/cocosip/go-opengauss/internal/proto"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func testDSN(server *fakeServer, extra string) string {
	return fmt.Sprintf("host=127.0.0.1 port=%d user=%s password=%s dbname=%s sslmode=disable %s",
		server.port(), testUser, testPassword, testDatabase, extra)
}

func openTestDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("opengauss", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestSQLOpenPingAndSimpleQuery(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		if query := s.expectSimpleQuery(); query != "" {
			s.fail("ping used query %q, want the empty statement", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)

		if query := s.expectSimpleQuery(); query != "SELECT 1" {
			s.fail("query = %q, want SELECT 1", query)
		}
		s.sendRowDescription(fakeColumn{name: "?column?", oid: 23})
		s.sendDataRow("1")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	var got int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&got); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if got != 1 {
		t.Errorf("scanned %d, want 1", got)
	}
}

func TestStartupKeepsSessionState(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(func(params map[string]string) {
			if params["user"] != testUser {
				s.fail("startup user = %q", params["user"])
			}
			if params["database"] != testDatabase {
				s.fail("startup database = %q", params["database"])
			}
			if params["client_encoding"] != "UTF8" {
				s.fail("startup client_encoding = %q", params["client_encoding"])
			}
		})
	})

	connector, err := NewConnector(testDSN(server, ""))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	raw, err := connector.Connect(testContext(t))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	physical, ok := raw.(*conn)
	if !ok {
		t.Fatalf("Connect returned %T, want *conn", raw)
	}
	defer func() { _ = physical.Close() }()

	if got := physical.parameters["server_version"]; got != "7.0.0" {
		t.Errorf("server_version = %q, want 7.0.0", got)
	}
	if physical.backendPID != 4242 || physical.backendKey != 9001 {
		t.Errorf("backend key = %d/%d, want 4242/9001", physical.backendPID, physical.backendKey)
	}
	if physical.txnStatus != proto.TxnIdle {
		t.Errorf("transaction status = %q, want idle", physical.txnStatus)
	}
}

func TestBoundIntegerParameter(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		query, args := s.expectExtended()
		if query != "SELECT $1::integer" {
			s.fail("query = %q", query)
		}
		if len(args) != 1 || args[0].isNull || args[0].text != "42" {
			s.fail("bound parameters = %+v, want text 42", args)
		}
		s.sendParseComplete()
		s.sendBindComplete()
		s.sendRowDescription(fakeColumn{name: "int4", oid: 23})
		s.sendDataRow("42")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	var got int
	if err := db.QueryRowContext(testContext(t), "SELECT $1::integer", 42).Scan(&got); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if got != 42 {
		t.Errorf("scanned %d, want 42", got)
	}
}

func TestPreparedStatementExec(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		name, query := s.expectPrepare()
		if query != "INSERT INTO t VALUES ($1)" {
			s.fail("prepared query = %q", query)
		}
		if name == "" {
			s.fail("prepared statement name is empty")
		}
		describe := s.expectType(proto.MsgDescribe)
		dr := &proto.ReadBuf{B: describe}
		if target := dr.Byte(); target != 'S' {
			s.fail("Describe target = %q, want statement", target)
		}
		if described := dr.String(); described != name {
			s.fail("described statement = %q, want %q", described, name)
		}
		s.expectType(proto.MsgSync)
		s.sendParseComplete()
		s.sendParameterDescription(23)
		s.sendNoData()
		s.sendReady(proto.TxnIdle)

		statement, args := s.expectBindExecute()
		if statement != name {
			s.fail("bind target = %q, want %q", statement, name)
		}
		if len(args) != 1 || args[0].isNull || args[0].text != "7" {
			s.fail("bound parameters = %+v, want text 7", args)
		}
		s.sendBindComplete()
		s.sendNoData()
		s.sendCommandComplete("INSERT 0 1")
		s.sendReady(proto.TxnIdle)

		closeBody := s.expectType(proto.MsgClose)
		closer := &proto.ReadBuf{B: closeBody}
		if target := closer.Byte(); target != 'S' {
			s.fail("Close target = %q, want statement", target)
		}
		if closed := closer.String(); closed != name {
			s.fail("closed statement = %q, want %q", closed, name)
		}
		s.sendCloseComplete()
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)

	stmt, err := db.PrepareContext(ctx, "INSERT INTO t VALUES ($1)")
	if err != nil {
		t.Fatalf("PrepareContext: %v", err)
	}
	res, err := stmt.ExecContext(ctx, 7)
	if err != nil {
		t.Fatalf("ExecContext: %v", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if affected != 1 {
		t.Errorf("RowsAffected = %d, want 1", affected)
	}
	if err := stmt.Close(); err != nil {
		t.Fatalf("stmt.Close: %v", err)
	}
}

func TestTransactionRollbackAndReuse(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		if query := s.expectSimpleQuery(); query != "BEGIN" {
			s.fail("begin query = %q", query)
		}
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		if query := s.expectSimpleQuery(); query != "INSERT INTO t VALUES (1)" {
			s.fail("insert query = %q", query)
		}
		s.sendCommandComplete("INSERT 0 1")
		s.sendReady(proto.TxnInTransaction)

		if query := s.expectSimpleQuery(); query != "ROLLBACK" {
			s.fail("rollback query = %q", query)
		}
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)

		if query := s.expectSimpleQuery(); query != "SELECT count(*) FROM t" {
			s.fail("count query = %q", query)
		}
		s.sendRowDescription(fakeColumn{name: "count", oid: 20})
		s.sendDataRow("0")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	res, err := tx.ExecContext(ctx, "INSERT INTO t VALUES (1)")
	if err != nil {
		t.Fatalf("tx.ExecContext: %v", err)
	}
	if affected, err := res.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("RowsAffected = %d, %v; want 1, nil", affected, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count Scan: %v", err)
	}
	if count != 0 {
		t.Errorf("count after rollback = %d, want 0", count)
	}
}

func TestServerErrorPreservesSQLStateAndReuse(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		if query := s.expectSimpleQuery(); query != "SELECT missing" {
			s.fail("query = %q", query)
		}
		s.sendError("42P01", `relation "missing" does not exist`)
		s.sendReady(proto.TxnIdle)

		if query := s.expectSimpleQuery(); query != "SELECT 1" {
			s.fail("follow-up query = %q", query)
		}
		s.sendRowDescription(fakeColumn{name: "?column?", oid: 23})
		s.sendDataRow("1")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)

	pinned, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	defer func() { _ = pinned.Close() }()

	_, err = pinned.QueryContext(ctx, "SELECT missing")
	if err == nil {
		t.Fatal("expected a server error")
	}
	var serverErr *Error
	if !errors.As(err, &serverErr) {
		t.Fatalf("error %v is not *Error", err)
	}
	if serverErr.SQLState() != "42P01" {
		t.Errorf("SQLState = %q, want 42P01", serverErr.SQLState())
	}
	if strings.Contains(err.Error(), testPassword) {
		t.Errorf("error %q leaks the password", err)
	}

	var got int
	if err := pinned.QueryRowContext(ctx, "SELECT 1").Scan(&got); err != nil {
		t.Fatalf("reuse after error: %v", err)
	}
	if got != 1 {
		t.Errorf("scanned %d, want 1", got)
	}
}

func TestAuthenticationFailure(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.readStartup()
		s.sendAuthenticationSHA256(katSalt, katToken, 10000)
		s.expectPassword()
		s.sendError("28P01", `password authentication failed for user "alice"`)
	})

	connector, err := NewConnector(testDSN(server, ""))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	_, err = connector.Connect(testContext(t))
	if err == nil {
		t.Fatal("expected the authentication to fail")
	}
	var serverErr *Error
	if !errors.As(err, &serverErr) {
		t.Fatalf("error %v is not *Error", err)
	}
	if serverErr.SQLState() != "28P01" {
		t.Errorf("SQLState = %q, want 28P01", serverErr.SQLState())
	}
	if errors.Is(err, driver.ErrBadConn) {
		t.Errorf("Connect returned driver.ErrBadConn: %v", err)
	}
	if strings.Contains(err.Error(), testPassword) {
		t.Errorf("error %q leaks the password", err)
	}
}

func TestFailedConnectLeavesPoolEmpty(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.readStartup()
		// AuthenticationOk without ReadyForQuery, then the peer disconnects.
		s.sendAuthenticationOK()
	})

	db := openTestDB(t, testDSN(server, ""))
	if err := db.PingContext(testContext(t)); err == nil {
		t.Fatal("expected the connection to fail before ReadyForQuery")
	}
	if got := db.Stats().OpenConnections; got != 0 {
		t.Errorf("OpenConnections = %d, want 0", got)
	}
}

func TestUnsupportedAuthenticationMethod(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.readStartup()
		s.sendAuthenticationRequest(auth.RequestGSS)
	})

	connector, err := NewConnector(testDSN(server, ""))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	_, err = connector.Connect(testContext(t))
	if err == nil {
		t.Fatal("expected an unsupported-authentication error")
	}
	for _, want := range []string{"authentication", "GSSAPI"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if errors.Is(err, driver.ErrBadConn) {
		t.Errorf("Connect returned driver.ErrBadConn: %v", err)
	}
}

func TestMD5Authentication(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.readStartup()
		s.sendAuthenticationMD5([]byte{1, 2, 3, 4})
		proof := s.expectPassword()
		if want := "md5c0bdd3ab2715422c9d1adb98cdd9e645\x00"; string(proof) != want {
			s.fail("MD5 proof = %q, want %q", proof, want)
		}
		s.sendAuthenticationOK()
		s.sendReady(proto.TxnIdle)
	})

	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=alice password=alice dbname=app sslmode=disable", server.port())
	connector, err := NewConnector(dsn)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	raw, err := connector.Connect(testContext(t))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = raw.(*conn).Close()
}

func TestStartupHonorsContextDeadline(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.readStartup()
		// Stall: never send AuthenticationRequest.
		time.Sleep(300 * time.Millisecond)
	})

	connector, err := NewConnector(testDSN(server, ""))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = connector.Connect(ctx)
	if err == nil {
		t.Fatal("expected the startup to time out")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error %v does not wrap context.DeadlineExceeded", err)
	}
}

func TestEarlyRowsCloseResynchronizes(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		if query := s.expectSimpleQuery(); query != "SELECT n FROM t" {
			s.fail("query = %q", query)
		}
		s.sendRowDescription(fakeColumn{name: "n", oid: 23})
		s.sendDataRow("1")
		s.sendDataRow("2")
		s.sendCommandComplete("SELECT 2")
		s.sendReady(proto.TxnIdle)

		if query := s.expectSimpleQuery(); query != "SELECT 1" {
			s.fail("follow-up query = %q", query)
		}
		s.sendRowDescription(fakeColumn{name: "?column?", oid: 23})
		s.sendDataRow("1")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)

	pinned, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	defer func() { _ = pinned.Close() }()

	rows, err := pinned.QueryContext(ctx, "SELECT n FROM t")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("Next: %v", rows.Err())
	}
	var first int
	if err := rows.Scan(&first); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if first != 1 {
		t.Errorf("first row = %d, want 1", first)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close: %v", err)
	}

	var got int
	if err := pinned.QueryRowContext(ctx, "SELECT 1").Scan(&got); err != nil {
		t.Fatalf("reuse after early close: %v", err)
	}
	if got != 1 {
		t.Errorf("scanned %d, want 1", got)
	}
}

func TestSplitAndCoalescedServerMessages(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		if query := s.expectSimpleQuery(); query != "SELECT 42" {
			s.fail("query = %q", query)
		}
		// RowDescription arrives in single-byte writes.
		s.sendSlow(rowDescriptionFrame(fakeColumn{name: "x", oid: 23}))
		// DataRow, CommandComplete and ReadyForQuery arrive in one write.
		coalesced := append(append(append([]byte{},
			dataRowFrame("42")...),
			commandCompleteFrame("SELECT 1")...),
			readyFrame(proto.TxnIdle)...)
		s.writeRaw(coalesced)

		if query := s.expectSimpleQuery(); query != "SELECT 7" {
			s.fail("second query = %q", query)
		}
		s.sendRowDescription(fakeColumn{name: "x", oid: 23})
		s.sendDataRow("7")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)

	var got int
	if err := db.QueryRowContext(ctx, "SELECT 42").Scan(&got); err != nil {
		t.Fatalf("first Scan: %v", err)
	}
	if got != 42 {
		t.Errorf("first value = %d, want 42", got)
	}
	if err := db.QueryRowContext(ctx, "SELECT 7").Scan(&got); err != nil {
		t.Fatalf("second Scan: %v", err)
	}
	if got != 7 {
		t.Errorf("second value = %d, want 7", got)
	}
}

func TestTLSVerifyFull(t *testing.T) {
	cert, caPEM := newTestCertificate(t, nil, []net.IP{net.ParseIP("127.0.0.1")})
	rootCert := writeRootCert(t, caPEM)

	server := newFakeServer(t, func(s *fakeSession) {
		if !s.acceptTLS(cert) {
			return
		}
		s.completeStartup(nil)

		if query := s.expectSimpleQuery(); query != "" {
			s.fail("ping query = %q", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, "sslmode=verify-full sslrootcert="+filepath.ToSlash(rootCert)))
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("verify-full ping: %v", err)
	}
	if got := server.connections(); got != 1 {
		t.Errorf("connections = %d, want 1", got)
	}
}

func TestTLSHostnameMismatchIsRejected(t *testing.T) {
	cert, caPEM := newTestCertificate(t, []string{"db.example.com"}, nil)
	rootCert := writeRootCert(t, caPEM)

	server := newFakeServer(t, func(s *fakeSession) {
		// The client must reject the certificate; a plaintext retry would
		// open a second connection, which the assertion below rules out.
		s.acceptTLS(cert)
	})

	connector, err := NewConnector(testDSN(server, "sslmode=verify-full sslrootcert="+filepath.ToSlash(rootCert)))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	_, err = connector.Connect(testContext(t))
	if err == nil {
		t.Fatal("expected the host name verification to fail")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "certificate") {
		t.Errorf("error %q does not mention the certificate", err)
	}
	if got := server.connections(); got != 1 {
		t.Errorf("connections = %d, want 1 (no plaintext fallback)", got)
	}
}

func TestTLSRequiredWhenServerRefuses(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.rejectTLS()
	})

	connector, err := NewConnector(testDSN(server, "sslmode=verify-full"))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	_, err = connector.Connect(testContext(t))
	if err == nil {
		t.Fatal("expected SSL to be required")
	}
	if !strings.Contains(err.Error(), "SSL") {
		t.Errorf("error %q does not mention SSL", err)
	}
	if got := server.connections(); got != 1 {
		t.Errorf("connections = %d, want 1 (no plaintext fallback)", got)
	}
}

func TestPreparedStatementHonorsContextDeadline(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		name, _ := s.expectPrepare()
		describe := s.expectType(proto.MsgDescribe)
		dr := &proto.ReadBuf{B: describe}
		if target := dr.Byte(); target != 'S' {
			s.fail("Describe target = %q, want statement", target)
		}
		if described := dr.String(); described != name {
			s.fail("described statement = %q, want %q", described, name)
		}
		s.expectType(proto.MsgSync)
		s.sendParseComplete()
		s.sendParameterDescription()
		s.sendNoData()
		s.sendReady(proto.TxnIdle)

		// Read the execution request but never answer it; the client must give
		// up when its deadline expires.
		s.expectBindExecute()
		time.Sleep(300 * time.Millisecond)
	})

	db := openTestDB(t, testDSN(server, ""))
	stmt, err := db.PrepareContext(testContext(t), "UPDATE t SET v = 1")
	if err != nil {
		t.Fatalf("PrepareContext: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = stmt.ExecContext(ctx)
	if err == nil {
		t.Fatal("expected the execution to time out")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error %v does not wrap context.DeadlineExceeded", err)
	}
}

func TestDialFailureIsNotBadConn(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	connector, err := NewConnector(fmt.Sprintf(
		"host=127.0.0.1 port=%d user=%s password=%s connect_timeout=2", port, testUser, testPassword))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	_, err = connector.Connect(testContext(t))
	if err == nil {
		t.Fatal("expected the dial to fail")
	}
	if !strings.Contains(err.Error(), "dial") {
		t.Errorf("error %q does not identify the dial stage", err)
	}
	if errors.Is(err, driver.ErrBadConn) {
		t.Errorf("Connect returned driver.ErrBadConn: %v", err)
	}
}

func TestNewConnectorRejectsInvalidDSNWithoutNetwork(t *testing.T) {
	if _, err := NewConnector("user=alice port=notaport"); err == nil {
		t.Fatal("expected the DSN to be rejected")
	}
}

func TestPreferFallsBackToPlaintext(t *testing.T) {
	var (
		mu      sync.Mutex
		attempt int
	)
	server := newFakeServer(t, func(s *fakeSession) {
		mu.Lock()
		attempt++
		current := attempt
		mu.Unlock()

		if current == 1 {
			// sslmode=prefer offers TLS first; this server has none.
			s.rejectTLS()
			return
		}
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "" {
			s.fail("ping query = %q", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, "sslmode=prefer"))
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("ping with prefer fallback: %v", err)
	}
	if got := server.connections(); got != 2 {
		t.Errorf("connections = %d, want 2", got)
	}
}

func TestAllowRetriesWithTLS(t *testing.T) {
	cert, _ := newTestCertificate(t, nil, []net.IP{net.ParseIP("127.0.0.1")})
	var (
		mu      sync.Mutex
		attempt int
	)
	server := newFakeServer(t, func(s *fakeSession) {
		mu.Lock()
		attempt++
		current := attempt
		mu.Unlock()

		if current == 1 {
			// sslmode=allow starts in plaintext; this server requires SSL.
			s.readStartup()
			s.sendError("28000", "SSL connection is required")
			return
		}
		if !s.acceptTLS(cert) {
			return
		}
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "" {
			s.fail("ping query = %q", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, "sslmode=allow"))
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("ping with allow TLS retry: %v", err)
	}
	if got := server.connections(); got != 2 {
		t.Errorf("connections = %d, want 2", got)
	}
}

func TestRowStreamingHonorsContextDeadline(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "SELECT n FROM t" {
			s.fail("query = %q", query)
		}
		s.sendRowDescription(fakeColumn{name: "n", oid: 23})
		// Stall without sending rows or CommandComplete.
		time.Sleep(300 * time.Millisecond)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SELECT n FROM t")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()

	if rows.Next() {
		t.Fatal("Next returned a row from a stalled stream")
	}
	if err := rows.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("rows.Err() = %v, want context.DeadlineExceeded", err)
	}
}

func TestCommitReportsServerRollback(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		if query := s.expectSimpleQuery(); query != "BEGIN" {
			s.fail("begin query = %q", query)
		}
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		if query := s.expectSimpleQuery(); query != "INSERT INTO t VALUES (1)" {
			s.fail("insert query = %q", query)
		}
		s.sendError("23505", "duplicate key value violates unique constraint")
		s.sendReady(proto.TxnFailed)

		// A failed transaction answers COMMIT with a ROLLBACK tag.
		if query := s.expectSimpleQuery(); query != "COMMIT" {
			s.fail("commit query = %q", query)
		}
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO t VALUES (1)"); err == nil {
		t.Fatal("expected the insert to fail")
	}
	err = tx.Commit()
	if err == nil {
		t.Fatal("Commit reported success after the server rolled the transaction back")
	}
	if !strings.Contains(err.Error(), "rolled the transaction back") {
		t.Errorf("Commit error = %v, want a server-rollback explanation", err)
	}
}

func TestScanNullAndEmptyValues(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "SELECT NULL::text, ''::text" {
			s.fail("query = %q", query)
		}
		s.sendRowDescription(fakeColumn{name: "a", oid: 25}, fakeColumn{name: "b", oid: 25})
		s.sendDataRow(nil, "")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	var nullValue sql.NullString
	var empty string
	if err := db.QueryRowContext(testContext(t), "SELECT NULL::text, ''::text").Scan(&nullValue, &empty); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if nullValue.Valid {
		t.Errorf("NULL scanned as %q", nullValue.String)
	}
	if empty != "" {
		t.Errorf("empty string scanned as %q", empty)
	}
}

func TestCoalescedStartupMessages(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.readStartup()

		// The AuthenticationRequest is split into single-byte writes.
		req := proto.NewMessageWriter(proto.MsgAuthenticationRequest)
		req.Int32(auth.RequestSHA256)
		req.Int32(auth.StoredSHA256)
		req.Bytes([]byte(katSalt))
		req.Bytes([]byte(katToken))
		req.Int32(10000)
		s.sendSlow(req.Finish())

		proof := s.expectPassword()
		if want := katProof + "\x00"; string(proof) != want {
			s.fail("client proof = %q, want %q", proof, want)
		}

		// AuthenticationOk, two session parameters and ReadyForQuery arrive
		// in one write.
		coalesced := append(append(append(append([]byte{},
			authOKFrame()...),
			parameterStatusFrame("server_version", "7.0.0")...),
			parameterStatusFrame("client_encoding", "UTF8")...),
			readyFrame(proto.TxnIdle)...)
		s.writeRaw(coalesced)

		if query := s.expectSimpleQuery(); query != "" {
			s.fail("ping query = %q", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestTLSVerifyCAIgnoresHostname(t *testing.T) {
	cert, caPEM := newTestCertificate(t, []string{"db.example.com"}, nil)
	rootCert := writeRootCert(t, caPEM)

	server := newFakeServer(t, func(s *fakeSession) {
		if !s.acceptTLS(cert) {
			return
		}
		s.completeStartup(nil)

		if query := s.expectSimpleQuery(); query != "" {
			s.fail("ping query = %q", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, "sslmode=verify-ca sslrootcert="+filepath.ToSlash(rootCert)))
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("verify-ca ping with a mismatched host name: %v", err)
	}
}

func TestConnectorRejectsUnreadableRootCert(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.pem")
	_, err := NewConnector("user=alice sslmode=prefer sslrootcert=" + filepath.ToSlash(missing))
	if err == nil {
		t.Fatal("expected the missing CA file to be rejected")
	}
	if !strings.Contains(err.Error(), "sslrootcert") {
		t.Errorf("error %q does not mention sslrootcert", err)
	}
}

func TestPassfilePasswordAuthenticates(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		if query := s.expectSimpleQuery(); query != "" {
			s.fail("ping query = %q", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})

	passfile := writeConfigFile(t, "pgpass", fmt.Sprintf("127.0.0.1:%d:%s:%s:%s\n",
		server.port(), testDatabase, testUser, testPassword))
	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=%s dbname=%s sslmode=disable passfile=%s",
		server.port(), testUser, testDatabase, filepath.ToSlash(passfile))

	db := openTestDB(t, dsn)
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext with a passfile password: %v", err)
	}
}

func TestMultiHostFallsBackToSecondTarget(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		if query := s.expectSimpleQuery(); query != "" {
			s.fail("ping query = %q", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})

	closed := closedPort(t)
	dsn := fmt.Sprintf("host=127.0.0.1,127.0.0.1 port=%d,%d user=%s password=%s dbname=%s sslmode=disable",
		closed, server.port(), testUser, testPassword, testDatabase)
	db := openTestDB(t, dsn)
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext with a second target: %v", err)
	}
	if got := server.connections(); got != 1 {
		t.Errorf("connections = %d, want 1", got)
	}
}

func TestMultiHostReportsEveryFailedTarget(t *testing.T) {
	first, second := closedPort(t), closedPort(t)
	dsn := fmt.Sprintf("host=127.0.0.1,127.0.0.1 port=%d,%d user=%s sslmode=disable",
		first, second, testUser)
	db := openTestDB(t, dsn)
	err := db.PingContext(testContext(t))
	if err == nil {
		t.Fatal("expected every target to fail")
	}
	message := err.Error()
	if !strings.Contains(message, strconv.Itoa(first)) || !strings.Contains(message, strconv.Itoa(second)) {
		t.Errorf("error %q does not name both targets", message)
	}
}

func TestMultiHostUsesPerTargetServerName(t *testing.T) {
	cert, caPEM := newTestCertificate(t, []string{"localhost"}, nil)
	rootCert := writeRootCert(t, caPEM)

	server := newFakeServer(t, func(s *fakeSession) {
		if !s.acceptTLS(cert) {
			return
		}
		s.completeStartup(nil)

		if query := s.expectSimpleQuery(); query != "" {
			s.fail("ping query = %q", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})

	// The first target is unreachable, so the connection comes from the
	// second one, whose certificate names localhost rather than the first
	// target's address.
	dsn := fmt.Sprintf("host=127.0.0.1,%s port=%d,%d user=%s password=%s dbname=%s sslmode=verify-full sslrootcert=%s",
		"localhost", closedPort(t), server.port(), testUser, testPassword, testDatabase, filepath.ToSlash(rootCert))
	db := openTestDB(t, dsn)
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("verify-full against the second target: %v", err)
	}
}

func TestMultiHostStopsAfterAuthenticationFailure(t *testing.T) {
	rejecting := newFakeServer(t, func(s *fakeSession) {
		s.readStartup()
		// Send an unsupported method so the client fails before any second
		// target is contacted.
		m := proto.NewMessageWriter(proto.MsgAuthenticationRequest)
		m.Int32(auth.RequestGSS)
		s.writeRaw(m.Finish())
	})
	second := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
	})

	dsn := fmt.Sprintf("host=127.0.0.1,127.0.0.1 port=%d,%d user=%s password=%s dbname=%s sslmode=disable",
		rejecting.port(), second.port(), testUser, testPassword, testDatabase)
	db := openTestDB(t, dsn)
	err := db.PingContext(testContext(t))
	if err == nil {
		t.Fatal("expected the authentication failure to be reported")
	}
	if !strings.Contains(err.Error(), "authentication") {
		t.Errorf("error = %v, want an authentication failure", err)
	}
	if got := second.connections(); got != 0 {
		t.Errorf("second target accepted %d connections, want 0", got)
	}
}

func TestMultiHostPassfileMatchesEachTarget(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		if query := s.expectSimpleQuery(); query != "" {
			s.fail("ping query = %q", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})

	// Only the second target has an entry, so a first-target-only lookup
	// would leave the password empty and fail authentication.
	passfile := writeConfigFile(t, "pgpass", fmt.Sprintf("127.0.0.1:%d:%s:%s:%s\n",
		server.port(), testDatabase, testUser, testPassword))
	dsn := fmt.Sprintf("host=127.0.0.1,127.0.0.1 port=%d,%d user=%s dbname=%s sslmode=disable passfile=%s",
		closedPort(t), server.port(), testUser, testDatabase, filepath.ToSlash(passfile))

	db := openTestDB(t, dsn)
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext with a passfile entry for the second target: %v", err)
	}
}

func TestMultiHostAggregateErrorsAreDiscoverable(t *testing.T) {
	first, second := closedPort(t), closedPort(t)
	dsn := fmt.Sprintf("host=127.0.0.1,127.0.0.1 port=%d,%d user=%s sslmode=disable",
		first, second, testUser)
	db := openTestDB(t, dsn)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := db.PingContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want errors.Is(context.Canceled)", err)
	}
}

func TestMultiHostStopsAfterFallbackAuthenticationFailure(t *testing.T) {
	// sslmode=prefer fails the TLS attempt on the first target and the
	// authentication on its plaintext retry, so the joined failure does not
	// begin with the authentication error.
	var (
		mu       sync.Mutex
		attempts int
	)
	rejecting := newFakeServer(t, func(s *fakeSession) {
		mu.Lock()
		attempts++
		current := attempts
		mu.Unlock()

		if current%2 == 1 {
			// First attempt: refuse TLS so the policy falls back to plaintext.
			s.rejectTLS()
			return
		}
		s.readStartup()
		s.sendAuthenticationSHA256(katSalt, katToken, 10000)
		s.expectPassword()
		s.sendError("28P01", `password authentication failed for user "alice"`)
	})
	second := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
	})

	dsn := fmt.Sprintf("host=127.0.0.1,127.0.0.1 port=%d,%d user=%s password=%s dbname=%s sslmode=prefer",
		rejecting.port(), second.port(), testUser, testPassword, testDatabase)
	db := openTestDB(t, dsn)
	if err := db.PingContext(testContext(t)); err == nil {
		t.Fatal("expected the rejected password to fail the connection")
	}
	if got := second.connections(); got != 0 {
		t.Errorf("second target accepted %d connections after a rejected password, want 0", got)
	}
}

func TestMultiHostReportsLaterAuthenticationFailure(t *testing.T) {
	rejecting := newFakeServer(t, func(s *fakeSession) {
		s.readStartup()
		s.sendAuthenticationSHA256(katSalt, katToken, 10000)
		s.expectPassword()
		s.sendError("28P01", `password authentication failed for user "alice"`)
	})

	dsn := fmt.Sprintf("host=127.0.0.1,127.0.0.1 port=%d,%d user=%s password=%s dbname=%s sslmode=disable",
		closedPort(t), rejecting.port(), testUser, testPassword, testDatabase)
	db := openTestDB(t, dsn)
	err := db.PingContext(testContext(t))
	if err == nil {
		t.Fatal("expected the second target to reject the password")
	}
	var serverErr *Error
	if !errors.As(err, &serverErr) {
		t.Fatalf("error %v does not expose the server *Error", err)
	}
	if serverErr.SQLState() != "28P01" {
		t.Errorf("SQLState = %q, want 28P01 from the second target", serverErr.SQLState())
	}
}

func TestConnectTimeoutAppliesPerTarget(t *testing.T) {
	// The server accepts and never answers, so the whole target attempt is
	// bounded by connect_timeout; the old per-attempt timer would take twice
	// as long under sslmode=allow.
	silent := newSilentServer(t)
	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=%s password=%s dbname=%s sslmode=allow connect_timeout=1",
		silent, testUser, testPassword, testDatabase)

	db := openTestDB(t, dsn)
	start := time.Now()
	if err := db.PingContext(testContext(t)); err == nil {
		t.Fatal("expected the silent server to time out")
	}
	if elapsed := time.Since(start); elapsed > 1800*time.Millisecond {
		t.Errorf("connect_timeout took %v, want about 1s for the target", elapsed)
	}
}

// newSilentServer listens and accepts without ever replying, returning its
// port.
func newSilentServer(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { _ = conn.Close() }()
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	return ln.Addr().(*net.TCPAddr).Port
}

func TestTypedResultValues(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		query := s.expectSimpleQuery()
		if query != "SELECT types" {
			s.fail("query = %q", query)
		}
		s.sendRowDescription(
			fakeColumn{name: "b", oid: 16},
			fakeColumn{name: "i8", oid: 20},
			fakeColumn{name: "f8", oid: 701},
			fakeColumn{name: "raw", oid: 17},
			fakeColumn{name: "ts", oid: 1114},
			fakeColumn{name: "tstz", oid: 1184},
			fakeColumn{name: "amount", oid: 1700},
			fakeColumn{name: "missing", oid: 25},
			fakeColumn{name: "other", oid: 999999},
		)
		s.sendDataRow("t", "-9007199254740993", "2.5", `\x0001ff`,
			"2026-01-02 15:04:05.5", "2026-01-02 15:04:05.5+08",
			"12345678901234567890.12", nil, "opaque")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	result, err := db.QueryContext(testContext(t), "SELECT types")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = result.Close() }()

	types, err := result.ColumnTypes()
	if err != nil {
		t.Fatalf("ColumnTypes: %v", err)
	}
	wantNames := []string{"bool", "int8", "float8", "bytea", "timestamp", "timestamptz", "numeric", "text", ""}
	wantScan := []reflect.Type{
		reflect.TypeOf(false), reflect.TypeOf(int64(0)), reflect.TypeOf(float64(0)),
		reflect.TypeOf([]byte(nil)), reflect.TypeOf(time.Time{}), reflect.TypeOf(time.Time{}),
		reflect.TypeOf(""), reflect.TypeOf(""), reflect.TypeOf(""),
	}
	if len(types) != len(wantNames) {
		t.Fatalf("ColumnTypes = %d columns, want %d", len(types), len(wantNames))
	}
	for i, columnType := range types {
		if got := columnType.DatabaseTypeName(); got != wantNames[i] {
			t.Errorf("DatabaseTypeName(%d) = %q, want %q", i, got, wantNames[i])
		}
		if got := columnType.ScanType(); got != wantScan[i] {
			t.Errorf("ScanType(%d) = %v, want %v", i, got, wantScan[i])
		}
	}

	if !result.Next() {
		t.Fatalf("Next: %v", result.Err())
	}
	var (
		b        bool
		i8       int64
		f8       float64
		raw      []byte
		ts, tstz time.Time
		amount   string
		missing  sql.NullString
		other    string
	)
	if err := result.Scan(&b, &i8, &f8, &raw, &ts, &tstz, &amount, &missing, &other); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !b {
		t.Error("bool = false, want true")
	}
	if i8 != -9007199254740993 {
		t.Errorf("int8 = %d, want -9007199254740993", i8)
	}
	if f8 != 2.5 {
		t.Errorf("float8 = %v, want 2.5", f8)
	}
	if !bytes.Equal(raw, []byte{0x00, 0x01, 0xff}) {
		t.Errorf("bytea = %v, want 00 01 ff", raw)
	}
	wantTS := time.Date(2026, 1, 2, 15, 4, 5, 500000000, time.UTC)
	if !ts.Equal(wantTS) {
		t.Errorf("timestamp = %v, want %v", ts, wantTS)
	}
	if wantInstant := time.Date(2026, 1, 2, 7, 4, 5, 500000000, time.UTC); !tstz.Equal(wantInstant) {
		t.Errorf("timestamptz = %v, want the instant %v", tstz, wantInstant)
	}
	if hour, _, _ := tstz.Clock(); hour != 15 {
		t.Errorf("timestamptz hour = %d, want the wall clock of the +08 value", hour)
	}
	if _, offset := tstz.Zone(); offset != 8*3600 {
		t.Errorf("timestamptz offset = %d, want 28800", offset)
	}
	if amount != "12345678901234567890.12" {
		t.Errorf("numeric = %q, want the exact text", amount)
	}
	if missing.Valid {
		t.Errorf("NULL column scanned as %q, want invalid", missing.String)
	}
	if other != "opaque" {
		t.Errorf("unknown OID = %q, want the text", other)
	}
	if result.Next() {
		t.Error("a second row was returned")
	}
}

func TestByteaParameterIsText(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		query, args := s.expectExtended()
		if query != "SELECT $1::bytea" {
			s.fail("query = %q", query)
		}
		// []byte carries text, so a bytea value must already be in the
		// server's text representation and is sent untouched.
		if len(args) != 1 || args[0].text != `\x0001ff` {
			s.fail("bound parameters = %+v, want the bytea text unchanged", args)
		}
		s.sendParseComplete()
		s.sendBindComplete()
		s.sendRowDescription(fakeColumn{name: "raw", oid: 17})
		s.sendDataRow(`\x0001ff`)
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	var got []byte
	if err := db.QueryRowContext(testContext(t), "SELECT $1::bytea", []byte(`\x0001ff`)).Scan(&got); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !bytes.Equal(got, []byte{0x00, 0x01, 0xff}) {
		t.Errorf("bytea = %v, want 00 01 ff", got)
	}
}

func TestBooleanParameterUsesOneAndZero(t *testing.T) {
	var (
		mu   sync.Mutex
		seen []string
	)
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		query, args := s.expectExtended()
		if query != "SELECT $1::boolean, $2::boolean" {
			s.fail("query = %q", query)
		}
		texts := make([]string, 0, len(args))
		for _, arg := range args {
			texts = append(texts, arg.text)
		}
		mu.Lock()
		seen = texts
		mu.Unlock()
		s.sendParseComplete()
		s.sendBindComplete()
		s.sendRowDescription(fakeColumn{name: "a", oid: 16}, fakeColumn{name: "b", oid: 16})
		s.sendDataRow("1", "0")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	var first, second bool
	if err := db.QueryRowContext(testContext(t), "SELECT $1::boolean, $2::boolean", true, false).Scan(&first, &second); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "1" || got[1] != "0" {
		t.Errorf("bound booleans = %v, want 1 and 0", got)
	}
	if !first || second {
		t.Errorf("scanned booleans = %v/%v, want true/false", first, second)
	}
}

// closedPort returns a local port with no listener.
func closedPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return port
}

func newTestCertificate(t *testing.T, dnsNames []string, ips []net.IP) (tls.Certificate, []byte) {
	t.Helper()
	certPEM, keyPEM := newTestCertificatePEM(t, dnsNames, ips)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	return cert, certPEM
}

func newTestCertificatePEM(t *testing.T, dnsNames []string, ips []net.IP) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "opengauss test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              dnsNames,
		IPAddresses:           ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func writeRootCert(t *testing.T, certPEM []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "root.crt")
	if err := os.WriteFile(path, certPEM, 0o600); err != nil {
		t.Fatalf("write root certificate: %v", err)
	}
	return path
}

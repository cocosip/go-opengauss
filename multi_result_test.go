package opengauss

import (
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/cocosip/go-opengauss/internal/proto"
)

var _ driver.RowsNextResultSet = (*rows)(nil)

func TestMultiResultSetsFromSimpleQuery(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "SELECT 1; SELECT 'two'" {
			s.fail("query = %q", query)
			return
		}
		s.sendRowDescription(fakeColumn{name: "one", oid: 23})
		s.sendDataRow("1")
		s.sendCommandComplete("SELECT 1")
		s.sendRowDescription(fakeColumn{name: "two", oid: 25})
		s.sendDataRow("two")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), "SELECT 1; SELECT 'two'")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()

	if columns, err := rows.Columns(); err != nil || len(columns) != 1 || columns[0] != "one" {
		t.Fatalf("first columns = %v, err = %v", columns, err)
	}
	if !rows.Next() {
		t.Fatalf("first set missing: %v", rows.Err())
	}
	var first int64
	if err := rows.Scan(&first); err != nil {
		t.Fatalf("Scan first set: %v", err)
	}
	if first != 1 {
		t.Errorf("first value = %d", first)
	}
	if rows.Next() {
		t.Fatal("the first set returned an extra row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("first set error: %v", err)
	}

	if !rows.NextResultSet() {
		t.Fatalf("NextResultSet = false, err = %v", rows.Err())
	}
	if columns, err := rows.Columns(); err != nil || len(columns) != 1 || columns[0] != "two" {
		t.Fatalf("second columns = %v, err = %v", columns, err)
	}
	if !rows.Next() {
		t.Fatalf("second set missing: %v", rows.Err())
	}
	var second string
	if err := rows.Scan(&second); err != nil {
		t.Fatalf("Scan second set: %v", err)
	}
	if second != "two" {
		t.Errorf("second value = %q", second)
	}
	if rows.Next() {
		t.Fatal("the second set returned an extra row")
	}
	if rows.NextResultSet() {
		t.Fatal("a third set was reported")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error after the last set: %v", err)
	}
}

// TestNextResultSetSkipsUnreadRows checks the driver contract: advancing works
// even when the current set still has rows.
func TestNextResultSetSkipsUnreadRows(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendRowDescription(fakeColumn{name: "one", oid: 23})
		s.sendDataRow("1")
		s.sendDataRow("2")
		s.sendCommandComplete("SELECT 2")
		s.sendRowDescription(fakeColumn{name: "two", oid: 25})
		s.sendDataRow("three")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), "SELECT 1 UNION ALL SELECT 2; SELECT 'three'")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("first row missing: %v", rows.Err())
	}
	// One row of the first set is still unread when the set is skipped.
	if !rows.NextResultSet() {
		t.Fatalf("NextResultSet = false, err = %v", rows.Err())
	}
	if !rows.Next() {
		t.Fatalf("second set missing: %v", rows.Err())
	}
	var value string
	if err := rows.Scan(&value); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if value != "three" {
		t.Errorf("value = %q", value)
	}
}

// TestResultSetsCanDifferInColumnCount checks that database/sql rebuilds the
// destination and metadata for a set with a different width.
func TestResultSetsCanDifferInColumnCount(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendRowDescription(fakeColumn{name: "a", oid: 23}, fakeColumn{name: "b", oid: 25})
		s.sendDataRow("1", "x")
		s.sendCommandComplete("SELECT 1")
		s.sendRowDescription(fakeColumn{name: "c", oid: 23})
		s.sendDataRow("2")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), "SELECT 1, 'x'; SELECT 2")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("first set missing: %v", rows.Err())
	}
	var number int64
	var text string
	if err := rows.Scan(&number, &text); err != nil {
		t.Fatalf("Scan first set: %v", err)
	}
	if rows.Next() {
		t.Fatal("the first set returned an extra row")
	}
	if !rows.NextResultSet() {
		t.Fatalf("NextResultSet = false, err = %v", rows.Err())
	}
	types, err := rows.ColumnTypes()
	if err != nil || len(types) != 1 || types[0].Name() != "c" {
		t.Fatalf("second set types = %v, err = %v", types, err)
	}
	if !rows.Next() {
		t.Fatalf("second set missing: %v", rows.Err())
	}
	number = 0
	if err := rows.Scan(&number); err != nil {
		t.Fatalf("Scan second set: %v", err)
	}
	if number != 2 {
		t.Errorf("second value = %d", number)
	}
}

// TestNextAfterEOFReturnsEOF checks that a further Next does not consume the
// pending set.
func TestNextAfterEOFReturnsEOF(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendRowDescription(fakeColumn{name: "a", oid: 23})
		s.sendDataRow("1")
		s.sendCommandComplete("SELECT 1")
		s.sendRowDescription(fakeColumn{name: "b", oid: 25})
		s.sendDataRow("two")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), "SELECT 1; SELECT 'two'")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("first row missing: %v", rows.Err())
	}
	if rows.Next() {
		t.Fatal("the first set returned an extra row")
	}
	if rows.Next() {
		t.Fatal("a further Next consumed the pending set")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error after EOF: %v", err)
	}
	if !rows.NextResultSet() {
		t.Fatalf("NextResultSet = false, err = %v", rows.Err())
	}
	if !rows.Next() {
		t.Fatalf("second set missing: %v", rows.Err())
	}
	var value string
	if err := rows.Scan(&value); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if value != "two" {
		t.Errorf("value = %q", value)
	}
}

// TestNoRowStatementBetweenResultSets checks a SET between two row-producing
// statements.
func TestNoRowStatementBetweenResultSets(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendRowDescription(fakeColumn{name: "a", oid: 23})
		s.sendDataRow("1")
		s.sendCommandComplete("SELECT 1")
		s.sendCommandComplete("SET")
		s.sendRowDescription(fakeColumn{name: "b", oid: 23})
		s.sendDataRow("2")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), "SELECT 1; SET x = 1; SELECT 2")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("first row missing: %v", rows.Err())
	}
	if rows.Next() {
		t.Fatal("the first set returned an extra row")
	}
	if !rows.NextResultSet() {
		t.Fatalf("NextResultSet = false, err = %v", rows.Err())
	}
	if !rows.Next() {
		t.Fatalf("second row missing: %v", rows.Err())
	}
	var value int64
	if err := rows.Scan(&value); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if value != 2 {
		t.Errorf("value = %d", value)
	}
}

func TestQuerySkipsStatementsWithoutRows(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "SET x = 1; SELECT 2" {
			s.fail("query = %q", query)
			return
		}
		s.sendCommandComplete("SET")
		s.sendRowDescription(fakeColumn{name: "value", oid: 23})
		s.sendDataRow("2")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), "SET x = 1; SELECT 2")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("row missing: %v", rows.Err())
	}
	var value int64
	if err := rows.Scan(&value); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if value != 2 {
		t.Errorf("value = %d", value)
	}
}

func TestEmptyResultSetIsSkippedByNextResultSet(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendRowDescription(fakeColumn{name: "a", oid: 23})
		s.sendDataRow("1")
		s.sendCommandComplete("SELECT 1")
		s.sendRowDescription(fakeColumn{name: "b", oid: 23})
		s.sendCommandComplete("SELECT 0")
		s.sendRowDescription(fakeColumn{name: "c", oid: 23})
		s.sendDataRow("3")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), "SELECT 1; SELECT 2 WHERE false; SELECT 3")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("first row missing: %v", rows.Err())
	}
	if rows.Next() {
		t.Fatal("the first set returned an extra row")
	}
	if !rows.NextResultSet() {
		t.Fatalf("NextResultSet = false, err = %v", rows.Err())
	}
	if columns, err := rows.Columns(); err != nil || len(columns) != 1 || columns[0] != "b" {
		t.Fatalf("empty set columns = %v, err = %v", columns, err)
	}
	if rows.Next() {
		t.Fatal("the empty set returned a row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("empty set error: %v", err)
	}
	if !rows.NextResultSet() {
		t.Fatalf("NextResultSet to the third set = false, err = %v", rows.Err())
	}
	if columns, err := rows.Columns(); err != nil || len(columns) != 1 || columns[0] != "c" {
		t.Fatalf("third set columns = %v, err = %v", columns, err)
	}
	if !rows.Next() {
		t.Fatalf("third row missing: %v", rows.Err())
	}
	var value int64
	if err := rows.Scan(&value); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if value != 3 {
		t.Errorf("value = %d", value)
	}
}

// TestEmptyStatementInBatchIsSkipped covers a simple query that contains an
// empty statement between two SELECTs.
func TestEmptyStatementInBatchIsSkipped(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendRowDescription(fakeColumn{name: "a", oid: 23})
		s.sendDataRow("1")
		s.sendCommandComplete("SELECT 1")
		s.sendEmptyQueryResponse()
		s.sendRowDescription(fakeColumn{name: "b", oid: 23})
		s.sendDataRow("2")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), "SELECT 1;; SELECT 2")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("first row missing: %v", rows.Err())
	}
	if rows.Next() {
		t.Fatal("the first set returned an extra row")
	}
	if !rows.NextResultSet() {
		t.Fatalf("NextResultSet = false, err = %v", rows.Err())
	}
	if !rows.Next() {
		t.Fatalf("second row missing: %v", rows.Err())
	}
	var value int64
	if err := rows.Scan(&value); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if value != 2 {
		t.Errorf("value = %d", value)
	}
}

// TestCloseAfterFirstResultSetKeepsSession checks that abandoning the remaining
// sets drains them and leaves the session reusable.
func TestCloseAfterFirstResultSetKeepsSession(t *testing.T) {
	testCloseAfterAbandoningSets(t, false)
}

// TestCloseAfterReadAheadKeepsSession covers closing once the next set's
// RowDescription was already read.
func TestCloseAfterReadAheadKeepsSession(t *testing.T) {
	testCloseAfterAbandoningSets(t, true)
}

func testCloseAfterAbandoningSets(t *testing.T, readAhead bool) {
	t.Helper()
	var connections atomic.Int32
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		connections.Add(1)
		if query := s.expectSimpleQuery(); query != "SELECT 1; SELECT 2" {
			s.fail("query = %q", query)
			return
		}
		s.sendRowDescription(fakeColumn{name: "v", oid: 23})
		s.sendDataRow("1")
		s.sendCommandComplete("SELECT 1")
		s.sendRowDescription(fakeColumn{name: "v", oid: 23})
		s.sendDataRow("2")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		if query := s.expectSimpleQuery(); query != "SELECT 3" {
			s.fail("second query = %q", query)
			return
		}
		s.sendRowDescription(fakeColumn{name: "v", oid: 23})
		s.sendDataRow("3")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, "SELECT 1; SELECT 2")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("first row missing: %v", rows.Err())
	}
	if readAhead && rows.Next() {
		t.Fatal("the first set returned an extra row")
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var value int64
	if err := db.QueryRowContext(ctx, "SELECT 3").Scan(&value); err != nil {
		t.Fatalf("query after abandoning the sets: %v", err)
	}
	if value != 3 {
		t.Errorf("value = %d", value)
	}
	if connections.Load() != 1 {
		t.Errorf("connections = %d, want the session reused", connections.Load())
	}
}

// TestErrorInSecondResultSetIsReported checks the SQLSTATE of a failing
// statement after a successful one in the same simple query.
func TestErrorInSecondResultSetIsReported(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "SELECT 1; SELECT 1/0" {
			s.fail("query = %q", query)
			return
		}
		s.sendRowDescription(fakeColumn{name: "v", oid: 23})
		s.sendDataRow("1")
		s.sendCommandComplete("SELECT 1")
		s.sendError("22012", "division by zero")
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, "SELECT 1; SELECT 1/0")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("first row missing: %v", rows.Err())
	}
	if rows.Next() {
		t.Fatal("the failing second statement returned a row")
	}
	var serverErr *Error
	if !errors.As(rows.Err(), &serverErr) || serverErr.Code != "22012" {
		t.Fatalf("Rows.Err = %v, want SQLSTATE 22012", rows.Err())
	}
	_ = rows.Close()
	// The error was drained, so the session is still usable and reused.
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping after the second-set error: %v", err)
	}
}

// TestErrorInSecondResultSetInsideTransaction checks that the failed
// transaction status is recorded and a rollback leaves the session reusable.
func TestErrorInSecondResultSetInsideTransaction(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "BEGIN" {
			s.fail("query = %q", query)
			return
		}
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)
		if query := s.expectSimpleQuery(); query != "SELECT 1; SELECT 1/0" {
			s.fail("query = %q", query)
			return
		}
		s.sendRowDescription(fakeColumn{name: "v", oid: 23})
		s.sendDataRow("1")
		s.sendCommandComplete("SELECT 1")
		s.sendError("22012", "division by zero")
		s.sendReady(proto.TxnFailed)
		if query := s.expectSimpleQuery(); query != "ROLLBACK" {
			s.fail("query = %q", query)
			return
		}
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	rows, err := tx.QueryContext(ctx, "SELECT 1; SELECT 1/0")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("first row missing: %v", rows.Err())
	}
	if rows.Next() {
		t.Fatal("the failing second statement returned a row")
	}
	var serverErr *Error
	if !errors.As(rows.Err(), &serverErr) || serverErr.Code != "22012" {
		t.Fatalf("Rows.Err = %v, want SQLSTATE 22012", rows.Err())
	}
	_ = rows.Close()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping after the rollback: %v", err)
	}
}

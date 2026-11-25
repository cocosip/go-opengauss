package opengauss

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cocosip/go-opengauss/internal/oid"
	"github.com/cocosip/go-opengauss/internal/proto"
)

// The tests in this file follow the official openGauss Go guide
// (example_common_operations_go) step by step against the scripted peer: DDL,
// a prepared insert loop, a prepared update with RowsAffected, a scalar
// function call, a manual transaction and a JSONB round trip. The live
// counterpart in TestLiveOfficialExamples runs the same steps on a server and
// adds row iteration and rollback checks.

// prepareStatement scripts the Parse/Describe/Sync handshake for a named
// statement and returns the statement name.
func prepareStatement(t *testing.T, s *fakeSession, query string, parameterOIDs ...int32) string {
	t.Helper()
	name, prepared := s.expectPrepare()
	if prepared != query {
		s.fail("prepared query = %q, want %q", prepared, query)
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
	s.sendParameterDescription(parameterOIDs...)
	s.sendNoData()
	s.sendReady(proto.TxnIdle)
	return name
}

// closeStatement scripts the Close/Sync/CloseComplete exchange of a named
// statement.
func closeStatement(s *fakeSession, name string) {
	closeBody := s.expectType(proto.MsgClose)
	closer := &proto.ReadBuf{B: closeBody}
	if target := closer.Byte(); target != 'S' {
		s.fail("Close target = %q, want statement", target)
	}
	if closed := closer.String(); closed != name {
		s.fail("closed statement = %q, want %q", closed, name)
	}
	s.expectType(proto.MsgSync)
	s.sendCloseComplete()
	s.sendReady(proto.TxnIdle)
}

func TestOfficialExampleCreateTableAndPreparedInsertLoop(t *testing.T) {
	const createTable = `CREATE TABLE IF NOT EXISTS customer_t1 (
    c_customer_sk   INTEGER,
    c_customer_name VARCHAR(32)
);`
	const insertSQL = "INSERT INTO customer_t1 (c_customer_sk, c_customer_name) VALUES ($1, $2)"

	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != createTable {
			s.fail("create query = %q, want %q", query, createTable)
		}
		s.sendCommandComplete("CREATE TABLE")
		s.sendReady(proto.TxnIdle)

		name := prepareStatement(t, s, insertSQL, oid.Int4, oid.Varchar)
		for i := 0; i < 3; i++ {
			statement, args := s.expectBindExecute()
			if statement != name {
				s.fail("bind target = %q, want %q", statement, name)
			}
			if len(args) != 2 || args[0].text != fmt.Sprint(i) || args[1].text != fmt.Sprintf("data %d", i) {
				s.fail("bound parameters = %+v, want %d and data %d", args, i, i)
			}
			s.sendBindComplete()
			s.sendNoData()
			s.sendCommandComplete("INSERT 0 1")
			s.sendReady(proto.TxnIdle)
		}
		closeStatement(s, name)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)

	if _, err := db.ExecContext(ctx, createTable); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	stmt, err := db.PrepareContext(ctx, insertSQL)
	if err != nil {
		t.Fatalf("PrepareContext: %v", err)
	}
	for i := 0; i < 3; i++ {
		res, err := stmt.ExecContext(ctx, i, fmt.Sprintf("data %d", i))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
		if affected, err := res.RowsAffected(); err != nil || affected != 1 {
			t.Fatalf("insert %d RowsAffected = %d, %v; want 1, nil", i, affected, err)
		}
	}
	if err := stmt.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestOfficialExamplePreparedUpdateReportsRowsAffected(t *testing.T) {
	const updateSQL = "UPDATE customer_t1 SET c_customer_name = $1 WHERE c_customer_sk = $2"

	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		name := prepareStatement(t, s, updateSQL, oid.Varchar, oid.Int4)

		statement, args := s.expectBindExecute()
		if statement != name {
			s.fail("bind target = %q, want %q", statement, name)
		}
		if len(args) != 2 || args[0].text != "new Data" || args[1].text != "1" {
			s.fail("bound parameters = %+v, want new Data and 1", args)
		}
		s.sendBindComplete()
		s.sendNoData()
		s.sendCommandComplete("UPDATE 1")
		s.sendReady(proto.TxnIdle)
		closeStatement(s, name)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)

	stmt, err := db.PrepareContext(ctx, updateSQL)
	if err != nil {
		t.Fatalf("PrepareContext: %v", err)
	}
	res, err := stmt.ExecContext(ctx, "new Data", 1)
	if err != nil {
		t.Fatalf("ExecContext: %v", err)
	}
	if affected, err := res.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("RowsAffected = %d, %v; want 1, nil", affected, err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestOfficialExampleScalarFunctionCall(t *testing.T) {
	const createFunction = `
CREATE OR REPLACE FUNCTION testproc(a int, b int, c int)
RETURNS int
AS $$
BEGIN
    RETURN a + b + c;
END;
$$ LANGUAGE plpgsql;
`
	const callSQL = "SELECT testproc($1, $2, $3)"

	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != createFunction {
			s.fail("create function query = %q", query)
		}
		s.sendCommandComplete("CREATE FUNCTION")
		s.sendReady(proto.TxnIdle)

		query, args := s.expectExtended()
		if query != callSQL {
			s.fail("call query = %q, want %q", query, callSQL)
		}
		if len(args) != 3 || args[0].text != "20" || args[1].text != "50" || args[2].text != "90" {
			s.fail("bound parameters = %+v, want 20, 50 and 90", args)
		}
		s.sendParseComplete()
		s.sendBindComplete()
		s.sendRowDescription(fakeColumn{name: "testproc", oid: oid.Int4})
		s.sendDataRow("160")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)

	if _, err := db.ExecContext(ctx, createFunction); err != nil {
		t.Fatalf("CREATE FUNCTION: %v", err)
	}
	var sum int
	if err := db.QueryRowContext(ctx, callSQL, 20, 50, 90).Scan(&sum); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if sum != 160 {
		t.Errorf("testproc result = %d, want 160", sum)
	}
}

func TestOfficialExampleTransaction(t *testing.T) {
	const insertSQL = "INSERT INTO customer_t1 (c_customer_sk, c_customer_name) VALUES ($1, $2)"
	const updateSQL = "UPDATE customer_t1 SET c_customer_name = $1 WHERE c_customer_sk = $2"

	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)

		if query := s.expectSimpleQuery(); query != "BEGIN" {
			s.fail("begin query = %q", query)
		}
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		query, args := s.expectExtended()
		if query != insertSQL || len(args) != 2 || args[0].text != "100" || args[1].text != "tx-data" {
			s.fail("insert = %q %+v", query, args)
		}
		s.sendParseComplete()
		s.sendBindComplete()
		s.sendNoData()
		s.sendCommandComplete("INSERT 0 1")
		s.sendReady(proto.TxnInTransaction)

		query, args = s.expectExtended()
		if query != updateSQL || len(args) != 2 || args[0].text != "tx-updated" || args[1].text != "100" {
			s.fail("update = %q %+v", query, args)
		}
		s.sendParseComplete()
		s.sendBindComplete()
		s.sendNoData()
		s.sendCommandComplete("UPDATE 1")
		s.sendReady(proto.TxnInTransaction)

		if query := s.expectSimpleQuery(); query != "COMMIT" {
			s.fail("commit query = %q", query)
		}
		s.sendCommandComplete("COMMIT")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, insertSQL, 100, "tx-data"); err != nil {
		t.Fatalf("tx insert: %v", err)
	}
	if _, err := tx.ExecContext(ctx, updateSQL, "tx-updated", 100); err != nil {
		t.Fatalf("tx update: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestOfficialExampleJSONBRoundTrip(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	const createTable = `CREATE TABLE IF NOT EXISTS t_jsonb (
    id   serial PRIMARY KEY,
    data jsonb
);`
	const insertSQL = "INSERT INTO t_jsonb (data) VALUES ($1)"
	const selectSQL = "SELECT data FROM t_jsonb"
	const returningSQL = "INSERT INTO t_jsonb (data) VALUES ($1) RETURNING id"
	document := `{"name":"Alice","age":30}`
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != createTable {
			s.fail("create query = %q", query)
		}
		s.sendCommandComplete("CREATE TABLE")
		s.sendReady(proto.TxnIdle)

		query, args := s.expectExtended()
		if query != insertSQL || len(args) != 1 || args[0].text != document {
			s.fail("insert = %q %+v", query, args)
		}
		s.sendParseComplete()
		s.sendBindComplete()
		s.sendNoData()
		s.sendCommandComplete("INSERT 0 1")
		s.sendReady(proto.TxnIdle)

		if query := s.expectSimpleQuery(); query != selectSQL {
			s.fail("select query = %q", query)
		}
		s.sendRowDescription(fakeColumn{name: "data", oid: oid.JSONB})
		s.sendDataRow(document)
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)

		query, args = s.expectExtended()
		if query != returningSQL || len(args) != 1 || args[0].text != document {
			s.fail("returning insert = %q %+v", query, args)
		}
		s.sendParseComplete()
		s.sendBindComplete()
		s.sendRowDescription(fakeColumn{name: "id", oid: oid.Int4})
		s.sendDataRow("1")
		s.sendCommandComplete("INSERT 0 1")
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)

	if _, err := db.ExecContext(ctx, createTable); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.ExecContext(ctx, insertSQL, document); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rows, err := db.QueryContext(ctx, selectSQL)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("no row returned: %v", rows.Err())
	}
	var value any
	if err := rows.Scan(&value); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	raw, ok := value.(string)
	if !ok {
		t.Fatalf("jsonb value = %T, want the documented string", value)
	}
	if rows.Next() {
		t.Fatal("more than one row returned")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows close: %v", err)
	}
	var decoded payload
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("Unmarshal(%q): %v", raw, err)
	}
	if decoded.Name != "Alice" || decoded.Age != 30 {
		t.Errorf("decoded payload = %+v, want Alice/30", decoded)
	}

	var id int
	if err := db.QueryRowContext(ctx, returningSQL, document).Scan(&id); err != nil {
		t.Fatalf("RETURNING Scan: %v", err)
	}
	if id != 1 {
		t.Errorf("returned id = %d, want 1", id)
	}
}

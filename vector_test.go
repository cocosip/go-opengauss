package opengauss

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocosip/go-opengauss/internal/proto"
)

const vectorQuery = `SELECT id FROM demotable ORDER BY embedding <-> $1 LIMIT $2`

// expectCurrentSetting answers the parameter read with the scripted value.
func (s *fakeSession) expectCurrentSetting(name, value string) {
	query, args := s.expectExtended()
	if query != readSettingSQL || len(args) != 1 || args[0].text != name {
		s.fail("current_setting query = %q args = %+v", query, args)
		return
	}
	s.sendRowDescription(fakeColumn{name: "current_setting", oid: 25})
	s.sendDataRow(value)
	s.sendCommandComplete("SELECT 1")
	s.sendReady(proto.TxnIdle)
}

// expectSetConfig answers a parameter write.
func (s *fakeSession) expectSetConfig(name, value string) {
	query, args := s.expectExtended()
	if query != writeSettingSQL || len(args) != 2 || args[0].text != name || args[1].text != value {
		s.fail("set_config query = %q args = %+v", query, args)
		return
	}
	s.sendRowDescription(fakeColumn{name: "set_config", oid: 25})
	s.sendDataRow(value)
	s.sendCommandComplete("SELECT 1")
	s.sendReady(proto.TxnIdle)
}

// expectVectorQuery answers one search and returns the scripted ids.
func (s *fakeSession) expectVectorQuery(wantArgs []string, ids ...string) {
	query, args := s.expectExtended()
	if query != vectorQuery {
		s.fail("search query = %q, want %q", query, vectorQuery)
		return
	}
	if len(args) != len(wantArgs) {
		s.fail("search args = %+v, want %q", args, wantArgs)
		return
	}
	for i := range wantArgs {
		if args[i].text != wantArgs[i] {
			s.fail("search arg %d = %q, want %q", i, args[i].text, wantArgs[i])
		}
	}
	s.sendRowDescription(fakeColumn{name: "id", oid: 23})
	for _, id := range ids {
		s.sendDataRow(id)
	}
	s.sendCommandComplete("SELECT 1")
	s.sendReady(proto.TxnIdle)
}

// vectorTestDB returns a handle whose configuration is valid but whose endpoint
// is never contacted by the argument checks these tests exercise.
func vectorTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("opengauss", "host=127.0.0.1 port=1 user=test dbname=test sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// readCommand reads one statement from the client: either an extended-protocol
// query or a simple one, and reports Terminate as terminated.
func (s *fakeSession) readCommand() (query string, args []fakeParam, terminated bool) {
	typ, body := s.readMessage()
	if s.err != nil {
		return "", nil, true
	}
	switch typ {
	case proto.MsgTerminate:
		return "", nil, true
	case proto.MsgQuery:
		r := &proto.ReadBuf{B: body}
		return r.String(), nil, false
	case proto.MsgParse:
		r := &proto.ReadBuf{B: body}
		_ = r.String() // statement name
		query = r.String()
		for count := int(r.Int16()); count > 0; count-- {
			r.Int32()
		}
		if r.Err != nil {
			s.fail("malformed Parse message: %v", r.Err)
			return "", nil, true
		}
		bind := s.expectType(proto.MsgBind)
		rb := &proto.ReadBuf{B: bind}
		_ = rb.String() // portal
		_ = rb.String() // statement
		formatCount := int(rb.Int16())
		for i := 0; i < formatCount; i++ {
			rb.Int16()
		}
		paramCount := int(rb.Int16())
		args = make([]fakeParam, paramCount)
		for i := 0; i < paramCount; i++ {
			length := int(rb.Int32())
			if length < 0 {
				args[i] = fakeParam{isNull: true}
				continue
			}
			args[i] = fakeParam{text: string(rb.Bytes(length))}
		}
		resultFormats := int(rb.Int16())
		for i := 0; i < resultFormats; i++ {
			rb.Int16()
		}
		if rb.Err != nil {
			s.fail("malformed Bind message: %v", rb.Err)
			return "", nil, true
		}
		s.expectType(proto.MsgDescribe)
		s.expectType(proto.MsgExecute)
		s.expectType(proto.MsgSync)
		return query, args, false
	default:
		s.fail("unexpected client message %q", typ)
		return "", nil, true
	}
}

func TestExecuteMultiSearchPreservesOrderAndRestoresSettings(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectCurrentSetting("hnsw_ef_search", "20")
		s.expectSetConfig("hnsw_ef_search", "40")
		s.expectVectorQuery([]string{"[1,2,3]", "2"}, "1")
		s.expectVectorQuery([]string{"[2,2,2]", "3"}, "2")
		s.expectSetConfig("hnsw_ef_search", "20")
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	results, err := ExecuteMultiSearch(testContext(t), db, vectorQuery,
		[][]any{{"[1,2,3]", 2}, {"[2,2,2]", 3}},
		map[string]any{"hnsw_ef_search": "40"}, 1)
	if err != nil {
		t.Fatalf("ExecuteMultiSearch: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
	for i, result := range results {
		if result.Index != i || result.Err != nil {
			t.Fatalf("result %d = %+v", i, result)
		}
	}
	if id := results[0].Rows[0]["id"]; id != int64(1) {
		t.Errorf("first result id = %v, want 1", id)
	}
	if id := results[1].Rows[0]["id"]; id != int64(2) {
		t.Errorf("second result id = %v, want 2", id)
	}
	if server.connections() != 1 {
		t.Errorf("connections = %d, want one pinned session", server.connections())
	}
}

func TestExecuteMultiSearchBoundsConcurrency(t *testing.T) {
	const query = `SELECT id FROM demotable ORDER BY embedding <=> $1 LIMIT $2`
	var arrived atomic.Int32
	ready := make(chan struct{})
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		// Both sessions must exist before either answers, which proves the two
		// workers hold their own pooled connections.
		if arrived.Add(1) == 2 {
			close(ready)
		}
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
		}
		for {
			commandQuery, args, terminated := s.readCommand()
			if terminated {
				return
			}
			if commandQuery != query {
				s.fail("search query = %q, want %q", commandQuery, query)
				return
			}
			if len(args) != 2 {
				s.fail("search args = %+v", args)
				return
			}
			id, known := map[string]string{"[1,2,3]": "1", "[2,2,2]": "2", "[3,3,3]": "3"}[args[0].text]
			if !known {
				s.fail("unexpected vector %q", args[0].text)
				return
			}
			s.sendRowDescription(fakeColumn{name: "id", oid: 23})
			s.sendDataRow(id)
			s.sendCommandComplete("SELECT 1")
			s.sendReady(proto.TxnIdle)
		}
	})

	db, err := sql.Open("opengauss", testDSN(server, ""))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	defer func() { _ = db.Close() }()

	// Three argument sets on two workers exercises a non-divisible split:
	// the first worker gets two items and the second one item.
	results, err := ExecuteMultiSearch(testContext(t), db, query,
		[][]any{{"[1,2,3]", 2}, {"[2,2,2]", 3}, {"[3,3,3]", 4}}, nil, 2)
	if err != nil {
		t.Fatalf("ExecuteMultiSearch: %v", err)
	}
	wantIDs := []int64{1, 2, 3}
	for i, want := range wantIDs {
		if results[i].Err != nil {
			t.Fatalf("result %d error = %v", i, results[i].Err)
		}
		if id := results[i].Rows[0]["id"]; id != want {
			t.Errorf("result %d id = %v, want %d", i, id, want)
		}
	}
	if server.connections() != 2 {
		t.Errorf("connections = %d, want both workers to hold a session", server.connections())
	}
}

func TestExecuteMultiSearchCapsWorkersAtArgumentSets(t *testing.T) {
	const query = `SELECT id FROM demotable ORDER BY embedding <-> $1`
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		gotQuery, args := s.expectExtended()
		if gotQuery != query || len(args) != 1 || args[0].text != "[1,2,3]" {
			s.fail("search = %q %+v", gotQuery, args)
			return
		}
		s.sendRowDescription(fakeColumn{name: "id", oid: 23})
		s.sendDataRow("1")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	results, err := ExecuteMultiSearch(testContext(t), db, query,
		[][]any{{"[1,2,3]"}}, nil, 5)
	if err != nil {
		t.Fatalf("ExecuteMultiSearch: %v", err)
	}
	if len(results) != 1 || results[0].Err != nil {
		t.Fatalf("results = %+v, want one successful result", results)
	}
	if server.connections() != 1 {
		t.Errorf("connections = %d, want one worker for one argument set", server.connections())
	}
}

func TestExecuteMultiSearchPinsSettingsPerWorker(t *testing.T) {
	var arrived atomic.Int32
	ready := make(chan struct{})
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if arrived.Add(1) == 2 {
			close(ready)
		}
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
		}
		s.expectCurrentSetting("hnsw_ef_search", "10")
		s.expectSetConfig("hnsw_ef_search", "50")
		query, args := s.expectExtended()
		if query != vectorQuery || len(args) != 2 {
			s.fail("search = %q %+v", query, args)
			return
		}
		id, known := map[string]string{"[1,2,3]": "1", "[2,2,2]": "2"}[args[0].text]
		if !known {
			s.fail("unexpected vector %q", args[0].text)
			return
		}
		s.sendRowDescription(fakeColumn{name: "id", oid: 23})
		s.sendDataRow(id)
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectSetConfig("hnsw_ef_search", "10")
		s.expectType(proto.MsgTerminate)
	})

	db, err := sql.Open("opengauss", testDSN(server, ""))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	defer func() { _ = db.Close() }()

	results, err := ExecuteMultiSearch(testContext(t), db, vectorQuery,
		[][]any{{"[1,2,3]", 2}, {"[2,2,2]", 3}},
		map[string]any{"hnsw_ef_search": "50"}, 2)
	if err != nil {
		t.Fatalf("ExecuteMultiSearch: %v", err)
	}
	if id := results[0].Rows[0]["id"]; id != int64(1) {
		t.Errorf("result 0 id = %v, want 1", id)
	}
	if id := results[1].Rows[0]["id"]; id != int64(2) {
		t.Errorf("result 1 id = %v, want 2", id)
	}
	if server.connections() != 2 {
		t.Errorf("connections = %d, want each worker to apply its own settings", server.connections())
	}
}

func TestExecuteMultiSearchReportsPerItemErrors(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectCurrentSetting("nprobes", "10")
		s.expectSetConfig("nprobes", "30")

		query, args := s.expectExtended()
		if query != vectorQuery || len(args) != 2 || args[0].text != "[1,2,3]" {
			s.fail("first search = %q %+v", query, args)
			return
		}
		s.sendError("42P01", `relation "demotable" does not exist`)
		s.sendReady(proto.TxnIdle)

		s.expectVectorQuery([]string{"[2,2,2]", "3"}, "7")
		// The settings are restored even though one query failed.
		s.expectSetConfig("nprobes", "10")
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	results, err := ExecuteMultiSearch(testContext(t), db, vectorQuery,
		[][]any{{"[1,2,3]", 2}, {"[2,2,2]", 3}},
		map[string]any{"nprobes": 30}, 1)
	if err != nil {
		t.Fatalf("ExecuteMultiSearch: %v", err)
	}
	var serverErr *Error
	if !errors.As(results[0].Err, &serverErr) || serverErr.Code != "42P01" {
		t.Fatalf("result 0 error = %v, want SQLSTATE 42P01", results[0].Err)
	}
	if results[0].Rows != nil {
		t.Errorf("result 0 rows = %v, want none", results[0].Rows)
	}
	if results[1].Err != nil {
		t.Fatalf("result 1 error = %v", results[1].Err)
	}
	if id := results[1].Rows[0]["id"]; id != int64(7) {
		t.Errorf("result 1 id = %v, want 7", id)
	}
}

func TestExecuteMultiSearchFailsAllItemsWhenAParameterCannotBeRead(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		query, args := s.expectExtended()
		if query != readSettingSQL || len(args) != 1 || args[0].text != "nope" {
			s.fail("read = %q %+v", query, args)
			return
		}
		s.sendError("42704", `unrecognized configuration parameter "nope"`)
		s.sendReady(proto.TxnIdle)
		// No search may run with the requested settings missing.
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	results, err := ExecuteMultiSearch(testContext(t), db, vectorQuery,
		[][]any{{"[1,2,3]", 2}, {"[2,2,2]", 3}},
		map[string]any{"nope": "1"}, 1)
	if err != nil {
		t.Fatalf("ExecuteMultiSearch: %v", err)
	}
	for i, result := range results {
		var serverErr *Error
		if !errors.As(result.Err, &serverErr) || serverErr.Code != "42704" {
			t.Fatalf("result %d error = %v, want SQLSTATE 42704", i, result.Err)
		}
	}
}

func TestExecuteMultiSearchRejectsInvalidArguments(t *testing.T) {
	db := vectorTestDB(t)
	validArgs := [][]any{{"[1,2,3]", 2}}
	validSettings := map[string]any{"hnsw_ef_search": "40"}
	tests := []struct {
		name        string
		db          *sql.DB
		ctx         context.Context
		query       string
		args        [][]any
		scanParams  map[string]any
		threadCount int
		want        string
	}{
		{"nil db", nil, context.Background(), vectorQuery, validArgs, nil, 1, "needs a *sql.DB"},
		{"nil context", db, nil, vectorQuery, validArgs, nil, 1, "non-nil context"},
		{"empty query", db, context.Background(), "  ", validArgs, nil, 1, "empty"},
		{"not a select", db, context.Background(), "INSERT INTO t VALUES ($1) <->", validArgs, nil, 1, "start with SELECT"},
		{"two statements", db, context.Background(), "SELECT 1 <-> 2; SELECT 3", validArgs, nil, 1, "exactly one statement"},
		{"no vector operator", db, context.Background(), "SELECT id FROM t", validArgs, nil, 1, "vector operator"},
		{"operator only in a literal", db, context.Background(), "SELECT 'a <-> b' FROM t", validArgs, nil, 1, "vector operator"},
		{"no arguments", db, context.Background(), vectorQuery, nil, nil, 1, "at least one argument"},
		{"empty argument set", db, context.Background(), vectorQuery, [][]any{{}}, nil, 1, "argument set 0 is empty"},
		{"no threads", db, context.Background(), vectorQuery, validArgs, nil, 0, "positive thread count"},
		{"nil parameter value", db, context.Background(), vectorQuery, validArgs, map[string]any{"hnsw_ef_search": nil}, 1, "no value"},
		{"unsupported parameter value", db, context.Background(), vectorQuery, validArgs, map[string]any{"hnsw_ef_search": struct{}{}}, 1, "scan parameter"},
	}
	for _, tc := range tests {
		_, err := ExecuteMultiSearch(tc.ctx, tc.db, tc.query, tc.args, tc.scanParams, tc.threadCount)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error = %v, want text %q", tc.name, err, tc.want)
		}
	}
	if _, err := ExecuteMultiSearch(context.Background(), db, vectorQuery, validArgs, validSettings, 1); err != nil {
		t.Fatalf("a valid call returned a global error: %v", err)
	}
}

func TestExecuteMultiSearchCancelledContextMarksEveryItem(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results, err := ExecuteMultiSearch(ctx, vectorTestDB(t), vectorQuery,
		[][]any{{"[1,2,3]", 2}, {"[2,2,2]", 3}}, nil, 1)
	if err != nil {
		t.Fatalf("ExecuteMultiSearch: %v", err)
	}
	for i, result := range results {
		if !errors.Is(result.Err, context.Canceled) {
			t.Errorf("result %d error = %v, want context.Canceled", i, result.Err)
		}
	}
}

func TestExecuteMultiSearchRestoresAfterPartialSettingFailure(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectCurrentSetting("a_setting", "0")
		s.expectSetConfig("a_setting", "1")
		query, args := s.expectExtended()
		if query != readSettingSQL || len(args) != 1 || args[0].text != "b_setting" {
			s.fail("second read = %q %+v", query, args)
			return
		}
		s.sendError("42704", `unrecognized configuration parameter "b_setting"`)
		s.sendReady(proto.TxnIdle)
		// The first parameter must be restored before the session is reused.
		s.expectSetConfig("a_setting", "0")
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	results, err := ExecuteMultiSearch(testContext(t), db, vectorQuery,
		[][]any{{"[1,2,3]", 2}, {"[2,2,2]", 3}},
		map[string]any{"a_setting": "1", "b_setting": "2"}, 1)
	if err != nil {
		t.Fatalf("ExecuteMultiSearch: %v", err)
	}
	for i, result := range results {
		var serverErr *Error
		if !errors.As(result.Err, &serverErr) || serverErr.Code != "42704" {
			t.Fatalf("result %d error = %v, want SQLSTATE 42704", i, result.Err)
		}
	}
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if server.connections() != 1 {
		t.Errorf("connections = %d, want the restored session reused", server.connections())
	}
}

func TestExecuteMultiSearchCancelledQueryDiscardsSession(t *testing.T) {
	const query = `SELECT id FROM demotable ORDER BY embedding <-> $1`
	var server *fakeServer
	var connections atomic.Int32
	queryStarted := make(chan struct{})
	server = newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if connections.Add(1) == 1 {
			s.expectCurrentSetting("hnsw_ef_search", "10")
			s.expectSetConfig("hnsw_ef_search", "50")
			s.expectExtended() // the search is in flight
			close(queryStarted)
			select {
			case <-server.cancels:
			case <-time.After(5 * time.Second):
				s.fail("no cancel request arrived")
				return
			}
			s.sendError("57014", "canceling statement due to user request")
			s.sendReady(proto.TxnIdle)
			return
		}
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-queryStarted
		cancel()
	}()

	results, err := ExecuteMultiSearch(ctx, db, query, [][]any{{"[1,2,3]"}},
		map[string]any{"hnsw_ef_search": "50"}, 1)
	if err != nil {
		t.Fatalf("ExecuteMultiSearch: %v", err)
	}
	if !errors.Is(results[0].Err, context.Canceled) {
		// The server may report the cancellation first; its SQLSTATE is then
		// the more precise error and must be preserved.
		var serverErr *Error
		if !errors.As(results[0].Err, &serverErr) || serverErr.Code != "57014" {
			t.Fatalf("result error = %v, want context.Canceled or SQLSTATE 57014", results[0].Err)
		}
	}
	// The interrupted session cannot be trusted, so it is replaced.
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if server.connections() != 2 {
		t.Errorf("connections = %d, want the cancelled session discarded", server.connections())
	}
}

func TestExecuteMultiSearchRowsKeepDriverValues(t *testing.T) {
	const query = `SELECT id, v, v, blob, missing FROM demotable ORDER BY embedding <-> $1`
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		gotQuery, args := s.expectExtended()
		if gotQuery != query || len(args) != 1 || args[0].text != "[1,2,3]" {
			s.fail("search = %q %+v", gotQuery, args)
			return
		}
		s.sendRowDescription(
			fakeColumn{name: "id", oid: 23},
			fakeColumn{name: "v", oid: 25},
			fakeColumn{name: "v", oid: 25},
			fakeColumn{name: "blob", oid: 17},
			fakeColumn{name: "missing", oid: 25},
		)
		s.sendDataRow("1", "first", "second", `\x01ff`, nil)
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	results, err := ExecuteMultiSearch(testContext(t), db, query,
		[][]any{{"[1,2,3]"}}, nil, 1)
	if err != nil {
		t.Fatalf("ExecuteMultiSearch: %v", err)
	}
	want := []map[string]any{{
		"id":      int64(1),
		"v":       "second",
		"blob":    []byte{0x01, 0xff},
		"missing": nil,
	}}
	if !reflect.DeepEqual(results[0].Rows, want) {
		t.Errorf("rows = %#v, want %#v", results[0].Rows, want)
	}
}

func TestValidateVectorQuery(t *testing.T) {
	accepted := []string{
		`SELECT id FROM demotable ORDER BY embedding <-> $1 LIMIT 2`,
		"  select id from t order by e <=> $1",
		"-- search\nSELECT id FROM t ORDER BY e <#> $1 LIMIT 1",
		"/* search */ SELECT id FROM t ORDER BY e <+> $1",
		`SELECT id FROM t WHERE name = 'a;b' ORDER BY e <~> $1`,
		`SELECT id FROM t WHERE "odd;name" = 'x' ORDER BY e <%> $1`,
		`SELECT id FROM t ORDER BY e <-> $1;`,
		`SELECT*FROM t ORDER BY e <-> $1`,
		`SELECT"col" FROM t ORDER BY e <-> $1`,
		`SELECT$1 <-> $2`,
		`SELECT/*note*/e <-> $1 FROM t`,
	}
	for _, query := range accepted {
		if err := validateVectorQuery(query); err != nil {
			t.Errorf("validateVectorQuery(%q) = %v, want nil", query, err)
		}
	}
	rejected := []string{
		"",
		"SELECT 1",
		"UPDATE t SET e = $1",
		"SELECT 1 <-> 2; SELECT 3 <-> 4",
		`SELECT 'a <-> b' FROM t`,
		"-- SELECT <-> from a comment",
		"SELECTED <-> FROM t",
		"SELECT 1 <-> 2; -- x\rDROP TABLE t",
	}
	for _, query := range rejected {
		if err := validateVectorQuery(query); err == nil {
			t.Errorf("validateVectorQuery(%q) = nil, want an error", query)
		}
	}
}

func TestScanSettingValue(t *testing.T) {
	tests := []struct {
		value any
		want  string
	}{
		{"40", "40"},
		{40, "40"},
		{int64(40), "40"},
		{true, "1"},
		{1.5, "1.5"},
	}
	for _, tc := range tests {
		text, err := scanSettingValue("guc", tc.value)
		if err != nil || text != tc.want {
			t.Errorf("scanSettingValue(%v) = (%q, %v), want %q", tc.value, text, err, tc.want)
		}
	}
	if _, err := scanSettingValue("guc", struct{}{}); err == nil {
		t.Error("an unsupported scan parameter value was accepted")
	}
}

func TestExecuteMultiSearchDiscardsSessionWhenRestoreFails(t *testing.T) {
	const query = `SELECT id FROM demotable ORDER BY embedding <-> $1`
	var connections atomic.Int32
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if connections.Add(1) == 1 {
			s.expectCurrentSetting("hnsw_ef_search", "10")
			s.expectSetConfig("hnsw_ef_search", "20")
			searchQuery, args := s.expectExtended()
			if searchQuery != query || len(args) != 1 || args[0].text != "[1,2,3]" {
				s.fail("search = %q %+v", searchQuery, args)
				return
			}
			s.sendRowDescription(fakeColumn{name: "id", oid: 23})
			s.sendDataRow("1")
			s.sendCommandComplete("SELECT 1")
			s.sendReady(proto.TxnIdle)

			restoreQuery, restoreArgs := s.expectExtended()
			if restoreQuery != writeSettingSQL || len(restoreArgs) != 2 || restoreArgs[1].text != "10" {
				s.fail("restore = %q %+v", restoreQuery, restoreArgs)
				return
			}
			// The session cannot be returned to its previous state, so the
			// driver must drop it instead of pooling changed parameters.
			s.sendError("57P01", "terminating connection due to administrator command")
			s.sendReady(proto.TxnIdle)
			return
		}
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	results, err := ExecuteMultiSearch(ctx, db, query, [][]any{{"[1,2,3]"}},
		map[string]any{"hnsw_ef_search": "20"}, 1)
	if err != nil {
		t.Fatalf("ExecuteMultiSearch: %v", err)
	}
	if results[0].Err != nil {
		t.Fatalf("search error = %v", results[0].Err)
	}
	// The discarded session is replaced by a fresh one.
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if server.connections() != 2 {
		t.Errorf("connections = %d, want the unrestored session discarded", server.connections())
	}
}

func TestExecuteMultiSearchRowsKeepColumnNames(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		query, args := s.expectExtended()
		if query != `SELECT id, embedding FROM demotable ORDER BY embedding <-> $1` {
			s.fail("query = %q", query)
			return
		}
		if len(args) != 1 || args[0].text != "[1,2,3]" {
			s.fail("args = %+v", args)
			return
		}
		s.sendRowDescription(fakeColumn{name: "id", oid: 23}, fakeColumn{name: "embedding", oid: 25})
		s.sendDataRow("1", "[1,2,3]")
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	results, err := ExecuteMultiSearch(testContext(t), db,
		`SELECT id, embedding FROM demotable ORDER BY embedding <-> $1`,
		[][]any{{"[1,2,3]"}}, nil, 1)
	if err != nil {
		t.Fatalf("ExecuteMultiSearch: %v", err)
	}
	want := []map[string]any{{"id": int64(1), "embedding": "[1,2,3]"}}
	if !reflect.DeepEqual(results[0].Rows, want) {
		t.Errorf("rows = %#v, want %#v", results[0].Rows, want)
	}
}

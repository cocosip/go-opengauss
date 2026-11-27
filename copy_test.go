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
		options CopyInOptions
		want    string
	}{
		{table: "logs", want: `COPY "logs" FROM STDIN`},
		{table: "logs", columns: []string{"id", "msg"}, want: `COPY "logs" ("id", "msg") FROM STDIN`},
		{table: "public.logs", columns: []string{"id"}, want: `COPY "public"."logs" ("id") FROM STDIN`},
		{table: `we"ird`, want: `COPY "we""ird" FROM STDIN`},
		// CSV always states its format; other options are sent only when they
		// differ from the CSV defaults the encoder applies.
		{
			table: "logs", columns: []string{"id", "msg"},
			options: CopyInOptions{Format: CopyFormatCSV},
			want:    `COPY "logs" ("id", "msg") FROM STDIN WITH (FORMAT csv)`,
		},
		{
			table: "logs", options: CopyInOptions{Format: CopyFormatCSV, Header: true},
			want: `COPY "logs" FROM STDIN WITH (FORMAT csv, HEADER true)`,
		},
		{
			table: "logs",
			options: CopyInOptions{
				Format: CopyFormatCSV, Delimiter: '|', Quote: '~', Escape: '\\', Null: "NA",
			},
			want: `COPY "logs" FROM STDIN WITH (FORMAT csv, DELIMITER '|', QUOTE '~', ESCAPE E'\\', NULL 'NA')`,
		},
		{
			// A quote character is escaped as a doubled SQL quote.
			table:   "logs",
			options: CopyInOptions{Format: CopyFormatCSV, Delimiter: ';', Quote: '\''},
			want:    `COPY "logs" FROM STDIN WITH (FORMAT csv, DELIMITER ';', QUOTE '''')`,
		},
	}
	for _, tc := range tests {
		options, err := checkCopyInOptions(tc.options)
		if err != nil {
			t.Fatalf("checkCopyInOptions(%#v) = %v", tc.options, err)
		}
		if got := copyInSQL(tc.table, tc.columns, options); got != tc.want {
			t.Errorf("copyInSQL(%q, %v, %#v) = %q, want %q", tc.table, tc.columns, tc.options, got, tc.want)
		}
		// The encoder must read the statement back as the same options.
		kind, clauseEnd := copyDirection(copyInSQL(tc.table, tc.columns, options))
		if kind != copyIn {
			t.Fatalf("copyDirection(%q) = %v, want copyIn", copyInSQL(tc.table, tc.columns, options), kind)
		}
		parsed, err := parseCopyInOptions(copyInSQL(tc.table, tc.columns, options), clauseEnd)
		if err != nil {
			t.Fatalf("parseCopyInOptions(%q) = %v", copyInSQL(tc.table, tc.columns, options), err)
		}
		if parsed != options {
			t.Errorf("round trip of %#v produced %#v", options, parsed)
		}
	}
}

func TestCopyDirection(t *testing.T) {
	tests := []struct {
		statement string
		want      copyKind
	}{
		{"COPY logs FROM STDIN", copyIn},
		{"  copy logs (a, b) from stdin", copyIn},
		{"COPY logs TO STDOUT", copyOut},
		{"COPY (SELECT 'FROM STDIN' AS x) TO STDOUT", copyOut},
		{"COPY (SELECT 'TO STDOUT' AS x) TO STDOUT", copyOut},
		{"COPY (SELECT $$ FROM STDIN $$) TO STDOUT", copyOut},
		{"COPY (SELECT $tag$ TO STDOUT $tag$) TO STDOUT", copyOut},
		{"COPY (SELECT * FROM stdin) TO STDOUT", copyOut},
		{"COPY (SELECT 1) TO/**/STDOUT", copyOut},
		{"COPY demotable TO/**/STDOUT -- note", copyOut},
		{"COPY logs FROM/**/STDIN", copyIn},
		{"/* FROM STDIN */ COPY logs FROM STDIN", copyIn},
		{"-- TO STDOUT\nCOPY logs TO STDOUT", copyOut},
		{"-- COPY logs TO STDOUT\nSELECT 1", copyNone},
		{"COPY logs FROM 'file.txt'", copyNone},
		{"INSERT INTO logs VALUES ($1)", copyNone},
		{"copycat", copyNone},
	}
	for _, tc := range tests {
		if got, _ := copyDirection(tc.statement); got != tc.want {
			t.Errorf("copyDirection(%q) = %v, want %v", tc.statement, got, tc.want)
		}
	}
}

func TestEncodeCopyRow(t *testing.T) {
	row, err := encodeCopyRow(nil, namedValues([]driver.Value{
		nil,
		"plain",
		"tab\there\nnewline\\backslash\r",
		"vertical\vbackspace\bformfeed\f",
		int64(42),
		true,
	}), CopyInOptions{})
	if err != nil {
		t.Fatalf("encodeCopyRow: %v", err)
	}
	want := "\\N\tplain\ttab\\there\\nnewline\\\\backslash\\r\tvertical\\vbackspace\\bformfeed\\f\t42\t1\n"
	if string(row) != want {
		t.Errorf("row = %q, want %q", row, want)
	}

	if _, err := encodeCopyRow(nil, namedValues([]driver.Value{struct{}{}}), CopyInOptions{}); err == nil {
		t.Error("expected an unsupported value type to fail")
	}
}

func TestEncodeCopyCSVRow(t *testing.T) {
	options, err := checkCopyInOptions(CopyInOptions{Format: CopyFormatCSV})
	if err != nil {
		t.Fatalf("checkCopyInOptions: %v", err)
	}
	row, err := encodeCopyCSVRow(nil, namedValues([]driver.Value{
		nil,
		"",
		"plain",
		"comma,here",
		`say "hi"`,
		"line\nbreak",
		"carriage\rreturn",
		int64(42),
		true,
	}), options)
	if err != nil {
		t.Fatalf("encodeCopyCSVRow: %v", err)
	}
	// NULL is the unquoted empty field; an empty string, a separator, a quote
	// and a line break are quoted, and a quote doubles.
	want := ",\"\",plain,\"comma,here\",\"say \"\"hi\"\"\",\"line\nbreak\",\"carriage\rreturn\",42,1\n"
	if string(row) != want {
		t.Errorf("row = %q, want %q", row, want)
	}

	custom, err := checkCopyInOptions(CopyInOptions{
		Format: CopyFormatCSV, Delimiter: '|', Quote: '~', Escape: '\\', Null: "NA",
	})
	if err != nil {
		t.Fatalf("checkCopyInOptions: %v", err)
	}
	row, err = encodeCopyCSVRow(nil, namedValues([]driver.Value{
		nil,
		"NA",
		"a|b",
		"back\\slash",
		"tilde~here",
		"pipe",
	}), custom)
	if err != nil {
		t.Fatalf("encodeCopyCSVRow: %v", err)
	}
	// A literal value equal to the NULL string is quoted, the quote is escaped
	// with the escape character, and a lone escape character outside quotes
	// stays literal because CSV gives it no meaning there.
	want = "NA|~NA~|~a|b~|back\\slash|~tilde\\~here~|pipe\n"
	if string(row) != want {
		t.Errorf("row = %q, want %q", row, want)
	}

	if _, err := encodeCopyCSVRow(nil, namedValues([]driver.Value{struct{}{}}), options); err == nil {
		t.Error("expected an unsupported value type to fail")
	}
}

func TestCopyInOptionErrors(t *testing.T) {
	tests := []struct {
		statement string
		err       string
	}{
		{`COPY "logs" FROM STDIN WITH (FORMAT binary)`, "binary format"},
		{`COPY "logs" FROM STDIN BINARY`, "binary format"},
		{`COPY "logs" FROM STDIN WITH (FORMAT xsv)`, "unknown COPY format"},
		{`COPY "logs" FROM STDIN WITH (ENCODING 'GBK')`, "ENCODING \"GBK\" is not implemented"},
		{`COPY "logs" FROM STDIN WITH (FORMAT csv, FORCE_QUOTE *)`, "TO STDOUT only"},
		{`COPY "logs" FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL (a))`, "no column names"},
		{`COPY "logs" FROM STDIN WITH (FORMAT csv, FORCE_NULL (a))`, "no column names"},
		{`COPY "logs" FROM STDIN WITH (FORMAT csv, FORCE QUOTE a)`, "use WITH (FORCE_QUOTE ...)"},
		{`COPY "logs" FROM STDIN WITH (HEADER)`, "HEADER is available only with FORMAT csv"},
		{`COPY "logs" FROM STDIN WITH (DELIMITER '|')`, "DELIMITER is available only with FORMAT csv"},
		{`COPY "logs" FROM STDIN WITH (FORMAT csv, DELIMITER '||')`, "one single-byte character"},
		{`COPY "logs" FROM STDIN WITH (FORMAT csv, DELIMITER ';', QUOTE ';')`, "must differ"},
		{`COPY "logs" FROM STDIN WITH (FORMAT csv, HEADER maybe)`, "is not a boolean"},
		{`COPY "logs" FROM STDIN WITH (FORMAT csv, HEADER); SELECT 1`, "only statement"},
		{`COPY "logs" FROM STDIN WITH (FORMAT csv`, "not closed"},
	}
	for _, test := range tests {
		kind, clauseEnd := copyDirection(test.statement)
		if kind != copyIn {
			t.Fatalf("copyDirection(%q) = %v, want copyIn", test.statement, kind)
		}
		if _, err := parseCopyInOptions(test.statement, clauseEnd); err == nil ||
			!strings.Contains(err.Error(), test.err) {
			t.Errorf("parseCopyInOptions(%q) = %v, want %q", test.statement, err, test.err)
		}
	}
	// The zero options are the default text format.
	options, err := parseCopyInOptions(`COPY "logs" FROM STDIN`, len(`COPY "logs" FROM STDIN`))
	if err != nil {
		t.Fatalf("parseCopyInOptions: %v", err)
	}
	want := CopyInOptions{Format: CopyFormatText}
	if options != want {
		t.Errorf("options = %#v, want %#v", options, want)
	}
	// ENCODING is accepted when it names the text the driver writes.
	const encoded = `COPY "logs" FROM STDIN WITH (FORMAT csv, ENCODING 'UTF8')`
	_, clauseEnd := copyDirection(encoded)
	options, err = parseCopyInOptions(encoded, clauseEnd)
	if err != nil {
		t.Fatalf("parseCopyInOptions with ENCODING: %v", err)
	}
	if options.Format != CopyFormatCSV {
		t.Errorf("options = %#v, want the CSV format", options)
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
		// Nothing was written, so the client aborts the empty stream with
		// CopyFail instead of finishing it as if it had succeeded.
		typ, body := s.readMessage()
		if typ != proto.MsgCopyFail {
			s.fail("expected CopyFail, got %q (%q)", typ, body)
			return
		}
		if reason := strings.TrimSuffix(string(body), "\x00"); !strings.Contains(reason, "2 columns but the row has 3") {
			s.fail("CopyFail reason = %q", reason)
		}
		s.sendError("57014", "COPY from stdin failed")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)

		// The rolled-back session is synchronized and usable again.
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
	// The stream was aborted, so Close has nothing left to do.
	if err := copyIn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if server.connections() != 1 {
		t.Errorf("connections = %d, want the synchronized session reused", server.connections())
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
		// The server aborts the COPY and discards the rest of the stream. The
		// transaction is now failed, which the client must observe.
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
		s.sendReady(proto.TxnFailed)

		s.expectSimpleQuery()
		s.sendCommandComplete("ROLLBACK")
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
	if !errors.As(err, &serverErr) || serverErr.Code != "57014" {
		t.Fatalf("error = %v, want SQLSTATE 57014 from the server", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if server.connections() != 1 {
		t.Errorf("connections = %d, want the synchronized session reused", server.connections())
	}
}

func TestCopyInRequiresTransaction(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		// Preparing the COPY statement outside a transaction is rejected
		// locally, so the server sees only the ping that proves the pooled
		// session is still usable.
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)

	_, err := db.PrepareContext(ctx, `COPY "logs" FROM STDIN`)
	if err == nil || !strings.Contains(err.Error(), "inside a transaction") {
		t.Fatalf("Prepare of COPY outside a transaction = %v, want a transaction requirement", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestExecRejectsCopyFromStdin(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)

	if _, err := db.ExecContext(ctx, `COPY "logs" FROM STDIN`); err == nil || !strings.Contains(err.Error(), "CopyIn(tx, ...)") {
		t.Fatalf("Exec of COPY = %v, want direction to CopyIn", err)
	}
	if _, err := db.QueryContext(ctx, `COPY "logs" FROM STDIN`); err == nil || !strings.Contains(err.Error(), "CopyIn(tx, ...)") {
		t.Fatalf("Query of COPY = %v, want direction to CopyIn", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext after the rejected statements: %v", err)
	}
}

func TestCopyInRejectsUnimplementedOptions(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		// No COPY statement is sent for the rejected option forms.
		s.expectSimpleQuery()
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
	for _, test := range []struct {
		statement string
		err       string
	}{
		{`COPY "logs" FROM STDIN WITH (FORMAT binary)`, "binary format"},
		{`COPY "logs" FROM STDIN WITH (ENCODING 'GBK')`, "ENCODING \"GBK\" is not implemented"},
		{`COPY "logs" FROM STDIN WITH (FORMAT csv, FORCE_QUOTE *)`, "TO STDOUT only"},
		{`COPY "logs" FROM STDIN WITH (FORMAT csv, FORCE_NULL (a))`, "no column names"},
		{`COPY "logs" FROM STDIN WITH (HEADER)`, "HEADER is available only with FORMAT csv"},
	} {
		if _, err := tx.PrepareContext(ctx, test.statement); err == nil ||
			!strings.Contains(err.Error(), test.err) {
			t.Fatalf("Prepare(%q) = %v, want %q", test.statement, err, test.err)
		}
	}
	// A trailing separator is still the default text format.
	statement, err := tx.PrepareContext(ctx, `COPY "logs" FROM STDIN;`)
	if err != nil {
		t.Fatalf("Prepare with a trailing separator: %v", err)
	}
	if err := statement.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

// TestCopyInCSVStreamsRows writes a CSV stream through the helper and checks the
// statement the driver sent and the rows it produced, including the header row
// the caller supplies itself.
func TestCopyInCSVStreamsRows(t *testing.T) {
	rowsCh := make(chan []string, 1)
	const copyStatement = `COPY "logs" ("id", "msg") FROM STDIN WITH (FORMAT csv, HEADER true)`
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "BEGIN" {
			s.fail("begin query = %q", query)
		}
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		if query := s.expectSimpleQuery(); query != copyStatement {
			s.fail("copy query = %q, want %q", query, copyStatement)
		}
		s.sendCopyInResponse(0, 2)
		rowsCh <- s.readCopyRows()
		s.sendCommandComplete("COPY 3")
		s.sendReady(proto.TxnInTransaction)

		if query := s.expectSimpleQuery(); query != "COMMIT" {
			s.fail("commit query = %q", query)
		}
		s.sendCommandComplete("COMMIT")
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
	copyIn, err := CopyInWithOptions(tx, "logs", CopyInOptions{Format: CopyFormatCSV, Header: true}, "id", "msg")
	if err != nil {
		t.Fatalf("CopyInWithOptions: %v", err)
	}
	// The caller writes the header line the statement announced.
	rows := [][]any{
		{"id", "msg"},
		{int64(1), `a,b "quoted"`},
		{int64(2), nil},
		{int64(3), ""},
	}
	for i, row := range rows {
		if err := copyIn.Exec(row...); err != nil {
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

	want := []string{
		"id,msg\n",
		"1,\"a,b \"\"quoted\"\"\"\n",
		"2,\n",
		"3,\"\"\n",
	}
	if got := <-rowsCh; !reflect.DeepEqual(got, want) {
		t.Errorf("rows = %q, want %q", got, want)
	}
	if server.connections() != 1 {
		t.Errorf("connections = %d, want the stream to reuse one session", server.connections())
	}
}

// TestCopyInCSVFromStatement proves a hand-written CSV COPY statement decodes its
// own options, so a caller is not limited to the helper.
func TestCopyInCSVFromStatement(t *testing.T) {
	rowsCh := make(chan []string, 1)
	const copyStatement = `COPY "logs" FROM STDIN WITH (FORMAT csv, DELIMITER '|', NULL 'NA')`
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		if query := s.expectSimpleQuery(); query != copyStatement {
			s.fail("copy query = %q, want %q", query, copyStatement)
		}
		s.sendCopyInResponse(0, 2)
		rowsCh <- s.readCopyRows()
		s.sendCommandComplete("COPY 1")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	statement, err := tx.PrepareContext(ctx, copyStatement)
	if err != nil {
		t.Fatalf("PrepareContext: %v", err)
	}
	if _, err := statement.Exec(nil, "a|b"); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := statement.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	want := []string{"NA|\"a|b\"\n"}
	if got := <-rowsCh; !reflect.DeepEqual(got, want) {
		t.Errorf("rows = %q, want %q", got, want)
	}
}

func TestCopyInAbortsAfterClientRowError(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCopyInResponse(0, 2)
		first, _ := s.readMessage()
		if first != proto.MsgCopyData {
			s.fail("expected the first row, got %q", first)
			return
		}
		// The second row has the wrong width, so the client aborts the stream
		// instead of finishing it with the rows already sent.
		type2, body := s.readMessage()
		if type2 != proto.MsgCopyFail {
			s.fail("expected CopyFail, got %q (%q)", type2, body)
			return
		}
		s.sendError("57014", "COPY from stdin failed")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)

		s.expectPing()
		s.expectType(proto.MsgTerminate)
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
	if err := copyIn.Exec(1, "a"); err != nil {
		t.Fatalf("first row: %v", err)
	}
	if err := copyIn.Exec(2, "b", "extra"); err == nil ||
		!strings.Contains(err.Error(), "2 columns but the row has 3") {
		t.Fatalf("second row = %v, want the column count error", err)
	}
	if err := copyIn.Close(); err != nil {
		t.Fatalf("Close after the aborted stream: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if server.connections() != 1 {
		t.Errorf("connections = %d, want the session reused after the abort", server.connections())
	}
}

func TestCopyInRejectsSecondStream(t *testing.T) {
	rowsCh := make(chan []string, 1)
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		if query := s.expectSimpleQuery(); query != `COPY "table_a" ("x") FROM STDIN` {
			s.fail("copy query = %q", query)
			return
		}
		s.sendCopyInResponse(0, 1)
		rowsCh <- s.readCopyRows()
		s.sendCommandComplete("COPY 1")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)

		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	defer func() { _ = db.Close() }()
	ctx := testContext(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	first, err := CopyIn(tx, "table_a", "x")
	if err != nil {
		t.Fatalf("CopyIn table_a: %v", err)
	}
	second, err := CopyIn(tx, "table_b", "x")
	if err != nil {
		t.Fatalf("CopyIn table_b: %v", err)
	}
	if err := first.Exec(1); err != nil {
		t.Fatalf("first stream row: %v", err)
	}
	if err := second.Exec(2); err == nil || !strings.Contains(err.Error(), "another COPY stream") {
		t.Fatalf("second stream row = %v, want ownership rejection", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if rows := <-rowsCh; !reflect.DeepEqual(rows, []string{"1\n"}) {
		t.Errorf("rows = %q, want only the first stream's row", rows)
	}
}

func TestCopyInRejectsRowsAfterFinish(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCopyInResponse(0, 1)
		if rows := s.readCopyRows(); !reflect.DeepEqual(rows, []string{"1\n"}) {
			s.fail("rows = %q", rows)
			return
		}
		s.sendCommandComplete("COPY 1")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)

		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	defer func() { _ = db.Close() }()
	ctx := testContext(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	copyIn, err := CopyIn(tx, "logs", "v")
	if err != nil {
		t.Fatalf("CopyIn: %v", err)
	}
	if err := copyIn.Exec(1); err != nil {
		t.Fatalf("row: %v", err)
	}
	if err := copyIn.Exec(); err != nil {
		t.Fatalf("finishing Exec: %v", err)
	}
	if err := copyIn.Exec(2); err == nil || !strings.Contains(err.Error(), "already finished") {
		t.Fatalf("row after the stream finished = %v, want a finished-stream error", err)
	}
	if err := copyIn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestCopyInToleratesTrailingSemicolon(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		if query := s.expectSimpleQuery(); query != `COPY "logs" FROM STDIN;` {
			s.fail("copy query = %q", query)
			return
		}
		s.sendCopyInResponse(0, 1)
		if rows := s.readCopyRows(); !reflect.DeepEqual(rows, []string{"1\n"}) {
			s.fail("rows = %q", rows)
			return
		}
		s.sendCommandComplete("COPY 1")
		// A single trailing separator makes the server answer the empty
		// statement inside the same simple query.
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)

		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	defer func() { _ = db.Close() }()
	ctx := testContext(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	statement, err := tx.PrepareContext(ctx, `COPY "logs" FROM STDIN;`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if _, err := statement.ExecContext(ctx, 1); err != nil {
		t.Fatalf("row: %v", err)
	}
	if err := statement.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestParseCopyResponseFormats(t *testing.T) {
	body := func(overall byte, formats ...byte) []byte {
		m := proto.NewMessageWriter(proto.MsgCopyInResponse)
		m.Byte(overall)
		m.Int16(int16(len(formats)))
		for _, format := range formats {
			m.Int16(int16(format))
		}
		return m.Finish()[5:]
	}

	format, columns, err := parseCopyResponse(body(0, 0, 0))
	if err != nil || format != 0 || columns != 2 {
		t.Fatalf("parseCopyResponse = (%d, %d, %v), want text with two columns", format, columns, err)
	}
	if _, _, err := parseCopyResponse(body(0, 1)); err == nil ||
		!strings.Contains(err.Error(), "column 1 uses binary format") {
		t.Fatalf("per-column binary format = %v, want a refusal", err)
	}
	if _, _, err := parseCopyResponse([]byte{0, 0}); err == nil {
		t.Error("a truncated copy response was accepted")
	}
}

func TestCopyInReportsErrorBeforeStreamStarts(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendError("42P01", `relation "logs" does not exist`)
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)

		s.expectPing()
		s.expectType(proto.MsgTerminate)
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
	err = copyIn.Exec(1)
	var serverErr *Error
	if !errors.As(err, &serverErr) || serverErr.Code != "42P01" {
		t.Fatalf("Exec = %v, want the server's SQLSTATE 42P01", err)
	}
	// No stream was started, so Close has nothing to finish.
	if err := copyIn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
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

func TestCopyInUsesServerRowWidth(t *testing.T) {
	rowsCh := make(chan []string, 1)
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)

		// No column list: the width comes from the server's response.
		if query := s.expectSimpleQuery(); query != `COPY "logs" FROM STDIN` {
			s.fail("copy query = %q", query)
			return
		}
		s.sendCopyInResponse(0, 2)
		rowsCh <- s.readCopyRows()
		s.sendCommandComplete("COPY 1")
		s.sendReady(proto.TxnInTransaction)

		s.expectSimpleQuery()
		s.sendCommandComplete("ROLLBACK")
		s.sendReady(proto.TxnIdle)

		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	defer func() { _ = db.Close() }()
	ctx := testContext(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	copyIn, err := CopyIn(tx, "logs")
	if err != nil {
		t.Fatalf("CopyIn: %v", err)
	}
	if err := copyIn.Exec(1, "a"); err != nil {
		t.Fatalf("row: %v", err)
	}
	if err := copyIn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if rows := <-rowsCh; !reflect.DeepEqual(rows, []string{"1\ta\n"}) {
		t.Errorf("rows = %q", rows)
	}
}

// TestUnexpectedCopyResponseIsRefused covers a server that answers a normal
// statement with a COPY stream: the driver must refuse the stream and discard
// the session instead of pretending to read a result or leaking the stream into
// the next user of the pooled connection.
func TestUnexpectedCopyResponseIsRefused(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if got := s.expectSimpleQuery(); got != "SELECT 1" {
			s.fail("query = %q, want SELECT 1", got)
		}
		// A protocol surprise: the server starts COPY IN for a SELECT.
		s.sendCopyInResponse(0, 1)
	})

	db := openTestDB(t, testDSN(server, ""))
	if _, err := db.ExecContext(testContext(t), "SELECT 1"); !errors.Is(err, errCopyInRequired) {
		t.Fatalf("ExecContext error = %v, want errCopyInRequired", err)
	}
	if idle := db.Stats().Idle; idle != 0 {
		t.Errorf("idle connections = %d, want 0 because the session must be discarded", idle)
	}
}

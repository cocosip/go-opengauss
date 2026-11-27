package opengauss

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocosip/go-opengauss/internal/proto"
)

func TestCopyOutStreamsRowsAcrossFrames(t *testing.T) {
	const query = "COPY demotable TO STDOUT"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if got := s.expectSimpleQuery(); got != query {
			s.fail("query = %q, want %q", got, query)
			return
		}
		s.sendCopyOutResponse(0, 3)
		// The second row is split in the middle of a field and the third
		// field of the second row is NULL.
		s.sendCopyData("1\tplain\t\\N\n2\tta")
		// Asynchronous messages may arrive between CopyData frames.
		s.sendNotice("copy in progress")
		s.sendParameterStatus("application_name", "copy out test")
		s.sendCopyData("b\\tc\t\\N\n")
		s.sendCopyDone()
		s.sendCommandComplete("COPY 2")
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("Columns: %v", err)
	}
	if want := []string{"column1", "column2", "column3"}; !reflect.DeepEqual(columns, want) {
		t.Fatalf("columns = %v, want %v", columns, want)
	}
	var got [][]any
	for rows.Next() {
		var a, b, c any
		if err := rows.Scan(&a, &b, &c); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, []any{a, b, c})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	want := [][]any{{"1", "plain", nil}, {"2", "tab\tc", nil}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}
	// The session must be reusable after the stream reached ReadyForQuery.
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if server.connections() != 1 {
		t.Errorf("connections = %d, want the same session reused", server.connections())
	}
}

func TestCopyOutDecodesTextEscapes(t *testing.T) {
	const query = "COPY demotable TO STDOUT"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCopyOutResponse(0, 1)
		s.sendCopyData(`a\tb\nc\\d\101\x42\q` + "\n")
		s.sendCopyDone()
		s.sendCommandComplete("COPY 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("Next = false, err = %v", rows.Err())
	}
	var value string
	if err := rows.Scan(&value); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if want := "a\tb\nc\\dABq"; value != want {
		t.Fatalf("value = %q, want %q", value, want)
	}
	if rows.Next() {
		t.Fatal("an extra row was returned")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
}

func TestCopyOutEarlyCloseDiscardsRemainingRows(t *testing.T) {
	const query = "COPY demotable TO STDOUT"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCopyOutResponse(0, 2)
		s.sendCopyData("1\tfirst\n2\tsecond\n")
		s.sendCopyDone()
		s.sendCommandComplete("COPY 2")
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("Next = false, err = %v", rows.Err())
	}
	var id, name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if id != "1" || name != "first" {
		t.Fatalf("row = %q %q", id, name)
	}
	// The frontend cannot end a copy-out stream, so Close discards the rest.
	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if server.connections() != 1 {
		t.Errorf("connections = %d, want the stream drained on the same session", server.connections())
	}
}

func TestCopyOutRejectsBinaryFormat(t *testing.T) {
	const query = "COPY demotable TO STDOUT"
	var server *fakeServer
	server = newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCopyOutResponse(1, 2)
		// Copy-out cannot be ended from the frontend, so the driver cancels
		// the stream it cannot decode.
		select {
		case <-server.cancels:
		case <-time.After(5 * time.Second):
			s.fail("no cancel request arrived")
			return
		}
		s.sendError("57014", "canceling statement due to user request")
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	_, err := db.QueryContext(ctx, query)
	if err == nil || !strings.Contains(err.Error(), "binary format") {
		t.Fatalf("QueryContext = %v, want a binary-format refusal", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestCopyOutRejectsParametersAndExec(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	if _, err := db.QueryContext(ctx, "COPY demotable TO STDOUT", 1); err == nil ||
		!strings.Contains(err.Error(), "does not accept parameters") {
		t.Fatalf("QueryContext with parameters = %v, want a refusal", err)
	}
	if _, err := db.ExecContext(ctx, "COPY demotable TO STDOUT"); err == nil ||
		!strings.Contains(err.Error(), "must be read with QueryContext") {
		t.Fatalf("ExecContext = %v, want a refusal", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestCopyOutServerErrorBeforeStream(t *testing.T) {
	const query = "COPY demotable TO STDOUT"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendError("42501", "permission denied for table demotable")
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	_, err := db.QueryContext(ctx, query)
	var serverErr *Error
	if !errors.As(err, &serverErr) || serverErr.Code != "42501" {
		t.Fatalf("QueryContext = %v, want SQLSTATE 42501", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestCopyOutPrepareIsRejected(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	_, err := db.PrepareContext(ctx, "COPY demotable TO STDOUT")
	if err == nil || !strings.Contains(err.Error(), "must be read with QueryContext") {
		t.Fatalf("PrepareContext = %v, want a refusal", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestCopyOutCloseBeforeFirstRow(t *testing.T) {
	const query = "COPY demotable TO STDOUT"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCopyOutResponse(0, 1)
		s.sendCopyData("1\n2\n")
		s.sendCopyDone()
		s.sendCommandComplete("COPY 2")
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestCopyOutToleratesEmptyTrailingStatement(t *testing.T) {
	const query = "COPY demotable TO STDOUT;"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCopyOutResponse(0, 1)
		s.sendCopyData("1\n")
		s.sendCopyDone()
		s.sendCommandComplete("COPY 1")
		// A server that reports the empty trailing statement must not turn a
		// completed stream into an error.
		s.writeRaw(proto.NewMessageWriter(proto.MsgEmptyQueryResponse).Finish())
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("Next = false, err = %v", rows.Err())
	}
	if rows.Next() {
		t.Fatal("an extra row was returned")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestCopyOutMidStreamErrorReturnsServerError(t *testing.T) {
	const query = "COPY demotable TO STDOUT"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCopyOutResponse(0, 1)
		s.sendCopyData("1\n")
		s.sendError("22P02", "invalid input syntax")
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("Next = false, err = %v", rows.Err())
	}
	if rows.Next() {
		t.Fatal("an extra row was returned")
	}
	var serverErr *Error
	if err := rows.Err(); !errors.As(err, &serverErr) || serverErr.Code != "22P02" {
		t.Fatalf("Rows.Err = %v, want SQLSTATE 22P02", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestCopyOutTruncatedFinalRow(t *testing.T) {
	const query = "COPY demotable TO STDOUT"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCopyOutResponse(0, 1)
		s.sendCopyData("1\npartial")
		s.sendCopyDone()
		s.sendCommandComplete("COPY 1")
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("Next = false, err = %v", rows.Err())
	}
	if rows.Next() {
		t.Fatal("a truncated row was accepted")
	}
	if err := rows.Err(); err == nil || !strings.Contains(err.Error(), "middle of a row") {
		t.Fatalf("Rows.Err = %v, want a truncated-row error", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestCopyOutRowCapAllowsMultiRowFrames(t *testing.T) {
	restore := setMaxCopyOutRow(16)
	defer restore()

	const query = "COPY demotable TO STDOUT"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCopyOutResponse(0, 1)
		// Nine one-byte rows share one frame, so the frame is larger than the
		// row cap while every row is far below it.
		s.sendCopyData("1\n2\n3\n4\n5\n6\n7\n8\n9\n")
		s.sendCopyDone()
		s.sendCommandComplete("COPY 9")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
	if count != 9 {
		t.Fatalf("rows = %d, want 9", count)
	}
}

func TestCopyOutRowCapStopsStream(t *testing.T) {
	restore := setMaxCopyOutRow(16)
	defer restore()

	const query = "COPY demotable TO STDOUT"
	var server *fakeServer
	server = newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCopyOutResponse(0, 1)
		s.sendCopyData(strings.Repeat("x", 20))
		// The driver stops the stream it cannot decode.
		select {
		case <-server.cancels:
		case <-time.After(5 * time.Second):
			s.fail("no cancel request arrived")
			return
		}
		s.sendError("57014", "canceling statement due to user request")
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		t.Fatal("an oversized row was accepted")
	}
	if err := rows.Err(); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("Rows.Err = %v, want an oversized-row error", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if server.connections() != 1 {
		t.Errorf("connections = %d, want the confirmed cancel to keep the session", server.connections())
	}
}

func TestCopyOutUnconfirmedCancelDiscardsSession(t *testing.T) {
	const query = "COPY demotable TO STDOUT"
	var server *fakeServer
	var connections atomic.Int32
	server = newFakeServerWithCancelHold(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if connections.Add(1) == 1 {
			s.expectSimpleQuery()
			s.sendCopyOutResponse(1, 1)
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
	}, 1500*time.Millisecond)

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	_, err := db.QueryContext(ctx, query)
	if err == nil || !strings.Contains(err.Error(), "binary format") {
		t.Fatalf("QueryContext = %v, want a binary-format refusal", err)
	}
	// The cancel was not confirmed before the grace period, so the session
	// must not be handed to another statement.
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if connections.Load() != 2 {
		t.Errorf("connections = %d, want the unconfirmed cancel to discard the session", connections.Load())
	}
}

// setMaxCopyOutRow lowers the row cap for one test and returns the restore
// function. The package tests do not run in parallel, so a global is safe here.
func setMaxCopyOutRow(limit int) func() {
	previous := maxCopyOutRow
	maxCopyOutRow = limit
	return func() { maxCopyOutRow = previous }
}

func TestCopyOutWrongFieldCountStopsStream(t *testing.T) {
	const query = "COPY demotable TO STDOUT"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCopyOutResponse(0, 3)
		s.sendCopyData("1\t2\n")
		s.sendCopyDone()
		s.sendCommandComplete("COPY 0")
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		t.Fatal("a malformed row was accepted")
	}
	if err := rows.Err(); err == nil || !strings.Contains(err.Error(), "2 fields") {
		t.Fatalf("Rows.Err = %v, want a field-count error", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestCopyOutCancelledStreamKeepsSessionSynchronized(t *testing.T) {
	const query = "COPY demotable TO STDOUT"
	var server *fakeServer
	var connections atomic.Int32
	queryStarted := make(chan struct{})
	server = newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if connections.Add(1) == 1 {
			s.expectSimpleQuery()
			s.sendCopyOutResponse(0, 1)
			s.sendCopyData("1\n")
			close(queryStarted)
			select {
			case <-server.cancels:
			case <-time.After(5 * time.Second):
				s.fail("no cancel request arrived")
				return
			}
			s.sendError("57014", "canceling statement due to user request")
			s.sendReady(proto.TxnIdle)
			s.expectPing()
			s.expectType(proto.MsgTerminate)
			return
		}
		s.fail("the cancelled COPY OUT session was not reused")
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-queryStarted
		cancel()
	}()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
	}
	if err := rows.Err(); err == nil {
		t.Fatal("the cancelled stream reported no error")
	} else if !errors.Is(err, context.Canceled) {
		var serverErr *Error
		if !errors.As(err, &serverErr) || serverErr.Code != "57014" {
			t.Fatalf("Rows.Err = %v, want the cancellation", err)
		}
	}
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if connections.Load() != 1 {
		t.Errorf("connections = %d, want the resynchronized session reused", connections.Load())
	}
}

func TestCopyOutStreamsManyRows(t *testing.T) {
	const (
		query = "COPY demotable TO STDOUT"
		total = 500
	)
	var payload strings.Builder
	for i := 0; i < total; i++ {
		fmt.Fprintf(&payload, "%d\tvalue-%d\n", i, i)
	}
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCopyOutResponse(0, 2)
		// Chunks of seven bytes cut rows and fields at arbitrary points.
		chunk := payload.String()
		for len(chunk) > 0 {
			size := 7
			if size > len(chunk) {
				size = len(chunk)
			}
			s.sendCopyData(chunk[:size])
			chunk = chunk[size:]
		}
		s.sendCopyDone()
		s.sendCommandComplete(fmt.Sprintf("COPY %d", total))
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		var id, value string
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("Scan row %d: %v", count, err)
		}
		if wantID := fmt.Sprintf("%d", count); id != wantID {
			t.Fatalf("row %d id = %q, want %q", count, id, wantID)
		}
		if wantValue := fmt.Sprintf("value-%d", count); value != wantValue {
			t.Fatalf("row %d value = %q, want %q", count, value, wantValue)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
	if count != total {
		t.Fatalf("rows = %d, want %d", count, total)
	}
}

func TestDecodeCopyField(t *testing.T) {
	tests := []struct {
		field string
		want  string
	}{
		{"plain", "plain"},
		{`tab\there`, "tab\there"},
		{`newline\nhere`, "newline\nhere"},
		{`carriage\rhere`, "carriage\rhere"},
		{`backspace\bhere`, "backspace\bhere"},
		{`formfeed\fhere`, "formfeed\fhere"},
		{`vertical\vhere`, "vertical\vhere"},
		{`back\\slash`, `back\slash`},
		{`octal\101`, "octalA"},
		{`hex\x42`, "hexB"},
		{`unknown\qescape`, "unknownqescape"},
		{`trailing\`, `trailing\`},
		{`\\N`, `\N`},
	}
	for _, tc := range tests {
		if got := string(decodeCopyField([]byte(tc.field))); got != tc.want {
			t.Errorf("decodeCopyField(%q) = %q, want %q", tc.field, got, tc.want)
		}
	}
}

func TestParseCopyOutOptions(t *testing.T) {
	tests := []struct {
		statement string
		want      copyOutFormat
		err       string
	}{
		{statement: "COPY t TO STDOUT", want: textCopyOutFormat()},
		{statement: "COPY t TO STDOUT;", want: textCopyOutFormat()},
		{statement: "COPY t TO STDOUT -- note\n", want: textCopyOutFormat()},
		{statement: "COPY t TO/**/STDOUT /* note */", want: textCopyOutFormat()},
		{statement: "COPY t TO STDOUT WITH (FORMAT text)", want: textCopyOutFormat()},
		{statement: "COPY t TO STDOUT CSV", want: csvCopyOutFormat()},
		{statement: "COPY t TO STDOUT WITH CSV", want: csvCopyOutFormat()},
		{statement: "COPY t TO STDOUT WITH (FORMAT csv, HEADER)", want: csvCopyOutFormat(true)},
		{statement: "COPY t TO STDOUT CSV HEADER", want: csvCopyOutFormat(true)},
		{statement: "COPY t TO STDOUT WITH CSV HEADER", want: csvCopyOutFormat(true)},
		{statement: "COPY t TO STDOUT WITH (HEADER false, FORMAT csv)", want: csvCopyOutFormat()},
		{statement: "COPY t TO STDOUT WITH (FORMAT csv, DELIMITER '|', QUOTE '''', ESCAPE '\\', NULL 'NA')",
			want: modifiedCSVCopyOutFormat('|', '\'', '\\', "NA")},
		// ESCAPE defaults to the quote character the statement selected.
		{statement: "COPY t TO STDOUT WITH (FORMAT csv, QUOTE '~')",
			want: modifiedCSVCopyOutFormat(',', '~', '~', "")},
		{statement: "COPY t TO STDOUT WITH (FORMAT binary)", err: "binary format"},
		{statement: "COPY t TO STDOUT BINARY", err: "binary format"},
		{statement: "COPY t TO STDOUT WITH (FORMAT xsv)", err: "unknown COPY format"},
		{statement: "COPY t TO STDOUT WITH (ENCODING 'UTF8')", err: "ENCODING is not implemented"},
		{statement: "COPY t TO STDOUT WITH (HEADER)", err: "HEADER is available only with FORMAT csv"},
		{statement: "COPY t TO STDOUT WITH (DELIMITER '|')", err: "DELIMITER is available only with FORMAT csv"},
		{statement: "COPY t TO STDOUT WITH (FORMAT csv, DELIMITER '||')", err: "one single-byte character"},
		{statement: "COPY t TO STDOUT WITH (FORMAT csv, HEADER maybe)", err: "is not a boolean"},
		{statement: "COPY t TO STDOUT WITH (FORMAT csv, HEADER); SELECT 1", err: "only statement"},
		{statement: "COPY t TO STDOUT WITH (FORMAT csv", err: "not closed"},
	}
	for _, test := range tests {
		kind, clauseEnd := copyDirection(test.statement)
		if kind != copyOut {
			t.Fatalf("copyDirection(%q) = %v, want copyOut", test.statement, kind)
		}
		format, err := parseCopyOutOptions(test.statement, clauseEnd)
		if test.err != "" {
			if err == nil || !strings.Contains(err.Error(), test.err) {
				t.Errorf("parseCopyOutOptions(%q) error = %v, want %q", test.statement, err, test.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseCopyOutOptions(%q) = %v", test.statement, err)
			continue
		}
		if !reflect.DeepEqual(format, test.want) {
			t.Errorf("parseCopyOutOptions(%q) = %#v, want %#v", test.statement, format, test.want)
		}
	}
}

// csvCopyOutFormat is the expected format of a CSV statement, optionally with
// its header line skipped.
func csvCopyOutFormat(header ...bool) copyOutFormat {
	format := textCopyOutFormat()
	format.useCSVDefaults()
	if len(header) > 0 {
		format.header = header[0]
	}
	return format
}

// modifiedCSVCopyOutFormat is a CSV format whose separators the statement set.
func modifiedCSVCopyOutFormat(delimiter, quote, escape byte, null string) copyOutFormat {
	format := csvCopyOutFormat()
	format.delimiter = delimiter
	format.quote = quote
	format.escape = escape
	if null != "" {
		format.null = []byte(null)
	}
	return format
}

// TestCopyOutCSVStreamsRows proves the CSV decoder handles a quoted separator,
// a newline inside a quoted field, a doubled quote, NULL and an empty string,
// with the header line skipped and the payload split across frames.
func TestCopyOutCSVStreamsRows(t *testing.T) {
	const query = "COPY demotable TO STDOUT WITH (FORMAT csv, HEADER)"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if got := s.expectSimpleQuery(); got != query {
			s.fail("query = %q, want %q", got, query)
			return
		}
		s.sendCopyOutResponse(0, 4)
		// The header row and the quoted field of the first row are split
		// across frames, and the newline inside the quotes does not end it.
		s.sendCopyData("id,name,note,extra\n1,\"a,b\",\"line1\n")
		s.sendCopyData("line2\",\"say \"\"hi\"\"\"\n2,,\"\",x\n")
		s.sendCopyDone()
		s.sendCommandComplete("COPY 2")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got [][]any
	for rows.Next() {
		var a, b, c, d any
		if err := rows.Scan(&a, &b, &c, &d); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, []any{a, b, c, d})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
	want := [][]any{
		{"1", "a,b", "line1\nline2", `say "hi"`},
		{"2", nil, "", "x"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}
}

// TestCopyOutCSVLegacySyntax reads a header line through the legacy CSV HEADER
// form of the statement.
func TestCopyOutCSVLegacySyntax(t *testing.T) {
	const query = "COPY demotable TO STDOUT CSV HEADER"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if got := s.expectSimpleQuery(); got != query {
			s.fail("query = %q, want %q", got, query)
			return
		}
		s.sendCopyOutResponse(0, 2)
		s.sendCopyData("id,value\n1,\"2\"\n")
		s.sendCopyDone()
		s.sendCommandComplete("COPY 1")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("Next = false, err = %v", rows.Err())
	}
	var id, value string
	if err := rows.Scan(&id, &value); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if id != "1" || value != "2" {
		t.Fatalf("row = %q %q, want 1 2", id, value)
	}
	if rows.Next() {
		t.Fatal("an extra row was returned")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
}

// TestCopyOutCSVCustomSeparators decodes a CSV stream whose separator, quote and
// NULL string the statement chose.
func TestCopyOutCSVCustomSeparators(t *testing.T) {
	const query = "COPY demotable TO STDOUT WITH (FORMAT csv, HEADER true, DELIMITER '|', NULL 'NA', QUOTE '~')"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if got := s.expectSimpleQuery(); got != query {
			s.fail("query = %q, want %q", got, query)
			return
		}
		s.sendCopyOutResponse(0, 2)
		s.sendCopyData("id|value\n1|NA\n2|~a|b~\n")
		s.sendCopyDone()
		s.sendCommandComplete("COPY 2")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got [][]any
	for rows.Next() {
		var id, value any
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, []any{id, value})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
	want := [][]any{{"1", nil}, {"2", "a|b"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}
}

// TestCopyOutCSVUnterminatedRowStopsStream proves a stream that ends inside a
// quoted field is reported instead of returning a half row.
func TestCopyOutCSVUnterminatedRowStopsStream(t *testing.T) {
	const query = "COPY demotable TO STDOUT WITH (FORMAT csv)"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCopyOutResponse(0, 2)
		s.sendCopyData("1,\"open\n")
		s.sendCopyDone()
		s.sendCommandComplete("COPY 0")
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		t.Fatal("a half row was returned")
	}
	if err := rows.Err(); err == nil || !strings.Contains(err.Error(), "middle of a row") {
		t.Fatalf("Rows.Err = %v, want the truncated-row error", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

// TestCopyOutCSVMalformedRowStopsStream proves a row the CSV rules cannot read
// stops the stream and leaves the session reusable.
func TestCopyOutCSVMalformedRowStopsStream(t *testing.T) {
	const query = "COPY demotable TO STDOUT WITH (FORMAT csv)"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCopyOutResponse(0, 2)
		s.sendCopyData("\"a\"x,b\n")
		s.sendCopyDone()
		s.sendCommandComplete("COPY 0")
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		t.Fatal("a malformed row was accepted")
	}
	if err := rows.Err(); err == nil || !strings.Contains(err.Error(), "CSV row") {
		t.Fatalf("Rows.Err = %v, want a CSV decoding error", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

// TestCopyOutCSVRowsAcrossManyFrames cuts CSV rows, quoted newlines and doubled
// quotes at arbitrary frame boundaries.
func TestCopyOutCSVRowsAcrossManyFrames(t *testing.T) {
	const (
		query = "COPY demotable TO STDOUT WITH (FORMAT csv)"
		total = 200
	)
	var payload strings.Builder
	for i := 0; i < total; i++ {
		fmt.Fprintf(&payload, "%d,\"value, %d\"\n", i, i)
	}
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectSimpleQuery()
		s.sendCopyOutResponse(0, 2)
		chunk := payload.String()
		for len(chunk) > 0 {
			size := 5
			if size > len(chunk) {
				size = len(chunk)
			}
			s.sendCopyData(chunk[:size])
			chunk = chunk[size:]
		}
		s.sendCopyDone()
		s.sendCommandComplete(fmt.Sprintf("COPY %d", total))
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		var id, value string
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("Scan row %d: %v", count, err)
		}
		if want := fmt.Sprintf("%d", count); id != want {
			t.Fatalf("row %d id = %q, want %q", count, id, want)
		}
		if want := fmt.Sprintf("value, %d", count); value != want {
			t.Fatalf("row %d value = %q, want %q", count, value, want)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
	if count != total {
		t.Fatalf("rows = %d, want %d", count, total)
	}
}

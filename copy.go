package opengauss

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

	"github.com/cocosip/go-opengauss/internal/proto"
)

// CopyIn starts a text COPY ... FROM STDIN stream into table. It must be called
// on a transaction, because the whole stream and its result share one session.
// Call Exec once per row and then Close, which finishes the stream and reads
// the server's result. A row is written in the text COPY format: values are
// converted like other parameters, NULL becomes \N, and tabs, newlines,
// carriage returns and backslashes are escaped.
func CopyIn(tx *sql.Tx, table string, columns ...string) (*CopyInStatement, error) {
	if tx == nil {
		return nil, errors.New("opengauss: CopyIn needs a transaction")
	}
	stmt, err := tx.Prepare(copyInSQL(table, columns))
	if err != nil {
		return nil, err
	}
	return &CopyInStatement{stmt: stmt}, nil
}

// CopyInStatement streams rows into one COPY statement. It is not safe for
// concurrent use: the rows of one stream must be written from one goroutine.
type CopyInStatement struct {
	stmt *sql.Stmt
}

// Exec appends one row. A call without values finishes the stream and returns
// the server's error, if any.
func (s *CopyInStatement) Exec(values ...any) error {
	return s.ExecContext(context.Background(), values...)
}

// ExecContext appends one row under ctx. The context of the call that opens the
// stream stays in force until the stream finishes, so a deadline or a
// cancellation there interrupts the whole COPY.
func (s *CopyInStatement) ExecContext(ctx context.Context, values ...any) error {
	_, err := s.stmt.ExecContext(ctx, values...)
	return err
}

// Close finishes the stream if it is still open and releases the statement.
func (s *CopyInStatement) Close() error {
	return s.stmt.Close()
}

// copyInSQL builds the COPY statement with quoted identifiers. A table name
// with dots is treated as schema-qualified.
func copyInSQL(table string, columns []string) string {
	var b strings.Builder
	b.WriteString("COPY ")
	for i, part := range strings.Split(table, ".") {
		if i > 0 {
			b.WriteByte('.')
		}
		b.WriteString(quoteIdentifier(part))
	}
	if len(columns) > 0 {
		b.WriteString(" (")
		for i, column := range columns {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(quoteIdentifier(column))
		}
		b.WriteByte(')')
	}
	b.WriteString(" FROM STDIN")
	return b.String()
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// isCopyFromStdin reports whether statement streams data, which the driver
// handles with the simple query protocol instead of a server-side prepare.
func isCopyFromStdin(statement string) bool {
	trimmed := strings.TrimSpace(statement)
	if len(trimmed) < len("copy") || !strings.EqualFold(trimmed[:len("copy")], "copy") {
		return false
	}
	return strings.Contains(strings.ToUpper(trimmed), " FROM STDIN")
}

// copyStmt is a COPY ... FROM STDIN statement. The server is not asked to
// prepare it: each Exec call sends one CopyData row and the final one sends
// CopyDone and reads the command result.
type copyStmt struct {
	conn      *conn
	statement string
	closed    bool
}

// NumInput reports that the row width comes from the server, not from a
// parameter list.
func (s *copyStmt) NumInput() int { return -1 }

// Exec implements driver.Stmt.
func (s *copyStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), namedValues(args))
}

// Query rejects a row-returning call: COPY ... FROM STDIN produces no rows.
func (s *copyStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("opengauss: COPY ... FROM STDIN returns no rows")
}

// ExecContext starts the stream on the first call, appends a row when values are
// given, and finishes the stream when called without values.
func (s *copyStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.closed {
		return nil, errors.New("opengauss: statement is already closed")
	}
	if s.conn.bad {
		return nil, driver.ErrBadConn
	}
	if !s.conn.copying {
		if err := s.conn.beginCopyIn(ctx, s.statement); err != nil {
			return nil, err
		}
	}
	if len(args) > 0 {
		if err := s.conn.writeCopyRow(args); err != nil {
			return nil, err
		}
		return result{}, nil
	}
	tag, err := s.conn.finishCopyIn()
	if err != nil {
		return nil, err
	}
	return result{tag: tag}, nil
}

// Close finishes an open stream so the session stays synchronized.
func (s *copyStmt) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.conn.bad || !s.conn.copying {
		return nil
	}
	_, err := s.conn.finishCopyIn()
	return err
}

// beginCopyIn sends the COPY statement and waits for CopyInResponse.
func (c *conn) beginCopyIn(ctx context.Context, statement string) error {
	if c.copying {
		return errors.New("opengauss: a COPY stream is already open")
	}
	watcher, err := c.beginOperation(ctx)
	if err != nil {
		return err
	}
	query := proto.NewMessageWriter(proto.MsgQuery)
	query.String(statement)
	if err := c.sendFrames(query.Finish()); err != nil {
		c.endOperation(watcher)
		return c.wrapIOError(ctx, "copy", err)
	}

	for {
		typ, body, err := c.recv()
		if err != nil {
			c.endOperation(watcher)
			return c.wrapIOError(ctx, "copy", err)
		}
		switch typ {
		case proto.MsgCopyInResponse:
			format, columns, err := parseCopyInResponse(body)
			if err != nil {
				c.bad = true
				c.endOperation(watcher)
				return err
			}
			if format != 0 {
				c.bad = true
				c.endOperation(watcher)
				return fmt.Errorf("opengauss: COPY uses binary format, which this driver does not request")
			}
			c.copying = true
			c.copyColumns = columns
			c.copyWatcher = watcher
			c.copyCtx = ctx
			return nil
		case proto.MsgErrorResponse:
			serverErr := parseErrorResponse(body)
			drainErr := c.drainUntilReady()
			c.endOperation(watcher)
			if drainErr != nil {
				return c.wrapIOError(ctx, "copy", drainErr)
			}
			return serverErr
		case proto.MsgNoticeResponse, proto.MsgNotificationResponse, proto.MsgParameterStatus:
			// Asynchronous messages are not part of the COPY exchange.
		default:
			c.bad = true
			c.endOperation(watcher)
			return fmt.Errorf("opengauss: unexpected message type %q while starting COPY", typ)
		}
	}
}

// writeCopyRow sends one text-format row.
func (c *conn) writeCopyRow(args []driver.NamedValue) error {
	if !c.copying {
		return errors.New("opengauss: the COPY stream is not open")
	}
	if len(args) != c.copyColumns {
		return fmt.Errorf("opengauss: COPY writes %d columns but the row has %d", c.copyColumns, len(args))
	}
	line, err := encodeCopyRow(nil, args)
	if err != nil {
		return err
	}
	writer := proto.NewMessageWriter(proto.MsgCopyData)
	writer.Bytes(line)
	if err := c.sendFrames(writer.Finish()); err != nil {
		// The stream can no longer be completed on this session.
		c.copying = false
		c.copyWatcher = nil
		c.bad = true
		return c.wrapIOError(c.copyCtx, "copy", err)
	}
	return nil
}

// finishCopyIn sends CopyDone and reads the command tag. A server error sent
// during the stream is returned after the session is synchronized again.
func (c *conn) finishCopyIn() (string, error) {
	if !c.copying {
		return "", errors.New("opengauss: the COPY stream is not open")
	}
	watcher := c.copyWatcher
	ctx := c.copyCtx
	c.copying = false
	c.copyWatcher = nil

	if err := c.sendFrames(proto.NewMessageWriter(proto.MsgCopyDone).Finish()); err != nil {
		c.bad = true
		c.endOperation(watcher)
		return "", c.wrapIOError(ctx, "copy", err)
	}

	var (
		tag       string
		serverErr error
	)
	for {
		typ, body, err := c.recv()
		if err != nil {
			c.bad = true
			c.endOperation(watcher)
			return "", c.wrapIOError(ctx, "copy", err)
		}
		switch typ {
		case proto.MsgCommandComplete:
			tag = commandTag(body)
		case proto.MsgReadyForQuery:
			c.setTxnStatus(body)
			c.endOperation(watcher)
			if serverErr != nil {
				return "", serverErr
			}
			return tag, nil
		case proto.MsgErrorResponse:
			serverErr = parseErrorResponse(body)
		case proto.MsgNoticeResponse, proto.MsgNotificationResponse, proto.MsgParameterStatus:
			// Asynchronous messages are not part of the COPY result.
		default:
			c.bad = true
			c.endOperation(watcher)
			return "", fmt.Errorf("opengauss: unexpected message type %q while finishing COPY", typ)
		}
	}
}

// parseCopyInResponse reads the format and column count of CopyInResponse.
func parseCopyInResponse(body []byte) (format byte, columns int, err error) {
	r := &proto.ReadBuf{B: body}
	format = r.Byte()
	count := int(r.Int16())
	if r.Err != nil || count < 0 {
		return 0, 0, proto.ErrMalformedMessage
	}
	for i := 0; i < count; i++ {
		r.Int16() // per-column format
	}
	if r.Err != nil {
		return 0, 0, proto.ErrMalformedMessage
	}
	return format, count, nil
}

// encodeCopyRow renders one row: values separated by tabs, terminated by a
// newline, with \N for NULL and the COPY text escapes for control characters.
func encodeCopyRow(dest []byte, args []driver.NamedValue) ([]byte, error) {
	for i, arg := range args {
		if i > 0 {
			dest = append(dest, '\t')
		}
		if arg.Value == nil {
			dest = append(dest, '\\', 'N')
			continue
		}
		text, err := encodeTextValue(arg.Value)
		if err != nil {
			return nil, fmt.Errorf("opengauss: COPY column %d: %w", i+1, err)
		}
		dest = appendCopyEscaped(dest, text)
	}
	return append(dest, '\n'), nil
}

// appendCopyEscaped writes text with the escapes COPY expects in text format.
func appendCopyEscaped(dest, text []byte) []byte {
	for _, b := range text {
		switch b {
		case '\\':
			dest = append(dest, '\\', '\\')
		case '\t':
			dest = append(dest, '\\', 't')
		case '\n':
			dest = append(dest, '\\', 'n')
		case '\r':
			dest = append(dest, '\\', 'r')
		case '\v':
			dest = append(dest, '\\', 'v')
		case '\b':
			dest = append(dest, '\\', 'b')
		case '\f':
			dest = append(dest, '\\', 'f')
		default:
			dest = append(dest, b)
		}
	}
	return dest
}

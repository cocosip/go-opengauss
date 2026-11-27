package opengauss

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/cocosip/go-opengauss/internal/proto"
)

// CopyIn starts a text COPY ... FROM STDIN stream into table. It must be called
// on a transaction, because the whole stream and its result share one session.
// Call Exec once per row and then Close, which finishes the stream and reads
// the server's result. A row is written in the default text COPY format: values
// are converted like other parameters, NULL becomes \N, and tabs, newlines,
// carriage returns and backslashes are escaped. Use CopyInWithOptions for CSV.
func CopyIn(tx *sql.Tx, table string, columns ...string) (*CopyInStatement, error) {
	return CopyInWithOptions(tx, table, CopyInOptions{}, columns...)
}

// CopyInWithOptions is CopyIn for a stream whose representation the caller
// chooses. With CopyFormatCSV, rows are written as CSV: a nil value becomes the
// unquoted NULL string, an empty string is quoted so it cannot read as NULL,
// and a value that holds the delimiter, the quote, a line break or the NULL
// string is quoted with the statement's quote and escape characters. Header
// only asks the server to expect a header line; the caller writes that line as
// its first row, because the COPY protocol carries rows only.
func CopyInWithOptions(tx *sql.Tx, table string, options CopyInOptions, columns ...string) (*CopyInStatement, error) {
	if tx == nil {
		return nil, errors.New("opengauss: CopyIn needs a transaction")
	}
	normalized, err := checkCopyInOptions(options)
	if err != nil {
		return nil, err
	}
	stmt, err := tx.Prepare(copyInSQL(table, columns, normalized))
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

// copyInSQL builds the COPY statement with quoted identifiers and the options
// the encoder writes. A table name with dots is treated as schema-qualified.
func copyInSQL(table string, columns []string, options CopyInOptions) string {
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
	if options.Format != CopyFormatCSV {
		return b.String()
	}
	// Only options that differ from the CSV defaults are sent, so the server
	// parses the same representation the encoder writes.
	b.WriteString(" WITH (FORMAT csv")
	if options.Delimiter != ',' {
		b.WriteString(", DELIMITER " + quoteCopyOptionLiteral(string(options.Delimiter)))
	}
	if options.Quote != '"' {
		b.WriteString(", QUOTE " + quoteCopyOptionLiteral(string(options.Quote)))
	}
	if options.Escape != options.Quote {
		b.WriteString(", ESCAPE " + quoteCopyOptionLiteral(string(options.Escape)))
	}
	if options.Header {
		b.WriteString(", HEADER true")
	}
	if options.Null != "" {
		b.WriteString(", NULL " + quoteCopyOptionLiteral(options.Null))
	}
	b.WriteByte(')')
	return b.String()
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// copyKind is the data direction of a COPY statement.
type copyKind int

const (
	copyNone copyKind = iota
	copyIn
	copyOut
)

// copyDirection reports the direction of a COPY statement and the offset just
// after the direction clause. The clause must be outside parentheses, so a
// subquery cannot select the wrong path, and keywords inside string literals
// (including dollar-quoted ones), quoted identifiers and comments are ignored.
func copyDirection(statement string) (copyKind, int) {
	i := skipLeadingComments(statement)
	const keyword = "COPY"
	if len(statement)-i < len(keyword) || !strings.EqualFold(statement[i:i+len(keyword)], keyword) {
		return copyNone, 0
	}
	i += len(keyword)
	if i < len(statement) && isIdentifierByte(statement[i]) {
		return copyNone, 0
	}
	depth := 0
	for i < len(statement) {
		switch c := statement[i]; {
		case c == '\'':
			i = skipQuoted(statement, i, '\'')
		case c == '"':
			i = skipQuoted(statement, i, '"')
		case c == '$':
			if end := skipDollarQuoted(statement, i); end > i {
				i = end
				continue
			}
			i++
		case c == '-' && i+1 < len(statement) && statement[i+1] == '-':
			for i < len(statement) && statement[i] != '\n' && statement[i] != '\r' {
				i++
			}
		case c == '/' && i+1 < len(statement) && statement[i+1] == '*':
			i = skipBlockComment(statement, i+2)
		case c == '(':
			depth++
			i++
		case c == ')':
			if depth > 0 {
				depth--
			}
			i++
		default:
			if depth == 0 {
				if end, ok := matchKeyword(statement, i, "FROM"); ok {
					if end, ok = matchKeyword(statement, skipSpaceAndComments(statement, end), "STDIN"); ok {
						return copyIn, end
					}
				}
				if end, ok := matchKeyword(statement, i, "TO"); ok {
					if end, ok = matchKeyword(statement, skipSpaceAndComments(statement, end), "STDOUT"); ok {
						return copyOut, end
					}
				}
			}
			i++
		}
	}
	return copyNone, 0
}

// matchKeyword matches one word case-insensitively at i, requiring word
// boundaries so that a longer identifier cannot match.
func matchKeyword(statement string, i int, word string) (int, bool) {
	if i < 0 || i+len(word) > len(statement) {
		return i, false
	}
	if !strings.EqualFold(statement[i:i+len(word)], word) {
		return i, false
	}
	if i > 0 && isIdentifierByte(statement[i-1]) {
		return i, false
	}
	if i+len(word) < len(statement) && isIdentifierByte(statement[i+len(word)]) {
		return i, false
	}
	return i + len(word), true
}

// skipDollarQuoted returns the offset after a dollar-quoted string that starts
// at i, or i when the text is not a dollar-quote opener.
func skipDollarQuoted(statement string, i int) int {
	j := i + 1
	if j < len(statement) && isTagStartByte(statement[j]) {
		for j < len(statement) && isIdentifierByte(statement[j]) {
			j++
		}
	}
	if j >= len(statement) || statement[j] != '$' {
		return i
	}
	tag := statement[i : j+1]
	end := strings.Index(statement[j+1:], tag)
	if end < 0 {
		return len(statement)
	}
	return j + 1 + end + len(tag)
}

func isTagStartByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c >= 0x80
}

// skipSpaceAndComments skips whitespace, line comments and block comments.
func skipSpaceAndComments(statement string, i int) int {
	for i < len(statement) {
		switch {
		case isSpace(statement[i]):
			i++
		case strings.HasPrefix(statement[i:], "--"):
			newline := strings.IndexAny(statement[i:], "\n\r")
			if newline < 0 {
				return len(statement)
			}
			i += newline + 1
		case strings.HasPrefix(statement[i:], "/*"):
			end := strings.Index(statement[i+2:], "*/")
			if end < 0 {
				return len(statement)
			}
			i += 2 + end + 2
		default:
			return i
		}
	}
	return i
}

// isIdentifierByte reports whether c can continue an unquoted SQL identifier.
func isIdentifierByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '$', c >= 0x80:
		return true
	}
	return false
}

var errCopyInRequired = errors.New("opengauss: COPY ... FROM STDIN must be started through CopyIn(tx, ...)")

var errCopyOutRequired = errors.New("opengauss: COPY ... TO STDOUT returns rows and must be read with QueryContext")

var errCopyOutParameters = errors.New("opengauss: COPY ... TO STDOUT does not accept parameters; write the statement without placeholders")

// copyStreamError names the COPY direction a server answered with, so an
// application that sent the statement through the wrong entry point sees which
// part is unsupported.
func copyStreamError(typ byte) error {
	switch typ {
	case proto.MsgCopyInResponse:
		return errCopyInRequired
	case proto.MsgCopyOutResponse:
		return errCopyOutRequired
	default:
		return errors.New("opengauss: the COPY BOTH protocol is not supported")
	}
}

// copyStmt is a COPY ... FROM STDIN statement. The server is not asked to
// prepare it: each Exec call sends one CopyData row and the final one sends
// CopyDone and reads the command result.
type copyStmt struct {
	conn      *conn
	statement string
	options   CopyInOptions
	closed    bool
	finished  bool
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
		return nil, s.conn.badConnError()
	}
	if s.finished {
		return nil, errors.New("opengauss: the COPY ... FROM STDIN stream is already finished")
	}
	if s.conn.copying {
		if s.conn.copyOwner != s {
			return nil, errors.New("opengauss: another COPY stream is open on this session")
		}
	} else if err := s.conn.beginCopyIn(ctx, s); err != nil {
		return nil, err
	}
	if len(args) > 0 {
		if err := s.conn.writeCopyRow(ctx, args); err != nil {
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

// Close finishes an open stream so the session stays synchronized. A close
// after a failed row does nothing: that error already ended the stream.
func (s *copyStmt) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.conn.bad || !s.conn.copying || s.conn.copyOwner != s {
		return nil
	}
	_, err := s.conn.finishCopyIn()
	return err
}

// beginCopyIn sends the COPY statement and waits for CopyInResponse.
func (c *conn) beginCopyIn(ctx context.Context, owner *copyStmt) error {
	if c.copying {
		return errors.New("opengauss: a COPY stream is already open")
	}
	watcher, err := c.beginOperation(ctx)
	if err != nil {
		return err
	}
	query := proto.NewMessageWriter(proto.MsgQuery)
	query.String(owner.statement)
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
			format, columns, err := parseCopyResponse(body)
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
			c.copyOwner = owner
			c.copyColumns = columns
			c.copyOptions = owner.options
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
		case proto.MsgParameterStatus:
			c.storeParameter(body)
			if c.encodingErr != nil {
				c.endOperation(watcher)
				return c.encodingErr
			}
		case proto.MsgNoticeResponse, proto.MsgNotificationResponse:
			// Asynchronous messages are not part of the COPY exchange.
		default:
			c.bad = true
			c.endOperation(watcher)
			return fmt.Errorf("opengauss: unexpected message type %q while starting COPY", typ)
		}
	}
}

// writeCopyRow sends one row in the representation the statement selected. A
// row that cannot be encoded, has the wrong width or belongs to an ended
// context ends the stream with CopyFail, so the server aborts the COPY instead
// of importing the rows already sent.
func (c *conn) writeCopyRow(ctx context.Context, args []driver.NamedValue) error {
	if !c.copying {
		return errors.New("opengauss: the COPY stream is not open")
	}
	if err := ctx.Err(); err != nil {
		c.abortCopyIn(err)
		return err
	}
	if len(args) != c.copyColumns {
		err := fmt.Errorf("opengauss: COPY writes %d columns but the row has %d", c.copyColumns, len(args))
		c.abortCopyIn(err)
		return err
	}
	line, err := encodeCopyRow(c.copyRowBuf[:0], args, c.copyOptions)
	if err != nil {
		c.copyRowBuf = nil
		c.abortCopyIn(err)
		return err
	}
	if len(line) > math.MaxInt32 {
		c.copyRowBuf = nil
		err := fmt.Errorf("opengauss: a COPY row of %d bytes exceeds the protocol limit", len(line))
		c.abortCopyIn(err)
		return err
	}
	c.copyRowBuf = line[:0]
	writer := proto.NewMessageWriter(proto.MsgCopyData)
	writer.Bytes(line)
	if err := c.sendFrames(writer.Finish()); err != nil {
		// The stream can no longer be completed on this session: stop the
		// watcher that belongs to the open stream before discarding the
		// connection.
		watcher := c.copyWatcher
		streamCtx := c.copyCtx
		if c.copyOwner != nil {
			c.copyOwner.finished = true
		}
		c.copying = false
		c.copyOwner = nil
		c.copyWatcher = nil
		c.copyCtx = nil
		c.endOperation(watcher)
		c.bad = true
		return c.wrapIOError(streamCtx, "copy", err)
	}
	return nil
}

// abortCopyIn asks the server to discard the stream and resynchronizes the
// session, so the transaction may still roll back and the connection may be
// reused. The original error is the caller's to report.
func (c *conn) abortCopyIn(reason error) {
	if !c.copying {
		return
	}
	watcher := c.copyWatcher
	c.copying = false
	if c.copyOwner != nil {
		c.copyOwner.finished = true
		c.copyOwner = nil
	}
	c.copyWatcher = nil
	c.copyCtx = nil

	fail := proto.NewMessageWriter(proto.MsgCopyFail)
	fail.String(reason.Error())
	if err := c.sendFrames(fail.Finish()); err != nil {
		c.bad = true
		c.endOperation(watcher)
		return
	}
	// The server answers CopyFail with an error and then ReadyForQuery. Both
	// are discarded: the caller already has the reason the stream ended.
	if err := c.drainUntilReady(); err != nil {
		c.bad = true
	}
	c.endOperation(watcher)
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
	c.copyCtx = nil
	if c.copyOwner != nil {
		c.copyOwner.finished = true
		c.copyOwner = nil
	}

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
			// The first tag belongs to the COPY itself; a trailing statement
			// must not replace the reported row count.
			if tag == "" {
				tag = commandTag(body)
			}
		case proto.MsgReadyForQuery:
			c.setTxnStatus(body)
			c.endOperation(watcher)
			if serverErr != nil {
				return "", serverErr
			}
			if c.encodingErr != nil {
				return "", c.encodingErr
			}
			return tag, nil
		case proto.MsgErrorResponse:
			serverErr = parseErrorResponse(body)
		case proto.MsgParameterStatus:
			c.storeParameter(body)
		case proto.MsgNoticeResponse, proto.MsgNotificationResponse,
			proto.MsgEmptyQueryResponse, proto.MsgRowDescription, proto.MsgDataRow, proto.MsgNoData,
			proto.MsgParseComplete, proto.MsgBindComplete, proto.MsgParameterDescription, proto.MsgCloseComplete:
			// Asynchronous messages and the payload of a trailing statement are
			// not part of the COPY result.
		default:
			c.bad = true
			c.endOperation(watcher)
			return "", fmt.Errorf("opengauss: unexpected message type %q while finishing COPY", typ)
		}
	}
}

// parseCopyResponse reads the format and column count of a CopyInResponse or
// CopyOutResponse. The driver only uses the default text format; an overall
// binary response is reported to the caller, while a binary column inside a
// text response is a protocol violation because the driver never asks for one.
func parseCopyResponse(body []byte) (format byte, columns int, err error) {
	r := &proto.ReadBuf{B: body}
	format = r.Byte()
	count := int(r.Int16())
	if r.Err != nil || count < 0 {
		return 0, 0, proto.ErrMalformedMessage
	}
	for i := 0; i < count; i++ {
		columnFormat := r.Int16()
		if r.Err != nil {
			return 0, 0, proto.ErrMalformedMessage
		}
		if columnFormat != 0 && format == 0 {
			return 0, 0, fmt.Errorf("opengauss: COPY column %d uses binary format, which this driver does not request", i+1)
		}
	}
	if r.Err != nil {
		return 0, 0, proto.ErrMalformedMessage
	}
	return format, count, nil
}

// encodeCopyRow renders one row in the representation the statement selected.
func encodeCopyRow(dest []byte, args []driver.NamedValue, options CopyInOptions) ([]byte, error) {
	if options.Format == CopyFormatCSV {
		return encodeCopyCSVRow(dest, args, options)
	}
	return encodeCopyTextRow(dest, args)
}

// encodeCopyTextRow renders one row of the default text format: values
// separated by tabs, terminated by a newline, with \N for NULL and the COPY text
// escapes for control characters.
func encodeCopyTextRow(dest []byte, args []driver.NamedValue) ([]byte, error) {
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

// encodeCopyCSVRow renders one CSV row. NULL is the unquoted NULL string; a
// value is quoted when it would otherwise read as NULL or lose a separator,
// quote or line break.
func encodeCopyCSVRow(dest []byte, args []driver.NamedValue, options CopyInOptions) ([]byte, error) {
	for i, arg := range args {
		if i > 0 {
			dest = append(dest, options.Delimiter)
		}
		if arg.Value == nil {
			dest = append(dest, options.Null...)
			continue
		}
		text, err := encodeTextValue(arg.Value)
		if err != nil {
			return nil, fmt.Errorf("opengauss: COPY column %d: %w", i+1, err)
		}
		dest = appendCopyCSVField(dest, text, options)
	}
	return append(dest, '\n'), nil
}

// appendCopyCSVField writes one CSV field, quoting it when the text is empty,
// equals the NULL string or carries a separator, the quote or a line break.
// Inside quotes the quote and the escape character are preceded by the escape
// character, which is the doubled quote by default.
func appendCopyCSVField(dest, text []byte, options CopyInOptions) []byte {
	quote := len(text) == 0 ||
		bytes.Equal(text, []byte(options.Null)) ||
		bytes.IndexByte(text, options.Delimiter) >= 0 ||
		bytes.IndexByte(text, options.Quote) >= 0 ||
		bytes.IndexByte(text, '\n') >= 0 ||
		bytes.IndexByte(text, '\r') >= 0
	if !quote {
		return append(dest, text...)
	}
	dest = append(dest, options.Quote)
	for _, b := range text {
		if b == options.Quote || b == options.Escape {
			dest = append(dest, options.Escape, b)
			continue
		}
		dest = append(dest, b)
	}
	return append(dest, options.Quote)
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

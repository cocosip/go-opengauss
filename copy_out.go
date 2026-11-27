package opengauss

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"

	"github.com/cocosip/go-opengauss/internal/proto"
)

// maxCopyOutRow bounds one decoded COPY ... TO STDOUT row, so a broken or
// hostile stream cannot exhaust memory before the server is stopped. It is a
// variable only so tests can lower it.
var maxCopyOutRow = 64 << 20

// copyOutRows streams the rows of a COPY ... TO STDOUT statement read through
// QueryContext. The COPY protocol carries no column names or type OIDs, so the
// columns are reported as column1..columnN and every value is a string (NULL is
// nil) in the text and CSV formats. A binary stream is returned field by field
// as []byte, because nothing in the protocol says how to decode a value without
// its type. The session stays pinned to the statement until the rows reach
// ReadyForQuery or are closed.
type copyOutRows struct {
	conn         *conn
	columns      int
	format       copyOutFormat
	headerDone   bool
	binaryHeader bool
	binaryDone   bool
	ctx          context.Context
	watcher      *cancelWatcher
	pending      []byte
	done         bool
}

func (r *copyOutRows) Columns() []string {
	names := make([]string, r.columns)
	for i := range names {
		names[i] = "column" + strconv.Itoa(i+1)
	}
	return names
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType: a COPY text or
// CSV field is scanned as a string and a binary field as []byte.
func (r *copyOutRows) ColumnTypeScanType(int) reflect.Type {
	if r.format.binary {
		return reflect.TypeOf([]byte(nil))
	}
	return reflect.TypeOf("")
}

// Next reads one row. Rows may be split across CopyData messages, so an
// incomplete row is buffered until its terminator arrives; in CSV, a newline
// inside a quoted field belongs to the row and does not end it.
func (r *copyOutRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	for {
		fields, ok, err := r.takeRow()
		if err != nil {
			r.done = true
			r.pending = nil
			if stopErr := r.abort(); stopErr != nil {
				r.conn.bad = true
				return errors.Join(err, stopErr)
			}
			return err
		}
		if ok {
			if r.format.header && !r.headerDone {
				r.headerDone = true
				continue
			}
			if err := r.decodeRow(fields, dest); err != nil {
				r.done = true
				r.pending = nil
				if stopErr := r.abort(); stopErr != nil {
					r.conn.bad = true
					return errors.Join(err, stopErr)
				}
				return err
			}
			return nil
		}
		typ, body, err := r.conn.recv()
		if err != nil {
			r.done = true
			r.pending = nil
			r.finish()
			if cause := contextCause(r.ctx); cause != nil {
				r.conn.bad = true
				return cause
			}
			return err
		}
		switch typ {
		case proto.MsgCopyData:
			r.pending = append(r.pending, body...)
			// The cap applies to one row, so a frame that carries several
			// complete rows may exceed it; only an unterminated row is too big.
			if len(r.pending) > maxCopyOutRow && !r.hasCompleteRow() {
				r.done = true
				r.pending = nil
				err := fmt.Errorf("opengauss: a COPY ... TO STDOUT row exceeds %d bytes", maxCopyOutRow)
				if stopErr := r.abort(); stopErr != nil {
					r.conn.bad = true
					return errors.Join(err, stopErr)
				}
				return err
			}
		case proto.MsgCopyDone:
			if len(r.pending) != 0 {
				err := errors.New("opengauss: the COPY ... TO STDOUT stream ended in the middle of a row")
				r.done = true
				r.pending = nil
				drainErr := r.conn.drainUntilReady()
				r.finish()
				if drainErr != nil {
					r.conn.bad = true
					return drainErr
				}
				return err
			}
		case proto.MsgCommandComplete, proto.MsgEmptyQueryResponse:
			// The command tag and an empty trailing statement carry no rows.
		case proto.MsgReadyForQuery:
			r.conn.setTxnStatus(body)
			r.done = true
			if len(r.pending) != 0 {
				r.pending = nil
				r.finish()
				return errors.New("opengauss: the COPY ... TO STDOUT stream ended in the middle of a row")
			}
			r.finish()
			return io.EOF
		case proto.MsgErrorResponse:
			serverErr := parseErrorResponse(body)
			r.done = true
			r.pending = nil
			drainErr := r.conn.drainUntilReady()
			r.finish()
			if drainErr != nil {
				r.conn.bad = true
				return drainErr
			}
			return serverErr
		case proto.MsgParameterStatus:
			r.conn.storeParameter(body)
			if r.conn.encodingErr != nil {
				// The remaining copy data would be decoded with the wrong
				// encoding, so the stream is abandoned before it is returned.
				r.done = true
				r.pending = nil
				if stopErr := r.abort(); stopErr != nil {
					r.conn.bad = true
					return errors.Join(r.conn.encodingErr, stopErr)
				}
				return r.conn.encodingErr
			}
		case proto.MsgNoticeResponse, proto.MsgNotificationResponse:
			// Asynchronous messages carry no copy data.
		default:
			r.conn.bad = true
			r.done = true
			r.pending = nil
			r.finish()
			return fmt.Errorf("opengauss: unexpected message type %q while reading COPY ... TO STDOUT", typ)
		}
	}
}

// Close discards the remaining rows. The copy-out protocol does not let the
// frontend end the transfer (only a cancel request or closing the connection
// can do that), so the remaining CopyData messages are read and dropped until
// the session is ready again; cancel the context to stop the server early.
func (r *copyOutRows) Close() error {
	if r.done {
		return nil
	}
	r.done = true
	r.pending = nil
	err := r.conn.discardCopyOut()
	r.watcher = nil
	if err != nil {
		if cause := contextCause(r.ctx); cause != nil {
			r.conn.bad = true
			return cause
		}
	}
	return err
}

// finish ends the operation scope and the session's stream state.
func (r *copyOutRows) finish() {
	r.conn.detachCopyOut()
	r.watcher = nil
}

// abort stops a stream the driver cannot decode: a cancel request bounds the
// transfer and the rest is discarded. A cancel whose delivery was not confirmed
// marks the connection unusable, because it could still abort a later statement.
func (r *copyOutRows) abort() error {
	err := r.conn.abortCopyOut()
	r.watcher = nil
	return err
}

// copyField is one decoded field: either NULL or its text.
type copyField struct {
	value []byte
	null  bool
}

// takeRow removes one complete row from the buffer and returns its fields.
func (r *copyOutRows) takeRow() ([]copyField, bool, error) {
	if r.format.binary {
		return r.takeBinaryRow()
	}
	fields, consumed, ok, err := parseCopyOutRow(r.pending, r.format)
	if err != nil || !ok {
		return nil, false, err
	}
	r.pending = r.pending[consumed:]
	if len(r.pending) == 0 {
		r.pending = nil
	}
	return fields, true, nil
}

// takeBinaryRow reads the stream header once and then one tuple per call. The
// trailer ends the tuples, after which the stream is drained to ReadyForQuery.
func (r *copyOutRows) takeBinaryRow() ([]copyField, bool, error) {
	if r.binaryDone {
		return nil, false, nil
	}
	if !r.binaryHeader {
		consumed, ok, err := consumeCopyBinaryHeader(r.pending)
		if err != nil || !ok {
			return nil, false, err
		}
		r.pending = r.pending[consumed:]
		r.binaryHeader = true
		if len(r.pending) == 0 {
			r.pending = nil
		}
	}
	fields, consumed, ended, ok, err := parseCopyBinaryTuple(r.pending, r.columns)
	if err != nil || !ok {
		return nil, false, err
	}
	r.pending = r.pending[consumed:]
	if len(r.pending) == 0 {
		r.pending = nil
	}
	if ended {
		r.binaryDone = true
		return nil, false, nil
	}
	return fields, true, nil
}

// hasCompleteRow reports whether the buffer already holds one whole row, which
// keeps the row cap from rejecting a frame that carries several complete rows.
func (r *copyOutRows) hasCompleteRow() bool {
	switch {
	case r.format.binary:
		if r.binaryDone {
			return false
		}
		buffer := r.pending
		if !r.binaryHeader {
			consumed, ok, _ := consumeCopyBinaryHeader(buffer)
			if !ok {
				return false
			}
			buffer = buffer[consumed:]
		}
		_, _, _, ok, _ := parseCopyBinaryTuple(buffer, r.columns)
		return ok
	case r.format.csv:
		_, _, ok, _ := parseCopyOutRow(r.pending, r.format)
		return ok
	default:
		return bytes.IndexByte(r.pending, '\n') >= 0
	}
}

// decodeRow writes one row's fields into dest: a NULL field is nil, a binary
// field is a copy of its bytes, and every other field is the decoded text.
func (r *copyOutRows) decodeRow(fields []copyField, dest []driver.Value) error {
	if len(dest) != r.columns {
		return fmt.Errorf("opengauss: a COPY ... TO STDOUT row has %d columns but the destination has %d", r.columns, len(dest))
	}
	if len(fields) != r.columns {
		return fmt.Errorf("opengauss: a COPY ... TO STDOUT row has %d fields but the stream announced %d columns", len(fields), r.columns)
	}
	for i, field := range fields {
		switch {
		case field.null:
			dest[i] = nil
		case r.format.binary:
			value := make([]byte, len(field.value))
			copy(value, field.value)
			dest[i] = value
		default:
			dest[i] = string(field.value)
		}
	}
	return nil
}

// parseCopyOutRow parses one row in the statement's format. It reports false
// when the buffer does not hold a whole row yet, and an error when the buffered
// rows cannot be read in that format.
func parseCopyOutRow(buf []byte, format copyOutFormat) ([]copyField, int, bool, error) {
	if format.csv {
		return parseCSVRow(buf, format)
	}
	return parseTextRow(buf, format)
}

// parseTextRow reads one row of the default text format and expands the COPY
// escapes. A field that is exactly \N is NULL; any other field is text.
func parseTextRow(buf []byte, format copyOutFormat) ([]copyField, int, bool, error) {
	index := bytes.IndexByte(buf, '\n')
	if index < 0 {
		return nil, 0, false, nil
	}
	raw := bytes.Split(buf[:index], []byte{format.delimiter})
	fields := make([]copyField, len(raw))
	for i, field := range raw {
		if bytes.Equal(field, format.null) {
			fields[i] = copyField{null: true}
			continue
		}
		fields[i] = copyField{value: decodeCopyField(field)}
	}
	return fields, index + 1, true, nil
}

// parseCSVRow reads one CSV row: a quoted field may hold the separator, the row
// terminator and a doubled quote, while an unquoted field equal to the NULL
// string is NULL.
func parseCSVRow(buf []byte, format copyOutFormat) ([]copyField, int, bool, error) {
	var fields []copyField
	i := 0
	for {
		if i >= len(buf) {
			return nil, 0, false, nil
		}
		field := copyField{}
		if buf[i] == format.quote {
			value, end, ok, err := parseQuotedCSVField(buf, i, format)
			if err != nil || !ok {
				return nil, 0, false, err
			}
			field.value, i = value, end
		} else {
			start := i
			for i < len(buf) && buf[i] != format.delimiter && buf[i] != '\n' && buf[i] != '\r' {
				i++
			}
			if i >= len(buf) {
				return nil, 0, false, nil
			}
			raw := buf[start:i]
			if bytes.Equal(raw, format.null) {
				field.null = true
			} else {
				field.value = append([]byte(nil), raw...)
			}
		}
		fields = append(fields, field)
		if i >= len(buf) {
			return nil, 0, false, nil
		}
		switch buf[i] {
		case format.delimiter:
			i++
		case '\n':
			return fields, i + 1, true, nil
		case '\r':
			if i+1 >= len(buf) {
				return nil, 0, false, nil
			}
			if buf[i+1] != '\n' {
				return nil, 0, false, errors.New("opengauss: a COPY ... TO STDOUT CSV row has a stray carriage return")
			}
			return fields, i + 2, true, nil
		default:
			return nil, 0, false, fmt.Errorf("opengauss: a COPY ... TO STDOUT CSV row has %q after a quoted field", buf[i])
		}
	}
}

// parseQuotedCSVField reads one quoted CSV field. The quote character doubles to
// stand for itself by default; a distinct ESCAPE character escapes the quote
// and itself, and any other character follows it unchanged.
func parseQuotedCSVField(buf []byte, start int, format copyOutFormat) ([]byte, int, bool, error) {
	var value []byte
	i := start + 1
	for {
		if i >= len(buf) {
			return nil, 0, false, nil
		}
		c := buf[i]
		if c == format.escape && format.escape != format.quote {
			if i+1 >= len(buf) {
				return nil, 0, false, nil
			}
			next := buf[i+1]
			if next == format.quote || next == format.escape {
				value = append(value, next)
				i += 2
				continue
			}
			value = append(value, c)
			i++
			continue
		}
		if c == format.quote {
			if format.escape == format.quote {
				if i+1 >= len(buf) {
					// A quote at the end of the buffer may be the first half of
					// a doubled quote, so the row is not complete yet.
					return nil, 0, false, nil
				}
				if buf[i+1] == format.quote {
					value = append(value, format.quote)
					i += 2
					continue
				}
			}
			return value, i + 1, true, nil
		}
		value = append(value, c)
		i++
	}
}

// decodeCopyField expands the escapes of the COPY text format. The server emits
// the short escapes only, but octal and hex sequences are accepted too because
// they are part of the format.
func decodeCopyField(field []byte) []byte {
	out := make([]byte, 0, len(field))
	for i := 0; i < len(field); i++ {
		if field[i] != '\\' || i+1 >= len(field) {
			out = append(out, field[i])
			continue
		}
		i++
		switch c := field[i]; c {
		case 'b':
			out = append(out, '\b')
		case 'f':
			out = append(out, '\f')
		case 'n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 't':
			out = append(out, '\t')
		case 'v':
			out = append(out, '\v')
		case '\\':
			out = append(out, '\\')
		case 'x':
			value, digits := 0, 0
			for digits < 2 && i+1 < len(field) {
				hex, ok := hexDigit(field[i+1])
				if !ok {
					break
				}
				value = value*16 + hex
				digits++
				i++
			}
			if digits == 0 {
				out = append(out, 'x')
				continue
			}
			out = append(out, byte(value))
		case '0', '1', '2', '3', '4', '5', '6', '7':
			value, digits := int(c-'0'), 1
			for digits < 3 && i+1 < len(field) && field[i+1] >= '0' && field[i+1] <= '7' {
				value = value*8 + int(field[i+1]-'0')
				digits++
				i++
			}
			out = append(out, byte(value))
		default:
			// Any other backslashed character stands for itself.
			out = append(out, c)
		}
	}
	return out
}

func hexDigit(c byte) (int, bool) {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0'), true
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10, true
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10, true
	}
	return 0, false
}

// queryCopyOut sends a COPY ... TO STDOUT statement and waits for the server's
// CopyOutResponse. The returned rows own the session until they finish, and
// format is the representation its CopyData rows carry, as the statement's
// options requested.
func (c *conn) queryCopyOut(ctx context.Context, statement string, format copyOutFormat) (driver.Rows, error) {
	watcher, err := c.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	query := proto.NewMessageWriter(proto.MsgQuery)
	query.String(statement)
	if err := c.sendFrames(query.Finish()); err != nil {
		c.endOperation(watcher)
		return nil, c.wrapIOError(ctx, "copy out", err)
	}
	for {
		typ, body, err := c.recv()
		if err != nil {
			c.endOperation(watcher)
			return nil, c.wrapIOError(ctx, "copy out", err)
		}
		switch typ {
		case proto.MsgCopyOutResponse:
			streamFormat, columns, err := parseCopyResponse(body)
			if err != nil {
				c.bad = true
				c.endOperation(watcher)
				return nil, err
			}
			wantStream := byte(0)
			if format.binary {
				wantStream = 1
			}
			if streamFormat != wantStream {
				// The announced stream cannot be decoded with the statement's
				// options. Copy-out cannot be ended from the frontend, so a
				// cancel request stops the server before the rest is discarded.
				c.copying = true
				c.copyWatcher = watcher
				c.copyCtx = ctx
				if stopErr := c.abortCopyOut(); stopErr != nil {
					return nil, c.wrapIOError(ctx, "copy out", stopErr)
				}
				if wantStream == 1 {
					return nil, errors.New("opengauss: the server announced a text COPY ... TO STDOUT stream for a statement that asked for binary")
				}
				return nil, errors.New("opengauss: the server announced a binary COPY ... TO STDOUT stream for a statement that did not ask for one")
			}
			c.copying = true
			c.copyOwner = nil
			c.copyColumns = columns
			c.copyWatcher = watcher
			c.copyCtx = ctx
			return &copyOutRows{conn: c, columns: columns, format: format, ctx: ctx, watcher: watcher}, nil
		case proto.MsgErrorResponse:
			serverErr := parseErrorResponse(body)
			drainErr := c.drainUntilReady()
			c.endOperation(watcher)
			if drainErr != nil {
				return nil, c.wrapIOError(ctx, "copy out", drainErr)
			}
			return nil, serverErr
		case proto.MsgParameterStatus:
			c.storeParameter(body)
			if c.encodingErr != nil {
				c.endOperation(watcher)
				return nil, c.encodingErr
			}
		case proto.MsgNoticeResponse, proto.MsgNotificationResponse:
			// Asynchronous messages are not part of the COPY exchange.
		default:
			c.bad = true
			c.endOperation(watcher)
			return nil, fmt.Errorf("opengauss: unexpected message type %q while starting COPY ... TO STDOUT", typ)
		}
	}
}

// detachCopyOut clears the session's stream state and ends the operation scope.
func (c *conn) detachCopyOut() {
	watcher := c.copyWatcher
	c.copying = false
	c.copyOwner = nil
	c.copyWatcher = nil
	c.copyCtx = nil
	c.endOperation(watcher)
}

// discardCopyOut reads and drops the rest of a COPY ... TO STDOUT stream until
// the session is ready again. The frontend may not send CopyDone in copy-out
// mode, so the remaining CopyData messages are discarded instead.
func (c *conn) discardCopyOut() error {
	if !c.copying {
		return nil
	}
	watcher := c.copyWatcher
	c.copying = false
	c.copyOwner = nil
	c.copyWatcher = nil
	c.copyCtx = nil

	err := c.drainUntilReady()
	c.endOperation(watcher)
	if err != nil {
		c.bad = true
	}
	return err
}

// abortCopyOut stops a stream the driver cannot decode: a cancel request bounds
// the transfer, then the remaining data is discarded. The connection is marked
// unusable when the postmaster did not confirm the cancel, because the late
// signal could abort the next statement on the session.
func (c *conn) abortCopyOut() error {
	confirmed := c.sendCancelRequest()
	err := c.discardCopyOut()
	if !confirmed {
		c.bad = true
	}
	return err
}

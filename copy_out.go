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
// nil): the text format cannot say more. The session stays pinned to the
// statement until the rows reach ReadyForQuery or are closed.
type copyOutRows struct {
	conn    *conn
	columns int
	ctx     context.Context
	watcher *cancelWatcher
	pending []byte
	done    bool
}

func (r *copyOutRows) Columns() []string {
	names := make([]string, r.columns)
	for i := range names {
		names[i] = "column" + strconv.Itoa(i+1)
	}
	return names
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType: a COPY text
// field is always scanned as a string.
func (r *copyOutRows) ColumnTypeScanType(int) reflect.Type {
	return reflect.TypeOf("")
}

// Next reads one text row. Rows may be split across CopyData messages, so an
// incomplete line is buffered until its terminator arrives.
func (r *copyOutRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	for {
		if line, ok := r.takeLine(); ok {
			if err := r.decodeRow(line, dest); err != nil {
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
			// complete rows may exceed it; only an unterminated run is too big.
			if len(r.pending) > maxCopyOutRow && bytes.IndexByte(r.pending, '\n') < 0 {
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

// takeLine removes one complete row from the buffer. The row terminator is not
// part of the returned line.
func (r *copyOutRows) takeLine() ([]byte, bool) {
	index := bytes.IndexByte(r.pending, '\n')
	if index < 0 {
		return nil, false
	}
	line := r.pending[:index]
	if index+1 == len(r.pending) {
		r.pending = nil
	} else {
		r.pending = r.pending[index+1:]
	}
	return line, true
}

// decodeRow splits one text row into fields and expands the COPY escapes. A
// field that is exactly \N is NULL; any other field is text.
func (r *copyOutRows) decodeRow(line []byte, dest []driver.Value) error {
	if len(dest) != r.columns {
		return fmt.Errorf("opengauss: a COPY ... TO STDOUT row has %d columns but the destination has %d", r.columns, len(dest))
	}
	fields := bytes.Split(line, []byte{'\t'})
	if len(fields) != r.columns {
		return fmt.Errorf("opengauss: a COPY ... TO STDOUT row has %d fields but the stream announced %d columns", len(fields), r.columns)
	}
	for i, field := range fields {
		if len(field) == 2 && field[0] == '\\' && field[1] == 'N' {
			dest[i] = nil
			continue
		}
		dest[i] = string(decodeCopyField(field))
	}
	return nil
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
// CopyOutResponse. The returned rows own the session until they finish.
func (c *conn) queryCopyOut(ctx context.Context, statement string) (driver.Rows, error) {
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
			format, columns, err := parseCopyResponse(body)
			if err != nil {
				c.bad = true
				c.endOperation(watcher)
				return nil, err
			}
			if format != 0 {
				// A binary stream cannot be decoded here. Copy-out mode cannot
				// be ended from the frontend, so a cancel request stops the
				// server before the remaining data is discarded.
				c.copying = true
				c.copyWatcher = watcher
				c.copyCtx = ctx
				if stopErr := c.abortCopyOut(); stopErr != nil {
					return nil, c.wrapIOError(ctx, "copy out", stopErr)
				}
				return nil, errors.New("opengauss: COPY ... TO STDOUT uses binary format, which this driver does not support; request the default text format")
			}
			c.copying = true
			c.copyOwner = nil
			c.copyColumns = columns
			c.copyWatcher = watcher
			c.copyCtx = ctx
			return &copyOutRows{conn: c, columns: columns, ctx: ctx, watcher: watcher}, nil
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

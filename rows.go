package opengauss

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
)

type rows struct {
	conn    *conn
	columns []string
	ctx     context.Context
	watcher *cancelWatcher
	done    bool
}

func (r *rows) Columns() []string {
	names := make([]string, len(r.columns))
	copy(names, r.columns)
	return names
}

// finish ends the operation scope opened for this result, clearing the socket
// deadline once the result is complete or abandoned.
func (r *rows) finish() {
	if r.watcher != nil {
		r.conn.endOperation(r.watcher)
		r.watcher = nil
	}
}

// Close discards unread rows and resynchronizes the connection, so a partially
// consumed result does not leak protocol state back into the pool.
func (r *rows) Close() error {
	if r.done {
		r.finish()
		return nil
	}
	r.done = true
	err := r.conn.drainUntilReady()
	r.finish()
	if err != nil {
		if cause := contextCause(r.ctx); cause != nil {
			r.conn.bad = true
			return cause
		}
	}
	return err
}

func (r *rows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	for {
		typ, body, err := r.conn.recv()
		if err != nil {
			r.done = true
			r.finish()
			if cause := contextCause(r.ctx); cause != nil {
				r.conn.bad = true
				return cause
			}
			return err
		}
		switch typ {
		case msgDataRow:
			if err := r.scanRow(body, dest); err != nil {
				r.done = true
				r.finish()
				return err
			}
			return nil
		case msgCommandComplete:
			// The command tag is only needed by Exec; keep reading until the
			// server marks the session ready.
		case msgReadyForQuery:
			r.conn.setTxnStatus(body)
			r.done = true
			r.finish()
			return io.EOF
		case msgErrorResponse:
			serverErr := parseErrorResponse(body)
			r.done = true
			drainErr := r.conn.drainUntilReady()
			r.finish()
			if drainErr != nil {
				if cause := contextCause(r.ctx); cause != nil {
					r.conn.bad = true
					return cause
				}
				return drainErr
			}
			return serverErr
		case msgNoticeResponse, msgNotificationResponse, msgParameterStatus:
			// Asynchronous messages carry no column data.
		case msgCopyInResponse, msgCopyOutResponse, msgCopyBothResponse:
			r.conn.bad = true
			r.done = true
			r.finish()
			return fmt.Errorf("opengauss: COPY is not implemented yet (message %q)", typ)
		default:
			r.conn.bad = true
			r.done = true
			r.finish()
			return fmt.Errorf("opengauss: unexpected message type %q while reading rows", typ)
		}
	}
}

func (r *rows) scanRow(body []byte, dest []driver.Value) error {
	if len(dest) != len(r.columns) {
		r.conn.bad = true
		return fmt.Errorf("opengauss: row has %d columns but the destination has %d", len(r.columns), len(dest))
	}
	rb := &readBuf{b: body}
	count := int(rb.int16())
	if rb.err != nil || count != len(r.columns) {
		r.conn.bad = true
		return fmt.Errorf("opengauss: malformed DataRow for %d columns", len(r.columns))
	}
	for i := 0; i < count; i++ {
		length := rb.int32()
		if rb.err != nil {
			break
		}
		if length < 0 {
			dest[i] = nil
			continue
		}
		dest[i] = rb.bytes(int(length))
		if rb.err != nil {
			break
		}
	}
	if rb.err != nil {
		r.conn.bad = true
		return rb.err
	}
	return nil
}

// parseRowDescription returns the column names. The remaining RowDescription
// fields describe types and formats that the text-only first milestone does not
// use; they are consumed here so the cursor stays aligned.
func parseRowDescription(body []byte) ([]string, error) {
	r := &readBuf{b: body}
	count := int(r.int16())
	// A negative count means the 16-bit field exceeded math.MaxInt16, and a
	// count larger than the remaining bytes cannot describe real columns.
	if r.err != nil || count < 0 || count > r.remaining() {
		return nil, errMalformedMessage
	}
	names := make([]string, 0, count)
	for i := 0; i < count; i++ {
		names = append(names, r.string())
		r.int32() // table OID
		r.int16() // attribute number
		r.int32() // data type OID
		r.int16() // data type size
		r.int32() // type modifier
		r.int16() // format code
		if r.err != nil {
			return nil, errMalformedMessage
		}
	}
	if r.err != nil {
		return nil, errMalformedMessage
	}
	return names, nil
}

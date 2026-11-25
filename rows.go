package opengauss

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"

	"github.com/cocosip/go-opengauss/internal/oid"
	"github.com/cocosip/go-opengauss/internal/proto"
)

// column is one result column: the name database/sql exposes and the type OID
// that selects the Go value produced for its text representation.
type column struct {
	name string
	oid  int32
}

type rows struct {
	conn    *conn
	columns []column
	ctx     context.Context
	watcher *cancelWatcher
	done    bool
}

func (r *rows) Columns() []string {
	names := make([]string, len(r.columns))
	for i, column := range r.columns {
		names[i] = column.name
	}
	return names
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName.
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.columns) {
		return ""
	}
	return oid.TypeName(r.columns[index].oid)
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType.
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.columns) {
		return reflect.TypeOf("")
	}
	return oid.ScanType(r.columns[index].oid)
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
		case proto.MsgDataRow:
			if err := r.scanRow(body, dest); err != nil {
				r.done = true
				r.finish()
				return err
			}
			return nil
		case proto.MsgCommandComplete:
			// The command tag is only needed by Exec; keep reading until the
			// server marks the session ready.
		case proto.MsgReadyForQuery:
			r.conn.setTxnStatus(body)
			r.done = true
			r.finish()
			return io.EOF
		case proto.MsgErrorResponse:
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
		case proto.MsgNoticeResponse, proto.MsgNotificationResponse, proto.MsgParameterStatus:
			// Asynchronous messages carry no column data.
		case proto.MsgCopyInResponse, proto.MsgCopyOutResponse, proto.MsgCopyBothResponse:
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
	rb := &proto.ReadBuf{B: body}
	count := int(rb.Int16())
	if rb.Err != nil || count != len(r.columns) {
		r.conn.bad = true
		return fmt.Errorf("opengauss: malformed DataRow for %d columns", len(r.columns))
	}
	for i := 0; i < count; i++ {
		length := rb.Int32()
		if rb.Err != nil {
			break
		}
		if length < 0 {
			dest[i] = nil
			continue
		}
		dest[i] = oid.Decode(r.columns[i].oid, rb.Bytes(int(length)))
		if rb.Err != nil {
			break
		}
	}
	if rb.Err != nil {
		r.conn.bad = true
		return rb.Err
	}
	return nil
}

// parseRowDescription returns the column names and type OIDs. The remaining
// RowDescription fields are consumed so the cursor stays aligned.
func parseRowDescription(body []byte) ([]column, error) {
	r := &proto.ReadBuf{B: body}
	count := int(r.Int16())
	// A negative count means the 16-bit field exceeded math.MaxInt16, and a
	// count larger than the remaining bytes cannot describe real columns.
	if r.Err != nil || count < 0 || count > r.Remaining() {
		return nil, proto.ErrMalformedMessage
	}
	columns := make([]column, 0, count)
	for i := 0; i < count; i++ {
		name := r.String()
		r.Int32() // table OID
		r.Int16() // attribute number
		typeOID := r.Int32()
		r.Int16() // data type size
		r.Int32() // type modifier
		format := r.Int16()
		if r.Err != nil {
			return nil, proto.ErrMalformedMessage
		}
		// The driver always requests text results. A binary column would be
		// decoded as text and silently produce wrong values, so refuse it.
		if format != 0 {
			return nil, fmt.Errorf("opengauss: result column %q uses binary format, which this driver does not request", name)
		}
		columns = append(columns, column{name: name, oid: typeOID})
	}
	if r.Err != nil {
		return nil, proto.ErrMalformedMessage
	}
	return columns, nil
}

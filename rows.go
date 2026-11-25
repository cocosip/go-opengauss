package opengauss

import (
	"context"
	"database/sql/driver"
	"errors"
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
	// hasNext records a RowDescription read ahead after the current result set
	// ended, so database/sql can advance with NextResultSet instead of closing
	// the rows. nextColumns belongs to that pending set; columns stays the
	// current set's metadata until NextResultSet commits the change.
	hasNext     bool
	nextColumns []column
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
	r.hasNext = false
	r.nextColumns = nil
	err := r.conn.drainUntilReady()
	r.finish()
	if err != nil {
		if cause := contextCause(r.ctx); cause != nil {
			r.conn.bad = true
			return cause
		}
		return err
	}
	// A client encoding the driver cannot decode may have arrived while the
	// unread rows were discarded; the session is already marked unusable.
	return r.conn.encodingErr
}

// HasNextResultSet implements driver.RowsNextResultSet. A simple query executed
// directly on a connection can carry several statements, and each statement
// that produces rows is one result set.
func (r *rows) HasNextResultSet() bool {
	return r.hasNext
}

// NextResultSet implements driver.RowsNextResultSet. The contract requires
// advancing even when the current set still has unread rows, so those rows are
// discarded first.
func (r *rows) NextResultSet() error {
	if r.done {
		return io.EOF
	}
	dest := make([]driver.Value, len(r.columns))
	for !r.hasNext && !r.done {
		if err := r.Next(dest); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
	}
	if r.done || !r.hasNext {
		return io.EOF
	}
	r.columns = r.nextColumns
	r.nextColumns = nil
	r.hasNext = false
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.done || r.hasNext {
		// The next result set must be started with NextResultSet; until then
		// the current set stays finished.
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
		case proto.MsgRowDescription:
			// The current set ended with its CommandComplete, so this
			// description belongs to the next statement's result set. It is
			// kept apart until NextResultSet makes it current.
			columns, err := parseRowDescription(body)
			if err != nil {
				r.conn.bad = true
				r.done = true
				r.finish()
				return err
			}
			r.nextColumns = columns
			r.hasNext = true
			return io.EOF
		case proto.MsgEmptyQueryResponse:
			// An empty statement in a simple-target batch produces no rows and
			// no tag; the batch continues with the next statement.
		case proto.MsgCommandComplete:
			// The current statement is done; keep reading until another result
			// set or ReadyForQuery.
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
		case proto.MsgParameterStatus:
			r.conn.storeParameter(body)
			if r.conn.encodingErr != nil {
				r.conn.bad = true
				r.done = true
				r.finish()
				return r.conn.encodingErr
			}
		case proto.MsgNoticeResponse, proto.MsgNotificationResponse:
			// Asynchronous messages carry no column data.
		case proto.MsgCopyInResponse, proto.MsgCopyOutResponse, proto.MsgCopyBothResponse:
			r.conn.bad = true
			r.done = true
			r.finish()
			return copyStreamError(typ)
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

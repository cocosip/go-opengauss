package opengauss

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// ExecContext runs statement and returns the command tag. A statement without
// arguments uses the simple query protocol; arguments select the extended
// Parse/Bind/Describe/Execute/Sync path.
func (c *conn) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if err := validateQuery(statement); err != nil {
		return nil, err
	}
	frames, err := queryFrames(statement, args)
	if err != nil {
		return nil, err
	}
	tag, err := c.execFrames(ctx, "exec", frames)
	if err != nil {
		return nil, err
	}
	return result{tag: tag}, nil
}

// QueryContext runs statement and returns the result rows.
func (c *conn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if err := validateQuery(statement); err != nil {
		return nil, err
	}
	frames, err := queryFrames(statement, args)
	if err != nil {
		return nil, err
	}
	return c.queryFramesOperation(ctx, "query", frames)
}

// execFrames sends messages and reads the command result within one operation
// scope, so the caller's deadline and cancellation cover the whole exchange.
func (c *conn) execFrames(ctx context.Context, stage string, frames [][]byte) (string, error) {
	watcher, err := c.beginOperation(ctx)
	if err != nil {
		return "", err
	}
	defer c.endOperation(watcher)

	if err := c.sendFrames(frames...); err != nil {
		return "", c.wrapIOError(ctx, stage, err)
	}
	tag, err := c.readExecResult()
	if err != nil {
		return "", c.wrapIOError(ctx, stage, err)
	}
	return tag, nil
}

// queryFramesOperation sends messages and reads the row metadata. When the
// result streams rows, the operation scope stays open until the rows reach
// ReadyForQuery or are closed, so the caller's deadline covers every read.
func (c *conn) queryFramesOperation(ctx context.Context, stage string, frames [][]byte) (*rows, error) {
	watcher, err := c.beginOperation(ctx)
	if err != nil {
		return nil, err
	}

	if err := c.sendFrames(frames...); err != nil {
		c.endOperation(watcher)
		return nil, c.wrapIOError(ctx, stage, err)
	}
	result, err := c.readQueryRows()
	if err != nil {
		c.endOperation(watcher)
		return nil, c.wrapIOError(ctx, stage, err)
	}
	if result.done {
		c.endOperation(watcher)
		return result, nil
	}
	result.ctx = ctx
	result.watcher = watcher
	return result, nil
}

// simpleExec sends one statement without parameters and returns its command
// tag, which lets transaction handling detect a server-side abort.
func (c *conn) simpleExec(ctx context.Context, statement string) (string, error) {
	if err := validateQuery(statement); err != nil {
		return "", err
	}
	query := newMessageWriter(msgQuery)
	query.string(statement)
	return c.execFrames(ctx, "exec", [][]byte{query.finish()})
}

// Ping verifies that the session is usable. The empty statement returns
// EmptyQueryResponse followed by ReadyForQuery.
func (c *conn) Ping(ctx context.Context) error {
	rows, err := c.QueryContext(ctx, "", nil)
	if err != nil {
		return err
	}
	return rows.Close()
}

// ResetSession discards a connection that is still inside a transaction, so
// the pool never hands out a session with stale transaction state.
func (c *conn) ResetSession(context.Context) error {
	if c.bad {
		return driver.ErrBadConn
	}
	if c.txnStatus != txnIdle {
		c.bad = true
		return driver.ErrBadConn
	}
	return nil
}

// Close sends a best-effort Terminate message and closes the socket.
func (c *conn) Close() error {
	if c.netConn == nil {
		return nil
	}
	if !c.bad {
		_ = c.sendFrames(newMessageWriter(msgTerminate).finish())
	}
	err := c.netConn.Close()
	c.netConn = nil
	return err
}

func validateQuery(statement string) error {
	if strings.IndexByte(statement, 0) >= 0 {
		return errors.New("opengauss: SQL text contains a NUL byte")
	}
	return nil
}

// queryFrames builds the frontend messages for one statement.
func queryFrames(statement string, args []driver.NamedValue) ([][]byte, error) {
	if len(args) == 0 {
		query := newMessageWriter(msgQuery)
		query.string(statement)
		return [][]byte{query.finish()}, nil
	}

	parse := newMessageWriter(msgParse)
	parse.string("") // unnamed statement
	parse.string(statement)
	parse.int16(0) // let the server infer parameter types

	bindExecute, err := bindExecuteFrames("", args)
	if err != nil {
		return nil, err
	}
	return append([][]byte{parse.finish()}, bindExecute...), nil
}

// bindExecuteFrames builds Bind, Describe(portal), Execute and Sync.
func bindExecuteFrames(statement string, args []driver.NamedValue) ([][]byte, error) {
	bind, err := bindFrame(statement, args)
	if err != nil {
		return nil, err
	}

	describe := newMessageWriter(msgDescribe)
	describe.byte('P')
	describe.string("") // unnamed portal

	execute := newMessageWriter(msgExecute)
	execute.string("")
	execute.int32(0) // no row limit

	return [][]byte{
		bind,
		describe.finish(),
		execute.finish(),
		newMessageWriter(msgSync).finish(),
	}, nil
}

// bindFrame builds a Bind message with text-format parameters and text-format
// results. The result format is left to the server's text representation so
// values round-trip without guessing binary layouts.
func bindFrame(statement string, args []driver.NamedValue) ([]byte, error) {
	if len(args) > math.MaxInt16 {
		return nil, fmt.Errorf("opengauss: too many parameters: %d", len(args))
	}
	bind := newMessageWriter(msgBind)
	bind.string("") // unnamed portal
	bind.string(statement)
	bind.int16(0) // all parameters use text format
	bind.int16(int16(len(args)))
	for _, arg := range args {
		if arg.Value == nil {
			bind.int32(-1)
			continue
		}
		data, err := encodeTextValue(arg.Value)
		if err != nil {
			return nil, fmt.Errorf("opengauss: parameter %d: %w", arg.Ordinal, err)
		}
		if len(data) > math.MaxInt32 {
			return nil, fmt.Errorf("opengauss: parameter %d is too large: %d bytes", arg.Ordinal, len(data))
		}
		bind.int32(int32(len(data)))
		bind.bytes(data)
	}
	bind.int16(0) // all results use text format
	return bind.finish(), nil
}

func encodeTextValue(value driver.Value) ([]byte, error) {
	switch v := value.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	case int64:
		return strconv.AppendInt(nil, v, 10), nil
	case float64:
		return strconv.AppendFloat(nil, v, 'f', -1, 64), nil
	case bool:
		if v {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	case time.Time:
		return []byte(v.Format("2006-01-02 15:04:05.999999999-07:00")), nil
	default:
		return nil, fmt.Errorf("unsupported Go type %T: supply a string, []byte, int64, float64, bool, time.Time or nil", value)
	}
}

func (c *conn) readExecResult() (string, error) {
	var tag string
	for {
		typ, body, err := c.recv()
		if err != nil {
			return "", err
		}
		switch typ {
		case msgCommandComplete:
			tag = commandTag(body)
		case msgReadyForQuery:
			c.setTxnStatus(body)
			return tag, nil
		case msgErrorResponse:
			serverErr := parseErrorResponse(body)
			if err := c.drainUntilReady(); err != nil {
				return "", err
			}
			return "", serverErr
		case msgNoticeResponse, msgNotificationResponse, msgParameterStatus:
			// Asynchronous messages are not part of the command result.
		case msgRowDescription, msgDataRow, msgEmptyQueryResponse, msgNoData,
			msgParseComplete, msgBindComplete, msgParameterDescription, msgCloseComplete:
			// Exec discards any result payload.
		case msgCopyInResponse, msgCopyOutResponse, msgCopyBothResponse:
			c.bad = true
			return "", fmt.Errorf("opengauss: COPY is not implemented yet (message %q)", typ)
		default:
			c.bad = true
			return "", fmt.Errorf("opengauss: unexpected message type %q while executing", typ)
		}
	}
}

func (c *conn) readQueryRows() (*rows, error) {
	for {
		typ, body, err := c.recv()
		if err != nil {
			return nil, err
		}
		switch typ {
		case msgRowDescription:
			columns, err := parseRowDescription(body)
			if err != nil {
				c.bad = true
				return nil, err
			}
			return &rows{conn: c, columns: columns}, nil
		case msgCommandComplete:
			if err := c.drainUntilReady(); err != nil {
				return nil, err
			}
			return &rows{conn: c, done: true}, nil
		case msgReadyForQuery:
			c.setTxnStatus(body)
			return &rows{conn: c, done: true}, nil
		case msgErrorResponse:
			serverErr := parseErrorResponse(body)
			if err := c.drainUntilReady(); err != nil {
				return nil, err
			}
			return nil, serverErr
		case msgEmptyQueryResponse, msgNoticeResponse, msgNotificationResponse, msgParameterStatus,
			msgParseComplete, msgBindComplete, msgNoData, msgParameterDescription, msgCloseComplete:
			// Empty query and extended-protocol acknowledgements carry no rows.
		case msgCopyInResponse, msgCopyOutResponse, msgCopyBothResponse:
			c.bad = true
			return nil, fmt.Errorf("opengauss: COPY is not implemented yet (message %q)", typ)
		default:
			c.bad = true
			return nil, fmt.Errorf("opengauss: unexpected message type %q while reading query result", typ)
		}
	}
}

// drainUntilReady consumes messages until ReadyForQuery, which the server
// sends after any error or completed command. Reusing the connection
// afterwards is only safe once this point is reached.
func (c *conn) drainUntilReady() error {
	for {
		typ, body, err := c.recv()
		if err != nil {
			return err
		}
		switch typ {
		case msgReadyForQuery:
			c.setTxnStatus(body)
			return nil
		case msgErrorResponse, msgNoticeResponse, msgNotificationResponse, msgParameterStatus,
			msgCommandComplete, msgDataRow, msgRowDescription, msgEmptyQueryResponse,
			msgParseComplete, msgBindComplete, msgNoData, msgParameterDescription, msgCloseComplete:
			// Discarded while resynchronizing.
		case msgCopyInResponse, msgCopyOutResponse, msgCopyBothResponse:
			c.bad = true
			return fmt.Errorf("opengauss: COPY is not implemented yet (message %q)", typ)
		default:
			c.bad = true
			return fmt.Errorf("opengauss: unexpected message type %q while resynchronizing", typ)
		}
	}
}

func commandTag(body []byte) string {
	r := &readBuf{b: body}
	return r.string()
}

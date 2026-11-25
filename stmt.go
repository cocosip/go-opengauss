package opengauss

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
)

// stmt is a prepared statement bound to one physical connection. It uses the
// extended protocol: Parse and Describe once, then Bind and Execute per call.
type stmt struct {
	conn       *conn
	name       string
	paramCount int
	closed     bool
}

// Prepare implements driver.Conn.
func (c *conn) Prepare(statement string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), statement)
}

// PrepareContext parses and describes statement so parameter count and type
// inference are resolved before the first execution.
func (c *conn) PrepareContext(ctx context.Context, statement string) (driver.Stmt, error) {
	if err := validateQuery(statement); err != nil {
		return nil, err
	}

	watcher, err := c.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer c.endOperation(watcher)

	c.stmtCounter++
	name := fmt.Sprintf("opengauss_stmt_%d", c.stmtCounter)

	parse := newMessageWriter(msgParse)
	parse.string(name)
	parse.string(statement)
	parse.int16(0) // let the server infer parameter types

	describe := newMessageWriter(msgDescribe)
	describe.byte('S')
	describe.string(name)

	if err := c.sendFrames(parse.finish(), describe.finish(), newMessageWriter(msgSync).finish()); err != nil {
		return nil, c.wrapIOError(ctx, "prepare", err)
	}

	paramCount, err := c.readPrepareResult()
	if err != nil {
		return nil, c.wrapIOError(ctx, "prepare", err)
	}
	return &stmt{conn: c, name: name, paramCount: paramCount}, nil
}

func (c *conn) readPrepareResult() (int, error) {
	paramCount := 0
	for {
		typ, body, err := c.recv()
		if err != nil {
			return 0, err
		}
		switch typ {
		case msgParseComplete:
			// Parse accepted.
		case msgParameterDescription:
			r := &readBuf{b: body}
			paramCount = int(r.int16())
			for i := 0; i < paramCount; i++ {
				r.int32()
			}
			if r.err != nil {
				c.bad = true
				return 0, r.err
			}
		case msgRowDescription:
			if _, err := parseRowDescription(body); err != nil {
				c.bad = true
				return 0, err
			}
		case msgNoData:
			// The statement returns no rows.
		case msgReadyForQuery:
			c.setTxnStatus(body)
			return paramCount, nil
		case msgErrorResponse:
			serverErr := parseErrorResponse(body)
			if err := c.drainUntilReady(); err != nil {
				return 0, err
			}
			return 0, serverErr
		case msgNoticeResponse, msgNotificationResponse, msgParameterStatus:
			// Asynchronous messages.
		default:
			c.bad = true
			return 0, fmt.Errorf("opengauss: unexpected message type %q while preparing", typ)
		}
	}
}

// NumInput reports the parameter count resolved by the server.
func (s *stmt) NumInput() int { return s.paramCount }

// Exec implements driver.Stmt.
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), namedValues(args))
}

// Query implements driver.Stmt.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), namedValues(args))
}

// ExecContext binds args and executes the prepared statement.
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	frames, err := s.frames(args)
	if err != nil {
		return nil, err
	}
	tag, err := s.conn.execFrames(ctx, "exec", frames)
	if err != nil {
		return nil, err
	}
	return result{tag: tag}, nil
}

// QueryContext binds args and executes the prepared statement, returning rows.
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	frames, err := s.frames(args)
	if err != nil {
		return nil, err
	}
	return s.conn.queryFramesOperation(ctx, "query", frames)
}

// Close deallocates the server-side statement.
func (s *stmt) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.conn.bad {
		return nil
	}

	closeStatement := newMessageWriter(msgClose)
	closeStatement.byte('S')
	closeStatement.string(s.name)

	if err := s.conn.sendFrames(closeStatement.finish(), newMessageWriter(msgSync).finish()); err != nil {
		return err
	}
	return s.conn.readCloseResult()
}

func (s *stmt) frames(args []driver.NamedValue) ([][]byte, error) {
	if s.closed {
		return nil, errors.New("opengauss: statement is already closed")
	}
	if s.conn.bad {
		return nil, driver.ErrBadConn
	}
	if len(args) != s.paramCount {
		return nil, fmt.Errorf("opengauss: statement expects %d parameters, got %d", s.paramCount, len(args))
	}
	return bindExecuteFrames(s.name, args)
}

func (c *conn) readCloseResult() error {
	for {
		typ, body, err := c.recv()
		if err != nil {
			return err
		}
		switch typ {
		case msgCloseComplete:
			// Statement deallocated.
		case msgReadyForQuery:
			c.setTxnStatus(body)
			return nil
		case msgErrorResponse:
			serverErr := parseErrorResponse(body)
			if err := c.drainUntilReady(); err != nil {
				return err
			}
			return serverErr
		case msgNoticeResponse, msgNotificationResponse, msgParameterStatus:
			// Asynchronous messages.
		default:
			c.bad = true
			return fmt.Errorf("opengauss: unexpected message type %q while closing a statement", typ)
		}
	}
}

func namedValues(args []driver.Value) []driver.NamedValue {
	values := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		values[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
	}
	return values
}

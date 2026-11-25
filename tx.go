package opengauss

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/cocosip/go-opengauss/internal/proto"
	"strings"
)

// tx is bound to one physical connection for its whole lifetime.
type tx struct {
	conn   *conn
	closed bool
}

// Begin implements driver.Conn.
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts a transaction with the requested isolation and read-only
// mode. openGauss has no READ WRITE suffix; the default is read-write.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.bad {
		return nil, driver.ErrBadConn
	}
	if c.txnStatus != proto.TxnIdle {
		c.bad = true
		return nil, errors.New("opengauss: cannot begin a transaction on a connection that is already in one")
	}

	statement, err := beginSQL(opts)
	if err != nil {
		return nil, err
	}
	tag, err := c.simpleExec(ctx, statement)
	if err != nil {
		return nil, err
	}
	if tag != "BEGIN" {
		c.bad = true
		return nil, fmt.Errorf("opengauss: unexpected command tag %q after %q", tag, statement)
	}
	if c.txnStatus != proto.TxnInTransaction {
		c.bad = true
		return nil, fmt.Errorf("opengauss: server did not report an open transaction after %q", statement)
	}
	return &tx{conn: c}, nil
}

func beginSQL(opts driver.TxOptions) (string, error) {
	var b strings.Builder
	b.WriteString("BEGIN")
	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault:
	case sql.LevelReadUncommitted:
		b.WriteString(" ISOLATION LEVEL READ UNCOMMITTED")
	case sql.LevelReadCommitted:
		b.WriteString(" ISOLATION LEVEL READ COMMITTED")
	case sql.LevelRepeatableRead, sql.LevelSnapshot:
		b.WriteString(" ISOLATION LEVEL REPEATABLE READ")
	case sql.LevelSerializable:
		b.WriteString(" ISOLATION LEVEL SERIALIZABLE")
	default:
		return "", fmt.Errorf("opengauss: unsupported isolation level %d", int(opts.Isolation))
	}
	if opts.ReadOnly {
		b.WriteString(" READ ONLY")
	}
	return b.String(), nil
}

// Commit commits the transaction. If the server rolled the transaction back
// because a statement inside it failed, Commit reports that instead of
// pretending the data was stored. If the response is lost the outcome is
// unknown, so the connection is discarded and the transaction is never
// replayed.
func (t *tx) Commit() error {
	if t.closed {
		return errors.New("opengauss: transaction is already closed")
	}
	t.closed = true
	if t.conn.bad {
		return driver.ErrBadConn
	}
	tag, err := t.conn.simpleExec(context.Background(), "COMMIT")
	if err != nil {
		if t.conn.txnStatus != proto.TxnIdle {
			t.conn.bad = true
		}
		return err
	}
	if tag == "ROLLBACK" {
		return errors.New("opengauss: server rolled the transaction back because a statement inside it failed")
	}
	if tag != "COMMIT" {
		t.conn.bad = true
		return fmt.Errorf("opengauss: unexpected command tag %q after COMMIT", tag)
	}
	if t.conn.txnStatus != proto.TxnIdle {
		t.conn.bad = true
		return errors.New("opengauss: server did not report the transaction as closed after COMMIT")
	}
	return nil
}

// Rollback rolls the transaction back and leaves the session reusable.
func (t *tx) Rollback() error {
	if t.closed {
		return errors.New("opengauss: transaction is already closed")
	}
	t.closed = true
	if t.conn.bad {
		return driver.ErrBadConn
	}
	if t.conn.txnStatus == proto.TxnIdle {
		// A previous statement already ended the transaction.
		return nil
	}
	_, err := t.conn.simpleExec(context.Background(), "ROLLBACK")
	return err
}

package opengauss

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("opengauss", &Driver{})
}

// Driver implements driver.Driver and driver.DriverContext. Applications
// normally reach it through sql.Open or through NewConnector and sql.OpenDB.
type Driver struct{}

// Open implements driver.Driver. It parses name as a DSN and opens one
// connection, which also backs the sql.Open path.
func (d *Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector implements driver.DriverContext so callers can resolve
// configuration once and share it across connections.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return NewConnector(name)
}

// Interface assertions document the database/sql contracts implemented here.
var (
	_ driver.Driver             = (*Driver)(nil)
	_ driver.DriverContext      = (*Driver)(nil)
	_ driver.Conn               = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
	_ driver.Stmt               = (*stmt)(nil)
	_ driver.StmtExecContext    = (*stmt)(nil)
	_ driver.StmtQueryContext   = (*stmt)(nil)
	_ driver.Tx                 = (*tx)(nil)
	_ driver.Rows               = (*rows)(nil)
)

package opengauss

import (
	"context"
	"database/sql/driver"
)

// Connector holds the immutable settings parsed from a DSN and opens physical
// connections on demand. NewConnector resolves configuration but never
// contacts the database; dial, TLS, authentication and session errors surface
// on Connect and on the first operation that needs a connection.
type Connector struct {
	config *Config
}

// NewConnector parses dsn into a Connector for sql.OpenDB. Local TLS material
// is validated here so a broken sslrootcert fails construction instead of
// silently changing the negotiated policy.
func NewConnector(dsn string) (*Connector, error) {
	config, err := parseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if config.SSLMode != SSLModeDisable {
		tlsConfig, err := config.buildTLSConfig()
		if err != nil {
			return nil, err
		}
		config.tlsConfig = tlsConfig
	}
	return &Connector{config: config}, nil
}

// Connect opens one physical connection. It never returns driver.ErrBadConn:
// a failed attempt is reported with its stage and cause.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	physical, err := connect(ctx, c.config)
	if err != nil {
		return nil, err
	}
	return physical, nil
}

// Driver returns the driver that produced this Connector.
func (c *Connector) Driver() driver.Driver {
	return &Driver{}
}

var _ driver.Connector = (*Connector)(nil)

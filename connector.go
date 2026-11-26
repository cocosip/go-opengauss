package opengauss

import (
	"context"
	"database/sql/driver"
	"net"
)

// DialFunc opens the network connection to one target. network is "tcp" or
// "unix" and address is the value the driver would otherwise pass to
// net.Dialer. The function must respect ctx. It is used for connection
// attempts only; a CancelRequest still reaches the peer directly.
type DialFunc func(ctx context.Context, network, address string) (net.Conn, error)

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
	return newConnector(dsn, nil)
}

// NewConnectorWithDialer is NewConnector with a custom DialFunc. A nil dial
// means net.Dialer. The dialer receives the target network and address for
// every attempt, including TLS fallbacks and later targets.
func NewConnectorWithDialer(dsn string, dial DialFunc) (*Connector, error) {
	return newConnector(dsn, dial)
}

func newConnector(dsn string, dial DialFunc) (*Connector, error) {
	config, err := parseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.dialFunc = dial
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

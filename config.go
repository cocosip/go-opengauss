package opengauss

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// SSLMode is the TLS policy requested by the application.
type SSLMode string

// The supported TLS policies. The names match the libpq sslmode values.
const (
	SSLModeDisable    SSLMode = "disable"
	SSLModeAllow      SSLMode = "allow"
	SSLModePrefer     SSLMode = "prefer"
	SSLModeRequire    SSLMode = "require"
	SSLModeVerifyCA   SSLMode = "verify-ca"
	SSLModeVerifyFull SSLMode = "verify-full"
)

func (m SSLMode) valid() bool {
	switch m {
	case SSLModeDisable, SSLModeAllow, SSLModePrefer, SSLModeRequire, SSLModeVerifyCA, SSLModeVerifyFull:
		return true
	}
	return false
}

// Config is the immutable set of connection settings parsed from a DSN.
// A Connector shares one Config between every physical connection it opens.
type Config struct {
	Host           string
	Port           int
	User           string
	Password       string
	Database       string
	SSLMode        SSLMode
	SSLRootCert    string
	SSLClientCert  string
	SSLClientKey   string
	ConnectTimeout time.Duration

	// tlsConfig is prepared once by NewConnector, so certificate files are
	// validated before any connection attempt. It is nil for sslmode=disable.
	tlsConfig *tls.Config
}

// String returns a description of the endpoint with the password removed. It
// is safe to include in logs and errors.
func (c *Config) String() string {
	return fmt.Sprintf("opengauss://%s@%s/%s?sslmode=%s",
		c.User, net.JoinHostPort(c.Host, strconv.Itoa(c.Port)), c.Database, c.SSLMode)
}

const defaultPort = 5432

// parseConfig resolves a keyword/value DSN, a URL, the process environment and
// an optional service file and passfile. Precedence follows libpq: explicit
// DSN parameters win over service-file entries, which win over environment
// variables; the passfile only supplies a password that nothing else set.
// Settings the driver does not implement are rejected rather than ignored, so
// an application never silently connects with a weaker policy than it asked
// for.
func parseConfig(dsn string) (*Config, error) {
	values, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}

	service, serviceSet := values["service"], values["service"] != ""
	if !serviceSet {
		service = os.Getenv("PGSERVICE")
	}
	delete(values, "service")
	if service != "" {
		if err := applyServiceFile(values, service); err != nil {
			return nil, err
		}
	}

	passfile, passfileSet := values["passfile"], values["passfile"] != ""
	if !passfileSet {
		passfile = os.Getenv("PGPASSFILE")
	}
	delete(values, "passfile")
	applyEnvironment(values)

	cfg, err := newConfig(values)
	if err != nil {
		return nil, err
	}
	if cfg.Password == "" {
		password, err := lookupPassfile(passfile, passfileSet && passfile != "", cfg)
		if err != nil {
			return nil, err
		}
		cfg.Password = password
	}
	return cfg, nil
}

// parseDSN parses only the DSN text. An empty DSN yields no parameters, which
// lets the environment and service file supply everything.
func parseDSN(dsn string) (map[string]string, error) {
	if strings.TrimSpace(dsn) == "" {
		return map[string]string{}, nil
	}
	if schemeEnd := strings.Index(dsn, "://"); schemeEnd > 0 && isScheme(dsn[:schemeEnd]) {
		return parseURLDSN(dsn)
	}
	return parseKeywordDSN(dsn)
}

// environmentParameters maps libpq environment variables to DSN parameters.
var environmentParameters = []struct{ env, param string }{
	{"PGHOST", "host"},
	{"PGPORT", "port"},
	{"PGDATABASE", "dbname"},
	{"PGUSER", "user"},
	{"PGPASSWORD", "password"},
	{"PGSSLMODE", "sslmode"},
	{"PGSSLROOTCERT", "sslrootcert"},
	{"PGSSLCERT", "sslcert"},
	{"PGSSLKEY", "sslkey"},
	{"PGCONNECT_TIMEOUT", "connect_timeout"},
}

// applyEnvironment fills parameters that neither the DSN nor the service file
// set. PGDATABASE is the environment alias of dbname, so it must not collide
// with an explicit database= alias.
func applyEnvironment(values map[string]string) {
	for _, entry := range environmentParameters {
		if _, set := values[entry.param]; set {
			continue
		}
		if entry.param == "dbname" {
			if _, set := values["database"]; set {
				continue
			}
		}
		if value := os.Getenv(entry.env); value != "" {
			values[entry.param] = value
		}
	}
}

// parseKeywordDSN parses a libpq-style keyword/value DSN. Values may be
// single-quoted and may use backslash escapes inside and outside quotes.
func parseKeywordDSN(dsn string) (map[string]string, error) {
	values := make(map[string]string)
	i, n := 0, len(dsn)
	for i < n {
		for i < n && isSpace(dsn[i]) {
			i++
		}
		if i >= n {
			break
		}

		keyStart := i
		for i < n && dsn[i] != '=' && !isSpace(dsn[i]) {
			i++
		}
		key := dsn[keyStart:i]
		if key == "" {
			return nil, fmt.Errorf("opengauss: invalid keyword/value DSN at offset %d: expected key=value", keyStart)
		}

		for i < n && isSpace(dsn[i]) {
			i++
		}
		if i >= n || dsn[i] != '=' {
			return nil, fmt.Errorf("opengauss: invalid keyword/value DSN: parameter %q has no =value", key)
		}
		i++ // consume '='
		for i < n && isSpace(dsn[i]) {
			i++
		}

		var value strings.Builder
		if i < n && dsn[i] == '\'' {
			i++
			closed := false
			for i < n {
				switch c := dsn[i]; {
				case c == '\\' && i+1 < n:
					value.WriteByte(dsn[i+1])
					i += 2
				case c == '\'':
					i++
					closed = true
				default:
					value.WriteByte(c)
					i++
				}
				if closed {
					break
				}
			}
			if !closed {
				return nil, fmt.Errorf("opengauss: unterminated quote in value for parameter %q", key)
			}
		} else {
			for i < n && !isSpace(dsn[i]) {
				if dsn[i] == '\\' && i+1 < n {
					value.WriteByte(dsn[i+1])
					i += 2
					continue
				}
				value.WriteByte(dsn[i])
				i++
			}
		}
		values[key] = value.String()
	}
	return values, nil
}

// parseURLDSN parses a postgres://, postgresql:// or opengauss:// URL with a
// single host.
func parseURLDSN(dsn string) (map[string]string, error) {
	schemeEnd := strings.Index(dsn, "://")
	if schemeEnd <= 0 {
		return nil, errors.New("opengauss: invalid URL DSN: missing scheme")
	}
	// Reject multi-host authorities before url.Parse sees them, because a
	// comma-separated host is not a valid single authority.
	rest := dsn[schemeEnd+3:]
	authority := rest
	if end := strings.IndexAny(rest, "/?#"); end >= 0 {
		authority = rest[:end]
	}
	if strings.Contains(authority, ",") {
		return nil, errors.New("opengauss: multi-host DSNs are not supported yet: use a single host and port")
	}

	u, err := url.Parse(dsn)
	if err != nil {
		// url.Error embeds the raw DSN, which may contain the password.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return nil, fmt.Errorf("opengauss: invalid URL DSN: %w", err)
	}
	switch u.Scheme {
	case "postgres", "postgresql", "opengauss":
	default:
		return nil, fmt.Errorf("opengauss: unsupported DSN scheme %q: use postgres:// or opengauss://", u.Scheme)
	}

	values := make(map[string]string)
	if host := u.Hostname(); host != "" {
		values["host"] = host
	}
	if port := u.Port(); port != "" {
		values["port"] = port
	}
	if u.User != nil {
		values["user"] = u.User.Username()
		if password, ok := u.User.Password(); ok {
			values["password"] = password
		}
	}
	if dbname := strings.TrimPrefix(u.Path, "/"); dbname != "" {
		if strings.Contains(dbname, "/") {
			return nil, fmt.Errorf("opengauss: invalid URL DSN: database path %q contains more than one segment", u.Path)
		}
		values["dbname"] = dbname
	}
	queryValues, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		// Do not echo the raw query: it may carry a password.
		return nil, errors.New("opengauss: invalid URL DSN: the query string is not valid percent-encoded form data")
	}
	for key, vs := range queryValues {
		key = strings.ToLower(key)
		if len(vs) > 1 {
			return nil, fmt.Errorf("opengauss: connection parameter %q is specified more than once", key)
		}
		if _, duplicate := values[key]; duplicate {
			return nil, fmt.Errorf("opengauss: connection parameter %q is specified more than once", key)
		}
		values[key] = vs[0]
	}
	return values, nil
}

// newConfig validates the parsed parameter set. Explicit settings are
// authoritative; only host, port, database name and sslmode have defaults.
func newConfig(values map[string]string) (*Config, error) {
	cfg := &Config{
		Host:    "localhost",
		Port:    defaultPort,
		SSLMode: SSLModePrefer,
	}
	for key, value := range values {
		if strings.IndexByte(value, 0) >= 0 {
			return nil, fmt.Errorf("opengauss: connection parameter %q contains a NUL byte", key)
		}
		switch key {
		case "host":
			if value == "" {
				return nil, errors.New("opengauss: host must not be empty")
			}
			cfg.Host = value
		case "port":
			port, err := strconv.Atoi(value)
			if err != nil || port < 1 || port > 65535 {
				return nil, fmt.Errorf("opengauss: invalid port %q: must be an integer between 1 and 65535", value)
			}
			cfg.Port = port
		case "user":
			cfg.User = value
		case "password":
			cfg.Password = value
		case "dbname", "database":
			if cfg.Database != "" && cfg.Database != value {
				return nil, errors.New("opengauss: dbname and database are both specified with different values")
			}
			cfg.Database = value
		case "sslmode":
			mode := SSLMode(value)
			if !mode.valid() {
				return nil, fmt.Errorf("opengauss: unsupported sslmode %q: use disable, allow, prefer, require, verify-ca or verify-full", value)
			}
			cfg.SSLMode = mode
		case "sslrootcert":
			cfg.SSLRootCert = value
		case "sslcert":
			cfg.SSLClientCert = value
		case "sslkey":
			cfg.SSLClientKey = value
		case "sslpassword":
			if value != "" {
				return nil, errors.New("opengauss: sslpassword is not implemented: the driver supports only unencrypted sslkey files")
			}
		case "connect_timeout":
			seconds, err := strconv.Atoi(value)
			if err != nil || seconds < 0 {
				return nil, fmt.Errorf("opengauss: invalid connect_timeout %q: must be a non-negative number of seconds", value)
			}
			cfg.ConnectTimeout = time.Duration(seconds) * time.Second
		default:
			return nil, fmt.Errorf("opengauss: unsupported connection parameter %q: it is not implemented yet and is rejected instead of being ignored", key)
		}
	}
	if cfg.User == "" {
		return nil, errors.New("opengauss: missing the required user parameter: set user in the DSN, PGUSER in the environment or a service file")
	}
	if (cfg.SSLClientCert == "") != (cfg.SSLClientKey == "") && cfg.SSLMode != SSLModeDisable {
		return nil, errors.New("opengauss: sslcert and sslkey must be specified together")
	}
	if cfg.Database == "" {
		cfg.Database = cfg.User
	}
	return cfg, nil
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

// isScheme reports whether prefix is an RFC 3986 scheme name. It keeps a
// keyword/value DSN whose value contains "://" from being parsed as a URL.
func isScheme(prefix string) bool {
	if prefix == "" {
		return false
	}
	for i := 0; i < len(prefix); i++ {
		c := prefix[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case i > 0 && (c >= '0' && c <= '9' || c == '+' || c == '-' || c == '.'):
		default:
			return false
		}
	}
	return true
}

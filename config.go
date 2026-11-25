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

// Target is one TCP endpoint. A connector tries its targets in order.
type Target struct {
	Host string
	Port int
}

// String formats the target as host:port for errors and logs.
func (t Target) String() string {
	return net.JoinHostPort(t.Host, strconv.Itoa(t.Port))
}

// TargetSessionAttrs selects which servers a connection attempt may use when a
// host list names several endpoints.
type TargetSessionAttrs string

const (
	TargetSessionAttrsAny           TargetSessionAttrs = "any"
	TargetSessionAttrsReadWrite     TargetSessionAttrs = "read-write"
	TargetSessionAttrsReadOnly      TargetSessionAttrs = "read-only"
	TargetSessionAttrsPrimary       TargetSessionAttrs = "primary"
	TargetSessionAttrsStandby       TargetSessionAttrs = "standby"
	TargetSessionAttrsPreferStandby TargetSessionAttrs = "prefer-standby"
)

func (a TargetSessionAttrs) valid() bool {
	switch a {
	case TargetSessionAttrsAny, TargetSessionAttrsReadWrite, TargetSessionAttrsReadOnly,
		TargetSessionAttrsPrimary, TargetSessionAttrsStandby, TargetSessionAttrsPreferStandby:
		return true
	}
	return false
}

// Config is the immutable set of connection settings parsed from a DSN.
// A Connector shares one Config between every physical connection it opens.
type Config struct {
	// Host and Port are the first entry of Targets, kept for the common
	// single-endpoint case.
	Host           string
	Port           int
	Targets        []Target
	User           string
	Password       string
	Database       string
	SSLMode        SSLMode
	SSLRootCert    string
	SSLClientCert  string
	SSLClientKey   string
	ConnectTimeout time.Duration

	// TargetSessionAttrs restricts which server roles a connection may use. The
	// zero value means "any", which skips the role probe entirely.
	TargetSessionAttrs TargetSessionAttrs

	// passfile and passfileExplicit remember where a missing password may be
	// resolved, so each target can be matched against its own entry.
	passfile         string
	passfileExplicit bool

	// passwordSet records that the application supplied the password, which
	// therefore applies to every target instead of being looked up per host.
	passwordSet bool

	// tlsConfig is prepared once by NewConnector, so certificate files are
	// validated before any connection attempt. It is nil for sslmode=disable.
	tlsConfig *tls.Config
}

// String returns a description of the endpoints with the password removed. It
// is safe to include in logs and errors.
func (c *Config) String() string {
	endpoints := make([]string, len(c.Targets))
	for i, target := range c.Targets {
		endpoints[i] = target.String()
	}
	return fmt.Sprintf("opengauss://%s@%s/%s?sslmode=%s",
		c.User, strings.Join(endpoints, ","), c.Database, c.SSLMode)
}

// passwordForTarget resolves the password to use against one endpoint. An
// explicitly supplied password applies to every target; otherwise the passfile
// is resolved per host/port pair, as libpq does.
func (c *Config) passwordForTarget(target Target) (string, error) {
	if c.passwordSet {
		return c.Password, nil
	}
	return lookupPassfile(c.passfile, c.passfileExplicit, target, c.Database, c.User)
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
	cfg.passfile = passfile
	cfg.passfileExplicit = passfileSet && passfile != ""
	if !cfg.passwordSet {
		// Resolve the first target now so local configuration errors surface
		// at construction; each target still resolves its own entry.
		password, err := lookupPassfile(cfg.passfile, cfg.passfileExplicit, cfg.Targets[0], cfg.Database, cfg.User)
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
	{"PGTARGETSESSIONATTRS", "target_session_attrs"},
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
			// Do not echo the token: a malformed DSN may carry a password.
			return nil, errors.New("opengauss: invalid keyword/value DSN: expected key=value")
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

// parseURLDSN parses a postgres://, postgresql:// or opengauss:// URL. The
// authority may hold a comma-separated host list, which net/url cannot parse,
// so the authority is split here and the remainder is parsed by the standard
// library.
func parseURLDSN(dsn string) (map[string]string, error) {
	schemeEnd := strings.Index(dsn, "://")
	if schemeEnd <= 0 {
		return nil, errors.New("opengauss: invalid URL DSN: missing scheme")
	}
	scheme := strings.ToLower(dsn[:schemeEnd])
	switch scheme {
	case "postgres", "postgresql", "opengauss":
	default:
		return nil, fmt.Errorf("opengauss: unsupported DSN scheme %q: use postgres:// or opengauss://", scheme)
	}

	rest := dsn[schemeEnd+3:]
	authority, tail := rest, ""
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		authority, tail = rest[:i], rest[i:]
	}
	userinfo, hostList := "", authority
	if i := strings.LastIndex(authority, "@"); i >= 0 {
		userinfo, hostList = authority[:i], authority[i+1:]
	}

	values := make(map[string]string)
	if userinfo != "" {
		user, password, hasPassword, err := parseURLUserinfo(userinfo)
		if err != nil {
			return nil, err
		}
		values["user"] = user
		if hasPassword {
			values["password"] = password
		}
	}
	if hostList != "" {
		hosts, ports, anyPort, err := parseURLHostList(hostList)
		if err != nil {
			return nil, err
		}
		values["host"] = strings.Join(hosts, ",")
		// Without an explicit port the default applies, and a port given in
		// the query string must still be accepted.
		if anyPort {
			values["port"] = strings.Join(ports, ",")
		}
	}

	u, err := url.Parse("opengauss://host" + tail)
	if err != nil {
		return nil, errors.New("opengauss: invalid URL DSN: the path or query is malformed")
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

// parseURLUserinfo splits and decodes "user[:password]". Error messages never
// echo the decoded text, which may contain a password.
func parseURLUserinfo(userinfo string) (user, password string, hasPassword bool, err error) {
	userPart, passwordPart, hasPassword := strings.Cut(userinfo, ":")
	user, err = url.PathUnescape(userPart)
	if err != nil {
		return "", "", false, errors.New("opengauss: invalid URL DSN: the user is not valid percent-encoded text")
	}
	if !hasPassword {
		return user, "", false, nil
	}
	password, err = url.PathUnescape(passwordPart)
	if err != nil {
		return "", "", false, errors.New("opengauss: invalid URL DSN: the password is not valid percent-encoded text")
	}
	return user, password, true, nil
}

// parseURLHostList splits a URL host list such as "db1:5432,db2". Hosts
// without a port receive the default port; anyPort reports whether the list
// named a port at all.
func parseURLHostList(raw string) (hosts, ports []string, anyPort bool, err error) {
	endpoints := strings.Split(raw, ",")
	hosts = make([]string, 0, len(endpoints))
	ports = make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		host, port, hasPort, err := splitEndpoint(endpoint)
		if err != nil {
			return nil, nil, false, fmt.Errorf("opengauss: invalid URL DSN: %w", err)
		}
		if host == "" {
			return nil, nil, false, errors.New("opengauss: invalid URL DSN: empty host in the host list")
		}
		unescaped, err := url.PathUnescape(host)
		if err != nil {
			return nil, nil, false, fmt.Errorf("opengauss: invalid URL DSN: host %q is not valid percent-encoded text", host)
		}
		host = unescaped
		if hasPort {
			anyPort = true
		} else {
			port = strconv.Itoa(defaultPort)
		}
		hosts = append(hosts, host)
		ports = append(ports, port)
	}
	return hosts, ports, anyPort, nil
}

// splitEndpoint splits one host[:port] entry, understanding [IPv6] brackets.
func splitEndpoint(endpoint string) (host, port string, hasPort bool, err error) {
	if strings.HasPrefix(endpoint, "[") {
		end := strings.Index(endpoint, "]")
		if end < 0 {
			return "", "", false, fmt.Errorf("unterminated IPv6 address %q", endpoint)
		}
		host = endpoint[1:end]
		rest := endpoint[end+1:]
		if rest == "" {
			return host, "", false, nil
		}
		if !strings.HasPrefix(rest, ":") {
			return "", "", false, fmt.Errorf("invalid host %q", endpoint)
		}
		return host, rest[1:], true, nil
	}
	if i := strings.LastIndex(endpoint, ":"); i >= 0 {
		return endpoint[:i], endpoint[i+1:], true, nil
	}
	return endpoint, "", false, nil
}

// newConfig validates the parsed parameter set. Explicit settings are
// authoritative; only host, port, database name and sslmode have defaults.
func newConfig(values map[string]string) (*Config, error) {
	cfg := &Config{
		SSLMode: SSLModePrefer,
	}
	for key, value := range values {
		if strings.IndexByte(value, 0) >= 0 {
			return nil, fmt.Errorf("opengauss: connection parameter %q contains a NUL byte", key)
		}
		switch key {
		case "host", "port":
			// Expanded into Targets after the loop.
		case "user":
			cfg.User = value
		case "password":
			cfg.Password = value
			// An explicit empty password still allows the passfile to supply
			// one, as libpq does; a non-empty value applies to every target.
			cfg.passwordSet = value != ""
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
		case "target_session_attrs":
			attrs := TargetSessionAttrs(value)
			if !attrs.valid() {
				return nil, fmt.Errorf("opengauss: unsupported target_session_attrs %q: use any, read-write, read-only, primary, standby or prefer-standby", value)
			}
			cfg.TargetSessionAttrs = attrs
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
	targets, err := parseTargets(values["host"], presence(values, "host"), values["port"], presence(values, "port"))
	if err != nil {
		return nil, err
	}
	cfg.Targets = targets
	cfg.Host, cfg.Port = targets[0].Host, targets[0].Port
	if (cfg.SSLClientCert == "") != (cfg.SSLClientKey == "") && cfg.SSLMode != SSLModeDisable {
		return nil, errors.New("opengauss: sslcert and sslkey must be specified together")
	}
	if cfg.Database == "" {
		cfg.Database = cfg.User
	}
	return cfg, nil
}

// presence reports whether values contains key, which distinguishes an
// omitted parameter from an explicitly empty one.
func presence(values map[string]string, key string) bool {
	_, ok := values[key]
	return ok
}

// parseTargets expands the host and port lists into endpoints. One port
// applies to every host; otherwise the lists must have the same length.
func parseTargets(hostValue string, hostSet bool, portValue string, portSet bool) ([]Target, error) {
	hosts := []string{"localhost"}
	if hostSet {
		if hostValue == "" {
			return nil, errors.New("opengauss: host must not be empty")
		}
		hosts = strings.Split(hostValue, ",")
		for _, host := range hosts {
			if host == "" {
				return nil, errors.New("opengauss: the host list contains an empty entry")
			}
			if strings.ContainsAny(host, " \t\n\r\v\f") {
				return nil, fmt.Errorf("opengauss: invalid host %q: it contains whitespace", host)
			}
		}
	}
	ports := make([]int, len(hosts))
	for i := range ports {
		ports[i] = defaultPort
	}
	if portSet {
		parts := strings.Split(portValue, ",")
		parsed := make([]int, len(parts))
		for i, part := range parts {
			port, err := strconv.Atoi(part)
			if err != nil || port < 1 || port > 65535 {
				return nil, fmt.Errorf("opengauss: invalid port %q: must be an integer between 1 and 65535", part)
			}
			parsed[i] = port
		}
		switch len(parsed) {
		case 1:
			for i := range ports {
				ports[i] = parsed[0]
			}
		case len(hosts):
			copy(ports, parsed)
		default:
			return nil, fmt.Errorf("opengauss: the port list has %d entries but the host list has %d", len(parsed), len(hosts))
		}
	}
	targets := make([]Target, len(hosts))
	for i := range hosts {
		targets[i] = Target{Host: hosts[i], Port: ports[i]}
	}
	return targets, nil
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

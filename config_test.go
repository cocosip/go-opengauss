package opengauss

import (
	"context"
	"database/sql"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestParseConfigUnixSocket(t *testing.T) {
	clearPGEnvironment(t)
	cfg, err := parseConfig("host=/var/run/postgresql user=alice dbname=app")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	target := cfg.Targets[0]
	if target.String() != "/var/run/postgresql" {
		t.Errorf("target = %q, want the socket directory", target.String())
	}
	if target.network() != "unix" {
		t.Errorf("network = %q, want unix", target.network())
	}
	if want := filepath.Join("/var/run/postgresql", ".s.PGSQL.5432"); target.dialAddress() != want {
		t.Errorf("dial address = %q, want %q", target.dialAddress(), want)
	}

	cfg, err = parseConfig("host=/tmp port=6432 user=alice")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if want := filepath.Join("/tmp", ".s.PGSQL.6432"); cfg.Targets[0].dialAddress() != want {
		t.Errorf("dial address = %q, want %q", cfg.Targets[0].dialAddress(), want)
	}
}

func TestDialUnixSocket(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-domain sockets are not available on Windows")
	}
	dir, err := os.MkdirTemp("", "opengauss-socket")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	listener, err := net.Listen("unix", filepath.Join(dir, ".s.PGSQL.5432"))
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer func() { _ = listener.Close() }()

	accepted := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		close(accepted)
		_ = conn.Close()
	}()

	var gotNetwork, gotAddress string
	conn, err := dial(testContext(t), Target{Host: dir, Port: 5432}, func(ctx context.Context, network, address string) (net.Conn, error) {
		gotNetwork, gotAddress = network, address
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, address)
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.Close()
	if gotNetwork != "unix" || gotAddress != filepath.Join(dir, ".s.PGSQL.5432") {
		t.Errorf("dial target = %s %s, want unix %s", gotNetwork, gotAddress, filepath.Join(dir, ".s.PGSQL.5432"))
	}
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("the Unix socket server did not accept the connection")
	}
}

func TestTLSServerName(t *testing.T) {
	if got := tlsServerName(Target{Host: "/var/run/postgresql", Port: 5432}); got != "" {
		t.Errorf("Unix socket server name = %q, want an empty name", got)
	}
	if got := tlsServerName(Target{Host: "db.example.com", Port: 5432}); got != "db.example.com" {
		t.Errorf("TCP server name = %q, want db.example.com", got)
	}
}

func TestParseConfigKeywordValue(t *testing.T) {
	clearPGEnvironment(t)
	dsn := "host=db.example.com port=15432 user=alice password='s3cr et' dbname=app sslmode=require sslrootcert=/etc/ssl/ca.pem connect_timeout=7"
	cfg, err := parseConfig(dsn)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Host != "db.example.com" {
		t.Errorf("Host = %q, want db.example.com", cfg.Host)
	}
	if cfg.Port != 15432 {
		t.Errorf("Port = %d, want 15432", cfg.Port)
	}
	if cfg.User != "alice" {
		t.Errorf("User = %q, want alice", cfg.User)
	}
	if cfg.Password != "s3cr et" {
		t.Errorf("Password = %q, want %q", cfg.Password, "s3cr et")
	}
	if cfg.Database != "app" {
		t.Errorf("Database = %q, want app", cfg.Database)
	}
	if cfg.SSLMode != SSLModeRequire {
		t.Errorf("SSLMode = %q, want require", cfg.SSLMode)
	}
	if cfg.SSLRootCert != "/etc/ssl/ca.pem" {
		t.Errorf("SSLRootCert = %q", cfg.SSLRootCert)
	}
	if cfg.ConnectTimeout != 7*time.Second {
		t.Errorf("ConnectTimeout = %v, want 7s", cfg.ConnectTimeout)
	}
}

func TestParseConfigEscapes(t *testing.T) {
	clearPGEnvironment(t)
	cfg, err := parseConfig(`user=alice password='pa\'ss' dbname=a\ b`)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Password != "pa'ss" {
		t.Errorf("Password = %q, want %q", cfg.Password, "pa'ss")
	}
	if cfg.Database != "a b" {
		t.Errorf("Database = %q, want %q", cfg.Database, "a b")
	}
}

func TestParseConfigValueContainingURLScheme(t *testing.T) {
	clearPGEnvironment(t)
	cfg, err := parseConfig("user=alice password=x://y dbname=app")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Password != "x://y" {
		t.Errorf("Password = %q, want x://y", cfg.Password)
	}
}

func TestParseConfigURL(t *testing.T) {
	clearPGEnvironment(t)
	dsn := "postgres://alice:p%40ss@db.example.com:15432/app?sslmode=verify-full&sslrootcert=ca.pem&connect_timeout=5"
	cfg, err := parseConfig(dsn)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Host != "db.example.com" || cfg.Port != 15432 {
		t.Errorf("endpoint = %s:%d", cfg.Host, cfg.Port)
	}
	if cfg.User != "alice" || cfg.Password != "p@ss" {
		t.Errorf("credentials = %q/%q", cfg.User, cfg.Password)
	}
	if cfg.Database != "app" {
		t.Errorf("Database = %q", cfg.Database)
	}
	if cfg.SSLMode != SSLModeVerifyFull {
		t.Errorf("SSLMode = %q", cfg.SSLMode)
	}
	if cfg.ConnectTimeout != 5*time.Second {
		t.Errorf("ConnectTimeout = %v", cfg.ConnectTimeout)
	}
}

func TestParseConfigDefaults(t *testing.T) {
	clearPGEnvironment(t)
	cfg, err := parseConfig("user=alice")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Host != "localhost" {
		t.Errorf("Host = %q, want localhost", cfg.Host)
	}
	if cfg.Port != 5432 {
		t.Errorf("Port = %d, want 5432", cfg.Port)
	}
	if cfg.Database != "alice" {
		t.Errorf("Database = %q, want user name", cfg.Database)
	}
	if cfg.SSLMode != SSLModePrefer {
		t.Errorf("SSLMode = %q, want prefer", cfg.SSLMode)
	}
}

func TestParseConfigLastValueWins(t *testing.T) {
	clearPGEnvironment(t)
	cfg, err := parseConfig("user=alice host=first host=second")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Host != "second" {
		t.Errorf("Host = %q, want second", cfg.Host)
	}
}

func TestParseConfigMultiHostKeyword(t *testing.T) {
	clearPGEnvironment(t)
	cfg, err := parseConfig("user=alice host=db1,db2 port=5432,6432")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if len(cfg.Targets) != 2 {
		t.Fatalf("Targets = %v, want two endpoints", cfg.Targets)
	}
	if cfg.Targets[0] != (Target{Host: "db1", Port: 5432}) || cfg.Targets[1] != (Target{Host: "db2", Port: 6432}) {
		t.Errorf("Targets = %v", cfg.Targets)
	}
	if cfg.Host != "db1" || cfg.Port != 5432 {
		t.Errorf("Host/Port = %s:%d, want the first endpoint", cfg.Host, cfg.Port)
	}
}

func TestParseConfigSinglePortAppliesToAllHosts(t *testing.T) {
	clearPGEnvironment(t)
	cfg, err := parseConfig("user=alice host=db1,db2 port=6000")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if len(cfg.Targets) != 2 || cfg.Targets[0].Port != 6000 || cfg.Targets[1].Port != 6000 {
		t.Errorf("Targets = %v, want port 6000 for both hosts", cfg.Targets)
	}
}

func TestParseConfigPortCountMismatchRejects(t *testing.T) {
	clearPGEnvironment(t)
	if _, err := parseConfig("user=alice host=db1,db2 port=5432,5433,5434"); err == nil ||
		!strings.Contains(err.Error(), "port") {
		t.Errorf("error = %v, want a port-count error", err)
	}
}

func TestParseConfigMultiHostURL(t *testing.T) {
	clearPGEnvironment(t)
	cfg, err := parseConfig("postgres://alice:secret@db1:5432,db2:6432/app?sslmode=disable")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if len(cfg.Targets) != 2 || cfg.Targets[0].Host != "db1" || cfg.Targets[0].Port != 5432 ||
		cfg.Targets[1].Host != "db2" || cfg.Targets[1].Port != 6432 {
		t.Errorf("Targets = %v", cfg.Targets)
	}
}

func TestParseConfigMultiHostURLUsesDefaultPort(t *testing.T) {
	clearPGEnvironment(t)
	cfg, err := parseConfig("postgres://alice@db1,db2:6432/app")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if len(cfg.Targets) != 2 || cfg.Targets[0].Port != 5432 || cfg.Targets[1].Port != 6432 {
		t.Errorf("Targets = %v, want the default port for the host without one", cfg.Targets)
	}
}

func TestParseConfigRejectsEmptyHostListEntry(t *testing.T) {
	clearPGEnvironment(t)
	if _, err := parseConfig("user=alice host=db1,,db2"); err == nil ||
		!strings.Contains(err.Error(), "host") {
		t.Errorf("error = %v, want a host-list error", err)
	}
}

func TestConfigStringUsesFirstTarget(t *testing.T) {
	clearPGEnvironment(t)
	cfg, err := parseConfig("user=alice host=db1,db2 port=5432,6432 dbname=app")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if got := cfg.String(); !strings.Contains(got, "db1:5432") {
		t.Errorf("String() = %q, want the first endpoint", got)
	}
}

func TestParseConfigRejectsEmptyHostAndPort(t *testing.T) {
	clearPGEnvironment(t)
	for _, dsn := range []string{"user=alice host=", "user=alice port=", "user=alice host=db1, port=5432"} {
		if _, err := parseConfig(dsn); err == nil {
			t.Errorf("parseConfig(%q) accepted an empty host or port", dsn)
		}
	}
}

func TestParseConfigURLHostListVariants(t *testing.T) {
	clearPGEnvironment(t)
	tests := []struct {
		name string
		dsn  string
		want []Target
	}{
		{
			"mixed explicit and default port",
			"postgres://alice@h1:5432,h2/db",
			[]Target{{"h1", 5432}, {"h2", 5432}},
		},
		{
			"explicit ports",
			"postgres://alice@h1:5432,h2:6432/db",
			[]Target{{"h1", 5432}, {"h2", 6432}},
		},
		{
			"bracketed IPv6",
			"postgres://alice@[::1]:5432,[2001:db8::2]:6432/db",
			[]Target{{"::1", 5432}, {"2001:db8::2", 6432}},
		},
		{
			"bracketed IPv6 without port",
			"postgres://alice@[::1],[::2]/db",
			[]Target{{"::1", 5432}, {"::2", 5432}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfig(tc.dsn)
			if err != nil {
				t.Fatalf("parseConfig: %v", err)
			}
			if len(cfg.Targets) != len(tc.want) {
				t.Fatalf("Targets = %v, want %v", cfg.Targets, tc.want)
			}
			for i := range tc.want {
				if cfg.Targets[i] != tc.want[i] {
					t.Errorf("Targets[%d] = %v, want %v", i, cfg.Targets[i], tc.want[i])
				}
			}
		})
	}
}

func TestParseConfigURLQueryPortWithoutAuthorityPort(t *testing.T) {
	clearPGEnvironment(t)
	cfg, err := parseConfig("postgres://alice@h1,h2/db?port=6432")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if len(cfg.Targets) != 2 || cfg.Targets[0].Port != 6432 || cfg.Targets[1].Port != 6432 {
		t.Errorf("Targets = %v, want port 6432 for both hosts", cfg.Targets)
	}
}

func TestParseConfigURLUserinfoEscapes(t *testing.T) {
	clearPGEnvironment(t)
	cfg, err := parseConfig("postgres://ali%3Ace:p%40ss%2Fword@db/app")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.User != "ali:ce" || cfg.Password != "p@ss/word" {
		t.Errorf("credentials = %q/%q", cfg.User, cfg.Password)
	}
}

func TestParseConfigTargetSessionAttrs(t *testing.T) {
	clearPGEnvironment(t)
	tests := []struct {
		dsn  string
		want TargetSessionAttrs
	}{
		{"host=db user=alice target_session_attrs=read-write", TargetSessionAttrsReadWrite},
		{"host=db user=alice target_session_attrs=read-only", TargetSessionAttrsReadOnly},
		{"host=db user=alice target_session_attrs=primary", TargetSessionAttrsPrimary},
		{"host=db user=alice target_session_attrs=standby", TargetSessionAttrsStandby},
		{"host=db user=alice target_session_attrs=prefer-standby", TargetSessionAttrsPreferStandby},
		{"host=db user=alice target_session_attrs=any", TargetSessionAttrsAny},
		{"host=db user=alice", ""},
		{"postgres://alice@db/app?target_session_attrs=standby", TargetSessionAttrsStandby},
	}
	for _, tc := range tests {
		cfg, err := parseConfig(tc.dsn)
		if err != nil {
			t.Errorf("parseConfig(%q): %v", tc.dsn, err)
			continue
		}
		if cfg.TargetSessionAttrs != tc.want {
			t.Errorf("parseConfig(%q) TargetSessionAttrs = %q, want %q", tc.dsn, cfg.TargetSessionAttrs, tc.want)
		}
	}
}

func TestTargetSessionAttrsPrecedence(t *testing.T) {
	clearPGEnvironment(t)
	t.Setenv("PGTARGETSESSIONATTRS", "standby")

	cfg, err := parseConfig("host=db user=alice")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.TargetSessionAttrs != TargetSessionAttrsStandby {
		t.Errorf("TargetSessionAttrs = %q, want the environment value", cfg.TargetSessionAttrs)
	}

	cfg, err = parseConfig("host=db user=alice target_session_attrs=any")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.TargetSessionAttrs != TargetSessionAttrsAny {
		t.Errorf("TargetSessionAttrs = %q, want the DSN value", cfg.TargetSessionAttrs)
	}
}

func TestParseConfigRejects(t *testing.T) {
	clearPGEnvironment(t)
	tests := []struct {
		name    string
		dsn     string
		wantSub string
	}{
		{"empty without environment", "", "user"},
		{"missing user", "host=localhost", "user"},
		{"port not a number", "user=alice port=abc", "port"},
		{"port zero", "user=alice port=0", "port"},
		{"port too large", "user=alice port=65536", "port"},
		{"negative port", "user=alice port=-1", "port"},
		{"bad sslmode", "user=alice sslmode=bogus", "sslmode"},
		{"unsupported option", "user=alice sslcrl=revoked.pem", "sslcrl"},
		{"bad target_session_attrs", "user=alice target_session_attrs=leader", "target_session_attrs"},
		{"bad connect_timeout", "user=alice connect_timeout=soon", "connect_timeout"},
		{"negative connect_timeout", "user=alice connect_timeout=-3", "connect_timeout"},
		{"unknown scheme", "mysql://alice@localhost/db", "scheme"},
		{"dbname twice", "postgres://alice@localhost/db?dbname=other", "dbname"},
		{"dbname conflict", "user=alice dbname=a database=b", "dbname"},
		{"unknown URL parameter", "postgres://alice@localhost/db?gssencmode=prefer", "gssencmode"},
		{"unterminated quote", "user=alice password='secret", "quote"},
		{"missing equals", "user", "keyword/value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseConfig(tt.dsn)
			if err == nil {
				t.Fatalf("parseConfig(%q) = nil error", tt.dsn)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not mention %q", err, tt.wantSub)
			}
		})
	}
}

func TestConfigStringRedactsPassword(t *testing.T) {
	clearPGEnvironment(t)
	cfg, err := parseConfig("user=alice password=topsecret host=localhost dbname=app")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	redacted := cfg.String()
	if strings.Contains(redacted, "topsecret") {
		t.Errorf("redacted output %q still contains the password", redacted)
	}
	if !strings.Contains(redacted, "alice") {
		t.Errorf("redacted output %q lost the user name", redacted)
	}
}

func TestParseConfigErrorDoesNotLeakPassword(t *testing.T) {
	clearPGEnvironment(t)
	_, err := parseConfig("user=alice password=topsecret port=notaport")
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "topsecret") {
		t.Errorf("error %q leaks the password", err)
	}
}

func TestURLParseErrorsDoNotLeakPassword(t *testing.T) {
	clearPGEnvironment(t)
	// url.Error would normally embed the whole DSN, password included.
	dsns := []string{
		"postgres://alice:topsecret@host:badport/db",
		"postgres://alice:topsecret@ho st/db",
		"postgres://alice:topsecret@[::1/db",
	}
	for _, dsn := range dsns {
		if _, err := NewConnector(dsn); err == nil {
			t.Errorf("NewConnector(%q) accepted an invalid URL", dsn)
		} else if strings.Contains(err.Error(), "topsecret") {
			t.Errorf("NewConnector error leaks the password: %v", err)
		}
		if db, err := sql.Open("opengauss", dsn); err == nil {
			_ = db.Close()
			t.Errorf("sql.Open(%q) accepted an invalid URL", dsn)
		} else if strings.Contains(err.Error(), "topsecret") {
			t.Errorf("sql.Open error leaks the password: %v", err)
		}
	}
}

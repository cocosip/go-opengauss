package opengauss

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestParseConfigKeywordValue(t *testing.T) {
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
	cfg, err := parseConfig("user=alice password=x://y dbname=app")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Password != "x://y" {
		t.Errorf("Password = %q, want x://y", cfg.Password)
	}
}

func TestParseConfigURL(t *testing.T) {
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
	cfg, err := parseConfig("user=alice host=first host=second")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Host != "second" {
		t.Errorf("Host = %q, want second", cfg.Host)
	}
}

func TestParseConfigRejects(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantSub string
	}{
		{"empty", "", "empty DSN"},
		{"missing user", "host=localhost", "user"},
		{"port not a number", "user=alice port=abc", "port"},
		{"port zero", "user=alice port=0", "port"},
		{"port too large", "user=alice port=65536", "port"},
		{"negative port", "user=alice port=-1", "port"},
		{"bad sslmode", "user=alice sslmode=bogus", "sslmode"},
		{"unsupported option", "user=alice target_session_attrs=read-write", "target_session_attrs"},
		{"bad connect_timeout", "user=alice connect_timeout=soon", "connect_timeout"},
		{"negative connect_timeout", "user=alice connect_timeout=-3", "connect_timeout"},
		{"unknown scheme", "mysql://alice@localhost/db", "scheme"},
		{"multi host URL", "postgres://alice@h1:5432,h2:5432/db", "multi-host"},
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
	_, err := parseConfig("user=alice password=topsecret port=notaport")
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "topsecret") {
		t.Errorf("error %q leaks the password", err)
	}
}

func TestURLParseErrorsDoNotLeakPassword(t *testing.T) {
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

package opengauss

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// clearPGEnvironment makes configuration tests independent of the developer's
// shell environment.
func clearPGEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD", "PGSSLMODE",
		"PGSSLROOTCERT", "PGSSLCERT", "PGSSLKEY", "PGCONNECT_TIMEOUT", "PGTARGETSESSIONATTRS",
		"PGAPPNAME", "PGOPTIONS",
		"PGSERVICE", "PGSERVICEFILE", "PGPASSFILE",
	} {
		t.Setenv(name, "")
	}
}

func writeConfigFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func TestEmptyDSNUsesEnvironment(t *testing.T) {
	clearPGEnvironment(t)
	t.Setenv("PGHOST", "db.internal")
	t.Setenv("PGPORT", "6432")
	t.Setenv("PGUSER", "alice")
	t.Setenv("PGDATABASE", "app")
	t.Setenv("PGPASSWORD", "envsecret")
	t.Setenv("PGSSLMODE", "require")
	t.Setenv("PGCONNECT_TIMEOUT", "7")

	cfg, err := parseConfig("")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Host != "db.internal" || cfg.Port != 6432 || cfg.User != "alice" ||
		cfg.Database != "app" || cfg.Password != "envsecret" ||
		cfg.SSLMode != SSLModeRequire || cfg.ConnectTimeout != 7*time.Second {
		t.Errorf("config from environment = %+v", cfg)
	}
}

func TestEmptyDSNWithoutUserFails(t *testing.T) {
	clearPGEnvironment(t)
	if _, err := parseConfig(""); err == nil || !strings.Contains(err.Error(), "user") {
		t.Fatalf("parseConfig(\"\") error = %v, want a missing-user error", err)
	}
}

func TestExplicitDSNOverridesEnvironment(t *testing.T) {
	clearPGEnvironment(t)
	t.Setenv("PGHOST", "envhost")
	t.Setenv("PGPORT", "6000")
	t.Setenv("PGUSER", "envuser")

	cfg, err := parseConfig("host=explicit port=5000 user=bob dbname=app")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Host != "explicit" || cfg.Port != 5000 || cfg.User != "bob" {
		t.Errorf("explicit values did not win: %+v", cfg)
	}
}

func TestServiceFileOverridesEnvironment(t *testing.T) {
	clearPGEnvironment(t)
	serviceFile := writeConfigFile(t, "pg_service.conf", `[svc]
host=svchost
port=5433
user=svcuser
dbname=svcdb
`)
	t.Setenv("PGSERVICEFILE", serviceFile)
	t.Setenv("PGHOST", "envhost")
	t.Setenv("PGPORT", "6000")
	t.Setenv("PGUSER", "envuser")
	t.Setenv("PGDATABASE", "envdb")

	cfg, err := parseConfig("service=svc")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.User != "svcuser" || cfg.Host != "svchost" || cfg.Port != 5433 || cfg.Database != "svcdb" {
		t.Errorf("service values must override the environment, got %+v", cfg)
	}

	// Explicit DSN parameters still win over the service file.
	cfg, err = parseConfig("service=svc host=dsnhost user=dsnuser")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Host != "dsnhost" || cfg.User != "dsnuser" || cfg.Port != 5433 || cfg.Database != "svcdb" {
		t.Errorf("DSN values must override the service file, got %+v", cfg)
	}
}

func TestServiceFileSuppliesMissingParameters(t *testing.T) {
	clearPGEnvironment(t)
	serviceFile := writeConfigFile(t, "pg_service.conf", `# global comment
[other]
host=ignored

[svc]
host=svchost
port=5433
user=svcuser
dbname=svcdb
sslmode=require
`)
	t.Setenv("PGSERVICEFILE", serviceFile)

	cfg, err := parseConfig("service=svc")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Host != "svchost" || cfg.Port != 5433 || cfg.User != "svcuser" ||
		cfg.Database != "svcdb" || cfg.SSLMode != SSLModeRequire {
		t.Errorf("config from service file = %+v", cfg)
	}
}

func TestServiceFileErrors(t *testing.T) {
	clearPGEnvironment(t)

	t.Setenv("PGSERVICEFILE", filepath.Join(t.TempDir(), "missing.conf"))
	if _, err := parseConfig("service=svc"); err == nil || !strings.Contains(err.Error(), "service file") {
		t.Errorf("missing service file error = %v", err)
	}

	t.Setenv("PGSERVICEFILE", writeConfigFile(t, "pg_service.conf", "[other]\nhost=x\n"))
	if _, err := parseConfig("service=svc"); err == nil || !strings.Contains(err.Error(), `service "svc"`) {
		t.Errorf("unknown service error = %v", err)
	}

	t.Setenv("PGSERVICEFILE", writeConfigFile(t, "pg_service.conf", "[svc]\nhost\n"))
	if _, err := parseConfig("service=svc"); err == nil || !strings.Contains(err.Error(), "line") {
		t.Errorf("malformed service file error = %v", err)
	}

	t.Setenv("PGSERVICEFILE", writeConfigFile(t, "pg_service.conf", "[svc]\nsslinline=true\n"))
	if _, err := parseConfig("service=svc"); err == nil || !strings.Contains(err.Error(), "unsupported connection parameter") {
		t.Errorf("unsupported service parameter error = %v", err)
	}
}

func TestStartupParameterPrecedence(t *testing.T) {
	clearPGEnvironment(t)
	serviceFile := writeConfigFile(t, "pg_service.conf", `[svc]
host=svchost
user=svcuser
application_name=svcapp
`)
	t.Setenv("PGSERVICEFILE", serviceFile)
	t.Setenv("PGAPPNAME", "envapp")
	t.Setenv("PGOPTIONS", "-c search_path=env")

	cfg, err := parseConfig("service=svc")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.ApplicationName != "svcapp" {
		t.Errorf("service application_name = %q, want svcapp", cfg.ApplicationName)
	}
	if cfg.Options != "-c search_path=env" {
		t.Errorf("options from PGOPTIONS = %q", cfg.Options)
	}

	cfg, err = parseConfig("service=svc application_name=dsnapp options='-c search_path=dsn'")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.ApplicationName != "dsnapp" || cfg.Options != "-c search_path=dsn" {
		t.Errorf("DSN startup parameters = %q %q, want dsnapp and the DSN options", cfg.ApplicationName, cfg.Options)
	}
}

func TestFallbackApplicationName(t *testing.T) {
	clearPGEnvironment(t)

	cfg, err := parseConfig("host=db user=alice dbname=app fallback_application_name=fallback")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.ApplicationName != "fallback" {
		t.Errorf("application_name = %q, want the fallback", cfg.ApplicationName)
	}

	cfg, err = parseConfig("host=db user=alice dbname=app application_name=explicit fallback_application_name=fallback")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.ApplicationName != "explicit" {
		t.Errorf("application_name = %q, want the explicit value", cfg.ApplicationName)
	}

	cfg, err = parseConfig("host=db user=alice dbname=app application_name='' fallback_application_name=fallback")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.ApplicationName != "fallback" {
		t.Errorf("application_name = %q, want the fallback for an empty explicit value", cfg.ApplicationName)
	}
}

func TestPassfileMatchesUnixSocketAsLocalhost(t *testing.T) {
	clearPGEnvironment(t)
	passfile := writeConfigFile(t, "pgpass", "localhost:5432:app:alice:socketsecret\n")
	t.Setenv("PGPASSFILE", passfile)

	cfg, err := parseConfig("host=/var/run/postgresql user=alice dbname=app")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Password != "socketsecret" {
		t.Errorf("Password = %q, want the passfile entry matched as localhost", cfg.Password)
	}
}

func TestPassfileSuppliesPassword(t *testing.T) {
	clearPGEnvironment(t)
	passfile := writeConfigFile(t, "pgpass", `# host:port:database:user:password
127.0.0.1:5432:app:alice:topsecret
127.0.0.1:5433:*:bob:otherpass
*:5432:app:carol:starpass
127.0.0.1:5432:esc:alice:p\:a\\ss
`)

	cases := []struct {
		name string
		dsn  string
		want string
	}{
		{"exact", "host=127.0.0.1 port=5432 dbname=app user=alice passfile=" + filepath.ToSlash(passfile), "topsecret"},
		{"database wildcard", "host=127.0.0.1 port=5433 dbname=any user=bob passfile=" + filepath.ToSlash(passfile), "otherpass"},
		{"host wildcard", "host=elsewhere port=5432 dbname=app user=carol passfile=" + filepath.ToSlash(passfile), "starpass"},
		{"escaped colon", "host=127.0.0.1 port=5432 dbname=esc user=alice passfile=" + filepath.ToSlash(passfile), `p:a\ss`},
		{"no match", "host=127.0.0.1 port=9999 dbname=app user=alice passfile=" + filepath.ToSlash(passfile), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfig(tc.dsn)
			if err != nil {
				t.Fatalf("parseConfig: %v", err)
			}
			if cfg.Password != tc.want {
				t.Errorf("Password = %q, want %q", cfg.Password, tc.want)
			}
		})
	}
}

func TestPassfileEnvironmentVariable(t *testing.T) {
	clearPGEnvironment(t)
	passfile := writeConfigFile(t, "pgpass", "127.0.0.1:5432:app:alice:envpass\n")
	t.Setenv("PGPASSFILE", passfile)

	cfg, err := parseConfig("host=127.0.0.1 port=5432 dbname=app user=alice")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Password != "envpass" {
		t.Errorf("Password = %q, want the passfile entry", cfg.Password)
	}
}

func TestExplicitPasswordOverridesPassfile(t *testing.T) {
	clearPGEnvironment(t)
	passfile := writeConfigFile(t, "pgpass", "127.0.0.1:5432:app:alice:filepass\n")

	cfg, err := parseConfig("host=127.0.0.1 port=5432 dbname=app user=alice password=dsnpass passfile=" + filepath.ToSlash(passfile))
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Password != "dsnpass" {
		t.Errorf("Password = %q, want the DSN value", cfg.Password)
	}
}

func TestExplicitPassfileMustExist(t *testing.T) {
	clearPGEnvironment(t)
	missing := filepath.Join(t.TempDir(), "missing.pgpass")
	_, err := parseConfig("user=alice passfile=" + filepath.ToSlash(missing))
	if err == nil || !strings.Contains(err.Error(), "passfile") {
		t.Errorf("error = %v, want a passfile error", err)
	}
}

func TestPassfileInServiceFile(t *testing.T) {
	clearPGEnvironment(t)
	passfile := writeConfigFile(t, "pgpass", "127.0.0.1:5432:app:alice:svcpass\n")
	serviceFile := writeConfigFile(t, "pg_service.conf", "[svc]\nhost=127.0.0.1\ndbname=app\nuser=alice\npassfile="+filepath.ToSlash(passfile)+"\n")
	t.Setenv("PGSERVICEFILE", serviceFile)

	cfg, err := parseConfig("service=svc")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Password != "svcpass" {
		t.Errorf("Password = %q, want the password from the service file's passfile", cfg.Password)
	}
}

func TestClientCertificatePairNotRequiredWhenTLSDisabled(t *testing.T) {
	clearPGEnvironment(t)
	t.Setenv("PGSSLCERT", "/nonexistent/client.crt")

	// PGSSLCERT is common in psql environments; it must not break a
	// plaintext connection.
	if _, err := parseConfig("user=alice sslmode=disable"); err != nil {
		t.Errorf("sslmode=disable with PGSSLCERT: %v", err)
	}
	if _, err := parseConfig("user=alice sslmode=require"); err == nil ||
		!strings.Contains(err.Error(), "sslcert and sslkey") {
		t.Errorf("sslmode=require with only sslcert error = %v", err)
	}
}

func TestPassfileRejectsInsecurePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX file modes are not enforced on Windows")
	}
	clearPGEnvironment(t)
	passfile := writeConfigFile(t, "pgpass", "127.0.0.1:5432:app:alice:secret\n")
	if err := os.Chmod(passfile, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	_, err := parseConfig("host=127.0.0.1 port=5432 user=alice passfile=" + filepath.ToSlash(passfile))
	if err == nil || !strings.Contains(err.Error(), "permissions") {
		t.Errorf("error = %v, want an insecure-permissions error", err)
	}
}

func TestPassfileSkipsMalformedLinesAndHandlesCRLF(t *testing.T) {
	clearPGEnvironment(t)
	passfile := writeConfigFile(t, "pgpass",
		"bad line\r\nshort:line\r\n127.0.0.1:5432:app:alice:crlfpass\r\n")

	cfg, err := parseConfig("host=127.0.0.1 port=5432 dbname=app user=alice passfile=" + filepath.ToSlash(passfile))
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Password != "crlfpass" {
		t.Errorf("Password = %q, want the matching line after malformed ones", cfg.Password)
	}
}

func TestPassfilePasswordStopsAtUnescapedColon(t *testing.T) {
	clearPGEnvironment(t)
	passfile := writeConfigFile(t, "pgpass", "127.0.0.1:5432:app:alice:pa:ss\n")

	cfg, err := parseConfig("host=127.0.0.1 port=5432 dbname=app user=alice passfile=" + filepath.ToSlash(passfile))
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Password != "pa" {
		t.Errorf("Password = %q, want libpq's stop at the first unescaped colon", cfg.Password)
	}
}

func TestDefaultServiceFileOnWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows-specific path")
	}
	appData := t.TempDir()
	t.Setenv("AppData", appData)
	want := filepath.Join(appData, "postgresql", ".pg_service.conf")
	if got := defaultServiceFile(); got != want {
		t.Errorf("defaultServiceFile() = %q, want %q", got, want)
	}
}

func TestEmptyPasswordFallsBackToPassfile(t *testing.T) {
	clearPGEnvironment(t)
	passfile := writeConfigFile(t, "pgpass", "127.0.0.1:5432:app:alice:filepass\n")

	for _, dsn := range []string{
		"host=127.0.0.1 port=5432 dbname=app user=alice password='' passfile=" + filepath.ToSlash(passfile),
		"postgres://alice:@127.0.0.1:5432/app?passfile=" + filepath.ToSlash(passfile),
	} {
		cfg, err := parseConfig(dsn)
		if err != nil {
			t.Fatalf("parseConfig(%q): %v", dsn, err)
		}
		if cfg.Password != "filepass" {
			t.Errorf("parseConfig(%q) Password = %q, want the passfile value", dsn, cfg.Password)
		}
	}
}

func TestURLSchemeIsCaseInsensitive(t *testing.T) {
	clearPGEnvironment(t)
	for _, dsn := range []string{"POSTGRES://alice@db/app", "Postgresql://alice@db/app"} {
		cfg, err := parseConfig(dsn)
		if err != nil {
			t.Errorf("parseConfig(%q): %v", dsn, err)
			continue
		}
		if cfg.Host != "db" || cfg.User != "alice" {
			t.Errorf("parseConfig(%q) = %s/%s", dsn, cfg.User, cfg.Host)
		}
	}
}

func TestURLPercentEncodedHostIsDecoded(t *testing.T) {
	clearPGEnvironment(t)
	cfg, err := parseConfig("postgres://alice@ho%73t/app")
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Host != "host" {
		t.Errorf("Host = %q, want the decoded host", cfg.Host)
	}
}

func TestMalformedKeywordDSNDoesNotEchoValue(t *testing.T) {
	clearPGEnvironment(t)
	_, err := parseConfig("postgres:/alice:topsecret@host/db")
	if err == nil {
		t.Fatal("expected a malformed DSN error")
	}
	if strings.Contains(err.Error(), "topsecret") {
		t.Errorf("error %q echoes the DSN", err)
	}
}

func TestClientCertificateParameters(t *testing.T) {
	clearPGEnvironment(t)

	if _, err := parseConfig("user=alice sslcert=cert.pem"); err == nil ||
		!strings.Contains(err.Error(), "sslcert and sslkey") {
		t.Errorf("sslcert without sslkey error = %v", err)
	}
	if _, err := parseConfig("user=alice sslkey=key.pem"); err == nil ||
		!strings.Contains(err.Error(), "sslcert and sslkey") {
		t.Errorf("sslkey without sslcert error = %v", err)
	}
	if _, err := parseConfig("user=alice sslpassword=secret"); err == nil ||
		!strings.Contains(err.Error(), "sslpassword") {
		t.Errorf("sslpassword error = %v", err)
	}

	certPEM, keyPEM := newTestCertificatePEM(t, []string{"db.example.com"}, nil)
	certFile := writeConfigFile(t, "client.crt", string(certPEM))
	keyFile := writeConfigFile(t, "client.key", string(keyPEM))

	cfg, err := parseConfig("user=alice sslcert=" + filepath.ToSlash(certFile) + " sslkey=" + filepath.ToSlash(keyFile))
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.SSLClientCert != filepath.ToSlash(certFile) || cfg.SSLClientKey != filepath.ToSlash(keyFile) {
		t.Errorf("client certificate fields = %q, %q", cfg.SSLClientCert, cfg.SSLClientKey)
	}

	connector, err := NewConnector("user=alice sslmode=require sslcert=" + filepath.ToSlash(certFile) + " sslkey=" + filepath.ToSlash(keyFile))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	if got := len(connector.config.tlsConfig.Certificates); got != 1 {
		t.Errorf("TLS client certificates = %d, want 1", got)
	}
}

func TestConnectorRejectsUnreadableClientKey(t *testing.T) {
	clearPGEnvironment(t)
	certPEM, _ := newTestCertificatePEM(t, []string{"db.example.com"}, nil)
	certFile := writeConfigFile(t, "client.crt", string(certPEM))
	missingKey := filepath.Join(t.TempDir(), "missing.key")

	_, err := NewConnector("user=alice sslmode=require sslcert=" + filepath.ToSlash(certFile) + " sslkey=" + filepath.ToSlash(missingKey))
	if err == nil || !strings.Contains(err.Error(), "sslcert") {
		t.Errorf("error = %v, want a client certificate error", err)
	}
}

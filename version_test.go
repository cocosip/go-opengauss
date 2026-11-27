package opengauss

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cocosip/go-opengauss/internal/proto"
)

func TestParseVersionBanner(t *testing.T) {
	tests := []struct {
		banner  string
		product string
		major   int
		minor   int
		patch   int
	}{
		// An openGauss banner: the build hash and the compile date are ignored.
		{
			banner:  "(openGauss 7.0.0 build 0b9e0c7e) compiled at 2025-09-20 09:00:00 commit 1a2b3c4",
			product: "openGauss", major: 7, minor: 0, patch: 0,
		},
		// A release-candidate suffix does not change the numbers.
		{
			banner:  "openGauss 6.0.0-RC1 (GaussDB Kernel 5.0.0 build 1234abcd) compiled at 2024-01-10",
			product: "openGauss", major: 6, minor: 0, patch: 0,
		},
		{banner: "MogDB 5.0.0 build 9f8e7d6c", product: "MogDB", major: 5, minor: 0, patch: 0},
		{
			banner:  "PostgreSQL 9.2.4 on x86_64-unknown-linux-gnu, compiled by gcc (GCC) 4.8.5, 64-bit",
			product: "PostgreSQL", major: 9, minor: 2, patch: 4,
		},
		// Missing components are zero.
		{banner: "openGauss 7", product: "openGauss", major: 7, minor: 0, patch: 0},
		{banner: "openGauss 7.1", product: "openGauss", major: 7, minor: 1, patch: 0},
		// An unknown product is still identified by its name.
		{banner: "SomeNewDB 1.2.3 (build 4)", product: "SomeNewDB", major: 1, minor: 2, patch: 3},
		// No product word followed by a version: nothing is guessed.
		{banner: "openGauss unknown", product: ""},
		{banner: "9.2.4", product: ""},
		{banner: "compiled at 2025-09-20", product: ""},
		{banner: "", product: ""},
		{banner: "   ", product: ""},
		// A version component that overflows an int leaves the numbers at zero
		// instead of wrapping.
		{banner: "openGauss 2025010112345678901234.1.2", product: "openGauss"},
	}
	for _, test := range tests {
		version := parseVersionBanner(test.banner)
		if version.Product != test.product || version.Major != test.major ||
			version.Minor != test.minor || version.Patch != test.patch {
			t.Errorf("parseVersionBanner(%q) = %#v, want product %q version %d.%d.%d",
				test.banner, version, test.product, test.major, test.minor, test.patch)
		}
		if version.Raw != test.banner {
			t.Errorf("parseVersionBanner(%q) kept Raw %q", test.banner, version.Raw)
		}
	}
}

func TestServerVersionString(t *testing.T) {
	if got := (ServerVersion{Product: "openGauss", Major: 7, Minor: 0, Patch: 1}).String(); got != "openGauss 7.0.1" {
		t.Errorf("String() = %q, want %q", got, "openGauss 7.0.1")
	}
	if got := (ServerVersion{}).String(); got != "unknown 0.0.0" {
		t.Errorf("empty String() = %q, want %q", got, "unknown 0.0.0")
	}
}

func TestDetectServerVersionCachesBanner(t *testing.T) {
	const banner = "(openGauss 7.0.0 build 0b9e0c7e) compiled at 2025-09-20 09:00:00 commit 1a2b3c4"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "SELECT version()" {
			s.fail("query = %q, want the banner query", query)
			return
		}
		s.sendRowDescription(fakeColumn{name: "version", oid: 25})
		s.sendDataRow(banner)
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
		// The second call is answered from the cache, so only the Ping of the
		// reuse check arrives before the session is terminated.
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	pinned, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	version, err := DetectServerVersion(ctx, pinned)
	if err != nil {
		t.Fatalf("DetectServerVersion: %v", err)
	}
	want := ServerVersion{Product: "openGauss", Major: 7, Raw: banner}
	want.Minor, want.Patch = 0, 0
	if version != want {
		t.Fatalf("version = %#v, want %#v", version, want)
	}
	again, err := DetectServerVersion(ctx, pinned)
	if err != nil {
		t.Fatalf("second DetectServerVersion: %v", err)
	}
	if again != version {
		t.Fatalf("cached version = %#v, want %#v", again, version)
	}
	if err := pinned.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if server.connections() != 1 {
		t.Errorf("connections = %d, want the cached session reused", server.connections())
	}
}

func TestDetectServerVersionReportsServerError(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "SELECT version()" {
			s.fail("query = %q, want the banner query", query)
			return
		}
		s.sendError("42501", "permission denied for function version")
		s.sendReady(proto.TxnIdle)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	ctx := testContext(t)
	pinned, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	if _, err := DetectServerVersion(ctx, pinned); err == nil {
		t.Fatal("the server error was not reported")
	} else {
		var serverErr *Error
		if !errors.As(err, &serverErr) || serverErr.Code != "42501" {
			t.Fatalf("DetectServerVersion error = %v, want SQLSTATE 42501", err)
		}
	}
	if err := pinned.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A failed probe leaves the session usable.
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestDetectServerVersionRefusesInsideTransaction(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "BEGIN" {
			s.fail("query = %q, want BEGIN", query)
			return
		}
		s.sendCommandComplete("BEGIN")
		s.sendReady(proto.TxnInTransaction)
		// The probe is refused locally, so no banner query arrives.
		s.expectType(proto.MsgTerminate)
	})

	connector, err := NewConnector(testDSN(server, ""))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	ctx := testContext(t)
	driverConn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	c, ok := driverConn.(*conn)
	if !ok {
		t.Fatalf("Connect returned a %T", driverConn)
	}
	defer func() { _ = c.Close() }()
	if _, err := c.ExecContext(ctx, "BEGIN", nil); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if _, err := c.detectServerVersion(ctx); err == nil || !strings.Contains(err.Error(), "inside a transaction") {
		t.Fatalf("detectServerVersion inside a transaction = %v, want the transaction guard", err)
	}
}

func TestDetectServerVersionNeedsAConnection(t *testing.T) {
	if _, err := DetectServerVersion(context.Background(), nil); err == nil {
		t.Fatal("a nil connection was accepted")
	}
}

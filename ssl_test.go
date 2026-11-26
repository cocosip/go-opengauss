package opengauss

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"strings"
	"testing"

	"github.com/cocosip/go-opengauss/internal/proto"
)

func TestRegisterTLSConfigValidation(t *testing.T) {
	if err := RegisterTLSConfig("x", nil); err == nil {
		t.Error("nil config was accepted")
	}
	if err := RegisterTLSConfig("", &tls.Config{}); err == nil {
		t.Error("empty name was accepted")
	}
	for _, name := range []string{"require", "Require", "verify-full", "disable", "true", "0"} {
		if err := RegisterTLSConfig(name, &tls.Config{}); err == nil {
			t.Errorf("reserved name %q was accepted", name)
		}
	}
	if err := RegisterTLSConfig("app-tls", &tls.Config{}); err != nil {
		t.Fatalf("RegisterTLSConfig: %v", err)
	}
	t.Cleanup(func() { DeregisterTLSConfig("app-tls") })
	if !hasRegisteredTLSConfig("app-tls") {
		t.Error("registered config is missing")
	}

	cfg := &Config{SSLMode: "app-tls"}
	tlsConfig, err := cfg.buildTLSConfig()
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if tlsConfig == nil {
		t.Fatal("buildTLSConfig returned nil")
	}

	DeregisterTLSConfig("app-tls")
	if hasRegisteredTLSConfig("app-tls") {
		t.Error("deregistered config is still present")
	}
	if _, err := cfg.buildTLSConfig(); err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Errorf("buildTLSConfig after deregistration = %v", err)
	}
}

func TestRegisteredTLSConfigConnects(t *testing.T) {
	clearPGEnvironment(t)
	cert, caPEM := newTestCertificate(t, nil, []net.IP{net.ParseIP("127.0.0.1")})
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caPEM)
	if err := RegisterTLSConfig("app-tls", &tls.Config{RootCAs: roots}); err != nil {
		t.Fatalf("RegisterTLSConfig: %v", err)
	}
	t.Cleanup(func() { DeregisterTLSConfig("app-tls") })

	server := newFakeServer(t, func(s *fakeSession) {
		if !s.acceptTLS(cert) {
			return
		}
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "" {
			s.fail("ping query = %q", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, "sslmode=app-tls"))
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("ping with a registered TLS config: %v", err)
	}
}

func TestRegisteredTLSConfigKeepsServerName(t *testing.T) {
	clearPGEnvironment(t)
	cert, caPEM := newTestCertificate(t, []string{"db.example.com"}, nil)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caPEM)
	config := &tls.Config{RootCAs: roots, ServerName: "db.example.com"}
	if err := RegisterTLSConfig("named-tls", config); err != nil {
		t.Fatalf("RegisterTLSConfig: %v", err)
	}
	t.Cleanup(func() { DeregisterTLSConfig("named-tls") })

	server := newFakeServer(t, func(s *fakeSession) {
		if !s.acceptTLS(cert) {
			return
		}
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "" {
			s.fail("ping query = %q", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})

	db := openTestDB(t, testDSN(server, "sslmode=named-tls"))
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("ping must verify db.example.com from the registered config: %v", err)
	}
}

func TestUnregisteredTLSConfigRejected(t *testing.T) {
	clearPGEnvironment(t)
	if _, err := parseConfig("user=alice sslmode=missing-tls"); err == nil ||
		!strings.Contains(err.Error(), "RegisterTLSConfig") {
		t.Errorf("unknown sslmode error = %v", err)
	}
}

func TestInlineClientCertificate(t *testing.T) {
	certPEM, keyPEM := newTestCertificatePEM(t, []string{"db.example.com"}, nil)
	cfg := &Config{
		SSLMode:       SSLModeRequire,
		SSLInline:     true,
		SSLClientCert: string(certPEM),
		SSLClientKey:  string(keyPEM),
	}
	tlsConfig, err := cfg.buildTLSConfig()
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if len(tlsConfig.Certificates) != 1 {
		t.Errorf("TLS client certificates = %d, want 1", len(tlsConfig.Certificates))
	}
}

func TestEncryptedClientKey(t *testing.T) {
	certPEM, keyPEM := newTestCertificatePEM(t, []string{"db.example.com"}, nil)
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		t.Fatal("decode test key")
	}
	//nolint:staticcheck // exercise the RFC 1423 format libpq clients produce.
	encrypted, err := x509.EncryptPEMBlock(rand.Reader, block.Type, block.Bytes, []byte("secret"), x509.PEMCipherAES256)
	if err != nil {
		t.Fatalf("EncryptPEMBlock: %v", err)
	}
	encryptedPEM := pem.EncodeToMemory(encrypted)

	cfg := &Config{
		SSLMode:       SSLModeRequire,
		SSLInline:     true,
		SSLClientCert: string(certPEM),
		SSLClientKey:  string(encryptedPEM),
		sslPassword:   "secret",
	}
	if _, err := cfg.buildTLSConfig(); err != nil {
		t.Fatalf("buildTLSConfig with the correct password: %v", err)
	}

	cfg.sslPassword = "wrong"
	if _, err := cfg.buildTLSConfig(); err == nil || !strings.Contains(err.Error(), "sslpassword") {
		t.Errorf("wrong password error = %v", err)
	}

	cfg.sslPassword = ""
	if _, err := cfg.buildTLSConfig(); err == nil || !strings.Contains(err.Error(), "sslpassword") {
		t.Errorf("missing password error = %v", err)
	}
}

func TestInlineRootCertificate(t *testing.T) {
	caPEM, _ := newTestCertificatePEM(t, []string{"db.example.com"}, nil)
	cfg := &Config{
		SSLMode:     SSLModeVerifyCA,
		SSLInline:   true,
		SSLRootCert: string(caPEM),
	}
	tlsConfig, err := cfg.buildTLSConfig()
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if tlsConfig.RootCAs == nil {
		t.Error("RootCAs is nil, want the inline CA pool")
	}
}

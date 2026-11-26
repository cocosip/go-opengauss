package opengauss

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"strings"
	"testing"
)

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

package opengauss

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
)

// RegisterTLSConfig registers config under name, which can then be used as
// the sslmode value. The config is cloned for every physical connection, so a
// caller may keep modifying the original after registration. A non-empty
// ServerName in config is preserved; an empty one is filled from the target
// when the connection is opened.
//
// The reserved sslmode names and the boolean spellings libpq accepts are
// rejected. Registering the same name again replaces the previous entry.
func RegisterTLSConfig(name string, config *tls.Config) error {
	if config == nil {
		return errors.New("opengauss: RegisterTLSConfig needs a non-nil config")
	}
	if name == "" {
		return errors.New("opengauss: RegisterTLSConfig needs a non-empty name")
	}
	if SSLMode(strings.ToLower(name)).valid() || isBoolName(name) {
		return fmt.Errorf("opengauss: TLS config name %q is reserved", name)
	}
	tlsConfigsMu.Lock()
	if tlsConfigs == nil {
		tlsConfigs = make(map[string]*tls.Config)
	}
	tlsConfigs[name] = config
	tlsConfigsMu.Unlock()
	return nil
}

// DeregisterTLSConfig removes the config registered under name. Connections
// that already opened keep the clone they were created with.
func DeregisterTLSConfig(name string) {
	tlsConfigsMu.Lock()
	delete(tlsConfigs, name)
	tlsConfigsMu.Unlock()
}

// isBoolName reports whether name is a boolean spelling that could otherwise
// be mistaken for an sslmode value.
func isBoolName(name string) bool {
	switch strings.ToLower(name) {
	case "1", "true", "0", "false":
		return true
	}
	return false
}

// hasRegisteredTLSConfig reports whether name resolves to a registered config.
func hasRegisteredTLSConfig(name string) bool {
	tlsConfigsMu.RLock()
	_, ok := tlsConfigs[name]
	tlsConfigsMu.RUnlock()
	return ok
}

// registeredTLSConfig returns a clone of the config registered under name.
func registeredTLSConfig(name string) (*tls.Config, bool) {
	tlsConfigsMu.RLock()
	config, ok := tlsConfigs[name]
	tlsConfigsMu.RUnlock()
	if !ok {
		return nil, false
	}
	return config.Clone(), true
}

var (
	tlsConfigsMu sync.RWMutex
	tlsConfigs   map[string]*tls.Config
)

// buildTLSConfig builds the client TLS configuration for the requested
// sslmode. The certificate chain and host name are verified exactly as far as
// the mode asks; the driver never quietly weakens a verify-* policy. When
// sslrootcert is omitted for a verify-* mode, Go's system roots are used,
// unlike libpq's ~/.postgresql/root.crt default.
func (c *Config) buildTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{}
	switch c.SSLMode {
	case SSLModeVerifyFull:
		// Go's default verification checks both the chain and the host name.
	case SSLModeVerifyCA:
		verifyCAOnly(tlsConfig)
	case SSLModeRequire:
		if c.SSLRootCert == "" {
			tlsConfig.InsecureSkipVerify = true
		} else {
			// libpq treats require plus a root CA as verify-ca.
			verifyCAOnly(tlsConfig)
		}
	case SSLModeAllow, SSLModePrefer:
		tlsConfig.InsecureSkipVerify = true
	default:
		// A name registered with RegisterTLSConfig supplies its own policy,
		// including certificates and roots.
		registered, ok := registeredTLSConfig(string(c.SSLMode))
		if !ok {
			return nil, fmt.Errorf("opengauss: sslmode %q is not registered", c.SSLMode)
		}
		return registered, nil
	}

	if c.SSLClientCert != "" {
		certPEM := []byte(c.SSLClientCert)
		keyPEM := []byte(c.SSLClientKey)
		if !c.SSLInline {
			var err error
			if certPEM, err = os.ReadFile(c.SSLClientCert); err != nil {
				return nil, fmt.Errorf("opengauss: read sslcert: %w", err)
			}
			if keyPEM, err = os.ReadFile(c.SSLClientKey); err != nil {
				return nil, fmt.Errorf("opengauss: read sslkey: %w", err)
			}
		}
		keyPEM, err := decryptSSLKey(keyPEM, c.sslPassword)
		if err != nil {
			return nil, err
		}
		certificate, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, fmt.Errorf("opengauss: load sslcert/sslkey: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	if c.SSLRootCert != "" {
		pemData := []byte(c.SSLRootCert)
		if !c.SSLInline {
			var err error
			if pemData, err = os.ReadFile(c.SSLRootCert); err != nil {
				return nil, fmt.Errorf("opengauss: read sslrootcert: %w", err)
			}
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pemData) {
			return nil, fmt.Errorf("opengauss: sslrootcert %q contains no PEM certificates", c.SSLRootCert)
		}
		tlsConfig.RootCAs = roots
	}
	return tlsConfig, nil
}

// decryptSSLKey returns keyPEM with its legacy RFC 1423 encryption removed when
// sslpassword was supplied. An unencrypted key is returned unchanged, so an
// unused sslpassword is harmless.
func decryptSSLKey(keyPEM []byte, password string) ([]byte, error) {
	block, _ := pem.Decode(keyPEM)
	//nolint:staticcheck // libpq and OpenSSL write RFC 1423 encrypted PEM keys.
	if block == nil || !x509.IsEncryptedPEMBlock(block) {
		return keyPEM, nil
	}
	if password == "" {
		return nil, errors.New("opengauss: sslkey is encrypted but sslpassword was not supplied")
	}
	//nolint:staticcheck // libpq and OpenSSL write RFC 1423 encrypted PEM keys.
	der, err := x509.DecryptPEMBlock(block, []byte(password))
	if err != nil {
		if errors.Is(err, x509.IncorrectPasswordError) {
			return nil, errors.New("opengauss: sslpassword does not decrypt sslkey")
		}
		return nil, fmt.Errorf("opengauss: decrypt sslkey: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: block.Type, Bytes: der}), nil
}

// verifyCAOnly verifies the server chain without its host name, which matches
// libpq's verify-ca. Go always checks the host name when InsecureSkipVerify is
// false, so the chain check is performed here instead.
func verifyCAOnly(tlsConfig *tls.Config) {
	tlsConfig.InsecureSkipVerify = true
	tlsConfig.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("server supplied no TLS certificate")
		}
		certs := make([]*x509.Certificate, len(rawCerts))
		for i, raw := range rawCerts {
			cert, err := x509.ParseCertificate(raw)
			if err != nil {
				return fmt.Errorf("parse server certificate: %w", err)
			}
			certs[i] = cert
		}
		options := x509.VerifyOptions{
			Roots:         tlsConfig.RootCAs,
			Intermediates: x509.NewCertPool(),
		}
		for _, cert := range certs[1:] {
			options.Intermediates.AddCert(cert)
		}
		_, err := certs[0].Verify(options)
		return err
	}
}

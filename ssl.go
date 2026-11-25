package opengauss

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
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
	}

	if c.SSLClientCert != "" {
		certificate, err := tls.LoadX509KeyPair(c.SSLClientCert, c.SSLClientKey)
		if err != nil {
			return nil, fmt.Errorf("opengauss: load sslcert/sslkey: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	if c.SSLRootCert != "" {
		pemData, err := os.ReadFile(c.SSLRootCert)
		if err != nil {
			return nil, fmt.Errorf("opengauss: read sslrootcert: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pemData) {
			return nil, fmt.Errorf("opengauss: sslrootcert %q contains no PEM certificates", c.SSLRootCert)
		}
		tlsConfig.RootCAs = roots
	}
	return tlsConfig, nil
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

package opengauss

import "sync"

// GSS provides GSSAPI authentication, such as Kerberos. A provider is
// registered with RegisterGSSProvider; the driver only frames its tokens and
// never implements the Kerberos exchange itself.
type GSS interface {
	// GetInitToken returns the initial token for service on host, normally
	// "postgres" on the server the DSN names.
	GetInitToken(host, service string) ([]byte, error)
	// GetInitTokenFromSpn returns the initial token for an explicit service
	// principal name, used when krbspn is set.
	GetInitTokenFromSpn(spn string) ([]byte, error)
	// Continue feeds a server token back into the GSS context. It reports
	// whether the exchange is complete and, when it is not, the next token to
	// send.
	Continue(inToken []byte) (done bool, outToken []byte, err error)
}

// NewGSSFunc creates the GSS context for one connection.
type NewGSSFunc func() (GSS, error)

var (
	gssMu  sync.RWMutex
	newGSS NewGSSFunc
)

// RegisterGSSProvider registers the provider used for AuthenticationGSS.
// Passing nil removes the current provider. The provider is asked for a
// context on every connection that the server asks to authenticate with
// GSSAPI; the driver's GSS support is protocol framing only.
func RegisterGSSProvider(provider NewGSSFunc) {
	gssMu.Lock()
	newGSS = provider
	gssMu.Unlock()
}

func currentGSSProvider() NewGSSFunc {
	gssMu.RLock()
	provider := newGSS
	gssMu.RUnlock()
	return provider
}

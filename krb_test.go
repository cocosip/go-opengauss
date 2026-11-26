package opengauss

import (
	"errors"
	"strings"
	"testing"

	"github.com/cocosip/go-opengauss/internal/auth"
	"github.com/cocosip/go-opengauss/internal/proto"
)

// fakeGSS is a scripted GSS provider; it records the context it was asked for
// and returns fixed tokens.
type fakeGSS struct {
	host, service string
	spn           string
	token         []byte
	initErr       error
	spnErr        error

	continued   []byte
	done        bool
	out         []byte
	continueErr error
}

func (f *fakeGSS) GetInitToken(host, service string) ([]byte, error) {
	f.host, f.service = host, service
	return f.token, f.initErr
}

func (f *fakeGSS) GetInitTokenFromSpn(spn string) ([]byte, error) {
	f.spn = spn
	return f.token, f.spnErr
}

func (f *fakeGSS) Continue(inToken []byte) (bool, []byte, error) {
	f.continued = inToken
	return f.done, f.out, f.continueErr
}

func registerFakeGSS(t *testing.T, client *fakeGSS) {
	t.Helper()
	RegisterGSSProvider(func() (GSS, error) { return client, nil })
	t.Cleanup(func() { RegisterGSSProvider(nil) })
}

// gssStartup sends AuthenticationGSS, checks the first client token, then
// finishes the startup exchange.
func gssStartup(s *fakeSession, wantToken string) {
	s.readStartup()
	s.sendAuthenticationGSS(auth.RequestGSS, nil)
	if got := string(s.expectPassword()); got != wantToken {
		s.fail("initial GSS token = %q, want %q", got, wantToken)
	}
}

func completeGSSStartup(s *fakeSession) {
	s.sendAuthenticationOK()
	s.sendParameterStatus("server_version", "7.0.0")
	s.sendParameterStatus("client_encoding", "UTF8")
	s.sendBackendKeyData(4242, 9001)
	s.sendReady(proto.TxnIdle)
}

func completePing(s *fakeSession) {
	if query := s.expectSimpleQuery(); query != "" {
		s.fail("ping query = %q", query)
	}
	s.sendEmptyQueryResponse()
	s.sendReady(proto.TxnIdle)
}

func TestGSSAuthentication(t *testing.T) {
	client := &fakeGSS{token: []byte("initial-token")}
	registerFakeGSS(t, client)

	server := newFakeServer(t, func(s *fakeSession) {
		gssStartup(s, "initial-token")
		completeGSSStartup(s)
		completePing(s)
	})

	db := openTestDB(t, testDSN(server, ""))
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if client.host != "127.0.0.1" || client.service != "postgres" {
		t.Errorf("GSS context = %q/%q, want 127.0.0.1/postgres", client.host, client.service)
	}
}

func TestGSSServiceNameOverride(t *testing.T) {
	client := &fakeGSS{token: []byte("initial-token")}
	registerFakeGSS(t, client)

	server := newFakeServer(t, func(s *fakeSession) {
		gssStartup(s, "initial-token")
		completeGSSStartup(s)
		completePing(s)
	})

	db := openTestDB(t, testDSN(server, "krbsrvname=openGauss"))
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if client.host != "127.0.0.1" || client.service != "openGauss" {
		t.Errorf("GSS context = %q/%q, want 127.0.0.1/openGauss", client.host, client.service)
	}
}

func TestGSSExplicitSPN(t *testing.T) {
	client := &fakeGSS{token: []byte("initial-token")}
	registerFakeGSS(t, client)

	server := newFakeServer(t, func(s *fakeSession) {
		gssStartup(s, "initial-token")
		completeGSSStartup(s)
		completePing(s)
	})

	db := openTestDB(t, testDSN(server, "krbspn=postgres/db.example.com"))
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if client.spn != "postgres/db.example.com" {
		t.Errorf("GSS SPN = %q, want postgres/db.example.com", client.spn)
	}
}

func TestGSSContinue(t *testing.T) {
	client := &fakeGSS{
		token: []byte("initial-token"),
		out:   []byte("final-token"),
	}
	registerFakeGSS(t, client)

	server := newFakeServer(t, func(s *fakeSession) {
		gssStartup(s, "initial-token")
		s.sendAuthenticationGSS(auth.RequestGSSContinue, []byte("server-token"))
		if got := string(s.expectPassword()); got != "final-token" {
			s.fail("continued GSS token = %q, want final-token", got)
		}
		completeGSSStartup(s)
		completePing(s)
	})

	db := openTestDB(t, testDSN(server, ""))
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if string(client.continued) != "server-token" {
		t.Errorf("Continue input = %q, want server-token", client.continued)
	}
}

func TestGSSContinueWithoutToken(t *testing.T) {
	client := &fakeGSS{token: []byte("initial-token"), done: true}
	registerFakeGSS(t, client)

	server := newFakeServer(t, func(s *fakeSession) {
		gssStartup(s, "initial-token")
		s.sendAuthenticationGSS(auth.RequestGSSContinue, []byte("server-token"))
		completeGSSStartup(s)
		completePing(s)
	})

	db := openTestDB(t, testDSN(server, ""))
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestGSSWithoutProvider(t *testing.T) {
	RegisterGSSProvider(nil)
	server := newFakeServer(t, func(s *fakeSession) {
		s.readStartup()
		s.sendAuthenticationGSS(auth.RequestGSS, nil)
	})

	connector, err := NewConnector(testDSN(server, ""))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	_, err = connector.Connect(testContext(t))
	if err == nil || !strings.Contains(err.Error(), "RegisterGSSProvider") {
		t.Errorf("connect error = %v, want a missing-provider error", err)
	}
}

func TestGSSProviderError(t *testing.T) {
	RegisterGSSProvider(func() (GSS, error) { return nil, errors.New("no ticket cache") })
	t.Cleanup(func() { RegisterGSSProvider(nil) })

	server := newFakeServer(t, func(s *fakeSession) {
		s.readStartup()
		s.sendAuthenticationGSS(auth.RequestGSS, nil)
	})
	connector, err := NewConnector(testDSN(server, ""))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	_, err = connector.Connect(testContext(t))
	if err == nil || !strings.Contains(err.Error(), "no ticket cache") {
		t.Errorf("connect error = %v, want the provider error", err)
	}
}

func TestGSSInitialTokenError(t *testing.T) {
	client := &fakeGSS{initErr: errors.New("kinit required")}
	registerFakeGSS(t, client)

	server := newFakeServer(t, func(s *fakeSession) {
		s.readStartup()
		s.sendAuthenticationGSS(auth.RequestGSS, nil)
	})
	connector, err := NewConnector(testDSN(server, ""))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	_, err = connector.Connect(testContext(t))
	if err == nil || !strings.Contains(err.Error(), "kinit required") {
		t.Errorf("connect error = %v, want the initial-token error", err)
	}
}

func TestGSSContinueError(t *testing.T) {
	client := &fakeGSS{
		token:       []byte("initial-token"),
		continueErr: errors.New("bad MIC"),
	}
	registerFakeGSS(t, client)

	server := newFakeServer(t, func(s *fakeSession) {
		gssStartup(s, "initial-token")
		s.sendAuthenticationGSS(auth.RequestGSSContinue, []byte("server-token"))
	})
	connector, err := NewConnector(testDSN(server, ""))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	_, err = connector.Connect(testContext(t))
	if err == nil || !strings.Contains(err.Error(), "bad MIC") {
		t.Errorf("connect error = %v, want the continue error", err)
	}
}

func TestGSSContinueWithoutStart(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.readStartup()
		s.sendAuthenticationGSS(auth.RequestGSSContinue, []byte("server-token"))
	})
	connector, err := NewConnector(testDSN(server, ""))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	_, err = connector.Connect(testContext(t))
	if err == nil || !strings.Contains(err.Error(), "without AuthenticationGSS") {
		t.Errorf("connect error = %v, want a protocol-order error", err)
	}
}

// TestGSSProviderIsPerConnection checks that each connection asks the
// provider for its own context.
func TestGSSProviderIsPerConnection(t *testing.T) {
	var created int
	t.Cleanup(func() { RegisterGSSProvider(nil) })
	RegisterGSSProvider(func() (GSS, error) {
		created++
		return &fakeGSS{token: []byte("initial-token")}, nil
	})

	server := newFakeServer(t, func(s *fakeSession) {
		gssStartup(s, "initial-token")
		completeGSSStartup(s)
		completePing(s)
	})
	db := openTestDB(t, testDSN(server, ""))
	db.SetMaxOpenConns(2)

	ctx := testContext(t)
	first, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("first Conn: %v", err)
	}
	if err := first.PingContext(ctx); err != nil {
		t.Fatalf("first PingContext: %v", err)
	}
	second, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("second Conn: %v", err)
	}
	if err := second.PingContext(ctx); err != nil {
		t.Fatalf("second PingContext: %v", err)
	}
	_ = first.Close()
	_ = second.Close()

	if created != 2 {
		t.Errorf("providers created = %d, want 2", created)
	}
}

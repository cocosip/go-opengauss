package opengauss

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/cocosip/go-opengauss/internal/proto"
)

func TestConnectorUsesCustomDialer(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "" {
			s.fail("ping query = %q", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})

	var calls int
	var gotNetwork, gotAddress string
	connector, err := NewConnectorWithDialer(testDSN(server, ""), func(ctx context.Context, network, address string) (net.Conn, error) {
		calls++
		gotNetwork, gotAddress = network, address
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, address)
	})
	if err != nil {
		t.Fatalf("NewConnectorWithDialer: %v", err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })

	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
	if calls != 1 {
		t.Errorf("dial calls = %d, want 1", calls)
	}
	if want := fmt.Sprintf("127.0.0.1:%d", server.port()); gotNetwork != "tcp" || gotAddress != want {
		t.Errorf("dial target = %s %s, want tcp %s", gotNetwork, gotAddress, want)
	}
}

func TestConnectorCustomDialerError(t *testing.T) {
	refused := errors.New("dialer refused")
	connector, err := NewConnectorWithDialer("host=127.0.0.1 user=alice dbname=app sslmode=disable",
		func(context.Context, string, string) (net.Conn, error) {
			return nil, refused
		})
	if err != nil {
		t.Fatalf("NewConnectorWithDialer: %v", err)
	}
	_, err = connector.Connect(testContext(t))
	if err == nil {
		t.Fatal("expected the dial error")
	}
	if !errors.Is(err, refused) {
		t.Errorf("error = %v, want the dialer's error", err)
	}
	if !strings.Contains(err.Error(), "dial") {
		t.Errorf("error %q does not identify the dial stage", err)
	}
}

func TestNewConnectorWithNilDialer(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != "" {
			s.fail("ping query = %q", query)
		}
		s.sendEmptyQueryResponse()
		s.sendReady(proto.TxnIdle)
	})
	connector, err := NewConnectorWithDialer(testDSN(server, ""), nil)
	if err != nil {
		t.Fatalf("NewConnectorWithDialer: %v", err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(testContext(t)); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

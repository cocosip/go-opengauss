package opengauss

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocosip/go-opengauss/internal/proto"
)

// listenerEvent is one callback invocation recorded by a test.
type listenerEvent struct {
	ev  ListenerEvent
	err error
}

// recordEvents returns a callback that reports every event on a buffered
// channel, so the listener goroutine never blocks on the test.
func recordEvents() (EventCallback, <-chan listenerEvent) {
	events := make(chan listenerEvent, 16)
	return func(_ *Listener, ev ListenerEvent, err error) {
		events <- listenerEvent{ev: ev, err: err}
	}, events
}

// waitEvent reads events until ev arrives, failing the test on a timeout.
func waitEvent(t *testing.T, events <-chan listenerEvent, want ListenerEvent) listenerEvent {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event := <-events:
			if event.ev == want {
				return event
			}
		case <-deadline:
			t.Fatalf("no listener event %v arrived", want)
			return listenerEvent{}
		}
	}
}

// newTestListener opens a listener against the scripted peer with short
// reconnect intervals and closes it when the test ends.
func newTestListener(t *testing.T, dsn string, callback EventCallback) *Listener {
	t.Helper()
	listener, err := NewListenerWithCallback(dsn, callback, 10*time.Millisecond, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

// receiveNotification returns the next delivered notification.
func receiveNotification(t *testing.T, listener *Listener) *Notification {
	t.Helper()
	select {
	case notification, ok := <-listener.NotificationChannel():
		if !ok {
			t.Fatal("the notification channel was closed")
		}
		return notification
	case <-time.After(5 * time.Second):
		t.Fatal("no notification arrived")
		return nil
	}
}

// notificationFrame builds one NotificationResponse message.
func notificationFrame(pid int32, channel, payload string) []byte {
	m := proto.NewMessageWriter(proto.MsgNotificationResponse)
	m.Int32(pid)
	m.String(channel)
	m.String(payload)
	return m.Finish()
}

// expectQuery asserts one simple query and answers it successfully.
func (s *fakeSession) expectQuery(want string) {
	if query := s.expectSimpleQuery(); query != want {
		s.fail("query = %q, want %q", query, want)
	}
	s.sendCommandComplete("OK")
	s.sendReady(proto.TxnIdle)
}

// expectPing asserts the empty statement used as a Ping.
func (s *fakeSession) expectPing() {
	if query := s.expectSimpleQuery(); query != "" {
		s.fail("ping used query %q, want the empty statement", query)
	}
	s.sendEmptyQueryResponse()
	s.sendReady(proto.TxnIdle)
}

func TestListenerReceivesNotificationForListen(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != `LISTEN "events"` {
			s.fail("query = %q, want LISTEN \"events\"", query)
			return
		}
		// A notification may arrive before the LISTEN result is complete; it
		// must still be delivered in order.
		s.writeRaw(notificationFrame(77, "events", "payload one"))
		s.sendCommandComplete("LISTEN")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	listener := newTestListener(t, testDSN(server, ""), nil)
	if err := listener.Listen("events"); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	notification := receiveNotification(t, listener)
	if notification.PID != 77 || notification.Channel != "events" || notification.Extra != "payload one" {
		t.Fatalf("notification = %+v, want PID 77 on events with payload one", notification)
	}
}

func TestListenerCommandsQuoteNamesAndPayloads(t *testing.T) {
	const channel = `odd" name`
	const payload = `it's a \ backslash`
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectQuery(`LISTEN "odd"" name"`)
		s.expectQuery(`UNLISTEN "odd"" name"`)
		s.expectQuery(`NOTIFY "odd"" name", E'it''s a \\ backslash'`)
		s.expectQuery(`NOTIFY "odd"" name", E''`)
		s.expectPing()
		s.expectType(proto.MsgTerminate)
	})

	listener := newTestListener(t, testDSN(server, ""), nil)
	if err := listener.Listen(channel); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := listener.Unlisten(channel); err != nil {
		t.Fatalf("Unlisten: %v", err)
	}
	if err := listener.Notify(channel, payload); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if err := listener.Notify(channel, ""); err != nil {
		t.Fatalf("Notify with an empty payload: %v", err)
	}
	if err := listener.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestListenerReconnectsAndRegistersChannelsAgain(t *testing.T) {
	var connections atomic.Int32
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		switch connections.Add(1) {
		case 1:
			s.expectQuery(`LISTEN "events"`)
			// Drop the session without an answer: the listener must notice and
			// reconnect.
		default:
			s.expectQuery(`LISTEN "events"`)
			s.writeRaw(notificationFrame(5, "events", "after reconnect"))
			s.expectType(proto.MsgTerminate)
		}
	})

	callback, events := recordEvents()
	listener := newTestListener(t, testDSN(server, ""), callback)
	if err := listener.Listen("events"); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	waitEvent(t, events, ListenerEventConnected)
	disconnected := waitEvent(t, events, ListenerEventDisconnected)
	if disconnected.err == nil {
		t.Error("a lost session was reported without an error")
	}
	waitEvent(t, events, ListenerEventReconnected)

	notification := receiveNotification(t, listener)
	if notification.Channel != "events" || notification.Extra != "after reconnect" {
		t.Fatalf("notification = %+v, want the post-reconnect message", notification)
	}
}

func TestListenerDoesNotReRegisterUnlistenedChannel(t *testing.T) {
	var connections atomic.Int32
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		switch connections.Add(1) {
		case 1:
			s.expectQuery(`LISTEN "events"`)
			s.expectQuery(`UNLISTEN "events"`)
			// Drop the session. After the unlisten was accepted, the channel
			// must not be registered again on the next session.
		default:
			s.expectPing()
			s.expectType(proto.MsgTerminate)
		}
	})

	callback, events := recordEvents()
	listener := newTestListener(t, testDSN(server, ""), callback)
	if err := listener.Listen("events"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := listener.Unlisten("events"); err != nil {
		t.Fatalf("Unlisten: %v", err)
	}
	waitEvent(t, events, ListenerEventDisconnected)
	waitEvent(t, events, ListenerEventReconnected)
	if err := listener.Ping(); err != nil {
		t.Fatalf("Ping after reconnect: %v", err)
	}
}

func TestListenerReportsNotices(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectQuery(`LISTEN "events"`)
		s.sendNotice("a listening-session notice")
		s.expectType(proto.MsgTerminate)
	})

	notices := make(chan *Error, 4)
	listener := newTestListener(t, testDSN(server, ""), nil)
	listener.SetNoticeHandler(func(notice *Error) { notices <- notice })
	if err := listener.Listen("events"); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	select {
	case notice := <-notices:
		if notice.Message != "a listening-session notice" || notice.Severity != "NOTICE" {
			t.Fatalf("notice = %+v, want the scripted NOTICE", notice)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no notice was delivered")
	}
}

func TestListenerRejectsInvalidChannelNames(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		// The rejected commands must never reach the server.
		s.expectType(proto.MsgTerminate)
	})

	listener := newTestListener(t, testDSN(server, ""), nil)
	if err := listener.Listen(""); err == nil {
		t.Error("Listen with an empty channel name was accepted")
	}
	if err := listener.Listen("a\x00b"); err == nil {
		t.Error("Listen with a NUL byte in the channel name was accepted")
	}
	if err := listener.Unlisten(""); err == nil {
		t.Error("Unlisten with an empty channel name was accepted")
	}
	if err := listener.Notify("channel", "a\x00b"); err == nil {
		t.Error("Notify with a NUL byte in the payload was accepted")
	}
}

func TestListenerCloseEndsSessionAndCommands(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectType(proto.MsgTerminate)
	})

	listener, err := NewListener(testDSN(server, ""), 10*time.Millisecond, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, ok := <-listener.NotificationChannel(); ok {
		t.Error("the notification channel stayed open after Close")
	}
	if err := listener.Ping(); !errors.Is(err, ErrListenerClosed) {
		t.Fatalf("Ping after Close = %v, want ErrListenerClosed", err)
	}
	if err := listener.Listen("events"); !errors.Is(err, ErrListenerClosed) {
		t.Fatalf("Listen after Close = %v, want ErrListenerClosed", err)
	}
}

func TestListenerDiscardsSessionOnMalformedNotification(t *testing.T) {
	var connections atomic.Int32
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		switch connections.Add(1) {
		case 1:
			s.expectQuery(`LISTEN "events"`)
			// A truncated notification body cannot be decoded; the session must
			// not continue with an unknown message boundary.
			s.writeRaw(proto.NewMessageWriter(proto.MsgNotificationResponse).Finish())
		default:
			s.expectQuery(`LISTEN "events"`)
			s.expectType(proto.MsgTerminate)
		}
	})

	callback, events := recordEvents()
	listener := newTestListener(t, testDSN(server, ""), callback)
	if err := listener.Listen("events"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	waitEvent(t, events, ListenerEventConnected)
	if event := waitEvent(t, events, ListenerEventDisconnected); event.err == nil {
		t.Error("the malformed notification was not reported")
	}
	waitEvent(t, events, ListenerEventReconnected)
}

func TestListenerCommandsFailWhileReconnecting(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		s.expectQuery(`LISTEN "events"`)
		// Drop the session; the long reconnect interval keeps the listener
		// without a session while the test sends the next command.
	})

	callback, events := recordEvents()
	listener, err := NewListenerWithCallback(testDSN(server, ""), callback, 10*time.Second, 10*time.Second)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	if err := listener.Listen("events"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	waitEvent(t, events, ListenerEventDisconnected)

	err = listener.Listen("events")
	if err == nil || !strings.Contains(err.Error(), "reconnecting") {
		t.Fatalf("Listen while reconnecting = %v, want a reconnecting error", err)
	}
}

func TestParseNotificationRejectsTruncatedBody(t *testing.T) {
	if _, err := parseNotification([]byte{0, 0}); err == nil {
		t.Error("a truncated notification body was accepted")
	}
}

func TestListenerSerializesConcurrentCommands(t *testing.T) {
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		seen := make(map[string]bool, 2)
		for i := 0; i < 2; i++ {
			seen[s.expectSimpleQuery()] = true
			s.sendCommandComplete("LISTEN")
			s.sendReady(proto.TxnIdle)
		}
		if !seen[`LISTEN "one"`] || !seen[`LISTEN "two"`] {
			s.fail("commands seen = %v, want both channel registrations", seen)
		}
		s.expectType(proto.MsgTerminate)
	})

	listener := newTestListener(t, testDSN(server, ""), nil)
	errs := make(chan error, 2)
	go func() { errs <- listener.Listen("one") }()
	go func() { errs <- listener.Listen("two") }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent Listen: %v", err)
		}
	}
}

func TestListenerDoesNotRegisterARejectedChannel(t *testing.T) {
	var connections atomic.Int32
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		switch connections.Add(1) {
		case 1:
			if query := s.expectSimpleQuery(); query != `LISTEN "events"` {
				s.fail("query = %q, want LISTEN \"events\"", query)
				return
			}
			s.sendError("42501", "permission denied for LISTEN")
			s.sendReady(proto.TxnIdle)
			// Drop the session so the listener reconnects.
		default:
			// The rejected LISTEN must not be replayed on the new session.
			s.expectPing()
			s.expectType(proto.MsgTerminate)
		}
	})

	callback, events := recordEvents()
	listener := newTestListener(t, testDSN(server, ""), callback)
	err := listener.Listen("events")
	var serverErr *Error
	if !errors.As(err, &serverErr) || serverErr.SQLState() != "42501" {
		t.Fatalf("Listen = %v, want the server's SQLSTATE 42501", err)
	}
	waitEvent(t, events, ListenerEventDisconnected)
	waitEvent(t, events, ListenerEventReconnected)
	if err := listener.Ping(); err != nil {
		t.Fatalf("Ping after reconnect: %v", err)
	}
}

func TestQuoteLiteralEscapesSingleQuotesAndBackslashes(t *testing.T) {
	if got, want := quoteLiteral(`a'b\c`), `E'a''b\\c'`; got != want {
		t.Fatalf("quoteLiteral = %s, want %s", got, want)
	}
}

// TestListenerNextBackoffDoublesAndCaps checks the reconnect delay strategy:
// the delay doubles after every failed attempt and stops at the configured
// maximum instead of growing without bound.
func TestListenerNextBackoffDoublesAndCaps(t *testing.T) {
	l := &Listener{minReconnectInterval: 100 * time.Millisecond, maxReconnectInterval: time.Second}
	tests := []struct {
		current time.Duration
		want    time.Duration
	}{
		{100 * time.Millisecond, 200 * time.Millisecond},
		{400 * time.Millisecond, 800 * time.Millisecond},
		{500 * time.Millisecond, time.Second},
		{800 * time.Millisecond, time.Second},
		{time.Second, time.Second},
	}
	for _, tc := range tests {
		if got := l.nextBackoff(tc.current); got != tc.want {
			t.Errorf("nextBackoff(%v) = %v, want %v", tc.current, got, tc.want)
		}
	}
}

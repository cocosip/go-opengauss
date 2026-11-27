package opengauss

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cocosip/go-opengauss/internal/proto"
)

// Notification is one asynchronous message a server delivered for a NOTIFY.
type Notification struct {
	// PID is the process ID of the backend that sent the notification.
	PID int
	// Channel is the channel named by LISTEN and NOTIFY.
	Channel string
	// Extra is the NOTIFY payload, empty when the statement carried none.
	Extra string
}

// ListenerEvent identifies a listener session state change reported to an
// EventCallback.
type ListenerEvent int

const (
	// ListenerEventConnected reports the session opened by NewListener.
	ListenerEventConnected ListenerEvent = iota
	// ListenerEventDisconnected reports a lost session. Notifications
	// published after this event and before ListenerEventReconnected are lost,
	// so the application reconciles its state when the session returns.
	ListenerEventDisconnected
	// ListenerEventReconnected reports that the session is usable again and
	// every channel that had been registered is registered again.
	ListenerEventReconnected
)

// EventCallback observes listener session events. It runs on the listener's
// goroutine, so it must return quickly and must not call back into the
// Listener: Listen, Unlisten, Notify, Ping and Close all wait for that
// goroutine.
type EventCallback func(l *Listener, ev ListenerEvent, err error)

var (
	// ErrListenerClosed is returned by a command on a closed Listener.
	ErrListenerClosed = errors.New("opengauss: listener is closed")

	// errListenerReconnecting means the session is down and the command was
	// not sent. The channel set of the previous session is restored on
	// reconnect; a failed command is the caller's to retry.
	errListenerReconnecting = errors.New("opengauss: listener has no session: it is reconnecting")
)

// listenerNotificationBuffer holds delivered notifications until the consumer
// reads them. A full buffer stalls the listener session, which is the
// backpressure a slow consumer deserves.
const listenerNotificationBuffer = 10

// listenerIdlePing is how long the listening session may stay idle before the
// driver sends an empty statement. The server hands pending notifications over
// with the response to the session's next statement instead of waking an idle
// client, so without this probe a notification would wait until the
// application happened to run something on the listener. It is a variable so
// tests can shorten or disable it.
var listenerIdlePing = time.Second

// Listener keeps one dedicated session open for LISTEN/NOTIFY. It owns that
// connection outright and never takes one from a database/sql pool, because a
// pooled session could be handed to unrelated statements. Commands run with
// the simple query protocol outside any transaction, so a LISTEN takes effect
// as soon as it returns and a NOTIFY is delivered when its statement commits.
//
// Notifications are delivered in the order the server sent them on the channel
// returned by NotificationChannel. An idle session is probed with an empty
// statement once a second, because openGauss hands a published notification
// over with the response to a session's next statement instead of waking an
// idle client: without the probe a notification would wait until the
// application happened to use the listener. After a connection failure the
// listener reconnects with exponential backoff, registers every previously
// accepted channel again, and reports the lost interval through the
// EventCallback: the server does not retain notifications for an absent
// session.
type Listener struct {
	config               *Config
	callback             EventCallback
	minReconnectInterval time.Duration
	maxReconnectInterval time.Duration

	notifications chan *Notification
	requests      chan listenerRequest
	closed        chan struct{}
	done          chan struct{}
	closeOnce     sync.Once

	mu     sync.Mutex
	notice func(*Error)
}

// listenerRequest is one statement for the listener goroutine. Exactly one of
// register and unregister is set for the channel commands, so the set of
// accepted channels is only updated after the server acknowledged them.
type listenerRequest struct {
	statement  string
	register   string
	unregister string
	done       chan error
}

// listenerMessage is one backend message read from the listener session.
type listenerMessage struct {
	typ  byte
	body []byte
	err  error
}

// listenerFlight is the statement currently awaiting ReadyForQuery.
type listenerFlight struct {
	done        chan error
	resubscribe bool
	register    string
	unregister  string
}

// NewListener resolves dsn and opens the dedicated listening session. The
// connection attempt uses the DSN's connect_timeout. A listener is independent
// of the sql.DB reachable through the same DSN: applications that write to the
// database should keep using a pool and listen on this session. When the
// connection fails, reconnection attempts start at minReconnectInterval and
// double up to maxReconnectInterval; zero values select one second and one
// minute.
func NewListener(dsn string, minReconnectInterval, maxReconnectInterval time.Duration) (*Listener, error) {
	return newListener(dsn, nil, minReconnectInterval, maxReconnectInterval)
}

// NewListenerWithCallback is NewListener with a callback that reports the
// initial connection and every later disconnect and reconnect, so the
// application can detect the notification gap after a failure.
func NewListenerWithCallback(dsn string, callback EventCallback, minReconnectInterval, maxReconnectInterval time.Duration) (*Listener, error) {
	return newListener(dsn, callback, minReconnectInterval, maxReconnectInterval)
}

func newListener(dsn string, callback EventCallback, minReconnectInterval, maxReconnectInterval time.Duration) (*Listener, error) {
	config, err := parseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if config.SSLMode != SSLModeDisable {
		tlsConfig, err := config.buildTLSConfig()
		if err != nil {
			return nil, err
		}
		config.tlsConfig = tlsConfig
	}
	if minReconnectInterval <= 0 {
		minReconnectInterval = time.Second
	}
	if maxReconnectInterval <= 0 {
		maxReconnectInterval = time.Minute
	}
	if maxReconnectInterval < minReconnectInterval {
		maxReconnectInterval = minReconnectInterval
	}
	listener := &Listener{
		config:               config,
		callback:             callback,
		minReconnectInterval: minReconnectInterval,
		maxReconnectInterval: maxReconnectInterval,
		notifications:        make(chan *Notification, listenerNotificationBuffer),
		requests:             make(chan listenerRequest),
		closed:               make(chan struct{}),
		done:                 make(chan struct{}),
	}
	current, err := listener.open()
	if err != nil {
		return nil, err
	}
	go listener.loop(current)
	return listener, nil
}

// open dials, authenticates and applies the configured target_session_attrs,
// using the same rules as a pooled connection.
func (l *Listener) open() (*conn, error) {
	ctx := context.Background()
	if l.config.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, l.config.ConnectTimeout)
		defer cancel()
	}
	return connect(ctx, l.config)
}

// NotificationChannel delivers notifications until Close. Reading it is the
// application's responsibility: the listener session stalls while the buffer
// is full.
func (l *Listener) NotificationChannel() <-chan *Notification {
	return l.notifications
}

// Listen registers channel on the session. The registration is complete when
// Listen returns nil; notifications published before that are not delivered.
func (l *Listener) Listen(channel string) error {
	if err := validateChannelName(channel); err != nil {
		return err
	}
	return l.exec(listenerRequest{statement: "LISTEN " + quoteIdentifier(channel), register: channel})
}

// Unlisten removes channel from the session. Notifications already queued are
// still delivered.
func (l *Listener) Unlisten(channel string) error {
	if err := validateChannelName(channel); err != nil {
		return err
	}
	return l.exec(listenerRequest{statement: "UNLISTEN " + quoteIdentifier(channel), unregister: channel})
}

// Notify sends payload on channel. The statement commits immediately, so the
// server delivers it to every listening session, including this one.
func (l *Listener) Notify(channel, payload string) error {
	if err := validateChannelName(channel); err != nil {
		return err
	}
	if strings.IndexByte(payload, 0) >= 0 {
		return errors.New("opengauss: the notification payload contains a NUL byte")
	}
	statement := "NOTIFY " + quoteIdentifier(channel) + ", " + quoteLiteral(payload)
	return l.exec(listenerRequest{statement: statement})
}

// Ping checks that the session is alive. A listener whose peer vanished
// silently is detected by the next statement, by a TCP keep-alive failure or
// by this call.
func (l *Listener) Ping() error {
	// The empty statement is answered with EmptyQueryResponse and
	// ReadyForQuery, the cheapest round trip the protocol offers.
	return l.exec(listenerRequest{statement: ""})
}

// SetNoticeHandler registers a callback for NOTICE messages received on the
// listening session. A nil handler discards them. The callback runs on the
// listener's goroutine and must return quickly; it must not call back into the
// Listener.
func (l *Listener) SetNoticeHandler(handler func(*Error)) {
	l.mu.Lock()
	l.notice = handler
	l.mu.Unlock()
}

// Close ends the session, unblocks a waiting consumer and releases the
// connection. It is safe to call more than once. Notifications that were
// delivered but not read are discarded with the channel.
func (l *Listener) Close() error {
	l.closeOnce.Do(func() {
		close(l.closed)
		<-l.done
		close(l.notifications)
	})
	return nil
}

// exec hands one statement to the listener goroutine and waits for the server's
// answer. The request is never dropped: the goroutine answers it, and the
// cleanup path answers the ones that arrived during shutdown.
func (l *Listener) exec(req listenerRequest) error {
	req.done = make(chan error, 1)
	select {
	case <-l.closed:
		return ErrListenerClosed
	case l.requests <- req:
	}
	return <-req.done
}

// loop owns the session: it reads every backend message, serializes the
// statements of all callers and reconnects when the session is lost.
func (l *Listener) loop(current *conn) {
	var (
		incoming     chan listenerMessage
		abort        chan struct{}
		flight       *listenerFlight
		serverErr    error
		queue        []listenerRequest
		resubscribe  []string
		resubscribed bool
		backoff      = l.minReconnectInterval
		listened     = make(map[string]struct{})
	)

	// Cleanup first unblocks the reader and closes the socket, then answers
	// every statement that will never run, and finally releases Close.
	defer func() {
		if abort != nil {
			close(abort)
		}
		if current != nil {
			_ = current.Close()
		}
		if flight != nil && flight.done != nil {
			flight.done <- ErrListenerClosed
		}
		for _, req := range queue {
			req.done <- ErrListenerClosed
		}
		for {
			select {
			case req := <-l.requests:
				req.done <- ErrListenerClosed
			default:
				close(l.done)
				return
			}
		}
	}()

	// startReader makes the connection's reads visible to the loop. The abort
	// channel releases the reader when the loop discards this connection. The
	// reader does not touch conn's state: the loop owns c.bad, the transaction
	// status and the parameters, so the two goroutines never race on them.
	startReader := func(c *conn) (chan listenerMessage, chan struct{}) {
		messages := make(chan listenerMessage)
		abort := make(chan struct{})
		go func() {
			for {
				typ, body, err := proto.ReadMessage(c.br)
				select {
				case messages <- listenerMessage{typ: typ, body: body, err: err}:
				case <-abort:
					return
				}
				if err != nil {
					return
				}
			}
		}()
		return messages, abort
	}

	// disconnect drops the session and reports it. The in-flight statement and
	// the queued ones did not run; the accepted channel set is kept for the
	// next session.
	disconnect := func(cause error) {
		if abort != nil {
			close(abort)
			abort = nil
		}
		if current != nil {
			_ = current.Close()
			current = nil
		}
		incoming = nil
		if flight != nil {
			if flight.done != nil {
				flight.done <- cause
			}
			flight = nil
		}
		serverErr = nil
		for _, req := range queue {
			req.done <- cause
		}
		queue = nil
		resubscribe = nil
		resubscribed = false
		backoff = l.minReconnectInterval
		l.report(ListenerEventDisconnected, cause)
	}

	incoming, abort = startReader(current)
	l.report(ListenerEventConnected, nil)

	idle := time.NewTimer(listenerIdlePing)
	defer idle.Stop()

	for {
		// Reconnect until a session is available again. While the session is
		// down, statements fail instead of waiting for an unknown outage.
		if current == nil {
			retryAt := time.Now().Add(backoff)
			for current == nil {
				wait := time.NewTimer(time.Until(retryAt))
				select {
				case <-l.closed:
					wait.Stop()
					return
				case req := <-l.requests:
					wait.Stop()
					req.done <- errListenerReconnecting
					continue
				case <-wait.C:
				}
				reconnected, err := l.open()
				if err != nil {
					backoff = l.nextBackoff(backoff)
					retryAt = time.Now().Add(backoff)
					l.report(ListenerEventDisconnected, err)
					continue
				}
				current = reconnected
				incoming, abort = startReader(current)
				backoff = l.minReconnectInterval
				resubscribe = resubscribe[:0]
				for channel := range listened {
					resubscribe = append(resubscribe, channel)
				}
				resubscribed = true
			}
		}

		// Start the next statement when the session is free: first the
		// channels of the previous session, then the callers' requests.
		if flight == nil {
			switch {
			case len(resubscribe) > 0:
				channel := resubscribe[0]
				resubscribe = resubscribe[1:]
				flight = &listenerFlight{resubscribe: true}
				if err := sendSimpleQuery(current, "LISTEN "+quoteIdentifier(channel)); err != nil {
					disconnect(err)
					continue
				}
			case resubscribed:
				resubscribed = false
				l.report(ListenerEventReconnected, nil)
			case len(queue) > 0:
				req := queue[0]
				queue = queue[1:]
				flight = &listenerFlight{done: req.done, register: req.register, unregister: req.unregister}
				if err := sendSimpleQuery(current, req.statement); err != nil {
					disconnect(err)
					continue
				}
			}
		}

		// The idle probe only runs while nothing else owns the session, and the
		// timer measures the time spent waiting here.
		if !idle.Stop() {
			select {
			case <-idle.C:
			default:
			}
		}
		idle.Reset(listenerIdlePing)
		var idleC <-chan time.Time
		if current != nil && flight == nil && len(queue) == 0 && len(resubscribe) == 0 && !resubscribed {
			idleC = idle.C
		}

		select {
		case <-l.closed:
			return
		case req := <-l.requests:
			queue = append(queue, req)
		case <-idleC:
			// Prompt the server to hand over notifications it is holding
			// back, without reporting anything to a caller.
			flight = &listenerFlight{}
			if err := sendSimpleQuery(current, ""); err != nil {
				disconnect(err)
				continue
			}
		case msg := <-incoming:
			if msg.err != nil {
				disconnect(msg.err)
				continue
			}
			switch msg.typ {
			case proto.MsgNotificationResponse:
				notification, err := parseNotification(msg.body)
				if err != nil {
					disconnect(err)
					continue
				}
				select {
				case l.notifications <- notification:
				case <-l.closed:
					return
				}
			case proto.MsgNoticeResponse:
				l.handleNotice(parseErrorResponse(msg.body))
			case proto.MsgParameterStatus:
				current.storeParameter(msg.body)
				if current.encodingErr != nil {
					disconnect(current.encodingErr)
				} else if current.bad {
					disconnect(proto.ErrMalformedMessage)
				}
			case proto.MsgCommandComplete, proto.MsgEmptyQueryResponse:
				// Part of the statement currently awaiting ReadyForQuery.
				if flight == nil {
					disconnect(fmt.Errorf("opengauss: unexpected message %q on a listener session", msg.typ))
				}
			case proto.MsgErrorResponse:
				serverErr = parseErrorResponse(msg.body)
			case proto.MsgReadyForQuery:
				current.setTxnStatus(msg.body)
				if current.bad {
					disconnect(proto.ErrMalformedMessage)
					continue
				}
				if flight == nil {
					disconnect(errors.New("opengauss: unexpected ReadyForQuery on a listener session"))
					continue
				}
				err := serverErr
				serverErr = nil
				if flight.resubscribe {
					flight = nil
					if err != nil {
						// A channel that was accepted on the previous session
						// cannot be registered again, so the new session is
						// unusable for this listener.
						disconnect(err)
						continue
					}
					if len(resubscribe) == 0 {
						resubscribed = false
						l.report(ListenerEventReconnected, nil)
					}
					continue
				}
				if err == nil {
					if flight.register != "" {
						listened[flight.register] = struct{}{}
					}
					if flight.unregister != "" {
						delete(listened, flight.unregister)
					}
				}
				if flight.done != nil {
					flight.done <- err
				} else if err != nil {
					// The idle probe found a broken session.
					disconnect(err)
					continue
				}
				flight = nil
			default:
				disconnect(fmt.Errorf("opengauss: unexpected message type %q on a listener session", msg.typ))
			}
		}
	}
}

// nextBackoff doubles the delay, capped at the configured maximum.
func (l *Listener) nextBackoff(current time.Duration) time.Duration {
	if current >= l.maxReconnectInterval/2 {
		return l.maxReconnectInterval
	}
	return current * 2
}

func (l *Listener) report(ev ListenerEvent, err error) {
	if l.callback != nil {
		l.callback(l, ev, err)
	}
}

func (l *Listener) handleNotice(notice *Error) {
	l.mu.Lock()
	handler := l.notice
	l.mu.Unlock()
	if handler != nil {
		handler(notice)
	}
}

// sendSimpleQuery writes one statement without parameters. The listener loop
// is the only writer on its session.
func sendSimpleQuery(c *conn, statement string) error {
	query := proto.NewMessageWriter(proto.MsgQuery)
	query.String(statement)
	return c.sendFrames(query.Finish())
}

// parseNotification decodes a NotificationResponse body.
func parseNotification(body []byte) (*Notification, error) {
	r := &proto.ReadBuf{B: body}
	pid := r.Int32()
	channel := r.String()
	payload := r.String()
	if r.Err != nil {
		return nil, r.Err
	}
	return &Notification{PID: int(pid), Channel: channel, Extra: payload}, nil
}

func validateChannelName(channel string) error {
	if channel == "" {
		return errors.New("opengauss: the notification channel name is empty")
	}
	if strings.IndexByte(channel, 0) >= 0 {
		return errors.New("opengauss: the notification channel name contains a NUL byte")
	}
	return nil
}

// quoteLiteral renders a string literal with E-string escaping, so backslashes
// stay literal whatever standard_conforming_strings the session uses.
func quoteLiteral(text string) string {
	var b strings.Builder
	b.Grow(len(text) + 3)
	b.WriteString("E'")
	for i := 0; i < len(text); i++ {
		switch c := text[i]; c {
		case '\'':
			b.WriteString("''")
		case '\\':
			b.WriteString(`\\`)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('\'')
	return b.String()
}

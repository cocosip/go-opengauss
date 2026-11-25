package opengauss

import (
	"bufio"
	"context"
	"crypto/tls"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

// Connection stages used to report where a failure occurred and to decide
// whether a TLS fallback is still allowed.
const (
	stageDial           = "dial"
	stageTLS            = "tls"
	stageStartup        = "startup"
	stageAuthentication = "authentication"
)

// errSSLNotSupported reports that the server answered the SSLRequest with "N".
var errSSLNotSupported = errors.New("server does not support SSL")

// stageError records the connection stage that failed while preserving the
// underlying cause for errors.Is and errors.As.
type stageError struct {
	stage string
	err   error
}

func (e *stageError) Error() string { return "opengauss: " + e.stage + ": " + e.err.Error() }

func (e *stageError) Unwrap() error { return e.err }

func stageOf(err error) string {
	var target *stageError
	if errors.As(err, &target) {
		return target.stage
	}
	return ""
}

type conn struct {
	netConn net.Conn
	br      *bufio.Reader
	config  *Config

	// Facts observed during startup and refreshed by ReadyForQuery.
	parameters  map[string]string
	backendPID  int32
	backendKey  int32
	txnStatus   byte
	stmtCounter int

	// bad marks a connection whose protocol state can no longer be trusted.
	// Public methods return driver.ErrBadConn once it is set so database/sql
	// discards the connection instead of reusing a desynchronized session.
	bad bool
}

// connect opens one physical connection according to the configured TLS
// policy, falling back only where the policy allows it.
func connect(ctx context.Context, cfg *Config) (*conn, error) {
	switch cfg.SSLMode {
	case SSLModeDisable:
		return connectOnce(ctx, cfg, false)
	case SSLModeAllow:
		plain, plainErr := connectOnce(ctx, cfg, false)
		if plainErr == nil {
			return plain, nil
		}
		// A plaintext attempt may be refused by the server's host-based
		// authentication. Retrying over TLS is only safe before a password
		// reached the server, so an authentication failure is not retried.
		if stageOf(plainErr) != stageStartup {
			return nil, plainErr
		}
		secure, secureErr := connectOnce(ctx, cfg, true)
		if secureErr == nil {
			return secure, nil
		}
		return nil, joinConnectErrors(cfg, plainErr, secureErr)
	case SSLModePrefer:
		secure, secureErr := connectOnce(ctx, cfg, true)
		if secureErr == nil {
			return secure, nil
		}
		if stageOf(secureErr) != stageTLS {
			return nil, secureErr
		}
		plain, plainErr := connectOnce(ctx, cfg, false)
		if plainErr == nil {
			return plain, nil
		}
		return nil, joinConnectErrors(cfg, secureErr, plainErr)
	default:
		return connectOnce(ctx, cfg, true)
	}
}

func joinConnectErrors(cfg *Config, errs ...error) error {
	return fmt.Errorf("opengauss: connect to %s: %w", cfg, errors.Join(errs...))
}

func connectOnce(ctx context.Context, cfg *Config, useTLS bool) (*conn, error) {
	netConn, err := dial(ctx, cfg)
	if err != nil {
		return nil, &stageError{stageDial, err}
	}

	c := &conn{
		netConn: netConn,
		br:      bufio.NewReader(netConn),
		config:  cfg,
	}
	connected := false
	defer func() {
		if !connected {
			_ = netConn.Close()
		}
	}()

	watcher := watchCancellation(ctx, netConn)
	defer watcher.stop()

	if err := c.setStartupDeadline(ctx); err != nil {
		return nil, &stageError{stageDial, err}
	}
	if useTLS {
		if err := c.negotiateTLS(ctx); err != nil {
			return nil, &stageError{stageTLS, err}
		}
	}
	if err := c.startup(ctx); err != nil {
		return nil, err
	}

	watcher.stop()
	if err := netConn.SetDeadline(time.Time{}); err != nil {
		return nil, &stageError{stageDial, err}
	}
	connected = true
	return c, nil
}

func dial(ctx context.Context, cfg *Config) (net.Conn, error) {
	dialer := net.Dialer{Timeout: cfg.ConnectTimeout}
	address := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	netConn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = errors.Join(ctxErr, err)
		}
		return nil, fmt.Errorf("dial %s: %w", address, err)
	}
	return netConn, nil
}

// contextCause reports the caller's context cause when it has ended, including
// the case where the socket deadline fired marginally before the context
// timer became observable.
func contextCause(ctx context.Context) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// setStartupDeadline bounds the whole startup handshake by the caller's
// deadline and by connect_timeout, whichever ends first.
func (c *conn) setStartupDeadline(ctx context.Context) error {
	var deadline time.Time
	if c.config.ConnectTimeout > 0 {
		deadline = time.Now().Add(c.config.ConnectTimeout)
	}
	if ctxDeadline, ok := ctx.Deadline(); ok && (deadline.IsZero() || ctxDeadline.Before(deadline)) {
		deadline = ctxDeadline
	}
	if deadline.IsZero() {
		return nil
	}
	return c.netConn.SetDeadline(deadline)
}

// negotiateTLS performs the SSLRequest exchange and upgrades the connection.
// A plaintext response is rejected so a verify-* policy never degrades.
func (c *conn) negotiateTLS(ctx context.Context) error {
	request := make([]byte, 8)
	binary.BigEndian.PutUint32(request[0:4], 8)
	binary.BigEndian.PutUint32(request[4:8], uint32(sslRequestCode))
	if _, err := c.netConn.Write(request); err != nil {
		return err
	}

	var response [1]byte
	if _, err := io.ReadFull(c.br, response[:]); err != nil {
		return err
	}
	switch response[0] {
	case 'S':
		tlsCfg := c.config.tlsConfig
		if tlsCfg == nil {
			return errors.New("TLS configuration is missing")
		}
		tlsConn := tls.Client(c.netConn, tlsCfg)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		}
		c.netConn = tlsConn
		c.br = bufio.NewReader(tlsConn)
		return nil
	case 'N':
		return errSSLNotSupported
	default:
		return fmt.Errorf("unexpected SSLRequest response %q", response[0])
	}
}

// startupFrame builds the protocol 3.51 StartupMessage.
func startupFrame(cfg *Config) []byte {
	body := binary.BigEndian.AppendUint32(nil, uint32(protocolVersion351))
	appendStartupParameter := func(key, value string) {
		body = append(body, key...)
		body = append(body, 0)
		body = append(body, value...)
		body = append(body, 0)
	}
	appendStartupParameter("user", cfg.User)
	appendStartupParameter("database", cfg.Database)
	appendStartupParameter("client_encoding", "UTF8")
	body = append(body, 0)

	frame := binary.BigEndian.AppendUint32(nil, uint32(len(body)+4))
	return append(frame, body...)
}

// startup authenticates and waits for ReadyForQuery. A connection is only
// handed to database/sql after the server declared the session ready.
func (c *conn) startup(ctx context.Context) error {
	if err := c.sendFrames(startupFrame(c.config)); err != nil {
		return &stageError{stageStartup, err}
	}

	passwordSent := false
	for {
		typ, body, err := c.recv()
		if err != nil {
			if cause := contextCause(ctx); cause != nil {
				err = cause
			}
			return &stageError{stageStartup, err}
		}
		switch typ {
		case msgAuthenticationRequest:
			r := &readBuf{b: body}
			code := r.int32()
			if r.err != nil {
				return &stageError{stageAuthentication, r.err}
			}
			payload, authErr := authResponse(code, r, c.config)
			if authErr != nil {
				return &stageError{stageAuthentication, authErr}
			}
			if r.err != nil {
				return &stageError{stageAuthentication, r.err}
			}
			if payload != nil {
				message := newMessageWriter(msgPasswordMessage)
				message.bytes(payload)
				if err := c.sendFrames(message.finish()); err != nil {
					return &stageError{stageAuthentication, err}
				}
				passwordSent = true
			}
		case msgParameterStatus:
			c.storeParameter(body)
		case msgBackendKeyData:
			r := &readBuf{b: body}
			c.backendPID = r.int32()
			c.backendKey = r.int32()
			if r.err != nil {
				return &stageError{stageStartup, r.err}
			}
		case msgNoticeResponse, msgNotificationResponse:
			// Startup notices and notifications are informational; consume and
			// continue.
		case msgErrorResponse:
			stage := stageStartup
			if passwordSent {
				stage = stageAuthentication
			}
			return &stageError{stage, parseErrorResponse(body)}
		case msgReadyForQuery:
			c.setTxnStatus(body)
			if c.bad {
				return &stageError{stageStartup, errMalformedMessage}
			}
			return nil
		default:
			return &stageError{stageStartup, fmt.Errorf("unexpected message type %q during startup", typ)}
		}
	}
}

func (c *conn) storeParameter(body []byte) {
	r := &readBuf{b: body}
	name := r.string()
	value := r.string()
	if r.err != nil {
		c.bad = true
		return
	}
	if c.parameters == nil {
		c.parameters = make(map[string]string)
	}
	c.parameters[name] = value
}

func (c *conn) recv() (byte, []byte, error) {
	typ, body, err := readMessage(c.br)
	if err != nil {
		c.bad = true
	}
	return typ, body, err
}

// sendFrames coalesces complete messages into one write.
func (c *conn) sendFrames(frames ...[]byte) error {
	var buf []byte
	for _, frame := range frames {
		buf = append(buf, frame...)
	}
	if _, err := c.netConn.Write(buf); err != nil {
		c.bad = true
		return err
	}
	return nil
}

func (c *conn) setTxnStatus(body []byte) {
	if len(body) < 1 {
		c.bad = true
		return
	}
	switch status := body[0]; status {
	case txnIdle, txnInTransaction, txnFailed:
		c.txnStatus = status
	default:
		c.bad = true
	}
}

// cancelWatcher interrupts in-flight I/O when the caller's context ends. The
// protocol state is unknown after an interruption, so the caller marks the
// connection bad. A nil watcher means the context cannot be canceled.
type cancelWatcher struct {
	netConn net.Conn
	done    chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func watchCancellation(ctx context.Context, netConn net.Conn) *cancelWatcher {
	if ctx.Done() == nil {
		return nil
	}
	watcher := &cancelWatcher{
		netConn: netConn,
		done:    make(chan struct{}),
		exited:  make(chan struct{}),
	}
	go func() {
		defer close(watcher.exited)
		select {
		case <-ctx.Done():
			_ = netConn.SetDeadline(time.Now())
		case <-watcher.done:
		}
	}()
	return watcher
}

// stop waits until the watcher goroutine has finished, so a deadline cleared
// afterwards cannot be overwritten by a late cancellation.
func (w *cancelWatcher) stop() {
	if w == nil {
		return
	}
	w.once.Do(func() { close(w.done) })
	<-w.exited
}

func (c *conn) beginOperation(ctx context.Context) (*cancelWatcher, error) {
	if c.bad {
		return nil, driver.ErrBadConn
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.netConn.SetDeadline(deadline); err != nil {
			c.bad = true
			return nil, err
		}
	}
	return watchCancellation(ctx, c.netConn), nil
}

func (c *conn) endOperation(watcher *cancelWatcher) {
	watcher.stop()
	if !c.bad {
		_ = c.netConn.SetDeadline(time.Time{})
	}
}

// wrapIOError adds the operation stage and preserves the caller's context
// cause. Server-reported errors pass through unchanged so errors.As keeps
// working.
func (c *conn) wrapIOError(ctx context.Context, stage string, err error) error {
	var serverErr *Error
	if errors.As(err, &serverErr) {
		return err
	}
	if cause := contextCause(ctx); cause != nil {
		err = errors.Join(cause, err)
	}
	return &stageError{stage, err}
}

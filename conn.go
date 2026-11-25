package opengauss

import (
	"bufio"
	"context"
	"crypto/tls"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/cocosip/go-opengauss/internal/auth"
	"github.com/cocosip/go-opengauss/internal/proto"
	"io"
	"net"
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

// failureHasStage reports whether any failure inside err, including the
// members of a joined TLS fallback, happened at the given stage.
func failureHasStage(err error, stage string) bool {
	if err == nil {
		return false
	}
	if target, ok := err.(*stageError); ok && target.stage == stage {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if failureHasStage(child, stage) {
				return true
			}
		}
		return false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return failureHasStage(wrapped.Unwrap(), stage)
	}
	return false
}

type conn struct {
	netConn  net.Conn
	br       *bufio.Reader
	config   *Config
	target   Target
	password string

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

// connect opens one physical connection. Targets are tried in order, and only
// the TLS fallback rules of the configured policy may retry within a target.
func connect(ctx context.Context, cfg *Config) (*conn, error) {
	errs := make([]error, 0, len(cfg.Targets))
	for _, target := range cfg.Targets {
		c, err := connectTarget(ctx, cfg, target)
		if err == nil {
			return c, nil
		}
		errs = append(errs, fmt.Errorf("opengauss: connect to %s: %w", target, err))
		// libpq stops at an authentication failure: the credentials were
		// rejected, so they are not replayed against the remaining targets.
		// The check covers the second attempt of an allow/prefer fallback,
		// where the joined failure is not the first error of the join.
		if failureHasStage(err, stageAuthentication) {
			break
		}
		if ctx.Err() != nil {
			break
		}
	}
	return nil, fmt.Errorf("opengauss: no target accepted a connection: %w", errors.Join(errs...))
}

// connectTarget applies the TLS fallback policy of one endpoint. The
// connect_timeout covers every attempt against that endpoint.
func connectTarget(ctx context.Context, cfg *Config, target Target) (*conn, error) {
	password, err := cfg.passwordForTarget(target)
	if err != nil {
		return nil, err
	}
	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
	}
	switch cfg.SSLMode {
	case SSLModeDisable:
		return connectOnce(ctx, cfg, target, password, false)
	case SSLModeAllow:
		plain, plainErr := connectOnce(ctx, cfg, target, password, false)
		if plainErr == nil {
			return plain, nil
		}
		// A plaintext attempt may be refused by the server's host-based
		// authentication. Retrying over TLS is only safe before a password
		// reached the server, so an authentication failure is not retried.
		if stageOf(plainErr) != stageStartup {
			return nil, plainErr
		}
		secure, secureErr := connectOnce(ctx, cfg, target, password, true)
		if secureErr == nil {
			return secure, nil
		}
		return nil, errors.Join(plainErr, secureErr)
	case SSLModePrefer:
		secure, secureErr := connectOnce(ctx, cfg, target, password, true)
		if secureErr == nil {
			return secure, nil
		}
		if stageOf(secureErr) != stageTLS {
			return nil, secureErr
		}
		plain, plainErr := connectOnce(ctx, cfg, target, password, false)
		if plainErr == nil {
			return plain, nil
		}
		return nil, errors.Join(secureErr, plainErr)
	default:
		return connectOnce(ctx, cfg, target, password, true)
	}
}

func connectOnce(ctx context.Context, cfg *Config, target Target, password string, useTLS bool) (*conn, error) {
	netConn, err := dial(ctx, target)
	if err != nil {
		return nil, &stageError{stageDial, err}
	}

	c := &conn{
		netConn:  netConn,
		br:       bufio.NewReader(netConn),
		config:   cfg,
		target:   target,
		password: password,
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

func dial(ctx context.Context, target Target) (net.Conn, error) {
	dialer := net.Dialer{}
	address := target.String()
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
// deadline, which connectTarget already restricted to connect_timeout.
func (c *conn) setStartupDeadline(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil
	}
	if err := c.netConn.SetDeadline(deadline); err != nil {
		return err
	}
	return nil
}

// negotiateTLS performs the SSLRequest exchange and upgrades the connection.
// A plaintext response is rejected so a verify-* policy never degrades.
func (c *conn) negotiateTLS(ctx context.Context) error {
	request := make([]byte, 8)
	binary.BigEndian.PutUint32(request[0:4], 8)
	binary.BigEndian.PutUint32(request[4:8], uint32(proto.SSLRequestCode))
	if _, err := c.netConn.Write(request); err != nil {
		return err
	}

	var response [1]byte
	if _, err := io.ReadFull(c.br, response[:]); err != nil {
		return err
	}
	switch response[0] {
	case 'S':
		if c.config.tlsConfig == nil {
			return errors.New("TLS configuration is missing")
		}
		// The DNS name belongs to the endpoint actually dialed, which differs
		// between targets.
		tlsCfg := c.config.tlsConfig.Clone()
		tlsCfg.ServerName = c.target.Host
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
	body := binary.BigEndian.AppendUint32(nil, uint32(proto.ProtocolVersion351))
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
		case proto.MsgAuthenticationRequest:
			r := &proto.ReadBuf{B: body}
			code := r.Int32()
			if r.Err != nil {
				return &stageError{stageAuthentication, r.Err}
			}
			payload, authErr := auth.Response(code, r, c.config.User, c.password)
			if authErr != nil {
				return &stageError{stageAuthentication, authErr}
			}
			if r.Err != nil {
				return &stageError{stageAuthentication, r.Err}
			}
			if payload != nil {
				message := proto.NewMessageWriter(proto.MsgPasswordMessage)
				message.Bytes(payload)
				if err := c.sendFrames(message.Finish()); err != nil {
					return &stageError{stageAuthentication, err}
				}
				passwordSent = true
			}
		case proto.MsgParameterStatus:
			c.storeParameter(body)
		case proto.MsgBackendKeyData:
			r := &proto.ReadBuf{B: body}
			c.backendPID = r.Int32()
			c.backendKey = r.Int32()
			if r.Err != nil {
				return &stageError{stageStartup, r.Err}
			}
		case proto.MsgNoticeResponse, proto.MsgNotificationResponse:
			// Startup notices and notifications are informational; consume and
			// continue.
		case proto.MsgErrorResponse:
			stage := stageStartup
			if passwordSent {
				stage = stageAuthentication
			}
			return &stageError{stage, parseErrorResponse(body)}
		case proto.MsgReadyForQuery:
			c.setTxnStatus(body)
			if c.bad {
				return &stageError{stageStartup, proto.ErrMalformedMessage}
			}
			return nil
		default:
			return &stageError{stageStartup, fmt.Errorf("unexpected message type %q during startup", typ)}
		}
	}
}

func (c *conn) storeParameter(body []byte) {
	r := &proto.ReadBuf{B: body}
	name := r.String()
	value := r.String()
	if r.Err != nil {
		c.bad = true
		return
	}
	if c.parameters == nil {
		c.parameters = make(map[string]string)
	}
	c.parameters[name] = value
}

func (c *conn) recv() (byte, []byte, error) {
	typ, body, err := proto.ReadMessage(c.br)
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
	case proto.TxnIdle, proto.TxnInTransaction, proto.TxnFailed:
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

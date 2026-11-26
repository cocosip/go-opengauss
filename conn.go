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
	"strings"
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
	stageSessionState   = "target_session_attrs"
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

	// encodingErr records a client_encoding the driver cannot decode. The
	// connection is unusable once it is set: text would arrive in an encoding
	// the driver treats as UTF-8.
	encodingErr error

	// An open COPY stream owns the session until CopyDone: copying records
	// that state, copyOwner is the statement that may extend the stream,
	// copyColumns is the row width from CopyInResponse, copyRowBuf is reused
	// for row encoding, and the watcher and context belong to the call that
	// started the stream.
	copying     bool
	copyOwner   *copyStmt
	copyColumns int
	copyRowBuf  []byte
	copyWatcher *cancelWatcher
	copyCtx     context.Context

	// bad marks a connection whose protocol state can no longer be trusted.
	// Public methods return driver.ErrBadConn once it is set so database/sql
	// discards the connection instead of reusing a desynchronized session.
	bad bool
}

// connect opens one physical connection. Targets are tried in order, and only
// the TLS fallback rules of the configured policy may retry within a target.
func connect(ctx context.Context, cfg *Config) (*conn, error) {
	errs := make([]error, 0, len(cfg.Targets))
	var fallback *conn
	for _, target := range cfg.Targets {
		c, state, err := connectTarget(ctx, cfg, target)
		if err != nil {
			errs = append(errs, fmt.Errorf("opengauss: connect to %s: %w", target, err))
			// libpq stops at an authentication failure: the credentials were
			// rejected, so they are not replayed against the remaining
			// targets. The check covers the second attempt of an allow/prefer
			// fallback, where the joined failure is not the first error.
			if failureHasStage(err, stageAuthentication) {
				// Even a prefer-standby fallback must not hide a rejected
				// password: the credentials are wrong for every target.
				if fallback != nil {
					_ = fallback.Close()
					fallback = nil
				}
				break
			}
			if ctx.Err() != nil {
				break
			}
			continue
		}

		if cfg.TargetSessionAttrs == "" || cfg.TargetSessionAttrs == TargetSessionAttrsAny {
			return c, nil
		}
		if cfg.TargetSessionAttrs.satisfiedBy(state) {
			if fallback != nil {
				_ = fallback.Close()
			}
			return c, nil
		}
		errs = append(errs, fmt.Errorf("opengauss: connect to %s: target_session_attrs=%s rejected a %s server",
			target, cfg.TargetSessionAttrs, state))
		if cfg.TargetSessionAttrs == TargetSessionAttrsPreferStandby {
			// Keep the first acceptable server in case no standby exists.
			if fallback == nil {
				fallback = c
			} else {
				_ = c.Close()
			}
			continue
		}
		_ = c.Close()
	}
	if fallback != nil {
		return fallback, nil
	}
	return nil, fmt.Errorf("opengauss: no target accepted a connection: %w", errors.Join(errs...))
}

// connectTarget opens one endpoint under the TLS fallback policy and reports the
// server's session state when target_session_attrs asks for it. The
// connect_timeout covers every attempt against that endpoint, including the
// state probe.
func connectTarget(ctx context.Context, cfg *Config, target Target) (*conn, sessionState, error) {
	password, err := cfg.passwordForTarget(target)
	if err != nil {
		return nil, sessionState{}, err
	}
	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
	}
	c, err := connectForPolicy(ctx, cfg, target, password)
	if err != nil {
		return nil, sessionState{}, err
	}
	if cfg.TargetSessionAttrs == "" || cfg.TargetSessionAttrs == TargetSessionAttrsAny {
		return c, sessionState{}, nil
	}
	state, err := c.detectSessionState(ctx)
	if err != nil {
		_ = c.Close()
		return nil, sessionState{}, &stageError{stageSessionState, err}
	}
	return c, state, nil
}

// connectForPolicy applies the TLS fallback rules of one endpoint.
func connectForPolicy(ctx context.Context, cfg *Config, target Target, password string) (*conn, error) {
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
		// reached the server, so an authentication or encoding failure is not
		// retried.
		if stageOf(plainErr) != stageStartup || errors.Is(plainErr, errUnsupportedClientEncoding) {
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
	netConn, err := dial(ctx, target, cfg.dialFunc)
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

	watcher := c.watchCancellation(ctx, false)
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

func dial(ctx context.Context, target Target, dialFunc DialFunc) (net.Conn, error) {
	if dialFunc == nil {
		var dialer net.Dialer
		dialFunc = dialer.DialContext
	}
	address := target.dialAddress()
	netConn, err := dialFunc(ctx, target.network(), address)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = errors.Join(ctxErr, err)
		}
		return nil, fmt.Errorf("dial %s: %w", address, err)
	}
	return netConn, nil
}

// tlsServerName returns the name used for certificate verification. A Unix
// socket has no DNS name, so libpq treats verify-full there as verify-ca: the
// chain is verified and the host name is not.
func tlsServerName(target Target) string {
	if target.isUnix() {
		return ""
	}
	return target.Host
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
		if tlsCfg.ServerName == "" {
			tlsCfg.ServerName = tlsServerName(c.target)
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
	if cfg.ApplicationName != "" {
		appendStartupParameter("application_name", cfg.ApplicationName)
	}
	if cfg.Options != "" {
		appendStartupParameter("options", cfg.Options)
	}
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
			if c.encodingErr != nil {
				return &stageError{stageStartup, c.encodingErr}
			}
			if c.bad {
				return &stageError{stageStartup, proto.ErrMalformedMessage}
			}
			return nil
		default:
			return &stageError{stageStartup, fmt.Errorf("unexpected message type %q during startup", typ)}
		}
	}
}

// errUnsupportedClientEncoding identifies a session whose client encoding the
// driver cannot decode as UTF-8. A TLS fallback must not retry it, because the
// password already reached the server.
var errUnsupportedClientEncoding = errors.New("opengauss: unsupported client encoding")

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
	if strings.EqualFold(name, "client_encoding") && !supportedClientEncoding(value) {
		c.bad = true
		c.encodingErr = fmt.Errorf("%w: the server reports client_encoding %q, but this driver supports only UTF8 and SQL_ASCII", errUnsupportedClientEncoding, value)
	}
}

// supportedClientEncoding reports whether text in this client encoding can be
// passed through as Go strings. UTF8 is the encoding the driver requests;
// SQL_ASCII stores bytes without conversion, so the driver passes them through
// unchanged instead of interpreting them.
func supportedClientEncoding(name string) bool {
	return strings.EqualFold(name, "UTF8") || strings.EqualFold(name, "SQL_ASCII")
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

// cancelGrace bounds how long an interrupted operation waits for the server to
// acknowledge a CancelRequest. If no answer arrives the socket deadline forces
// the read to fail, and the caller discards the unsynchronized session.
const cancelGrace = time.Second

// cancelWatcher interrupts in-flight I/O when the caller's context ends. For an
// operation it first asks the server to abort the running statement on a
// separate connection, so a cooperative server answers with an error and the
// session stays synchronized. A nil watcher means the context cannot be
// canceled.
type cancelWatcher struct {
	conn    *conn
	netConn net.Conn
	done    chan struct{}
	exited  chan struct{}
	once    sync.Once

	// cancelSent and cancelConfirmed are written by the watcher goroutine
	// before it closes exited, and read by endOperation afterwards.
	cancelSent      bool
	cancelConfirmed bool
}

// watchCancellation starts the watcher. requestCancel selects the CancelRequest
// exchange, which is only meaningful once a backend key is known.
func (c *conn) watchCancellation(ctx context.Context, requestCancel bool) *cancelWatcher {
	if ctx.Done() == nil {
		return nil
	}
	watcher := &cancelWatcher{
		conn:    c,
		netConn: c.netConn,
		done:    make(chan struct{}),
		exited:  make(chan struct{}),
	}
	go func() {
		defer close(watcher.exited)
		select {
		case <-ctx.Done():
			if !requestCancel {
				// During startup there is no backend to cancel, so interrupt
				// the handshake immediately.
				_ = watcher.netConn.SetDeadline(time.Now())
				return
			}
			// Bound the pending read before the cancel exchange, so a slow
			// cancel cannot delay the caller past the grace period.
			_ = watcher.netConn.SetDeadline(time.Now().Add(cancelGrace))
			watcher.cancelSent = true
			watcher.cancelConfirmed = c.sendCancelRequest()
		case <-watcher.done:
		}
	}()
	return watcher
}

// sendCancelRequest asks the server to abort the statement running on this
// backend and reports whether the postmaster confirmed the request by closing
// the cancel connection. Every failure is ignored: the interrupted operation
// still fails locally through the socket deadline.
func (c *conn) sendCancelRequest() bool {
	if c.backendPID == 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), cancelGrace)
	defer cancel()

	// Cancel the exact peer this session is connected to. Re-resolving the
	// host name could reach a different server where the key means nothing,
	// and a Unix socket has no TCP address to reuse.
	network, address := "tcp", ""
	if remote := c.netConn.RemoteAddr(); remote != nil {
		address = remote.String()
	}
	if c.target.isUnix() {
		network, address = "unix", c.target.dialAddress()
	}
	var dialer net.Dialer
	netConn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return false
	}
	defer func() { _ = netConn.Close() }()
	if err := netConn.SetDeadline(time.Now().Add(cancelGrace)); err != nil {
		return false
	}

	request := make([]byte, 16)
	binary.BigEndian.PutUint32(request[0:4], 16)
	binary.BigEndian.PutUint32(request[4:8], uint32(proto.CancelRequestCode))
	binary.BigEndian.PutUint32(request[8:12], uint32(c.backendPID))
	binary.BigEndian.PutUint32(request[12:16], uint32(c.backendKey))
	if _, err := netConn.Write(request); err != nil {
		return false
	}
	// The postmaster closes the connection once it has processed the request.
	// Waiting for that end of file confirms the cancel cannot still abort a
	// later statement on this session.
	if _, err := io.Copy(io.Discard, netConn); err != nil {
		return false
	}
	return true
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

// badConnError explains why the session is unusable. An unsupported client
// encoding is reported instead of driver.ErrBadConn so the caller learns the
// cause, while database/sql still discards the session through ResetSession.
func (c *conn) badConnError() error {
	if c.encodingErr != nil {
		return c.encodingErr
	}
	return driver.ErrBadConn
}

func (c *conn) beginOperation(ctx context.Context) (*cancelWatcher, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.bad {
		return nil, c.badConnError()
	}
	// A session in the middle of a COPY stream belongs to that stream until it
	// finishes; any other statement would interleave with the CopyData frames.
	if c.copying {
		return nil, errors.New("opengauss: a COPY stream is still open on this session")
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.netConn.SetDeadline(deadline); err != nil {
			c.bad = true
			return nil, err
		}
	}
	return c.watchCancellation(ctx, true), nil
}

func (c *conn) endOperation(watcher *cancelWatcher) {
	watcher.stop()
	if !c.bad && watcher != nil && watcher.cancelSent && !watcher.cancelConfirmed {
		// The postmaster never acknowledged the cancel, so it may still be
		// delivered and abort a later statement on this session.
		c.bad = true
	}
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
		// The reply was not consumed, so the session may hold a pending result
		// and must not return to the pool.
		c.bad = true
		err = errors.Join(cause, err)
	}
	return &stageError{stage, err}
}

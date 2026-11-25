package opengauss

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
)

// Credentials used by the scripted peer. The password matches the published
// SHA256 known-answer vector in auth_test.go.
const (
	testUser     = "alice"
	testDatabase = "app"
	testPassword = "sha256@abc123"
)

// fakeServer is a scripted openGauss backend for offline protocol tests. Each
// connection runs the same handler, which asserts the exact client messages.
type fakeServer struct {
	t        *testing.T
	ln       net.Listener
	handle   func(*fakeSession)
	wg       sync.WaitGroup
	accepted atomic.Int32
}

func newFakeServer(t *testing.T, handle func(*fakeSession)) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &fakeServer{t: t, ln: ln, handle: handle}
	server.wg.Add(1)
	go server.serve()
	t.Cleanup(server.close)
	return server
}

func (s *fakeServer) port() int {
	return s.ln.Addr().(*net.TCPAddr).Port
}

func (s *fakeServer) serve() {
	defer s.wg.Done()
	for {
		netConn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		s.accepted.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() { _ = netConn.Close() }()
			session := &fakeSession{t: s.t, netConn: netConn, br: bufio.NewReader(netConn)}
			s.handle(session)
			if session.err != nil {
				s.t.Errorf("fake peer: %v", session.err)
			}
		}()
	}
}

func (s *fakeServer) close() {
	_ = s.ln.Close()
	s.wg.Wait()
}

// connections reports how many TCP connections the peer accepted.
func (s *fakeServer) connections() int {
	return int(s.accepted.Load())
}

// fakeSession is one scripted backend connection.
type fakeSession struct {
	t       *testing.T
	netConn net.Conn
	br      *bufio.Reader
	err     error
}

func (s *fakeSession) fail(format string, args ...any) {
	if s.err == nil {
		s.err = fmt.Errorf(format, args...)
	}
}

func (s *fakeSession) readN(n int) []byte {
	if s.err != nil || n < 0 {
		return nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(s.br, buf); err != nil {
		s.fail("read %d bytes: %v", n, err)
		return nil
	}
	return buf
}

func (s *fakeSession) writeRaw(data []byte) {
	if s.err != nil {
		return
	}
	if _, err := s.netConn.Write(data); err != nil {
		s.fail("write: %v", err)
	}
}

func (s *fakeSession) readMessage() (byte, []byte) {
	header := s.readN(5)
	if header == nil {
		return 0, nil
	}
	length := int(binary.BigEndian.Uint32(header[1:]))
	if length < 4 || length > 1<<20 {
		s.fail("bad client message length %d", length)
		return 0, nil
	}
	return header[0], s.readN(length - 4)
}

func (s *fakeSession) expectType(want byte) []byte {
	typ, body := s.readMessage()
	if typ != want {
		s.fail("expected client message %q, got %q (%q)", want, typ, body)
	}
	return body
}

// readStartup reads the StartupMessage and returns its parameters.
func (s *fakeSession) readStartup() map[string]string {
	raw := s.readN(4)
	if raw == nil {
		return nil
	}
	length := int(binary.BigEndian.Uint32(raw))
	if length < 8 || length > 1<<16 {
		s.fail("bad startup length %d", length)
		return nil
	}
	body := s.readN(length - 4)
	if body == nil {
		return nil
	}
	r := &readBuf{b: body}
	version := r.int32()
	if version != protocolVersion351 {
		s.fail("startup protocol version = %d, want %d", version, protocolVersion351)
	}
	params := make(map[string]string)
	for r.remaining() > 0 {
		key := r.string()
		if key == "" {
			break
		}
		params[key] = r.string()
	}
	if r.err != nil {
		s.fail("malformed startup message: %v", r.err)
	}
	return params
}

// rejectTLS reads the SSLRequest and answers that the server has no SSL.
func (s *fakeSession) rejectTLS() {
	raw := s.readN(4)
	if raw == nil {
		return
	}
	length := int(binary.BigEndian.Uint32(raw))
	body := s.readN(length - 4)
	if len(body) != 4 || int32(binary.BigEndian.Uint32(body)) != sslRequestCode {
		s.fail("expected SSLRequest, got body %x", body)
		return
	}
	s.writeRaw([]byte("N"))
}

// acceptTLS reads the SSLRequest, answers 'S' and performs the server side of
// the TLS handshake. It returns false when the handshake fails, which lets a
// test assert a rejected certificate without failing the peer.
func (s *fakeSession) acceptTLS(cert tls.Certificate) bool {
	raw := s.readN(4)
	if raw == nil {
		return false
	}
	length := int(binary.BigEndian.Uint32(raw))
	body := s.readN(length - 4)
	if len(body) != 4 || int32(binary.BigEndian.Uint32(body)) != sslRequestCode {
		s.fail("expected SSLRequest, got body %x", body)
		return false
	}
	s.writeRaw([]byte("S"))

	tlsConn := tls.Server(s.netConn, &tls.Config{Certificates: []tls.Certificate{cert}})
	if err := tlsConn.Handshake(); err != nil {
		// The client may reject the certificate; that is a valid outcome.
		return false
	}
	s.netConn = tlsConn
	s.br = bufio.NewReader(tlsConn)
	return true
}

func (s *fakeSession) expectPassword() []byte {
	return s.expectType(msgPasswordMessage)
}

func (s *fakeSession) expectSimpleQuery() string {
	body := s.expectType(msgQuery)
	r := &readBuf{b: body}
	query := r.string()
	if r.err != nil {
		s.fail("malformed Query message: %v", r.err)
	}
	return query
}

func (s *fakeSession) expectPrepare() (name, query string) {
	body := s.expectType(msgParse)
	r := &readBuf{b: body}
	name = r.string()
	query = r.string()
	paramTypes := int(r.int16())
	for i := 0; i < paramTypes; i++ {
		r.int32()
	}
	if r.err != nil {
		s.fail("malformed Parse message: %v", r.err)
	}
	return name, query
}

// fakeParam is one Bind parameter as the server received it.
type fakeParam struct {
	text   string
	isNull bool
}

// expectExtended reads Parse, Bind, Describe, Execute and Sync, and returns
// the query text with the bound parameters.
func (s *fakeSession) expectExtended() (query string, args []fakeParam) {
	_, query = s.expectPrepare()
	_, args = s.expectBindExecute()
	return query, args
}

// expectBindExecute reads Bind, Describe, Execute and Sync for a prepared
// statement.
func (s *fakeSession) expectBindExecute() (statement string, args []fakeParam) {
	bind := s.expectType(msgBind)
	rb := &readBuf{b: bind}
	rb.string() // portal
	statement = rb.string()
	formatCount := int(rb.int16())
	for i := 0; i < formatCount; i++ {
		rb.int16()
	}
	paramCount := int(rb.int16())
	args = make([]fakeParam, paramCount)
	for i := 0; i < paramCount; i++ {
		length := int(rb.int32())
		if length < 0 {
			args[i] = fakeParam{isNull: true}
			continue
		}
		args[i] = fakeParam{text: string(rb.bytes(length))}
	}
	resultFormats := int(rb.int16())
	for i := 0; i < resultFormats; i++ {
		rb.int16()
	}
	if rb.err != nil {
		s.fail("malformed Bind message: %v", rb.err)
	}

	describe := s.expectType(msgDescribe)
	dr := &readBuf{b: describe}
	if target := dr.byte(); target != 'P' {
		s.fail("Describe target = %q, want portal", target)
	}
	dr.string()
	execute := s.expectType(msgExecute)
	er := &readBuf{b: execute}
	er.string() // portal
	er.int32()  // row limit
	s.expectType(msgSync)
	return statement, args
}

// completeStartup performs the normal SHA256 login and checks the password
// proof against the published known-answer vector.
func (s *fakeSession) completeStartup(check func(params map[string]string)) {
	params := s.readStartup()
	if check != nil {
		check(params)
	}
	s.sendAuthenticationSHA256(katSalt, katToken, 10000)
	proof := s.expectPassword()
	if want := katProof + "\x00"; string(proof) != want {
		s.fail("client password proof = %q, want %q", proof, want)
	}
	s.sendAuthenticationOK()
	s.sendParameterStatus("server_version", "7.0.0")
	s.sendParameterStatus("client_encoding", "UTF8")
	s.sendBackendKeyData(4242, 9001)
	s.sendNotice("intervening notice must be consumed")
	s.sendReady(txnIdle)
}

func (s *fakeSession) sendAuthenticationSHA256(salt, token string, iteration int32) {
	m := newMessageWriter(msgAuthenticationRequest)
	m.int32(authSHA256)
	m.int32(passwordStoredSHA256)
	m.bytes([]byte(salt))
	m.bytes([]byte(token))
	m.int32(iteration)
	s.writeRaw(m.finish())
}

func (s *fakeSession) sendAuthenticationMD5(salt []byte) {
	m := newMessageWriter(msgAuthenticationRequest)
	m.int32(authMD5Password)
	m.bytes(salt)
	s.writeRaw(m.finish())
}

func (s *fakeSession) sendAuthenticationOK() {
	s.writeRaw(authOKFrame())
}

// authOKFrame builds the AuthenticationOk message.
func authOKFrame() []byte {
	m := newMessageWriter(msgAuthenticationRequest)
	m.int32(authOK)
	return m.finish()
}

// parameterStatusFrame builds one ParameterStatus message.
func parameterStatusFrame(name, value string) []byte {
	m := newMessageWriter(msgParameterStatus)
	m.string(name)
	m.string(value)
	return m.finish()
}

func (s *fakeSession) sendAuthenticationRequest(code int32) {
	m := newMessageWriter(msgAuthenticationRequest)
	m.int32(code)
	s.writeRaw(m.finish())
}

func (s *fakeSession) sendParameterStatus(name, value string) {
	m := newMessageWriter(msgParameterStatus)
	m.string(name)
	m.string(value)
	s.writeRaw(m.finish())
}

func (s *fakeSession) sendBackendKeyData(pid, key int32) {
	m := newMessageWriter(msgBackendKeyData)
	m.int32(pid)
	m.int32(key)
	s.writeRaw(m.finish())
}

func (s *fakeSession) sendNotice(message string) {
	m := newMessageWriter(msgNoticeResponse)
	m.byte('S')
	m.string("NOTICE")
	m.byte('M')
	m.string(message)
	m.byte(0)
	s.writeRaw(m.finish())
}

func (s *fakeSession) sendError(sqlstate, message string) {
	m := newMessageWriter(msgErrorResponse)
	m.byte('S')
	m.string("ERROR")
	m.byte('C')
	m.string(sqlstate)
	m.byte('M')
	m.string(message)
	m.byte(0)
	s.writeRaw(m.finish())
}

func (s *fakeSession) sendReady(status byte) {
	s.writeRaw(readyFrame(status))
}

// Frame builders. They are also used by tests that split or coalesce writes.
func readyFrame(status byte) []byte {
	m := newMessageWriter(msgReadyForQuery)
	m.byte(status)
	return m.finish()
}

func commandCompleteFrame(tag string) []byte {
	m := newMessageWriter(msgCommandComplete)
	m.string(tag)
	return m.finish()
}

func rowDescriptionFrame(columns ...fakeColumn) []byte {
	m := newMessageWriter(msgRowDescription)
	m.int16(int16(len(columns)))
	for _, col := range columns {
		m.string(col.name)
		m.int32(0) // table OID
		m.int16(0) // attribute number
		m.int32(col.oid)
		m.int16(-1) // type size
		m.int32(-1) // type modifier
		m.int16(0)  // text format
	}
	return m.finish()
}

// dataRowFrame encodes one DataRow. A nil element is SQL NULL.
func dataRowFrame(values ...any) []byte {
	m := newMessageWriter(msgDataRow)
	m.int16(int16(len(values)))
	for _, value := range values {
		switch v := value.(type) {
		case nil:
			m.int32(-1)
		case string:
			m.int32(int32(len(v)))
			m.bytes([]byte(v))
		case []byte:
			m.int32(int32(len(v)))
			m.bytes(v)
		default:
			return nil
		}
	}
	return m.finish()
}

func (s *fakeSession) sendParseComplete() { s.writeRaw(newMessageWriter(msgParseComplete).finish()) }
func (s *fakeSession) sendBindComplete()  { s.writeRaw(newMessageWriter(msgBindComplete).finish()) }
func (s *fakeSession) sendCloseComplete() { s.writeRaw(newMessageWriter(msgCloseComplete).finish()) }
func (s *fakeSession) sendNoData()        { s.writeRaw(newMessageWriter(msgNoData).finish()) }
func (s *fakeSession) sendEmptyQueryResponse() {
	s.writeRaw(newMessageWriter(msgEmptyQueryResponse).finish())
}

func (s *fakeSession) sendParameterDescription(oids ...int32) {
	m := newMessageWriter(msgParameterDescription)
	m.int16(int16(len(oids)))
	for _, oid := range oids {
		m.int32(oid)
	}
	s.writeRaw(m.finish())
}

// fakeColumn is one RowDescription column.
type fakeColumn struct {
	name string
	oid  int32
}

func (s *fakeSession) sendRowDescription(columns ...fakeColumn) {
	s.writeRaw(rowDescriptionFrame(columns...))
}

// sendDataRow sends one DataRow. A nil element encodes SQL NULL.
func (s *fakeSession) sendDataRow(values ...any) {
	frame := dataRowFrame(values...)
	if frame == nil {
		s.fail("unsupported DataRow value")
		return
	}
	s.writeRaw(frame)
}

func (s *fakeSession) sendCommandComplete(tag string) {
	s.writeRaw(commandCompleteFrame(tag))
}

// sendSlow writes one frame in single-byte chunks to exercise reassembly.
func (s *fakeSession) sendSlow(frame []byte) {
	for _, b := range frame {
		s.writeRaw([]byte{b})
	}
}

package opengauss

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"github.com/cocosip/go-opengauss/internal/auth"
	"github.com/cocosip/go-opengauss/internal/proto"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Credentials used by the scripted peer. The password matches the published
// SHA256 known-answer vector in internal/auth.
const (
	katSalt      = "3458fe51abe962f7b6011a1d73fc14edf50539fae89fb9dda75fbb642d9859bf"
	katToken     = "2cf71f10"
	katProof     = "f5213af93974ae3f0a3eedba7b0ef27796bcd4d84bb737d9c35fce54fcfe28fd"
	testUser     = "alice"
	testDatabase = "app"
	testPassword = "sha256@abc123"
)

// fakeServer is a scripted openGauss backend for offline protocol tests. Each
// connection runs the same handler, which asserts the exact client messages.
// CancelRequest connections are recognized before the handler and recorded.
type fakeServer struct {
	t          *testing.T
	ln         net.Listener
	handle     func(*fakeSession)
	wg         sync.WaitGroup
	accepted   atomic.Int32
	cancels    chan cancelRecord
	cancelHold time.Duration
}

// cancelRecord is the backend identity carried by a CancelRequest.
type cancelRecord struct {
	pid int32
	key int32
}

func newFakeServer(t *testing.T, handle func(*fakeSession)) *fakeServer {
	return newFakeServerWithCancelHold(t, handle, 0)
}

// newFakeServerWithCancelHold delays closing the cancel connection by hold, so
// tests can observe the client waiting for the postmaster's acknowledgement.
func newFakeServerWithCancelHold(t *testing.T, handle func(*fakeSession), hold time.Duration) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &fakeServer{t: t, ln: ln, handle: handle, cancels: make(chan cancelRecord, 8), cancelHold: hold}
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
		go func() {
			defer s.wg.Done()
			defer func() { _ = netConn.Close() }()
			br := bufio.NewReader(netConn)

			// The first eight bytes distinguish a CancelRequest from a
			// startup packet; a cancel has no reply and no session.
			first := make([]byte, 8)
			if _, err := io.ReadFull(br, first); err != nil {
				return
			}
			if int32(binary.BigEndian.Uint32(first[4:8])) == proto.CancelRequestCode {
				if length := binary.BigEndian.Uint32(first[0:4]); length != 16 {
					s.t.Errorf("fake peer: CancelRequest length = %d, want 16", length)
					return
				}
				rest := make([]byte, 8)
				if _, err := io.ReadFull(br, rest); err != nil {
					return
				}
				select {
				case s.cancels <- cancelRecord{
					pid: int32(binary.BigEndian.Uint32(rest[0:4])),
					key: int32(binary.BigEndian.Uint32(rest[4:8])),
				}:
				default:
				}
				if s.cancelHold > 0 {
					time.Sleep(s.cancelHold)
				}
				return
			}

			s.accepted.Add(1)
			session := &fakeSession{
				t:       s.t,
				netConn: netConn,
				br:      bufio.NewReader(io.MultiReader(bytes.NewReader(first), br)),
			}
			s.handle(session)
			if session.err != nil {
				s.t.Errorf("fake peer: %v", session.err)
			}
		}()
	}
}

// waitCancel returns the backend identity of the next CancelRequest, failing
// the test when none arrives.
func (s *fakeServer) waitCancel(t *testing.T) cancelRecord {
	t.Helper()
	select {
	case record := <-s.cancels:
		return record
	case <-time.After(5 * time.Second):
		t.Error("no CancelRequest arrived")
		return cancelRecord{}
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
	r := &proto.ReadBuf{B: body}
	version := r.Int32()
	if version != proto.ProtocolVersion351 {
		s.fail("startup protocol version = %d, want %d", version, proto.ProtocolVersion351)
	}
	params := make(map[string]string)
	for r.Remaining() > 0 {
		key := r.String()
		if key == "" {
			break
		}
		params[key] = r.String()
	}
	if r.Err != nil {
		s.fail("malformed startup message: %v", r.Err)
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
	if len(body) != 4 || int32(binary.BigEndian.Uint32(body)) != proto.SSLRequestCode {
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
	if len(body) != 4 || int32(binary.BigEndian.Uint32(body)) != proto.SSLRequestCode {
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
	return s.expectType(proto.MsgPasswordMessage)
}

func (s *fakeSession) expectSimpleQuery() string {
	body := s.expectType(proto.MsgQuery)
	r := &proto.ReadBuf{B: body}
	query := r.String()
	if r.Err != nil {
		s.fail("malformed Query message: %v", r.Err)
	}
	return query
}

func (s *fakeSession) expectPrepare() (name, query string) {
	body := s.expectType(proto.MsgParse)
	r := &proto.ReadBuf{B: body}
	name = r.String()
	query = r.String()
	paramTypes := int(r.Int16())
	for i := 0; i < paramTypes; i++ {
		r.Int32()
	}
	if r.Err != nil {
		s.fail("malformed Parse message: %v", r.Err)
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
	bind := s.expectType(proto.MsgBind)
	rb := &proto.ReadBuf{B: bind}
	_ = rb.String() // portal
	statement = rb.String()
	formatCount := int(rb.Int16())
	for i := 0; i < formatCount; i++ {
		rb.Int16()
	}
	paramCount := int(rb.Int16())
	args = make([]fakeParam, paramCount)
	for i := 0; i < paramCount; i++ {
		length := int(rb.Int32())
		if length < 0 {
			args[i] = fakeParam{isNull: true}
			continue
		}
		args[i] = fakeParam{text: string(rb.Bytes(length))}
	}
	resultFormats := int(rb.Int16())
	for i := 0; i < resultFormats; i++ {
		rb.Int16()
	}
	if rb.Err != nil {
		s.fail("malformed Bind message: %v", rb.Err)
	}

	describe := s.expectType(proto.MsgDescribe)
	dr := &proto.ReadBuf{B: describe}
	if target := dr.Byte(); target != 'P' {
		s.fail("Describe target = %q, want portal", target)
	}
	_ = dr.String()
	execute := s.expectType(proto.MsgExecute)
	er := &proto.ReadBuf{B: execute}
	_ = er.String() // portal
	er.Int32()      // row limit
	s.expectType(proto.MsgSync)
	return statement, args
}

// completeStartup performs the normal SHA256 login and checks the password
// proof against the published known-answer vector.
func (s *fakeSession) completeStartup(check func(params map[string]string)) {
	s.completeStartupEncoding("UTF8", check)
}

// completeStartupEncoding runs the SHA256 startup and reports encoding as the
// server's client_encoding, so tests can exercise unsupported encodings.
func (s *fakeSession) completeStartupEncoding(encoding string, check func(params map[string]string)) {
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
	s.sendParameterStatus("client_encoding", encoding)
	s.sendBackendKeyData(4242, 9001)
	s.sendNotice("intervening notice must be consumed")
	s.sendReady(proto.TxnIdle)
}

func (s *fakeSession) sendAuthenticationSHA256(salt, token string, iteration int32) {
	m := proto.NewMessageWriter(proto.MsgAuthenticationRequest)
	m.Int32(auth.RequestSHA256)
	m.Int32(auth.StoredSHA256)
	m.Bytes([]byte(salt))
	m.Bytes([]byte(token))
	m.Int32(iteration)
	s.writeRaw(m.Finish())
}

func (s *fakeSession) sendAuthenticationMD5(salt []byte) {
	m := proto.NewMessageWriter(proto.MsgAuthenticationRequest)
	m.Int32(auth.RequestMD5)
	m.Bytes(salt)
	s.writeRaw(m.Finish())
}

// sendAuthenticationMD5SHA256 sends request 11 with its 64-character hex salt
// and 4-byte MD5 salt.
func (s *fakeSession) sendAuthenticationMD5SHA256(random64code string, salt []byte) {
	m := proto.NewMessageWriter(proto.MsgAuthenticationRequest)
	m.Int32(auth.RequestMD5SHA256)
	m.Bytes([]byte(random64code))
	m.Bytes(salt)
	s.writeRaw(m.Finish())
}

// sendAuthenticationSM3 sends request 13, whose body matches request 10.
func (s *fakeSession) sendAuthenticationSM3(stored int32, salt, token string, iteration int32) {
	m := proto.NewMessageWriter(proto.MsgAuthenticationRequest)
	m.Int32(auth.RequestSM3)
	m.Int32(stored)
	m.Bytes([]byte(salt))
	m.Bytes([]byte(token))
	m.Int32(iteration)
	s.writeRaw(m.Finish())
}

// sendAuthenticationGSS sends AuthenticationGSS (7) with an empty body, or
// AuthenticationGSSContinue (8) with the server token.
func (s *fakeSession) sendAuthenticationGSS(code int32, token []byte) {
	m := proto.NewMessageWriter(proto.MsgAuthenticationRequest)
	m.Int32(code)
	m.Bytes(token)
	s.writeRaw(m.Finish())
}

func (s *fakeSession) sendAuthenticationOK() {
	s.writeRaw(authOKFrame())
}

// authOKFrame builds the AuthenticationOk message.
func authOKFrame() []byte {
	m := proto.NewMessageWriter(proto.MsgAuthenticationRequest)
	m.Int32(auth.OK)
	return m.Finish()
}

// parameterStatusFrame builds one ParameterStatus message.
func parameterStatusFrame(name, value string) []byte {
	m := proto.NewMessageWriter(proto.MsgParameterStatus)
	m.String(name)
	m.String(value)
	return m.Finish()
}

func (s *fakeSession) sendAuthenticationRequest(code int32) {
	m := proto.NewMessageWriter(proto.MsgAuthenticationRequest)
	m.Int32(code)
	s.writeRaw(m.Finish())
}

func (s *fakeSession) sendParameterStatus(name, value string) {
	m := proto.NewMessageWriter(proto.MsgParameterStatus)
	m.String(name)
	m.String(value)
	s.writeRaw(m.Finish())
}

func (s *fakeSession) sendBackendKeyData(pid, key int32) {
	m := proto.NewMessageWriter(proto.MsgBackendKeyData)
	m.Int32(pid)
	m.Int32(key)
	s.writeRaw(m.Finish())
}

func (s *fakeSession) sendNotice(message string) {
	m := proto.NewMessageWriter(proto.MsgNoticeResponse)
	m.Byte('S')
	m.String("NOTICE")
	m.Byte('M')
	m.String(message)
	m.Byte(0)
	s.writeRaw(m.Finish())
}

func (s *fakeSession) sendError(sqlstate, message string) {
	m := proto.NewMessageWriter(proto.MsgErrorResponse)
	m.Byte('S')
	m.String("ERROR")
	m.Byte('C')
	m.String(sqlstate)
	m.Byte('M')
	m.String(message)
	m.Byte(0)
	s.writeRaw(m.Finish())
}

func (s *fakeSession) sendReady(status byte) {
	s.writeRaw(readyFrame(status))
}

// Frame builders. They are also used by tests that split or coalesce writes.
func readyFrame(status byte) []byte {
	m := proto.NewMessageWriter(proto.MsgReadyForQuery)
	m.Byte(status)
	return m.Finish()
}

func commandCompleteFrame(tag string) []byte {
	m := proto.NewMessageWriter(proto.MsgCommandComplete)
	m.String(tag)
	return m.Finish()
}

func rowDescriptionFrame(columns ...fakeColumn) []byte {
	m := proto.NewMessageWriter(proto.MsgRowDescription)
	m.Int16(int16(len(columns)))
	for _, col := range columns {
		m.String(col.name)
		m.Int32(0) // table OID
		m.Int16(0) // attribute number
		m.Int32(col.oid)
		m.Int16(-1) // type size
		typmod := col.typmod
		if typmod == 0 {
			typmod = -1
		}
		m.Int32(typmod) // type modifier
		m.Int16(0)      // text format
	}
	return m.Finish()
}

// dataRowFrame encodes one DataRow. A nil element is SQL NULL.
func dataRowFrame(values ...any) []byte {
	m := proto.NewMessageWriter(proto.MsgDataRow)
	m.Int16(int16(len(values)))
	for _, value := range values {
		switch v := value.(type) {
		case nil:
			m.Int32(-1)
		case string:
			m.Int32(int32(len(v)))
			m.Bytes([]byte(v))
		case []byte:
			m.Int32(int32(len(v)))
			m.Bytes(v)
		default:
			return nil
		}
	}
	return m.Finish()
}

func (s *fakeSession) sendParseComplete() {
	s.writeRaw(proto.NewMessageWriter(proto.MsgParseComplete).Finish())
}
func (s *fakeSession) sendBindComplete() {
	s.writeRaw(proto.NewMessageWriter(proto.MsgBindComplete).Finish())
}
func (s *fakeSession) sendCloseComplete() {
	s.writeRaw(proto.NewMessageWriter(proto.MsgCloseComplete).Finish())
}
func (s *fakeSession) sendNoData() { s.writeRaw(proto.NewMessageWriter(proto.MsgNoData).Finish()) }
func (s *fakeSession) sendEmptyQueryResponse() {
	s.writeRaw(proto.NewMessageWriter(proto.MsgEmptyQueryResponse).Finish())
}

func (s *fakeSession) sendParameterDescription(oids ...int32) {
	m := proto.NewMessageWriter(proto.MsgParameterDescription)
	m.Int16(int16(len(oids)))
	for _, oid := range oids {
		m.Int32(oid)
	}
	s.writeRaw(m.Finish())
}

// fakeColumn is one RowDescription column.
type fakeColumn struct {
	name string
	oid  int32
	// typmod carries a declared length or numeric precision and scale; the
	// zero value means the server reported no modifier (-1).
	typmod int32
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

// newRoleServer answers the target_session_attrs probe with the given role and
// then closes the session, so tests assert which endpoint the driver picked.
func newRoleServer(t *testing.T, inRecovery, readOnly bool) *fakeServer {
	t.Helper()
	return newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if query := s.expectSimpleQuery(); query != sessionStateQuery {
			s.fail("state query = %q, want %q", query, sessionStateQuery)
			return
		}
		s.sendRowDescription(
			fakeColumn{name: "pg_is_in_recovery", oid: 16},
			fakeColumn{name: "current_setting", oid: 25},
		)
		s.sendDataRow(serverBool(inRecovery), readOnlyText(readOnly))
		s.sendCommandComplete("SELECT 1")
		s.sendReady(proto.TxnIdle)
	})
}

func serverBool(value bool) string {
	if value {
		return "t"
	}
	return "f"
}

func readOnlyText(readOnly bool) string {
	if readOnly {
		return "on"
	}
	return "off"
}

// sendCopyInResponse announces the row width of a COPY ... FROM STDIN stream.
func (s *fakeSession) sendCopyInResponse(format byte, columns int) {
	m := proto.NewMessageWriter(proto.MsgCopyInResponse)
	m.Byte(format)
	m.Int16(int16(columns))
	for i := 0; i < columns; i++ {
		m.Int16(int16(format))
	}
	s.writeRaw(m.Finish())
}

// sendCopyOutResponse announces the row width of a COPY ... TO STDOUT stream.
// The overall format selects text or binary, and every column carries the same
// format because COPY does not support a mixed stream.
func (s *fakeSession) sendCopyOutResponse(format byte, columns int) {
	m := proto.NewMessageWriter(proto.MsgCopyOutResponse)
	m.Byte(format)
	m.Int16(int16(columns))
	for i := 0; i < columns; i++ {
		m.Int16(int16(format))
	}
	s.writeRaw(m.Finish())
}

// sendCopyData sends one CopyData chunk of a COPY ... TO STDOUT stream.
func (s *fakeSession) sendCopyData(payload string) {
	m := proto.NewMessageWriter(proto.MsgCopyData)
	m.Bytes([]byte(payload))
	s.writeRaw(m.Finish())
}

// sendCopyDone ends the server side of a COPY ... TO STDOUT stream.
func (s *fakeSession) sendCopyDone() {
	s.writeRaw(proto.NewMessageWriter(proto.MsgCopyDone).Finish())
}

// readCopyRows collects the CopyData payloads of one stream until CopyDone.
func (s *fakeSession) readCopyRows() []string {
	var rows []string
	for {
		typ, body := s.readMessage()
		if s.err != nil {
			return rows
		}
		switch typ {
		case proto.MsgCopyData:
			rows = append(rows, string(body))
		case proto.MsgCopyDone:
			return rows
		case proto.MsgCopyFail:
			s.fail("COPY aborted with %q", string(body))
			return rows
		default:
			s.fail("unexpected message %q during COPY", typ)
			return rows
		}
	}
}

package opengauss

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Backend and frontend message type bytes.
const (
	msgAuthenticationRequest = 'R'
	msgBackendKeyData        = 'K'
	msgBind                  = 'B'
	msgBindComplete          = '2'
	msgClose                 = 'C'
	msgCloseComplete         = '3'
	msgCommandComplete       = 'C'
	msgCopyBothResponse      = 'W'
	msgCopyInResponse        = 'G'
	msgCopyOutResponse       = 'H'
	msgDataRow               = 'D'
	msgDescribe              = 'D'
	msgEmptyQueryResponse    = 'I'
	msgErrorResponse         = 'E'
	msgExecute               = 'E'
	msgNoData                = 'n'
	msgNoticeResponse        = 'N'
	msgNotificationResponse  = 'A'
	msgParameterDescription  = 't'
	msgParameterStatus       = 'S'
	msgParse                 = 'P'
	msgParseComplete         = '1'
	msgPasswordMessage       = 'p'
	msgQuery                 = 'Q'
	msgReadyForQuery         = 'Z'
	msgRowDescription        = 'T'
	msgSync                  = 'S'
	msgTerminate             = 'X'
)

// Transaction status indicators carried by ReadyForQuery.
const (
	txnIdle          byte = 'I'
	txnInTransaction byte = 'T'
	txnFailed        byte = 'E'
)

const (
	// protocolVersion351 is the startup protocol version that all supported
	// openGauss releases declare in src/include/libpq/pqcomm.h.
	protocolVersion351 int32 = 196659
	sslRequestCode     int32 = 80877103

	// maxMessageSize bounds allocations caused by a corrupt or hostile length
	// field. Large column values still fit comfortably.
	maxMessageSize = 128 << 20
)

var errMalformedMessage = errors.New("opengauss: malformed backend message")

// readBuf is a cursor over one backend message body. The first malformed
// read sets a sticky error, so callers can parse a sequence and inspect
// readBuf.err once at the end instead of checking every accessor.
type readBuf struct {
	b   []byte
	err error
}

func (r *readBuf) fail() {
	if r.err == nil {
		r.err = errMalformedMessage
	}
}

func (r *readBuf) int32() int32 {
	if r.err != nil {
		return 0
	}
	if len(r.b) < 4 {
		r.fail()
		return 0
	}
	v := int32(binary.BigEndian.Uint32(r.b))
	r.b = r.b[4:]
	return v
}

func (r *readBuf) int16() int16 {
	if r.err != nil {
		return 0
	}
	if len(r.b) < 2 {
		r.fail()
		return 0
	}
	v := int16(binary.BigEndian.Uint16(r.b))
	r.b = r.b[2:]
	return v
}

func (r *readBuf) byte() byte {
	if r.err != nil {
		return 0
	}
	if len(r.b) < 1 {
		r.fail()
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

// string reads a NUL-terminated string.
func (r *readBuf) string() string {
	if r.err != nil {
		return ""
	}
	i := bytes.IndexByte(r.b, 0)
	if i < 0 {
		r.fail()
		return ""
	}
	s := string(r.b[:i])
	r.b = r.b[i+1:]
	return s
}

func (r *readBuf) bytes(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || len(r.b) < n {
		r.fail()
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *readBuf) remaining() int { return len(r.b) }

// messageWriter builds one frontend message. The length placeholder is filled
// in by finish.
type messageWriter struct {
	buf []byte
}

func newMessageWriter(typ byte) *messageWriter {
	return &messageWriter{buf: []byte{typ, 0, 0, 0, 0}}
}

func (w *messageWriter) int32(v int32) {
	w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(v))
}

func (w *messageWriter) int16(v int16) {
	w.buf = binary.BigEndian.AppendUint16(w.buf, uint16(v))
}

func (w *messageWriter) byte(b byte) {
	w.buf = append(w.buf, b)
}

func (w *messageWriter) bytes(b []byte) {
	w.buf = append(w.buf, b...)
}

func (w *messageWriter) string(s string) {
	w.buf = append(append(w.buf, s...), 0)
}

// finish fills the length field and returns the complete framed message.
func (w *messageWriter) finish() []byte {
	binary.BigEndian.PutUint32(w.buf[1:5], uint32(len(w.buf)-1))
	return w.buf
}

// readMessage reads one complete backend message. A truncated header or body
// returns the underlying I/O error so callers can preserve the cause.
func readMessage(br *bufio.Reader) (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(br, header[:]); err != nil {
		return 0, nil, err
	}
	typ := header[0]
	length := int(binary.BigEndian.Uint32(header[1:]))
	if length < 4 {
		return 0, nil, fmt.Errorf("opengauss: invalid backend message length %d", length)
	}
	bodyLen := length - 4
	if bodyLen > maxMessageSize {
		return 0, nil, fmt.Errorf("opengauss: backend message too large: %d bytes", bodyLen)
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(br, body); err != nil {
		return 0, nil, fmt.Errorf("opengauss: truncated backend message: %w", err)
	}
	return typ, body, nil
}

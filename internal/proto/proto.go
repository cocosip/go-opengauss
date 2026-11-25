// Package proto implements framing for the openGauss/PostgreSQL frontend and
// backend protocol. It contains no connection or database/sql logic.
package proto

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
	MsgAuthenticationRequest = 'R'
	MsgBackendKeyData        = 'K'
	MsgBind                  = 'B'
	MsgBindComplete          = '2'
	MsgClose                 = 'C'
	MsgCloseComplete         = '3'
	MsgCommandComplete       = 'C'
	MsgCopyBothResponse      = 'W'
	MsgCopyInResponse        = 'G'
	MsgCopyOutResponse       = 'H'
	MsgDataRow               = 'D'
	MsgDescribe              = 'D'
	MsgEmptyQueryResponse    = 'I'
	MsgErrorResponse         = 'E'
	MsgExecute               = 'E'
	MsgNoData                = 'n'
	MsgNoticeResponse        = 'N'
	MsgNotificationResponse  = 'A'
	MsgParameterDescription  = 't'
	MsgParameterStatus       = 'S'
	MsgParse                 = 'P'
	MsgParseComplete         = '1'
	MsgPasswordMessage       = 'p'
	MsgQuery                 = 'Q'
	MsgReadyForQuery         = 'Z'
	MsgRowDescription        = 'T'
	MsgSync                  = 'S'
	MsgTerminate             = 'X'
)

// Transaction status indicators carried by ReadyForQuery.
const (
	TxnIdle          byte = 'I'
	TxnInTransaction byte = 'T'
	TxnFailed        byte = 'E'
)

const (
	// ProtocolVersion351 is the startup protocol version that all supported
	// openGauss releases declare in src/include/libpq/pqcomm.h.
	ProtocolVersion351 int32 = 196659
	SSLRequestCode     int32 = 80877103

	// MaxMessageSize bounds allocations caused by a corrupt or hostile length
	// field. Large column values still fit comfortably.
	MaxMessageSize = 128 << 20
)

var ErrMalformedMessage = errors.New("opengauss: malformed backend message")

// ReadBuf is a cursor over one backend message body. The first malformed read
// sets a sticky error, so callers can parse a sequence and inspect Err once at
// the end instead of checking every accessor.
type ReadBuf struct {
	B   []byte
	Err error
}

func (r *ReadBuf) fail() {
	if r.Err == nil {
		r.Err = ErrMalformedMessage
	}
}

func (r *ReadBuf) Int32() int32 {
	if r.Err != nil {
		return 0
	}
	if len(r.B) < 4 {
		r.fail()
		return 0
	}
	v := int32(binary.BigEndian.Uint32(r.B))
	r.B = r.B[4:]
	return v
}

func (r *ReadBuf) Int16() int16 {
	if r.Err != nil {
		return 0
	}
	if len(r.B) < 2 {
		r.fail()
		return 0
	}
	v := int16(binary.BigEndian.Uint16(r.B))
	r.B = r.B[2:]
	return v
}

func (r *ReadBuf) Byte() byte {
	if r.Err != nil {
		return 0
	}
	if len(r.B) < 1 {
		r.fail()
		return 0
	}
	v := r.B[0]
	r.B = r.B[1:]
	return v
}

// String reads a NUL-terminated string.
func (r *ReadBuf) String() string {
	if r.Err != nil {
		return ""
	}
	i := bytes.IndexByte(r.B, 0)
	if i < 0 {
		r.fail()
		return ""
	}
	s := string(r.B[:i])
	r.B = r.B[i+1:]
	return s
}

func (r *ReadBuf) Bytes(n int) []byte {
	if r.Err != nil {
		return nil
	}
	if n < 0 || len(r.B) < n {
		r.fail()
		return nil
	}
	v := r.B[:n]
	r.B = r.B[n:]
	return v
}

func (r *ReadBuf) Remaining() int { return len(r.B) }

// MessageWriter builds one frontend message. The length placeholder is filled
// in by Finish.
type MessageWriter struct {
	buf []byte
}

func NewMessageWriter(typ byte) *MessageWriter {
	return &MessageWriter{buf: []byte{typ, 0, 0, 0, 0}}
}

func (w *MessageWriter) Int32(v int32) {
	w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(v))
}

func (w *MessageWriter) Int16(v int16) {
	w.buf = binary.BigEndian.AppendUint16(w.buf, uint16(v))
}

func (w *MessageWriter) Byte(b byte) {
	w.buf = append(w.buf, b)
}

func (w *MessageWriter) Bytes(b []byte) {
	w.buf = append(w.buf, b...)
}

func (w *MessageWriter) String(s string) {
	w.buf = append(append(w.buf, s...), 0)
}

// Finish fills the length field and returns the complete framed message.
func (w *MessageWriter) Finish() []byte {
	binary.BigEndian.PutUint32(w.buf[1:5], uint32(len(w.buf)-1))
	return w.buf
}

// ReadMessage reads one complete backend message. A truncated header or body
// returns the underlying I/O error so callers can preserve the cause.
func ReadMessage(br *bufio.Reader) (byte, []byte, error) {
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
	if bodyLen > MaxMessageSize {
		return 0, nil, fmt.Errorf("opengauss: backend message too large: %d bytes", bodyLen)
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(br, body); err != nil {
		return 0, nil, fmt.Errorf("opengauss: truncated backend message: %w", err)
	}
	return typ, body, nil
}

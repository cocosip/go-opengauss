package proto

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strings"
	"testing"
)

func TestMessageWriterFramesQuery(t *testing.T) {
	w := NewMessageWriter(MsgQuery)
	w.String("SELECT 1")
	frame := w.Finish()

	want := []byte{'Q', 0, 0, 0, 13, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0}
	if !bytes.Equal(frame, want) {
		t.Fatalf("frame = %v, want %v", frame, want)
	}
}

func TestMessageWriterFramesEmptyBody(t *testing.T) {
	frame := NewMessageWriter(MsgSync).Finish()
	want := []byte{'S', 0, 0, 0, 4}
	if !bytes.Equal(frame, want) {
		t.Fatalf("frame = %v, want %v", frame, want)
	}
}

func TestReadBufStickyError(t *testing.T) {
	r := &ReadBuf{B: []byte{0, 0, 0, 7}}
	if got := r.Int32(); got != 7 {
		t.Fatalf("int32 = %d, want 7", got)
	}
	if r.Err != nil {
		t.Fatalf("unexpected error after valid read: %v", r.Err)
	}
	r.Int16()
	if r.Err == nil {
		t.Fatal("reading past the end must set the sticky error")
	}
}

func TestReadBufStringNeedsTerminator(t *testing.T) {
	r := &ReadBuf{B: []byte("abc")}
	if got := r.String(); got != "" {
		t.Fatalf("string = %q, want empty result on malformed input", got)
	}
	if r.Err == nil {
		t.Fatal("missing terminator must set the sticky error")
	}
}

func TestReadMessageSplitAndCoalesced(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	first := NewMessageWriter(MsgParameterStatus)
	first.String("server_version")
	first.String("7.0.0")
	firstFrame := first.Finish()

	second := NewMessageWriter(MsgReadyForQuery)
	second.Byte(TxnIdle)
	secondFrame := second.Finish()

	go func() {
		defer func() { _ = server.Close() }()
		// First message split into single-byte writes.
		for _, b := range firstFrame {
			if _, err := server.Write([]byte{b}); err != nil {
				return
			}
		}
		// Two complete messages coalesced into a single write.
		if _, err := server.Write(append(secondFrame, secondFrame...)); err != nil {
			return
		}
	}()

	br := bufio.NewReader(client)

	typ, body, err := ReadMessage(br)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if typ != MsgParameterStatus {
		t.Fatalf("type = %q, want %q", typ, MsgParameterStatus)
	}
	r := &ReadBuf{B: body}
	if name, value := r.String(), r.String(); name != "server_version" || value != "7.0.0" {
		t.Fatalf("parameter = %q/%q", name, value)
	}
	if r.Err != nil {
		t.Fatalf("parse: %v", r.Err)
	}

	for i := 0; i < 2; i++ {
		typ, body, err = ReadMessage(br)
		if err != nil {
			t.Fatalf("ReadMessage %d: %v", i, err)
		}
		if typ != MsgReadyForQuery {
			t.Fatalf("type = %q, want ReadyForQuery", typ)
		}
		if len(body) != 1 || body[0] != TxnIdle {
			t.Fatalf("body = %v, want idle transaction status", body)
		}
	}
}

func TestReadMessageRejectsBadLength(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want string
	}{
		{"length below header", []byte{'Z', 0, 0, 0, 3}, "length"},
		{"length above cap", []byte{'Z', 0x7f, 0xff, 0xff, 0xff}, "too large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			br := bufio.NewReader(bytes.NewReader(tt.raw))
			_, _, err := ReadMessage(br)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not mention %q", err, tt.want)
			}
		})
	}
}

func TestReadMessageTruncated(t *testing.T) {
	// Header promises 4 bytes of payload but only 2 arrive.
	br := bufio.NewReader(bytes.NewReader([]byte{'D', 0, 0, 0, 8, 1, 2}))
	_, _, err := ReadMessage(br)
	if err == nil {
		t.Fatal("expected an error for a truncated message")
	}
	if err != io.ErrUnexpectedEOF && !strings.Contains(err.Error(), "unexpected EOF") {
		t.Errorf("error = %v, want an unexpected-EOF cause", err)
	}
}

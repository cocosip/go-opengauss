package opengauss

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strings"
	"testing"
)

func TestMessageWriterFramesQuery(t *testing.T) {
	w := newMessageWriter(msgQuery)
	w.string("SELECT 1")
	frame := w.finish()

	want := []byte{'Q', 0, 0, 0, 13, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0}
	if !bytes.Equal(frame, want) {
		t.Fatalf("frame = %v, want %v", frame, want)
	}
}

func TestMessageWriterFramesEmptyBody(t *testing.T) {
	frame := newMessageWriter(msgSync).finish()
	want := []byte{'S', 0, 0, 0, 4}
	if !bytes.Equal(frame, want) {
		t.Fatalf("frame = %v, want %v", frame, want)
	}
}

func TestReadBufStickyError(t *testing.T) {
	r := &readBuf{b: []byte{0, 0, 0, 7}}
	if got := r.int32(); got != 7 {
		t.Fatalf("int32 = %d, want 7", got)
	}
	if r.err != nil {
		t.Fatalf("unexpected error after valid read: %v", r.err)
	}
	r.int16()
	if r.err == nil {
		t.Fatal("reading past the end must set the sticky error")
	}
}

func TestReadBufStringNeedsTerminator(t *testing.T) {
	r := &readBuf{b: []byte("abc")}
	if got := r.string(); got != "" {
		t.Fatalf("string = %q, want empty result on malformed input", got)
	}
	if r.err == nil {
		t.Fatal("missing terminator must set the sticky error")
	}
}

func TestReadMessageSplitAndCoalesced(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	first := newMessageWriter(msgParameterStatus)
	first.string("server_version")
	first.string("7.0.0")
	firstFrame := first.finish()

	second := newMessageWriter(msgReadyForQuery)
	second.byte(txnIdle)
	secondFrame := second.finish()

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

	typ, body, err := readMessage(br)
	if err != nil {
		t.Fatalf("readMessage: %v", err)
	}
	if typ != msgParameterStatus {
		t.Fatalf("type = %q, want %q", typ, msgParameterStatus)
	}
	r := &readBuf{b: body}
	if name, value := r.string(), r.string(); name != "server_version" || value != "7.0.0" {
		t.Fatalf("parameter = %q/%q", name, value)
	}
	if r.err != nil {
		t.Fatalf("parse: %v", r.err)
	}

	for i := 0; i < 2; i++ {
		typ, body, err = readMessage(br)
		if err != nil {
			t.Fatalf("readMessage %d: %v", i, err)
		}
		if typ != msgReadyForQuery {
			t.Fatalf("type = %q, want ReadyForQuery", typ)
		}
		if len(body) != 1 || body[0] != txnIdle {
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
			_, _, err := readMessage(br)
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
	_, _, err := readMessage(br)
	if err == nil {
		t.Fatal("expected an error for a truncated message")
	}
	if err != io.ErrUnexpectedEOF && !strings.Contains(err.Error(), "unexpected EOF") {
		t.Errorf("error = %v, want an unexpected-EOF cause", err)
	}
}

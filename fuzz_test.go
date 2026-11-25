package opengauss

import (
	"bufio"
	"bytes"
	"github.com/cocosip/go-opengauss/internal/proto"
	"testing"
)

// The fuzz targets below assert that hostile backend bytes never panic the
// driver. `go test` runs the seed corpus; `go test -fuzz=Fuzz...` explores.

func FuzzReadMessage(f *testing.F) {
	f.Add([]byte{'Z', 0, 0, 0, 5, proto.TxnIdle})
	f.Add([]byte{'T', 0, 0, 0, 6, 0, 1})
	f.Add([]byte{'E', 0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = proto.ReadMessage(bufio.NewReader(bytes.NewReader(data)))
	})
}

func FuzzParseErrorResponse(f *testing.F) {
	f.Add([]byte{'S', 'E', 'R', 'R', 'O', 'R', 0, 'C', '4', '2', 'P', '0', '1', 0, 0})
	f.Add([]byte{0})
	f.Fuzz(func(t *testing.T, data []byte) {
		_ = parseErrorResponse(data)
	})
}

func FuzzParseRowDescription(f *testing.F) {
	f.Add([]byte{0, 1, 'a', 0, 0, 0, 0, 0, 0, 0, 0, 0, 23, 0, 4, 0xff, 0xff, 0xff, 0xff, 0, 0})
	f.Add([]byte{0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = parseRowDescription(data)
	})
}

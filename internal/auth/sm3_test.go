package auth

import (
	"encoding/hex"
	"testing"
)

func TestSM3KnownAnswers(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{"empty", nil, "1ab21d8355cfa17f8e61194831e81a8f22bec8c728fefb747ed035eb5082aa2b"},
		{"abc", []byte("abc"), "66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0"},
		{"55 bytes", []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			"288337eef51eec62e7544d7270424c8dbe656254c99852870a73b2453a6a7fb1"},
		{"56 bytes", []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			"ba00ebedaab54065a5fd4f9f56326016203166bcee3eed44ea868d59d67aa3c8"},
		{"one block", []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			"616ec433c359e7c2b19f360e2b8f2a1b6e9ed76b8dc1a7d207b31a5341c611e9"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hex.EncodeToString(sm3Sum(tt.data))
			if got != tt.want {
				t.Errorf("sm3Sum = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestSM3LongInput(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i)
	}
	got := hex.EncodeToString(sm3Sum(data))
	const want = "1f00bad6a72e851e0f6e94fd317f97b74d5fbc4c090aefb91e7554e3f9c8c7fb"
	if got != want {
		t.Errorf("sm3Sum = %s, want %s", got, want)
	}
}

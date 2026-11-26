package auth

import (
	"encoding/binary"
	"github.com/cocosip/go-opengauss/internal/proto"
	"strings"
	"testing"
)

func TestSHA256ProofKnownAnswers(t *testing.T) {
	tests := []struct {
		name      string
		password  string
		salt      string
		token     string
		iteration int
		wantProof string
	}{
		{
			// Vector published by the reference client's rfcdigest_test.go.
			name:      "reference client vector",
			password:  "sha256@abc123",
			salt:      "3458fe51abe962f7b6011a1d73fc14edf50539fae89fb9dda75fbb642d9859bf",
			token:     "2cf71f10",
			iteration: 10000,
			wantProof: "f5213af93974ae3f0a3eedba7b0ef27796bcd4d84bb737d9c35fce54fcfe28fd",
		},
		{
			// Vectors produced with the reference implementation at 1b2ca1e.
			name:      "iteration 2048",
			password:  "Secret123!",
			salt:      "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
			token:     "0102030405060708",
			iteration: 2048,
			wantProof: "017f1ac85ec64ebb081f0023cbaea11cca6db4c80598fe5feffa9a4727fb55ef",
		},
		{
			name:      "iteration 4096",
			password:  "Secret123!",
			salt:      "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
			token:     "0102030405060708",
			iteration: 4096,
			wantProof: "f8c4b9c3d9e69d53bc2aededb9e8b265e826fd1d1839d26604eb663dc51bc46a",
		},
		{
			name:      "iteration 1024",
			password:  "Secret123!",
			salt:      "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
			token:     "0102030405060708",
			iteration: 1024,
			wantProof: "843eac815e3d57bfc313faeaafb395c356f8a061a09db1b234ea6d78017b8204",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sha256Proof(tt.password, tt.salt, tt.token, tt.iteration)
			if err != nil {
				t.Fatalf("sha256Proof: %v", err)
			}
			if got != tt.wantProof {
				t.Errorf("proof = %s, want %s", got, tt.wantProof)
			}
		})
	}
}

func TestSHA256ProofRejectsBadInput(t *testing.T) {
	const (
		salt  = "3458fe51abe962f7b6011a1d73fc14edf50539fae89fb9dda75fbb642d9859bf"
		token = "2cf71f10"
	)
	tests := []struct {
		name      string
		password  string
		salt      string
		token     string
		iteration int
		wantSub   string
	}{
		{"empty password", "", salt, token, 10000, "password"},
		{"zero iterations", "pw", salt, token, 0, "iteration"},
		{"excessive iterations", "pw", salt, token, 2_000_000, "iteration"},
		{"salt not hex", "pw", "zz" + salt[2:], token, 10000, "random64code"},
		{"token not hex", "pw", salt, "zzzzzzzz", 10000, "token"},
		{"odd token", "pw", salt, "2cf71f1", 10000, "token"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sha256Proof(tt.password, tt.salt, tt.token, tt.iteration)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not mention %q", err, tt.wantSub)
			}
		})
	}
}

// RequestMD5SHA256Body builds the body of an MD5-SHA256 authentication
// request: a 64-character hex salt followed by a 4-byte MD5 salt.
func RequestMD5SHA256Body(random64code string, salt []byte) []byte {
	return append([]byte(random64code), salt...)
}

func TestMD5SHA256ProofKnownAnswer(t *testing.T) {
	const random64code = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	got, err := md5SHA256Proof("md5sha256@abc123", random64code, []byte{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("md5SHA256Proof: %v", err)
	}
	const want = "md5decfe7602711422c70372aeb7e53e4ab"
	if got != want {
		t.Errorf("proof = %s, want %s", got, want)
	}
}

func TestSM3ProofKnownAnswer(t *testing.T) {
	got, err := sm3Proof("sm3@abc123",
		"5ae737626add65f8da1b063104a6c4e2dc25b7343d8512a74826dc5b5e3e5188",
		"01020304", 10000)
	if err != nil {
		t.Fatalf("sm3Proof: %v", err)
	}
	const want = "5ac942bc74b003ab3d818ac2bd1ef3fcc40becc45bf393d550e8aa9f7932f7e3"
	if got != want {
		t.Errorf("proof = %s, want %s", got, want)
	}
}

func TestMD5PasswordKnownAnswers(t *testing.T) {
	tests := []struct {
		name     string
		password string
		user     string
		salt     []byte
		want     string
	}{
		{"alice", "alice", "alice", []byte{1, 2, 3, 4}, "md5c0bdd3ab2715422c9d1adb98cdd9e645"},
		{"bob", "Secret123!", "bob", []byte{222, 173, 190, 239}, "md5989d54d53eb3f075593466e7da5b821b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := md5Password(tt.password, tt.user, tt.salt); got != tt.want {
				t.Errorf("md5Password = %s, want %s", got, tt.want)
			}
		})
	}
}

// RequestSHA256Body builds the body of an AuthenticationSHA256Request.
func RequestSHA256Body(stored int32, salt, token string, iteration int32) []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(stored))
	b = append(b, salt...)
	b = append(b, token...)
	return binary.BigEndian.AppendUint32(b, uint32(iteration))
}

const (
	katSalt  = "3458fe51abe962f7b6011a1d73fc14edf50539fae89fb9dda75fbb642d9859bf"
	katToken = "2cf71f10"
	katProof = "f5213af93974ae3f0a3eedba7b0ef27796bcd4d84bb737d9c35fce54fcfe28fd"
)

func TestAuthResponseSHA256StoredMethods(t *testing.T) {
	cfg := creds{user: "alice", password: "sha256@abc123"}
	for _, stored := range []int32{StoredPlain, StoredSHA256} {
		body := RequestSHA256Body(stored, katSalt, katToken, 10000)
		payload, err := Response(RequestSHA256, &proto.ReadBuf{B: body}, cfg.user, cfg.password)
		if err != nil {
			t.Fatalf("stored method %d: %v", stored, err)
		}
		want := katProof + "\x00"
		if string(payload) != want {
			t.Errorf("stored method %d: payload = %q, want %q", stored, payload, want)
		}
	}
}

func TestAuthResponseSHA256StoredMD5(t *testing.T) {
	cfg := creds{user: "alice", password: "alice"}
	body := binary.BigEndian.AppendUint32(nil, uint32(StoredMD5))
	body = append(body, 1, 2, 3, 4)
	payload, err := Response(RequestSHA256, &proto.ReadBuf{B: body}, cfg.user, cfg.password)
	if err != nil {
		t.Fatalf("authResponse: %v", err)
	}
	if want := "md5c0bdd3ab2715422c9d1adb98cdd9e645\x00"; string(payload) != want {
		t.Errorf("payload = %q, want %q", payload, want)
	}
}

func TestAuthResponseSHA256RejectsUnknownStoredMethod(t *testing.T) {
	cfg := creds{user: "alice", password: "pw"}
	body := RequestSHA256Body(9, katSalt, katToken, 10000)
	_, err := Response(RequestSHA256, &proto.ReadBuf{B: body}, cfg.user, cfg.password)
	if err == nil || !strings.Contains(err.Error(), "stored password method") {
		t.Fatalf("error = %v, want unsupported stored method", err)
	}
}

func TestAuthResponseSHA256RejectsMissingPassword(t *testing.T) {
	body := RequestSHA256Body(StoredSHA256, katSalt, katToken, 10000)
	_, err := Response(RequestSHA256, &proto.ReadBuf{B: body}, "alice", "")
	if err == nil || !strings.Contains(err.Error(), "password") {
		t.Fatalf("error = %v, want a missing-password error", err)
	}
}

func TestAuthResponseSHA256RejectsTruncatedBody(t *testing.T) {
	body := binary.BigEndian.AppendUint32(nil, uint32(StoredSHA256))
	_, err := Response(RequestSHA256, &proto.ReadBuf{B: body}, "alice", "pw")
	if err == nil {
		t.Fatal("expected an error for a truncated authentication request")
	}
}

func TestAuthResponseMD5SHA256(t *testing.T) {
	const random64code = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	body := RequestMD5SHA256Body(random64code, []byte{1, 2, 3, 4})
	payload, err := Response(RequestMD5SHA256, &proto.ReadBuf{B: body}, "alice", "md5sha256@abc123")
	if err != nil {
		t.Fatalf("Response: %v", err)
	}
	if want := "md5decfe7602711422c70372aeb7e53e4ab\x00"; string(payload) != want {
		t.Errorf("payload = %q, want %q", payload, want)
	}
}

func TestAuthResponseMD5SHA256RejectsMissingPassword(t *testing.T) {
	const random64code = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	body := RequestMD5SHA256Body(random64code, []byte{1, 2, 3, 4})
	_, err := Response(RequestMD5SHA256, &proto.ReadBuf{B: body}, "alice", "")
	if err == nil || !strings.Contains(err.Error(), "password") {
		t.Fatalf("error = %v, want a missing-password error", err)
	}
}

func TestAuthResponseMD5SHA256RejectsTruncatedBody(t *testing.T) {
	body := []byte("0123456789abcdef")
	_, err := Response(RequestMD5SHA256, &proto.ReadBuf{B: body}, "alice", "pw")
	if err == nil {
		t.Fatal("expected an error for a truncated authentication request")
	}
}

func TestAuthResponseSM3(t *testing.T) {
	body := RequestSHA256Body(StoredSM3,
		"5ae737626add65f8da1b063104a6c4e2dc25b7343d8512a74826dc5b5e3e5188",
		"01020304", 10000)
	payload, err := Response(RequestSM3, &proto.ReadBuf{B: body}, "alice", "sm3@abc123")
	if err != nil {
		t.Fatalf("Response: %v", err)
	}
	want := "5ac942bc74b003ab3d818ac2bd1ef3fcc40becc45bf393d550e8aa9f7932f7e3\x00"
	if string(payload) != want {
		t.Errorf("payload = %q, want %q", payload, want)
	}
}

func TestAuthResponseSM3RejectsOtherStoredMethod(t *testing.T) {
	body := RequestSHA256Body(StoredSHA256, katSalt, katToken, 10000)
	_, err := Response(RequestSM3, &proto.ReadBuf{B: body}, "alice", "pw")
	if err == nil || !strings.Contains(err.Error(), "stored password method") {
		t.Fatalf("error = %v, want unsupported stored method", err)
	}
}

func TestAuthResponseSM3RejectsMissingPassword(t *testing.T) {
	body := RequestSHA256Body(StoredSM3, katSalt, katToken, 10000)
	_, err := Response(RequestSM3, &proto.ReadBuf{B: body}, "alice", "")
	if err == nil || !strings.Contains(err.Error(), "password") {
		t.Fatalf("error = %v, want a missing-password error", err)
	}
}

func TestAuthResponseSM3RejectsTruncatedBody(t *testing.T) {
	body := binary.BigEndian.AppendUint32(nil, uint32(StoredSM3))
	_, err := Response(RequestSM3, &proto.ReadBuf{B: body}, "alice", "pw")
	if err == nil {
		t.Fatal("expected an error for a truncated authentication request")
	}
}

func TestAuthResponseOtherMethods(t *testing.T) {
	cfg := creds{user: "alice", password: "alice"}

	payload, err := Response(RequestCleartext, &proto.ReadBuf{}, cfg.user, cfg.password)
	if err != nil {
		t.Fatalf("cleartext: %v", err)
	}
	if string(payload) != "alice\x00" {
		t.Errorf("cleartext payload = %q", payload)
	}

	payload, err = Response(RequestMD5, &proto.ReadBuf{B: []byte{1, 2, 3, 4}}, cfg.user, cfg.password)
	if err != nil {
		t.Fatalf("md5: %v", err)
	}
	if want := "md5c0bdd3ab2715422c9d1adb98cdd9e645\x00"; string(payload) != want {
		t.Errorf("md5 payload = %q, want %q", payload, want)
	}

	payload, err = Response(OK, &proto.ReadBuf{}, cfg.user, cfg.password)
	if err != nil || payload != nil {
		t.Fatalf("AuthenticationOk = %q/%v, want no payload", payload, err)
	}
}

func TestAuthResponseUnsupportedMethods(t *testing.T) {
	tests := []struct {
		code    int32
		wantSub string
	}{
		{RequestGSS, "GSSAPI"},
		{RequestGSSContinue, "GSSAPI"},
		{RequestSSPI, "SSPI"},
		{RequestLiteSHA256, "Lite SHA256_RFC"},
		{99, "request 99"},
	}
	for _, tt := range tests {
		t.Run(methodName(tt.code), func(t *testing.T) {
			_, err := Response(tt.code, &proto.ReadBuf{}, "alice", "alice")
			if err == nil {
				t.Fatal("expected an unsupported-method error")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not mention %q", err, tt.wantSub)
			}
			if !strings.Contains(err.Error(), "unsupported authentication") {
				t.Errorf("error %q does not identify the stage", err)
			}
		})
	}
}

type creds struct{ user, password string }

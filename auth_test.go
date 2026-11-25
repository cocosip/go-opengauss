package opengauss

import (
	"encoding/binary"
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

// authSHA256Body builds the body of an AuthenticationSHA256Request.
func authSHA256Body(stored int32, salt, token string, iteration int32) []byte {
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
	cfg := &Config{User: "alice", Password: "sha256@abc123"}
	for _, stored := range []int32{passwordStoredPlain, passwordStoredSHA256} {
		body := authSHA256Body(stored, katSalt, katToken, 10000)
		payload, err := authResponse(authSHA256, &readBuf{b: body}, cfg)
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
	cfg := &Config{User: "alice", Password: "alice"}
	body := binary.BigEndian.AppendUint32(nil, uint32(passwordStoredMD5))
	body = append(body, 1, 2, 3, 4)
	payload, err := authResponse(authSHA256, &readBuf{b: body}, cfg)
	if err != nil {
		t.Fatalf("authResponse: %v", err)
	}
	if want := "md5c0bdd3ab2715422c9d1adb98cdd9e645\x00"; string(payload) != want {
		t.Errorf("payload = %q, want %q", payload, want)
	}
}

func TestAuthResponseSHA256RejectsUnknownStoredMethod(t *testing.T) {
	cfg := &Config{User: "alice", Password: "pw"}
	body := authSHA256Body(9, katSalt, katToken, 10000)
	_, err := authResponse(authSHA256, &readBuf{b: body}, cfg)
	if err == nil || !strings.Contains(err.Error(), "stored password method") {
		t.Fatalf("error = %v, want unsupported stored method", err)
	}
}

func TestAuthResponseSHA256RejectsMissingPassword(t *testing.T) {
	body := authSHA256Body(passwordStoredSHA256, katSalt, katToken, 10000)
	_, err := authResponse(authSHA256, &readBuf{b: body}, &Config{User: "alice"})
	if err == nil || !strings.Contains(err.Error(), "password") {
		t.Fatalf("error = %v, want a missing-password error", err)
	}
}

func TestAuthResponseSHA256RejectsTruncatedBody(t *testing.T) {
	body := binary.BigEndian.AppendUint32(nil, uint32(passwordStoredSHA256))
	_, err := authResponse(authSHA256, &readBuf{b: body}, &Config{User: "alice", Password: "pw"})
	if err == nil {
		t.Fatal("expected an error for a truncated authentication request")
	}
}

func TestAuthResponseOtherMethods(t *testing.T) {
	cfg := &Config{User: "alice", Password: "alice"}

	payload, err := authResponse(authCleartextPassword, &readBuf{}, cfg)
	if err != nil {
		t.Fatalf("cleartext: %v", err)
	}
	if string(payload) != "alice\x00" {
		t.Errorf("cleartext payload = %q", payload)
	}

	payload, err = authResponse(authMD5Password, &readBuf{b: []byte{1, 2, 3, 4}}, cfg)
	if err != nil {
		t.Fatalf("md5: %v", err)
	}
	if want := "md5c0bdd3ab2715422c9d1adb98cdd9e645\x00"; string(payload) != want {
		t.Errorf("md5 payload = %q, want %q", payload, want)
	}

	payload, err = authResponse(authOK, &readBuf{}, cfg)
	if err != nil || payload != nil {
		t.Fatalf("AuthenticationOk = %q/%v, want no payload", payload, err)
	}
}

func TestAuthResponseUnsupportedMethods(t *testing.T) {
	tests := []struct {
		code    int32
		wantSub string
	}{
		{authGSS, "GSSAPI"},
		{authGSSContinue, "GSSAPI"},
		{authSSPI, "SSPI"},
		{authMD5SHA256, "MD5-SHA256"},
		{authLiteSHA256, "Lite SHA256_RFC"},
		{authSM3, "SM3"},
		{99, "request 99"},
	}
	for _, tt := range tests {
		t.Run(authMethodName(tt.code), func(t *testing.T) {
			_, err := authResponse(tt.code, &readBuf{}, &Config{User: "alice", Password: "alice"})
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

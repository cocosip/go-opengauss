// Package auth implements the password exchanges of the openGauss protocol.
// It parses AuthenticationRequest bodies and produces the PasswordMessage
// payloads; it does no I/O.
package auth

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/pbkdf2"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/cocosip/go-opengauss/internal/proto"
)

// Authentication request codes from src/include/libpq/pqcomm.h.
const (
	OK                 int32 = 0
	RequestCleartext   int32 = 3
	RequestMD5         int32 = 5
	RequestGSS         int32 = 7
	RequestGSSContinue int32 = 8
	RequestSSPI        int32 = 9
	RequestSHA256      int32 = 10
	RequestMD5SHA256   int32 = 11
	RequestLiteSHA256  int32 = 12
	RequestSM3         int32 = 13
)

// Password storage methods reported inside an authenticated request.
const (
	StoredPlain  int32 = 0
	StoredMD5    int32 = 1
	StoredSHA256 int32 = 2
	StoredSM3    int32 = 3
)

// maxProofIterations caps the PBKDF2 work a server may demand. Real servers
// use 2048 or 10000 iterations; a hostile peer must not pin a CPU core.
const maxProofIterations = 1_000_000

// methodName describes an authentication request for errors and logs.
func methodName(code int32) string {
	switch code {
	case OK:
		return "AuthenticationOk"
	case RequestCleartext:
		return "cleartext password"
	case RequestMD5:
		return "MD5"
	case RequestGSS:
		return "GSSAPI"
	case RequestGSSContinue:
		return "GSSAPI continue"
	case RequestSSPI:
		return "SSPI"
	case RequestSHA256:
		return "SHA256"
	case RequestMD5SHA256:
		return "MD5-SHA256"
	case RequestLiteSHA256:
		return "Lite SHA256_RFC"
	case RequestSM3:
		return "SM3"
	default:
		return fmt.Sprintf("unknown (request %d)", code)
	}
}

// Response produces the PasswordMessage body for one AuthenticationRequest.
// The cursor must be positioned after the request code. It returns a nil
// payload when the request needs no response.
//
// GSSAPI requests need a provider context that this package does not own, so
// the connection layer handles 7/8. Exchanges that are still unimplemented are
// rejected with the method name instead of being silently skipped.
func Response(code int32, r *proto.ReadBuf, user, password string) ([]byte, error) {
	switch code {
	case OK:
		return nil, nil
	case RequestCleartext:
		if password == "" {
			return nil, errors.New("server requested a password but the DSN supplied none")
		}
		return append([]byte(password), 0), nil
	case RequestMD5:
		salt := r.Bytes(4)
		if r.Err != nil {
			return nil, r.Err
		}
		return append([]byte(md5Password(password, user, salt)), 0), nil
	case RequestSHA256:
		return sha256Response(r, user, password)
	case RequestMD5SHA256:
		return md5SHA256Response(r, password)
	case RequestLiteSHA256:
		return nil, unsupported(code, "request 12 needs a matching Lite build and is not implemented yet")
	case RequestSM3:
		return sm3Response(r, password)
	case RequestGSS, RequestGSSContinue:
		return nil, unsupported(code, "the connection layer handles GSSAPI with a provider registered by RegisterGSSProvider")
	case RequestSSPI:
		return nil, unsupported(code, "Windows integrated logon is not implemented yet")
	default:
		return nil, unsupported(code, "the driver does not know this exchange")
	}
}

func unsupported(code int32, hint string) error {
	return fmt.Errorf("unsupported authentication method %s: %s", methodName(code), hint)
}

func sha256Response(r *proto.ReadBuf, user, password string) ([]byte, error) {
	storedMethod := r.Int32()
	if r.Err != nil {
		return nil, r.Err
	}
	switch storedMethod {
	case StoredPlain, StoredSHA256:
		if password == "" {
			return nil, errors.New("server requested password authentication but the DSN supplied no password")
		}
		random64code := string(r.Bytes(64))
		token := string(r.Bytes(8))
		iteration := int(r.Int32())
		if r.Err != nil {
			return nil, r.Err
		}
		proof, err := sha256Proof(password, random64code, token, iteration)
		if err != nil {
			return nil, err
		}
		return append([]byte(proof), 0), nil
	case StoredMD5:
		salt := r.Bytes(4)
		if r.Err != nil {
			return nil, r.Err
		}
		return append([]byte(md5Password(password, user, salt)), 0), nil
	default:
		return nil, fmt.Errorf("unsupported stored password method %d: the server must store the password as plain, md5 or sha256", storedMethod)
	}
}

// md5SHA256Response answers request 11: the server sends a 64-character hex
// salt and a 4-byte MD5 salt, and the response carries the "md5" prefix.
func md5SHA256Response(r *proto.ReadBuf, password string) ([]byte, error) {
	if password == "" {
		return nil, errors.New("server requested password authentication but the DSN supplied no password")
	}
	random64code := string(r.Bytes(64))
	salt := r.Bytes(4)
	if r.Err != nil {
		return nil, r.Err
	}
	proof, err := md5SHA256Proof(password, random64code, salt)
	if err != nil {
		return nil, err
	}
	return append([]byte(proof), 0), nil
}

// md5SHA256Proof derives the request-11 proof used when the server stores an
// MD5-SHA256 verifier. The layout matches the reference JDBC client:
//
//	K = PBKDF2-HMAC-SHA1(password, random64code, 2048, 32)
//	proof = MD5(random64code + hex(HMAC-SHA256(K, "Sever Key")) +
//	            hex(SHA256(HMAC-SHA256(K, "Client Key"))) + md5Salt)
//
// "Sever Key" is the server's misspelling and stays as it is on the wire.
func md5SHA256Proof(password, random64code string, salt []byte) (string, error) {
	random32code, err := hex.DecodeString(random64code)
	if err != nil {
		return "", fmt.Errorf("invalid random64code %q: %w", random64code, err)
	}
	key, err := pbkdf2.Key(sha1.New, password, random32code, 2048, sha256.Size)
	if err != nil {
		return "", fmt.Errorf("derive MD5-SHA256 authentication key: %w", err)
	}
	serverKey := hmacSHA256(key, []byte("Sever Key"))
	clientKey := hmacSHA256(key, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	encoded := random64code + hex.EncodeToString(serverKey) + hex.EncodeToString(storedKey[:])
	sum := md5.Sum(append([]byte(encoded), salt...))
	return "md5" + hex.EncodeToString(sum[:]), nil
}

// sm3Response answers request 13, whose layout matches request 10 but whose
// stored key is hashed with SM3.
func sm3Response(r *proto.ReadBuf, password string) ([]byte, error) {
	storedMethod := r.Int32()
	if r.Err != nil {
		return nil, r.Err
	}
	if storedMethod != StoredSM3 {
		return nil, fmt.Errorf("unsupported stored password method %d: the server must store the password as sm3", storedMethod)
	}
	if password == "" {
		return nil, errors.New("server requested password authentication but the DSN supplied no password")
	}
	random64code := string(r.Bytes(64))
	token := string(r.Bytes(8))
	iteration := int(r.Int32())
	if r.Err != nil {
		return nil, r.Err
	}
	proof, err := sm3Proof(password, random64code, token, iteration)
	if err != nil {
		return nil, err
	}
	return append([]byte(proof), 0), nil
}

// sha256Proof computes the RFC5802-style client proof used by openGauss
// request 10. The server sends a 64-char hex salt, an 8-char hex token and an
// iteration count; the proof is
//
//	K = PBKDF2-HMAC-SHA1(password, salt, iterations, 32)
//	clientKey = HMAC-SHA256(K, "Client Key")
//	proof = HMAC-SHA256(SHA256(clientKey), token) XOR clientKey
//
// The upstream implementation also derives a server key from "Sever Key",
// misspelling included, to verify a server proof. Request 10 does not supply
// one, so only the client key is derived here.
func sha256Proof(password, random64code, token string, iteration int) (string, error) {
	return rfc5802Proof(password, random64code, token, iteration, func(b []byte) []byte {
		sum := sha256.Sum256(b)
		return sum[:]
	})
}

// sm3Proof is request 13's variant of the RFC5802 proof: only the stored key
// uses SM3 instead of SHA-256.
func sm3Proof(password, random64code, token string, iteration int) (string, error) {
	return rfc5802Proof(password, random64code, token, iteration, sm3Sum)
}

// rfc5802Proof derives the client proof for requests 10 and 13. The caller
// selects the hash that produces the stored key from the client key.
func rfc5802Proof(password, random64code, token string, iteration int, storedHash func([]byte) []byte) (string, error) {
	if password == "" {
		return "", errors.New("empty password: the server requested password authentication")
	}
	if iteration <= 0 || iteration > maxProofIterations {
		return "", fmt.Errorf("invalid authentication iteration count %d", iteration)
	}
	salt, err := hex.DecodeString(random64code)
	if err != nil {
		return "", fmt.Errorf("invalid random64code %q: %w", random64code, err)
	}
	tokenBytes, err := hex.DecodeString(token)
	if err != nil {
		return "", fmt.Errorf("invalid token %q: %w", token, err)
	}

	key, err := pbkdf2.Key(sha1.New, password, salt, iteration, sha256.Size)
	if err != nil {
		return "", fmt.Errorf("derive authentication key: %w", err)
	}
	clientKey := hmacSHA256(key, []byte("Client Key"))
	storedKey := storedHash(clientKey)
	hmacResult := hmacSHA256(storedKey, tokenBytes)

	proof := make([]byte, sha256.Size)
	for i := range proof {
		proof[i] = hmacResult[i] ^ clientKey[i]
	}
	return hex.EncodeToString(proof), nil
}

func hmacSHA256(key, message []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(message)
	return mac.Sum(nil)
}

// md5Password computes the PostgreSQL-style md5 response. It is only used for
// wire compatibility with servers that still store md5 password hashes.
func md5Password(password, user string, salt []byte) string {
	inner := md5.Sum([]byte(password + user))
	innerHex := hex.EncodeToString(inner[:])
	outer := md5.New()
	outer.Write([]byte(innerHex))
	outer.Write(salt)
	return "md5" + hex.EncodeToString(outer.Sum(nil))
}

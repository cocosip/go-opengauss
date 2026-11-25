package opengauss

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/pbkdf2"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

// Authentication request codes from src/include/libpq/pqcomm.h.
const (
	authOK                int32 = 0
	authCleartextPassword int32 = 3
	authMD5Password       int32 = 5
	authGSS               int32 = 7
	authGSSContinue       int32 = 8
	authSSPI              int32 = 9
	authSHA256            int32 = 10
	authMD5SHA256         int32 = 11
	authLiteSHA256        int32 = 12
	authSM3               int32 = 13
)

// Password storage methods reported inside an AuthenticationSHA256Request.
const (
	passwordStoredPlain  int32 = 0
	passwordStoredMD5    int32 = 1
	passwordStoredSHA256 int32 = 2
)

// maxSHA256Iterations caps the PBKDF2 work a server may demand. Real servers
// use 2048 or 10000 iterations; a hostile peer must not pin a CPU core.
const maxSHA256Iterations = 1_000_000

// authMethodName describes an authentication request for errors and logs.
func authMethodName(code int32) string {
	switch code {
	case authOK:
		return "AuthenticationOk"
	case authCleartextPassword:
		return "cleartext password"
	case authMD5Password:
		return "MD5"
	case authGSS:
		return "GSSAPI"
	case authGSSContinue:
		return "GSSAPI continue"
	case authSSPI:
		return "SSPI"
	case authSHA256:
		return "SHA256"
	case authMD5SHA256:
		return "MD5-SHA256"
	case authLiteSHA256:
		return "Lite SHA256_RFC"
	case authSM3:
		return "SM3"
	default:
		return fmt.Sprintf("unknown (request %d)", code)
	}
}

// authResponse produces the PasswordMessage body for one AuthenticationRequest
// body. It returns a nil payload when the request needs no response.
//
// The openGauss-specific exchanges that are not implemented yet are rejected
// with the method name instead of being silently skipped.
func authResponse(code int32, r *readBuf, cfg *Config) ([]byte, error) {
	switch code {
	case authOK:
		return nil, nil
	case authCleartextPassword:
		if cfg.Password == "" {
			return nil, errors.New("server requested a password but the DSN supplied none")
		}
		return append([]byte(cfg.Password), 0), nil
	case authMD5Password:
		salt := r.bytes(4)
		if r.err != nil {
			return nil, r.err
		}
		return append([]byte(md5Password(cfg.Password, cfg.User, salt)), 0), nil
	case authSHA256:
		return authSHA256Response(r, cfg)
	case authMD5SHA256:
		return nil, unsupportedAuth(code, "use a server that requests SHA256, MD5 or cleartext password authentication")
	case authLiteSHA256:
		return nil, unsupportedAuth(code, "request 12 needs a matching Lite build and is not implemented yet")
	case authSM3:
		return nil, unsupportedAuth(code, "SM3 authentication is not implemented yet")
	case authGSS, authGSSContinue:
		return nil, unsupportedAuth(code, "Kerberos/SPNEGO authentication is not implemented yet")
	case authSSPI:
		return nil, unsupportedAuth(code, "Windows integrated logon is not implemented yet")
	default:
		return nil, unsupportedAuth(code, "the driver does not know this exchange")
	}
}

func unsupportedAuth(code int32, hint string) error {
	return fmt.Errorf("unsupported authentication method %s: %s", authMethodName(code), hint)
}

func authSHA256Response(r *readBuf, cfg *Config) ([]byte, error) {
	storedMethod := r.int32()
	if r.err != nil {
		return nil, r.err
	}
	switch storedMethod {
	case passwordStoredPlain, passwordStoredSHA256:
		if cfg.Password == "" {
			return nil, errors.New("server requested password authentication but the DSN supplied no password")
		}
		random64code := string(r.bytes(64))
		token := string(r.bytes(8))
		iteration := int(r.int32())
		if r.err != nil {
			return nil, r.err
		}
		proof, err := sha256Proof(cfg.Password, random64code, token, iteration)
		if err != nil {
			return nil, err
		}
		return append([]byte(proof), 0), nil
	case passwordStoredMD5:
		salt := r.bytes(4)
		if r.err != nil {
			return nil, r.err
		}
		return append([]byte(md5Password(cfg.Password, cfg.User, salt)), 0), nil
	default:
		return nil, fmt.Errorf("unsupported stored password method %d: the server must store the password as plain, md5 or sha256", storedMethod)
	}
}

// sha256Proof computes the RFC5802-style client proof used by openGauss
// request 10. The server sends a 64-character hex salt, an 8-character hex
// token and an iteration count; the proof is
//
//	K = PBKDF2-HMAC-SHA1(password, salt, iterations, 32)
//	clientKey = HMAC-SHA256(K, "Client Key")
//	proof = HMAC-SHA256(SHA256(clientKey), token) XOR clientKey
//
// The upstream implementation also derives a server key from "Sever Key",
// misspelling included, to verify a server proof. Request 10 does not supply
// one, so only the client key is derived here.
func sha256Proof(password, random64code, token string, iteration int) (string, error) {
	if password == "" {
		return "", errors.New("empty password: the server requested SHA256 authentication")
	}
	if iteration <= 0 || iteration > maxSHA256Iterations {
		return "", fmt.Errorf("invalid SHA256 iteration count %d", iteration)
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
		return "", fmt.Errorf("derive SHA256 authentication key: %w", err)
	}
	clientKey := hmacSHA256(key, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	hmacResult := hmacSHA256(storedKey[:], tokenBytes)

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

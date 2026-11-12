package relay

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// SignatureHeader is the request header that carries the signature.
const SignatureHeader = "X-Signature-256"

const signaturePrefix = "sha256="

// Sign returns the header value ("sha256=<hex>") for a body and secret.
func Sign(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return signaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

// Verify checks a header value against the body. The comparison runs in
// constant time so the response time does not leak how much matched.
func Verify(secret, body []byte, header string) bool {
	if !strings.HasPrefix(header, signaturePrefix) {
		return false
	}
	got, err := hex.DecodeString(strings.TrimPrefix(header, signaturePrefix))
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hmac.Equal(got, mac.Sum(nil))
}

// SPDX-License-Identifier: Apache-2.0

package callback

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"
)

func TestSignVerifyRoundTrip(t *testing.T) {
	secret := []byte("s3cr3t")
	body := []byte(`{"amount":100}`)
	const ts int64 = 1721484202

	header := Sign(secret, body, ts)

	gotTS, err := Verify(secret, body, header)
	if err != nil {
		t.Fatalf("Verify failed on a freshly signed request: %v", err)
	}
	if gotTS != ts {
		t.Errorf("Verify returned ts %d, want %d", gotTS, ts)
	}
}

// TestSignMatchesIndependentHMAC pins the signed-message construction
// (`<ts>.<body>`) so a receiver in any language can reproduce it.
func TestSignMatchesIndependentHMAC(t *testing.T) {
	secret := []byte("key")
	body := []byte("payload-bytes")
	const ts int64 = 1000

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(ts, 10) + "."))
	mac.Write(body)
	want := "t=" + strconv.FormatInt(ts, 10) + ",v1=" + hex.EncodeToString(mac.Sum(nil))

	if got := Sign(secret, body, ts); got != want {
		t.Errorf("Sign = %q, want %q", got, want)
	}
}

func TestVerifyRejectsTamperedBody(t *testing.T) {
	secret := []byte("key")
	header := Sign(secret, []byte("original"), 42)

	if _, err := Verify(secret, []byte("tampered"), header); err == nil {
		t.Fatal("Verify accepted a body that differs from the signed one")
	}
}

func TestVerifyRejectsWrongSecret(t *testing.T) {
	body := []byte("body")
	header := Sign([]byte("right"), body, 42)

	if _, err := Verify([]byte("wrong"), body, header); err == nil {
		t.Fatal("Verify accepted a signature made with a different secret")
	}
}

func TestParseSignatureErrors(t *testing.T) {
	cases := map[string]string{
		"missing v1":     "t=100",
		"missing t":      "v1=deadbeef",
		"empty":          "",
		"bad timestamp":  "t=notanumber,v1=deadbeef",
		"no assignments": "garbage",
	}
	for name, header := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := ParseSignature(header); err == nil {
				t.Errorf("ParseSignature(%q) succeeded, want error", header)
			}
		})
	}
}

// TestParseSignatureFieldOrder confirms the two fields parse regardless of order.
func TestParseSignatureFieldOrder(t *testing.T) {
	ts, v1, err := ParseSignature("v1=abc123,t=77")
	if err != nil {
		t.Fatalf("ParseSignature: %v", err)
	}
	if ts != 77 || v1 != "abc123" {
		t.Errorf("got ts=%d v1=%q, want 77/abc123", ts, v1)
	}
}

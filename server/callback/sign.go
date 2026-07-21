// SPDX-License-Identifier: Apache-2.0

// Package callback dispatches a task's raw payload to a queue's configured HTTP
// callback endpoint and classifies the receiver's response into the engine's
// retry/terminal outcome (design 04 §4, FR-29). It is the server-mode execution
// path, the counterpart to the embedded SDK's in-process handler.
package callback

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// SignatureHeader carries the HMAC signature so a receiver can verify the caller
// (design 04 §4). Its value is `t=<unix>,v1=<hex(hmac-sha256(secret,"<t>.<body>"))>`:
// standard webhook hygiene, with the timestamp bounding replay.
const SignatureHeader = "X-RDQ-Signature"

// tag computes the hex-encoded HMAC-SHA256 of the signed message "<ts>.<body>".
// Splitting the timestamp into the signed input is what lets a receiver reject a
// captured-and-replayed request: the signature only validates for the ts it was
// minted with.
func tag(secret, body []byte, ts int64) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// Sign returns the SignatureHeader value binding body to ts under secret.
func Sign(secret, body []byte, ts int64) string {
	return fmt.Sprintf("t=%d,v1=%s", ts, tag(secret, body, ts))
}

// ParseSignature splits a SignatureHeader value into its timestamp and v1 tag.
// It tolerates the fields in any order but requires both to be present.
func ParseSignature(header string) (ts int64, v1 string, err error) {
	var haveTS, haveV1 bool
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch k {
		case "t":
			ts, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return 0, "", fmt.Errorf("callback: bad signature timestamp %q: %w", v, err)
			}
			haveTS = true
		case "v1":
			v1, haveV1 = v, true
		}
	}
	if !haveTS || !haveV1 {
		return 0, "", errors.New("callback: signature missing t= or v1= field")
	}
	return ts, v1, nil
}

// Verify recomputes the tag over body for the timestamp carried in header and
// constant-time compares it against the presented v1 (hmac.Equal), returning the
// signed timestamp so a caller may additionally enforce a freshness window. A
// verification failure — bad format or tag mismatch — is a non-nil error. This
// is the function a receiver (or test) uses to independently confirm a request
// came from rdq.
func Verify(secret, body []byte, header string) (ts int64, err error) {
	ts, v1, err := ParseSignature(header)
	if err != nil {
		return 0, err
	}
	want, errDec := hex.DecodeString(v1)
	if errDec != nil {
		return 0, fmt.Errorf("callback: signature v1 is not hex: %w", errDec)
	}
	got, _ := hex.DecodeString(tag(secret, body, ts))
	if !hmac.Equal(got, want) {
		return 0, errors.New("callback: signature mismatch")
	}
	return ts, nil
}

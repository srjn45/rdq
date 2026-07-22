// SPDX-License-Identifier: Apache-2.0

package policy

import (
	"errors"
	"fmt"
	"testing"

	"github.com/srjn45/rdq/core/config"
	"github.com/srjn45/rdq/core/envelope"
)

// --- fixtures shared across the ladder tests -------------------------------

// sentinelErr is a package-level sentinel matched via errors.Is (IsRule).
var sentinelErr = errors.New("policy_test: sentinel")

// typedErr is a concrete error type matched via errors.As (AsRule).
type typedErr struct{ msg string }

func (e *typedErr) Error() string { return e.msg }

// exampleClassification is the design-03 §4 example block, reused so the glob
// cases exercise the same patterns the docs advertise.
func exampleClassification() *config.ClassificationConfig {
	return &config.ClassificationConfig{
		Retryable: []string{"java.net.*", "TIMEOUT"},
		Permanent: []string{"*.ValidationException"},
	}
}

// TestClassifyLadder drives one table across all five precedence layers,
// asserting both the Decision and the Layer that produced it so a rung firing
// out of order is caught, not just a wrong final answer.
func TestClassifyLadder(t *testing.T) {
	// A classifier wired with every optional layer populated, so each case can
	// prove its rung wins over everything below it.
	full := Classifier{
		Mapper: func(err error) (Decision, bool) {
			// Claims only errors carrying "map-me"; declines everything else.
			if err != nil && err.Error() == "map-me" {
				return DecisionPermanent, true
			}
			return DecisionRetryable, false
		},
		CodeRules: []CodeRule{
			IsRule(sentinelErr, DecisionPermanent),
			AsRule[*typedErr](DecisionPermanent),
		},
		Config: exampleClassification(),
	}

	tests := []struct {
		name    string
		clf     Classifier
		err     error
		errType string
		want    Verdict
	}{
		{
			name:    "layer1 mapper is authoritative over everything below",
			clf:     full,
			err:     errors.New("map-me"),
			errType: "com.acme.ValidationException", // would be permanent via glob anyway; proves mapper ran
			want:    Verdict{DecisionPermanent, LayerOutcomeMapper},
		},
		{
			name:    "layer1 mapper declines then defers to lower layers",
			clf:     full,
			err:     errors.New("not-mapped"),
			errType: "java.net.SocketTimeoutException",
			want:    Verdict{DecisionRetryable, LayerConfigGlob},
		},
		{
			name:    "layer2 wrapper overrides a code rule that would say permanent",
			clf:     full,
			err:     Retryable(sentinelErr), // sentinel → IsRule permanent, but wrapper forces retryable
			errType: "irrelevant",
			want:    Verdict{DecisionRetryable, LayerWrapper},
		},
		{
			name:    "layer2 wrapper overrides a permanent glob",
			clf:     full,
			err:     Retryable(errors.New("boom")),
			errType: "com.acme.ValidationException",
			want:    Verdict{DecisionRetryable, LayerWrapper},
		},
		{
			name:    "layer2 wrapper detected through fmt.Errorf %w chain",
			clf:     full,
			err:     fmt.Errorf("outer: %w", Permanent(errors.New("inner"))),
			errType: "irrelevant",
			want:    Verdict{DecisionPermanent, LayerWrapper},
		},
		{
			name:    "layer3 errors.Is sentinel rule beats config glob",
			clf:     full,
			err:     fmt.Errorf("wrapped: %w", sentinelErr),
			errType: "java.net.SocketException", // retryable glob, but code rule wins
			want:    Verdict{DecisionPermanent, LayerCodeClassifier},
		},
		{
			name:    "layer3 errors.As typed rule matches",
			clf:     full,
			err:     &typedErr{msg: "typed"},
			errType: "TIMEOUT", // retryable glob, but code rule wins
			want:    Verdict{DecisionPermanent, LayerCodeClassifier},
		},
		{
			name:    "layer4 glob retryable when no code layer matches",
			clf:     full,
			err:     errors.New("plain"),
			errType: "java.net.UnknownHostException",
			want:    Verdict{DecisionRetryable, LayerConfigGlob},
		},
		{
			name:    "layer4 glob permanent",
			clf:     full,
			err:     errors.New("plain"),
			errType: "com.acme.ValidationException",
			want:    Verdict{DecisionPermanent, LayerConfigGlob},
		},
		{
			name:    "layer4 exact-code glob (no wildcards) matches TIMEOUT",
			clf:     full,
			err:     errors.New("plain"),
			errType: "TIMEOUT",
			want:    Verdict{DecisionRetryable, LayerConfigGlob},
		},
		{
			name:    "layer5 default retryable when nothing matches",
			clf:     full,
			err:     errors.New("plain"),
			errType: "org.unknown.Thing",
			want:    Verdict{DecisionRetryable, LayerDefault},
		},
		{
			name:    "zero classifier collapses to default",
			clf:     Classifier{},
			err:     errors.New("plain"),
			errType: "anything",
			want:    Verdict{DecisionRetryable, LayerDefault},
		},
		{
			name:    "server-mode: nil err, globs-only, matches permanent",
			clf:     Classifier{Config: exampleClassification()},
			err:     nil,
			errType: "com.acme.ValidationException",
			want:    Verdict{DecisionPermanent, LayerConfigGlob},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.clf.Classify(tc.err, tc.errType)
			if got != tc.want {
				t.Fatalf("Classify(%v, %q) = %+v, want %+v", tc.err, tc.errType, got, tc.want)
			}
		})
	}
}

// TestGlobPrecedencePermanentWins pins the intra-layer-4 tie-break: a type that
// matches both a retryable and a permanent glob is classified permanent.
func TestGlobPrecedencePermanentWins(t *testing.T) {
	clf := Classifier{
		Config: &config.ClassificationConfig{
			Retryable: []string{"com.acme.*"},            // would match
			Permanent: []string{"*.ValidationException"}, // also matches — must win
		},
	}
	got := clf.Classify(errors.New("x"), "com.acme.ValidationException")
	want := Verdict{DecisionPermanent, LayerConfigGlob}
	if got != want {
		t.Fatalf("overlap glob = %+v, want %+v (permanent must win)", got, want)
	}
}

// TestClassifyWrapperOutermostWins proves that with nested wrappers the outer
// one — the most recently applied — decides, since errors.As returns the
// outermost match.
func TestClassifyWrapperOutermostWins(t *testing.T) {
	// Permanent wraps Retryable: outer is Permanent, so permanent wins.
	err := Permanent(Retryable(errors.New("cause")))
	got := Classifier{}.Classify(err, "")
	want := Verdict{DecisionPermanent, LayerWrapper}
	if got != want {
		t.Fatalf("nested wrapper = %+v, want %+v", got, want)
	}
}

// TestWrapperUnwrapsToCause guarantees the layer-2 wrappers stay transparent to
// errors.Is/As so lower-layer inspection and error.type derivation see the
// underlying cause, not the wrapper.
func TestWrapperUnwrapsToCause(t *testing.T) {
	if !errors.Is(Permanent(sentinelErr), sentinelErr) {
		t.Error("Permanent(sentinelErr) should errors.Is sentinelErr")
	}
	var te *typedErr
	if !errors.As(Retryable(&typedErr{msg: "y"}), &te) {
		t.Error("Retryable(*typedErr) should errors.As *typedErr")
	}
	if got := Permanent(errors.New("cause")).Error(); got != "cause" {
		t.Errorf("wrapper Error() = %q, want %q", got, "cause")
	}
}

// TestDecisionOutcome checks the projection onto the envelope Outcome enum.
func TestDecisionOutcome(t *testing.T) {
	if got := DecisionRetryable.Outcome(); got != envelope.OutcomeRetryableFailure {
		t.Errorf("DecisionRetryable.Outcome() = %v, want %v", got, envelope.OutcomeRetryableFailure)
	}
	if got := DecisionPermanent.Outcome(); got != envelope.OutcomePermanentFailure {
		t.Errorf("DecisionPermanent.Outcome() = %v, want %v", got, envelope.OutcomePermanentFailure)
	}
}

// TestErrorType covers the G6 error.type derivation: innermost unwrapped %T,
// seen through fmt wrappers and the layer-2 classification wrappers.
func TestErrorType(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"leaf typed", &typedErr{}, "*policy.typedErr"},
		{"fmt wrap sees through to leaf", fmt.Errorf("ctx: %w", &typedErr{}), "*policy.typedErr"},
		{"classification wrapper is transparent", Permanent(&typedErr{}), "*policy.typedErr"},
		{"errors.New leaf", errors.New("x"), "*errors.errorString"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ErrorType(tc.err); got != tc.want {
				t.Fatalf("ErrorType(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// TestGlob exercises the wildcard matcher directly, including the dotted-name
// spanning that distinguishes it from path.Match.
func TestGlob(t *testing.T) {
	tests := []struct {
		pattern, s string
		want       bool
	}{
		// exact (no wildcards)
		{"TIMEOUT", "TIMEOUT", true},
		{"TIMEOUT", "TIMEOUT2", false},
		{"TIMEOUT", "timeout", false}, // case-sensitive
		// trailing star spans dots
		{"java.net.*", "java.net.SocketTimeoutException", true},
		{"java.net.*", "java.net.ssl.SSLException", true}, // '*' crosses the dot
		{"java.net.*", "java.netFoo", false},              // dot is literal
		{"java.net.*", "java.io.IOException", false},
		// leading star
		{"*.ValidationException", "com.acme.ValidationException", true},
		{"*.ValidationException", "ValidationException", false}, // needs a leading segment + dot
		{"*.ValidationException", "com.acme.OtherException", false},
		// star matches empty
		{"foo*", "foo", true},
		{"*foo", "foo", true},
		{"*", "", true},
		{"*", "anything.at.all", true},
		{"", "", true},
		{"", "x", false},
		// question mark: exactly one char
		{"f?o", "foo", true},
		{"f?o", "fo", false},
		{"f?o", "fooo", false},
		{"?", ".", true}, // '?' matches a dot too
		// interior star
		{"a*z", "az", true},
		{"a*z", "abcz", true},
		{"a*z", "abc", false},
		// backtracking: star must give back characters
		{"*a*b", "xaxb", true},
		{"a*b*c", "abbbbc", true},
		{"a*b*c", "abbbb", false},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s~%s", tc.pattern, tc.s), func(t *testing.T) {
			if got := Glob(tc.pattern, tc.s); got != tc.want {
				t.Fatalf("Glob(%q, %q) = %v, want %v", tc.pattern, tc.s, got, tc.want)
			}
		})
	}
}

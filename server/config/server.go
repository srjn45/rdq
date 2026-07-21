// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"

	coreconfig "github.com/srjn45/rdq/core/config"
)

// secretRefEnvScheme is the only secret_ref indirection scheme in v1: a process
// environment variable (design 03 §3). It mirrors the scheme validated in
// core/config; here it is also RESOLVED (the env var actually dereferenced),
// which is a server-side concern outside the language-neutral core schema.
const secretRefEnvScheme = "env:"

// ServerConfig is the platform-operator-owned configuration that sits OUTSIDE
// per-queue config (design 03 §5): the auth token file, the global callback
// allowlist, and anything else "what the platform operator sets, not what a
// queue's owning team may set". A queue author cannot widen the callback
// allowlist — that is the SSRF boundary (PRD FR-25/§12).
type ServerConfig struct {
	// TokensPath is the path to the static bearer-token file (design 04 §5,
	// server/auth). Empty leaves the /v1 auth boundary open (dev/embedded mode).
	TokensPath string `yaml:"tokens_path,omitempty" json:"tokens_path,omitempty"`

	// CallbackAllowlist is the set of permitted callback base URLs (design 03
	// §5). A queue's callback URL is delivered only if it matches an entry here.
	// Empty means deny-by-default: NO callback URL is permitted, so a callback
	// queue cannot ship until an operator opts its target in.
	CallbackAllowlist []string `yaml:"callback_allowlist,omitempty" json:"callback_allowlist,omitempty"`
}

// Allowlist is the parsed, ready-to-match form of CallbackAllowlist.
type Allowlist struct {
	entries []allowEntry
}

// allowEntry is one parsed allowlist rule. A callback URL matches when its
// scheme (if the entry pins one), host, and path-prefix (if the entry pins one)
// all agree. Host match is exact and case-insensitive — there is deliberately
// no subdomain wildcard, so `payments.internal` never authorizes
// `evil.payments.internal` (SSRF hygiene: deny by default).
type allowEntry struct {
	scheme string // "" = any scheme
	host   string // lower-cased host[:port]; always required
	prefix string // path prefix; "" = any path
	raw    string // original entry text, for error messages
}

// ParseAllowlist parses a CallbackAllowlist into matchable entries. Each entry
// is either a full base URL (scheme://host[:port][/path-prefix]) or a bare
// host[:port] (matching any scheme/path). An entry with no host is an error so a
// malformed allowlist fails fast at load.
func ParseAllowlist(entries []string) (*Allowlist, error) {
	al := &Allowlist{}
	for _, raw := range entries {
		e := strings.TrimSpace(raw)
		if e == "" {
			return nil, fmt.Errorf("server config: empty callback_allowlist entry")
		}
		var ae allowEntry
		ae.raw = raw
		if strings.Contains(e, "://") {
			u, err := url.Parse(e)
			if err != nil {
				return nil, fmt.Errorf("server config: callback_allowlist entry %q is not a valid URL: %w", raw, err)
			}
			if u.Host == "" {
				return nil, fmt.Errorf("server config: callback_allowlist entry %q has no host", raw)
			}
			ae.scheme = strings.ToLower(u.Scheme)
			ae.host = strings.ToLower(u.Host)
			ae.prefix = strings.TrimSuffix(u.Path, "/") // "/" alone means "any path"
		} else {
			// Bare host[:port]; url.Parse treats it as a path, so read Host from a
			// synthetic authority to normalize host:port handling uniformly.
			u, err := url.Parse("//" + e)
			if err != nil || u.Host == "" {
				return nil, fmt.Errorf("server config: callback_allowlist entry %q has no host", raw)
			}
			ae.host = strings.ToLower(u.Host)
		}
		al.entries = append(al.entries, ae)
	}
	return al, nil
}

// Allows reports whether rawURL is permitted by the allowlist. An unparseable or
// host-less URL is never allowed. An empty allowlist allows nothing.
func (a *Allowlist) Allows(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return false
	}
	scheme := strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Host)
	for _, e := range a.entries {
		if e.scheme != "" && e.scheme != scheme {
			continue
		}
		if e.host != host {
			continue
		}
		if e.prefix != "" && !pathHasPrefix(u.Path, e.prefix) {
			continue
		}
		return true
	}
	return false
}

// pathHasPrefix reports whether path lies under prefix as a path (segment
// boundary aware): "/hooks" covers "/hooks" and "/hooks/x" but not "/hooksXY".
func pathHasPrefix(path, prefix string) bool {
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	rest := path[len(prefix):]
	return rest == "" || rest[0] == '/'
}

// Allowlist parses the raw CallbackAllowlist. It is a convenience for callers
// that want to reuse the matcher across many URLs.
func (sc *ServerConfig) Allowlist() (*Allowlist, error) {
	return ParseAllowlist(sc.CallbackAllowlist)
}

// ValidateCallbacks enforces, at config-load time, that every configured queue
// with a callback (1) targets a URL on the global allowlist and (2) has a
// secret_ref that resolves to a present environment variable. This is the T5.6
// guarantee that a callback URL off the allowlist is REJECTED AT CONFIG LOAD —
// before any claim loop can dispatch to it. env is the environment lookup
// (nil ⇒ os.LookupEnv); it is a parameter so tests need not mutate the process
// environment. Queues are checked in sorted order for a deterministic first
// error.
func (sc *ServerConfig) ValidateCallbacks(queues map[string]*coreconfig.QueueConfig, env func(string) (string, bool)) error {
	al, err := sc.Allowlist()
	if err != nil {
		return err
	}
	names := make([]string, 0, len(queues))
	for name := range queues {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		qc := queues[name]
		if qc == nil || qc.Callback == nil || qc.Callback.URL == nil {
			continue
		}
		cb := qc.Callback
		if !al.Allows(*cb.URL) {
			return fmt.Errorf("server config: queue %q callback url %q is not on the callback allowlist (SSRF)", name, *cb.URL)
		}
		if cb.Auth != nil && cb.Auth.SecretRef != nil {
			if _, err := ResolveSecretRef(*cb.Auth.SecretRef, env); err != nil {
				return fmt.Errorf("server config: queue %q: %w", name, err)
			}
		}
	}
	return nil
}

// ResolveSecretRef dereferences a secret_ref to its raw secret bytes (design 03
// §3). v1 supports only the env: scheme: `env:PAYMENTS_CB_TOKEN` resolves to the
// value of that environment variable. It fails fast on a non-env scheme, an
// empty variable name, an unset variable, or an empty value — so a callback is
// never dispatched with a missing credential. env is the lookup (nil ⇒
// os.LookupEnv). The returned bytes feed callback.Target.Secret; the caller owns
// them and this package retains no reference.
func ResolveSecretRef(ref string, env func(string) (string, bool)) ([]byte, error) {
	if env == nil {
		env = os.LookupEnv
	}
	name, ok := strings.CutPrefix(ref, secretRefEnvScheme)
	if !ok {
		return nil, fmt.Errorf("secret_ref %q must use the %s scheme (v1 supports env: only)", ref, secretRefEnvScheme)
	}
	if name == "" {
		return nil, fmt.Errorf("secret_ref %q names no environment variable", ref)
	}
	val, ok := env(name)
	if !ok {
		return nil, fmt.Errorf("secret_ref %q: environment variable %s is not set", ref, name)
	}
	if val == "" {
		return nil, fmt.Errorf("secret_ref %q: environment variable %s is empty", ref, name)
	}
	return []byte(val), nil
}

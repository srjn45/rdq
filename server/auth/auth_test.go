// SPDX-License-Identifier: Apache-2.0

package auth

import "testing"

// TestParseRoleOrdering: roles parse and compare submitter < operator < admin,
// so a >= check models "at least this role".
func TestParseRoleOrdering(t *testing.T) {
	sub, _ := ParseRole("submitter")
	op, _ := ParseRole("operator")
	adm, _ := ParseRole("admin")
	if !(sub < op && op < adm) {
		t.Fatalf("role ordering wrong: submitter=%d operator=%d admin=%d", sub, op, adm)
	}
	if _, err := ParseRole("superuser"); err == nil {
		t.Error("ParseRole(superuser) should error on unknown role")
	}
}

// TestPrincipalAllowsRoleMatrix: a grant satisfies a check only when its role is
// high enough AND its queue glob matches — the core of the role matrix.
func TestPrincipalAllowsRoleMatrix(t *testing.T) {
	p := &Principal{Name: "ops", Grants: []Grant{
		{Queue: "payments.*", Role: RoleOperator},
		{Queue: "billing", Role: RoleSubmitter},
	}}

	cases := []struct {
		queue string
		need  Role
		want  bool
	}{
		{"payments.charge", RoleSubmitter, true}, // operator subsumes submitter
		{"payments.charge", RoleOperator, true},
		{"payments.charge", RoleAdmin, false}, // operator is not admin
		{"billing", RoleSubmitter, true},      // exact-match grant
		{"billing", RoleOperator, false},      // submitter cannot operate
		{"orders", RoleSubmitter, false},      // no grant covers this queue
		{"paymentsX", RoleSubmitter, false},   // glob boundary: payments.* ≠ paymentsX
	}
	for _, tc := range cases {
		if got := p.Allows(tc.queue, tc.need); got != tc.want {
			t.Errorf("Allows(%q, %v) = %v, want %v", tc.queue, tc.need, got, tc.want)
		}
	}
}

// TestPrincipalAllowsGlobal: a cross-queue op needs a catch-all ("*") grant;
// admin scoped to a glob is not a platform-wide admin.
func TestPrincipalAllowsGlobal(t *testing.T) {
	scoped := &Principal{Grants: []Grant{{Queue: "payments.*", Role: RoleAdmin}}}
	if scoped.AllowsGlobal(RoleAdmin) {
		t.Error("admin on payments.* must NOT satisfy a global admin op")
	}
	global := &Principal{Grants: []Grant{{Queue: "*", Role: RoleAdmin}}}
	if !global.AllowsGlobal(RoleAdmin) {
		t.Error("admin on * must satisfy a global admin op")
	}
	subGlobal := &Principal{Grants: []Grant{{Queue: "*", Role: RoleSubmitter}}}
	if subGlobal.AllowsGlobal(RoleAdmin) {
		t.Error("submitter on * must NOT satisfy a global admin op")
	}
}

// TestBearerToken parses (and rejects) Authorization header values.
func TestBearerToken(t *testing.T) {
	cases := []struct {
		hdr    string
		want   string
		wantOK bool
	}{
		{"Bearer tok_abc", "tok_abc", true},
		{"bearer tok_abc", "tok_abc", true},  // scheme is case-insensitive
		{"BEARER   spaced ", "spaced", true}, // trimmed
		{"", "", false},                      // missing
		{"Basic dXNlcjpwdw==", "", false},    // wrong scheme
		{"Bearer ", "", false},               // empty token
		{"Bearer", "", false},                // no token at all
	}
	for _, tc := range cases {
		got, ok := BearerToken(tc.hdr)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("BearerToken(%q) = (%q, %v), want (%q, %v)", tc.hdr, got, ok, tc.want, tc.wantOK)
		}
	}
}

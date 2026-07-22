// SPDX-License-Identifier: Apache-2.0

package http

import (
	"os"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// specPath is the normative OpenAPI artifact, relative to this package.
const specPath = "../openapi.yaml"

// loadSpec parses the committed spec, failing the test if it is not well-formed
// YAML — the in-repo, CI-enforced stand-in for an external spec linter.
func loadSpec(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("read %s: %v", specPath, err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("%s is not well-formed YAML: %v", specPath, err)
	}
	return doc
}

// TestSpecIsWellFormedOpenAPI checks the document has the structural spine every
// OpenAPI 3.x document requires.
func TestSpecIsWellFormedOpenAPI(t *testing.T) {
	doc := loadSpec(t)

	version, _ := doc["openapi"].(string)
	if !strings.HasPrefix(version, "3.") {
		t.Errorf("openapi version = %q, want 3.x", version)
	}

	info, ok := doc["info"].(map[string]any)
	if !ok {
		t.Fatal("missing info object")
	}
	if info["title"] == nil || info["version"] == nil {
		t.Errorf("info missing title/version: %+v", info)
	}

	paths, ok := doc["paths"].(map[string]any)
	if !ok || len(paths) == 0 {
		t.Fatal("missing or empty paths")
	}
	for _, want := range []string{"/healthz", "/readyz"} {
		if _, ok := paths[want].(map[string]any); !ok {
			t.Errorf("spec missing health path %s", want)
		}
	}
}

// TestSpecEveryOperationHasResponses lints that no operation is missing its
// responses object — the most common way a hand-edited spec goes invalid.
func TestSpecEveryOperationHasResponses(t *testing.T) {
	doc := loadSpec(t)
	paths := doc["paths"].(map[string]any)
	methods := map[string]bool{"get": true, "put": true, "post": true, "delete": true, "patch": true}

	for path, item := range paths {
		ops, ok := item.(map[string]any)
		if !ok {
			t.Errorf("path %s is not an object", path)
			continue
		}
		for method, op := range ops {
			if !methods[method] {
				continue
			}
			opMap, ok := op.(map[string]any)
			if !ok {
				t.Errorf("%s %s is not an operation object", method, path)
				continue
			}
			if resp, ok := opMap["responses"].(map[string]any); !ok || len(resp) == 0 {
				t.Errorf("%s %s has no responses", method, path)
			}
		}
	}
}

// TestSpecProblemCodesMatchGoConstants is the anti-drift check: the spec's
// ProblemCode enum MUST be exactly the set of stable codes the code emits. Add a
// code in one place without the other and this fails.
func TestSpecProblemCodesMatchGoConstants(t *testing.T) {
	doc := loadSpec(t)

	components, ok := doc["components"].(map[string]any)
	if !ok {
		t.Fatal("missing components")
	}
	schemas, ok := components["schemas"].(map[string]any)
	if !ok {
		t.Fatal("missing components.schemas")
	}
	pc, ok := schemas["ProblemCode"].(map[string]any)
	if !ok {
		t.Fatal("missing components.schemas.ProblemCode")
	}
	enumRaw, ok := pc["enum"].([]any)
	if !ok {
		t.Fatal("ProblemCode has no enum")
	}

	specCodes := make([]string, 0, len(enumRaw))
	for _, v := range enumRaw {
		specCodes = append(specCodes, v.(string))
	}
	sort.Strings(specCodes)

	goCodes := make([]string, 0, len(problemDefs))
	for code := range problemDefs {
		goCodes = append(goCodes, string(code))
	}
	sort.Strings(goCodes)

	if strings.Join(specCodes, ",") != strings.Join(goCodes, ",") {
		t.Errorf("ProblemCode enum drift:\n  spec: %v\n  code: %v", specCodes, goCodes)
	}
}

// TestSpecErrorsUseProblemJSON ensures documented error responses carry the
// problem+json media type, so the spec matches the wire contract.
func TestSpecErrorsUseProblemJSON(t *testing.T) {
	doc := loadSpec(t)
	components := doc["components"].(map[string]any)
	responses, ok := components["responses"].(map[string]any)
	if !ok {
		t.Fatal("missing components.responses")
	}
	for name, r := range responses {
		rm := r.(map[string]any)
		content, ok := rm["content"].(map[string]any)
		if !ok {
			t.Errorf("response %s has no content", name)
			continue
		}
		if _, ok := content["application/problem+json"]; !ok {
			t.Errorf("response %s does not use application/problem+json", name)
		}
	}
}

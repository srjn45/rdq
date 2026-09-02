// SPDX-License-Identifier: Apache-2.0

package callback

import (
	"os"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// contractSpecPath is the callback-contract OpenAPI artifact, relative to this
// package. It is the normative contract for the endpoint a receiver implements
// (design 04 §4); these tests are the in-repo, CI-enforced anti-drift check that
// the spec keeps matching the dispatcher in this package.
const contractSpecPath = "../callback-contract.openapi.yaml"

// loadContractSpec parses the committed callback-contract spec, failing the test
// if it is not well-formed YAML.
func loadContractSpec(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(contractSpecPath)
	if err != nil {
		t.Fatalf("read %s: %v", contractSpecPath, err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("%s is not well-formed YAML: %v", contractSpecPath, err)
	}
	return doc
}

// callbackOperation returns the single POST operation the contract defines,
// resolving the representative path without hard-coding it (so renaming the
// illustrative route does not break the tests).
func callbackOperation(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	paths, ok := doc["paths"].(map[string]any)
	if !ok || len(paths) == 0 {
		t.Fatal("missing or empty paths")
	}
	for path, item := range paths {
		ops, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("path %s is not an object", path)
		}
		if post, ok := ops["post"].(map[string]any); ok {
			return post
		}
	}
	t.Fatal("contract defines no POST operation")
	return nil
}

// TestCallbackSpecIsWellFormedOpenAPI checks the document has the structural
// spine every OpenAPI 3.x document requires, and that the callback operation
// carries a responses object.
func TestCallbackSpecIsWellFormedOpenAPI(t *testing.T) {
	doc := loadContractSpec(t)

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

	op := callbackOperation(t, doc)
	if resp, ok := op["responses"].(map[string]any); !ok || len(resp) == 0 {
		t.Error("callback operation has no responses")
	}
}

// TestCallbackSpecHeadersMatchGoConstants is the anti-drift check: the X-RDQ-*
// header parameters the spec documents MUST be exactly the set of header
// constants the dispatcher sets (setHeaders in http.go / SignatureHeader in
// sign.go). Add, rename, or drop an X-RDQ-* header in one place without the other
// and this fails — the callback analogue of TestSpecProblemCodesMatchGoConstants.
func TestCallbackSpecHeadersMatchGoConstants(t *testing.T) {
	doc := loadContractSpec(t)

	components, ok := doc["components"].(map[string]any)
	if !ok {
		t.Fatal("missing components")
	}
	params, ok := components["parameters"].(map[string]any)
	if !ok {
		t.Fatal("missing components.parameters")
	}

	var specHeaders []string
	for name, p := range params {
		pm, ok := p.(map[string]any)
		if !ok {
			t.Errorf("parameter %s is not an object", name)
			continue
		}
		if pm["in"] != "header" {
			continue
		}
		hn, _ := pm["name"].(string)
		if strings.HasPrefix(hn, "X-RDQ-") {
			specHeaders = append(specHeaders, hn)
		}
	}
	sort.Strings(specHeaders)

	goHeaders := []string{HeaderTaskID, HeaderQueue, HeaderHandlerRef, HeaderAttempt, SignatureHeader}
	sort.Strings(goHeaders)

	if strings.Join(specHeaders, ",") != strings.Join(goHeaders, ",") {
		t.Errorf("X-RDQ-* header drift between contract spec and Go constants:\n  spec: %v\n  code: %v",
			specHeaders, goHeaders)
	}
}

// TestCallbackSpecDocumentsOutcomeClasses ensures the response-code contract that
// the dispatcher classifies (2xx ack, 4xx permanent, 5xx retryable — classifyStatus)
// stays documented for receiver implementers. These are the status classes a
// generated stub must be able to return.
func TestCallbackSpecDocumentsOutcomeClasses(t *testing.T) {
	doc := loadContractSpec(t)
	op := callbackOperation(t, doc)
	responses, ok := op["responses"].(map[string]any)
	if !ok {
		t.Fatal("callback operation has no responses")
	}

	// classifyStatus resolves every delivery into success (2xx), permanent (4xx),
	// or retryable (5xx); the contract must document one matcher for each class.
	for _, want := range []string{"2XX", "4XX", "5XX"} {
		if _, ok := responses[want]; !ok {
			t.Errorf("contract does not document a %q response class", want)
		}
	}
}

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func writeRequest(t *testing.T, path string, sourceRoot string, output string) {
	t.Helper()
	request := requestEnvelope{
		Schema:        requestSchema,
		Target:        "go",
		SourceRoot:    sourceRoot,
		Output:        output,
		ContractScope: "rust-authority",
	}
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRequestBindsSourceAndOutput(t *testing.T) {
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	outputRoot := filepath.Join(root, "output")
	if err := os.MkdirAll(sourceRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	requestPath := filepath.Join(root, "request.json")
	writeRequest(t, requestPath, sourceRoot, outputRoot)
	if err := validateRequest(requestPath, sourceRoot, outputRoot); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
}

func TestValidateRequestRejectsOutputDrift(t *testing.T) {
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	outputRoot := filepath.Join(root, "output")
	if err := os.MkdirAll(sourceRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	requestPath := filepath.Join(root, "request.json")
	writeRequest(t, requestPath, sourceRoot, filepath.Join(root, "other-output"))
	if err := validateRequest(requestPath, sourceRoot, outputRoot); err == nil {
		t.Fatal("output drift was accepted")
	}
}

func TestValidateRequestRejectsMissingOutput(t *testing.T) {
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	outputRoot := filepath.Join(root, "output")
	if err := os.MkdirAll(sourceRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	requestPath := filepath.Join(root, "request.json")
	data := []byte(`{"schema":"` + requestSchema + `","target":"go","source_root":"` + sourceRoot + `","contract_scope":"rust-authority"}`)
	if err := os.WriteFile(requestPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := validateRequest(requestPath, sourceRoot, outputRoot); err == nil {
		t.Fatal("missing output was accepted")
	}
}

func TestCopyModuleFilesIncludesRustGeneratedFacade(t *testing.T) {
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	outputRoot := filepath.Join(root, "output")
	if err := os.MkdirAll(filepath.Join(sourceRoot, "go"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"go.mod", "go.sum", "README.md", "client.go"} {
		if err := os.WriteFile(filepath.Join(sourceRoot, "go", name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"LICENSE", "NOTICE"} {
		if err := os.WriteFile(filepath.Join(sourceRoot, name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := copyModuleFiles(sourceRoot, outputRoot); err != nil {
		t.Fatalf("copy module files: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(outputRoot, "client.go"))
	if err != nil {
		t.Fatalf("generated facade was not staged: %v", err)
	}
	if string(got) != "client.go" {
		t.Fatalf("generated facade contents = %q", got)
	}
}

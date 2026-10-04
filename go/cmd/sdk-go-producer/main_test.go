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

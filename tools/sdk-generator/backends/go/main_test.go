package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The helper exercises process invocation and staging, not protoc correctness.
func TestMain(m *testing.M) {
	if len(os.Args) == 2 && os.Args[1] == "--version" && os.Getenv("SDK_TOOL_TEST_VERSION") != "" {
		fmt.Println("test-tool " + os.Getenv("SDK_TOOL_TEST_VERSION"))
		os.Exit(0)
	}
	if os.Getenv("SDK_TOOL_TEST_GENERATE") == "1" {
		if err := stagingTool(os.Args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func fixture(t *testing.T) (string, string, string, string) {
	t.Helper()
	root := t.TempDir()
	source, authority := filepath.Join(root, "source"), filepath.Join(root, "authority")
	for _, dir := range []string{source, authority} {
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	request, output := filepath.Join(root, "request.json"), filepath.Join(root, "output")
	writeJSON(t, request, requestEnvelope{Schema: requestSchema, Target: "go", SourceRoot: source, Output: output})
	return source, authority, request, output
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestOutputAdmission(t *testing.T) {
	source, authority, request, output := fixture(t)
	if _, err := newOutputPath(source, authority, request, output); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{source, authority, request, filepath.Dir(source), filepath.Join(source, "generated"), filepath.Join(authority, "generated")} {
		if _, err := newOutputPath(source, authority, request, bad); err == nil {
			t.Errorf("admitted overlapping output %s", bad)
		}
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("admission mutated output: %v", err)
	}
	if err := os.Mkdir(output, 0o755); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(output, "consumer-data")
	if err := os.WriteFile(sentinel, []byte("preserve"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := newOutputPath(source, authority, request, output); err == nil {
		t.Fatal("admitted existing output")
	}
	if b, err := os.ReadFile(sentinel); err != nil || string(b) != "preserve" {
		t.Fatalf("existing data changed: %q %v", b, err)
	}
}

func TestInputAndToolFailuresDoNotCreateOutput(t *testing.T) {
	for _, validInput := range []bool{false, true} {
		t.Run(map[bool]string{false: "input", true: "tool"}[validInput], func(t *testing.T) {
			source, authority, request, output := fixture(t)
			payload := []byte("syntax = \"proto3\";")
			if err := os.WriteFile(filepath.Join(authority, "example.proto"), payload, 0o644); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(payload)
			digest := "wrong"
			if validInput {
				digest = hex.EncodeToString(sum[:])
			}
			writeJSON(t, filepath.Join(authority, "rust-authority.json"), authorityManifest{Schema: "acyclic.sdk.rust-authority.v1", Authority: "rust", SourceRevision: "test-only", Families: []family{{Source: "example.proto", SourceSHA256: digest}}})
			missingTool := filepath.Join(t.TempDir(), "missing-tool")
			err := run(source, authority, request, output, missingTool, missingTool, missingTool, "", "test-version", "test-version", "test-version")
			if err == nil {
				t.Fatal("accepted invalid input or unavailable tool")
			}
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Fatalf("failure created output: %v", err)
			}
		})
	}
}

func TestToolAdmission(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(working, executable)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("SDK_TOOL_TEST_VERSION", "1.20")
	if _, _, _, err := resolveTools(relative, relative, relative, "", "1.20", "1.20"); err == nil {
		t.Fatal("admitted missing version pin")
	}
	if _, _, _, err := resolveTools(relative, relative, relative, "1.2", "1.20", "1.20"); err == nil {
		t.Fatal("admitted prefix version match")
	}
	tool, _, _, err := resolveTools(relative, relative, relative, "1.20", "1.20", "1.20")
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(tool) {
		t.Fatalf("relative tool escaped resolution: %s", tool)
	}
	command := exec.Command(tool, "--version")
	command.Dir = t.TempDir()
	if output, err := command.Output(); err != nil || strings.TrimSpace(string(output)) != "test-tool 1.20" {
		t.Fatalf("tool broke after changing working directory: %s %v", output, err)
	}
}

func TestMissingFamilyBinding(t *testing.T) {
	root := t.TempDir()
	gen := filepath.Join(root, "gen")
	if err := os.Mkdir(gen, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gen, "a.pb.go"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := requireBindings(root, []string{"a.proto"}); err != nil {
		t.Fatal(err)
	}
	if err := requireBindings(root, []string{"a.proto", "b.proto"}); err == nil {
		t.Fatal("accepted missing family despite unrelated output")
	}
}

func TestDuplicateFamilyInput(t *testing.T) {
	source, authority, request, output := fixture(t)
	payload := []byte("syntax = \"proto3\";")
	if err := os.WriteFile(filepath.Join(authority, "a.proto"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	f := family{Source: "a.proto", SourceSHA256: hex.EncodeToString(sum[:])}
	writeJSON(t, filepath.Join(authority, "rust-authority.json"), authorityManifest{Schema: "acyclic.sdk.rust-authority.v1", Authority: "rust", SourceRevision: "test-only", Families: []family{f, f}})
	err := run(source, authority, request, output, "missing", "missing", "missing", "", "", "", "")
	if err == nil || !strings.Contains(err.Error(), "duplicate authority source") {
		t.Fatalf("wrong failure: %v", err)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("duplicate input created output: %v", err)
	}
}

func TestRejectEscapedInput(t *testing.T) {
	_, authority, request, _ := fixture(t)
	for _, path := range []string{"../request.json", "sub/../request.json", request, ".", "..", ""} {
		if err := validateInput(authority, path, "unused"); err == nil {
			t.Errorf("admitted %q", path)
		}
	}

}

func TestRequestAndOutputAliases(t *testing.T) {
	source, authority, request, output := fixture(t)
	alias := filepath.Join(t.TempDir(), "parent-alias")
	if err := os.Symlink(filepath.Dir(source), alias); err != nil {
		t.Skipf("symlink privilege unavailable: %v", err)
	}
	writeJSON(t, request, requestEnvelope{Schema: requestSchema, Target: "go", SourceRoot: filepath.Join(alias, "source"), Output: filepath.Join(alias, "output")})
	if err := validateRequest(request, source, output); err != nil {
		t.Fatal(err)
	}
	resolved, err := newOutputPath(source, authority, request, filepath.Join(alias, "output"))
	if err != nil || !samePath(resolved, output) {
		t.Fatalf("output alias did not resolve: %s %v", resolved, err)
	}
	if _, err := newOutputPath(source, authority, request, filepath.Join(alias, "source", "generated")); err == nil {
		t.Fatal("admitted aliased source output")
	}
}

func TestRejectSymlinkEscapedInput(t *testing.T) {
	_, authority, request, _ := fixture(t)
	link := filepath.Join(authority, "alias.proto")
	if err := os.Symlink(request, link); err != nil {
		t.Skipf("symlink privilege unavailable: %v", err)
	}
	if err := validateInput(authority, "alias.proto", "unused"); err == nil {
		t.Fatal("admitted symlink escaping authority")
	}
}

func TestUnattestedOptionsDoNotCreateOutput(t *testing.T) {
	source, authority, request, output := fixture(t)
	payload := []byte("syntax = \"proto3\";")
	if err := os.WriteFile(filepath.Join(authority, "example.proto"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	writeJSON(t, filepath.Join(authority, "rust-authority.json"), authorityManifest{Schema: "acyclic.sdk.rust-authority.v1", Authority: "rust", SourceRevision: "test-only", Families: []family{{Source: "example.proto", SourceSHA256: hex.EncodeToString(sum[:])}}})
	options := filepath.Join(authority, "validation", "v1")
	if err := os.MkdirAll(options, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(options, "options.proto"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	missingTool := filepath.Join(t.TempDir(), "missing-tool")
	err := run(source, authority, request, output, missingTool, missingTool, missingTool, "", "", "", "")
	if err == nil || !strings.Contains(err.Error(), "must be a digested authority input") {
		t.Fatalf("wrong failure: %v", err)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("unattested input created output: %v", err)
	}
}

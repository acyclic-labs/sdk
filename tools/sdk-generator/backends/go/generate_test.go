package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// Simulate protoc's output solely to exercise the producer's successful path.
// Fail if the command can see any source the test did not explicitly approve.
func stagingTool(args []string) error {
	var input, output, grpcOutput string
	var sources []string
	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--proto_path="):
			input = strings.TrimPrefix(arg, "--proto_path=")
		case strings.HasPrefix(arg, "--go_out=paths=source_relative:"):
			output = strings.TrimPrefix(arg, "--go_out=paths=source_relative:")
		case strings.HasPrefix(arg, "--go-grpc_out=paths=source_relative:"):
			grpcOutput = strings.TrimPrefix(arg, "--go-grpc_out=paths=source_relative:")
		case !strings.HasPrefix(arg, "--"):
			sources = append(sources, arg)
		}
	}
	if input == "" || output == "" || output != grpcOutput {
		return fmt.Errorf("invalid generation paths: %v", args)
	}
	working, err := os.Getwd()
	if err != nil || !samePath(working, input) {
		return fmt.Errorf("generation working directory is not isolated: %s %v", working, err)
	}
	var files []string
	err = filepath.WalkDir(input, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		relative, err := filepath.Rel(input, path)
		files = append(files, filepath.ToSlash(relative))
		return err
	})
	if err != nil {
		return err
	}
	sort.Strings(files)
	sorted := append([]string(nil), sources...)
	sort.Strings(sorted)
	if !reflect.DeepEqual(files, sorted) {
		return fmt.Errorf("generation can see unapproved files: %v (sources %v)", files, sources)
	}
	for _, source := range sources {
		path := filepath.Join(output, filepath.FromSlash(strings.TrimSuffix(source, ".proto")+".pb.go"))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, []byte("// staging test output\npackage example\n"), 0o644); err != nil {
			return err
		}
	}
	b, err := json.Marshal(args)
	if err != nil {
		return err
	}
	return os.WriteFile(os.Getenv("SDK_TOOL_TEST_ARGS"), b, 0o644)
}

func writeInput(t *testing.T, root, name, payload string) family {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(payload))
	return family{Source: name, SourceSHA256: hex.EncodeToString(sum[:])}
}

func TestSuccessfulStaging(t *testing.T) {
	source, authority, request, output := fixture(t)
	families := []family{
		writeInput(t, authority, "example/v1/example.proto", "syntax = \"proto3\";"),
		writeInput(t, authority, "validation/v1/options.proto", "syntax = \"proto2\";"),
	}
	writeInput(t, authority, "hidden.proto", "unapproved import")
	manifestPath := filepath.Join(authority, "rust-authority.json")
	writeJSON(t, manifestPath, authorityManifest{Schema: "acyclic.sdk.rust-authority.v1", Authority: "rust", SourceRevision: "test-only", Families: families})
	moduleFiles := map[string]string{
		"go/go.mod": "module staging.example\n\ngo 1.27.0\n",
		"go/go.sum": "", "go/README.md": "staging fixture",
		"LICENSE": "fixture license", "NOTICE": "fixture notice",
	}
	for name, content := range moduleFiles {
		writeInput(t, source, name, content)
	}
	writeInput(t, source, "go/client.go", "legacy facade must not be copied")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	argsPath := filepath.Join(t.TempDir(), "args.json")
	t.Setenv("SDK_TOOL_TEST_VERSION", "1.20")
	t.Setenv("SDK_TOOL_TEST_GENERATE", "1")
	t.Setenv("SDK_TOOL_TEST_ARGS", argsPath)
	if err := run(source, authority, request, output, executable, executable, executable, runtime.Version(), "1.20", "1.20", "1.20"); err != nil {
		t.Fatal(err)
	}
	for name, want := range moduleFiles {
		got, err := os.ReadFile(filepath.Join(output, filepath.Base(name)))
		if err != nil || string(got) != want {
			t.Fatalf("module file %s: %q %v", name, got, err)
		}
	}
	b, err := os.ReadFile(filepath.Join(output, "generation-receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got receipt
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	manifestBytes, _ := os.ReadFile(manifestPath)
	manifestSum := sha256.Sum256(manifestBytes)
	toolBytes, _ := os.ReadFile(executable)
	toolSum := sha256.Sum256(toolBytes)
	if got.Schema != "acyclic.sdk.go-producer-receipt.v1" || got.SourceRevision != "test-only" || got.AuthoritySHA256 != hex.EncodeToString(manifestSum[:]) || got.GoVersion != runtime.Version() {
		t.Fatalf("incorrect receipt identity: %+v", got)
	}
	if len(got.Outputs) != 7 || len(got.OutputSHA256) != 7 || len(got.ToolSHA256) != 3 || len(got.Inputs) != 2 {
		t.Fatalf("incorrect receipt inventory: %+v", got)
	}
	if _, err := os.Stat(filepath.Join(output, "client.go")); !os.IsNotExist(err) {
		t.Fatalf("legacy facade copied: %v", err)
	}
	for _, name := range got.Outputs {
		b, err := os.ReadFile(filepath.Join(output, filepath.FromSlash(name)))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		if got.OutputSHA256[name] != hex.EncodeToString(sum[:]) {
			t.Errorf("incorrect output digest for %s", name)
		}
	}
	for name, digest := range got.ToolSHA256 {
		if digest != hex.EncodeToString(toolSum[:]) {
			t.Errorf("incorrect tool digest for %s", name)
		}
	}
	b, err = os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	var args []string
	if err := json.Unmarshal(b, &args); err != nil {
		t.Fatal(err)
	}
	wantFlags := []string{
		"--plugin=protoc-gen-go=" + executable,
		"--plugin=protoc-gen-go-grpc=" + executable,
		"--go_opt=Mvalidation/v1/options.proto=github.com/acyclic-labs/sdk/go/gen/validation/v1",
		"--go-grpc_opt=Mvalidation/v1/options.proto=github.com/acyclic-labs/sdk/go/gen/validation/v1",
	}
	for _, want := range wantFlags {
		if !contains(args, want) {
			t.Errorf("missing generation argument: %s", want)
		}
	}
	for _, arg := range args {
		if strings.HasPrefix(arg, "--proto_path=") {
			path := strings.TrimPrefix(arg, "--proto_path=")
			if samePath(path, authority) {
				t.Fatal("protoc used unrestricted authority directory")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("temporary inputs retained after success: %v", err)
			}
		}
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestStagedSourcesAreAttestedSnapshots(t *testing.T) {
	_, authority, _, _ := fixture(t)
	approved := writeInput(t, authority, "approved.proto", "approved bytes")
	writeInput(t, authority, "hidden.proto", "hidden bytes")
	root, err := stageSources(authority, []family{approved})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	writeInput(t, authority, "approved.proto", "changed after staging")
	b, err := os.ReadFile(filepath.Join(root, "approved.proto"))
	if err != nil || string(b) != "approved bytes" {
		t.Fatalf("staged bytes changed: %q %v", b, err)
	}
	if _, err := os.Stat(filepath.Join(root, "hidden.proto")); !os.IsNotExist(err) {
		t.Fatalf("unapproved import available: %v", err)
	}
	if _, err := stageSources(authority, []family{approved}); err == nil {
		t.Fatal("staged changed bytes against the original digest")
	}
}

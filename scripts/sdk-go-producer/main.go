package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

const requestSchema = "acyclic.sdk.generation.request.v1"

type requestEnvelope struct {
	Schema        string `json:"schema"`
	Target        string `json:"target"`
	SourceRoot    string `json:"source_root"`
	Output        string `json:"output"`
	ContractScope string `json:"contract_scope"`
}
type authorityManifest struct {
	Schema         string   `json:"schema"`
	Authority      string   `json:"authority"`
	SourceRevision string   `json:"source_revision"`
	Families       []family `json:"families"`
}
type family struct {
	Source           string `json:"source"`
	SourceSHA256     string `json:"source_sha256"`
	Descriptor       string `json:"descriptor"`
	DescriptorSHA256 string `json:"descriptor_sha256"`
}
type receipt struct {
	Schema          string            `json:"schema"`
	Target          string            `json:"target"`
	SourceRevision  string            `json:"source_revision"`
	Authority       string            `json:"authority"`
	AuthoritySHA256 string            `json:"authority_manifest_sha256"`
	GoVersion       string            `json:"go_version"`
	ProtocVersion   string            `json:"protoc_version"`
	ProtocGenGo     string            `json:"protoc_gen_go_version"`
	ProtocGenGoGRPC string            `json:"protoc_gen_go_grpc_version"`
	Inputs          []string          `json:"inputs"`
	Outputs         []string          `json:"outputs"`
	OutputSHA256    map[string]string `json:"output_sha256"`
	ToolSHA256      map[string]string `json:"tool_sha256"`
	GeneratedAt     string            `json:"generated_at_utc"`
}

func main() {
	sourceRoot := flag.String("source-root", "", "source checkout root")
	authorityRoot := flag.String("authority", "", "staged Rust authority directory or manifest")
	requestPath := flag.String("request", "", "generation request JSON")
	outputRoot := flag.String("output", "", "isolated package output")
	protoc := flag.String("protoc", "protoc", "pinned protoc executable")
	genGo := flag.String("protoc-gen-go", "protoc-gen-go", "pinned Go protobuf plugin")
	genGRPC := flag.String("protoc-gen-go-grpc", "protoc-gen-go-grpc", "pinned Go gRPC plugin")
	wantGo := flag.String("go-version", "", "required Go version")
	wantProtoc := flag.String("protoc-version", "", "required protoc version")
	wantGenGo := flag.String("protoc-gen-go-version", "", "required protoc-gen-go version")
	wantGenGRPC := flag.String("protoc-gen-go-grpc-version", "", "required protoc-gen-go-grpc version")
	flag.Parse()
	if err := run(*sourceRoot, *authorityRoot, *requestPath, *outputRoot, *protoc, *genGo, *genGRPC, *wantGo, *wantProtoc, *wantGenGo, *wantGenGRPC); err != nil {
		fmt.Fprintln(os.Stderr, "sdk-go-producer:", err)
		os.Exit(1)
	}
}

func run(sourceRoot, authorityRoot, requestPath, outputRoot, protoc, genGo, genGRPC, wantGo, wantProtoc, wantGenGo, wantGenGRPC string) error {
	if sourceRoot == "" || authorityRoot == "" || requestPath == "" || outputRoot == "" {
		return errors.New("--source-root, --authority, --request and --output are required")
	}
	var err error
	sourceRoot, err = filepath.Abs(sourceRoot)
	if err != nil {
		return err
	}
	authorityRoot, err = filepath.Abs(authorityRoot)
	if err != nil {
		return err
	}
	requestPath, err = filepath.Abs(requestPath)
	if err != nil {
		return err
	}
	outputRoot, err = filepath.Abs(outputRoot)
	if err != nil {
		return err
	}
	if err := validateRequest(requestPath, sourceRoot, outputRoot); err != nil {
		return err
	}
	manifestPath := authorityRoot
	if st, statErr := os.Stat(manifestPath); statErr == nil && !st.IsDir() {
		authorityRoot = filepath.Dir(authorityRoot)
	} else {
		manifestPath = filepath.Join(authorityRoot, "rust-authority.json")
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("read authority manifest: %w", err)
	}
	var manifest authorityManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return fmt.Errorf("decode authority manifest: %w", err)
	}
	if manifest.Schema != "acyclic.sdk.rust-authority.v1" || manifest.Authority != "rust" || manifest.SourceRevision == "" {
		return errors.New("authority manifest is not a Rust authority export")
	}
	if len(manifest.Families) == 0 {
		return errors.New("authority manifest has no families")
	}
	sort.Slice(manifest.Families, func(i, j int) bool { return manifest.Families[i].Source < manifest.Families[j].Source })
	outputRoot, err = newOutputPath(sourceRoot, authorityRoot, requestPath, outputRoot)
	if err != nil {
		return err
	}
	if wantGo != "" && runtime.Version() != wantGo {
		return fmt.Errorf("Go runtime %s does not match pinned %s", runtime.Version(), wantGo)
	}
	familySources := make([]string, 0, len(manifest.Families))
	seenSources := map[string]bool{}
	for _, f := range manifest.Families {
		if err := validateInput(authorityRoot, f.Source, f.SourceSHA256); err != nil {
			return err
		}
		resolved, err := filepath.EvalSymlinks(filepath.Join(authorityRoot, f.Source))
		if err != nil {
			return err
		}
		key := pathKey(resolved)
		if seenSources[key] {
			return fmt.Errorf("duplicate authority source: %s", f.Source)
		}
		seenSources[key] = true
		if f.Descriptor != "" {
			if err := validateInput(authorityRoot, f.Descriptor, f.DescriptorSHA256); err != nil {
				return err
			}
		}
		familySources = append(familySources, filepath.ToSlash(f.Source))
	}
	sources := append([]string(nil), familySources...)
	validation := filepath.Join(authorityRoot, "validation", "v1", "options.proto")
	hasValidation := false
	if _, err := os.Stat(validation); err == nil {
		attested := false
		for _, source := range familySources {
			attested = attested || source == "validation/v1/options.proto"
		}
		if !attested {
			return errors.New("validation/v1/options.proto must be a digested authority input")
		}
		hasValidation = true
	}
	sort.Strings(sources)
	protoc, genGo, genGRPC, err = resolveTools(protoc, genGo, genGRPC, wantProtoc, wantGenGo, wantGenGRPC)
	if err != nil {
		return err
	}
	toolHashes := map[string]string{}
	for name, path := range map[string]string{"protoc": protoc, "protoc-gen-go": genGo, "protoc-gen-go-grpc": genGRPC} {
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		toolHashes[name] = hex.EncodeToString(sum[:])
	}
	// Use a fresh destination. Never erase an earlier package or caller data.
	if err := os.Mkdir(outputRoot, 0o755); err != nil {
		return fmt.Errorf("create new output: %w", err)
	}
	if err := os.Mkdir(filepath.Join(outputRoot, "gen"), 0o755); err != nil {
		return err
	}
	args := []string{"--proto_path=" + authorityRoot, "--plugin=protoc-gen-go=" + genGo, "--plugin=protoc-gen-go-grpc=" + genGRPC, "--go_out=paths=source_relative:" + filepath.Join(outputRoot, "gen"), "--go-grpc_out=paths=source_relative:" + filepath.Join(outputRoot, "gen")}
	if hasValidation {
		args = append(args, "--go_opt=Mvalidation/v1/options.proto=github.com/acyclic-labs/sdk/go/gen/validation/v1")
		args = append(args, "--go-grpc_opt=Mvalidation/v1/options.proto=github.com/acyclic-labs/sdk/go/gen/validation/v1")
	}
	args = append(args, familySources...)
	command := exec.Command(protoc, args...)
	command.Dir = sourceRoot
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("protoc generation failed: %w\n%s", err, strings.TrimSpace(string(output)))
	}
	if err := requireBindings(outputRoot, sources); err != nil {
		return err
	}
	if err := copyModuleFiles(sourceRoot, outputRoot); err != nil {
		return err
	}
	outputs, hashes, err := collectOutputs(outputRoot)
	if err != nil {
		return err
	}
	manifestSum := sha256.Sum256(manifestBytes)
	r := receipt{Schema: "acyclic.sdk.go-producer-receipt.v1", Target: "go", SourceRevision: manifest.SourceRevision, Authority: "rust", AuthoritySHA256: hex.EncodeToString(manifestSum[:]), GoVersion: runtime.Version(), ProtocVersion: wantProtoc, ProtocGenGo: wantGenGo, ProtocGenGoGRPC: wantGenGRPC, Inputs: sources, Outputs: outputs, OutputSHA256: hashes, ToolSHA256: toolHashes, GeneratedAt: time.Now().UTC().Format(time.RFC3339)}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(filepath.Join(outputRoot, "generation-receipt.json"), b, 0o644)
}

func validateRequest(path, sourceRoot, outputRoot string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read request: %w", err)
	}
	var r requestEnvelope
	if err := json.Unmarshal(b, &r); err != nil {
		return fmt.Errorf("decode request: %w", err)
	}
	if r.Schema != requestSchema {
		return fmt.Errorf("unexpected request schema %q", r.Schema)
	}
	if r.Target != "" && r.Target != "go" && r.Target != "sdk-language-producer" {
		return fmt.Errorf("request target %q is not Go", r.Target)
	}
	if r.ContractScope != "" && r.ContractScope != "rust-authority" {
		return fmt.Errorf("request contract scope %q is not Rust authority", r.ContractScope)
	}
	if r.SourceRoot != "" {
		expected, err := resolvedPath(r.SourceRoot)
		if err != nil {
			return err
		}
		actual, err := resolvedPath(sourceRoot)
		if err != nil {
			return err
		}
		if !samePath(expected, actual) {
			return fmt.Errorf("request source root %q does not match %q", r.SourceRoot, sourceRoot)
		}
	}
	if r.Output == "" {
		return errors.New("request output is required")
	}
	expectedOutput, err := resolvedPath(r.Output)
	if err != nil {
		return fmt.Errorf("resolve request output: %w", err)
	}
	actualOutput, err := resolvedPath(outputRoot)
	if err != nil {
		return err
	}
	if !samePath(expectedOutput, actualOutput) {
		return fmt.Errorf("request output %q does not match %q", r.Output, outputRoot)
	}
	return nil
}

func resolveTools(protoc, genGo, genGRPC, wantProtoc, wantGenGo, wantGenGRPC string) (string, string, string, error) {
	checks := []struct{ name, path, want string }{{"protoc", protoc, wantProtoc}, {"protoc-gen-go", genGo, wantGenGo}, {"protoc-gen-go-grpc", genGRPC, wantGenGRPC}}
	resolved := make([]string, len(checks))
	for i, c := range checks {
		if c.want == "" {
			return "", "", "", fmt.Errorf("expected version is required for %s", c.name)
		}
		path := c.path
		if found, err := exec.LookPath(c.path); err == nil {
			path = found
		} else {
			if _, statErr := os.Stat(c.path); statErr != nil {
				return "", "", "", fmt.Errorf("%s executable %q not found: %w", c.name, c.path, err)
			}
		}
		path, err := filepath.Abs(path)
		if err != nil {
			return "", "", "", err
		}
		out, err := exec.Command(path, "--version").CombinedOutput()
		if err != nil {
			return "", "", "", fmt.Errorf("%s --version: %w", c.name, err)
		}
		reported := strings.TrimSpace(string(out))
		if reported != c.want && !strings.HasSuffix(reported, " "+c.want) {
			return "", "", "", fmt.Errorf("%s reports %q, expected %q", c.name, strings.TrimSpace(string(out)), c.want)
		}
		resolved[i] = path
	}
	return resolved[0], resolved[1], resolved[2], nil
}

func validateInput(root, relative, want string) error {
	clean := filepath.ToSlash(filepath.Clean(relative))
	if relative == "" || filepath.IsAbs(relative) || filepath.VolumeName(relative) != "" || clean != filepath.ToSlash(relative) || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("unsafe authority input %q", relative)
	}
	path := filepath.Join(root, filepath.FromSlash(relative))
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("resolve authority input %s: %w", relative, err)
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	if !within(resolvedRoot, resolved) {
		return fmt.Errorf("authority input escapes root: %s", relative)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read authority input %s: %w", relative, err)
	}
	if want == "" {
		return fmt.Errorf("authority input %s has no digest", relative)
	}
	got := sha256.Sum256(b)
	if !strings.EqualFold(hex.EncodeToString(got[:]), want) {
		return fmt.Errorf("authority input %s digest drift: got %s want %s", relative, hex.EncodeToString(got[:]), want)
	}
	return nil
}

func copyModuleFiles(sourceRoot, outputRoot string) error {
	if err := os.MkdirAll(outputRoot, 0o755); err != nil {
		return fmt.Errorf("create module output: %w", err)
	}
	moduleRoot := filepath.Join(sourceRoot, "go")
	// client.go is a Rust-owned generated facade. Keep it inside the staged
	// module so consumers receive seamless transport defaults with bindings.
	for _, name := range []string{"go.mod", "go.sum", "README.md", "client.go"} {
		b, err := os.ReadFile(filepath.Join(moduleRoot, name))
		if err != nil {
			return fmt.Errorf("read Go module file %s: %w", name, err)
		}
		if err := os.WriteFile(filepath.Join(outputRoot, name), b, 0o644); err != nil {
			return err
		}
	}
	for _, name := range []string{"LICENSE", "NOTICE"} {
		b, err := os.ReadFile(filepath.Join(sourceRoot, name))
		if err != nil {
			return fmt.Errorf("read package license %s: %w", name, err)
		}
		if err := os.WriteFile(filepath.Join(outputRoot, name), b, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func collectOutputs(root string) ([]string, map[string]string, error) {
	var paths []string
	hashes := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Base(path) == "generation-receipt.json" {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		paths = append(paths, rel)
		hashes[rel] = hex.EncodeToString(sum[:])
		return nil
	})
	sort.Strings(paths)
	return paths, hashes, err
}

// newOutputPath requires an existing parent and an absent output leaf. Resolve
// aliases before checking overlap; os.Mkdir later rejects a raced existing leaf.
// This producer never removes an existing output, even after a failed run.
func newOutputPath(source, authority, request, output string) (string, error) {
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("output must not already exist: %s", output)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(output))
	if err != nil {
		return "", fmt.Errorf("output parent must exist: %w", err)
	}
	output = filepath.Join(parent, filepath.Base(output))
	for _, input := range []string{source, authority, request} {
		resolved, err := filepath.EvalSymlinks(input)
		if err != nil {
			return "", fmt.Errorf("resolve protected input: %w", err)
		}
		if within(resolved, output) || within(output, resolved) {
			return "", fmt.Errorf("output overlaps protected input: %s", input)
		}
	}
	return output, nil
}

func within(root, path string) bool {
	if samePath(root, path) {
		return true
	}
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}

func samePath(a, b string) bool {
	return pathKey(a) == pathKey(b)
}

func resolvedPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err == nil {
		return resolved, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(abs)), nil
}

func pathKey(path string) string {
	if runtime.GOOS == "windows" {
		return strings.ToLower(filepath.Clean(path))
	}
	return filepath.Clean(path)
}

func requireBindings(output string, sources []string) error {
	for _, source := range sources {
		binding := filepath.Join(output, "gen", filepath.FromSlash(strings.TrimSuffix(source, filepath.Ext(source))+".pb.go"))
		info, err := os.Lstat(binding)
		if err != nil {
			return fmt.Errorf("missing generated binding for %s: %w", source, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("generated binding is not a regular file: %s", binding)
		}
	}
	return nil
}

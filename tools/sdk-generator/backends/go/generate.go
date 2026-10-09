package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

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

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Give protoc only approved source bytes. Imports omitted from the manifest
// cannot be resolved from other files in the authority export.
func stageSources(authority string, families []family) (string, error) {
	root, err := os.MkdirTemp("", "sdk-go-authority-")
	if err != nil {
		return "", err
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.RemoveAll(root)
		}
	}()
	for _, f := range families {
		b, err := readInput(authority, f.Source, f.SourceSHA256)
		if err != nil {
			return "", err
		}
		path := filepath.Join(root, filepath.FromSlash(f.Source))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return "", err
		}
		if err := os.WriteFile(path, b, 0o644); err != nil {
			return "", err
		}
	}
	complete = true
	return root, nil
}

func copyModuleFiles(sourceRoot, outputRoot string) error {
	if err := os.MkdirAll(outputRoot, 0o755); err != nil {
		return fmt.Errorf("create module output: %w", err)
	}
	moduleRoot := filepath.Join(sourceRoot, "go")
	// Stage package metadata only. A legacy facade must not silently become the
	// public interface of a newly generated package.
	for _, name := range []string{"go.mod", "go.sum", "README.md"} {
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

// Require an existing parent and an absent leaf, resolving aliases before the
// overlap checks. The later Mkdir rejects a raced existing leaf. Never remove
// an existing output, even after failed generation.
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

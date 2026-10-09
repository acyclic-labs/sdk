package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

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

func validateInput(root, relative, want string) error {
	_, err := readInput(root, relative, want)
	return err
}

// Return the exact bytes that passed admission, without rereading after hashing.
func readInput(root, relative, want string) ([]byte, error) {
	clean := filepath.ToSlash(filepath.Clean(relative))
	if relative == "" || filepath.IsAbs(relative) || filepath.VolumeName(relative) != "" || clean != filepath.ToSlash(relative) || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return nil, fmt.Errorf("unsafe authority input %q", relative)
	}
	path := filepath.Join(root, filepath.FromSlash(relative))
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, fmt.Errorf("resolve authority input %s: %w", relative, err)
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	if !within(resolvedRoot, resolved) {
		return nil, fmt.Errorf("authority input escapes root: %s", relative)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read authority input %s: %w", relative, err)
	}
	if want == "" {
		return nil, fmt.Errorf("authority input %s has no digest", relative)
	}
	got := sha256.Sum256(b)
	if !strings.EqualFold(hex.EncodeToString(got[:]), want) {
		return nil, fmt.Errorf("authority input %s digest drift: got %s want %s", relative, hex.EncodeToString(got[:]), want)
	}
	return b, nil
}

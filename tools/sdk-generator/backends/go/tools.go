package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

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

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Optional real-compiler control; normal staging CI needs no tool downloads.
func TestProtocRejectsUnattestedImports(t *testing.T) {
	protoc := os.Getenv("SDK_TEST_PROTOC")
	if protoc == "" {
		t.Skip("set SDK_TEST_PROTOC and SDK_TEST_PROTOC_VERSION for real-compiler import checks")
	}
	version := os.Getenv("SDK_TEST_PROTOC_VERSION")
	if version == "" {
		t.Fatal("SDK_TEST_PROTOC_VERSION is required")
	}
	protoc, err := filepath.Abs(protoc)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(protoc, "--version").CombinedOutput(); err != nil || strings.TrimSpace(string(out)) != version {
		t.Fatalf("protoc identity: %s %v", out, err)
	}
	_, authority, _, _ := fixture(t)
	root := writeInput(t, authority, "root.proto", `syntax = "proto3";
import "hidden.proto";
message Root { Hidden value = 1; }
`)
	dependency := writeInput(t, authority, "hidden.proto", `syntax = "proto3";
message Hidden { string value = 1; }
`)
	for _, approved := range []bool{false, true} {
		t.Run(map[bool]string{false: "unattested", true: "attested"}[approved], func(t *testing.T) {
			families := []family{root}
			if approved {
				families = append(families, dependency)
			}
			inputs, err := stageSources(authority, families)
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(inputs)
			command := exec.Command(protoc, "--proto_path="+inputs, "--descriptor_set_out="+filepath.Join(t.TempDir(), "output.bin"), "root.proto")
			command.Dir = inputs
			out, err := command.CombinedOutput()
			if approved && err != nil {
				t.Fatalf("attested import rejected: %s %v", out, err)
			}
			if !approved && (err == nil || !strings.Contains(string(out), "hidden.proto: File not found")) {
				t.Fatalf("unattested import not rejected as missing: %s %v", out, err)
			}
		})
	}
}

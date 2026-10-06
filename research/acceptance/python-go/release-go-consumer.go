// Descriptor-driven Rust-authority Go runtime qualification.
//
// Canonical mode consumes the exact ordered request plan emitted by Rust. It
// never synthesizes requests or maintains a second RPC inventory.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/acyclic-labs/sdk/go/gen/actors/v1"
	_ "github.com/acyclic-labs/sdk/go/gen/filesystem/v2"
	_ "github.com/acyclic-labs/sdk/go/gen/harness/v2"
	_ "github.com/acyclic-labs/sdk/go/gen/inference/v1"
	_ "github.com/acyclic-labs/sdk/go/gen/machines/v1"
	_ "github.com/acyclic-labs/sdk/go/gen/objects/v2"
	_ "github.com/acyclic-labs/sdk/go/gen/protocol/v1"
	_ "github.com/acyclic-labs/sdk/go/gen/stream/v2"
	_ "github.com/acyclic-labs/sdk/go/gen/workers/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

type authority struct {
	SourceRevision   string            `json:"source_revision"`
	SourceGitSHA     string            `json:"source_git_sha"`
	SourceFileHashes map[string]string `json:"source_file_hashes"`
	Families         []struct {
		Source     string `json:"source"`
		RPCMethods []struct {
			RPC   string `json:"rpc"`
			Shape string `json:"shape"`
		} `json:"rpc_methods"`
	} `json:"families"`
}
type inventoryItem struct{ family, rpc, shape string }
type requestRecord struct {
	RequestBase64 string `json:"request_base64"`
	BytesBase64   string `json:"bytes_base64"`
}
type expectedFrame struct {
	ResponseSHA256 string `json:"response_sha256"`
}
type expectedOutcome struct {
	GRPCCode string `json:"grpc_code"`
}
type planRecord struct {
	ExecutionStep   int             `json:"execution_step"`
	Family          string          `json:"family"`
	RPC             string          `json:"rpc"`
	RequestBase64   string          `json:"request_base64"`
	RequestFrames   []requestRecord `json:"request_frames"`
	ExpectedOutcome expectedOutcome `json:"expected_outcome"`
	ResponseFrames  []expectedFrame `json:"response_frames"`
}
type typedManifest struct {
	Complete           bool         `json:"complete"`
	SourceRevision     string       `json:"source_revision"`
	AuthoritySHA256    string       `json:"authority_sha256"`
	ExecutionPlanCount int          `json:"execution_plan_count"`
	ExecutionPlan      []planRecord `json:"execution_plan"`
}
type frame struct {
	Sequence int    `json:"sequence"`
	Type     string `json:"type"`
	Bytes    string `json:"bytes_base64"`
	SHA256   string `json:"sha256"`
}
type scenario struct {
	Schema         string  `json:"schema"`
	SourceRevision string  `json:"source_revision"`
	Family         string  `json:"family"`
	RPC            string  `json:"rpc"`
	Shape          string  `json:"shape"`
	Invoked        bool    `json:"invoked"`
	ExecutionMode  string  `json:"execution_mode"`
	Transport      string  `json:"transport"`
	Status         string  `json:"status"`
	ExitCode       int     `json:"exit_code"`
	RequestFrames  []frame `json:"request_frames"`
	ResponseFrames []frame `json:"response_frames"`
	Terminal       any     `json:"terminal"`
	ResponseCount  int     `json:"response_count"`
	ElapsedMillis  float64 `json:"elapsed_ms"`
	Error          string  `json:"error,omitempty"`
}

type rawProtoCodec struct{ raw [][]byte }

func (c *rawProtoCodec) Name() string                  { return "proto" }
func (c *rawProtoCodec) Marshal(v any) ([]byte, error) { return proto.Marshal(v.(proto.Message)) }
func (c *rawProtoCodec) Unmarshal(b []byte, v any) error {
	c.raw = append(c.raw, append([]byte(nil), b...))
	return proto.Unmarshal(b, v.(proto.Message))
}

func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func frameFor(message protoreflect.ProtoMessage, sequence int) frame {
	bytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	return frame{sequence, string(message.ProtoReflect().Descriptor().FullName()), base64.StdEncoding.EncodeToString(bytes), digest(bytes)}
}
func rawFrame(message protoreflect.ProtoMessage, sequence int, bytes []byte) frame {
	return frame{sequence, string(message.ProtoReflect().Descriptor().FullName()), base64.StdEncoding.EncodeToString(bytes), digest(bytes)}
}
func uniqueRPCs(items []inventoryItem) int {
	seen := map[string]bool{}
	for _, item := range items {
		seen[item.rpc] = true
	}
	return len(seen)
}

func loadAuthority(path string) (authority, []inventoryItem, error) {
	var root authority
	bytes, err := os.ReadFile(path)
	if err != nil {
		return root, nil, err
	}
	if err := json.Unmarshal(bytes, &root); err != nil {
		return root, nil, err
	}
	var inventory []inventoryItem
	for _, family := range root.Families {
		name := strings.SplitN(family.Source, "/", 2)[0]
		for _, method := range family.RPCMethods {
			inventory = append(inventory, inventoryItem{name, method.RPC, method.Shape})
		}
	}
	if len(inventory) != 106 || uniqueRPCs(inventory) != len(inventory) {
		return root, nil, fmt.Errorf("Rust authority must contain 106 unique RPCs, got %d", len(inventory))
	}
	return root, inventory, nil
}
func loadTypedManifest(path string) (typedManifest, error) {
	var manifest typedManifest
	bytes, err := os.ReadFile(path)
	if err != nil {
		return manifest, err
	}
	if err := json.Unmarshal(bytes, &manifest); err != nil {
		return manifest, err
	}
	if !manifest.Complete || len(manifest.ExecutionPlan) == 0 || manifest.ExecutionPlanCount != len(manifest.ExecutionPlan) {
		return manifest, errors.New("Rust typed manifest must contain a complete ordered execution_plan")
	}
	if manifest.SourceRevision == "" {
		return manifest, errors.New("Rust typed manifest source_revision is required")
	}
	for index, record := range manifest.ExecutionPlan {
		if record.ExecutionStep != index || record.RPC == "" {
			return manifest, fmt.Errorf("invalid Rust execution step %d", index)
		}
	}
	return manifest, nil
}
func validateManifestBinding(manifest typedManifest, root authority) error {
	if manifest.SourceRevision != root.SourceGitSHA {
		return errors.New("Rust typed manifest source_revision does not match authority source_git_sha")
	}
	if !strings.HasPrefix(manifest.AuthoritySHA256, "sha256:") {
		return errors.New("Rust typed manifest authority_sha256 is required")
	}
	return nil
}
func typedRequests(record planRecord, descriptor protoreflect.MessageDescriptor) ([]protoreflect.ProtoMessage, []frame, error) {
	frames := record.RequestFrames
	if len(frames) == 0 {
		frames = []requestRecord{{RequestBase64: record.RequestBase64}}
	}
	requests := make([]protoreflect.ProtoMessage, 0, len(frames))
	requestFrames := make([]frame, 0, len(frames))
	for index, item := range frames {
		encoded := item.RequestBase64
		if encoded == "" {
			encoded = item.BytesBase64
		}
		bytes, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, nil, err
		}
		message := dynamicpb.NewMessage(descriptor)
		if err := proto.Unmarshal(bytes, message); err != nil {
			return nil, nil, err
		}
		requests = append(requests, message)
		requestFrames = append(requestFrames, rawFrame(message, index, bytes))
	}
	return requests, requestFrames, nil
}
func treeHash(root string) (string, map[string]string, error) {
	files := map[string]string{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		bytes, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(relative)] = digest(bytes)
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	keys := make([]string, 0, len(files))
	for key := range files {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var canonical strings.Builder
	for _, key := range keys {
		canonical.WriteString(key)
		canonical.WriteByte(0)
		canonical.WriteString(files[key])
		canonical.WriteByte('\n')
	}
	return digest([]byte(canonical.String())), files, nil
}
func canonicalCode(err error) string {
	if err == nil {
		return "OK"
	}
	name := strings.ToUpper(status.Code(err).String())
	return strings.NewReplacer("INVALIDARGUMENT", "INVALID_ARGUMENT", "FAILEDPRECONDITION", "FAILED_PRECONDITION", "DEADLINEEXCEEDED", "DEADLINE_EXCEEDED").Replace(name)
}

func main() {
	authorityPath := flag.String("authority", "", "Rust authority manifest")
	manifestPath := flag.String("manifest", "", "Rust typed-request manifest")
	endpoint := flag.String("endpoint", "127.0.0.1:18081", "gRPC endpoint")
	out := flag.String("out", "scenario-log.json", "scenario log path")
	revision := flag.String("source-revision", "", "source revision override")
	packageRoot := flag.String("package-root", "", "installed package root")
	executable := flag.String("executable", "", "qualifier executable path")
	flag.Parse()
	root, inventory, err := loadAuthority(*authorityPath)
	if err != nil {
		panic(err)
	}
	var manifest typedManifest
	canonical := *manifestPath != ""
	if canonical {
		manifest, err = loadTypedManifest(*manifestPath)
		if err != nil {
			panic(err)
		}
		if err := validateManifestBinding(manifest, root); err != nil {
			panic(err)
		}
		inventory = make([]inventoryItem, 0, len(manifest.ExecutionPlan))
		for _, record := range manifest.ExecutionPlan {
			inventory = append(inventory, inventoryItem{record.Family, record.RPC, ""})
		}
	}
	if *revision == "" {
		if canonical {
			*revision = manifest.SourceRevision
		} else {
			*revision = root.SourceRevision
		}
	}
	codec := &rawProtoCodec{}
	connectContext, connectCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer connectCancel()
	conn, err := grpc.DialContext(connectContext, *endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(codec)))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	results := make([]scenario, 0, len(inventory))
	for index, item := range inventory {
		parts := strings.SplitN(item.rpc, "/", 2)
		descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(parts[0]))
		if err != nil {
			panic(err)
		}
		service := descriptor.(protoreflect.ServiceDescriptor)
		method := service.Methods().ByName(protoreflect.Name(parts[1]))
		codec.raw = nil
		var requests []protoreflect.ProtoMessage
		var requestFrames []frame
		if canonical {
			requests, requestFrames, err = typedRequests(manifest.ExecutionPlan[index], method.Input())
			if err != nil {
				panic(err)
			}
		} else {
			request := dynamicpb.NewMessage(method.Input())
			requests = []protoreflect.ProtoMessage{request}
			requestFrames = []frame{frameFor(request, 0)}
		}
		started := time.Now()
		responses := make([]frame, 0)
		actualCode, callError := "OK", ""
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if method.IsStreamingClient() || method.IsStreamingServer() {
			stream, streamErr := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: method.IsStreamingServer(), ClientStreams: method.IsStreamingClient()}, "/"+item.rpc)
			if streamErr == nil {
				for _, request := range requests {
					if streamErr = stream.SendMsg(request); streamErr != nil {
						break
					}
				}
				if streamErr == nil {
					streamErr = stream.CloseSend()
				}
			}
			if streamErr == nil {
				for sequence := 0; ; sequence++ {
					response := dynamicpb.NewMessage(method.Output())
					streamErr = stream.RecvMsg(response)
					if streamErr != nil {
						break
					}
					if sequence < len(codec.raw) {
						responses = append(responses, rawFrame(response, sequence, codec.raw[sequence]))
					} else {
						responses = append(responses, frameFor(response, sequence))
					}
					if !method.IsStreamingServer() {
						break
					}
				}
			}
			if streamErr != nil && !errors.Is(streamErr, io.EOF) {
				actualCode, callError = canonicalCode(streamErr), streamErr.Error()
			}
		} else {
			response := dynamicpb.NewMessage(method.Output())
			invokeErr := conn.Invoke(ctx, "/"+item.rpc, requests[0], response)
			if invokeErr != nil {
				actualCode, callError = canonicalCode(invokeErr), invokeErr.Error()
			} else if len(codec.raw) > 0 {
				responses = append(responses, rawFrame(response, 0, codec.raw[0]))
			} else {
				responses = append(responses, frameFor(response, 0))
			}
		}
		cancel()
		expectedCode := "OK"
		if canonical {
			expectedCode = manifest.ExecutionPlan[index].ExpectedOutcome.GRPCCode
		}
		statusValue := "passed"
		if actualCode != expectedCode {
			statusValue = "failed"
		}
		result := scenario{"acyclic.sdk.rpc-scenario-result.v2", *revision, item.family, item.rpc, item.shape, true, "remote", "grpc", statusValue, 0, requestFrames, responses, map[string]string{"code": actualCode, "details": callError}, len(responses), float64(time.Since(started).Microseconds()) / 1000, callError}
		if statusValue == "failed" {
			result.ExitCode = 1
		}
		results = append(results, result)
	}
	metadata := map[string]any{}
	if canonical {
		authorityBytes, _ := os.ReadFile(*authorityPath)
		modelDigest := root.SourceRevision
		if !strings.HasPrefix(modelDigest, "sha256:") {
			modelDigest = "sha256:" + modelDigest
		}
		authorityID := map[string]any{"source_git_sha": root.SourceGitSHA, "model_digest": modelDigest, "source_file_hashes": root.SourceFileHashes}
		metadata["rust_authority_manifest_sha256"] = digest(authorityBytes)
		metadata["authority"] = authorityID
		if *packageRoot != "" {
			tree, files, treeErr := treeHash(*packageRoot)
			if treeErr != nil {
				panic(treeErr)
			}
			metadata["executed_package"] = map[string]any{"language": "go", "source_git_sha": root.SourceGitSHA, "model_digest": modelDigest, "source_file_hashes": root.SourceFileHashes, "package_tree_sha256": tree, "package_source_file_hashes": files, "artifact_sha256": tree}
		}
		if *executable != "" {
			bytes, readErr := os.ReadFile(*executable)
			if readErr != nil {
				panic(readErr)
			}
			metadata["executable_sha256"] = digest(bytes)
		}
	}
	payload := map[string]any{"schema": "acyclic.sdk.rpc-scenario-log.v2", "consumer": "go-rust-authority-release", "source_revision": *revision, "execution_mode": "remote", "scenarios": results, "observations": results}
	for key, value := range metadata {
		payload[key] = value
	}
	bytes, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		panic(err)
	}
	if dir := filepath.Dir(*out); dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			panic(err)
		}
	}
	if err := os.WriteFile(*out, append(bytes, '\n'), 0644); err != nil {
		panic(err)
	}
}

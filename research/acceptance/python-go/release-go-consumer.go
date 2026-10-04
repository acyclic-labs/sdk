// Descriptor-driven Rust-authority Go runtime qualification.
//
// Importing every generated package registers the Rust descriptors. The
// consumer then discovers all methods from the authority manifest, invokes
// them over gRPC, and records exact request/response bytes for the central
// Rust semantic verifier.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

type authority struct {
	SourceRevision string `json:"source_revision"`
	Families []struct { Source string `json:"source"`; RPCMethods []struct { RPC string `json:"rpc"`; Shape string `json:"shape"` } `json:"rpc_methods"` } `json:"families"`
}
type inventoryItem struct { family, rpc, shape string }
type frame struct { Sequence int `json:"sequence"`; Type string `json:"type"`; Bytes string `json:"bytes_base64"`; SHA256 string `json:"sha256"` }
type scenario struct {
	Schema string `json:"schema"`; SourceRevision string `json:"source_revision"`; Family string `json:"family"`; RPC string `json:"rpc"`; Shape string `json:"shape"`
	Invoked bool `json:"invoked"`; ExecutionMode string `json:"execution_mode"`; Transport string `json:"transport"`; Status string `json:"status"`; ExitCode int `json:"exit_code"`
	RequestFrames []frame `json:"request_frames"`; ResponseFrames []frame `json:"response_frames"`; Terminal any `json:"terminal"`; ResponseCount int `json:"response_count"`; ElapsedMillis float64 `json:"elapsed_ms"`; Error string `json:"error,omitempty"`
}

func frameFor(message protoreflect.ProtoMessage, sequence int) frame {
	bytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	sum := sha256.Sum256(bytes)
	return frame{sequence, string(message.ProtoReflect().Descriptor().FullName()), base64.StdEncoding.EncodeToString(bytes), "sha256:" + hex.EncodeToString(sum[:])}
}

func scalar(field protoreflect.FieldDescriptor, seed int) protoreflect.Value {
	switch field.Kind() {
	case protoreflect.BoolKind: return protoreflect.ValueOfBool(true)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind: return protoreflect.ValueOfInt32(int32(seed))
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind: return protoreflect.ValueOfInt64(int64(seed))
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind: return protoreflect.ValueOfUint32(uint32(seed + 1))
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind: return protoreflect.ValueOfUint64(uint64(1<<63 + seed))
	case protoreflect.FloatKind: return protoreflect.ValueOfFloat32(1.25)
	case protoreflect.DoubleKind: return protoreflect.ValueOfFloat64(2.5)
	case protoreflect.StringKind: return protoreflect.ValueOfString("fixture-" + string(field.Name()))
	case protoreflect.BytesKind: return protoreflect.ValueOfBytes([]byte{byte(seed % 251), byte(seed % 251), byte(seed % 251), byte(seed % 251)})
	case protoreflect.EnumKind:
		values := field.Enum().Values(); number := values.Get(0).Number(); if values.Len() > 1 { number = values.Get(1).Number() }; return protoreflect.ValueOfEnum(number)
	default: panic("unsupported protobuf scalar kind")
	}
}

func fill(message protoreflect.Message, depth, seed int) {
	if depth > 3 { return }
	fields := message.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i); if field.IsMap() { continue }; if field.ContainingOneof() != nil && i > 0 { continue }
		valueSeed := seed + i + 1
		if field.Cardinality() == protoreflect.Repeated {
			list := message.Mutable(field).List()
			if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind { child := dynamicpb.NewMessage(field.Message()); fill(child, depth+1, valueSeed); list.Append(protoreflect.ValueOfMessage(child)) } else { list.Append(scalar(field, valueSeed)) }
		} else if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
			child := dynamicpb.NewMessage(field.Message()); fill(child, depth+1, valueSeed); message.Set(field, protoreflect.ValueOfMessage(child))
		} else { message.Set(field, scalar(field, valueSeed)) }
	}
}

func loadAuthority(path string) (authority, []inventoryItem, error) {
	var root authority; bytes, err := os.ReadFile(path); if err != nil { return root, nil, err }; if err := json.Unmarshal(bytes, &root); err != nil { return root, nil, err }
	var inventory []inventoryItem
	for _, family := range root.Families { name := strings.SplitN(family.Source, "/", 2)[0]; for _, method := range family.RPCMethods { inventory = append(inventory, inventoryItem{name, method.RPC, method.Shape}) } }
	if len(inventory) != 106 { return root, nil, fmt.Errorf("Rust authority contains %d RPCs; expected 106", len(inventory)) }
	return root, inventory, nil
}

func main() {
	authorityPath := flag.String("authority", "", "Rust authority manifest"); endpoint := flag.String("endpoint", "127.0.0.1:18081", "gRPC endpoint"); out := flag.String("out", "scenario-log.json", "scenario log path"); revision := flag.String("source-revision", "", "source revision override"); flag.Parse()
	root, inventory, err := loadAuthority(*authorityPath); if err != nil { panic(err) }; if *revision == "" { *revision = root.SourceRevision }
	connectContext, connectCancel := context.WithTimeout(context.Background(), 15*time.Second); defer connectCancel(); conn, err := grpc.DialContext(connectContext, *endpoint, grpc.WithInsecure(), grpc.WithBlock()); if err != nil { panic(err) }; defer conn.Close()
	results := make([]scenario, 0, len(inventory))
	for _, item := range inventory {
		parts := strings.SplitN(item.rpc, "/", 2); descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(parts[0])); if err != nil { panic(err) }; service := descriptor.(protoreflect.ServiceDescriptor); method := service.Methods().ByName(protoreflect.Name(parts[1]))
		request := dynamicpb.NewMessage(method.Input()); fill(request, 0, 17); requests := []frame{frameFor(request, 0)}; if method.IsStreamingClient() { second := dynamicpb.NewMessage(method.Input()); fill(second, 0, 97); requests = append(requests, frameFor(second, 1)) }
		started := time.Now(); responses := make([]frame, 0); status, callError := "passed", ""; ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if method.IsStreamingClient() || method.IsStreamingServer() {
			stream, streamErr := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: method.IsStreamingServer(), ClientStreams: method.IsStreamingClient()}, "/"+item.rpc)
			if streamErr == nil && method.IsStreamingClient() { streamErr = stream.SendMsg(request); if streamErr == nil { second := dynamicpb.NewMessage(method.Input()); fill(second, 0, 97); streamErr = stream.SendMsg(second) }; if streamErr == nil { streamErr = stream.CloseSend() } }
			if streamErr == nil {
				if method.IsStreamingServer() { for sequence := 0; ; sequence++ { response := dynamicpb.NewMessage(method.Output()); streamErr = stream.RecvMsg(response); if streamErr != nil { break }; responses = append(responses, frameFor(response, sequence)) } } else { response := dynamicpb.NewMessage(method.Output()); streamErr = stream.RecvMsg(response); if streamErr == nil { responses = append(responses, frameFor(response, 0)) } }
			}
			if streamErr != nil && streamErr.Error() != "EOF" { status, callError = "failed", streamErr.Error() }
		} else { response := dynamicpb.NewMessage(method.Output()); if invokeErr := conn.Invoke(ctx, "/"+item.rpc, request, response); invokeErr != nil { status, callError = "failed", invokeErr.Error() } else { responses = append(responses, frameFor(response, 0)) } }
		cancel(); result := scenario{"acyclic.sdk.rpc-scenario-result.v2", *revision, item.family, item.rpc, item.shape, true, "remote", "grpc", status, 0, requests, responses, map[string]string{"code": "OK", "details": ""}, len(responses), float64(time.Since(started).Microseconds()) / 1000, callError}; if callError != "" { result.ExitCode = 1 }; results = append(results, result)
	}
	bytes, err := json.MarshalIndent(map[string]any{"schema": "acyclic.sdk.rpc-scenario-log.v2", "consumer": "go-rust-authority-release", "source_revision": *revision, "execution_mode": "remote", "scenarios": results}, "", "  "); if err != nil { panic(err) }; if dir := filepath.Dir(*out); dir != "." { if err := os.MkdirAll(dir, 0755); err != nil { panic(err) } }; if err := os.WriteFile(*out, append(bytes, '\n'), 0644); err != nil { panic(err) }
}

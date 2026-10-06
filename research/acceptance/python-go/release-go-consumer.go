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
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

type authority struct {
	SourceRevision string `json:"source_revision"`
	Families       []struct {
		Source     string `json:"source"`
		RPCMethods []struct {
			RPC   string `json:"rpc"`
			Shape string `json:"shape"`
		} `json:"rpc_methods"`
	} `json:"families"`
}
type inventoryItem struct{ family, rpc, shape string }
type frame struct {
	Sequence int    `json:"sequence"`
	Type     string `json:"type"`
	Bytes    string `json:"bytes_base64"`
	SHA256   string `json:"sha256"`
}
type scenario struct {
	Schema         string  `json:"schema"`
	SourceRevision string  `json:"source_revision"`
	ExecutionStep  int     `json:"execution_step"`
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
	TerminalKind   *string `json:"terminal_kind"`
	ResponseCount  int     `json:"response_count"`
	ElapsedMillis  float64 `json:"elapsed_ms"`
	Error          string  `json:"error,omitempty"`
}
type typedManifest struct {
	Complete      bool          `json:"complete"`
	Records       []typedRecord `json:"records"`
	ExecutionPlan []typedRecord `json:"execution_plan"`
}
type typedRecord struct {
	RPC            string               `json:"rpc"`
	RequestType    string               `json:"request_type"`
	RequestBase64  string               `json:"request_base64"`
	RequestFrames  []typedRequestFrame  `json:"request_frames"`
	ResponseFrames []typedResponseFrame `json:"response_frames"`
}
type typedRequestFrame struct {
	Sequence      int    `json:"sequence"`
	RequestType   string `json:"type"`
	RequestBase64 string `json:"bytes_base64"`
	RequestSHA256 string `json:"sha256"`
}
type typedResponseFrame struct {
	Sequence int `json:"sequence"`
}

func frameFor(message protoreflect.ProtoMessage, sequence int) frame {
	bytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	sum := sha256.Sum256(bytes)
	return frame{sequence, string(message.ProtoReflect().Descriptor().FullName()), base64.StdEncoding.EncodeToString(bytes), "sha256:" + hex.EncodeToString(sum[:])}
}

func scalar(field protoreflect.FieldDescriptor, seed int) protoreflect.Value {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(int32(seed))
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(int64(seed))
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(uint32(seed + 1))
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(uint64(1)<<63 + uint64(seed))
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(1.25)
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(2.5)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("fixture-" + string(field.Name()))
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte{byte(seed % 251), byte(seed % 251), byte(seed % 251), byte(seed % 251)})
	case protoreflect.EnumKind:
		values := field.Enum().Values()
		number := values.Get(0).Number()
		if values.Len() > 1 {
			number = values.Get(1).Number()
		}
		return protoreflect.ValueOfEnum(number)
	default:
		panic("unsupported protobuf scalar kind")
	}
}

func fill(message protoreflect.Message, depth, seed int) {
	if depth > 3 {
		return
	}
	fields := message.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.IsMap() {
			continue
		}
		if field.ContainingOneof() != nil && i > 0 {
			continue
		}
		valueSeed := seed + i + 1
		if field.Cardinality() == protoreflect.Repeated {
			list := message.Mutable(field).List()
			if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
				child := dynamicpb.NewMessage(field.Message())
				fill(child, depth+1, valueSeed)
				list.Append(protoreflect.ValueOfMessage(child))
			} else {
				list.Append(scalar(field, valueSeed))
			}
		} else if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
			child := dynamicpb.NewMessage(field.Message())
			fill(child, depth+1, valueSeed)
			message.Set(field, protoreflect.ValueOfMessage(child))
		} else {
			message.Set(field, scalar(field, valueSeed))
		}
	}
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
	seen := make(map[string]struct{}, len(inventory))
	for _, item := range inventory {
		if _, exists := seen[item.rpc]; exists {
			return root, nil, fmt.Errorf("Rust authority repeats RPC %s", item.rpc)
		}
		seen[item.rpc] = struct{}{}
	}
	if len(inventory) == 0 {
		return root, nil, fmt.Errorf("Rust authority contains no RPCs")
	}
	return root, inventory, nil
}

func loadTypedManifest(path string, inventory []inventoryItem) (map[string]typedRecord, []typedRecord, error) {
	var manifest typedManifest
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	if err := json.Unmarshal(bytes, &manifest); err != nil {
		return nil, nil, err
	}
	if !manifest.Complete {
		return nil, nil, fmt.Errorf("Rust typed manifest is incomplete")
	}
	plan := manifest.ExecutionPlan
	if len(plan) == 0 {
		plan = manifest.Records
	}
	if len(plan) == 0 {
		return nil, nil, fmt.Errorf("Rust typed manifest has no execution records")
	}
	known := make(map[string]inventoryItem, len(inventory))
	for _, item := range inventory {
		known[item.rpc] = item
	}
	records := make(map[string]typedRecord, len(manifest.Records))
	for _, record := range manifest.Records {
		if record.RPC == "" {
			return nil, nil, fmt.Errorf("Rust typed manifest record is missing RPC")
		}
		if _, exists := records[record.RPC]; exists {
			return nil, nil, fmt.Errorf("Rust typed manifest repeats %s", record.RPC)
		}
		records[record.RPC] = record
	}
	for _, record := range plan {
		if record.RPC == "" {
			return nil, nil, fmt.Errorf("Rust typed manifest execution record is missing RPC")
		}
		if _, ok := known[record.RPC]; !ok {
			return nil, nil, fmt.Errorf("Rust typed manifest execution record %s is absent from Rust authority", record.RPC)
		}
	}
	planned := make(map[string]struct{}, len(plan))
	for _, record := range plan {
		planned[record.RPC] = struct{}{}
	}
	for _, item := range inventory {
		if _, ok := planned[item.rpc]; !ok {
			return nil, nil, fmt.Errorf("Rust typed execution plan is missing %s", item.rpc)
		}
		if _, ok := records[item.rpc]; !ok {
			return nil, nil, fmt.Errorf("Rust typed manifest is missing %s", item.rpc)
		}
	}
	return records, plan, nil
}

func requestFromTypedRecord(record typedRecord, method protoreflect.MethodDescriptor) (*dynamicpb.Message, error) {
	bytes, err := base64.StdEncoding.DecodeString(record.RequestBase64)
	if err != nil {
		return nil, fmt.Errorf("%s request bytes: %w", record.RPC, err)
	}
	request := dynamicpb.NewMessage(method.Input())
	if err := (proto.UnmarshalOptions{}).Unmarshal(bytes, request); err != nil {
		return nil, fmt.Errorf("%s request decode: %w", record.RPC, err)
	}
	return request, nil
}

func requestsFromTypedRecord(record typedRecord, method protoreflect.MethodDescriptor) ([]*dynamicpb.Message, error) {
	frames := record.RequestFrames
	if len(frames) == 0 {
		request, err := requestFromTypedRecord(record, method)
		if err != nil {
			return nil, err
		}
		return []*dynamicpb.Message{request}, nil
	}
	requests := make([]*dynamicpb.Message, 0, len(frames))
	for index, frame := range frames {
		if frame.Sequence != index {
			return nil, fmt.Errorf("%s request frames are out of order at index %d", record.RPC, index)
		}
		bytes, err := base64.StdEncoding.DecodeString(frame.RequestBase64)
		if err != nil {
			return nil, fmt.Errorf("%s request frame %d bytes: %w", record.RPC, index, err)
		}
		digest := sha256.Sum256(bytes)
		if frame.RequestSHA256 != "" && frame.RequestSHA256 != "sha256:"+hex.EncodeToString(digest[:]) {
			return nil, fmt.Errorf("%s request frame %d digest differs from Rust manifest", record.RPC, index)
		}
		request := dynamicpb.NewMessage(method.Input())
		if err := (proto.UnmarshalOptions{}).Unmarshal(bytes, request); err != nil {
			return nil, fmt.Errorf("%s request frame %d decode: %w", record.RPC, index, err)
		}
		requests = append(requests, request)
	}
	return requests, nil
}

func main() {
	authorityPath := flag.String("authority", "", "Rust authority manifest")
	endpoint := flag.String("endpoint", "127.0.0.1:18081", "gRPC endpoint")
	out := flag.String("out", "scenario-log.json", "scenario log path")
	revision := flag.String("source-revision", "", "source revision override")
	typedManifestPath := flag.String("typed-manifest", "", "Rust executable typed request manifest")
	flag.Parse()
	root, inventory, err := loadAuthority(*authorityPath)
	if err != nil {
		panic(err)
	}
	if *revision == "" {
		*revision = root.SourceRevision
	}
	var typed map[string]typedRecord
	var executionPlan []typedRecord
	runInventory := inventory
	if *typedManifestPath == "" {
		panic("--typed-manifest is required: request bytes and stream dependencies must come from the Rust executable manifest")
	}
	manifestBytes, err := os.ReadFile(*typedManifestPath)
	if err != nil {
		panic(err)
	}
	manifestSum := sha256.Sum256(manifestBytes)
	manifestDigest := "sha256:" + hex.EncodeToString(manifestSum[:])
	{
		typed, executionPlan, err = loadTypedManifest(*typedManifestPath, inventory)
		if err != nil {
			panic(err)
		}
		byRPC := make(map[string]inventoryItem, len(inventory))
		for _, item := range inventory {
			byRPC[item.rpc] = item
		}
		runInventory = make([]inventoryItem, 0, len(executionPlan))
		for _, record := range executionPlan {
			runInventory = append(runInventory, byRPC[record.RPC])
		}
	}
	connectContext, connectCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer connectCancel()
	conn, err := grpc.DialContext(connectContext, *endpoint, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	results := make([]scenario, 0, len(inventory))
	for step, item := range runInventory {
		parts := strings.SplitN(item.rpc, "/", 2)
		descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(parts[0]))
		if err != nil {
			panic(err)
		}
		service := descriptor.(protoreflect.ServiceDescriptor)
		method := service.Methods().ByName(protoreflect.Name(parts[1]))
		request := dynamicpb.NewMessage(method.Input())
		requestMessages := []*dynamicpb.Message{request}
		var planRecord *typedRecord
		if step < len(executionPlan) {
			planRecord = &executionPlan[step]
		}
		if planRecord != nil {
			requestMessages, err = requestsFromTypedRecord(*planRecord, method)
			if err != nil {
				panic(err)
			}
			request = requestMessages[0]
		} else if record, ok := typed[item.rpc]; ok {
			requestMessages, err = requestsFromTypedRecord(record, method)
			if err != nil {
				panic(err)
			}
			request = requestMessages[0]
		} else {
			fill(request, 0, 17)
			if method.IsStreamingClient() {
				second := dynamicpb.NewMessage(method.Input())
				fill(second, 0, 97)
				requestMessages = append(requestMessages, second)
			}
		}
		requests := make([]frame, 0, len(requestMessages))
		for sequence, message := range requestMessages {
			requests = append(requests, frameFor(message, sequence))
		}
		started := time.Now()
		responses := make([]frame, 0)
		status, callError := "observed", ""
		var callErr error
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if method.IsStreamingClient() || method.IsStreamingServer() {
			// Keep the descriptor identical to the generated Go clients.  In
			// particular, grpc-go uses StreamName when constructing the
			// method statistics and transport metadata for server streams.
			// The fully-qualified RPC path remains the third argument.
			stream, streamErr := conn.NewStream(ctx, &grpc.StreamDesc{StreamName: string(method.Name()), ServerStreams: method.IsStreamingServer(), ClientStreams: method.IsStreamingClient()}, "/"+item.rpc)
			// A server-streaming RPC still receives one request message before
			// its response stream begins.  The generated clients always send
			// this message and half-close; omitting it leaves the fixture waiting
			// until the consumer deadline.  Client-streaming calls may add the
			// legacy second seed only when no Rust-owned manifest is in use.
			if streamErr == nil {
				for _, message := range requestMessages {
					streamErr = stream.SendMsg(message)
					if streamErr != nil {
						break
					}
				}
				if streamErr == nil {
					streamErr = stream.CloseSend()
				}
			}
			if streamErr == nil {
				if method.IsStreamingServer() {
					expectedFrames := 0
					if planRecord != nil {
						expectedFrames = len(planRecord.ResponseFrames)
					} else if record, ok := typed[item.rpc]; ok {
						expectedFrames = len(record.ResponseFrames)
					}
					for sequence := 0; ; sequence++ {
						if expectedFrames > 0 && sequence >= expectedFrames {
							break
						}
						response := dynamicpb.NewMessage(method.Output())
						streamErr = stream.RecvMsg(response)
						if streamErr != nil {
							break
						}
						responses = append(responses, frameFor(response, sequence))
					}
				} else {
					response := dynamicpb.NewMessage(method.Output())
					streamErr = stream.RecvMsg(response)
					if streamErr == nil {
						responses = append(responses, frameFor(response, 0))
					}
				}
			}
		} else {
			response := dynamicpb.NewMessage(method.Output())
			if invokeErr := conn.Invoke(ctx, "/"+item.rpc, request, response); invokeErr != nil {
				status, callError, callErr = "observed_error", invokeErr.Error(), invokeErr
			} else {
				responses = append(responses, frameFor(response, 0))
			}
		}
		if callErr != nil {
			status = "observed_error"
		}
		terminal := map[string]string{"code": "OK", "details": ""}
		if callErr != nil {
			terminal["code"], terminal["details"] = grpcstatus.Code(callErr).String(), callError
		}
		var terminalKind *string
		if callErr != nil {
			kind := "error"
			if grpcstatus.Code(callErr).String() == "DeadlineExceeded" {
				kind = "timeout"
			}
			terminalKind = &kind
		} else if item.shape != "unary" {
			kind := "eof"
			terminalKind = &kind
		}
		cancel()
		result := scenario{Schema: "acyclic.sdk.rpc-scenario-result.v2", SourceRevision: *revision, ExecutionStep: step, Family: item.family, RPC: item.rpc, Shape: item.shape, Invoked: true, ExecutionMode: "remote", Transport: "grpc", Status: status, RequestFrames: requests, ResponseFrames: responses, Terminal: terminal, TerminalKind: terminalKind, ResponseCount: len(responses), ElapsedMillis: float64(time.Since(started).Microseconds()) / 1000, Error: callError}
		results = append(results, result)
	}
	bytes, err := json.MarshalIndent(map[string]any{"schema": "acyclic.sdk.rpc-scenario-log.v2", "consumer": "go-rust-authority-release", "source_revision": *revision, "manifest_sha256": manifestDigest, "execution_mode": "remote", "scenarios": results}, "", "  ")
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

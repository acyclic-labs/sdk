package main

import (
	"bytes"
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

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
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
	SourceGitSHA   string   `json:"source_git_sha"`
	SourceRevision string   `json:"source_revision"`
	Families       []family `json:"families"`
}
type family struct {
	Source           string      `json:"source"`
	SourceSHA256     string      `json:"source_sha256"`
	Descriptor       string      `json:"descriptor"`
	DescriptorSHA256 string      `json:"descriptor_sha256"`
	RPCMethods       []rpcMethod `json:"rpc_methods"`
}

type rpcMethod struct {
	RPC   string `json:"rpc"`
	Shape string `json:"shape"`
}
type receipt struct {
	Schema          string            `json:"schema"`
	Target          string            `json:"target"`
	SourceRevision  string            `json:"source_revision"`
	Authority       string            `json:"authority"`
	GoVersion       string            `json:"go_version"`
	ProtocVersion   string            `json:"protoc_version"`
	ProtocGenGo     string            `json:"protoc_gen_go_version"`
	ProtocGenGoGRPC string            `json:"protoc_gen_go_grpc_version"`
	Inputs          []string          `json:"inputs"`
	Outputs         []string          `json:"outputs"`
	OutputSHA256    map[string]string `json:"output_sha256"`
	GeneratedAt     string            `json:"generated_at_utc"`
}

type typedConsumerReceipt struct {
	Schema            string                   `json:"schema"`
	Target            string                   `json:"target"`
	SourceRevision    string                   `json:"source_revision"`
	SourceDigest      string                   `json:"source_digest"`
	RustModelDigest   string                   `json:"rust_model_digest"`
	GeneratedConsumer typedConsumerBoundFile   `json:"generated_consumer"`
	Command           typedConsumerCommand     `json:"command"`
	Assertions        []typedConsumerAssertion `json:"assertions"`
}

type typedConsumerBoundFile struct {
	Path           string `json:"path"`
	SHA256         string `json:"sha256"`
	SourceRevision string `json:"source_revision"`
	SourceDigest   string `json:"source_digest"`
}

type typedConsumerCommand struct {
	Executed       bool     `json:"executed"`
	ExitCode       int      `json:"exit_code"`
	Argv           []string `json:"argv"`
	SourceRevision string   `json:"source_revision"`
	SourceDigest   string   `json:"source_digest"`
	ToolPath       string   `json:"tool_path"`
	ToolSHA256     string   `json:"tool_sha256"`
	ToolVersion    string   `json:"tool_version"`
	StdoutPath     string   `json:"stdout_path"`
	StdoutSHA256   string   `json:"stdout_sha256"`
	StderrPath     string   `json:"stderr_path"`
	StderrSHA256   string   `json:"stderr_sha256"`
}

type typedConsumerAssertion struct {
	ID          string                `json:"id"`
	Status      string                `json:"status"`
	Executed    bool                  `json:"executed"`
	SourceBound bool                  `json:"source_bound"`
	Evidence    typedConsumerEvidence `json:"evidence"`
}

type typedConsumerEvidence struct {
	Runtime               bool   `json:"runtime"`
	GeneratedConsumerPath string `json:"generated_consumer_path"`
	StdoutPath            string `json:"stdout_path"`
	StderrPath            string `json:"stderr_path"`
	GeneratedConsumerSHA  string `json:"generated_consumer_sha256"`
	StdoutSHA             string `json:"stdout_sha256"`
	StderrSHA             string `json:"stderr_sha256"`
}

type expectedField struct {
	Message     string
	Name        string
	Number      int32
	JSONName    string
	Kind        string
	HasPresence bool
	Oneof       string
	EnumType    string
	Repeated    bool
}

type expectedRPC struct {
	Service      string
	Method       string
	ClientStream bool
	ServerStream bool
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
	if err := os.RemoveAll(outputRoot); err != nil {
		return fmt.Errorf("reset output: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(outputRoot, "gen"), 0o755); err != nil {
		return err
	}
	protoc, genGo, genGRPC, err = resolveTools(protoc, genGo, genGRPC, wantProtoc, wantGenGo, wantGenGRPC)
	if err != nil {
		return err
	}
	if wantGo != "" && runtime.Version() != wantGo {
		return fmt.Errorf("Go runtime %s does not match pinned %s", runtime.Version(), wantGo)
	}
	goToolPath, goToolVersion, goToolSHA256, err := resolveGoTool(wantGo)
	if err != nil {
		return err
	}
	familySources := make([]string, 0, len(manifest.Families))
	for _, f := range manifest.Families {
		if err := validateInput(authorityRoot, f.Source, f.SourceSHA256); err != nil {
			return err
		}
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
		hasValidation = true
		sources = append(sources, "validation/v1/options.proto")
	}
	sort.Strings(sources)
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
	if hasValidation {
		validationArgs := []string{"--proto_path=" + authorityRoot, "--plugin=protoc-gen-go=" + genGo, "--go_out=paths=source_relative:" + filepath.Join(outputRoot, "gen"), "--go_opt=Mvalidation/v1/options.proto=github.com/acyclic-labs/sdk/go/gen/validation/v1", "validation/v1/options.proto"}
		validationCommand := exec.Command(protoc, validationArgs...)
		validationCommand.Dir = sourceRoot
		if output, err := validationCommand.CombinedOutput(); err != nil {
			return fmt.Errorf("validation options generation failed: %w\n%s", err, strings.TrimSpace(string(output)))
		}
	}
	if err := copyModuleFiles(sourceRoot, outputRoot); err != nil {
		return err
	}
	if err := emitTypedConsumer(authorityRoot, outputRoot, &manifest, protoc, goToolPath, goToolVersion, goToolSHA256); err != nil {
		return fmt.Errorf("emit Go typed consumer: %w", err)
	}
	outputs, hashes, err := collectOutputs(outputRoot)
	if err != nil {
		return err
	}
	if len(outputs) < len(manifest.Families) {
		return fmt.Errorf("generated only %d files for %d authority families", len(outputs), len(manifest.Families))
	}
	r := receipt{Schema: "acyclic.sdk.go-producer-receipt.v1", Target: "go", SourceRevision: manifest.SourceRevision, Authority: "rust", GoVersion: runtime.Version(), ProtocVersion: wantProtoc, ProtocGenGo: wantGenGo, ProtocGenGoGRPC: wantGenGRPC, Inputs: sources, Outputs: outputs, OutputSHA256: hashes, GeneratedAt: time.Now().UTC().Format(time.RFC3339)}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(filepath.Join(outputRoot, "generation-receipt.json"), b, 0o644)
}

// emitTypedConsumer creates the source-bound Go witness consumed by the Rust
// generation supervisor. The witness is generated from the exact Rust-owned
// .proto inputs used for this invocation; it does not carry a second hand-written
// contract or RPC inventory. Its checks exercise the generated Go descriptors
// and fail if a generator changes field identity, presence, oneofs, scalar
// kinds, enum values, or stream direction.
func emitTypedConsumer(authorityRoot, outputRoot string, manifest *authorityManifest, protoc, goToolPath, goToolVersion, goToolSHA256 string) error {
	if manifest.SourceGitSHA == "" || len(manifest.SourceGitSHA) != 40 {
		return errors.New("Rust authority manifest has no 40-character source_git_sha")
	}
	if manifest.SourceRevision == "" || len(manifest.SourceRevision) != 64 {
		return errors.New("Rust authority manifest has no 64-character source_revision")
	}

	fields := make([]expectedField, 0)
	enums := make([]expectedEnum, 0)
	enumValues := map[string][]int32{}
	services := make([]expectedRPC, 0)
	imports := make([]string, 0, len(manifest.Families))
	seenImports := map[string]bool{}
	sourceFiles := make([]string, 0, len(manifest.Families))
	for _, family := range manifest.Families {
		sourceFiles = append(sourceFiles, filepath.ToSlash(family.Source))
		importPath := filepath.ToSlash(filepath.Dir(family.Source))
		if importPath != "" && !seenImports[importPath] {
			seenImports[importPath] = true
			imports = append(imports, importPath)
		}
		for _, method := range family.RPCMethods {
			service, methodName, ok := strings.Cut(method.RPC, "/")
			if !ok || service == "" || methodName == "" {
				return fmt.Errorf("authority RPC identity is malformed: %q", method.RPC)
			}
			clientStreaming, serverStreaming, err := streamShape(method.Shape)
			if err != nil {
				return fmt.Errorf("authority RPC %s: %w", method.RPC, err)
			}
			services = append(services, expectedRPC{Service: service, Method: methodName, ClientStream: clientStreaming, ServerStream: serverStreaming})
		}
	}
	descriptorPath := filepath.Join(outputRoot, "typed-consumer-authority.fds.bin")
	descriptorArgs := []string{"--proto_path=" + authorityRoot, "--include_imports", "--descriptor_set_out=" + descriptorPath}
	descriptorArgs = append(descriptorArgs, sourceFiles...)
	descriptorCommand := exec.Command(protoc, descriptorArgs...)
	descriptorCommand.Dir = authorityRoot
	if output, err := descriptorCommand.CombinedOutput(); err != nil {
		return fmt.Errorf("source descriptor generation failed: %w\n%s", err, strings.TrimSpace(string(output)))
	}
	defer os.Remove(descriptorPath)
	files, err := readDescriptorFiles(descriptorPath)
	if err != nil {
		return fmt.Errorf("read source descriptor set: %w", err)
	}
	for _, family := range manifest.Families {
		for _, file := range files {
			if filepath.ToSlash(file.GetName()) != filepath.ToSlash(family.Source) {
				continue
			}
			for _, enum := range file.EnumType {
				collectEnumMetadata(enum, file.GetPackage(), "", enumValues)
			}
			for _, message := range file.MessageType {
				collectMessageMetadata(message, file.GetPackage(), "", &fields, enumValues)
			}
		}
	}
	for _, field := range fields {
		if field.Kind != "enum" {
			continue
		}
		values := enumValues[normalizeTypeName(field.EnumType)]
		if len(values) == 0 {
			return fmt.Errorf("Rust authority enum %s.%s has no declared values", field.Message, field.Name)
		}
		enums = append(enums, expectedEnum{Message: field.Message, Field: field.Name, Values: values, Repeated: field.Repeated})
	}
	sort.Slice(fields, func(i, j int) bool {
		if fields[i].Message != fields[j].Message {
			return fields[i].Message < fields[j].Message
		}
		return fields[i].Number < fields[j].Number
	})
	sort.Slice(enums, func(i, j int) bool {
		if enums[i].Message != enums[j].Message {
			return enums[i].Message < enums[j].Message
		}
		return enums[i].Field < enums[j].Field
	})
	sort.Slice(services, func(i, j int) bool {
		if services[i].Service != services[j].Service {
			return services[i].Service < services[j].Service
		}
		return services[i].Method < services[j].Method
	})
	if len(fields) == 0 || len(services) == 0 {
		return errors.New("Rust authority descriptors did not produce fields and RPCs")
	}

	var source strings.Builder
	source.WriteString("// Code generated by the Rust-authority Go producer; DO NOT EDIT.\n")
	source.WriteString("package main\n\n")
	source.WriteString("import (\n")
	for _, importPath := range imports {
		fmt.Fprintf(&source, "\t_ %q\n", "github.com/acyclic-labs/sdk/go/gen/"+importPath)
	}
	source.WriteString("\t\"encoding/json\"\n")
	source.WriteString("\t\"fmt\"\n")
	source.WriteString("\t\"google.golang.org/protobuf/reflect/protoreflect\"\n")
	source.WriteString("\t\"google.golang.org/protobuf/reflect/protoregistry\"\n")
	source.WriteString("\t\"google.golang.org/protobuf/types/dynamicpb\"\n")
	source.WriteString(")\n\n")
	fmt.Fprintf(&source, "const sourceRevision = %q\nconst sourceDigest = %q\n\n", manifest.SourceGitSHA, manifest.SourceRevision)
	source.WriteString("type expectedField struct { message, name string; number int32; jsonName, kind string; presence bool; oneof string }\n")
	source.WriteString("type expectedEnum struct { message, field string; values []int32; repeated bool }\n")
	source.WriteString("type expectedRPC struct { service, method string; clientStream, serverStream bool }\n\n")
	source.WriteString("var expectedFields = []expectedField{\n")
	for _, field := range fields {
		fmt.Fprintf(&source, "\t{%q, %q, %d, %q, %q, %t, %q},\n", field.Message, field.Name, field.Number, field.JSONName, field.Kind, field.HasPresence, field.Oneof)
	}
	source.WriteString("}\n\nvar expectedEnums = []expectedEnum{\n")
	for _, enum := range enums {
		fmt.Fprintf(&source, "\t{%q, %q, []int32{", enum.Message, enum.Field)
		for index, value := range enum.Values {
			if index > 0 {
				source.WriteString(", ")
			}
			fmt.Fprintf(&source, "%d", value)
		}
		fmt.Fprintf(&source, "}, %t},\n", enum.Repeated)
	}
	source.WriteString("}\n\nvar expectedRPCs = []expectedRPC{\n")
	for _, rpc := range services {
		fmt.Fprintf(&source, "\t{%q, %q, %t, %t},\n", rpc.Service, rpc.Method, rpc.ClientStream, rpc.ServerStream)
	}
	source.WriteString("}\n\n")
	source.WriteString(typedConsumerProgram)

	consumerPath := filepath.Join(outputRoot, "typed-consumer.go")
	consumerBytes := []byte(source.String())
	if err := os.WriteFile(consumerPath, consumerBytes, 0o644); err != nil {
		return err
	}
	stdoutPath := filepath.Join(outputRoot, "typed-consumer-supervisor.stdout")
	stderrPath := filepath.Join(outputRoot, "typed-consumer-supervisor.stderr")
	command := exec.Command(goToolPath, "run", "typed-consumer.go")
	command.Dir = outputRoot
	var stderr bytes.Buffer
	command.Stderr = &stderr
	process, err := command.Output()
	if err != nil {
		stderrBytes := stderr.Bytes()
		if len(stderrBytes) == 0 {
			if exitError, ok := err.(*exec.ExitError); ok {
				stderrBytes = exitError.Stderr
			}
		}
		_ = os.WriteFile(stdoutPath, process, 0o644)
		_ = os.WriteFile(stderrPath, stderrBytes, 0o644)
		return fmt.Errorf("typed Go consumer failed: %w\n%s", err, strings.TrimSpace(string(stderrBytes)))
	}
	if err := os.WriteFile(stdoutPath, process, 0o644); err != nil {
		return err
	}
	stderrBytes := stderr.Bytes()
	if err := os.WriteFile(stderrPath, stderrBytes, 0o644); err != nil {
		return err
	}
	consumerHash := sha256.Sum256(consumerBytes)
	stdoutHash := sha256.Sum256(process)
	stderrHash := sha256.Sum256(stderrBytes)
	consumerDigest := "sha256:" + hex.EncodeToString(consumerHash[:])
	stdoutDigest := "sha256:" + hex.EncodeToString(stdoutHash[:])
	stderrDigest := "sha256:" + hex.EncodeToString(stderrHash[:])
	evidence := func() typedConsumerEvidence {
		return typedConsumerEvidence{
			Runtime:               true,
			GeneratedConsumerPath: "typed-consumer.go",
			StdoutPath:            "typed-consumer-supervisor.stdout",
			StderrPath:            "typed-consumer-supervisor.stderr",
			GeneratedConsumerSHA:  consumerDigest,
			StdoutSHA:             stdoutDigest,
			StderrSHA:             stderrDigest,
		}
	}
	assertions := make([]typedConsumerAssertion, 0, 6)
	for _, id := range []string{"field-identities", "presence-oneof", "bytes", "uint64", "enums", "rpc-stream-signatures"} {
		assertions = append(assertions, typedConsumerAssertion{ID: id, Status: "passed", Executed: true, SourceBound: true, Evidence: evidence()})
	}
	receipt := typedConsumerReceipt{
		Schema:            "acyclic.sdk.typed-consumer-receipt.v1",
		Target:            "go",
		SourceRevision:    manifest.SourceGitSHA,
		SourceDigest:      manifest.SourceRevision,
		RustModelDigest:   manifest.SourceRevision,
		GeneratedConsumer: typedConsumerBoundFile{Path: "typed-consumer.go", SHA256: consumerDigest, SourceRevision: manifest.SourceGitSHA, SourceDigest: manifest.SourceRevision},
		Command:           typedConsumerCommand{Executed: true, ExitCode: 0, Argv: []string{"go", "run", "typed-consumer.go"}, SourceRevision: manifest.SourceGitSHA, SourceDigest: manifest.SourceRevision, ToolPath: goToolPath, ToolSHA256: goToolSHA256, ToolVersion: goToolVersion, StdoutPath: "typed-consumer-supervisor.stdout", StdoutSHA256: stdoutDigest, StderrPath: "typed-consumer-supervisor.stderr", StderrSHA256: stderrDigest},
		Assertions:        assertions,
	}
	receiptBytes, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return err
	}
	receiptBytes = append(receiptBytes, '\n')
	return os.WriteFile(filepath.Join(outputRoot, "typed-consumer-receipt.json"), receiptBytes, 0o644)
}

type expectedEnum struct {
	Message  string
	Field    string
	Values   []int32
	Repeated bool
}

func readDescriptorFiles(path string) ([]*descriptorpb.FileDescriptorProto, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(b, &set); err == nil && len(set.File) > 0 {
		return set.File, nil
	}
	var file descriptorpb.FileDescriptorProto
	if err := proto.Unmarshal(b, &file); err != nil || file.GetName() == "" {
		return nil, fmt.Errorf("not a FileDescriptorSet or FileDescriptorProto")
	}
	return []*descriptorpb.FileDescriptorProto{&file}, nil
}

func collectMessageMetadata(message *descriptorpb.DescriptorProto, packageName, parent string, fields *[]expectedField, enumValues map[string][]int32) {
	fullName := message.GetName()
	if parent != "" {
		fullName = parent + "." + fullName
	} else if packageName != "" {
		fullName = packageName + "." + fullName
	}
	oneofs := make([]string, len(message.OneofDecl))
	for index, oneof := range message.OneofDecl {
		oneofs[index] = oneof.GetName()
	}
	for _, field := range message.Field {
		kind := descriptorKind(field.GetType())
		presence := field.GetProto3Optional() || field.OneofIndex != nil || ((field.GetType() == descriptorpb.FieldDescriptorProto_TYPE_MESSAGE || field.GetType() == descriptorpb.FieldDescriptorProto_TYPE_GROUP) && field.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REPEATED)
		oneof := ""
		if field.OneofIndex != nil && int(*field.OneofIndex) < len(oneofs) {
			oneof = oneofs[*field.OneofIndex]
		}
		jsonName := field.GetJsonName()
		if jsonName == "" {
			jsonName = lowerCamel(field.GetName())
		}
		*fields = append(*fields, expectedField{Message: fullName, Name: field.GetName(), Number: field.GetNumber(), JSONName: jsonName, Kind: kind, HasPresence: presence, Oneof: oneof, EnumType: field.GetTypeName(), Repeated: field.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED})
	}
	for _, nested := range message.EnumType {
		collectEnumMetadata(nested, "", fullName, enumValues)
	}
	for _, nested := range message.NestedType {
		collectMessageMetadata(nested, "", fullName, fields, enumValues)
	}
}

func collectEnumMetadata(enum *descriptorpb.EnumDescriptorProto, packageName, parent string, values map[string][]int32) {
	fullName := enum.GetName()
	if parent != "" {
		fullName = parent + "." + fullName
	} else if packageName != "" {
		fullName = packageName + "." + fullName
	}
	items := make([]int32, 0, len(enum.Value))
	for _, value := range enum.Value {
		items = append(items, value.GetNumber())
	}
	values[normalizeTypeName(fullName)] = items
}

func normalizeTypeName(value string) string { return strings.TrimPrefix(value, ".") }

func descriptorKind(kind descriptorpb.FieldDescriptorProto_Type) string {
	return map[descriptorpb.FieldDescriptorProto_Type]string{
		descriptorpb.FieldDescriptorProto_TYPE_DOUBLE:   "double",
		descriptorpb.FieldDescriptorProto_TYPE_FLOAT:    "float",
		descriptorpb.FieldDescriptorProto_TYPE_INT64:    "int64",
		descriptorpb.FieldDescriptorProto_TYPE_UINT64:   "uint64",
		descriptorpb.FieldDescriptorProto_TYPE_INT32:    "int32",
		descriptorpb.FieldDescriptorProto_TYPE_FIXED64:  "fixed64",
		descriptorpb.FieldDescriptorProto_TYPE_FIXED32:  "fixed32",
		descriptorpb.FieldDescriptorProto_TYPE_BOOL:     "bool",
		descriptorpb.FieldDescriptorProto_TYPE_STRING:   "string",
		descriptorpb.FieldDescriptorProto_TYPE_GROUP:    "group",
		descriptorpb.FieldDescriptorProto_TYPE_MESSAGE:  "message",
		descriptorpb.FieldDescriptorProto_TYPE_BYTES:    "bytes",
		descriptorpb.FieldDescriptorProto_TYPE_UINT32:   "uint32",
		descriptorpb.FieldDescriptorProto_TYPE_ENUM:     "enum",
		descriptorpb.FieldDescriptorProto_TYPE_SFIXED32: "sfixed32",
		descriptorpb.FieldDescriptorProto_TYPE_SFIXED64: "sfixed64",
		descriptorpb.FieldDescriptorProto_TYPE_SINT32:   "sint32",
		descriptorpb.FieldDescriptorProto_TYPE_SINT64:   "sint64",
	}[kind]
}

func lowerCamel(value string) string {
	parts := strings.Split(value, "_")
	if len(parts) == 0 {
		return value
	}
	result := parts[0]
	for _, part := range parts[1:] {
		if part == "" {
			continue
		}
		result += strings.ToUpper(part[:1]) + part[1:]
	}
	return result
}

func streamShape(shape string) (bool, bool, error) {
	switch strings.ToLower(shape) {
	case "unary":
		return false, false, nil
	case "client", "client-streaming":
		return true, false, nil
	case "server", "server-streaming":
		return false, true, nil
	case "bidi", "bidirectional", "bidi-streaming":
		return true, true, nil
	default:
		return false, false, fmt.Errorf("unsupported stream shape %q", shape)
	}
}

const typedConsumerProgram = `
func descriptorMessage(name string) protoreflect.MessageDescriptor {
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil { panic(err) }
	message, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok { panic("descriptor is not a message") }
	return message
}

func descriptorService(name string) protoreflect.ServiceDescriptor {
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil { panic(err) }
	service, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok { panic("descriptor is not a service") }
	return service
}

func main() {
	fieldCount, bytesCount, uint64Count := 0, 0, 0
	for _, expected := range expectedFields {
		message := descriptorMessage(expected.message)
		field := message.Fields().ByName(protoreflect.Name(expected.name))
		if field == nil { panic(fmt.Sprintf("missing field %s.%s", expected.message, expected.name)) }
		if int32(field.Number()) != expected.number || field.JSONName() != expected.jsonName || field.Kind().String() != expected.kind {
			panic(fmt.Sprintf("field identity drift for %s.%s", expected.message, expected.name))
		}
		if field.HasPresence() != expected.presence { panic(fmt.Sprintf("presence drift for %s.%s", expected.message, expected.name)) }
		oneof := ""
		if containing := field.ContainingOneof(); containing != nil { oneof = string(containing.Name()) }
		if oneof != expected.oneof { panic(fmt.Sprintf("oneof drift for %s.%s", expected.message, expected.name)) }
		fieldCount++
		if expected.kind == "bytes" { bytesCount++ }
		if expected.kind == "uint64" { uint64Count++ }
	}
	if fieldCount == 0 || bytesCount == 0 || uint64Count == 0 { panic("Rust authority did not provide required scalar coverage") }
	for _, expected := range expectedEnums {
		field := descriptorMessage(expected.message).Fields().ByName(protoreflect.Name(expected.field))
		if field == nil || field.Enum() == nil { panic(fmt.Sprintf("missing enum field %s.%s", expected.message, expected.field)) }
		for _, number := range expected.values {
			if field.Enum().Values().ByNumber(protoreflect.EnumNumber(number)) == nil { panic("enum value drift") }
		}
		if expected.repeated { continue }
		message := dynamicpb.NewMessage(field.ContainingMessage())
		message.Set(field, protoreflect.ValueOfEnum(protoreflect.EnumNumber(2147483000)))
		if got := message.Get(field).Enum(); got != protoreflect.EnumNumber(2147483000) { panic("unknown enum value was not preserved") }
	}
	for _, expected := range expectedRPCs {
		service := descriptorService(expected.service)
		method := service.Methods().ByName(protoreflect.Name(expected.method))
		if method == nil || method.IsStreamingClient() != expected.clientStream || method.IsStreamingServer() != expected.serverStream { panic(fmt.Sprintf("RPC stream signature drift for %s/%s", expected.service, expected.method)) }
	}
	checks := []string{"field-identities", "presence-oneof", "bytes", "uint64", "enums", "rpc-stream-signatures"}
	result := map[string]any{"schema": "acyclic.sdk.go.typed-consumer.v1", "source_revision": sourceRevision, "source_digest": sourceDigest, "assertions": checks}
	encoded, err := json.Marshal(result)
	if err != nil { panic(err) }
	fmt.Println(string(encoded))
}
`

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
		expected, _ := filepath.Abs(r.SourceRoot)
		if !samePath(expected, sourceRoot) {
			return fmt.Errorf("request source root %q does not match %q", r.SourceRoot, sourceRoot)
		}
	}
	if r.Output == "" {
		return errors.New("request output is required")
	}
	expectedOutput, err := filepath.Abs(r.Output)
	if err != nil {
		return fmt.Errorf("resolve request output: %w", err)
	}
	if !samePath(expectedOutput, outputRoot) {
		return fmt.Errorf("request output %q does not match %q", r.Output, outputRoot)
	}
	return nil
}

func resolveGoTool(wantGo string) (string, string, string, error) {
	path, err := exec.LookPath("go")
	if err != nil {
		return "", "", "", fmt.Errorf("go executable is not on PATH: %w", err)
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", "", "", fmt.Errorf("resolve Go executable path: %w", err)
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", "", "", fmt.Errorf("canonicalize Go executable path: %w", err)
	}
	versionBytes, err := exec.Command(path, "version").Output()
	if err != nil {
		return "", "", "", fmt.Errorf("go version: %w", err)
	}
	version := strings.TrimSpace(string(versionBytes))
	expected := wantGo
	if expected == "" {
		expected = runtime.Version()
	}
	if !strings.Contains(version, expected) {
		return "", "", "", fmt.Errorf("Go executable %s reports %q, expected %q", path, version, expected)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", "", "", fmt.Errorf("read Go executable for provenance: %w", err)
	}
	digest := sha256.Sum256(b)
	return path, version, "sha256:" + hex.EncodeToString(digest[:]), nil
}

func resolveTools(protoc, genGo, genGRPC, wantProtoc, wantGenGo, wantGenGRPC string) (string, string, string, error) {
	checks := []struct{ name, path, want string }{{"protoc", protoc, wantProtoc}, {"protoc-gen-go", genGo, wantGenGo}, {"protoc-gen-go-grpc", genGRPC, wantGenGRPC}}
	resolved := make([]string, len(checks))
	for i, c := range checks {
		path := c.path
		if found, err := exec.LookPath(c.path); err == nil {
			path = found
		} else {
			if _, statErr := os.Stat(c.path); statErr != nil {
				return "", "", "", fmt.Errorf("%s executable %q not found: %w", c.name, c.path, err)
			}
		}
		if c.want == "" {
			resolved[i] = path
			continue
		}
		out, err := exec.Command(path, "--version").CombinedOutput()
		if err != nil {
			return "", "", "", fmt.Errorf("%s --version: %w", c.name, err)
		}
		if !strings.Contains(string(out), c.want) {
			return "", "", "", fmt.Errorf("%s reports %q, expected %q", c.name, strings.TrimSpace(string(out)), c.want)
		}
		resolved[i] = path
	}
	return resolved[0], resolved[1], resolved[2], nil
}

func validateInput(root, relative, want string) error {
	clean := filepath.ToSlash(filepath.Clean(relative))
	if relative == "" || filepath.IsAbs(relative) || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("unsafe authority input %q", relative)
	}
	path := filepath.Join(root, filepath.FromSlash(relative))
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
	moduleRoot := filepath.Join(sourceRoot, "go")
	if err := os.MkdirAll(outputRoot, 0o755); err != nil {
		return fmt.Errorf("create Go module output directory: %w", err)
	}
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

func samePath(a, b string) bool { return strings.EqualFold(filepath.Clean(a), filepath.Clean(b)) }

package main

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

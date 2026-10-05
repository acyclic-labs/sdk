use crate::{
    CapabilityStatus, Language, RenderedSnippet, ScenarioMetadata, ValidationLevel,
    ValidationReceipt, ValidationStatus, filesystem_scenarios, harness_scenarios,
    inference_scenarios, machines_scenarios, objects_scenarios, workers_scenarios,
};
use sha2::{Digest, Sha256};

/// Whether a projection exercises a native Rust facade or the remote SDK.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuideProjectionMode {
    /// The Rust-owned embedded provider or native facade.
    Embedded,
    /// The generated remote client and its transport.
    Remote,
}

/// A Rust-owned guide scenario projected onto one generated language package.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuideProjection {
    pub scenario_id: &'static str,
    pub family: &'static str,
    pub source: &'static str,
    pub operation: &'static str,
    pub language: Language,
    pub mode: GuideProjectionMode,
    pub capability: CapabilityStatus,
    pub package: GuidePackageSpec,
    pub code: String,
}

/// Installable artifact identity used to compile a projection against the
/// package produced by the current generation revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuidePackageSpec {
    pub package_manager: &'static str,
    pub package_name: &'static str,
    pub artifact_path: &'static str,
}

/// Typed request identity consumed by package and documentation validators.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuideRemoteRequestSpec {
    pub scenario_id: &'static str,
    pub family: &'static str,
    pub operation: &'static str,
    pub service: &'static str,
    pub method: &'static str,
    pub request: &'static str,
}

/// The six guide families with a generated remote or native facade.
pub const GUIDE_PROJECTION_SCENARIOS: [(&str, &str, &str); 6] = [
    (
        filesystem_scenarios::SCENARIO_ID,
        "filesystem",
        filesystem_scenarios::SOURCE,
    ),
    (
        harness_scenarios::SCENARIO_ID,
        "harness",
        harness_scenarios::SOURCE,
    ),
    (
        inference_scenarios::SCENARIO_ID,
        "inference",
        inference_scenarios::SOURCE,
    ),
    (
        machines_scenarios::SCENARIO_ID,
        "machines",
        machines_scenarios::SOURCE,
    ),
    (
        objects_scenarios::SCENARIO_ID,
        "objects",
        objects_scenarios::SOURCE,
    ),
    (
        workers_scenarios::SCENARIO_ID,
        "workers",
        workers_scenarios::SOURCE,
    ),
];

/// One typed request manifest for each remote guide scenario.
pub const GUIDE_REMOTE_REQUESTS: [GuideRemoteRequestSpec; 6] = [
    GuideRemoteRequestSpec {
        scenario_id: filesystem_scenarios::SCENARIO_ID,
        family: "filesystem",
        operation: "acyclic.filesystem.v2.FilesystemService/Handshake",
        service: "FilesystemService",
        method: "Handshake",
        request: "HandshakeRequest",
    },
    GuideRemoteRequestSpec {
        scenario_id: harness_scenarios::SCENARIO_ID,
        family: "harness",
        operation: "acyclic.harness.v2.HarnessService/Submit",
        service: "HarnessService",
        method: "Submit",
        request: "CommandEnvelope",
    },
    GuideRemoteRequestSpec {
        scenario_id: inference_scenarios::SCENARIO_ID,
        family: "inference",
        operation: "inference.customer.v1.RunsService/Watch",
        service: "RunsService",
        method: "Watch",
        request: "WatchRunRequest",
    },
    GuideRemoteRequestSpec {
        scenario_id: machines_scenarios::SCENARIO_ID,
        family: "machines",
        operation: "acyclic.machines.v1.MachinesService/Create",
        service: "MachinesService",
        method: "Create",
        request: "CreateMachineRequest",
    },
    GuideRemoteRequestSpec {
        scenario_id: objects_scenarios::SCENARIO_ID,
        family: "objects",
        operation: "acyclic.objects.v2.ObjectsService/PutObject",
        service: "ObjectsService",
        method: "PutObject",
        request: "PutObjectRequest",
    },
    GuideRemoteRequestSpec {
        scenario_id: workers_scenarios::SCENARIO_ID,
        family: "workers",
        operation: "acyclic.workers.v1.WorkersService/PublishVersion",
        service: "WorkersService",
        method: "PublishVersion",
        request: "PublishVersionRequest",
    },
];

fn rust_body(scenario_id: &str) -> Option<String> {
    Some(match scenario_id {
        filesystem_scenarios::SCENARIO_ID => filesystem_scenarios::QUICKSTART_SNIPPET.to_owned(),
        harness_scenarios::SCENARIO_ID => harness_scenarios::QUICKSTART_SNIPPET.to_owned(),
        inference_scenarios::SCENARIO_ID => inference_scenarios::rust_snippet().to_owned(),
        machines_scenarios::SCENARIO_ID => machines_scenarios::rust_snippet().to_owned(),
        objects_scenarios::SCENARIO_ID => objects_scenarios::rust_snippet().to_owned(),
        workers_scenarios::SCENARIO_ID => workers_scenarios::rust_snippet().to_owned(),
        _ => return None,
    })
}

fn package_module(family: &str) -> Option<(&'static str, &'static str)> {
    Some(match family {
        "filesystem" => ("filesystem", "v2"),
        "harness" => ("harness", "v2"),
        "inference" => ("inference", "v1"),
        "machines" => ("machines", "v1"),
        "objects" => ("objects", "v2"),
        "workers" => ("workers", "v1"),
        _ => return None,
    })
}

fn remote_operation(
    family: &str,
) -> Option<(&'static str, &'static str, &'static str, &'static str)> {
    Some(match family {
        "filesystem" => (
            "FilesystemService",
            "Handshake",
            "HandshakeRequest",
            "acyclic.filesystem.v2.FilesystemService/Handshake",
        ),
        "harness" => (
            "HarnessService",
            "Submit",
            "CommandEnvelope",
            "acyclic.harness.v2.HarnessService/Submit",
        ),
        "inference" => (
            "RunsService",
            "Watch",
            "WatchRunRequest",
            "inference.customer.v1.RunsService/Watch",
        ),
        "machines" => (
            "MachinesService",
            "Create",
            "CreateMachineRequest",
            "acyclic.machines.v1.MachinesService/Create",
        ),
        "objects" => (
            "ObjectsService",
            "PutObject",
            "PutObjectRequest",
            "acyclic.objects.v2.ObjectsService/PutObject",
        ),
        "workers" => (
            "WorkersService",
            "PublishVersion",
            "PublishVersionRequest",
            "acyclic.workers.v1.WorkersService/PublishVersion",
        ),
        _ => return None,
    })
}

fn package_type(module: &str) -> Option<&'static str> {
    Some(match module {
        "filesystem" => "Filesystem",
        "harness" => "Harness",
        "inference" => "Inference",
        "machines" => "Machines",
        "objects" => "Objects",
        "workers" => "Workers",
        _ => return None,
    })
}

fn java_service_type(module: &str) -> Option<&'static str> {
    Some(match module {
        // The inference proto keeps its messages in Inference.java, while
        // the Runs RPC service is emitted as RunsServiceGrpc.java.
        "inference" => "Runs",
        _ => package_type(module)?,
    })
}

fn java_namespace(module: &str, version: &str) -> String {
    if module == "inference" {
        format!("inference.customer.{version}")
    } else {
        format!("acyclic.{module}.{version}")
    }
}

fn csharp_namespace(module: &str, version: &str) -> String {
    if module == "inference" {
        format!("Inference.Customer.{}", version_type(version).unwrap_or("V1"))
    } else {
        format!("Acyclic.{}.{}", package_type(module).unwrap_or(module), version_type(version).unwrap_or("V1"))
    }
}

fn package_spec(family: &str, language: Language) -> Option<GuidePackageSpec> {
    let package = match language {
        Language::Rust => GuidePackageSpec {
            package_manager: "cargo",
            package_name: match family {
                "filesystem" => "acyclic-fs",
                "harness" => "acyclic-harness",
                "inference" => "acyclic-inference",
                "machines" => "acyclic-machines",
                "objects" => "acyclic-objects",
                "workers" => "acyclic-workers",
                _ => return None,
            },
            artifact_path: match family {
                "filesystem" => "rust/crates/filesystem/Cargo.toml",
                "harness" => "rust/crates/harness/Cargo.toml",
                "inference" => "rust/crates/inference/Cargo.toml",
                "machines" => "rust/crates/machines/Cargo.toml",
                "objects" => "rust/crates/objects/Cargo.toml",
                "workers" => "rust/crates/workers/Cargo.toml",
                _ => return None,
            },
        },
        Language::Python => GuidePackageSpec {
            package_manager: "pip",
            package_name: "acyclic-sdk-transport",
            artifact_path: "python/dist/*.whl",
        },
        Language::TypeScript => GuidePackageSpec {
            package_manager: "npm",
            package_name: match family {
                "filesystem" => "@acyclic-labs/fs",
                "harness" => "@acyclic-labs/harness",
                "inference" => "@acyclic-labs/inference",
                "machines" => "@acyclic-labs/machines",
                "objects" => "@acyclic-labs/objects",
                "workers" => "@acyclic-labs/workers",
                _ => return None,
            },
            artifact_path: match family {
                "filesystem" => "typescript/packages/filesystem/package.json",
                "harness" => "typescript/packages/harness/package.json",
                "inference" => "typescript/packages/inference/package.json",
                "machines" => "typescript/packages/machines/package.json",
                "objects" => "typescript/packages/objects/package.json",
                "workers" => "typescript/packages/workers/package.json",
                _ => return None,
            },
        },
        Language::Go => GuidePackageSpec {
            package_manager: "go",
            package_name: "github.com/acyclic-labs/sdk/go",
            artifact_path: "go/go.mod",
        },
        Language::Java => GuidePackageSpec {
            package_manager: "maven",
            package_name: "dev.acyclic:acyclic-sdk-jvm-transport",
            artifact_path: "jvm/target/acyclic-sdk-jvm-transport-*.jar",
        },
        Language::CSharp => GuidePackageSpec {
            package_manager: "nuget",
            package_name: "Acyclic.Sdk.Transport",
            artifact_path: "dotnet/bin/**/Acyclic.Sdk.Transport.dll",
        },
        Language::Ruby => GuidePackageSpec {
            package_manager: "bundler",
            package_name: "acyclic-sdk",
            artifact_path: "ruby/acyclic-sdk.gemspec",
        },
        Language::Dart => GuidePackageSpec {
            package_manager: "pub",
            package_name: "acyclic_sdk",
            artifact_path: "dart/pubspec.yaml",
        },
        Language::Php => GuidePackageSpec {
            package_manager: "composer",
            package_name: "acyclic/sdk-transport",
            artifact_path: "php/composer.json",
        },
    };
    Some(package)
}

fn version_type(version: &str) -> Option<&'static str> {
    Some(match version {
        "v1" => "V1",
        "v2" => "V2",
        _ => return None,
    })
}

fn snake_case(value: &str) -> String {
    value
        .chars()
        .enumerate()
        .map(|(index, ch)| {
            if ch.is_ascii_uppercase() && index > 0 {
                format!("_{}", ch.to_ascii_lowercase())
            } else {
                ch.to_ascii_lowercase().to_string()
            }
        })
        .collect()
}

fn lower_camel(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_ascii_lowercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

fn workers_module_text() -> String {
    String::from_utf8_lossy(workers_scenarios::MODULE).into_owned()
}

fn workers_module_digest() -> String {
    format!("{:x}", Sha256::digest(workers_scenarios::MODULE))
}

/// Projects the Rust scenario as an actual generated remote SDK call.
///
/// The request values are the same stable identities used by the Rust fixture.
/// Embedded behavior remains in the Rust projection, which calls the native
/// provider APIs in the source scenario.
pub fn project(scenario_id: &'static str, language: Language) -> Option<GuideProjection> {
    let (_, family, source) = GUIDE_PROJECTION_SCENARIOS
        .iter()
        .find(|(id, _, _)| *id == scenario_id)?;
    let family = *family;
    let source = *source;
    let (module, version) = package_module(family)?;
    let (service, method, request, operation) = remote_operation(family)?;
    let package_type = package_type(module)?;
    let java_service_type = java_service_type(module)?;
    let java_namespace = java_namespace(module, version);
    let csharp_namespace = csharp_namespace(module, version);
    let version_type = version_type(version)?;
    let package = package_spec(family, language)?;
    let harness_protocol = acyclic_harness::wire_api::current_protocol();
    let harness_protocol_version = harness_protocol.version.as_str();
    let harness_protocol_digest = harness_protocol.descriptor_digest.as_str();
    let method_camel = lower_camel(method);
    let method_snake = snake_case(method);
    let ts_package = if family == "filesystem" { "fs" } else { module };
    let ts_call = if family == "workers" {
        format!(
            "const moduleBytes = new TextEncoder().encode({module:?});\nconst expectedSha256 = new Uint8Array(Buffer.from(\"{digest}\", \"hex\"));\nconst response = await client.publishVersion(create({request}Schema, {{ javascriptModule: moduleBytes, expectedSha256, idempotencyKey: \"publish-example-v1\" }}));",
            module = workers_module_text(),
            digest = workers_module_digest(),
            request = request,
        )
    } else if family == "objects" {
        format!(
            "const response = await client.putObject((async function* () {{ yield create({request}Schema, {{ frame: {{ case: \"header\", value: {{ bucket: {{ name: \"default\" }}, objectKey: \"hello.txt\" }} }} }}); yield create({request}Schema, {{ frame: {{ case: \"body\", value: new TextEncoder().encode(\"hello\") }} }}); yield create({request}Schema, {{ frame: {{ case: \"complete\", value: true }} }}); }})());",
            request = request,
        )
    } else if family == "inference" {
        format!(
            "const response = await client.{method}(create({request}Schema, {{ runId: new Uint8Array(16).fill(2), fromSequence: 0n }}));",
            method = method_camel,
            request = request,
        )
    } else {
        format!(
            "const response = await client.{method}(create({request}Schema, {{}}));",
            method = method_camel,
            request = request,
        )
    };
    let code = match language {
        Language::Rust => rust_body(scenario_id)?,
        Language::Python => {
            let call = if family == "workers" {
                format!(
                    "module = {module:?}\nrequest = {module_name}_pb2.{request}(javascript_module=module.encode(), expected_sha256=hashlib.sha256(module.encode()).digest(), idempotency_key=\"publish-example-v1\")\nresponse = client.{method}(request)",
                    module = workers_module_text(),
                    module_name = module,
                    request = request,
                    method = method,
                )
            } else if family == "objects" {
                format!(
                    "response = client.{method}(iter([{module}_pb2.{request}(header={module}_pb2.PutObjectHeader(bucket={module}_pb2.BucketRef(name=\"default\"), object_key=\"hello.txt\")), {module}_pb2.{request}(body=b\"hello\"), {module}_pb2.{request}(complete=True)]))",
                    method = method,
                    module = module,
                    request = request,
                )
            } else if family == "inference" {
                format!(
                    "request = {module}_pb2.{request}(run_id=bytes([2] * 16), from_sequence=0)\nresponse = client.{method}(request)",
                    module = module,
                    request = request,
                    method = method,
                )
            } else if family == "harness" {
                format!(
                    "from acyclic_sdk.generated.protocol.v1 import protocol_pb2\nrequest = {module}_pb2.{request}(protocol=protocol_pb2.ProtocolIdentity(version={version:?}, descriptor_digest={digest:?}), authority={module}_pb2.Authority(kind=5, id=\"fixture\"), operation={module}_pb2.OperationIdentity(operation_id=\"fixture-op\", idempotency_key=\"guide-harness\"), action_type=\"guide.submit\")\nresponse = client.{method}(request)",
                    module = module,
                    request = request,
                    version = harness_protocol_version,
                    digest = harness_protocol_digest,
                    method = method,
                )
            } else if family == "machines" {
                format!(
                    "request = {module}_pb2.{request}(protocol={module}_pb2.ProtocolVersion(major=1, minor=1), idempotency_key={module}_pb2.IdempotencyKey(value=bytes([1] * 16)))\nresponse = client.{method}(request)",
                    module = module,
                    request = request,
                    method = method,
                )
            } else {
                format!(
                    "request = {module}_pb2.{request}()\nresponse = client.{method}(request)",
                    module = module,
                    request = request,
                    method = method,
                )
            };
            format!(
                r#"# Rust scenario: {scenario_id}
import hashlib
import os
import grpc
from acyclic_sdk.generated.{module}.{version} import {module}_pb2, {module}_pb2_grpc

channel = grpc.insecure_channel(os.environ["FIXTURE_GRPC_ADDRESS"])
client = {module}_pb2_grpc.{service}Stub(channel)
{call}
print(response)"#,
                scenario_id = scenario_id,
                module = module,
                version = version,
                service = service,
                call = call,
            )
        }
        Language::TypeScript => {
            let harness_import = if family == "harness" {
                "import { ProtocolIdentitySchema } from \"@acyclic-labs/harness/protocol\";\n"
            } else {
                ""
            };
            let machines_import = if family == "machines" {
                "import { IdempotencyKeySchema } from \"@acyclic-labs/machines/proto\";\n"
            } else {
                ""
            };
            let harness_request = if family == "harness" {
                format!(
                    "create({request}Schema, {{ protocol: create(ProtocolIdentitySchema, {{ version: {version:?}, descriptorDigest: {digest:?} }}), authority: {{ kind: 5, id: \"fixture\" }}, operation: {{ operationId: \"fixture-op\", idempotencyKey: \"guide-harness\" }}, actionType: \"guide.submit\" }})",
                    request = request,
                    version = harness_protocol_version,
                    digest = harness_protocol_digest,
                )
            } else {
                format!("create({request}Schema, {{}})", request = request)
            };
            let ts_call = if family == "harness" {
                format!("const response = await client.{method}({harness_request});", method = method_camel, harness_request = harness_request)
            } else if family == "machines" {
                format!(
                    "const response = await client.{method}(create({request}Schema, {{ protocol: {{ major: 1, minor: 1 }}, idempotencyKey: create(IdempotencyKeySchema, {{ value: new Uint8Array(16).fill(1) }}) }}));",
                    method = method_camel,
                    request = request,
                )
            } else {
                ts_call
            };
            format!(
            r#"// Rust scenario: {scenario_id}
import {{ create }} from "@bufbuild/protobuf";
import {{ createClient }} from "@connectrpc/connect";
import {{ createGrpcTransport }} from "@connectrpc/connect-node";
import {{ Buffer }} from "node:buffer";
import {{ {service}, {request}Schema }} from "@acyclic-labs/{ts_package}/proto";
{harness_import}
{machines_import}

const transport = createGrpcTransport({{ baseUrl: process.env.FIXTURE_GRPC_ADDRESS! }});
const client = createClient({service}, transport);
{ts_call}
console.log(response);"#,
            scenario_id = scenario_id,
            service = service,
            request = request,
            ts_package = ts_package,
            harness_import = harness_import,
            machines_import = machines_import,
            ts_call = ts_call,
            )
        }
        Language::Go => {
            let call = if family == "workers" {
                format!(
                    "module := []byte({module:?})\nchecksum := sha256.Sum256(module)\nresponse, err := client.{method}(ctx, &generated.{request}{{JavascriptModule: module, ExpectedSha256: checksum[:], IdempotencyKey: \"publish-example-v1\"}})",
                    module = workers_module_text(),
                    method = method,
                    request = request,
                )
            } else if family == "objects" {
                format!(
                    r#"stream, err := client.PutObject(ctx)
    if err != nil {{ panic(err) }}
     if err := stream.Send(&generated.PutObjectRequest{{Frame: &generated.PutObjectRequest_Header{{Header: &generated.PutObjectHeader{{Bucket: &generated.BucketRef{{Name: "default"}}, ObjectKey: "hello.txt"}}}}}}); err != nil {{ panic(err) }}
     if err := stream.Send(&generated.PutObjectRequest{{Frame: &generated.PutObjectRequest_Body{{Body: []byte("hello")}}}}); err != nil {{ panic(err) }}
     if err := stream.Send(&generated.PutObjectRequest{{Frame: &generated.PutObjectRequest_Complete{{Complete: true}}}}); err != nil {{ panic(err) }}
    response, err := stream.CloseAndRecv()"#
                )
            } else if family == "inference" {
                format!(
                    "response, err := client.{method}(ctx, &generated.{request}{{RunId: []byte{{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}}, FromSequence: 0}})",
                    method = method,
                    request = request,
                )
            } else if family == "harness" {
                format!(
                    "response, err := client.{method}(ctx, &generated.{request}{{Protocol: &protocol.ProtocolIdentity{{Version: {version:?}, DescriptorDigest: {digest:?}}}, Authority: &generated.Authority{{Kind: generated.AggregateKind_AGGREGATE_KIND_TASK, Id: \"fixture\"}}, Operation: &generated.OperationIdentity{{OperationId: \"fixture-op\", IdempotencyKey: \"guide-harness\"}}, ActionType: \"guide.submit\"}})",
                    method = method,
                    request = request,
                    version = harness_protocol_version,
                    digest = harness_protocol_digest,
                )
            } else if family == "machines" {
                format!(
                    "response, err := client.{method}(ctx, &generated.{request}{{Protocol: &generated.ProtocolVersion{{Major: 1, Minor: 1}}, IdempotencyKey: &generated.IdempotencyKey{{Value: []byte{{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}}}}}})",
                    method = method,
                    request = request,
                )
            } else {
                format!(
                    "response, err := client.{method}(ctx, &generated.{request}{{}})",
                    method = method,
                    request = request
                )
            };
            format!(
                r#"// Rust scenario: {scenario_id}
package main

import (
    "context"
{go_crypto_import}
    "fmt"
    "os"
    generated "github.com/acyclic-labs/sdk/go/gen/{module}/{version}"
{go_protocol_import}
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func main() {{
    endpoint := os.Getenv("FIXTURE_GRPC_ADDRESS")
    conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {{ panic(err) }}
    defer conn.Close()
    ctx := context.Background()
    client := generated.New{service}Client(conn)
    {call}
    if err != nil {{ panic(err) }}
    fmt.Println(response)
}}"#,
                scenario_id = scenario_id,
                module = module,
                version = version,
                service = service,
                go_crypto_import = if family == "workers" {
                    "    \"crypto/sha256\"\n"
                } else {
                    ""
                },
                go_protocol_import = if family == "harness" {
                    "    protocol \"github.com/acyclic-labs/sdk/go/gen/protocol/v1\""
                } else {
                    ""
                },
                call = call,
            )
        }
        Language::Java => {
            let call = if family == "workers" {
                format!(
                    "      var module = com.google.protobuf.ByteString.copyFromUtf8({module:?});\n      var expectedSha256 = java.security.MessageDigest.getInstance(\"SHA-256\").digest(module.toByteArray());\n      var request = {package_type}.{request}.newBuilder().setJavascriptModule(module).setExpectedSha256(com.google.protobuf.ByteString.copyFrom(expectedSha256)).setIdempotencyKey(\"publish-example-v1\").build();\n      var response = {java_service_type}ServiceGrpc.newBlockingStub(channel).{method_camel}(request);\n      System.out.println(response);",
                    package_type = package_type,
                    module = workers_module_text(),
                    request = request,
                    java_service_type = java_service_type,
                    method_camel = method_camel,
                )
            } else if family == "objects" {
                format!(
                    r#"      var result = new java.util.concurrent.CompletableFuture<{package_type}.ObjectInfo>();
      var requestObserver = {java_service_type}ServiceGrpc.newStub(channel).putObject(new io.grpc.stub.StreamObserver<{package_type}.ObjectInfo>() {{
        public void onNext({package_type}.ObjectInfo value) {{ result.complete(value); }}
        public void onError(Throwable error) {{ result.completeExceptionally(error); }}
        public void onCompleted() {{ }}
      }});
       requestObserver.onNext({package_type}.PutObjectRequest.newBuilder().setHeader({package_type}.PutObjectHeader.newBuilder().setBucket({package_type}.BucketRef.newBuilder().setName("default")).setObjectKey("hello.txt")).build());
       requestObserver.onNext({package_type}.PutObjectRequest.newBuilder().setBody(com.google.protobuf.ByteString.copyFromUtf8("hello")).build());
      requestObserver.onNext({package_type}.PutObjectRequest.newBuilder().setComplete(true).build());
      requestObserver.onCompleted();
      System.out.println(result.join());"#,
                    package_type = package_type,
                    java_service_type = java_service_type,
                )
            } else if family == "inference" {
                format!(
                    "      var runId = new byte[16];\n      java.util.Arrays.fill(runId, (byte) 2);\n      var request = {package_type}.{request}.newBuilder().setRunId(com.google.protobuf.ByteString.copyFrom(runId)).setFromSequence(0L).build();\n      var response = {java_service_type}ServiceGrpc.newBlockingStub(channel).{method_camel}(request);\n      System.out.println(response);",
                    package_type = package_type,
                    request = request,
                    java_service_type = java_service_type,
                    method_camel = method_camel,
                )
            } else if family == "harness" {
                format!(
                    "      var request = {package_type}.{request}.newBuilder().setProtocol(acyclic.protocol.v1.Protocol.ProtocolIdentity.newBuilder().setVersion({version:?}).setDescriptorDigest({digest:?})).setAuthority({package_type}.Authority.newBuilder().setKind({package_type}.AggregateKind.AGGREGATE_KIND_TASK).setId(\"fixture\")).setOperation({package_type}.OperationIdentity.newBuilder().setOperationId(\"fixture-op\").setIdempotencyKey(\"guide-harness\")).setActionType(\"guide.submit\").build();\n      var response = {java_service_type}ServiceGrpc.newBlockingStub(channel).{method_camel}(request);\n      System.out.println(response);",
                    package_type = package_type,
                    request = request,
                    version = harness_protocol_version,
                    digest = harness_protocol_digest,
                    java_service_type = java_service_type,
                    method_camel = method_camel,
                )
            } else if family == "machines" {
                format!(
                    "      var key = new byte[16];\n      java.util.Arrays.fill(key, (byte) 1);\n      var request = {package_type}.{request}.newBuilder().setProtocol({package_type}.ProtocolVersion.newBuilder().setMajor(1).setMinor(1)).setIdempotencyKey({package_type}.IdempotencyKey.newBuilder().setValue(com.google.protobuf.ByteString.copyFrom(key))).build();\n      var response = {java_service_type}ServiceGrpc.newBlockingStub(channel).{method_camel}(request);\n      System.out.println(response);",
                    package_type = package_type,
                    request = request,
                    java_service_type = java_service_type,
                    method_camel = method_camel,
                )
            } else {
                format!(
                    "      var request = {package_type}.{request}.newBuilder().build();\n      var response = {java_service_type}ServiceGrpc.newBlockingStub(channel).{method_camel}(request);\n      System.out.println(response);",
                    package_type = package_type,
                    java_service_type = java_service_type,
                    request = request,
                    method_camel = method_camel,
                )
            };
            format!(
                r#"// Rust scenario: {scenario_id}
import io.grpc.ManagedChannelBuilder;
import {java_namespace}.{package_type};
import {java_namespace}.{java_service_type}ServiceGrpc;

public final class GuideSnippet {{
public static void main(String[] args) throws Exception {{
    var endpoint = System.getenv("FIXTURE_GRPC_ADDRESS");
    var channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
    try {{
{call}
    }} finally {{
      channel.shutdownNow();
    }}
  }}
}}"#,
                scenario_id = scenario_id,
                java_namespace = java_namespace,
                package_type = package_type,
                java_service_type = java_service_type,
                call = call,
            )
        }
        Language::CSharp => {
            let call = if family == "workers" {
                format!(
                    "var moduleBytes = ByteString.CopyFromUtf8({module:?});\nvar expectedSha256 = ByteString.CopyFrom(SHA256.HashData(moduleBytes.ToByteArray()));\nvar response = client.{method}(new {request} {{ JavascriptModule = moduleBytes, ExpectedSha256 = expectedSha256, IdempotencyKey = \"publish-example-v1\" }});",
                    module = workers_module_text(),
                    method = method,
                    request = request,
                )
            } else if family == "objects" {
                format!(
                    r#"using var call = client.PutObject();
     await call.RequestStream.WriteAsync(new {request} {{ Header = new PutObjectHeader {{ Bucket = new BucketRef {{ Name = "default" }}, ObjectKey = "hello.txt" }} }});
     await call.RequestStream.WriteAsync(new {request} {{ Body = ByteString.CopyFromUtf8("hello") }});
    await call.RequestStream.WriteAsync(new {request} {{ Complete = true }});
    await call.RequestStream.CompleteAsync();
    var response = await call.ResponseAsync;"#,
                    request = request,
                )
            } else if family == "inference" {
                format!(
                    "var response = client.{method}(new {request} {{ RunId = ByteString.CopyFrom(Enumerable.Repeat((byte)2, 16).ToArray()), FromSequence = 0 }});",
                    method = method,
                    request = request,
                )
            } else if family == "harness" {
                format!(
                    "var response = client.{method}(new {request} {{ Protocol = new Acyclic.Protocol.V1.ProtocolIdentity {{ Version = {version:?}, DescriptorDigest = {digest:?} }}, Authority = new Authority {{ Kind = AggregateKind.Task, Id = \"fixture\" }}, Operation = new OperationIdentity {{ OperationId = \"fixture-op\", IdempotencyKey = \"guide-harness\" }}, ActionType = \"guide.submit\" }});",
                    method = method,
                    request = request,
                    version = harness_protocol_version,
                    digest = harness_protocol_digest,
                )
            } else if family == "machines" {
                format!(
                    "var response = client.{method}(new {request} {{ Protocol = new ProtocolVersion {{ Major = 1, Minor = 1 }}, IdempotencyKey = new IdempotencyKey {{ Value = ByteString.CopyFrom(Enumerable.Repeat((byte)1, 16).ToArray()) }} }});",
                    method = method,
                    request = request,
                )
            } else {
                format!(
                    "var response = client.{method}(new {request}());",
                    method = method,
                    request = request,
                )
            };
            format!(
                r#"// Rust scenario: {scenario_id}
using Grpc.Net.Client;
using Google.Protobuf;
using System.Security.Cryptography;
using {csharp_namespace};

var endpoint = Environment.GetEnvironmentVariable("FIXTURE_GRPC_ADDRESS")
    ?? throw new InvalidOperationException("FIXTURE_GRPC_ADDRESS is required");
using var channel = GrpcChannel.ForAddress(endpoint);
var client = new {service}.{service}Client(channel);
{call}
Console.WriteLine(response);"#,
                scenario_id = scenario_id,
                csharp_namespace = csharp_namespace,
                service = service,
                call = call,
            )
        }
        Language::Ruby => {
            let call = if family == "workers" {
                format!(
                    "$module = {module:?}\n$request = Acyclic::{package_type}::{version_type}::{request}.new(javascript_module: $module, expected_sha256: Digest::SHA256.digest($module), idempotency_key: \"publish-example-v1\")\n$response = client.{method_snake}($request)",
                    module = workers_module_text(),
                    package_type = package_type,
                    version_type = version_type,
                    request = request,
                    method_snake = method_snake,
                )
            } else if family == "objects" {
                format!(
                    r#"response = client.{method_snake}([
  Acyclic::{package_type}::{version_type}::{request}.new(header: Acyclic::{package_type}::{version_type}::PutObjectHeader.new(bucket: Acyclic::{package_type}::{version_type}::BucketRef.new(name: "default"), object_key: "hello.txt")),
  Acyclic::{package_type}::{version_type}::{request}.new(body: "hello"),
  Acyclic::{package_type}::{version_type}::{request}.new(complete: true)
])"#,
                    method_snake = method_snake,
                    package_type = package_type,
                    version_type = version_type,
                    request = request,
                )
            } else if family == "inference" {
                format!(
                    "request = Acyclic::{package_type}::{version_type}::{request}.new(run_id: ([2] * 16).pack(\"C*\"), from_sequence: 0)\nresponse = client.{method_snake}(request)",
                    package_type = package_type,
                    version_type = version_type,
                    request = request,
                    method_snake = method_snake,
                )
            } else if family == "harness" {
                format!(
                    "request = Acyclic::{package_type}::{version_type}::{request}.new(protocol: Acyclic::Protocol::V1::ProtocolIdentity.new(version: {version:?}, descriptor_digest: {digest:?}), authority: Acyclic::{package_type}::{version_type}::Authority.new(kind: Acyclic::{package_type}::{version_type}::AggregateKind::AGGREGATE_KIND_TASK, id: \"fixture\"), operation: Acyclic::{package_type}::{version_type}::OperationIdentity.new(operation_id: \"fixture-op\", idempotency_key: \"guide-harness\"), action_type: \"guide.submit\")\nresponse = client.{method_snake}(request)",
                    package_type = package_type,
                    version_type = version_type,
                    request = request,
                    version = harness_protocol_version,
                    digest = harness_protocol_digest,
                    method_snake = method_snake,
                )
            } else if family == "machines" {
                format!(
                    "request = Acyclic::{package_type}::{version_type}::{request}.new(protocol: Acyclic::{package_type}::{version_type}::ProtocolVersion.new(major: 1, minor: 1), idempotency_key: Acyclic::{package_type}::{version_type}::IdempotencyKey.new(value: ([1] * 16).pack(\"C*\")))\nresponse = client.{method_snake}(request)",
                    package_type = package_type,
                    version_type = version_type,
                    request = request,
                    method_snake = method_snake,
                )
            } else {
                format!(
                    "request = Acyclic::{package_type}::{version_type}::{request}.new\nresponse = client.{method_snake}(request)",
                    package_type = package_type,
                    version_type = version_type,
                    request = request,
                    method_snake = method_snake,
                )
            };
            format!(
                r#"# Rust scenario: {scenario_id}
require "acyclic_sdk"
require "digest"

endpoint = ENV.fetch("FIXTURE_GRPC_ADDRESS")
client = Acyclic::{package_type}::{version_type}::{service}::Stub.new(endpoint, :this_channel_is_insecure)
{call}
puts response"#,
                scenario_id = scenario_id,
                package_type = package_type,
                version_type = version_type,
                service = service,
                call = call,
            )
        }
        Language::Dart => {
            let dart_imports = format!(
                "import 'dart:io';\n{}{}{}import 'package:grpc/grpc.dart';",
                if matches!(family, "workers" | "objects") {
                    "import 'dart:convert';\n"
                } else {
                    ""
                },
                if matches!(family, "workers" | "inference" | "machines") {
                    "import 'dart:typed_data';\n"
                } else {
                    ""
                },
                if family == "workers" {
                    "import 'package:crypto/crypto.dart';\n"
                } else {
                    ""
                },
            );
            let call = if family == "workers" {
                format!(
                    "final moduleBytes = Uint8List.fromList(utf8.encode({module:?}));\nfinal expectedSha256 = Uint8List.fromList(sha256.convert(moduleBytes).bytes);\nfinal response = await client.{method_camel}(generated.{request}()..javascriptModule = moduleBytes..expectedSha256 = expectedSha256..idempotencyKey = 'publish-example-v1');",
                    module = workers_module_text(),
                    method_camel = method_camel,
                    request = request,
                )
            } else if family == "objects" {
                format!(
                    r#"final response = await client.{method_camel}(Stream.fromIterable([
    generated.{request}()..header = (generated.PutObjectHeader()..bucket = (generated.BucketRef()..name = "default")..objectKey = "hello.txt"),
    generated.{request}()..body = utf8.encode("hello"),
    generated.{request}()..complete = true,
  ]));"#,
                    method_camel = method_camel,
                    request = request,
                )
            } else if family == "inference" {
                format!(
                    "final response = await client.{method_camel}(generated.{request}()..runId = (Uint8List(16)..fillRange(0, 16, 2))..fromSequence = 0);",
                    method_camel = method_camel,
                    request = request,
                )
            } else if family == "harness" {
                format!(
                    "final response = await client.{method_camel}(generated.{request}()..protocol = (protocol.ProtocolIdentity()..version = {version:?}..descriptorDigest = {digest:?})..authority = (generated.Authority()..kind = generated.AggregateKind.AGGREGATE_KIND_TASK..id = 'fixture')..operation = (generated.OperationIdentity()..operationId = 'fixture-op'..idempotencyKey = 'guide-harness')..actionType = 'guide.submit');",
                    method_camel = method_camel,
                    request = request,
                    version = harness_protocol_version,
                    digest = harness_protocol_digest,
                )
            } else if family == "machines" {
                format!(
                    "final response = await client.{method_camel}(generated.{request}()..protocol = (generated.ProtocolVersion()..major = 1..minor = 1)..idempotencyKey = (generated.IdempotencyKey()..value = (Uint8List(16)..fillRange(0, 16, 1))));",
                    method_camel = method_camel,
                    request = request,
                )
            } else {
                format!(
                    "final response = await client.{method_camel}(generated.{request}());",
                    method_camel = method_camel,
                    request = request,
                )
            };
            format!(
                r#"// Rust scenario: {scenario_id}
{dart_imports}
import 'package:acyclic_sdk/src/generated/{module}/{version}/{module}.pb.dart' as generated;
import 'package:acyclic_sdk/src/generated/{module}/{version}/{module}.pbgrpc.dart' as rpc;
{dart_protocol_import}

Future<void> main() async {{
  final endpoint = Platform.environment['FIXTURE_GRPC_ADDRESS']!;
  final parts = endpoint.split(':');
  final channel = ClientChannel(parts[0], port: int.parse(parts[1]),
      options: const ChannelOptions(credentials: ChannelCredentials.insecure()));
  try {{
    final client = rpc.{service}Client(channel);
    {call}
    print(response);
  }} finally {{
    await channel.shutdown();
  }}
}}"#,
                scenario_id = scenario_id,
                module = module,
                version = version,
                service = service,
                dart_imports = dart_imports,
                dart_protocol_import = if family == "harness" {
                    "import 'package:acyclic_sdk/src/generated/protocol/v1/protocol.pb.dart' as protocol;"
                } else {
                    ""
                },
                call = call,
            )
        }
        Language::Php => {
            let call = if family == "workers" {
                format!(
                    "$request = new \\Acyclic\\{package_type}\\{version}\\{request}(['javascript_module' => {module:?}, 'expected_sha256' => hash('sha256', {module:?}, true), 'idempotency_key' => 'publish-example-v1']);\n[$response, $status] = $client->{method}($request)->wait();",
                    package_type = package_type,
                    version = version,
                    request = request,
                    module = workers_module_text(),
                    method = method,
                )
            } else if family == "objects" {
                format!(
                    r#"$call = $client->{method}();
$call->write(new \Acyclic\{package_type}\{version}\{request}(['header' => new \Acyclic\{package_type}\{version}\PutObjectHeader(['bucket' => new \Acyclic\{package_type}\{version}\BucketRef(['name' => 'guide']), 'object_key' => 'hello.txt'])]));
$call->write(new \Acyclic\{package_type}\{version}\{request}(['body' => 'hello']));
$call->write(new \Acyclic\{package_type}\{version}\{request}(['complete' => true]));
$call->writesDone();
[$response, $status] = $call->wait();"#,
                    method = method,
                    package_type = package_type,
                    version = version,
                    request = request,
                )
            } else if family == "inference" {
                format!(
                    r#"$request = new \Acyclic\{package_type}\{version}\{request}(['run_id' => str_repeat(chr(2), 16), 'from_sequence' => 0]);
[$response, $status] = $client->{method}($request)->wait();"#,
                    package_type = package_type,
                    version = version,
                    request = request,
                    method = method,
                )
            } else if family == "harness" {
                format!(
                    r#"$request = new \Acyclic\{package_type}\{version}\{request}([
    'protocol' => new \Acyclic\Protocol\V1\ProtocolIdentity(['version' => {protocol_version:?}, 'descriptor_digest' => {protocol_digest:?}]),
    'authority' => new \Acyclic\Harness\V2\Authority(['kind' => \Acyclic\Harness\V2\AggregateKind::AGGREGATE_KIND_TASK, 'id' => 'fixture']),
    'operation' => new \Acyclic\Harness\V2\OperationIdentity(['operation_id' => 'fixture-op', 'idempotency_key' => 'guide-harness']),
    'action_type' => 'guide.submit',
]);
[$response, $status] = $client->{method}($request)->wait();"#,
                    package_type = package_type,
                    version = version,
                    request = request,
                    protocol_version = harness_protocol_version,
                    protocol_digest = harness_protocol_digest,
                    method = method,
                )
            } else if family == "machines" {
                format!(
                    r#"$request = new \\Acyclic\{package_type}\{version}\{request}([
    'protocol' => new \\Acyclic\{package_type}\{version}\ProtocolVersion(['major' => 1, 'minor' => 1]),
    'idempotency_key' => new \\Acyclic\{package_type}\{version}\IdempotencyKey(['value' => str_repeat(chr(1), 16)]),
]);
[$response, $status] = $client->{method}($request)->wait();"#,
                    package_type = package_type,
                    version = version,
                    request = request,
                    method = method,
                )
            } else {
                format!(
                    r#"$request = new \Acyclic\{package_type}\{version}\{request}();
[$response, $status] = $client->{method}($request)->wait();"#,
                    package_type = package_type,
                    version = version,
                    request = request,
                    method = method,
                )
            };
            format!(
                r#"<?php
// Rust scenario: {scenario_id}
require dirname(__DIR__) . '/vendor/autoload.php';

$endpoint = getenv('FIXTURE_GRPC_ADDRESS');
$client = new \Acyclic\{package_type}\{version}\{service}Client(
    $endpoint,
    ['credentials' => \Grpc\ChannelCredentials::createInsecure()]
);
{call}
if ($status->code !== \Grpc\STATUS_OK) throw new RuntimeException($status->details);
echo $response->serializeToJsonString(), PHP_EOL;"#,
                scenario_id = scenario_id,
                package_type = package_type,
                version = version,
                service = service,
                call = call,
            )
        }
    };
    Some(GuideProjection {
        scenario_id,
        family,
        source,
        operation,
        language,
        mode: if matches!(language, Language::Rust) {
            GuideProjectionMode::Embedded
        } else {
            GuideProjectionMode::Remote
        },
        capability: CapabilityStatus::Supported,
        package,
        code,
    })
}

/// Returns all six guide scenarios for every published language target.
pub fn all() -> Vec<GuideProjection> {
    GUIDE_PROJECTION_SCENARIOS
        .iter()
        .flat_map(|(scenario_id, _, _)| {
            Language::ALL
                .into_iter()
                .filter_map(|language| project(scenario_id, language))
        })
        .collect()
}

/// Converts a projection into the common generated-snippet metadata shape.
pub fn rendered(projection: GuideProjection) -> RenderedSnippet {
    RenderedSnippet {
        metadata: ScenarioMetadata {
            id: projection.scenario_id,
            family: projection.family,
            title: projection.operation,
            language: projection.language,
            source: projection.source,
            validation: ValidationReceipt {
                level: ValidationLevel::Rendered,
                status: ValidationStatus::NotRun,
                evidence: match projection.mode {
                    GuideProjectionMode::Embedded => {
                        "Rust native provider or facade scenario source"
                    }
                    GuideProjectionMode::Remote => {
                        "generated remote client call bound to the matching package artifact"
                    }
                },
            },
        },
        capability: projection.capability,
        code: projection.code,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_guide_family_has_all_language_projections() {
        let projections = all();
        assert_eq!(
            projections.len(),
            GUIDE_PROJECTION_SCENARIOS.len() * Language::ALL.len()
        );
        for (scenario_id, _, source) in GUIDE_PROJECTION_SCENARIOS {
            for language in Language::ALL {
                let projection = project(scenario_id, language).expect("guide projection");
                assert_eq!(projection.source, source);
                assert!(!projection.package.package_name.is_empty());
                assert!(!projection.package.artifact_path.is_empty());
                if language == Language::Rust {
                    assert_eq!(projection.mode, GuideProjectionMode::Embedded);
                    assert!(!projection.code.is_empty());
                } else {
                    assert_eq!(projection.mode, GuideProjectionMode::Remote);
                    assert!(
                        projection.code.contains("client")
                            || projection.code.contains("Client")
                            || projection.code.contains("Stub")
                    );
                    assert!(!projection.code.trim().is_empty());
                }
            }
        }
    }

    #[test]
    fn rust_projection_is_the_scenario_source() {
        for (scenario_id, _, _) in GUIDE_PROJECTION_SCENARIOS {
            let projection = project(scenario_id, Language::Rust).expect("Rust projection");
            assert_eq!(
                projection.code,
                rust_body(scenario_id).expect("Rust scenario body")
            );
        }
    }

    #[test]
    fn request_manifest_matches_every_remote_projection() {
        assert_eq!(
            GUIDE_REMOTE_REQUESTS.len(),
            GUIDE_PROJECTION_SCENARIOS.len()
        );
        for spec in GUIDE_REMOTE_REQUESTS {
            for language in Language::ALL {
                let projection = project(spec.scenario_id, language).expect("projection");
                assert_eq!(projection.operation, spec.operation);
                if language != Language::Rust {
                    assert!(projection.code.contains(spec.request));
                }
            }
        }
    }

    #[test]
    fn inference_projection_uses_wire_package_identity() {
        let spec = GUIDE_REMOTE_REQUESTS
            .iter()
            .find(|spec| spec.scenario_id == inference_scenarios::SCENARIO_ID)
            .expect("inference request manifest");
        assert_eq!(
            spec.operation,
            "inference.customer.v1.RunsService/Watch"
        );
        let projection = project(inference_scenarios::SCENARIO_ID, Language::TypeScript)
            .expect("inference TypeScript projection");
        assert_eq!(
            projection.operation,
            "inference.customer.v1.RunsService/Watch"
        );
        assert!(projection.code.contains("WatchRunRequestSchema"));
        let java = project(inference_scenarios::SCENARIO_ID, Language::Java)
            .expect("inference Java projection");
        assert!(java.code.contains("import inference.customer.v1.Inference;"));
        assert!(java.code.contains("import inference.customer.v1.RunsServiceGrpc;"));
        assert!(java.code.contains("setRunId"));
        assert!(java.code.contains("setFromSequence(0L)"));
        let python = project(inference_scenarios::SCENARIO_ID, Language::Python)
            .expect("inference Python projection");
        assert!(python.code.contains("run_id=bytes([2] * 16)"));
    }

    #[test]
    fn object_projection_uses_typed_streaming_requests() {
        for language in Language::ALL {
            let code = project(objects_scenarios::SCENARIO_ID, language)
                .expect("objects projection")
                .code;
            match language {
                Language::Rust => {}
                Language::Python => {
                    assert!(code.contains("iter(["));
                    assert!(code.contains("complete=True"));
                }
                Language::TypeScript => {
                    assert!(code.contains("create(PutObjectRequestSchema"));
                    assert!(code.contains("case: \"complete\""));
                }
                Language::Go => {
                    assert!(code.contains("PutObjectRequest_Complete"));
                }
                Language::Java => {
                    assert!(code.contains("setComplete(true)"));
                }
                Language::CSharp => {
                    assert!(code.contains("Complete = true"));
                }
                Language::Ruby => {
                    assert!(code.contains("complete: true"));
                }
                Language::Dart => {
                    assert!(code.contains("..complete = true"));
                }
                Language::Php => {
                    assert!(code.contains("writesDone()"));
                }
            }
        }
    }

    #[test]
    fn workers_projection_uses_canonical_module_and_digest() {
        for language in Language::ALL {
            let code = project(workers_scenarios::SCENARIO_ID, language)
                .expect("workers projection")
                .code;
            if language == Language::Rust {
                assert!(code.contains("publish-example-v1"));
                continue;
            }
            assert!(code.contains("publish-example-v1"));
            let lower = code.to_ascii_lowercase();
            assert!(lower.contains("sha") || lower.contains("digest"));
            assert!(code.contains("javascript") || code.contains("Javascript"));
        }
    }
}

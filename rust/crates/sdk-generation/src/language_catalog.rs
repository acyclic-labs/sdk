//! Rust-owned language capabilities, generator pins, and package recipes.
//!
//! Edit this source to change the target inventory. The JSON catalog is a
//! generated projection and is never used as production contract input.

#![doc = include_str!("../guides/language-generation.md")]

use schemars::JsonSchema;
use serde::Serialize;
use std::collections::BTreeMap;

pub const COMPILED_SOURCE: &str = include_str!("language_catalog.rs");
pub const LANGUAGE_GUIDE: &str = include_str!("../guides/language-generation.md");

#[derive(Debug, Clone, Copy, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[allow(dead_code)] // Reserved classifications are part of the public catalog vocabulary.
enum TargetKind {
    Language,
    Platform,
    Tooling,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[allow(dead_code)] // Qualification transitions retain one stable serialized vocabulary.
enum TargetStatus {
    Qualified,
    BaselineSupported,
    StrongCandidate,
    Candidate,
    Experimental,
    HttpOnly,
    AdapterOnly,
    NotQualifiable,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum Qualification {
    Unqualified,
    Prototype,
    Qualified,
    Blocked,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum Maturity {
    Stable,
    Beta,
    Experimental,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum RemoteLevel {
    FullGrpc,
    HttpProjection,
    None,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[allow(dead_code)] // Unknown remains representable before a target is investigated.
enum EmbeddedLevel {
    RustNative,
    FfiOrWasm,
    None,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum WireKind {
    ProtobufGrpc,
    JsonHttp,
    None,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum StreamShape {
    Unary,
    Client,
    Server,
    Bidi,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Catalog {
    #[schemars(range(min = 1, max = 1))]
    schema_version: u32,
    source_of_truth: SourceOfTruth,
    #[schemars(length(min = 1), extend("uniqueItems" = true))]
    qualification_gates: &'static [&'static str],
    #[schemars(length(min = 1))]
    targets: &'static [Target],
    #[schemars(extend("format" = "date"))]
    catalogued_at: &'static str,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum ContractAuthority {
    Rust,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SourceOfTruth {
    contract: ContractAuthority,
    #[schemars(length(min = 1))]
    descriptor: &'static str,
    #[schemars(length(min = 1), extend("uniqueItems" = true))]
    wire_semantics: &'static [&'static str],
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Target {
    #[schemars(regex(pattern = r"^[a-z0-9][a-z0-9-]*$"))]
    id: &'static str,
    kind: TargetKind,
    #[schemars(length(min = 1))]
    language_family: &'static str,
    status: TargetStatus,
    remote: RemoteCapability,
    embedded: EmbeddedCapability,
    maturity: Maturity,
    generator: Generator,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Producer")]
    producer: Option<Producer>,
    package: Package,
    #[schemars(length(min = 1))]
    evidence: &'static [Evidence],
    outstanding: &'static [&'static str],
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Vec<String>")]
    aliases: Option<&'static [&'static str]>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RemoteCapability {
    level: RemoteLevel,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Qualification")]
    qualification: Option<Qualification>,
    #[schemars(extend("uniqueItems" = true))]
    streaming: &'static [StreamShape],
    wire: WireKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    notes: Option<&'static str>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EmbeddedCapability {
    level: EmbeddedLevel,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Qualification")]
    qualification: Option<Qualification>,
    #[schemars(length(min = 1))]
    notes: &'static str,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Generator {
    #[schemars(length(min = 1))]
    name: &'static str,
    #[schemars(length(min = 1))]
    version: &'static str,
    #[schemars(extend("format" = "uri"))]
    source: &'static str,
    #[schemars(length(min = 1))]
    license: &'static str,
    #[schemars(length(min = 1))]
    pin: &'static str,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Producer {
    #[schemars(length(min = 1))]
    program: &'static str,
    #[schemars(length(min = 1))]
    args: &'static [&'static str],
    #[schemars(
        length(min = 1),
        regex(pattern = r"^(?![A-Za-z]:)(?!/)(?!\\)(?!.*(?:^|[/\\])\.\.(?:[/\\]|$)).+")
    )]
    output: &'static str,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Package {
    #[schemars(length(min = 1))]
    ecosystem: &'static str,
    #[schemars(length(min = 1))]
    artifact: &'static str,
    installable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    notes: Option<&'static str>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Evidence {
    #[schemars(extend("format" = "uri-reference"))]
    url: &'static str,
    #[schemars(length(min = 1))]
    claim: &'static str,
}

const CATALOG: Catalog = Catalog {
    schema_version: 1,
    source_of_truth: SourceOfTruth {
        contract: ContractAuthority::Rust,
        descriptor: "Rust contract model -> deterministic protobuf descriptor; buf/protoc descriptor is a compatibility oracle",
        wire_semantics: &[
            "proto3 optional presence",
            "oneof and required_oneof",
            "maps, repeated fields, bytes, timestamps and reserved ranges",
            "custom validation options 51001-51012",
            "unary, client-streaming and server-streaming RPC direction",
            "descriptor digest and capability handshake",
            "canonical errors, cancellation, idempotency and recovery",
        ],
    },
    qualification_gates: &[
        "Generate from the Rust-owned descriptor revision and record generator/runtime versions, source revision and artifact hashes.",
        "Compile and install a package from a local artifact; no registry publication is part of this loop.",
        "Round-trip optional fields, oneof, maps, bytes, timestamps, enum values, reserved names and custom options against Rust golden vectors.",
        "Exercise all active RPC shapes: unary, client streaming and server streaming; retain bidi support where the runtime provides it.",
        "Verify metadata, TLS, deadlines, cancellation, retry/recovery and idempotency behavior against the Rust conformance server.",
        "Report remote-client and embedded capability separately; Filesystem and Harness embedded behavior stays in Rust native/WASM bindings.",
        "Reject a target as full SDK coverage when it only consumes the derived JSON/OpenAPI projection.",
    ],
    targets: &[
        Target {
            id: "rust",
            kind: TargetKind::Language,
            language_family: "Rust",
            status: TargetStatus::BaselineSupported,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Prototype),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(
                    "Canonical workspace provider and descriptor baseline; this migration has not yet produced an independently qualified Rust SDK artifact for every family.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::RustNative,
                qualification: Some(Qualification::Prototype),
                notes: "Canonical Rust implementation and native conformance provider.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "workspace Rust descriptor/codegen",
                version: "workspace-locked",
                source: "https://buf.build/docs/generate/",
                license: "Apache-2.0",
                pin: "Lock Rust generator, descriptor digest and artifact hashes.",
            },
            producer: None,
            package: Package {
                ecosystem: "crates.io/Cargo",
                artifact: "family crates (no umbrella)",
                installable: false,
                notes: Some(
                    "Cargo.toml publishes family-specific crates acyclic-actors, acyclic-fs, acyclic-harness, acyclic-inference, acyclic-machines, acyclic-objects, acyclic-stream and acyclic-workers; the workspace has no complete acyclic-sdk umbrella artifact or install receipt.",
                ),
            },
            evidence: &[Evidence {
                url: "https://www.rust-lang.org/",
                claim: "Rust is the canonical implementation and source-of-truth boundary for this migration.",
            }],
            outstanding: &[
                "Complete the Rust-owned generation entrypoint across every active family and retain deterministic install receipts.",
                "Run the full active RPC, descriptor digest, cancellation, recovery and embedded conformance suites before calling the new pipeline qualified.",
            ],
            aliases: None,
        },
        Target {
            id: "typescript",
            kind: TargetKind::Language,
            language_family: "TypeScript",
            status: TargetStatus::Candidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Prototype),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(
                    "Generated transport and UI adapters only; shared handwritten behavior remains Rust-owned.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Embedded behavior remains Rust-owned; use a narrow FFI/WASM boundary.",
            },
            maturity: Maturity::Unknown,
            generator: Generator {
                name: "protobuf-es + grpc-web/native adapter",
                version: "workspace-locked",
                source: "https://github.com/bufbuild/protobuf-es",
                license: "Apache-2.0",
                pin: "Pin protoc-gen-es and transport adapter versions.",
            },
            producer: None,
            package: Package {
                ecosystem: "npm",
                artifact: "@acyclic-labs/sdk",
                installable: true,
                notes: Some(
                    "Thin generated transport and UI adapters only; shared handwritten behavior is being removed.",
                ),
            },
            evidence: &[Evidence {
                url: "https://github.com/bufbuild/protobuf-es",
                claim: "Buf protobuf-es provides TypeScript protobuf generation from descriptors.",
            }],
            outstanding: &[
                "Replace handwritten shared contracts only after conformance and drift checks pass.",
            ],
            aliases: None,
        },
        Target {
            id: "python",
            kind: TargetKind::Language,
            language_family: "Python",
            status: TargetStatus::Candidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Prototype),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(
                    "Python remote clients are generated from the Rust-owned protobuf contract; embedded behavior remains behind the Rust FFI/WASM boundary.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::FfiOrWasm,
                qualification: Some(Qualification::Unqualified),
                notes: "Embedded behavior remains Rust-owned; use a narrow FFI/WASM boundary.",
            },
            maturity: Maturity::Unknown,
            generator: Generator {
                name: "grpcio-tools",
                version: "1.83.0",
                source: "https://grpc.io/docs/languages/python/",
                license: "Apache-2.0",
                pin: "Pin grpcio-tools/protobuf and wheel hashes.",
            },
            producer: None,
            package: Package {
                ecosystem: "PyPI",
                artifact: "acyclic-sdk-transport",
                installable: true,
                notes: Some("Generated transport wheel plus Rust-owned facade."),
            },
            evidence: &[Evidence {
                url: "https://grpc.io/docs/languages/python/",
                claim: "Official Python gRPC runtime and generated stubs.",
            }],
            outstanding: &[
                "Run full Objects/Stream/Filesystem/Harness conformance and package install checks.",
            ],
            aliases: None,
        },
        Target {
            id: "go",
            kind: TargetKind::Language,
            language_family: "Go",
            status: TargetStatus::Candidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(
                    "Rust-authority-bound Go producer emits an installable module for all nine families plus validation options; transport qualification remains separately evidenced.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::FfiOrWasm,
                qualification: Some(Qualification::Unqualified),
                notes: "Embedded behavior remains Rust-owned; use a narrow FFI/WASM boundary.",
            },
            maturity: Maturity::Unknown,
            generator: Generator {
                name: "protoc-gen-go + protoc-gen-go-grpc",
                version: "Go 1.27.1; protoc 36.2; protoc-gen-go v1.36.10; protoc-gen-go-grpc 1.5.1",
                source: "https://grpc.io/docs/languages/go/",
                license: "Apache-2.0",
                pin: "Pin Go 1.27.1, protoc 36.2, plugin versions and SHA-256s, the Rust authority manifest, and every source/descriptor digest.",
            },
            producer: Some(Producer {
                program: "go",
                args: &[
                    "-C",
                    "{source_root}/go",
                    "run",
                    "./cmd/sdk-go-producer",
                    "--source-root",
                    "{source_root}",
                    "--authority",
                    "{wire_root}",
                    "--request",
                    "{request}",
                    "--output",
                    "{target_output}",
                    "--protoc",
                    "protoc",
                    "--protoc-gen-go",
                    "protoc-gen-go",
                    "--protoc-gen-go-grpc",
                    "protoc-gen-go-grpc",
                    "--go-version",
                    "go1.27.1",
                    "--protoc-version",
                    "libprotoc 36.2",
                    "--protoc-gen-go-version",
                    "v1.36.10",
                    "--protoc-gen-go-grpc-version",
                    "1.5.1",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "Go modules",
                artifact: "github.com/acyclic-labs/sdk/go",
                installable: true,
                notes: Some("Install test must use the generated module and common vectors."),
            },
            evidence: &[Evidence {
                url: "https://grpc.io/docs/languages/go/",
                claim: "Official Go gRPC language support and package workflow.",
            }],
            outstanding: &[
                "Keep the producer PATH bound to the pinned Go/protoc/plugin toolchain in CI, then retain the installed transport, TLS, cancellation and recovery receipts alongside each generated package.",
            ],
            aliases: None,
        },
        Target {
            id: "java",
            kind: TargetKind::Language,
            language_family: "JVM",
            status: TargetStatus::Candidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(""),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::FfiOrWasm,
                qualification: Some(Qualification::Unqualified),
                notes: "Embedded behavior remains Rust-owned; use a narrow FFI/WASM boundary.",
            },
            maturity: Maturity::Unknown,
            generator: Generator {
                name: "protoc + grpc-java",
                version: "protobuf 4.31.1; grpc-java 1.75.0; protobuf-maven-plugin 0.6.1",
                source: "https://grpc.io/docs/languages/java/",
                license: "Apache-2.0",
                pin: "Pin Maven artifacts and protoc; source-bound adapter jvm/producer-adapter.ps1.",
            },
            producer: Some(Producer {
                program: "powershell.exe",
                args: &[
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    "{source_root}/jvm/producer-adapter.ps1",
                    "-SourceRoot",
                    "{source_root}",
                    "-Authority",
                    "{wire_root}",
                    "-Request",
                    "{request}",
                    "-Output",
                    "{target_output}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "Maven Central",
                artifact: "dev.acyclic:sdk-java",
                installable: true,
                notes: Some(
                    "Java transport package; Kotlin is tracked separately for idiomatic APIs.",
                ),
            },
            evidence: &[Evidence {
                url: "https://grpc.io/docs/languages/java/",
                claim: "Official Java gRPC runtime and code generation.",
            }],
            outstanding: &[
                "Qualify custom options, presence, streaming and Rust facade generation.",
            ],
            aliases: Some(&["jvm"]),
        },
        Target {
            id: "csharp",
            kind: TargetKind::Language,
            language_family: "C#/.NET",
            status: TargetStatus::Candidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(""),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::FfiOrWasm,
                qualification: Some(Qualification::Unqualified),
                notes: "Embedded behavior remains Rust-owned; use a narrow FFI/WASM boundary.",
            },
            maturity: Maturity::Unknown,
            generator: Generator {
                name: "Grpc.Tools + Grpc.Net.Client",
                version: "Google.Protobuf 3.31.1; Grpc.Tools 2.71.0; Grpc.Net.Client 2.71.0",
                source: "https://grpc.io/docs/languages/csharp/",
                license: "Apache-2.0",
                pin: "Pin NuGet packages and protoc plugin; source-bound adapter dotnet/producer-adapter.ps1.",
            },
            producer: Some(Producer {
                program: "powershell.exe",
                args: &[
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    "{source_root}/dotnet/producer-adapter.ps1",
                    "-SourceRoot",
                    "{source_root}",
                    "-Authority",
                    "{wire_root}",
                    "-Request",
                    "{request}",
                    "-Output",
                    "{target_output}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "NuGet",
                artifact: "Acyclic.Sdk",
                installable: true,
                notes: Some("Remote package only; embedded behavior remains Rust."),
            },
            evidence: &[Evidence {
                url: "https://grpc.io/docs/languages/csharp/",
                claim: "Official .NET gRPC language support.",
            }],
            outstanding: &["Run Windows/Linux package and cancellation/recovery tests."],
            aliases: Some(&["dotnet"]),
        },
        Target {
            id: "swift",
            kind: TargetKind::Language,
            language_family: "Swift",
            status: TargetStatus::Candidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(""),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::FfiOrWasm,
                qualification: Some(Qualification::Unqualified),
                notes: "Embedded behavior remains Rust-owned; use a narrow FFI/WASM boundary.",
            },
            maturity: Maturity::Unknown,
            generator: Generator {
                name: "grpc-swift-2 + grpc-swift-protobuf + SwiftProtobuf",
                version: "grpc-swift-2 2.4.1; grpc-swift-protobuf 2.4.1; SwiftProtobuf 1.38.1",
                source: "https://github.com/grpc/grpc-swift-2",
                license: "Apache-2.0",
                pin: "Pin SwiftProtobuf and grpc-swift releases to one toolchain.",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "swift",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "Swift Package Manager",
                artifact: "AcyclicSDK generated package",
                installable: false,
                notes: Some(
                    "Generated package is source-bound at Q:/sdk/build/sdk-swift-generated-241 and builds with the official Swift 6.4 Windows toolchain. The pinned gRPC NIO POSIX transport reaches swift-nio-ssl and fails its unsupported-Windows guard; Transport Services is Apple-only, so the Windows fixture consumer remains outstanding pending a maintained Windows transport or Linux/macOS CI consumer.",
                ),
            },
            evidence: &[
                Evidence {
                    url: "https://github.com/grpc/grpc-swift-2",
                    claim: "Official gRPC Swift 2 implementation with GRPCCore and code generation.",
                },
                Evidence {
                    url: "https://www.swift.org/install/windows/manual/",
                    claim: "Swift.org documents native Windows installation with MSVC, Windows SDK, Python and Git.",
                },
            ],
            outstanding: &[
                "Build generated package on Windows or Linux with SwiftPM; on Windows record the pinned NIO transport guard failure or use a maintained Windows transport, then run custom options, metadata, streaming and clean install vectors.",
            ],
            aliases: None,
        },
        Target {
            id: "cpp",
            kind: TargetKind::Language,
            language_family: "C++",
            status: TargetStatus::Candidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(
                    "Pinned Generate.ps1 emits source-bound receipt; remote CMake requires matching Protobuf 36.2 and gRPC C++ 1.80.0.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::FfiOrWasm,
                qualification: Some(Qualification::Unqualified),
                notes: "Embedded behavior remains Rust-owned; use a narrow FFI/WASM boundary.",
            },
            maturity: Maturity::Unknown,
            generator: Generator {
                name: "protoc + gRPC C++",
                version: "Protobuf 36.2; gRPC C++ 1.80.0",
                source: "https://grpc.io/docs/languages/cpp/",
                license: "Apache-2.0",
                pin: "Pin gRPC/Protobuf CMake dependencies.",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "cpp",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "vcpkg/Conan/CMake",
                artifact: "acyclic-sdk-cpp source package",
                installable: false,
                notes: Some(
                    "Remote C++ package install remains pending against gRPC C++ 1.80.0; the cached 1.56.2 source is rejected. Embedded C++ has a separate source-bound local CMake receipt.",
                ),
            },
            evidence: &[
                Evidence {
                    url: "https://grpc.io/docs/languages/cpp/",
                    claim: "Official C++ gRPC runtime and plugin.",
                },
                Evidence {
                    url: "https://github.com/grpc/grpc/releases/tag/v1.80.0",
                    claim: "Pinned gRPC C++ 1.80.0 source release; cached 1.56.2 source is not accepted as qualification evidence.",
                },
            ],
            outstanding: &[
                "Run pinned remote generator/runtime build, ABI, cancellation, recovery and clean package installation; retain generation-receipt.json hashes.",
            ],
            aliases: None,
        },
        Target {
            id: "ruby",
            kind: TargetKind::Language,
            language_family: "Ruby",
            status: TargetStatus::Candidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(""),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::FfiOrWasm,
                qualification: Some(Qualification::Unqualified),
                notes: "Embedded behavior remains Rust-owned; use a narrow FFI/WASM boundary.",
            },
            maturity: Maturity::Unknown,
            generator: Generator {
                name: "grpc-tools + grpc Ruby",
                version: "pending qualification",
                source: "https://grpc.io/docs/languages/ruby/",
                license: "Apache-2.0",
                pin: "Pin grpc-tools and grpc gem releases.",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "ruby",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "RubyGems",
                artifact: "acyclic-sdk",
                installable: true,
                notes: Some("Generated Ruby transport and Rust-owned facade."),
            },
            evidence: &[Evidence {
                url: "https://grpc.io/docs/languages/ruby/",
                claim: "Official Ruby gRPC language support.",
            }],
            outstanding: &["Qualify gem installation and stream recovery vectors."],
            aliases: None,
        },
        Target {
            id: "php",
            kind: TargetKind::Language,
            language_family: "PHP",
            status: TargetStatus::Candidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(""),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::FfiOrWasm,
                qualification: Some(Qualification::Unqualified),
                notes: "Embedded behavior remains Rust-owned; use a narrow FFI/WASM boundary.",
            },
            maturity: Maturity::Unknown,
            generator: Generator {
                name: "grpc_php_plugin + protobuf PHP",
                version: "pending qualification",
                source: "https://grpc.io/docs/languages/php/",
                license: "Apache-2.0",
                pin: "Pin PECL gRPC and Composer protobuf packages.",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "php",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "Packagist/PECL",
                artifact: "acyclic/sdk",
                installable: true,
                notes: Some("PHP package requires PECL runtime qualification."),
            },
            evidence: &[Evidence {
                url: "https://grpc.io/docs/languages/php/",
                claim: "Official PHP gRPC runtime and package guidance.",
            }],
            outstanding: &["Qualify PECL availability, TLS/metadata and streaming behavior."],
            aliases: None,
        },
        Target {
            id: "dart",
            kind: TargetKind::Language,
            language_family: "Dart",
            status: TargetStatus::Candidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(""),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::FfiOrWasm,
                qualification: Some(Qualification::Unqualified),
                notes: "Embedded behavior remains Rust-owned; use a narrow FFI/WASM boundary.",
            },
            maturity: Maturity::Unknown,
            generator: Generator {
                name: "protoc_plugin + grpc Dart",
                version: "pending qualification",
                source: "https://grpc.io/docs/languages/dart/",
                license: "BSD-3-Clause/Apache-2.0",
                pin: "Pin Dart protobuf and grpc package versions.",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "dart",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "pub.dev",
                artifact: "acyclic_sdk",
                installable: true,
                notes: Some("Dart package; Flutter integration is a consumer adapter."),
            },
            evidence: &[Evidence {
                url: "https://grpc.io/docs/languages/dart/",
                claim: "Official Dart gRPC package and generated stubs.",
            }],
            outstanding: &["Qualify Flutter/Dart package installation and cancellation/recovery."],
            aliases: None,
        },
        Target {
            id: "kotlin",
            kind: TargetKind::Language,
            language_family: "JVM",
            status: TargetStatus::StrongCandidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some("Distinct Kotlin coroutine/stub artifact over Java protobuf messages."),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Use Rust JNI/JNA or WASM boundary; do not translate embedded behavior.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "grpc-kotlin protoc plugin",
                version: "1.4.3",
                source: "https://github.com/grpc/grpc-kotlin",
                license: "Apache-2.0",
                pin: "Maven Central release; pin plugin and grpc-kotlin-stub checksums together",
            },
            producer: Some(Producer {
                program: "powershell.exe",
                args: &[
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    "{source_root}/jvm/kotlin-producer-adapter.ps1",
                    "-Authority",
                    "{wire_root}",
                    "-Request",
                    "{request}",
                    "-Output",
                    "{target_output}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "Maven Central",
                artifact: "com.acyclic:sdk-kotlin",
                installable: true,
                notes: Some(
                    "Maven/Gradle; coroutine facade remains generated from Rust operation metadata.",
                ),
            },
            evidence: &[
                Evidence {
                    url: "https://github.com/grpc/grpc-kotlin",
                    claim: "Official Kotlin gRPC compiler plugin and runtime with unary and streaming stubs.",
                },
                Evidence {
                    url: "https://grpc.io/docs/languages/kotlin/",
                    claim: "Official language support and package guidance.",
                },
            ],
            outstanding: &[
                "Run Actors plus Stream and Objects vectors with proto3 presence and custom options.",
            ],
            aliases: None,
        },
        Target {
            id: "scala",
            kind: TargetKind::Language,
            language_family: "JVM",
            status: TargetStatus::StrongCandidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Prototype),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(
                    "ScalaPB 0.11.17 native protobuf/gRPC generation compiles 70 Rust Actors and Stream sources; the published artifact is consumed from an isolated Ivy resolver against the Rust fixture with auth, bytes, uint64 and Stream append/read checks. Full target qualification and cancellation recovery remain pending.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Use a Rust FFI/WASM boundary only.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "ScalaPB + scalapb-grpc",
                version: "0.11.17",
                source: "https://github.com/scalapb/ScalaPB",
                license: "BSD-3-Clause",
                pin: "Stable 0.11.17; do not use ScalaPB 1.0 alpha for release qualification.",
            },
            producer: None,
            package: Package {
                ecosystem: "Maven Central/SBT",
                artifact: "dev.acyclic:sdk-scala_2.13",
                installable: true,
                notes: Some(
                    "The ScalaPB artifact dev.acyclic:acyclic-sdk-scala-grpc-prototype_2.13:0.1.0 compiles, publishes and is consumed from an isolated Ivy resolver with sbt 1.10.11; receipt: research/additional-languages/scala-receipt.json.",
                ),
            },
            evidence: &[
                Evidence {
                    url: "https://github.com/scalapb/ScalaPB",
                    claim: "Protobuf compiler and Scala gRPC integration supporting streaming services.",
                },
                Evidence {
                    url: "https://github.com/scalapb/ScalaPB/releases/tag/v0.11.17",
                    claim: "Pinned stable release used for the candidate.",
                },
                Evidence {
                    url: "https://github.com/OpenAPITools/openapi-generator/blob/v7.25.0/modules/openapi-generator/src/main/java/org/openapitools/codegen/languages/ScalaSttpClientCodegen.java",
                    claim: "Pinned OAG Scala HTTP projection generator used for the compile smoke.",
                },
                Evidence {
                    url: "https://github.com/acyclic-labs/sdk/blob/main/rust/crates/sdk-examples/src/bin/fixture-server.rs",
                    claim: "Rust-owned loopback fixture exposes the generated Actors and Stream tonic services for bounded ScalaPB gRPC consumption.",
                },
                Evidence {
                    url: "research/additional-languages/scala-receipt.json",
                    claim: "ScalaPB package was consumed from an isolated local Ivy resolver against the Rust fixture with auth, bytes, uint64 and Stream append/read vectors.",
                },
            ],
            outstanding: &[
                "Run generated transport against Objects and remaining Stream service vectors, including client/server streaming.",
                "Verify custom validation options, descriptor digest handshake, metadata, cancellation and recovery before promotion.",
            ],
            aliases: None,
        },
        Target {
            id: "elixir",
            kind: TargetKind::Language,
            language_family: "BEAM",
            status: TargetStatus::Candidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(
                    "grpc and protobuf Hex packages; custom Rust facade still required for recovery policy.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Call Rust through NIF only if a separately qualified embedded ABI is requested.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "elixir-grpc + protobuf_generate",
                version: "1.0.3",
                source: "https://github.com/elixir-grpc/grpc",
                license: "Apache-2.0",
                pin: "Hex release 1.0.3 and matching protobuf package lock.",
            },
            producer: None,
            package: Package {
                ecosystem: "Hex",
                artifact: "acyclic_sdk",
                installable: true,
                notes: Some("mix package; test OTP/Elixir matrix explicitly."),
            },
            evidence: &[
                Evidence {
                    url: "https://github.com/elixir-grpc/grpc",
                    claim: "Elixir gRPC implementation with generated clients and unary/client/server/bidi streaming.",
                },
                Evidence {
                    url: "https://hex.pm/packages/grpc",
                    claim: "Hex package distribution and release metadata.",
                },
            ],
            outstanding: &[
                "No Elixir toolchain is installed on this host; run the same generated descriptor and package smoke in an OTP image.",
                "Verify unknown/custom option retention and descriptor digest handshake.",
            ],
            aliases: None,
        },
        Target {
            id: "ballerina",
            kind: TargetKind::Language,
            language_family: "Ballerina",
            status: TargetStatus::Candidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(
                    "Built-in bal grpc generator/runtime; toolchain is tied to Ballerina/JVM releases.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "No embedded parity without a Rust native/WASM bridge.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "bal grpc",
                version: "1.15.1",
                source: "https://central.ballerina.io/ballerina/grpc/latest",
                license: "Apache-2.0",
                pin: "Ballerina Central 1.15.1; pin Ballerina distribution and Java 21.",
            },
            producer: None,
            package: Package {
                ecosystem: "Ballerina Central",
                artifact: "acyclic/sdk",
                installable: true,
                notes: Some("bal package; requires Ballerina 2201.13.6-compatible toolchain."),
            },
            evidence: &[
                Evidence {
                    url: "https://ballerina.io/spec/grpc/",
                    claim: "gRPC service/client generation and all four streaming directions.",
                },
                Evidence {
                    url: "https://central.ballerina.io/ballerina/grpc/latest",
                    claim: "Apache-2.0 Ballerina Central runtime package.",
                },
            ],
            outstanding: &[
                "Validate proto2 custom option imports, proto3 optional presence and Ballerina Central artifact reproducibility.",
            ],
            aliases: None,
        },
        Target {
            id: "objective-c",
            kind: TargetKind::Language,
            language_family: "Apple",
            status: TargetStatus::Candidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some("Official gRPC Objective-C plugin and Protobuf CocoaPods runtime."),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Use Rust C ABI for embedded behavior; not a translated Objective-C implementation.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "grpc Objective-C protoc plugin",
                version: "1.62.2",
                source: "https://github.com/grpc/grpc/tree/master/src/objective-c",
                license: "Apache-2.0",
                pin: "Pin the plugin, gRPC-ProtoRPC pod and Protobuf pod to one released gRPC tag.",
            },
            producer: None,
            package: Package {
                ecosystem: "CocoaPods",
                artifact: "AcyclicSDK",
                installable: true,
                notes: Some(
                    "Apple platform package; requires Xcode and platform matrix qualification.",
                ),
            },
            evidence: &[
                Evidence {
                    url: "https://github.com/grpc/grpc/blob/master/examples/objective-c/route_guide/RouteGuide.podspec",
                    claim: "Official generated Objective-C gRPC package example using gRPC-ProtoRPC and Protobuf.",
                },
                Evidence {
                    url: "https://grpc.io/docs/languages/objective-c/",
                    claim: "Official Objective-C gRPC language support.",
                },
            ],
            outstanding: &[
                "Run macOS/iOS build and cancellation tests; verify custom option extensions are retained by the chosen protobuf runtime.",
            ],
            aliases: None,
        },
        Target {
            id: "erlang",
            kind: TargetKind::Language,
            language_family: "BEAM",
            status: TargetStatus::Experimental,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(
                    "grpcbox advertises generated services, reflection and all streaming shapes, but needs maintenance qualification.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Use a Rust NIF/port only for embedded behavior.",
            },
            maturity: Maturity::Experimental,
            generator: Generator {
                name: "grpcbox + gpb",
                version: "0.15.0",
                source: "https://github.com/tsloughter/grpcbox",
                license: "Apache-2.0",
                pin: "Pin grpcbox and gpb commits together; release cadence must be checked before promotion.",
            },
            producer: None,
            package: Package {
                ecosystem: "Hex/rebar3",
                artifact: "acyclic_sdk",
                installable: true,
                notes: Some("OTP package; no release claim before an OTP matrix passes."),
            },
            evidence: &[
                Evidence {
                    url: "https://github.com/tsloughter/grpcbox",
                    claim: "Erlang gRPC client/server with generated service behavior and four streaming forms.",
                },
                Evidence {
                    url: "https://github.com/tomas-abrahamsson/gpb",
                    claim: "Erlang protobuf compiler/runtime used by grpcbox.",
                },
            ],
            outstanding: &[
                "Confirm current release and maintainer activity; run descriptor, custom option, metadata and recovery vectors.",
            ],
            aliases: None,
        },
        Target {
            id: "ocaml",
            kind: TargetKind::Language,
            language_family: "OCaml",
            status: TargetStatus::Experimental,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(
                    "ocaml-grpc supports Eio/Lwt/Async and all streaming forms; ecosystem maturity is lower.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Use Rust C ABI/WASM for embedded behavior.",
            },
            maturity: Maturity::Experimental,
            generator: Generator {
                name: "ocaml-protoc-plugin + ocaml-grpc",
                version: "0.2.0",
                source: "https://github.com/dialohq/ocaml-grpc",
                license: "BSD-3-Clause",
                pin: "Pin opam package versions and OCaml compiler version.",
            },
            producer: None,
            package: Package {
                ecosystem: "opam",
                artifact: "acyclic-sdk",
                installable: true,
                notes: Some("opam package; qualify compiler/runtime combinations."),
            },
            evidence: &[
                Evidence {
                    url: "https://github.com/dialohq/ocaml-grpc",
                    claim: "OCaml gRPC over HTTP/2 with unary and streaming support.",
                },
                Evidence {
                    url: "https://opam.ocaml.org/packages/grpc/",
                    claim: "opam distribution for the gRPC runtime.",
                },
            ],
            outstanding: &[
                "No OCaml toolchain is installed on this host; run package install and full conformance in an opam container.",
                "Verify custom option and descriptor handshake support.",
            ],
            aliases: None,
        },
        Target {
            id: "common-lisp",
            kind: TargetKind::Language,
            language_family: "Common Lisp",
            status: TargetStatus::Experimental,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Unqualified),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(
                    "ag-gRPC demonstrates generated stubs but package/license provenance is not yet release-grade.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Embedded behavior remains Rust-owned.",
            },
            maturity: Maturity::Unknown,
            generator: Generator {
                name: "ag-gRPC",
                version: "unverified",
                source: "https://github.com/atgreen/ag-gRPC",
                license: "unverified",
                pin: "Must pin a reviewed commit and verify license before any package claim.",
            },
            producer: None,
            package: Package {
                ecosystem: "Quicklisp",
                artifact: "acyclic-sdk",
                installable: false,
                notes: Some(
                    "Inventory only until a reproducible Quicklisp/Ultralisp source is qualified.",
                ),
            },
            evidence: &[Evidence {
                url: "https://github.com/atgreen/ag-gRPC",
                claim: "Repository demonstrates Common Lisp protobuf/gRPC stubs and streaming examples.",
            }],
            outstanding: &[
                "Verify license, release provenance, package index, TLS, cancellation and descriptor compatibility.",
            ],
            aliases: None,
        },
        Target {
            id: "ada",
            kind: TargetKind::Language,
            language_family: "Ada",
            status: TargetStatus::HttpOnly,
            remote: RemoteCapability {
                level: RemoteLevel::HttpProjection,
                qualification: Some(Qualification::Unqualified),
                streaming: &[],
                wire: WireKind::JsonHttp,
                notes: Some(
                    "OpenAPI Generator Ada client; no qualifying protobuf/gRPC generator in the selected stack.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Rust native binding only.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "OpenAPI Generator ada",
                version: "7.25.0",
                source: "https://openapi-generator.tech/docs/generators/",
                license: "Apache-2.0",
                pin: "ef964b04480889ef86b56cfae84ade8ad4c91c41",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "ada",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "Alire/source",
                artifact: "acyclic_sdk",
                installable: false,
                notes: Some("Generated package install needs an Alire qualification environment."),
            },
            evidence: &[Evidence {
                url: "https://openapi-generator.tech/docs/generators/",
                claim: "Ada is listed as an OpenAPI client generator.",
            }],
            outstanding: &[
                "Qualify package installation and generated JSON models; cannot claim gRPC parity.",
            ],
            aliases: None,
        },
        Target {
            id: "c",
            kind: TargetKind::Language,
            language_family: "C",
            status: TargetStatus::HttpOnly,
            remote: RemoteCapability {
                level: RemoteLevel::HttpProjection,
                qualification: Some(Qualification::Unqualified),
                streaming: &[],
                wire: WireKind::JsonHttp,
                notes: Some(
                    "OpenAPI Generator C/libcurl output is HTTP-only and documented as beta.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::FfiOrWasm,
                qualification: Some(Qualification::Unqualified),
                notes: "C ABI may expose selected Rust embedded APIs, but generated HTTP code is not embedded parity.",
            },
            maturity: Maturity::Beta,
            generator: Generator {
                name: "OpenAPI Generator c",
                version: "7.25.0",
                source: "https://openapi-generator.tech/docs/generators/c/",
                license: "Apache-2.0",
                pin: "ef964b04480889ef86b56cfae84ade8ad4c91c41",
            },
            producer: None,
            package: Package {
                ecosystem: "CMake/source",
                artifact: "acyclic_sdk",
                installable: false,
                notes: Some("Compile smoke requires libcurl and a native C toolchain."),
            },
            evidence: &[Evidence {
                url: "https://openapi-generator.tech/docs/generators/c/",
                claim: "C client generator uses libcurl and is marked beta in its help metadata.",
            }],
            outstanding: &[
                "Keep C ABI generation separate from HTTP client generation; test ABI ownership and cancellation.",
            ],
            aliases: None,
        },
        Target {
            id: "clojure",
            kind: TargetKind::Language,
            language_family: "JVM",
            status: TargetStatus::HttpOnly,
            remote: RemoteCapability {
                level: RemoteLevel::HttpProjection,
                qualification: Some(Qualification::Unqualified),
                streaming: &[],
                wire: WireKind::JsonHttp,
                notes: Some(
                    "OAG Clojure client is an HTTP projection; Java gRPC stubs are a possible adapter but not independent Clojure generation.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Rust interop only.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "OpenAPI Generator clojure",
                version: "7.25.0",
                source: "https://openapi-generator.tech/docs/generators/",
                license: "Apache-2.0",
                pin: "ef964b04480889ef86b56cfae84ade8ad4c91c41",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "clojure",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "Maven/Leiningen",
                artifact: "acyclic-sdk",
                installable: true,
                notes: Some(
                    "HTTP-only generated package; classify separately from JVM gRPC transport.",
                ),
            },
            evidence: &[Evidence {
                url: "https://openapi-generator.tech/docs/generators/clojure/",
                claim: "Clojure client generator and Maven/Leiningen-oriented output.",
            }],
            outstanding: &[
                "Do not promote to full SDK without a Clojure-native facade and streaming/recovery tests.",
            ],
            aliases: None,
        },
        Target {
            id: "crystal",
            kind: TargetKind::Language,
            language_family: "Crystal",
            status: TargetStatus::HttpOnly,
            remote: RemoteCapability {
                level: RemoteLevel::HttpProjection,
                qualification: Some(Qualification::Unqualified),
                streaming: &[],
                wire: WireKind::JsonHttp,
                notes: Some("OAG Crystal client is beta; no maintained gRPC target selected."),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Rust native boundary only.",
            },
            maturity: Maturity::Beta,
            generator: Generator {
                name: "OpenAPI Generator crystal",
                version: "7.25.0",
                source: "https://openapi-generator.tech/docs/generators/",
                license: "Apache-2.0",
                pin: "ef964b04480889ef86b56cfae84ade8ad4c91c41",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "crystal",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "Shards",
                artifact: "acyclic_sdk",
                installable: false,
                notes: Some("Requires Crystal toolchain for package smoke."),
            },
            evidence: &[Evidence {
                url: "https://openapi-generator.tech/docs/generators/crystal/",
                claim: "Crystal generator is listed and marked beta.",
            }],
            outstanding: &[
                "Qualify Shards packaging and JSON semantics; streaming remains unsupported.",
            ],
            aliases: None,
        },
        Target {
            id: "elm",
            kind: TargetKind::Language,
            language_family: "Elm",
            status: TargetStatus::HttpOnly,
            remote: RemoteCapability {
                level: RemoteLevel::HttpProjection,
                qualification: Some(Qualification::Unqualified),
                streaming: &[],
                wire: WireKind::JsonHttp,
                notes: Some(
                    "OpenAPI Generator output is browser/HTTP oriented; no native gRPC runtime.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Browser UI can call a Rust/WASM adapter, not embed arbitrary service behavior.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "OpenAPI Generator elm",
                version: "7.25.0",
                source: "https://openapi-generator.tech/docs/generators/",
                license: "Apache-2.0",
                pin: "ef964b04480889ef86b56cfae84ade8ad4c91c41",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "elm",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "Elm package",
                artifact: "acyclic-sdk",
                installable: false,
                notes: Some("Needs Elm package build and browser smoke."),
            },
            evidence: &[Evidence {
                url: "https://openapi-generator.tech/docs/generators/elm/",
                claim: "Elm client generator is listed.",
            }],
            outstanding: &[
                "Document browser-only transport limits and test generated decoders against Rust JSON vectors.",
            ],
            aliases: None,
        },
        Target {
            id: "gdscript",
            kind: TargetKind::Language,
            language_family: "Godot",
            status: TargetStatus::HttpOnly,
            remote: RemoteCapability {
                level: RemoteLevel::HttpProjection,
                qualification: Some(Qualification::Unqualified),
                streaming: &[],
                wire: WireKind::JsonHttp,
                notes: Some(
                    "Godot 4 HTTP client target; platform/library variant rather than a general SDK runtime.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Use a Rust WASM/native Godot extension for embedded behavior.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "OpenAPI Generator gdscript",
                version: "7.25.0",
                source: "https://openapi-generator.tech/docs/generators/gdscript/",
                license: "Apache-2.0",
                pin: "ef964b04480889ef86b56cfae84ade8ad4c91c41",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "gdscript",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "Godot asset/project",
                artifact: "acyclic_sdk",
                installable: false,
                notes: Some("Validate Godot 4 import and HTTP behavior."),
            },
            evidence: &[Evidence {
                url: "https://openapi-generator.tech/docs/generators/gdscript/",
                claim: "GDScript client generator targets Godot.",
            }],
            outstanding: &["Keep as a presentation/game integration target; no gRPC parity claim."],
            aliases: None,
        },
        Target {
            id: "julia",
            kind: TargetKind::Language,
            language_family: "Julia",
            status: TargetStatus::HttpOnly,
            remote: RemoteCapability {
                level: RemoteLevel::HttpProjection,
                qualification: Some(Qualification::Qualified),
                streaming: &[],
                wire: WireKind::JsonHttp,
                notes: Some(
                    "Rust-owned five-family HTTP projection is qualified by the Julia receipt; no maintained Julia gRPC generator is selected.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Rust native/WASM boundary only.",
            },
            maturity: Maturity::Beta,
            generator: Generator {
                name: "OpenAPI Generator julia-client",
                version: "7.25.0",
                source: "https://openapi-generator.tech/docs/generators/julia-client/",
                license: "Apache-2.0",
                pin: "ef964b04480889ef86b56cfae84ade8ad4c91c41",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/research/additional-languages/openapi-targets/produce-julia.ps1",
                    "-SourceRoot",
                    "{source_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "Julia General registry",
                artifact: "AcyclicSDK",
                installable: true,
                notes: Some(
                    "Rust-produced local package installs with official Julia 1.13.1; registry publication is not claimed.",
                ),
            },
            evidence: &[Evidence {
                url: "https://openapi-generator.tech/docs/generators/julia-client/",
                claim: "Julia client generator is listed as beta.",
            }],
            outstanding: &[
                "Native Julia gRPC remains unqualified; keep this target scoped to the recorded five-family HTTP projection.",
            ],
            aliases: None,
        },
        Target {
            id: "nim",
            kind: TargetKind::Language,
            language_family: "Nim",
            status: TargetStatus::HttpOnly,
            remote: RemoteCapability {
                level: RemoteLevel::HttpProjection,
                qualification: Some(Qualification::Unqualified),
                streaming: &[],
                wire: WireKind::JsonHttp,
                notes: Some("OAG Nim client; no selected maintained gRPC runtime."),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Rust C ABI only.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "OpenAPI Generator nim",
                version: "7.25.0",
                source: "https://openapi-generator.tech/docs/generators/nim/",
                license: "Apache-2.0",
                pin: "ef964b04480889ef86b56cfae84ade8ad4c91c41",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "nim",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "Nimble",
                artifact: "acyclic_sdk",
                installable: false,
                notes: Some("Requires Nimble package smoke."),
            },
            evidence: &[Evidence {
                url: "https://openapi-generator.tech/docs/generators/nim/",
                claim: "Nim client generator is listed.",
            }],
            outstanding: &["Qualify Nimble metadata and HTTP behavior before any package promise."],
            aliases: None,
        },
        Target {
            id: "perl",
            kind: TargetKind::Language,
            language_family: "Perl",
            status: TargetStatus::HttpOnly,
            remote: RemoteCapability {
                level: RemoteLevel::HttpProjection,
                qualification: Some(Qualification::Prototype),
                streaming: &[],
                wire: WireKind::JsonHttp,
                notes: Some(
                    "Rust-derived OpenAPI projections for Actors, Workers, Stream, Objects and Inference have installed Perl HTTP consumers; OAG remains JSON/HTTP-only and does not provide protobuf support.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Rust ABI only.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "OpenAPI Generator perl",
                version: "7.25.0",
                source: "https://openapi-generator.tech/docs/generators/perl/",
                license: "Apache-2.0",
                pin: "OpenAPI Generator 7.25.0 jar SHA-256 41CE4F6B07F196676439D710759FA1CED7A08066D06FF1BF314681470289EFAE",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "perl",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "CPAN",
                artifact: "acyclic-http-perl-0.1.0.zip",
                installable: true,
                notes: Some(
                    "Five-family HTTP package installs into an isolated local-lib-contained CPAN tree; receipt: research/additional-languages/openapi-targets/perl-manifest.json.",
                ),
            },
            evidence: &[
                Evidence {
                    url: "https://openapi-generator.tech/docs/generators/perl/",
                    claim: "Perl generator documents JSON/XML support and protobuf unsupported.",
                },
                Evidence {
                    url: "research/additional-languages/openapi-targets/perl-manifest.json",
                    claim: "Rust-derived five-family package installed and passed bounded bytes, uint64, bearer, stream recovery and HTTP error vectors.",
                },
            ],
            outstanding: &[
                "Extend qualification to every operation and model vector before making broader target claims.",
                "Review the 70 installed CPAN distribution license records before any redistribution claim; the closure receipt is separate from the HTTP package.",
                "The generated Perl HTTP client has no operation cancellation primitive; native cancellation and endpoint-pool recovery remain unqualified.",
            ],
            aliases: None,
        },
        Target {
            id: "powershell",
            kind: TargetKind::Language,
            language_family: "PowerShell",
            status: TargetStatus::HttpOnly,
            remote: RemoteCapability {
                level: RemoteLevel::HttpProjection,
                qualification: Some(Qualification::Prototype),
                streaming: &[],
                wire: WireKind::JsonHttp,
                notes: Some(
                    "Rust-owned adapted PowerShell HTTP modules are installed and loopback-qualified for one route in each of the five HTTP family projections; no protobuf/gRPC package target.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Shell can invoke a Rust CLI but does not embed behavior.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "OpenAPI Generator powershell",
                version: "7.25.0",
                source: "https://openapi-generator.tech/docs/generators/powershell/",
                license: "Apache-2.0",
                pin: "ef964b04480889ef86b56cfae84ade8ad4c91c41",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "powershell",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "PowerShell Gallery/module",
                artifact: "Acyclic.SDK",
                installable: true,
                notes: Some(
                    "Five local Acyclic<Family>Http module packages import under PowerShell 7.6.5; receipt: research/additional-languages/openapi-targets/powershell-manifest.json.",
                ),
            },
            evidence: &[
                Evidence {
                    url: "https://openapi-generator.tech/docs/generators/powershell/",
                    claim: "PowerShell generator is listed with HTTP client output.",
                },
                Evidence {
                    url: "research/additional-languages/openapi-targets/powershell-manifest.json",
                    claim: "Rust-derived five-family module packages were extracted and imported under PowerShell 7.6.5; representative bytes, uint64, authentication and 503 error vectors passed.",
                },
            ],
            outstanding: &[
                "Extend qualification to every operation and model vector in each Rust HTTP family projection.",
                "Review generated module and PowerShell/runtime dependency licenses before redistribution; keep the package local-only.",
                "PowerShell generated client has no native gRPC, endpoint-pool recovery or operation cancellation qualification.",
            ],
            aliases: None,
        },
        Target {
            id: "r",
            kind: TargetKind::Language,
            language_family: "R",
            status: TargetStatus::HttpOnly,
            remote: RemoteCapability {
                level: RemoteLevel::HttpProjection,
                qualification: Some(Qualification::Unqualified),
                streaming: &[],
                wire: WireKind::JsonHttp,
                notes: Some(
                    "OAG R feature table is JSON/XML and does not provide protobuf/gRPC parity.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Rust native binding not applicable to generated HTTP package.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "OpenAPI Generator r",
                version: "7.25.0",
                source: "https://openapi-generator.tech/docs/generators/r/",
                license: "Apache-2.0",
                pin: "ef964b04480889ef86b56cfae84ade8ad4c91c41",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "r",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "CRAN",
                artifact: "acyclicSDK",
                installable: false,
                notes: Some("Requires R package build/check smoke."),
            },
            evidence: &[Evidence {
                url: "https://openapi-generator.tech/docs/generators/r/",
                claim: "R client generator and CRAN-style output; protobuf unsupported in feature table.",
            }],
            outstanding: &["Qualify CRAN metadata, R serialization and HTTP error behavior."],
            aliases: None,
        },
        Target {
            id: "bash",
            kind: TargetKind::Tooling,
            language_family: "POSIX shell",
            status: TargetStatus::HttpOnly,
            remote: RemoteCapability {
                level: RemoteLevel::HttpProjection,
                qualification: Some(Qualification::Prototype),
                streaming: &[],
                wire: WireKind::JsonHttp,
                notes: Some(
                    "Rust-derived OpenAPI projections generate an installable Bash/curl bundle for all five explicit HTTP families; receipt: research/additional-languages/openapi-targets/bash-manifest.json.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Invoke a Rust CLI for embedded behavior instead.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "OpenAPI Generator bash",
                version: "7.25.0",
                source: "https://openapi-generator.tech/docs/generators/bash/",
                license: "Apache-2.0",
                pin: "OpenAPI Generator 7.25.0 jar SHA-256 41CE4F6B07F196676439D710759FA1CED7A08066D06FF1BF314681470289EFAE",
            },
            producer: Some(Producer {
                program: "pwsh",
                args: &[
                    "-NoProfile",
                    "-File",
                    "{source_root}/scripts/run-language-producer.ps1",
                    "-TargetId",
                    "bash",
                    "-SourceRoot",
                    "{source_root}",
                    "-WireRoot",
                    "{wire_root}",
                    "-AuthorityManifest",
                    "{authority_manifest}",
                    "-OutputRoot",
                    "{output_root}",
                    "-TargetOutput",
                    "{target_output}",
                    "-Request",
                    "{request}",
                ],
                output: "package",
            }),
            package: Package {
                ecosystem: "shell script",
                artifact: "acyclic-http-bash-0.1.0.zip",
                installable: true,
                notes: Some(
                    "Five-family HTTP projection bundle is installable locally; Stream evidence is polling plus explicit recovery error handling, with no automatic retry or gRPC/embedded claim.",
                ),
            },
            evidence: &[Evidence {
                url: "https://openapi-generator.tech/docs/generators/bash/",
                claim: "Bash/curl client generator is listed.",
            }],
            outstanding: &[
                "Extend qualification to a Rust-derived streaming-capable family before making broader transport claims; current Stream evidence is polling plus an explicit recovery error.",
                "Keep protobuf/gRPC and embedded behavior unqualified.",
            ],
            aliases: None,
        },
        Target {
            id: "haskell",
            kind: TargetKind::Language,
            language_family: "Haskell",
            status: TargetStatus::Candidate,
            remote: RemoteCapability {
                level: RemoteLevel::FullGrpc,
                qualification: Some(Qualification::Prototype),
                streaming: &[
                    StreamShape::Unary,
                    StreamShape::Client,
                    StreamShape::Server,
                    StreamShape::Bidi,
                ],
                wire: WireKind::ProtobufGrpc,
                notes: Some(
                    "Rust-generated protobuf services and descriptor-specific semantic GADTs compile with proto-lens and use grapesy HTTP/2 transport.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Rust FFI remains the only embedded path.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "proto-lens-protoc + grapesy",
                version: "0.9.0.1 + 1.2.1",
                source: "https://github.com/google/proto-lens",
                license: "BSD-3-Clause",
                pin: "GHC 9.2.8; Cabal 3.10.2.1; proto-lens-protoc 0.9.0.1 sha256:513e4338ca74b06929251f78e1c0cd1cc6b26d5e32e80cbcbcc23848ac16e446; grapesy 1.2.1 sha256:be40dda86d9a08d042504d9b94b4f8377ed08a9ce45fee413b8d0637a50951ca; research/additional-languages/haskell-grapesy-prototype/cabal.project.freeze",
            },
            producer: None,
            package: Package {
                ecosystem: "Hackage",
                artifact: "acyclic-haskell-grapesy-prototype",
                installable: true,
                notes: Some(
                    "Cabal source archive and installed typed consumer are produced by the pinned local Haskell toolchain.",
                ),
            },
            evidence: &[
                Evidence {
                    url: "https://github.com/google/proto-lens",
                    claim: "Maintained protobuf Haskell generator; pinned 0.9.0.1 source declares BSD-3-Clause.",
                },
                Evidence {
                    url: "https://github.com/well-typed/grapesy",
                    claim: "Maintained HTTP/2 gRPC client/server supports unary and all streaming shapes; upstream publishes interoperability results.",
                },
                Evidence {
                    url: "research/additional-languages/haskell-grapesy-prototype/provenance.json",
                    claim: "Pinned toolchain archives and compiled Rust-derived 106 RPC / 18 service surface; descriptor-specific known oneof payloads use a closed GADT.",
                },
            ],
            outstanding: &[
                "Register generation-only producer and final package identity.",
                "Complete source-bound installed 111-step conformance, streaming, cancellation, recovery and TLS qualification.",
            ],
            aliases: None,
        },
        Target {
            id: "lua",
            kind: TargetKind::Language,
            language_family: "Lua",
            status: TargetStatus::NotQualifiable,
            remote: RemoteCapability {
                level: RemoteLevel::None,
                qualification: Some(Qualification::Blocked),
                streaming: &[],
                wire: WireKind::None,
                notes: Some(
                    "lua-protobuf provides serialization but no maintained gRPC runtime; OAG Lua output is beta HTTP-only.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Rust native/WASM boundary only.",
            },
            maturity: Maturity::Experimental,
            generator: Generator {
                name: "lua-protobuf / OpenAPI Generator lua",
                version: "lua-protobuf 0.5.3; OAG 7.25.0",
                source: "https://github.com/starwing/lua-protobuf",
                license: "MIT/Apache-2.0",
                pin: "Separate pins; neither supplies full remote parity.",
            },
            producer: None,
            package: Package {
                ecosystem: "LuaRocks",
                artifact: "acyclic-sdk",
                installable: false,
                notes: Some("Serialization-only and HTTP projection inventory."),
            },
            evidence: &[
                Evidence {
                    url: "https://github.com/starwing/lua-protobuf",
                    claim: "Lua 5.1-5.4/LuaJIT protobuf encoding/decoding through LuaRocks.",
                },
                Evidence {
                    url: "https://openapi-generator.tech/docs/generators/lua/",
                    claim: "Lua OpenAPI client is listed as beta.",
                },
            ],
            outstanding: &[
                "Do not qualify until a maintained Lua gRPC runtime and generated service plugin exist.",
            ],
            aliases: None,
        },
        Target {
            id: "docs-and-execution-targets",
            kind: TargetKind::Tooling,
            language_family: "multi-language",
            status: TargetStatus::AdapterOnly,
            remote: RemoteCapability {
                level: RemoteLevel::HttpProjection,
                qualification: Some(Qualification::Unqualified),
                streaming: &[],
                wire: WireKind::JsonHttp,
                notes: Some(
                    "OpenAPI Generator also lists k6, JMeter, Terraform provider and documentation targets; these are not SDK languages.",
                ),
            },
            embedded: EmbeddedCapability {
                level: EmbeddedLevel::None,
                qualification: Some(Qualification::Unqualified),
                notes: "Generated docs/examples must call qualified SDKs or the Rust-owned HTTP projection.",
            },
            maturity: Maturity::Stable,
            generator: Generator {
                name: "OpenAPI Generator docs/tooling templates",
                version: "7.25.0",
                source: "https://openapi-generator.tech/docs/generators/",
                license: "Apache-2.0",
                pin: "ef964b04480889ef86b56cfae84ade8ad4c91c41",
            },
            producer: None,
            package: Package {
                ecosystem: "generated artifact",
                artifact: "docs/snippets",
                installable: false,
                notes: Some("Keep separate from language package inventory."),
            },
            evidence: &[Evidence {
                url: "https://openapi-generator.tech/docs/generators/",
                claim: "Official matrix distinguishes client, server, documentation and non-language generators.",
            }],
            outstanding: &[
                "Compile/execute snippets only against matching generated package artifacts and mark HTTP projection limits.",
            ],
            aliases: None,
        },
    ],
    catalogued_at: "2026-10-03",
};

pub fn catalog() -> serde_json::Value {
    serde_json::to_value(&CATALOG).expect("static Rust target catalog is serializable")
}

/// Compatibility projection for consumers of the former package inventory.
/// Installation qualification remains receipt-driven; this metadata describes
/// Rust-owned package identities and whether a producer is implemented.
#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PackageInventory {
    #[serde(rename = "$schema")]
    schema: &'static str,
    families: BTreeMap<&'static str, PackageIdentity>,
}

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PackageIdentity {
    registry: &'static str,
    umbrella: &'static str,
    implemented: bool,
}

pub fn package_names() -> serde_json::Value {
    let families = CATALOG
        .targets
        .iter()
        .map(|target| {
            (
                target.id,
                PackageIdentity {
                    registry: target.package.ecosystem,
                    umbrella: target.package.artifact,
                    implemented: target.producer.is_some()
                        || matches!(target.id, "rust" | "typescript" | "python"),
                },
            )
        })
        .collect();
    serde_json::to_value(PackageInventory {
        schema: "../compatibility/schemas/package-names.schema.json",
        families,
    })
    .expect("Rust package inventory is serializable")
}

pub fn package_schema() -> serde_json::Value {
    let schema = schemars::generate::SchemaSettings::draft2020_12()
        .for_serialize()
        .into_generator()
        .into_root_schema_for::<PackageInventory>();
    let mut value = serde_json::to_value(schema).expect("derived package schema is serializable");
    value["$id"] = serde_json::json!("https://acyclic.dev/schemas/sdk-package-names.json");
    value
}

pub fn schema() -> serde_json::Value {
    let schema = schemars::generate::SchemaSettings::draft2020_12()
        .for_serialize()
        .into_generator()
        .into_root_schema_for::<Catalog>();
    let mut value = serde_json::to_value(schema).expect("derived catalog schema is serializable");
    // Draft 2020-12 validators do not define the OpenAPI uint32 format.
    // The Rust-derived integer type and explicit bounds carry this constraint.
    value["properties"]["schema_version"]
        .as_object_mut()
        .expect("catalog schema version is an object")
        .remove("format");
    value["$id"] = serde_json::json!("https://sdk.acyclic.dev/generation-targets.v1.json");
    value["title"] = serde_json::json!("Acyclic SDK generation targets");
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn all_target_recipes_have_unique_identities_and_exact_generator_pins() {
        let mut identities = BTreeSet::new();
        for target in CATALOG.targets {
            assert!(
                identities.insert(target.id),
                "duplicate target {}",
                target.id
            );
            assert!(!target.id.is_empty());
            assert!(
                !target.generator.version.is_empty(),
                "{} has no version",
                target.id
            );
            assert!(!target.generator.pin.is_empty(), "{} has no pin", target.id);
            assert!(!target.package.ecosystem.is_empty());
            assert!(!target.package.artifact.is_empty());
        }
    }

    #[test]
    fn catalog_schema_uses_standard_integer_constraints() {
        let generated = schema();
        let version = &generated["properties"]["schema_version"];
        assert_eq!(version["type"], "integer");
        assert_eq!(version["minimum"], 1);
        assert_eq!(version["maximum"], 1);
        assert!(version.get("format").is_none());
        assert_eq!(generated["additionalProperties"], false);
    }

    #[test]
    fn checked_in_projection_matches_compiled_rust_catalog() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let projection: serde_json::Value = serde_json::from_slice(
            &std::fs::read(root.join("languages/generation-targets.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            projection,
            catalog(),
            "regenerate the JSON projection from Rust"
        );
    }
}

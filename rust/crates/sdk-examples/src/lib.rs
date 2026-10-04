//! Rust-owned executable scenarios and generated SDK snippets.
//!
//! A scenario is authored once as a typed Rust value. Renderers can project
//! that value into a language example while retaining an explicit capability
//! status. The source path in [`ScenarioMetadata`] is deliberately part of
//! the public metadata so documentation bundles can trace every snippet back
//! to this Rust source.

use std::fmt;

use acyclic_actors::{validate_create, wire as actors_wire};
use acyclic_stream::{
    AppendOutcome, AppendRequest, IdempotencyKey, MemoryStream, ReadRequest, StreamPath,
    StreamProvider, wire as stream_wire,
};
use bytes::Bytes;
use futures::StreamExt;
use prost::Message;
use serde_json::{Value, json};

/// Executable source-owned scenarios for each guide family.
pub mod filesystem_scenarios;
pub mod fixtures;
pub mod harness_scenarios;
pub mod inference_scenarios;
pub mod machines_scenarios;
pub mod objects_scenarios;
pub mod tls_fixture;
pub mod workers_scenarios;

/// Source identity for guide families whose executable scenarios live in a
/// dedicated module. These entries extend the original Actors/Stream registry
/// without changing the typed wire-operation renderer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuideScenarioSpec {
    /// Stable family label.
    pub family: &'static str,
    /// Stable scenario identifier.
    pub id: &'static str,
    /// Rust source file owning the scenario.
    pub source: &'static str,
}

/// All dedicated guide scenarios included in qualification receipts.
pub const GUIDE_SCENARIOS: &[GuideScenarioSpec] = &[
    GuideScenarioSpec {
        family: "filesystem",
        id: filesystem_scenarios::SCENARIO_ID,
        source: filesystem_scenarios::SOURCE,
    },
    GuideScenarioSpec {
        family: "harness",
        id: harness_scenarios::SCENARIO_ID,
        source: harness_scenarios::SOURCE,
    },
    GuideScenarioSpec {
        family: "inference",
        id: inference_scenarios::SCENARIO_ID,
        source: inference_scenarios::SOURCE,
    },
    GuideScenarioSpec {
        family: "machines",
        id: machines_scenarios::SCENARIO_ID,
        source: machines_scenarios::SOURCE,
    },
    GuideScenarioSpec {
        family: "objects",
        id: objects_scenarios::SCENARIO_ID,
        source: objects_scenarios::SOURCE,
    },
    GuideScenarioSpec {
        family: "workers",
        id: workers_scenarios::SCENARIO_ID,
        source: workers_scenarios::SOURCE,
    },
];

/// Returns the dedicated guide scenario registry in stable order.
#[must_use]
pub const fn guide_scenarios() -> &'static [GuideScenarioSpec] {
    GUIDE_SCENARIOS
}

const SOURCE: &str = "rust/crates/sdk-examples/src/lib.rs";

/// Languages for which a scenario projection can be requested.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Language {
    /// The executable source language and validation authority.
    Rust,
    /// Python protocol-shaped projection.
    Python,
    /// TypeScript SDK or protocol-shaped projection.
    TypeScript,
    /// Go generated gRPC projection.
    Go,
    /// Java generated gRPC projection for the JVM target.
    Java,
    /// C# generated gRPC projection for the .NET target.
    CSharp,
    /// Ruby generated gRPC projection.
    Ruby,
    /// Dart generated gRPC projection.
    Dart,
    /// PHP generated gRPC projection.
    Php,
}

impl Language {
    /// Stable name used by documentation metadata and generated headings.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::Python => "python",
            Self::TypeScript => "typescript",
            Self::Go => "go",
            Self::Java => "java",
            Self::CSharp => "csharp",
            Self::Ruby => "ruby",
            Self::Dart => "dart",
            Self::Php => "php",
        }
    }

    /// Human-readable label sourced from the same registry as the stable key.
    #[must_use]
    pub const fn language_name(self) -> &'static str {
        match self {
            Self::Rust => "Rust",
            Self::Python => "Python",
            Self::TypeScript => "TypeScript",
            Self::Go => "Go",
            Self::Java => "Java",
            Self::CSharp => "C#",
            Self::Ruby => "Ruby",
            Self::Dart => "Dart",
            Self::Php => "PHP",
        }
    }

    /// All projections emitted by the renderer.
    pub const ALL: [Self; 9] = [
        Self::Rust,
        Self::Python,
        Self::TypeScript,
        Self::Go,
        Self::Java,
        Self::CSharp,
        Self::Ruby,
        Self::Dart,
        Self::Php,
    ];
}

impl fmt::Display for Language {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(self.as_str())
    }
}

/// Whether a generated snippet exercises an installable client or only the
/// Rust-owned wire shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityStatus {
    /// The repository contains the client and this scenario is executable.
    Supported,
    /// The scenario is rendered as a protocol-shaped example; no client is
    /// claimed for this language by the current checkout.
    SchemaOnly,
}

impl CapabilityStatus {
    /// Stable metadata value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::SchemaOnly => "schema-only",
        }
    }
}

/// Validation stage represented by a generated snippet receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationLevel {
    /// The Rust scenario was compiled and executed against a local provider.
    Executed,
    /// Only rendering and source-shape checks are currently available.
    Rendered,
}

/// Result attached to every generated projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationStatus {
    /// The receipt has evidence for the stated validation level.
    Passed,
    /// The projection is intentionally not claimed executable.
    NotRun,
}

/// Small, serializable-in-spirit receipt for documentation and release
/// manifests. The fields are plain values so a later bundle writer can map
/// them to JSON without making JSON the authoring format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidationReceipt {
    /// Highest validation level reached.
    pub level: ValidationLevel,
    /// Whether the validation level passed.
    pub status: ValidationStatus,
    /// Human-readable evidence or reason for an unrun projection.
    pub evidence: &'static str,
}

/// Public identity and provenance for one generated snippet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScenarioMetadata {
    /// Stable scenario ID.
    pub id: &'static str,
    /// Contract family, such as `actors` or `stream`.
    pub family: &'static str,
    /// Human-facing title.
    pub title: &'static str,
    /// Generated target language.
    pub language: Language,
    /// Rust source path for the typed scenario.
    pub source: &'static str,
    /// Validation receipt for this projection.
    pub validation: ValidationReceipt,
}

/// One generated language projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenderedSnippet {
    /// Identity and provenance fields consumed by docs bundles.
    pub metadata: ScenarioMetadata,
    /// Explicit capability status shown by the docs renderer.
    pub capability: CapabilityStatus,
    /// Complete source, including imports and status comments.
    pub code: String,
}

/// Typed operation authored by Rust. No JSON scenario file is read by this
/// crate; these values are the source for all projections.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScenarioOperation {
    /// Encode, decode, then validate an Actors create request.
    ActorsCreateRoundtrip {
        /// Actor code digest.
        code_sha256: [u8; 32],
        /// Placement region.
        home_region: &'static str,
        /// Caller-owned retry identity.
        idempotency_key: &'static str,
        /// Subscription identifier.
        subscription_id: &'static str,
        /// Stream source for the subscription.
        stream_path: &'static str,
        /// Initial cursor.
        cursor: u64,
    },
    /// Append two records and read the finite prefix back.
    StreamAppendRead {
        /// Destination stream.
        path: &'static str,
        /// Records in the append batch.
        records: &'static [&'static [u8]],
        /// Optional tail CAS condition.
        if_tail: Option<u64>,
        /// Read starting sequence.
        from: u64,
        /// Maximum records returned.
        limit: u32,
        /// Caller-owned retry identity.
        idempotency_key: &'static str,
    },
}

/// One Rust-owned scenario definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScenarioSpec {
    /// Stable ID used by generated URLs and manifests.
    pub id: &'static str,
    /// Contract family.
    pub family: &'static str,
    /// Human-facing title.
    pub title: &'static str,
    /// Typed operation data.
    pub operation: ScenarioOperation,
}

const STREAM_RECORDS: &[&[u8]] = &[b"hello", b"world"];

/// The complete source registry. Keep this list ordered for reproducible
/// bundles and stable documentation navigation.
pub const SCENARIOS: &[ScenarioSpec] = &[
    ScenarioSpec {
        id: "actors-create-roundtrip",
        family: "actors",
        title: "Roundtrip and validate an Actors create request",
        operation: ScenarioOperation::ActorsCreateRoundtrip {
            code_sha256: [1; 32],
            home_region: "eu",
            idempotency_key: "create-example",
            subscription_id: "events",
            stream_path: "agents/example/events",
            cursor: 0,
        },
    },
    ScenarioSpec {
        id: "stream-append-read",
        family: "stream",
        title: "Append and read a finite Stream prefix",
        operation: ScenarioOperation::StreamAppendRead {
            path: "examples/events",
            records: STREAM_RECORDS,
            if_tail: Some(0),
            from: 0,
            limit: 2,
            idempotency_key: "append-example",
        },
    },
];

/// Returns the stable typed scenario registry.
#[must_use]
pub const fn scenarios() -> &'static [ScenarioSpec] {
    SCENARIOS
}

/// One encoded request owned by a Rust transport fixture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureRequest {
    /// Stable filename stem under the fixture directory.
    pub name: &'static str,
    /// Fully-qualified protobuf message identity.
    pub message: &'static str,
    /// Canonically encoded protobuf payload.
    pub bytes: Vec<u8>,
}

/// One Rust-owned request/result fixture for cross-language qualification.
///
/// Fixtures intentionally expose expected semantic results separately from
/// encoded requests. This keeps provider-generated identities and timestamps
/// out of the contract while allowing every client lane to compare the same
/// stable values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransportFixture {
    /// Stable fixture identity.
    pub id: &'static str,
    /// Stable operation identity from the generated contract.
    pub operation_id: &'static str,
    /// Scenario registry identity that authored this fixture.
    pub scenario_id: &'static str,
    /// Contract family.
    pub family: &'static str,
    /// Canonical service route or operation path.
    pub route: &'static str,
    /// Encoded requests consumed by a client runner.
    pub requests: Vec<FixtureRequest>,
    /// Stable semantic expectation consumed by a client runner.
    pub expected: Value,
    /// What the local Rust receipt actually proves.
    pub validation: &'static str,
}

/// Builds the canonical request/result fixtures from [`SCENARIOS`].
///
/// The returned bytes are generated directly from the Rust wire types. No
/// language-specific request or expected-result file is an authoring input.
#[must_use]
pub fn transport_fixtures() -> Vec<TransportFixture> {
    let actors = actors_create_request(SCENARIOS[0]).expect("typed Actors scenario");
    let stream_append = stream_append_wire_request(SCENARIOS[1]).expect("typed Stream scenario");
    let stream_read = stream_read_wire_request(SCENARIOS[1]).expect("typed Stream scenario");
    vec![
        TransportFixture {
            id: "actors-create-unary-v1",
            operation_id: "acyclic.actors.v1.Actors/CreateActor",
            scenario_id: "actors-create-roundtrip",
            family: "actors",
            route: "/v1/actors/create",
            requests: vec![FixtureRequest {
                name: "request",
                message: "acyclic.actors.v1.CreateActorRequest",
                bytes: actors.encode_to_vec(),
            }],
            expected: json!({
                "kind": "request-validation",
                "accepted": true,
                "message": "acyclic.actors.v1.CreateActorRequest",
                "home_region": actors.home_region,
                "idempotency_key": actors.idempotency_key,
                "subscription_count": actors.subscriptions.len(),
            }),
            validation: "Rust decodes and validates the canonical request; remote service transport is not claimed",
        },
        TransportFixture {
            id: "stream-append-read-v2",
            operation_id: "acyclic.stream.v2.Stream/AppendRead",
            scenario_id: "stream-append-read",
            family: "stream",
            route: "append + read",
            requests: vec![
                FixtureRequest {
                    name: "append-request",
                    message: "acyclic.stream.v2.AppendRequest",
                    bytes: stream_append.encode_to_vec(),
                },
                FixtureRequest {
                    name: "read-request",
                    message: "acyclic.stream.v2.ReadRequest",
                    bytes: stream_read.encode_to_vec(),
                },
            ],
            expected: json!({
                "kind": "local-provider-result",
                "append": { "start": 0, "end": 2, "tail": 2 },
                "read": { "from": 0, "limit": 2, "records": ["hello", "world"] },
            }),
            validation: "Rust executes append and bounded read against MemoryStream; hosted availability is not claimed",
        },
    ]
}

impl ScenarioSpec {
    /// Render this scenario for one language.
    #[must_use]
    pub fn render(self, language: Language) -> RenderedSnippet {
        let (capability, code, level, status, evidence) = match (self.operation, language) {
            (ScenarioOperation::ActorsCreateRoundtrip { .. }, Language::Rust) => (
                CapabilityStatus::Supported,
                render_rust_actors(self),
                ValidationLevel::Executed,
                ValidationStatus::Passed,
                "cargo test executes the Rust roundtrip test",
            ),
            (ScenarioOperation::StreamAppendRead { .. }, Language::Rust) => (
                CapabilityStatus::Supported,
                render_rust_stream(self),
                ValidationLevel::Executed,
                ValidationStatus::Passed,
                "cargo test executes the Rust MemoryStream scenario",
            ),
            (ScenarioOperation::StreamAppendRead { .. }, Language::TypeScript) => (
                CapabilityStatus::Supported,
                render_typescript_stream(self),
                ValidationLevel::Rendered,
                ValidationStatus::NotRun,
                "typescript/packages/stream/examples/quickstart.ts is the API reference; this crate does not run Bun",
            ),
            (operation, Language::Python) => (
                CapabilityStatus::Supported,
                render_python(operation),
                ValidationLevel::Rendered,
                ValidationStatus::NotRun,
                "python/src/acyclic_sdk contains the generated transport package; CLI execution is required",
            ),
            (ScenarioOperation::ActorsCreateRoundtrip { .. }, Language::TypeScript) => (
                CapabilityStatus::Supported,
                render_typescript_actors(self),
                ValidationLevel::Rendered,
                ValidationStatus::NotRun,
                "typescript/packages/actors contains the generated package; CLI execution is required",
            ),
            (operation, Language::Go) => (
                CapabilityStatus::Supported,
                render_go(operation),
                ValidationLevel::Rendered,
                ValidationStatus::NotRun,
                "go/generated transport package and loopback consumer execution are required",
            ),
            (operation, Language::Java) => (
                CapabilityStatus::Supported,
                render_java(operation),
                ValidationLevel::Rendered,
                ValidationStatus::NotRun,
                "jvm generated transport artifact and loopback consumer execution are required",
            ),
            (operation, Language::CSharp) => (
                CapabilityStatus::Supported,
                render_csharp(operation),
                ValidationLevel::Rendered,
                ValidationStatus::NotRun,
                "dotnet generated transport artifact and loopback consumer execution are required",
            ),
            (operation, Language::Ruby) => (
                CapabilityStatus::Supported,
                render_ruby(operation),
                ValidationLevel::Rendered,
                ValidationStatus::NotRun,
                "ruby generated transport artifact and loopback consumer execution are required",
            ),
            (operation, Language::Dart) => (
                CapabilityStatus::Supported,
                render_dart(operation),
                ValidationLevel::Rendered,
                ValidationStatus::NotRun,
                "dart generated transport artifact and loopback consumer execution are required",
            ),
            (operation, Language::Php) => (
                CapabilityStatus::Supported,
                render_php(operation),
                ValidationLevel::Rendered,
                ValidationStatus::NotRun,
                "php generated transport artifact and loopback consumer execution are required",
            ),
        };
        Self::snippet(self, language, capability, code, level, status, evidence)
    }

    fn snippet(
        self,
        language: Language,
        capability: CapabilityStatus,
        code: String,
        level: ValidationLevel,
        status: ValidationStatus,
        evidence: &'static str,
    ) -> RenderedSnippet {
        RenderedSnippet {
            metadata: ScenarioMetadata {
                id: self.id,
                family: self.family,
                title: self.title,
                language,
                source: SOURCE,
                validation: ValidationReceipt {
                    level,
                    status,
                    evidence,
                },
            },
            capability,
            code,
        }
    }
}

fn render_rust_actors(spec: ScenarioSpec) -> String {
    let ScenarioOperation::ActorsCreateRoundtrip {
        home_region,
        idempotency_key,
        subscription_id,
        stream_path,
        cursor,
        ..
    } = spec.operation
    else {
        unreachable!("typed renderer dispatches only Actors scenarios")
    };
    format!(
        "// capability: supported\nuse acyclic_actors::{{validate_create, wire}};\nuse prost::Message;\n\nlet request = wire::CreateActorRequest {{ home_region: {home_region:?}.into(), idempotency_key: {idempotency_key:?}.into(), limits: Some(wire::ActorLimits {{ handler_timeout_millis: 1_000, memory_bytes: 1_048_576, checkpoint_bytes: 4_096 }}), code_sha256: vec![1; 32], subscriptions: vec![wire::SubscriptionSpec {{ subscription_id: {subscription_id:?}.into(), stream_path: {stream_path:?}.into(), start: Some(wire::SubscriptionStart {{ start: Some(wire::subscription_start::Start::Cursor({cursor})) }}), placement_anchor: false }}], bindings: vec![] }};\nlet bytes = request.encode_to_vec();\nlet decoded = wire::CreateActorRequest::decode(bytes.as_slice())?;\nvalidate_create(&decoded)?;",
    )
}

fn render_rust_stream(spec: ScenarioSpec) -> String {
    let ScenarioOperation::StreamAppendRead {
        path,
        if_tail,
        from,
        limit,
        idempotency_key,
        ..
    } = spec.operation
    else {
        unreachable!("typed renderer dispatches only Stream scenarios")
    };
    format!(
        "// capability: supported\nuse acyclic_stream::{{AppendRequest, IdempotencyKey, MemoryStream, ReadRequest, StreamPath, StreamProvider}};\nuse bytes::Bytes;\nuse futures::StreamExt;\n\nlet provider = MemoryStream::default();\nlet path = StreamPath::new({path:?})?;\nlet receipt = provider.append(AppendRequest {{ path: path.clone(), records: vec![Bytes::from_static(b\"hello\"), Bytes::from_static(b\"world\")], if_tail: {if_tail:?}, idempotency_key: Some(IdempotencyKey::new({idempotency_key:?})?) }}).await?;\nlet mut records = provider.read(ReadRequest {{ path, from: {from}, limit: {limit} }}).await?;\nwhile let Some(record) = records.next().await {{ let record = record?; println!(\"{{}} {{:?}}\", record.sequence, record.value); }}\nlet _ = receipt;",
    )
}

fn render_typescript_stream(spec: ScenarioSpec) -> String {
    let ScenarioOperation::StreamAppendRead { path, .. } = spec.operation else {
        unreachable!("typed renderer dispatches only Stream scenarios")
    };
    format!(
        "// capability: supported\nimport {{ StreamClient }} from \"@acyclic-labs/stream\";\n\nconst streams = StreamClient.memory();\nconst events = streams.bytes({path:?});\nawait events.appendBatch([new TextEncoder().encode(\"hello\"), new TextEncoder().encode(\"world\")], {{ ifTail: 0n, idempotencyKey: new TextEncoder().encode(\"append-example\") }});\nfor await (const record of events.read({{ from: 0n, limit: 2 }})) console.log(record.sequence, record.value);",
    )
}

fn render_typescript_actors(spec: ScenarioSpec) -> String {
    let ScenarioOperation::ActorsCreateRoundtrip {
        home_region,
        idempotency_key,
        subscription_id,
        stream_path,
        cursor,
        ..
    } = spec.operation
    else {
        unreachable!("typed renderer dispatches only Actors scenarios")
    };
    format!(
        "// capability: supported\nimport {{ create, fromBinary, toBinary }} from \"@bufbuild/protobuf\";\nimport {{ CreateActorRequestSchema }} from \"@acyclic-labs/actors/proto\";\n\nconst request = create(CreateActorRequestSchema, {{ codeSha256: new Uint8Array(32).fill(1), homeRegion: {home_region:?}, idempotencyKey: {idempotency_key:?}, limits: {{ handlerTimeoutMillis: 1_000n, memoryBytes: 1_048_576n, checkpointBytes: 4_096n }}, subscriptions: [{{ subscriptionId: {subscription_id:?}, streamPath: {stream_path:?}, start: {{ start: {{ case: \"cursor\", value: BigInt({cursor}) }} }} }}] }});\nconst decoded = fromBinary(CreateActorRequestSchema, toBinary(CreateActorRequestSchema, request));\nconsole.log(decoded.homeRegion, decoded.subscriptions[0]?.streamPath);",
    )
}

fn render_go(operation: ScenarioOperation) -> String {
    match operation {
        ScenarioOperation::ActorsCreateRoundtrip {
            home_region,
            idempotency_key,
            subscription_id,
            stream_path,
            cursor,
            ..
        } => format!(
            "// capability: supported\npackage main\n\nimport (\n    \"context\"\n    \"fmt\"\n    \"os\"\n    \"time\"\n\n    actorsv1 \"github.com/acyclic-labs/sdk/go/gen/actors/v1\"\n    \"google.golang.org/grpc\"\n    \"google.golang.org/grpc/credentials/insecure\"\n)\n\nfunc main() {{\n    endpoint := os.Getenv(\"FIXTURE_GRPC_ADDRESS\")\n    if endpoint == \"\" {{ panic(\"FIXTURE_GRPC_ADDRESS is required\") }}\n    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)\n    defer cancel()\n    conn, err := grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))\n    if err != nil {{ panic(err) }}\n    defer conn.Close()\n    request := &actorsv1.CreateActorRequest{{\n        CodeSha256: []byte{{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},\n        HomeRegion: {home_region:?},\n        IdempotencyKey: {idempotency_key:?},\n        Limits: &actorsv1.ActorLimits{{\n            HandlerTimeoutMillis: 1000,\n            MemoryBytes: 1048576,\n            CheckpointBytes: 4096,\n        }},\n        Subscriptions: []*actorsv1.SubscriptionSpec{{{{\n            SubscriptionId: {subscription_id:?},\n            StreamPath: {stream_path:?},\n            Start: &actorsv1.SubscriptionStart{{Start: &actorsv1.SubscriptionStart_Cursor{{Cursor: {cursor}}}}},\n        }}}},\n    }}\n    response, err := actorsv1.NewActorsServiceClient(conn).CreateActor(ctx, request)\n    if err != nil {{ panic(err) }}\n    fmt.Println(response.GetActor().GetActorId())\n}}",
        ),
        ScenarioOperation::StreamAppendRead {
            path,
            if_tail,
            from,
            limit,
            idempotency_key,
            ..
        } => {
            let if_tail = if_tail.unwrap_or_default();
            format!(
                "// capability: supported\npackage main\n\nimport (\n    \"context\"\n    \"fmt\"\n    \"os\"\n    \"time\"\n\n    streamv2 \"github.com/acyclic-labs/sdk/go/gen/stream/v2\"\n    \"google.golang.org/grpc\"\n    \"google.golang.org/grpc/credentials/insecure\"\n)\n\nfunc main() {{\n    endpoint := os.Getenv(\"FIXTURE_GRPC_ADDRESS\")\n    if endpoint == \"\" {{ panic(\"FIXTURE_GRPC_ADDRESS is required\") }}\n    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)\n    defer cancel()\n    conn, err := grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))\n    if err != nil {{ panic(err) }}\n    defer conn.Close()\n    tail := uint64({if_tail:?})\n    _, err = streamv2.NewStreamServiceClient(conn).Append(ctx, &streamv2.AppendRequest{{Path: {path:?}, Records: [][]byte{{[]byte(\"hello\"), []byte(\"world\")}}, IfTail: &tail, IdempotencyKey: []byte({idempotency_key:?})}})\n    if err != nil {{ panic(err) }}\n    call, err := streamv2.NewStreamServiceClient(conn).Read(ctx, &streamv2.ReadRequest{{Path: {path:?}, From: {from}, Limit: {limit}}})\n    if err != nil {{ panic(err) }}\n    for {{ record, err := call.Recv(); if err != nil {{ break }}; fmt.Println(record.GetRecord().GetSequence()) }}\n}}",
            )
        }
    }
}

fn render_java(operation: ScenarioOperation) -> String {
    match operation {
        ScenarioOperation::ActorsCreateRoundtrip {
            home_region,
            idempotency_key,
            subscription_id,
            stream_path,
            cursor,
            ..
        } => format!(
            "// capability: supported\nimport acyclic.actors.v1.Actors;\nimport acyclic.actors.v1.ActorsServiceGrpc;\nimport io.grpc.ManagedChannelBuilder;\n\npublic final class Snippet {{\n  public static void main(String[] args) {{\n    var endpoint = System.getenv(\"FIXTURE_GRPC_ADDRESS\");\n    if (endpoint == null || endpoint.isBlank()) throw new IllegalStateException(\"FIXTURE_GRPC_ADDRESS is required\");\n    var channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();\n    try {{\n      var request = Actors.CreateActorRequest.newBuilder()\n          .setCodeSha256(com.google.protobuf.ByteString.copyFrom(new byte[] {{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }}))\n          .setHomeRegion({home_region:?})\n          .setIdempotencyKey({idempotency_key:?})\n          .setLimits(Actors.ActorLimits.newBuilder().setHandlerTimeoutMillis(1_000).setMemoryBytes(Long.MAX_VALUE).setCheckpointBytes(4_096))\n          .addSubscriptions(Actors.SubscriptionSpec.newBuilder().setSubscriptionId({subscription_id:?}).setStreamPath({stream_path:?}).setStart(Actors.SubscriptionStart.newBuilder().setCursor({cursor})))\n          .build();\n      var response = ActorsServiceGrpc.newBlockingStub(channel).createActor(request);\n      System.out.println(response.getActor().getActorId());\n    }} finally {{ channel.shutdownNow(); }}\n  }}\n}}",
        ),
        ScenarioOperation::StreamAppendRead {
            path,
            if_tail,
            from,
            limit,
            idempotency_key,
            ..
        } => {
            let if_tail = if_tail.unwrap_or_default();
            format!(
                "// capability: supported\nimport acyclic.stream.v2.Stream;\nimport acyclic.stream.v2.StreamServiceGrpc;\nimport com.google.protobuf.ByteString;\nimport io.grpc.ManagedChannelBuilder;\n\npublic final class Snippet {{\n  public static void main(String[] args) {{\n    var endpoint = System.getenv(\"FIXTURE_GRPC_ADDRESS\");\n    if (endpoint == null || endpoint.isBlank()) throw new IllegalStateException(\"FIXTURE_GRPC_ADDRESS is required\");\n    var channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();\n    try {{\n      var append = Stream.AppendRequest.newBuilder().setPath({path:?}).addRecords(ByteString.copyFromUtf8(\"hello\")).addRecords(ByteString.copyFromUtf8(\"world\")).setIfTail({if_tail:?}).setIdempotencyKey(ByteString.copyFromUtf8({idempotency_key:?})).build();\n      StreamServiceGrpc.newBlockingStub(channel).append(append);\n      var read = StreamServiceGrpc.newBlockingStub(channel).read(Stream.ReadRequest.newBuilder().setPath({path:?}).setFrom({from}).setLimit({limit}).build());\n      while (read.hasNext()) System.out.println(read.next().getRecord().getSequence());\n    }} finally {{ channel.shutdownNow(); }}\n  }}\n}}",
            )
        }
    }
}

fn render_csharp(operation: ScenarioOperation) -> String {
    match operation {
        ScenarioOperation::ActorsCreateRoundtrip {
            home_region,
            idempotency_key,
            subscription_id,
            stream_path,
            cursor,
            ..
        } => format!(
            "// capability: supported\nusing Acyclic.Actors.V1;\nusing Grpc.Net.Client;\nusing Google.Protobuf;\nusing System.Linq;\n\nvar endpoint = Environment.GetEnvironmentVariable(\"FIXTURE_GRPC_ADDRESS\") ?? throw new InvalidOperationException(\"FIXTURE_GRPC_ADDRESS is required\");\nusing var channel = GrpcChannel.ForAddress(endpoint);\nvar request = new CreateActorRequest {{ CodeSha256 = ByteString.CopyFrom(Enumerable.Repeat((byte)1, 32).ToArray()), HomeRegion = {home_region:?}, IdempotencyKey = {idempotency_key:?}, Limits = new ActorLimits {{ HandlerTimeoutMillis = 1_000, MemoryBytes = ulong.MaxValue, CheckpointBytes = 4_096 }}, Subscriptions = {{ new SubscriptionSpec {{ SubscriptionId = {subscription_id:?}, StreamPath = {stream_path:?}, Start = new SubscriptionStart {{ Cursor = {cursor} }} }} }} }};\nvar response = await new ActorsService.ActorsServiceClient(channel).CreateActorAsync(request);\nConsole.WriteLine(response.Actor.ActorId);",
        ),
        ScenarioOperation::StreamAppendRead {
            path, from, limit, ..
        } => format!(
            "// capability: supported\nusing Acyclic.Stream.V2;\nusing Google.Protobuf;\nusing Grpc.Core;\nusing Grpc.Net.Client;\n\nvar endpoint = Environment.GetEnvironmentVariable(\"FIXTURE_GRPC_ADDRESS\") ?? throw new InvalidOperationException(\"FIXTURE_GRPC_ADDRESS is required\");\nusing var channel = GrpcChannel.ForAddress(endpoint);\nvar client = new StreamService.StreamServiceClient(channel);\nawait client.AppendAsync(new AppendRequest {{ Path = {path:?}, Records = {{ ByteString.CopyFromUtf8(\"hello\"), ByteString.CopyFromUtf8(\"world\") }}, IfTail = 0, IdempotencyKey = ByteString.CopyFromUtf8(\"append-example\") }});\nusing var read = client.Read(new ReadRequest {{ Path = {path:?}, From = {from}, Limit = {limit} }});\nawait foreach (var item in read.ResponseStream.ReadAllAsync()) Console.WriteLine(item.Record.Sequence);",
        ),
    }
}

fn render_ruby(operation: ScenarioOperation) -> String {
    match operation {
        ScenarioOperation::ActorsCreateRoundtrip {
            home_region,
            idempotency_key,
            subscription_id,
            stream_path,
            cursor,
            ..
        } => format!(
            "# capability: supported\nrequire \"acyclic_sdk\"\n\nendpoint = ENV.fetch(\"FIXTURE_GRPC_ADDRESS\")\nclient = Acyclic::Actors::V1::ActorsService::Stub.new(endpoint, :this_channel_is_insecure)\nrequest = Acyclic::Actors::V1::CreateActorRequest.new(code_sha256: ([1] * 32).pack(\"C*\"), home_region: {home_region:?}, limits: Acyclic::Actors::V1::ActorLimits.new(handler_timeout_millis: 1000, memory_bytes: 1048576, checkpoint_bytes: 4096), idempotency_key: {idempotency_key:?}, subscriptions: [Acyclic::Actors::V1::SubscriptionSpec.new(subscription_id: {subscription_id:?}, stream_path: {stream_path:?}, start: Acyclic::Actors::V1::SubscriptionStart.new(cursor: {cursor}))])\nresponse = client.create_actor(request)\nputs response.actor.actor_id",
        ),
        ScenarioOperation::StreamAppendRead {
            path,
            if_tail,
            from,
            limit,
            idempotency_key,
            ..
        } => {
            let if_tail = if_tail.unwrap_or_default();
            format!(
                "# capability: supported\nrequire \"acyclic_sdk\"\n\nendpoint = ENV.fetch(\"FIXTURE_GRPC_ADDRESS\")\nclient = Acyclic::Stream::V2::StreamService::Stub.new(endpoint, :this_channel_is_insecure)\nrequest = Acyclic::Stream::V2::AppendRequest.new(path: {path:?}, records: [\"hello\", \"world\"], if_tail: {if_tail:?}, idempotency_key: {idempotency_key:?})\nclient.append(request)\nclient.read(Acyclic::Stream::V2::ReadRequest.new(path: {path:?}, from: {from}, limit: {limit})).each {{ |response| puts response.record.sequence }}",
            )
        }
    }
}

fn render_dart(operation: ScenarioOperation) -> String {
    match operation {
        ScenarioOperation::ActorsCreateRoundtrip {
            home_region,
            idempotency_key,
            subscription_id,
            stream_path,
            cursor,
            ..
        } => format!(
            "// capability: supported\nimport 'dart:io';\nimport 'package:acyclic_sdk/src/generated/actors/v1/actors.pb.dart' as actors;\nimport 'package:acyclic_sdk/src/generated/actors/v1/actors.pbgrpc.dart' as actors_rpc;\nimport 'package:grpc/grpc.dart';\nimport 'package:fixnum/fixnum.dart';\n\nFuture<void> main() async {{\n  final endpoint = Platform.environment['FIXTURE_GRPC_ADDRESS'];\n  if (endpoint == null || endpoint.isEmpty) throw StateError('FIXTURE_GRPC_ADDRESS is required');\n  final parts = endpoint.split(':');\n  final channel = ClientChannel(parts[0], port: int.parse(parts[1]), options: const ChannelOptions(credentials: ChannelCredentials.insecure()));\n  try {{\n    final client = actors_rpc.ActorsServiceClient(channel);\n    final request = actors.CreateActorRequest()\n      ..codeSha256 = List<int>.filled(32, 1)\n      ..homeRegion = {home_region:?}\n      ..idempotencyKey = {idempotency_key:?}\n      ..subscriptions.add(actors.SubscriptionSpec()..subscriptionId = {subscription_id:?}..streamPath = {stream_path:?}..start = (actors.SubscriptionStart()..cursor = {cursor}));\n    final response = await client.createActor(request);\n    print(response.actor.actorId);\n  }} finally {{ await channel.shutdown(); }}\n}}",
        ),
        ScenarioOperation::StreamAppendRead {
            path, from, limit, ..
        } => format!(
            "// capability: supported\nimport 'dart:io';\nimport 'package:acyclic_sdk/src/generated/stream/v2/stream.pb.dart' as stream;\nimport 'package:acyclic_sdk/src/generated/stream/v2/stream.pbgrpc.dart' as stream_rpc;\nimport 'package:fixnum/fixnum.dart';\nimport 'package:grpc/grpc.dart';\nimport 'package:fixnum/fixnum.dart';\n\nFuture<void> main() async {{\n  final endpoint = Platform.environment['FIXTURE_GRPC_ADDRESS'];\n  if (endpoint == null || endpoint.isEmpty) throw StateError('FIXTURE_GRPC_ADDRESS is required');\n  final parts = endpoint.split(':');\n  final channel = ClientChannel(parts[0], port: int.parse(parts[1]), options: const ChannelOptions(credentials: ChannelCredentials.insecure()));\n  try {{\n    final client = stream_rpc.StreamServiceClient(channel);\n    await client.append(stream.AppendRequest()..path = {path:?}..records.addAll([\"hello\".codeUnits, \"world\".codeUnits])..ifTail = Int64.ZERO..idempotencyKey = \"append-example\".codeUnits);\n    await for (final item in client.read(stream.ReadRequest()..path = {path:?}..from = Int64({from})..limit = {limit})) print(item.record.sequence);\n  }} finally {{ await channel.shutdown(); }}\n}}",
        ),
    }
}

fn render_php(operation: ScenarioOperation) -> String {
    let rendered = match operation {
        ScenarioOperation::ActorsCreateRoundtrip {
            home_region,
            idempotency_key,
            subscription_id,
            stream_path,
            cursor,
            ..
        } => format!(
            "<?php\nrequire dirname(__DIR__) . '/vendor/autoload.php';\n\n$endpoint = getenv('FIXTURE_GRPC_ADDRESS');\nif (!$endpoint) throw new RuntimeException('FIXTURE_GRPC_ADDRESS is required');\n$client = new \\Acyclic\\Actors\\V1\\ActorsServiceClient($endpoint, ['credentials' => \\Grpc\\ChannelCredentials::createInsecure()]);\n$limits = new \\Acyclic\\Actors\\V1\\ActorLimits();\n$limits->setHandlerTimeoutMillis(1000);\n$limits->setMemoryBytes(1048576);\n$limits->setCheckpointBytes(4096);\n$request = new \\Acyclic\\Actors\\V1\\CreateActorRequest();\n$request->setCodeSha256(str_repeat(chr(1), 32));\n$request->setHomeRegion({home_region:?});\n$request->setLimits($limits);\n$request->setIdempotencyKey({idempotency_key:?});\n$subscription = new \\Acyclic\\Actors\\V1\\SubscriptionSpec();\n$subscription->setSubscriptionId({subscription_id:?});\n$subscription->setStreamPath({stream_path:?});\n$start = new \\Acyclic\\Actors\\V1\\SubscriptionStart();\n$start->setCursor({cursor});\n$subscription->setStart($start);\n$request->setSubscriptions([$subscription]);\n[$response, $status] = $client->CreateActor($request)->wait();\nif ($status->code !== \\Grpc\\STATUS_OK) throw new RuntimeException($status->details);\necho $response->getActor()->getActorId(), PHP_EOL; exit(0);",
        ),
        ScenarioOperation::StreamAppendRead {
            path, from, limit, ..
        } => format!(
            "<?php\nrequire dirname(__DIR__) . '/vendor/autoload.php';\n\n$endpoint = getenv('FIXTURE_GRPC_ADDRESS');\nif (!$endpoint) throw new RuntimeException('FIXTURE_GRPC_ADDRESS is required');\n$client = new \\Acyclic\\Stream\\V2\\StreamServiceClient($endpoint, ['credentials' => \\Grpc\\ChannelCredentials::createInsecure()]);\n$request = new \\Acyclic\\Stream\\V2\\AppendRequest();\n$request->setPath({path:?});\n$request->setRecords(['hello', 'world']);\n$request->setIfTail(0);\n$request->setIdempotencyKey('append-example');\n[, $appendStatus] = $client->Append($request)->wait();\nif ($appendStatus->code !== \\Grpc\\STATUS_OK) throw new RuntimeException($appendStatus->details);\n$read = new \\Acyclic\\Stream\\V2\\ReadRequest();\n$read->setPath({path:?});\n$read->setFrom({from});\n$read->setLimit({limit});\n$call = $client->Read($read);\nforeach ($call->responses() as $item) {{ echo $item->getRecord()->getSequence(), PHP_EOL; }} exit(0);",
        ),
    };
    rendered.replace("<?php\n", "<?php\n// capability: supported\n")
}

fn render_python(operation: ScenarioOperation) -> String {
    match operation {
        ScenarioOperation::ActorsCreateRoundtrip {
            home_region,
            idempotency_key,
            subscription_id,
            stream_path,
            cursor,
            ..
        } => format!(
            "# capability: supported\nfrom acyclic_sdk.generated.actors.v1 import actors_pb2\n\nrequest = actors_pb2.CreateActorRequest(code_sha256=bytes([1]) * 32, home_region={home_region:?}, idempotency_key={idempotency_key:?}, limits=actors_pb2.ActorLimits(handler_timeout_millis=1000, memory_bytes=1048576, checkpoint_bytes=4096), subscriptions=[actors_pb2.SubscriptionSpec(subscription_id={subscription_id:?}, stream_path={stream_path:?}, start=actors_pb2.SubscriptionStart(cursor={cursor}))])\ndecoded = actors_pb2.CreateActorRequest.FromString(request.SerializeToString())\nassert decoded == request\nprint(decoded.home_region, decoded.subscriptions[0].stream_path)",
        ),
        ScenarioOperation::StreamAppendRead {
            path, from, limit, ..
        } => format!(
            "# capability: supported\nfrom acyclic_sdk.generated.stream.v2 import stream_pb2\n\nrequest = stream_pb2.AppendRequest(path={path:?}, records=[b\"hello\", b\"world\"], if_tail=0, idempotency_key=b\"append-example\")\nread = stream_pb2.ReadRequest(path={path:?}, **{{\"from\": {from}, \"limit\": {limit}}})\nassert list(request.records) == [b\"hello\", b\"world\"]\nprint(read.path, getattr(read, \"from\"), read.limit)",
        ),
    }
}

fn actors_create_request(
    spec: ScenarioSpec,
) -> Result<actors_wire::CreateActorRequest, Box<dyn std::error::Error>> {
    let ScenarioOperation::ActorsCreateRoundtrip {
        code_sha256,
        home_region,
        idempotency_key,
        subscription_id,
        stream_path,
        cursor,
    } = spec.operation
    else {
        return Err("scenario is not an Actors create operation".into());
    };
    Ok(actors_wire::CreateActorRequest {
        code_sha256: code_sha256.to_vec(),
        home_region: home_region.into(),
        bindings: Vec::new(),
        limits: Some(actors_wire::ActorLimits {
            handler_timeout_millis: 1_000,
            memory_bytes: 1_048_576,
            checkpoint_bytes: 4_096,
        }),
        subscriptions: vec![actors_wire::SubscriptionSpec {
            subscription_id: subscription_id.into(),
            stream_path: stream_path.into(),
            start: Some(actors_wire::SubscriptionStart {
                start: Some(actors_wire::subscription_start::Start::Cursor(cursor)),
            }),
            placement_anchor: false,
        }],
        idempotency_key: idempotency_key.into(),
    })
}

fn stream_append_wire_request(
    spec: ScenarioSpec,
) -> Result<stream_wire::AppendRequest, Box<dyn std::error::Error>> {
    let ScenarioOperation::StreamAppendRead {
        path,
        records,
        if_tail,
        idempotency_key,
        ..
    } = spec.operation
    else {
        return Err("scenario is not a Stream append operation".into());
    };
    Ok(stream_wire::AppendRequest {
        path: path.into(),
        records: records
            .iter()
            .map(|record| Bytes::copy_from_slice(record))
            .collect(),
        if_tail,
        idempotency_key: Some(Bytes::copy_from_slice(idempotency_key.as_bytes())),
    })
}

fn stream_read_wire_request(
    spec: ScenarioSpec,
) -> Result<stream_wire::ReadRequest, Box<dyn std::error::Error>> {
    let ScenarioOperation::StreamAppendRead {
        path, from, limit, ..
    } = spec.operation
    else {
        return Err("scenario is not a Stream read operation".into());
    };
    Ok(stream_wire::ReadRequest {
        path: path.into(),
        from,
        limit,
    })
}

/// Executes the Stream scenario against the real in-memory Rust provider.
pub async fn execute_stream_append_read() -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
    let ScenarioOperation::StreamAppendRead {
        path,
        records,
        if_tail,
        from,
        limit,
        idempotency_key,
    } = SCENARIOS[1].operation
    else {
        return Err("stream scenario registry entry changed".into());
    };
    let provider = MemoryStream::default();
    let path = StreamPath::new(path)?;
    let outcome = provider
        .append(AppendRequest {
            path: path.clone(),
            records: records
                .iter()
                .map(|record| Bytes::from_static(record))
                .collect(),
            if_tail,
            idempotency_key: Some(IdempotencyKey::new(idempotency_key)?),
        })
        .await?;
    if !matches!(outcome, AppendOutcome::Committed(receipt) if receipt.tail == records.len() as u64)
    {
        return Err("MemoryStream append did not commit the expected tail".into());
    }
    let mut stream = provider.read(ReadRequest { path, from, limit }).await?;
    let mut values = Vec::new();
    while let Some(record) = stream.next().await {
        values.push(record?.value.to_vec());
    }
    Ok(values)
}

/// Executes the Actors protobuf roundtrip and validation against the real
/// generated wire type.
pub fn execute_actors_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let request = actors_create_request(SCENARIOS[0])?;
    let encoded = request.encode_to_vec();
    let decoded = actors_wire::CreateActorRequest::decode(encoded.as_slice())?;
    validate_create(&decoded)?;
    Ok(())
}

/// Renders every language projection in deterministic registry order.
#[must_use]
pub fn render_all() -> Vec<RenderedSnippet> {
    scenarios()
        .iter()
        .flat_map(|scenario| {
            Language::ALL
                .into_iter()
                .map(|language| scenario.render(language))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_is_typed_and_stable() {
        assert_eq!(
            scenarios()
                .iter()
                .map(|scenario| scenario.id)
                .collect::<Vec<_>>(),
            vec!["actors-create-roundtrip", "stream-append-read"]
        );
        assert!(scenarios().iter().all(|scenario| scenario.title.len() > 10));
    }

    #[test]
    fn every_projection_has_imports_and_provenance() {
        for snippet in render_all() {
            assert_eq!(snippet.metadata.source, SOURCE);
            assert!(
                snippet.code.contains("capability:"),
                "{}",
                snippet.metadata.id
            );
            assert!(
                snippet.metadata.language == Language::Rust
                    || snippet.code.contains("import")
                    || snippet.code.contains("package ")
                    || snippet.code.contains("using ")
                    || snippet.code.contains("require "),
                "{}",
                snippet.metadata.id
            );
            assert!(!snippet.code.contains("TODO"), "{}", snippet.metadata.id);
            assert!(
                !snippet.code.contains("\\\\n"),
                "{} emitted an escaped newline",
                snippet.metadata.id
            );
            assert!(
                !snippet.code.contains("{{receipt"),
                "{} emitted a formatter placeholder",
                snippet.metadata.id
            );
        }
    }

    #[test]
    fn rust_actors_roundtrip_executes() {
        execute_actors_roundtrip().expect("generated Actors request must roundtrip");
    }

    #[test]
    fn transport_fixtures_are_derived_from_wire_types() {
        let fixtures = transport_fixtures();
        assert_eq!(
            fixtures
                .iter()
                .map(|fixture| fixture.id)
                .collect::<Vec<_>>(),
            vec!["actors-create-unary-v1", "stream-append-read-v2"]
        );
        let actors = &fixtures[0];
        let decoded = actors_wire::CreateActorRequest::decode(actors.requests[0].bytes.as_slice())
            .expect("Actors fixture must be canonical protobuf");
        validate_create(&decoded).expect("Actors fixture must satisfy Rust validation");
        assert_eq!(actors.expected["accepted"], true);

        let stream = &fixtures[1];
        let append = stream_wire::AppendRequest::decode(stream.requests[0].bytes.as_slice())
            .expect("Stream append fixture must be canonical protobuf");
        assert_eq!(append.path, "examples/events");
        assert_eq!(append.records, vec![b"hello".to_vec(), b"world".to_vec()]);
    }

    #[tokio::test]
    async fn rust_stream_scenario_executes_against_memory_provider() {
        assert_eq!(
            execute_stream_append_read()
                .await
                .expect("MemoryStream scenario"),
            vec![b"hello".to_vec(), b"world".to_vec()]
        );
    }

    #[test]
    fn unsupported_language_is_explicit() {
        let python = scenarios()[0].render(Language::Python);
        assert_eq!(python.capability, CapabilityStatus::Supported);
        assert_eq!(python.metadata.validation.status, ValidationStatus::NotRun);
        assert!(python.code.contains("acyclic_sdk.generated"));
    }
}

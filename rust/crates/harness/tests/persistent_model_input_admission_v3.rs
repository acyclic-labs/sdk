#![cfg(feature = "filesystem-local")]
#![allow(clippy::too_many_lines)]

//! Persistent, black-box model-input qualification.
//!
//! Every request asserted here is captured at the `ModelProvider::generate`
//! boundary.  The fixture opens the public persistent composition, so the
//! request is produced by the LocalStream/LocalFs-backed executor rather than
//! by a test-only context builder.

use acyclic_harness::{
    conversation::{FileDescriptor, FileRef, Limits},
    executor::ExecutionEvent,
    filesystem::PersistentLocalHarness,
    model::{
        Model, ModelAttempt, ModelContent, ModelContentPart, ModelEvent, ModelMessage,
        ModelOptionPolicy, ModelProvider, ModelRequest,
    },
    model_input::{ModelInputManifest, PreparedModelInput},
    registry::ComponentIdentity,
    tool::ToolDefinition,
    Error, OperationId, Result,
};
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tempfile::tempdir;

#[derive(Clone, Copy)]
enum FixtureMode {
    Complete,
    MalformedToolCall,
    ValidToolSequence,
    OutputOverflow,
}

struct CapturingProvider {
    mode: FixtureMode,
    calls: AtomicUsize,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    policy: Option<ModelOptionPolicy>,
    provider_private_credential: Option<String>,
}

impl CapturingProvider {
    fn new(
        mode: FixtureMode,
        policy: Option<ModelOptionPolicy>,
        provider_private_credential: Option<String>,
    ) -> (Arc<Self>, Arc<Mutex<Vec<Vec<u8>>>>) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(Self {
                mode,
                calls: AtomicUsize::new(0),
                requests: requests.clone(),
                policy,
                provider_private_credential,
            }),
            requests,
        )
    }

    fn complete() -> (Arc<Self>, Arc<Mutex<Vec<Vec<u8>>>>) {
        Self::new(FixtureMode::Complete, None, None)
    }

    fn complete_with_private_credential(credential: &str) -> (Arc<Self>, Arc<Mutex<Vec<Vec<u8>>>>) {
        Self::new(FixtureMode::Complete, None, Some(credential.to_owned()))
    }

    fn malformed() -> (Arc<Self>, Arc<Mutex<Vec<Vec<u8>>>>) {
        Self::new(FixtureMode::MalformedToolCall, None, None)
    }

    fn valid_tool_sequence() -> (Arc<Self>, Arc<Mutex<Vec<Vec<u8>>>>) {
        Self::new(FixtureMode::ValidToolSequence, None, None)
    }

    fn output_overflow() -> (Arc<Self>, Arc<Mutex<Vec<Vec<u8>>>>) {
        Self::new(FixtureMode::OutputOverflow, None, None)
    }

    fn with_policy(policy: ModelOptionPolicy) -> (Arc<Self>, Arc<Mutex<Vec<Vec<u8>>>>) {
        Self::new(FixtureMode::Complete, Some(policy), None)
    }

    fn private_credential(&self) -> Option<&str> {
        self.provider_private_credential.as_deref()
    }
}

impl ModelProvider for CapturingProvider {
    fn model_option_policy(&self) -> Option<&ModelOptionPolicy> {
        self.policy.as_ref()
    }

    fn generate<'a>(&'a self, prepared: PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
        // This is the only request observation used by the tests.  Capturing
        // `bytes` here prevents an earlier reconstructed request from masking
        // a production serialization or admission bug.
        if let Some(credential) = &self.provider_private_credential {
            assert!(!credential.is_empty());
            assert!(!prepared
                .bytes()
                .windows(credential.len())
                .any(|window| window == credential.as_bytes()));
        }
        self.requests
            .lock()
            .expect("request capture lock")
            .push(prepared.bytes().to_vec());
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let events = match self.mode {
            FixtureMode::Complete => vec![
                Ok(ModelEvent::Content {
                    delta: "done λ🦀\r\n".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ],
            FixtureMode::MalformedToolCall => vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "malformed-stage".into(),
                    name: "acyclic.stage_file".into(),
                    arguments: json!({"path": 42, "text": "must be rejected"}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ],
            FixtureMode::ValidToolSequence if call == 0 => vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "stage-1".into(),
                    name: "acyclic.stage_file".into(),
                    arguments: json!({
                        "path": "notes/tool.txt",
                        "text": "tool result λ🦀",
                        "media_type": "text/plain",
                        "display_name": "tool.txt"
                    }),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ],
            FixtureMode::ValidToolSequence => vec![
                Ok(ModelEvent::Content {
                    delta: "tool exchange complete".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ],
            FixtureMode::OutputOverflow => vec![
                Ok(ModelEvent::Content {
                    delta: "x".repeat(128 * 1024),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ],
        };
        Box::pin(stream::iter(events))
    }

    fn reconcile<'a>(&'a self, _: ModelAttempt) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

fn model(options: Value) -> Result<Model> {
    Model::new("mock", "persistent-blackbox", "2026-10-04", options)
}

fn operation(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16])
}

fn captured(requests: &Arc<Mutex<Vec<Vec<u8>>>>) -> Vec<Vec<u8>> {
    requests.lock().expect("request capture lock").clone()
}

fn assert_request_is_exact(bytes: &[u8]) -> Result<ModelRequest> {
    let request: ModelRequest =
        serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    let prepared = PreparedModelInput::prepare(request.clone(), Limits::default())?;
    assert_eq!(
        bytes,
        prepared.bytes(),
        "provider bytes must be canonical admitted bytes"
    );
    assert_eq!(
        *blake3::hash(bytes).as_bytes(),
        prepared.manifest().request_digest,
        "provider bytes must match the admitted request digest"
    );
    Ok(request)
}

fn independent_canonical_json(value: &Value, bytes: &mut Vec<u8>) {
    match value {
        Value::Null => bytes.extend_from_slice(b"null"),
        Value::Bool(value) => bytes.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(value) => bytes.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => {
            bytes.extend_from_slice(serde_json::to_string(value).unwrap().as_bytes())
        }
        Value::Array(values) => {
            bytes.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    bytes.push(b',');
                }
                independent_canonical_json(value, bytes);
            }
            bytes.push(b']');
        }
        Value::Object(fields) => {
            bytes.push(b'{');
            let mut keys = fields.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    bytes.push(b',');
                }
                bytes.extend_from_slice(serde_json::to_string(key).unwrap().as_bytes());
                bytes.push(b':');
                independent_canonical_json(&fields[key], bytes);
            }
            bytes.push(b'}');
        }
    }
}

fn independent_digest(value: &Value) -> [u8; 32] {
    let mut bytes = Vec::new();
    independent_canonical_json(value, &mut bytes);
    *blake3::hash(&bytes).as_bytes()
}

fn expected_tool_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "acyclic.list_files".into(),
            revision: "1".into(),
            description: "List one bounded, generation-pinned page of the agent-private volume"
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "expected_generation": {"type": ["object", "null"]},
                    "after": {"type": ["string", "null"]},
                    "maximum_entries": {"type": "integer", "minimum": 1, "maximum": 64}
                },
                "required": ["path", "maximum_entries"],
                "additionalProperties": false
            }),
            output_schema: json!({
                "type": "object",
                "properties": {
                    "generation": {"type": "object"},
                    "entries": {"type": "array", "items": {"type": "object"}},
                    "has_more": {"type": "boolean"},
                    "next_after": {"type": ["string", "null"]}
                },
                "required": ["generation", "entries", "has_more", "next_after"],
                "additionalProperties": false
            }),
            model_output_schema: json!({
                "type": "object",
                "properties": {
                    "generation": {"type": "object"},
                    "entries": {"type": "array", "items": {"type": "object"}},
                    "has_more": {"type": "boolean"},
                    "next_after": {"type": ["string", "null"]}
                },
                "required": ["generation", "entries", "has_more", "next_after"],
                "additionalProperties": false
            }),
        },
        ToolDefinition {
            name: "acyclic.read_file".into(),
            revision: "1".into(),
            description: "Read bounded UTF-8 bytes from an authorized immutable FileRef".into(),
            input_schema: json!({
                "type": "object",
                "properties": {"file": {"type": "object"}},
                "required": ["file"],
                "additionalProperties": false
            }),
            output_schema: json!({
                "type": "object",
                "properties": {
                    "file": {"type": "object"},
                    "text": {"type": "string"}
                },
                "required": ["file", "text"],
                "additionalProperties": false
            }),
            model_output_schema: json!({"type": "string"}),
        },
        ToolDefinition {
            name: "acyclic.stage_file".into(),
            revision: "1".into(),
            description:
                "Stage a bounded UTF-8 file in the agent-private volume and return its immutable FileRef"
                    .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "text": {"type": "string"},
                    "media_type": {"type": "string"},
                    "display_name": {"type": "string"}
                },
                "required": ["path", "text", "media_type", "display_name"],
                "additionalProperties": false
            }),
            output_schema: json!({
                "type": "object",
                "properties": {"file": {"type": "object"}},
                "required": ["file"],
                "additionalProperties": false
            }),
            model_output_schema: json!({
                "type": "object",
                "properties": {"file": {"type": "object"}},
                "required": ["file"],
                "additionalProperties": false
            }),
        },
    ]
}

const EXPECTED_TOOL_DIGESTS: [[u8; 32]; 3] = [
    [
        255, 201, 32, 110, 227, 198, 143, 244, 25, 148, 43, 127, 12, 155, 217, 45, 133, 176, 215,
        102, 50, 237, 178, 216, 69, 211, 210, 245, 235, 20, 196, 219,
    ],
    [
        68, 110, 55, 113, 124, 217, 75, 169, 146, 228, 151, 218, 111, 70, 189, 96, 74, 39, 206, 12,
        78, 187, 99, 150, 157, 159, 6, 75, 195, 230, 92, 18,
    ],
    [
        1, 252, 178, 68, 58, 106, 247, 126, 121, 15, 84, 134, 70, 224, 178, 53, 2, 130, 254, 206,
        0, 143, 217, 110, 205, 97, 84, 255, 225, 181, 122, 5,
    ],
];

const EXPECTED_TOOL_SCHEMA_DIGESTS: [[[u8; 32]; 3]; 3] = [
    [
        [
            178, 98, 26, 180, 243, 138, 212, 244, 71, 232, 43, 250, 81, 13, 120, 195, 167, 168, 79,
            154, 9, 87, 40, 16, 242, 74, 131, 62, 243, 11, 184, 179,
        ],
        [
            168, 21, 138, 156, 155, 116, 68, 113, 179, 198, 191, 68, 90, 84, 32, 116, 47, 179, 41,
            162, 91, 244, 90, 103, 144, 168, 9, 57, 249, 10, 168, 222,
        ],
        [
            168, 21, 138, 156, 155, 116, 68, 113, 179, 198, 191, 68, 90, 84, 32, 116, 47, 179, 41,
            162, 91, 244, 90, 103, 144, 168, 9, 57, 249, 10, 168, 222,
        ],
    ],
    [
        [
            107, 183, 68, 19, 75, 57, 66, 174, 235, 186, 243, 173, 212, 198, 101, 42, 192, 200,
            241, 246, 104, 233, 188, 51, 174, 240, 6, 77, 86, 14, 225, 246,
        ],
        [
            155, 108, 47, 106, 13, 105, 205, 193, 196, 151, 35, 134, 34, 56, 93, 143, 248, 138, 45,
            223, 188, 107, 134, 50, 160, 155, 226, 219, 165, 191, 133, 30,
        ],
        [
            134, 77, 255, 124, 157, 233, 245, 47, 131, 11, 178, 240, 167, 233, 81, 80, 246, 235,
            12, 194, 52, 217, 117, 190, 232, 23, 127, 167, 135, 9, 75, 222,
        ],
    ],
    [
        [
            227, 227, 57, 7, 113, 114, 174, 166, 145, 52, 146, 85, 24, 121, 61, 235, 116, 146, 134,
            94, 208, 62, 79, 62, 184, 97, 54, 42, 208, 63, 223, 156,
        ],
        [
            107, 183, 68, 19, 75, 57, 66, 174, 235, 186, 243, 173, 212, 198, 101, 42, 192, 200,
            241, 246, 104, 233, 188, 51, 174, 240, 6, 77, 86, 14, 225, 246,
        ],
        [
            107, 183, 68, 19, 75, 57, 66, 174, 235, 186, 243, 173, 212, 198, 101, 42, 192, 200,
            241, 246, 104, 233, 188, 51, 174, 240, 6, 77, 86, 14, 225, 246,
        ],
    ],
];

const EXPECTED_BINDING_DIGEST: [u8; 32] = [
    146, 71, 57, 48, 107, 132, 160, 90, 189, 120, 218, 94, 0, 115, 93, 68, 33, 60, 242, 88, 117,
    153, 97, 92, 86, 4, 26, 153, 137, 196, 57, 112,
];

fn assert_request_allowlist(request: &ModelRequest) -> Result<()> {
    let json = serde_json::to_value(request).map_err(|error| Error::Invalid(error.to_string()))?;
    let object = json
        .as_object()
        .ok_or_else(|| Error::Invalid("model request is not an object".into()))?;
    let keys = object.keys().map(String::as_str).collect::<Vec<_>>();
    assert_eq!(
        keys,
        vec!["max_output_tokens", "messages", "model", "tools"]
    );

    let model = object
        .get("model")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::Invalid("model request model is not an object".into()))?;
    assert_eq!(
        model.keys().map(String::as_str).collect::<Vec<_>>(),
        vec!["name", "options", "provider", "revision"]
    );
    assert_eq!(model["options"], json!({}));

    let messages = object["messages"]
        .as_array()
        .ok_or_else(|| Error::Invalid("model request messages are not an array".into()))?;
    for message in messages {
        let message = message
            .as_object()
            .ok_or_else(|| Error::Invalid("model message is not an object".into()))?;
        assert_eq!(
            message.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["content", "role"]
        );
        let serialized_message = Value::Object(message.clone()).to_string();
        assert!(!serialized_message.contains("sibling-history"));
        assert!(!serialized_message.contains("ui_state"));
    }
    let tools = object["tools"]
        .as_array()
        .ok_or_else(|| Error::Invalid("model request tools are not an array".into()))?;
    let expected_tools = expected_tool_definitions();
    assert_eq!(
        request.tools, expected_tools,
        "ordered full tool definitions changed"
    );
    assert_eq!(tools.len(), EXPECTED_TOOL_DIGESTS.len());
    for (position, (tool, expected)) in request.tools.iter().zip(EXPECTED_TOOL_DIGESTS).enumerate()
    {
        assert_eq!(
            tool.digest()?,
            expected,
            "tool digest changed at position {position}"
        );
        let expected_schemas = EXPECTED_TOOL_SCHEMA_DIGESTS[position];
        assert_eq!(independent_digest(&tool.input_schema), expected_schemas[0]);
        assert_eq!(independent_digest(&tool.output_schema), expected_schemas[1]);
        assert_eq!(
            independent_digest(&tool.model_output_schema),
            expected_schemas[2]
        );
    }
    let prepared = PreparedModelInput::prepare(request.clone(), Limits::default())?;
    assert_eq!(
        prepared.manifest().binding_digest,
        EXPECTED_BINDING_DIGEST,
        "model/tool binding digest changed"
    );
    let serialized =
        serde_json::to_vec(request).map_err(|error| Error::Invalid(error.to_string()))?;
    for forbidden in [
        "transport_metadata",
        "runtime_credentials",
        "OPENAI_API_KEY",
        "sibling-history",
        "ui_state",
    ] {
        assert!(!serialized
            .windows(forbidden.len())
            .any(|w| w == forbidden.as_bytes()));
    }
    Ok(())
}

fn assert_manifest_matches_request(
    manifest: &ModelInputManifest,
    request: &ModelRequest,
    limits: Limits,
) -> Result<()> {
    let prepared = PreparedModelInput::prepare(request.clone(), limits)?;
    assert_eq!(manifest, prepared.manifest());
    assert_eq!(manifest.version, 3);
    assert_eq!(manifest.binding_digest, prepared.manifest().binding_digest);
    assert_eq!(manifest.request_digest, prepared.manifest().request_digest);
    assert_eq!(manifest.messages.len(), request.messages.len());
    for (position, (entry, message)) in manifest.messages.iter().zip(&request.messages).enumerate()
    {
        assert_eq!(entry.position, position);
        assert_eq!(entry.role, message.role);
        assert_eq!(entry.digest, prepared.manifest().messages[position].digest);
        assert_eq!(
            entry.files,
            message
                .content
                .file_refs()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        );
    }
    Ok(())
}

#[tokio::test]
async fn persistent_provider_receives_exact_unicode_request_and_cold_restart_replays_without_dispatch(
) -> Result<()> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider_secret = "provider-private-credential::synthetic";
    let (provider, requests) = CapturingProvider::complete_with_private_credential(provider_secret);
    assert_eq!(provider.private_credential(), Some(provider_secret));
    let model = model(json!({}))?;
    let limits = Limits::default();
    let prompt = "  preserve whitespace\tα🦀\r\n";
    let operation = operation(0x41);

    let session =
        PersistentLocalHarness::open(root.path(), model.clone(), provider.clone(), limits).await?;
    let sibling_fixture = session
        .storage()
        .stage(
            OperationId::from_bytes([0x49; 16]),
            "sibling/transcript.json",
            br#"{"messages":[{"role":"assistant","content":"private sibling transcript"}]}"#,
            "application/json",
            "transcript.json",
        )
        .await?;
    let ui_fixture = session
        .storage()
        .stage(
            OperationId::from_bytes([0x4c; 16]),
            "ui/state.json",
            br#"{"selectedTab":"hidden","draft":"ui-only state"}"#,
            "application/json",
            "state.json",
        )
        .await?;
    let first = session.run(operation, prompt).await?;
    assert!(first.text.contains("done"));
    let first_bytes = captured(&requests);
    assert_eq!(first_bytes.len(), 1, "one real provider dispatch expected");
    let first_request = assert_request_is_exact(&first_bytes[0])?;
    assert_request_allowlist(&first_request)?;
    let request_refs = first_request
        .messages
        .iter()
        .flat_map(|message| message.content.file_refs().into_iter().cloned())
        .collect::<Vec<_>>();
    assert!(
        request_refs
            .iter()
            .all(|reference| reference != &sibling_fixture && reference != &ui_fixture),
        "sibling transcript and UI-only fixtures must never enter model context"
    );
    let request_json =
        serde_json::to_vec(&first_request).map_err(|error| Error::Invalid(error.to_string()))?;
    for excluded_path in ["sibling/transcript.json", "ui/state.json"] {
        assert!(!request_json
            .windows(excluded_path.len())
            .any(|window| { window == excluded_path.as_bytes() }));
    }
    assert!(!first_bytes[0]
        .windows(provider_secret.len())
        .any(|window| window == provider_secret.as_bytes()));
    let prompt_file = first_request
        .messages
        .iter()
        .flat_map(|message| message.content.file_refs().into_iter().cloned())
        .next()
        .ok_or_else(|| {
            Error::Invalid("persistent prompt must remain a pinned file reference".into())
        })?;
    assert_eq!(
        session.storage().read(&prompt_file).await?,
        prompt.as_bytes()
    );
    assert!(
        !first_bytes[0]
            .windows(prompt.as_bytes().len())
            .any(|window| window == prompt.as_bytes()),
        "the provider request must retain the pinned content reference rather than silently retrieving bytes"
    );
    let first_json =
        serde_json::to_value(&first_request).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(first_json.get("transport_metadata").is_none());
    assert!(!first_bytes[0]
        .windows(b"ui_state".len())
        .any(|w| w == b"ui_state"));
    assert!(!first_bytes[0]
        .windows(b"sibling-history".len())
        .any(|w| w == b"sibling-history"));
    let records = session.storage().journal().replay(operation).await?;
    let (manifest_ref, request_ref) = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ModelInputPrepared {
                step: 0,
                manifest,
                request,
            } => Some((manifest.clone(), request.clone())),
            _ => None,
        })
        .ok_or_else(|| Error::Storage("persistent model input evidence is missing".into()))?;
    assert_eq!(
        session.storage().journal().load(&request_ref).await?,
        first_bytes[0]
    );
    let manifest: ModelInputManifest =
        serde_json::from_slice(&session.storage().journal().load(&manifest_ref).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_manifest_matches_request(&manifest, &first_request, limits)?;
    assert_eq!(
        manifest.request_digest,
        *blake3::hash(&first_bytes[0]).as_bytes()
    );
    let records = session.storage().journal().replay(operation).await?;
    for record in records {
        let event_bytes =
            serde_json::to_vec(&record.event).map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(!event_bytes
            .windows(provider_secret.len())
            .any(|window| window == provider_secret.as_bytes()));
        let references = match record.event {
            ExecutionEvent::ModelInputPrepared {
                manifest, request, ..
            } => vec![manifest, request],
            ExecutionEvent::ToolBatchCompleted { boundary, .. }
            | ExecutionEvent::BatchPublicationStarted {
                publication: boundary,
                ..
            }
            | ExecutionEvent::Model {
                event: boundary, ..
            } => vec![boundary],
            ExecutionEvent::ToolAdmissionRejected {
                invocation,
                feedback,
                ..
            } => feedback
                .into_iter()
                .chain(std::iter::once(invocation))
                .collect(),
            ExecutionEvent::ToolStarted { invocation, .. } => vec![invocation],
            ExecutionEvent::ToolCompleted {
                result, projection, ..
            } => vec![result, projection],
            ExecutionEvent::Started { .. }
            | ExecutionEvent::BatchPublicationCompleted { .. }
            | ExecutionEvent::ModelStarted { .. }
            | ExecutionEvent::ToolFailed { .. } => Vec::new(),
        };
        for reference in references {
            let bytes = session.storage().journal().load(&reference).await?;
            assert!(!bytes
                .windows(provider_secret.len())
                .any(|window| window == provider_secret.as_bytes()));
        }
    }
    drop(session);

    let reopened = PersistentLocalHarness::open(root.path(), model, provider, limits).await?;
    let replay = reopened.run(operation, prompt).await?;
    assert_eq!(replay, first, "restart must replay the durable outcome");
    assert_eq!(
        captured(&requests),
        first_bytes,
        "replay must not dispatch or rewrite input"
    );
    Ok(())
}

#[tokio::test]
async fn persistent_admission_rejects_malformed_tool_schema_before_filesystem_effect() -> Result<()>
{
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (provider, requests) = CapturingProvider::malformed();
    let session =
        PersistentLocalHarness::open(root.path(), model(json!({}))?, provider, Limits::default())
            .await?;
    let result = session
        .run_with_max_steps(operation(0x42), "reject malformed schema", 1)
        .await;
    assert!(
        result.is_err(),
        "malformed tool arguments must be a typed failure"
    );
    assert_eq!(
        captured(&requests).len(),
        1,
        "the malformed response is observed once"
    );
    let listed = session.list_private_directory("", None, None, 256).await?;
    assert!(
        listed
            .entries
            .iter()
            .all(|entry| !entry.name.contains("must-be-rejected")),
        "malformed stage_file must not create a workspace file"
    );
    let records = session.storage().journal().replay(operation(0x42)).await?;
    assert!(records.iter().any(|record| matches!(
        record.event,
        ExecutionEvent::ToolAdmissionRejected {
            reason: acyclic_harness::executor::ToolRejectionKind::InvalidArguments,
            ..
        }
    )));
    assert!(!records.iter().any(|record| matches!(
        record.event,
        ExecutionEvent::ToolStarted { .. } | ExecutionEvent::ToolCompleted { .. }
    )));
    Ok(())
}

#[tokio::test]
async fn persistent_tool_exchange_preserves_call_identity_result_pairing_and_journal_effect(
) -> Result<()> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (provider, requests) = CapturingProvider::valid_tool_sequence();
    let session =
        PersistentLocalHarness::open(root.path(), model(json!({}))?, provider, Limits::default())
            .await?;
    let operation = operation(0x4a);
    let output = session
        .run_with_max_steps(operation, "perform one staged tool call", 2)
        .await?;
    assert!(output.text.contains("tool exchange complete"));
    let requests = captured(&requests);
    assert_eq!(
        requests.len(),
        2,
        "tool call must be followed by one paired request"
    );
    let first = assert_request_is_exact(&requests[0])?;
    let second = assert_request_is_exact(&requests[1])?;
    assert_request_allowlist(&first)?;
    assert_request_allowlist(&second)?;
    let mut calls = Vec::new();
    let mut results = Vec::new();
    for message in &second.messages {
        match &message.content {
            ModelContent::Part(ModelContentPart::ToolCall {
                call_id,
                name,
                arguments,
            }) => calls.push((call_id.clone(), name.clone(), arguments.clone())),
            ModelContent::Part(ModelContentPart::ToolResult {
                call_id,
                name,
                value,
            }) => results.push((call_id.clone(), name.clone(), value.clone())),
            _ => {}
        }
    }
    assert_eq!(calls.len(), 1);
    assert_eq!(results.len(), 1);
    assert_eq!(
        calls[0].0, results[0].0,
        "tool result must pair by call identity"
    );
    assert_eq!(
        calls[0].1, results[0].1,
        "tool result must pair by tool name"
    );
    assert_eq!(calls[0].2["path"], "notes/tool.txt");
    assert!(
        results[0].2.get("file").is_some(),
        "stage result must satisfy output schema"
    );

    let records = session.storage().journal().replay(operation).await?;
    let completed = records.iter().find_map(|record| match &record.event {
        ExecutionEvent::ToolCompleted {
            call_id,
            result,
            projection,
            ..
        } => Some((call_id, result, projection)),
        _ => None,
    });
    let Some((call_id, result, projection)) = completed else {
        return Err(Error::Storage("paired tool completion is missing".into()));
    };
    assert_eq!(call_id, "stage-1");
    let result_json: Value =
        serde_json::from_slice(&session.storage().journal().load(result).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let projection_json: Value =
        serde_json::from_slice(&session.storage().journal().load(projection).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(result_json["value"], results[0].2);
    assert_eq!(projection_json, results[0].2);
    Ok(())
}

#[tokio::test]
async fn persistent_output_overflow_is_typed_and_remains_durable_uncertainty() -> Result<()> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let limits = Limits {
        file_bytes: 64 * 1024,
        render_bytes: 64 * 1024,
        ..Limits::default()
    };
    let (provider, requests) = CapturingProvider::output_overflow();
    let model = model(json!({}))?;
    let operation = operation(0x4b);
    let session =
        PersistentLocalHarness::open(root.path(), model.clone(), provider.clone(), limits).await?;
    let error = session.run(operation, "bounded output").await;
    assert!(
        matches!(error, Err(Error::Invalid(message)) if message.contains("assistant output exceeds file limit"))
    );
    assert_eq!(captured(&requests).len(), 1);
    let records = session.storage().journal().replay(operation).await?;
    assert!(records
        .iter()
        .any(|record| matches!(record.event, ExecutionEvent::ModelStarted { step: 0, .. })));
    assert!(!records
        .iter()
        .any(|record| matches!(record.event, ExecutionEvent::Model { step: 0, .. })));
    drop(session);
    let reopened = PersistentLocalHarness::open(root.path(), model, provider, limits).await?;
    assert!(matches!(
        reopened.run(operation, "bounded output").await,
        Err(Error::Indeterminate(_))
    ));
    assert_eq!(
        captured(&requests).len(),
        1,
        "uncertain output must not be replayed automatically"
    );
    Ok(())
}

#[tokio::test]
async fn persistent_admission_rejects_oversized_context_and_undeclared_options_before_generate(
) -> Result<()> {
    let mut limits = Limits::default();
    limits.file_bytes = 1024;
    limits.render_bytes = 8;
    limits.attachments = 8;

    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (provider, requests) = CapturingProvider::complete();
    let session =
        PersistentLocalHarness::open(root.path(), model(json!({}))?, provider, limits).await?;
    let oversized = session
        .run(operation(0x43), "this input exceeds eight bytes")
        .await;
    assert!(
        oversized.is_err(),
        "oversized model input must return a typed failure"
    );
    assert!(
        captured(&requests).is_empty(),
        "oversized input must not reach generate"
    );

    let policy = ModelOptionPolicy::new(
        ComponentIdentity {
            name: "mock.options".into(),
            version: "1".into(),
            digest: [0x71; 32],
        },
        json!({
            "type": "object",
            "properties": {"mode": {"type": "string"}},
            "required": ["mode"],
            "additionalProperties": false
        }),
    )?;
    let options_root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (policy_provider, policy_requests) = CapturingProvider::with_policy(policy);
    let policy_session = PersistentLocalHarness::open(
        options_root.path(),
        model(json!({"mode": "safe", "api_key": "runtime-secret"}))?,
        policy_provider,
        Limits::default(),
    )
    .await?;
    let undeclared = policy_session
        .run(operation(0x44), "valid-sized input")
        .await;
    assert!(
        undeclared.is_err(),
        "undeclared model options must be rejected"
    );
    assert!(
        captured(&policy_requests).is_empty(),
        "option rejection must precede generate"
    );
    assert!(captured(&policy_requests).iter().all(|bytes| {
        !bytes
            .windows(b"runtime-secret".len())
            .any(|w| w == b"runtime-secret")
    }));
    Ok(())
}

#[tokio::test]
async fn persistent_generation_refs_remain_pinned_across_restart_replacement_and_corruption(
) -> Result<()> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (provider, requests) = CapturingProvider::complete();
    let model = model(json!({}))?;
    let session = PersistentLocalHarness::open(
        root.path(),
        model.clone(),
        provider.clone(),
        Limits::default(),
    )
    .await?;
    let original_bytes = b"pinned generation\r\n";
    let original = session
        .storage()
        .stage(
            operation(0x45),
            "src/pinned.txt",
            original_bytes,
            "text/plain",
            "pinned.txt",
        )
        .await?;
    session
        .storage()
        .content_verifier()
        .verify(&original)
        .await?;
    let first = session
        .storage()
        .run_conversation(
            session.bundle(),
            operation(0x45),
            original.clone(),
            vec![],
            1,
        )
        .await?;
    let first_bytes = captured(&requests);
    assert_eq!(first_bytes.len(), 1);
    let first_request = assert_request_is_exact(&first_bytes[0])?;
    assert_request_allowlist(&first_request)?;
    assert!(first_request.messages.iter().any(|message| {
        message
            .content
            .file_refs()
            .into_iter()
            .any(|reference| reference == &original)
    }));
    drop(session);

    let reopened =
        PersistentLocalHarness::open(root.path(), model, provider, Limits::default()).await?;
    assert_eq!(reopened.storage().read(&original).await?, original_bytes);
    let replacement = reopened
        .storage()
        .stage(
            operation(0x46),
            original.path(),
            b"replacement",
            "text/plain",
            "pinned.txt",
        )
        .await?;
    assert_ne!(original.version(), replacement.version());
    assert_eq!(reopened.storage().read(&original).await?, original_bytes);
    assert_eq!(reopened.storage().read(&replacement).await?, b"replacement");
    assert_eq!(
        reopened
            .storage()
            .run_conversation(
                reopened.bundle(),
                operation(0x45),
                original.clone(),
                vec![],
                1
            )
            .await?,
        first,
        "reopen must replay the original request after replacement"
    );
    assert_eq!(
        captured(&requests),
        first_bytes,
        "replay must not dispatch a replacement ref"
    );

    let missing = FileRef::new(
        original.volume().clone(),
        "src/missing.txt",
        original.version(),
        original.descriptor().clone(),
        "missing.txt",
    )?;
    let before_missing = captured(&requests).len();
    assert!(reopened
        .storage()
        .run_conversation(
            reopened.bundle(),
            operation(0x47),
            missing.clone(),
            vec![],
            1
        )
        .await
        .is_err());
    assert_eq!(captured(&requests).len(), before_missing);
    let corrupt = FileRef::new(
        original.volume().clone(),
        original.path(),
        original.version(),
        FileDescriptor::from_bytes(b"corrupt bytes", "text/plain")?,
        original.display_name(),
    )?;
    let before_corrupt = captured(&requests).len();
    assert!(reopened
        .storage()
        .run_conversation(
            reopened.bundle(),
            operation(0x48),
            corrupt.clone(),
            vec![],
            1
        )
        .await
        .is_err());
    assert_eq!(captured(&requests).len(), before_corrupt);
    Ok(())
}

// Keep this import in the fixture's public contract: the request's messages
// must remain role/content pairs with no UI or transport side channel.
#[allow(dead_code)]
fn _model_message_shape(message: &ModelMessage) -> (&str, &ModelContent) {
    (message.role.as_str(), &message.content)
}

//! Actual live adapter captures for the fixture codec, not a production model codec.
#![cfg(feature = "filesystem")]
#![allow(
    clippy::too_many_lines,
    reason = "ordered boundary scenarios retain their positive and negative controls together"
)]

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

#[path = "../examples/explicit-image-conversion.rs"]
mod explicit_conversion;
use acyclic_harness::runtime::TaskSpawner as _;

use acyclic_fs::Fs;
use acyclic_harness::runtime::TaskStateProvider;
use acyclic_harness::{
    Admission, AgentId, Capabilities, Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    bundle::HarnessBuilder,
    context::{ContextPipeline, ModelContextCapacity, ModelTokenCount},
    conversation::{
        ContentPublisher, ContentResidencyVerifier, FileRef, Limits, VolumeClass, VolumeOperation,
        VolumeOwner, VolumeRef,
    },
    core::{
        Action, AggregateKind, Authority, AuthorityIssuer, Command, ExtensionDependency,
        ExtensionForkPolicy, Reducer, SchemaRegistry, Scope,
    },
    distributed::{WorkPull, Worker},
    durable_tool::DurableToolRunner,
    executor::TurnInput,
    extension::{ExtensionIdentity, ExtensionRegistry, ExtensionRuntime, NativeExtension},
    filesystem::{
        FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
        FilesystemHost, FilesystemTaskRuntime,
    },
    model::{
        FileProjectionPolicy, ImageDetail, Model, ModelContent, ModelContentPart,
        ModelDataPart, ModelDispatch, ModelEvent, ModelMessage, ModelProvider, ModelRequest,
        ModelRole, NativeConfigurationBinding, NativeMediaIntent, NativeMediaPolicy,
        PreparedModelRequest, ToolResultContent,
    },
    resources::ProviderRef,
    runtime::{
        AgentHarness, ContentBindings, RuntimeScope, TaskDefinition, TaskRegistry,
        ToolContext,
    },
    scheduler::{LeaseFence, ResourceSnapshot, SessionLimits},
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
    workflow::{
        MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, ResumableMachine,
    },
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream, MemoryStream};
use futures::{stream, StreamExt as _};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct ReaderSpy {
    inner: Arc<dyn ContentResidencyVerifier>,
    reads: AtomicUsize,
    corrupt: AtomicBool,
    missing: AtomicBool,
}
impl ContentResidencyVerifier for ReaderSpy {
    fn read<'a>(&'a self, file: &'a FileRef, maximum_bytes: u64,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
            if self.missing.load(Ordering::SeqCst) {
                return Err(Error::NotFound(
                    "original native media is unavailable".into(),
                ));
            }
            let bytes = self.inner.read(file, maximum_bytes, io)?.await?;
            if self.corrupt.load(Ordering::SeqCst) {
                let mut corrupted = bytes.as_ref().to_vec();
                let first = corrupted
                    .first_mut()
                    .ok_or_else(|| Error::Invalid("fixture cannot corrupt empty body".into()))?;
                *first ^= 1;
                return Ok(corrupted.into());
            }
            Ok(bytes)
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Measurement {
    work: u64,
    duration_ms: u64,
    frames: u32,
    pages: u32,
}

fn number(bytes: &[u8], offset: usize) -> Result<u32> {
    let field = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| Error::Invalid("truncated fixture header".into()))?;
    let field: [u8; 4] = field
        .try_into()
        .map_err(|_| Error::Invalid("fixture integer".into()))?;
    Ok(u32::from_le_bytes(field))
}

fn measure(bytes: &[u8], intent: &NativeMediaIntent) -> Result<Measurement> {
    let mut result = Measurement {
        work: 0,
        duration_ms: 0,
        frames: 0,
        pages: 0,
    };
    let duration = |units: u32, rate: u32| -> Result<u64> {
        if rate == 0 {
            return Err(Error::Invalid("fixture rate is zero".into()));
        }
        Ok((u64::from(units) * 1_000).div_ceil(u64::from(rate)))
    };
    match (bytes.get(..4), intent) {
        (Some(b"IMG1"), NativeMediaIntent::Image { .. }) => {
            let pixels = number(bytes, 4)?;
            if pixels == 0 || bytes.len() as u64 != 8 + u64::from(pixels) {
                return Err(Error::Invalid("fixture pixel payload differs".into()));
            }
            result.work = u64::from(pixels);
        }
        (Some(b"AUD1"), NativeMediaIntent::Audio { .. }) => {
            let samples = number(bytes, 4)?;
            result.duration_ms = duration(samples, number(bytes, 8)?)?;
            if samples == 0 || bytes.len() as u64 != 12 + u64::from(samples) * 2 {
                return Err(Error::Invalid("fixture sample payload differs".into()));
            }
            result.work = u64::from(samples);
        }
        (Some(b"VID1"), NativeMediaIntent::Video { .. }) => {
            result.frames = number(bytes, 4)?;
            result.duration_ms = duration(result.frames, number(bytes, 8)?)?;
            let pixels = number(bytes, 12)?;
            result.work = u64::from(result.frames) * u64::from(pixels);
            if result.frames == 0 || pixels == 0 || bytes.len() as u64 != 16 + result.work {
                return Err(Error::Invalid("fixture frame payload differs".into()));
            }
        }
        (Some(b"DOC1"), NativeMediaIntent::Document { .. }) => {
            result.pages = number(bytes, 4)?;
            if result.pages == 0 {
                return Err(Error::Invalid("fixture document is empty".into()));
            }
            let mut cursor = 8usize;
            for _ in 0..result.pages {
                let length = usize::try_from(number(bytes, cursor)?)
                    .map_err(|_| Error::Invalid("fixture page length".into()))?;
                cursor = cursor
                    .checked_add(4)
                    .ok_or_else(|| Error::Invalid("page offset overflow".into()))?;
                let end = cursor
                    .checked_add(length)
                    .ok_or_else(|| Error::Invalid("page length overflow".into()))?;
                let page = bytes
                    .get(cursor..end)
                    .ok_or_else(|| Error::Invalid("truncated fixture page".into()))?;
                std::str::from_utf8(page)
                    .map_err(|_| Error::Invalid("fixture page is not UTF-8".into()))?;
                result.work += page.len() as u64;
                cursor = end;
            }
            if cursor != bytes.len() {
                return Err(Error::Invalid("extra fixture document bytes".into()));
            }
        }
        _ => {
            return Err(Error::Unsupported(
                "fixture codec does not support this modality/body".into(),
            ));
        }
    }
    Ok(result)
}

#[derive(Clone, Debug, PartialEq)]
enum CapturedPart {
    Text(String),
    Call(String, String),
    Result(String, String),
    Json(Value),
    Media(Box<MediaCapture>),
}
#[derive(Clone, Debug, PartialEq)]
struct MediaCapture {
    file: FileRef,
    policy: NativeMediaPolicy,
    bytes: acyclic_stream::OwnedBytes,
    options_bytes: Option<acyclic_stream::OwnedBytes>,
    options: Option<Value>,
    measurement: Measurement,
}
#[derive(Clone, Debug, PartialEq)]
struct Capture {
    adapter_revision: String,
    request_digest: [u8; 32],
    request_bytes: Vec<u8>,
    parts: Vec<CapturedPart>,
}

type NativeReceipts = BTreeMap<([u8; 16], u32), (ModelDispatch, Vec<ModelEvent>)>;

struct MockAdapter {
    reader: Arc<ReaderSpy>,
    counts: AtomicUsize,
    generates: AtomicUsize,
    wrong_count_digest: AtomicBool,
    captures: Mutex<Vec<Capture>>,
    receipts: Mutex<NativeReceipts>,
    receipts_enabled: bool,
    interrupt_after_capture: AtomicBool,
    reconciles: AtomicUsize,
}
impl MockAdapter {
    fn new(reader: Arc<ReaderSpy>) -> Self {
        Self {
            reader,
            counts: AtomicUsize::new(0),
            generates: AtomicUsize::new(0),
            wrong_count_digest: AtomicBool::new(false),
            captures: Mutex::new(Vec::new()),
            receipts: Mutex::new(BTreeMap::new()),
            receipts_enabled: false,
            interrupt_after_capture: AtomicBool::new(false),
            reconciles: AtomicUsize::new(0),
        }
    }
    fn selection(model: &Model) -> Result<()> {
        if model.provider != "fixture-native"
            || model.name != "all"
            || model.revision != "mock-native-1"
        {
            return Err(Error::Unsupported(
                "fixture adapter revision is unavailable".into(),
            ));
        }
        Ok(())
    }
    async fn file(&self, file: &FileRef, policy: &FileProjectionPolicy) -> Result<CapturedPart> {
        if *policy == FileProjectionPolicy::BoundedFull && file.descriptor().media_type() == "text/plain" {
            let bytes = self.reader.read(file, file.descriptor().byte_length(), None)?.await?;
            file.descriptor().verify(&bytes, &mut None)?;
            return Ok(CapturedPart::Text(std::str::from_utf8(&bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?.into()));
        }
        let FileProjectionPolicy::Native(policy) = policy else {
            return Err(Error::Unsupported(
                "fixture accepts explicit native input only".into(),
            ));
        };
        policy.validate(file, Limits::default(), None)?;
        let bytes = self.reader.read(file, file.descriptor().byte_length(), None)?.await?;
        file.descriptor().verify(&bytes, &mut None)?;
        let measured = measure(&bytes, &policy.intent)?;
        let exceeds = measured.work > policy.maximum_work
            || match &policy.intent {
                NativeMediaIntent::Image { .. } => false,
                NativeMediaIntent::Audio {
                    maximum_duration_ms,
                } => measured.duration_ms > *maximum_duration_ms,
                NativeMediaIntent::Video {
                    maximum_duration_ms,
                    maximum_frames,
                } => {
                    measured.duration_ms > *maximum_duration_ms || measured.frames > *maximum_frames
                }
                NativeMediaIntent::Document { maximum_pages } => measured.pages > *maximum_pages,
            };
        if exceeds {
            return Err(Error::Invalid(
                "actual fixture measurements exceed native ceiling".into(),
            ));
        }
        let (options_bytes, options) = if let Some(binding) = &policy.configuration {
            let bytes = self.reader.read(&binding.configuration.content, binding.configuration.content.descriptor().byte_length(), None)?.await?;
            binding.configuration.content.descriptor().verify(&bytes, &mut None)?;
            let options: Value = serde_json::from_slice(&bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if options != json!({"mode":"strict","scale":2}) {
                return Err(Error::Unsupported(
                    "fixture adapter does not support these registered options".into(),
                ));
            }
            (Some(bytes), Some(options))
        } else {
            (None, None)
        };
        Ok(CapturedPart::Media(Box::new(MediaCapture {
            file: file.clone(),
            policy: policy.as_ref().clone(),
            bytes,
            options_bytes,
            options,
            measurement: measured,
        })))
    }
}
impl ModelProvider for MockAdapter {
    fn context_capacity(&self, model: &Model) -> Result<ModelContextCapacity> {
        Self::selection(model)?;
        Ok(ModelContextCapacity {
            context_tokens: model.options["context_tokens"].as_u64().and_then(|tokens| u32::try_from(tokens).ok()).unwrap_or(131_072),
            output_tokens: 4_096,
        })
    }
    fn count_tokens(&self, request: &PreparedModelRequest) -> Result<ModelTokenCount> {
        Self::selection(&request.request().model)?;
        self.counts.fetch_add(1, Ordering::SeqCst);
        let message_tokens = request
            .request()
            .messages
            .iter()
            .map(|message| {
                let mut units = serde_json::to_vec(message)
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .len() as u64
                    + 1;
                for file in message.content.file_refs() {
                    units += file.descriptor().byte_length();
                }
                u32::try_from(units).map_err(|_| Error::Invalid("fixture count overflow".into()))
            })
            .collect::<Result<Vec<_>>>()?;
        let fixed = serde_json::to_vec(&(
            &request.request().model,
            &request.request().tools,
            request.request().max_output_tokens,
        ))
        .map_err(|error| Error::Invalid(error.to_string()))?
        .len()
            + 64;
        Ok(ModelTokenCount {
            request_digest: if self.wrong_count_digest.load(Ordering::SeqCst) {
                [99; 32]
            } else {
                request.request_digest()
            },
            fixed_tokens: u32::try_from(fixed)
                .map_err(|_| Error::Invalid("fixture framing count overflow".into()))?,
            message_tokens,
        })
    }
    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxProviderStream<'a, Result<ModelEvent>> {
        self.generates.fetch_add(1, Ordering::SeqCst);
        Box::pin(stream::once(async move {
            Self::selection(&request.request().model)?;
            if dispatch.request_digest != request.request_digest() {
                return Err(Error::Conflict("fixture dispatch digest changed".into()));
            }
            let mut parts = Vec::new();
            for message in &request.request().messages {
                if let ModelContent::Text(text) = &message.content {
                    parts.push(CapturedPart::Text(text.clone()));
                }
                for part in message.content.parts() {
                    match part {
                        ModelContentPart::Text { text } => {
                            parts.push(CapturedPart::Text(text.clone()));
                        }
                        ModelContentPart::File { file, policy } => {
                            parts.push(self.file(file, policy).await?);
                        }
                        ModelContentPart::ToolCall { call_id, name, .. } => {
                            parts.push(CapturedPart::Call(call_id.clone(), name.clone()));
                        }
                        ModelContentPart::ToolResult {
                            call_id,
                            name,
                            content,
                        } => {
                            parts.push(CapturedPart::Result(call_id.clone(), name.clone()));
                            match content {
                                ToolResultContent::Json { value } => {
                                    parts.push(CapturedPart::Json(value.clone()));
                                }
                                ToolResultContent::Parts { parts: data } => {
                                    for part in data {
                                        match part {
                                            ModelDataPart::Text { text } => {
                                                parts.push(CapturedPart::Text(text.clone()));
                                            }
                                            ModelDataPart::File { file, policy } => {
                                                parts.push(self.file(file, policy).await?);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            self.captures
                .lock()
                .map_err(|_| Error::Storage("fixture capture lock".into()))?
                .push(Capture {
                    adapter_revision: request.request().model.revision.clone(),
                    request_digest: dispatch.request_digest,
                    request_bytes: request.bytes().to_vec(),
                    parts,
                });
            let mut events = Vec::new();
            if request.request().messages.iter().any(|message| matches!(
                &message.content, ModelContent::Text(text) if text.starts_with("Summarize the preceding context")
            )) {
                events.push(ModelEvent::Content { delta: "retained native summary".into() });
            }
            events.push(ModelEvent::Completed { metadata: Value::Null });
            if self.receipts_enabled {
                let key = (dispatch.operation_id.into_bytes(), dispatch.step);
                let previous = self
                    .receipts
                    .lock()
                    .map_err(|_| Error::Storage("fixture receipt lock".into()))?
                    .insert(key, (dispatch, events.clone()));
                if previous.is_some() {
                    return Err(Error::Conflict(
                        "fixture attempted duplicate generation".into(),
                    ));
                }
            }
            if self.interrupt_after_capture.swap(false, Ordering::SeqCst) {
                return Err(Error::Storage(
                    "fixture response lost after native capture".into(),
                ));
            }
            Ok(events)
        }).flat_map(|events| stream::iter(match events {
            Ok(events) => events.into_iter().map(Ok).collect(),
            Err(error) => vec![Err(error)],
        })))
    }
    fn reconcile<'a>(&'a self, dispatch: acyclic_harness::model::ModelDispatch, observed: &'a [acyclic_harness::model::ModelEvent]) -> BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        self.reconciles.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let receipts = self
                .receipts
                .lock()
                .map_err(|_| Error::Storage("fixture receipt lock".into()))?;
            let Some((recorded, events)) =
                receipts.get(&(dispatch.operation_id.into_bytes(), dispatch.step))
            else {
                return Ok(None);
            };
            if recorded.request_digest != dispatch.request_digest
                || !events.starts_with(observed)
            {
                return Err(Error::Conflict("fixture original attempt differs".into()));
            }
            let remaining = events
                .get(observed.len()..)
                .ok_or_else(|| Error::Conflict("fixture observed prefix exceeds receipt".into()))?;
            Ok(Some(remaining.to_vec()))
        })
    }
}

struct FixtureExtension;
impl NativeExtension for FixtureExtension {
    fn identity(&self) -> ExtensionIdentity {
        ExtensionIdentity {
            name: "fixture.media".into(),
            version: 1,
            digest: [31; 32],
        }
    }
}
struct NativeProjector(Vec<(FileRef, NativeMediaPolicy)>);
impl ToolExecutor for NativeProjector {
    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "fixture projection does not execute a tool".into(),
            ))
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "fixture projection does not reconcile a tool".into(),
            ))
        })
    }
}
impl ToolProjection for NativeProjector {
    fn project(&self, _: &ToolInvocation, result: &ToolResult,
        work: acyclic_harness::NativeWork<'_>) -> Result<(Value, acyclic_harness::NativeCustody)> {
        self.supports_native_projection(work)?;
        let expected: Vec<FileRef> = self.0.iter().map(|(file, _)| file.clone()).collect();
        if result.value != json!({"files":expected}) {
            return Err(Error::Conflict(
                "fixture canonical selection changed".into(),
            ));
        }
        let parts = std::iter::once(ModelDataPart::Text {
            text: "tool-native".into(),
        })
        .chain(self.0.iter().map(|(file, policy)| ModelDataPart::File {
            file: file.clone(),
            policy: FileProjectionPolicy::Native(Box::new(policy.clone())),
        }))
        .collect();
        serde_json::to_value(ToolResultContent::Parts { parts })
            .map(|value| (value, None))
            .map_err(|error| Error::Invalid(error.to_string()))
    }
}

fn policy(intent: NativeMediaIntent, binding: &NativeConfigurationBinding) -> NativeMediaPolicy {
    NativeMediaPolicy {
        intent,
        maximum_bytes: 4_096,
        maximum_work: 64,
        configuration: Some(binding.clone()),
    }
}
fn only_file(file: &FileRef, policy: &NativeMediaPolicy) -> Vec<ModelMessage> {
    vec![ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Part(ModelContentPart::File {
            file: file.clone(),
            policy: FileProjectionPolicy::Native(Box::new(policy.clone())),
        }),
    }]
}

async fn exercise_live_native_boundary() -> Result<()> {
    let provider = ProviderRef::new("native-boundary".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let agent_id = AgentId::new();
    let volume = VolumeRef::new(
        provider,
        "native-boundary".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent_id),
     &mut None)?;
    host.create_volume(&volume).await?;
    let authority = Authority {
        kind: AggregateKind::Agent,
        id: "native-boundary".into(),
    };
    let issuer = AuthorityIssuer::new("native-boundary", [21; 32], authority.clone());
    let read = volume.capability(VolumeOperation::Read, &mut None)?;
    let write = volume.capability(VolumeOperation::Write, &mut None)?;
    let owner = issuer.issue("owner", &Capabilities::new([
            read.clone(),
            write,
            "extension:configure".into(),
            "extension:activate".into(),
            "operation:declare".into(),
            "operation:observe".into(),
            "operation:cancel".into(),
            "model:generate".into(),
            "task:spawn:fixture.native_replay@1".into(),
            "tool:call:example.convert_image".into(),
        ]), Some(agent_id), None, &mut None).expect("scope proof");
    let writer = FilesystemContentPublisher::new(host.clone(), &volume, &issuer.verifier(&mut None).expect("verifier copy"), &owner, 65_536, None)?;
    let options_bytes = br#"{"mode":"strict","scale":2}"#;
    let options_file = writer
        .stage(
            OperationId::new(),
            "options.json",
            options_bytes,
            "application/json",
            "options.json", None,
        )?.await?.0;
    let extension = ExtensionDependency {
        name: "fixture.media".into(),
        version: 1,
    };
    let mut schemas = SchemaRegistry::new();
    schemas.register(extension.name.clone(),1,json!({"type":"object"}),[31;32],ExtensionForkPolicy::Inherit,[],Some(json!({"type":"object","required":["mode","scale"],"properties":{"mode":{"const":"strict"},"scale":{"type":"integer","minimum":1,"maximum":4}},"additionalProperties":false})))?;
    let mut agent = Reducer::new(authority, issuer.verifier(&mut None).expect("verifier copy"), schemas.clone());
    agent.schemas().validate_configuration_bytes(&extension, &options_file, options_bytes, None)?;
    for (revision, action) in [
        (
            0,
            Action::ConfigureExtension {
                extension: extension.clone(),
                content: options_file,
            },
        ),
        (
            1,
            Action::SelectExtensions {
                roots: vec![extension.clone()],
            },
        ),
    ] {
        agent.apply(Command {
            operation_id: OperationId::new(),
            idempotency_key: IdempotencyKey::new(format!("native:{revision}"))?,
            expected_revision: revision,
            scope: owner.clone(),
            causal_parent: None,
            action,
        })?;
    }
    let admission = agent
        .extension_admission()?
        .ok_or_else(|| Error::NotFound("native selection".into()))?;
    let binding = NativeConfigurationBinding {
        source: admission.source().clone(),
        configuration: admission
            .configurations()
            .first()
            .ok_or_else(|| Error::NotFound("native configuration".into()))?
            .clone(),
        implementation_digest: [31; 32],
    };
    let extensions = ExtensionRegistry::default();
    extensions.install(Arc::new(FixtureExtension))?;
    let linked = ExtensionRuntime::from_reducer(extensions, &agent)?;
    let bodies = [
        [b"IMG1".as_slice(), &4u32.to_le_bytes(), &[0, 127, 255, 1]].concat(),
        [
            b"AUD1".as_slice(),
            &8u32.to_le_bytes(),
            &80u32.to_le_bytes(),
            &[0; 16],
        ]
        .concat(),
        [
            b"VID1".as_slice(),
            &2u32.to_le_bytes(),
            &10u32.to_le_bytes(),
            &2u32.to_le_bytes(),
            &[1, 2, 3, 4],
        ]
        .concat(),
        [
            b"DOC1".as_slice(),
            &2u32.to_le_bytes(),
            &3u32.to_le_bytes(),
            b"one",
            &3u32.to_le_bytes(),
            b"two",
        ]
        .concat(),
    ];
    let intents = [
        NativeMediaIntent::Image {
            detail: ImageDetail::High,
        },
        NativeMediaIntent::Audio {
            maximum_duration_ms: 1_000,
        },
        NativeMediaIntent::Video {
            maximum_duration_ms: 1_000,
            maximum_frames: 16,
        },
        NativeMediaIntent::Document { maximum_pages: 16 },
    ];
    let mut selected = Vec::new();
    for (index, body) in bodies.iter().enumerate() {
        selected.push((
            writer
                .stage(
                    OperationId::new(),
                    &format!("media/{index}.bin"),
                    body,
                    "application/x-harness-native-fixture",
                    "native.bin", None,
                )?.await?.0,
            policy(
                intents
                    .get(index)
                    .ok_or_else(|| Error::Invalid("fixture intent".into()))?
                    .clone(),
                &binding,
            ),
        ));
    }
    let reader = Arc::new(ReaderSpy {
        inner: FilesystemContentVerifier::new(host.clone(), &issuer.verifier(&mut None).expect("verifier copy"), &owner, 65_536, None)?,
        reads: AtomicUsize::new(0),
        corrupt: AtomicBool::new(false),
        missing: AtomicBool::new(false),
    });
    let adapter = Arc::new(MockAdapter::new(reader.clone()));
    let projector = Arc::new(NativeProjector(selected.iter().skip(2).cloned().collect()));
    let definition = ToolDefinition {
        name: "fixture.media_tool".into(),
        revision: "native-1".into(),
        description: "Explicit native fixture projection".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({"type":"object","required":["files"]}),
        projection_schema: json!({"type":"object","required":["kind","parts"],"properties":{"kind":{"const":"parts"},"parts":{"type":"array"}},"additionalProperties":false}),
    };
    let mut tools = ToolRegistry::default();
    tools.register(Tool {
        definition: definition.clone(),
        executor: projector.clone(),
        projection: projector.clone(),
    })?;
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "native-tool".into(),
        name: definition.name.clone(),
        arguments: json!({}),
    };
    let canonical = ToolResult {
        value: json!({"files":selected.iter().skip(2).map(|(file,_)|file.clone()).collect::<Vec<_>>()}),
    };
    let tool_content: ToolResultContent =
        serde_json::from_value(projector.project(&invocation, &canonical, None)?.0)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let mut messages = vec![ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Parts(vec![
            ModelContentPart::Text {
                text: "before".into(),
            },
            ModelContentPart::File {
                file: selected
                    .first()
                    .ok_or_else(|| Error::NotFound("image".into()))?
                    .0
                    .clone(),
                policy: FileProjectionPolicy::Native(Box::new(
                    selected
                        .first()
                        .ok_or_else(|| Error::NotFound("image policy".into()))?
                        .1
                        .clone(),
                )),
            },
            ModelContentPart::Text {
                text: "between".into(),
            },
            ModelContentPart::File {
                file: selected
                    .get(1)
                    .ok_or_else(|| Error::NotFound("audio".into()))?
                    .0
                    .clone(),
                policy: FileProjectionPolicy::Native(Box::new(
                    selected
                        .get(1)
                        .ok_or_else(|| Error::NotFound("audio policy".into()))?
                        .1
                        .clone(),
                )),
            },
        ]),
    }];
    messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::ToolCall {
            call_id: invocation.call_id.clone(),
            name: invocation.name.clone(),
            arguments: invocation.arguments.clone(),
        }),
    });
    messages.push(ModelMessage {
        role: ModelRole::Tool,
        content: ModelContent::Part(ModelContentPart::ToolResult {
            call_id: invocation.call_id,
            name: invocation.name,
            content: tool_content,
        }),
    });
    let mut tasks = TaskRegistry::default();
    let unsupported_file = selected
        .first()
        .ok_or_else(|| Error::NotFound("image for option capability".into()))?
        .0
        .clone();
    let durable_selected = selected.clone();
    let durable_scope = RuntimeScope::new(owner.capabilities().clone(), Limits::default())?
        .with_extensions_from(&agent)?
        .with_extension_runtime(linked.clone())?;
    let test_adapter = adapter.clone();
    let test_reader = reader.clone();
    tasks.register(TaskDefinition::live(
        "fixture.native_capture",
        "1",
        move |context, (): ()| {
            let adapter = test_adapter.clone();
            let reader = test_reader.clone();
            let selected = selected.clone();
            let messages = messages.clone();
            let bodies = bodies.clone();
            async move {
                context.model_events(messages.clone(), Some(16)).await?;
                assert_eq!(adapter.counts.load(Ordering::SeqCst), 1);
                assert_eq!(adapter.generates.load(Ordering::SeqCst), 1);
                let capture = adapter
                    .captures
                    .lock()
                    .map_err(|_| Error::Storage("capture lock".into()))?
                    .first()
                    .ok_or_else(|| Error::NotFound("adapter capture".into()))?
                    .clone();
                assert_eq!(capture.adapter_revision, "mock-native-1");
                assert_eq!(
                    capture.request_digest,
                    *blake3::hash(&capture.request_bytes).as_bytes()
                );
                let decoded: ModelRequest = serde_json::from_slice(&capture.request_bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                assert_eq!(decoded.messages, messages);
                assert_eq!(decoded.max_output_tokens, Some(16));
                assert_eq!(capture.parts.iter().map(|part| match part {
                    CapturedPart::Text(_) => "text", CapturedPart::Call(..) => "call",
                    CapturedPart::Result(..) => "result", CapturedPart::Json(_) => "json",
                    CapturedPart::Media(..) => "media",
                }).collect::<Vec<_>>(), vec!["text", "media", "text", "media", "call", "result", "text", "media", "media"]);
                assert_eq!(
                    capture
                        .parts
                        .iter()
                        .filter_map(|part| if let CapturedPart::Text(text) = part {
                            Some(text.as_str())
                        } else {
                            None
                        })
                        .collect::<Vec<_>>(),
                    vec!["before", "between", "tool-native"]
                );
                let media = capture
                    .parts
                    .iter()
                    .filter_map(|part| {
                        if let CapturedPart::Media(media) = part {
                            Some((&media.file, &media.policy, &media.bytes, &media.options_bytes, &media.options, &media.measurement))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(media.len(), 4);
                for (index, (file, policy, bytes, option_bytes, options, measured)) in
                    media.iter().enumerate()
                {
                    let expected = selected
                        .get(index)
                        .ok_or_else(|| Error::NotFound("selected media".into()))?;
                    assert_eq!(*file, &expected.0);
                    assert_eq!(*policy, &expected.1);
                    assert_eq!(
                        bytes.as_ref(),
                        bodies
                            .get(index)
                            .ok_or_else(|| Error::NotFound("media body".into()))?
                            .as_slice()
                    );
                    assert_eq!(option_bytes.as_deref(), Some(options_bytes.as_slice()));
                    assert_eq!(*options, &Some(json!({"mode":"strict","scale":2})));
                    assert_eq!(*measured, &measure(bytes, &policy.intent)?);
                }
                assert!(capture.parts.contains(&CapturedPart::Call(
                    "native-tool".into(),
                    "fixture.media_tool".into()
                )));
                assert!(capture.parts.contains(&CapturedPart::Result(
                    "native-tool".into(),
                    "fixture.media_tool".into()
                )));
                for (file, native) in &selected {
                    context
                        .model_events(only_file(file, native), Some(16))
                        .await?;
                    let captures = adapter.captures.lock().map_err(|_| Error::Storage("capture lock".into()))?;
                    let last = captures.last().ok_or_else(|| Error::NotFound("media-only capture".into()))?;
                    assert_eq!(last.parts.len(), 1);
                    assert!(matches!(last.parts.first(), Some(CapturedPart::Media(captured)) if &captured.file == file));
                }
                for (file, native) in &selected {
                    let mut bounded = native.clone();
                    bounded.maximum_work = 1;
                    assert!(matches!(
                        context
                            .model_events(only_file(file, &bounded), Some(16))
                            .await,
                        Err(Error::Invalid(_))
                    ));
                }
                for (index, intent) in [
                    (
                        1,
                        NativeMediaIntent::Audio {
                            maximum_duration_ms: 50,
                        },
                    ),
                    (
                        2,
                        NativeMediaIntent::Video {
                            maximum_duration_ms: 100,
                            maximum_frames: 16,
                        },
                    ),
                    (
                        2,
                        NativeMediaIntent::Video {
                            maximum_duration_ms: 1_000,
                            maximum_frames: 1,
                        },
                    ),
                    (3, NativeMediaIntent::Document { maximum_pages: 1 }),
                ] {
                    let (file, original) = selected
                        .get(index)
                        .ok_or_else(|| Error::NotFound("measured native input".into()))?;
                    let mut native = original.clone();
                    native.intent = intent;
                    let generated = adapter.generates.load(Ordering::SeqCst);
                    let captured = adapter
                        .captures
                        .lock()
                        .map_err(|_| Error::Storage("capture lock".into()))?
                        .len();
                    assert!(matches!(
                        context
                            .model_events(only_file(file, &native), Some(16))
                            .await,
                        Err(Error::Invalid(_))
                    ));
                    assert_eq!(adapter.generates.load(Ordering::SeqCst), generated + 1);
                    assert_eq!(
                        adapter
                            .captures
                            .lock()
                            .map_err(|_| Error::Storage("capture lock".into()))?
                            .len(),
                        captured
                    );
                }
                let original = selected
                    .first()
                    .ok_or_else(|| Error::NotFound("image".into()))?;
                let mut unsupported = original.1.clone();
                unsupported.intent = NativeMediaIntent::Audio {
                    maximum_duration_ms: 1_000,
                };
                let generated = adapter.generates.load(Ordering::SeqCst);
                let captures = adapter
                    .captures
                    .lock()
                    .map_err(|_| Error::Storage("capture lock".into()))?
                    .len();
                assert!(matches!(
                    context
                        .model_events(only_file(&original.0, &unsupported), Some(16))
                        .await,
                    Err(Error::Unsupported(_))
                ));
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generated + 1);
                assert_eq!(
                    adapter
                        .captures
                        .lock()
                        .map_err(|_| Error::Storage("capture lock".into()))?
                        .len(),
                    captures
                );
                for change in [0, 1, 2, 3] {
                    let mut native = original.1.clone();
                    if change == 0 {
                        native.maximum_bytes = original.0.descriptor().byte_length() - 1;
                    }
                    if let Some(binding) = native.configuration.as_mut() {
                        match change {
                            1 => binding.source.revision += 1,
                            2 => binding.configuration.schema_digest = [77; 32],
                            3 => binding.implementation_digest = [78; 32],
                            _ => {}
                        }
                    }
                    let reads = reader.reads.load(Ordering::SeqCst);
                    let counts = adapter.counts.load(Ordering::SeqCst);
                    let generates = adapter.generates.load(Ordering::SeqCst);
                    assert!(
                        context
                            .model_events(only_file(&original.0, &native), Some(16))
                            .await
                            .is_err()
                    );
                    assert_eq!(reader.reads.load(Ordering::SeqCst), reads);
                    assert_eq!(adapter.counts.load(Ordering::SeqCst), counts);
                    assert_eq!(adapter.generates.load(Ordering::SeqCst), generates);
                }
                let narrowed = context.scoped(
                    Capabilities::new(["model:generate", "tool:call:fixture.media_tool"]),
                    context.scope().limits(),
                )?;
                let reads = reader.reads.load(Ordering::SeqCst);
                let counts = adapter.counts.load(Ordering::SeqCst);
                let generates = adapter.generates.load(Ordering::SeqCst);
                assert!(matches!(
                    narrowed.model_events(messages, Some(16)).await,
                    Err(Error::Unauthorized(_))
                ));
                assert_eq!(reader.reads.load(Ordering::SeqCst), reads);
                assert_eq!(adapter.counts.load(Ordering::SeqCst), counts);
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generates);
                for limits in [
                    Limits {
                        file_bytes: 8,
                        render_bytes: 4,
                        ..context.scope().limits()
                    },
                    Limits {
                        render_bytes: 4,
                        ..context.scope().limits()
                    },
                ] {
                    let bounded = context.scoped(context.scope().grants().clone(), limits)?;
                    let reads = reader.reads.load(Ordering::SeqCst);
                    let counts = adapter.counts.load(Ordering::SeqCst);
                    let generates = adapter.generates.load(Ordering::SeqCst);
                    assert!(matches!(
                        bounded
                            .model_events(only_file(&original.0, &original.1), Some(16))
                            .await,
                        Err(Error::Invalid(_))
                    ));
                    assert_eq!(reader.reads.load(Ordering::SeqCst), reads);
                    assert_eq!(adapter.counts.load(Ordering::SeqCst), counts);
                    assert_eq!(adapter.generates.load(Ordering::SeqCst), generates);
                }
                let changed_revision = context.scoped_model(
                    context.scope().grants().clone(),
                    context.scope().limits(),
                    Model::new("fixture-native".into(), "all".into(), "mock-native-2".into(), Value::Null)?,
                    adapter.clone(),
                )?;
                let counts = adapter.counts.load(Ordering::SeqCst);
                let generates = adapter.generates.load(Ordering::SeqCst);
                assert!(matches!(
                    changed_revision
                        .model_events(only_file(&original.0, &original.1), Some(16))
                        .await,
                    Err(Error::Unsupported(_))
                ));
                assert_eq!(adapter.counts.load(Ordering::SeqCst), counts);
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generates);
                adapter.wrong_count_digest.store(true, Ordering::SeqCst);
                assert!(
                    context
                        .model_events(only_file(&original.0, &original.1), Some(16))
                        .await
                        .is_err()
                );
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generates);
                adapter.wrong_count_digest.store(false, Ordering::SeqCst);
                let counts = adapter.counts.load(Ordering::SeqCst);
                let generates = adapter.generates.load(Ordering::SeqCst);
                let reads = reader.reads.load(Ordering::SeqCst);
                let captured = adapter.captures.lock().map_err(|_| Error::Storage("capture lock".into()))?.len();
                reader.missing.store(true, Ordering::SeqCst);
                assert!(matches!(context.model_events(only_file(&original.0, &original.1), Some(16)).await, Err(Error::NotFound(_))));
                reader.missing.store(false, Ordering::SeqCst);
                assert!(reader.reads.load(Ordering::SeqCst) > reads);
                assert_eq!(adapter.counts.load(Ordering::SeqCst), counts);
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generates);
                assert_eq!(adapter.captures.lock().map_err(|_| Error::Storage("capture lock".into()))?.len(), captured);
                let reads = reader.reads.load(Ordering::SeqCst);
                reader.corrupt.store(true, Ordering::SeqCst);
                assert!(context.model_events(only_file(&original.0, &original.1), Some(16)).await.is_err());
                assert!(reader.reads.load(Ordering::SeqCst) > reads);
                assert_eq!(adapter.counts.load(Ordering::SeqCst), counts);
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generates);
                reader.corrupt.store(false, Ordering::SeqCst);
                context.model_events(only_file(&original.0, &original.1), Some(16)).await?;
                assert_eq!(adapter.counts.load(Ordering::SeqCst), counts + 1);
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generates + 1);
                Ok(())
            }
        },
    )?)?;
    let journal_authority = Authority {
        kind: AggregateKind::Conversation,
        id: acyclic_harness::ConversationId::new().to_string(),
    };
    let journal_issuer = AuthorityIssuer::new("native-conversation", [22; 32], journal_authority);
    let journal_scope = journal_issuer.issue("conversation-owner", &Capabilities::new([read.clone(), volume.capability(VolumeOperation::Write, &mut None)?]), Some(agent_id), None, &mut None).expect("scope proof");
    let journal = Arc::new(FilesystemExecutionJournal::new(
        Arc::new(MemoryStream::default()),
        host.clone(),
        &volume,
        &journal_issuer.verifier(&mut None).expect("verifier copy"), &acyclic_harness::core::SchemaRegistry::new(),
        &journal_scope,
        65_536, None,
    )?);
    let bundle = HarnessBuilder::new()
        .journal(journal.clone())
        .model(
            Model::new("fixture-native".into(), "all".into(), "mock-native-1".into(), Value::Null)?,
            adapter.clone(),
        )
        .content(ContentBindings {
            reader: reader.clone(),
            writer: None,
        })
        .tasks(tasks)
        .tools(tools)
        .extensions_from(&agent)?
        .extensions(linked.clone())
        .grant("model:generate")
        .grant("task:spawn:fixture.native_capture@1")
        .grant("tool:call:fixture.media_tool")
        .grant(read.clone())
        .build()?;
    let next_bytes = br#"{"mode":"strict","scale":3}"#;
    let next = writer
        .stage(
            OperationId::new(),
            "options.json",
            next_bytes,
            "application/json",
            "options.json", None,
        )?.await?.0;
    agent.schemas().validate_configuration_bytes(&extension, &next, next_bytes, None)?;
    for (revision, action) in [
        (
            2,
            Action::ConfigureExtension {
                extension: extension.clone(),
                content: next,
            },
        ),
        (
            3,
            Action::SelectExtensions {
                roots: vec![extension.clone()],
            },
        ),
    ] {
        agent.apply(Command {
            operation_id: OperationId::new(),
            idempotency_key: IdempotencyKey::new(format!("native:{revision}"))?,
            expected_revision: revision,
            scope: owner.clone(),
            causal_parent: None,
            action,
        })?;
    }
    let runtime = bundle.runtime();
    let task = runtime.task::<(), ()>("fixture.native_capture")?;
    assert_eq!(
        runtime.spawn(&task, ()).await?.result().await?,
        Outcome::Succeeded(())
    );
    exercise_retained_native_boundary(
        host.clone(),
        volume.clone(),
        owner.clone(),
        durable_scope,
        reader.clone(),
        durable_selected,
        &writer,
    )
    .await?;
    let current = agent
        .extension_admission()?
        .ok_or_else(|| Error::NotFound("new native selection".into()))?;
    let current_binding = NativeConfigurationBinding {
        source: current.source().clone(),
        configuration: current
            .configurations()
            .first()
            .ok_or_else(|| Error::NotFound("new native options".into()))?
            .clone(),
        implementation_digest: [31; 32],
    };
    let unsupported_native = policy(
        NativeMediaIntent::Image {
            detail: ImageDetail::High,
        },
        &current_binding,
    );
    let mut unsupported_tasks = TaskRegistry::default();
    let option_adapter = adapter.clone();
    unsupported_tasks.register(TaskDefinition::live(
        "fixture.unsupported_options",
        "1",
        move |context, (): ()| {
            let adapter = option_adapter.clone();
            let file = unsupported_file.clone();
            let native = unsupported_native.clone();
            async move {
                let generated = adapter.generates.load(Ordering::SeqCst);
                let counts = adapter.counts.load(Ordering::SeqCst);
                let captures = adapter
                    .captures
                    .lock()
                    .map_err(|_| Error::Storage("capture lock".into()))?
                    .len();
                assert!(matches!(
                    context
                        .model_events(only_file(&file, &native), Some(16))
                        .await,
                    Err(Error::Unsupported(_))
                ));
                assert_eq!(adapter.counts.load(Ordering::SeqCst), counts + 1);
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generated + 1);
                assert_eq!(
                    adapter
                        .captures
                        .lock()
                        .map_err(|_| Error::Storage("capture lock".into()))?
                        .len(),
                    captures
                );
                Ok(())
            }
        },
    )?)?;
    let current_bundle = HarnessBuilder::new()
        .journal(journal)
        .model(
            Model::new("fixture-native".into(), "all".into(), "mock-native-1".into(), Value::Null)?,
            adapter.clone(),
        )
        .content(ContentBindings {
            reader,
            writer: None,
        })
        .tasks(unsupported_tasks)
        .extensions_from(&agent)?
        .extensions(linked)
        .grant("model:generate")
        .grant("task:spawn:fixture.unsupported_options@1")
        .grant(read)
        .build()?;
    let current_runtime = current_bundle.runtime();
    let unsupported_task = current_runtime.task::<(), ()>("fixture.unsupported_options")?;
    assert_eq!(
        current_runtime
            .spawn(&unsupported_task, ())
            .await?
            .result()
            .await?,
        Outcome::Succeeded(())
    );
    let malformed_bytes = br#"{"mode":false,"scale":2}"#;
    let malformed = writer
        .stage(
            OperationId::new(),
            "malformed-options.json",
            malformed_bytes,
            "application/json",
            "malformed-options.json", None,
                )?.await?.0;
    let counts = adapter.counts.load(Ordering::SeqCst);
    let generated = adapter.generates.load(Ordering::SeqCst);
    assert!(
        agent.schemas()
            .validate_configuration_bytes(&extension, &malformed, malformed_bytes, None)
            .is_err()
    );
    assert_eq!(adapter.counts.load(Ordering::SeqCst), counts);
    assert_eq!(adapter.generates.load(Ordering::SeqCst), generated);
    Ok(())
}

struct ReplayMachine {
    identity: MachineIdentity,
    schema: Value,
}
impl ResumableMachine for ReplayMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, _: &Value) -> Result<MachineTransition> {
        Ok(MachineTransition {
            state: state.clone(),
            commands: Vec::new(),
            status: MachineStatus::Completed {
                value: state.clone(),
            },
        })
    }
}

#[allow(
    clippy::too_many_arguments,
    clippy::cognitive_complexity,
    reason = "one retained original-admission scenario includes reopen and mutation controls"
)]
async fn exercise_retained_native_boundary<A, O>(
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    signed: Scope,
    scope: RuntimeScope,
    reader: Arc<ReaderSpy>,
    selected: Vec<(FileRef, NativeMediaPolicy)>,
    writer: &FilesystemContentPublisher<A, O>,
) -> Result<()>
where
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    let machine: Arc<dyn ResumableMachine> = Arc::new(ReplayMachine {
        identity: MachineIdentity {
            name: "fixture.native_replay".into(),
            version: "1".into(),
            digest: [114; 32],
        },
        schema: json!({"type":"integer"}),
    });
    let definition = TaskDefinition::<u64, u64>::resumable(
        machine.clone(),
        json!({"type":"integer"}),
        json!({"type":"integer"}),
    )?;
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let definition = tasks.get_version::<u64, u64>("fixture.native_replay", "1", #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] None)?;
    let mut machines = MachineRegistry::default();
    machines.register(machine, &mut None)?;
    let stream = Arc::new(MemoryStream::default());
    let admission = OperationId::from_bytes([115; 16]);
    let task = TaskId::from_bytes(admission.into_bytes());
    let issuer = AuthorityIssuer::new(
        "native-replay-task",
        [23; 32],
        Authority {
            kind: AggregateKind::Task,
            id: task.to_string(),
        },
    );
    let signed = issuer.issue("native-replay-task-owner", signed.capabilities(), Some(signed
            .agent()
            .ok_or_else(|| Error::Unauthorized("original Agent identity absent".into()))?), None, &mut None).expect("scope proof");
    let turn = OperationId::from_bytes([116; 16]);
    let mut model = MockAdapter::new(reader);
    model.receipts_enabled = true;
    model.interrupt_after_capture.store(true, Ordering::SeqCst);
    let adapter = Arc::new(model);
    let content = ModelContent::Parts(
        selected
            .iter()
            .map(|(file, policy)| ModelContentPart::File {
                file: file.clone(),
                policy: FileProjectionPolicy::Native(Box::new(policy.clone())),
            })
            .collect(),
    );
    let conversion = explicit_conversion::conversion_tool()?;
    let mut conversion_tools = ToolRegistry::default();
    conversion_tools.register(conversion.clone())?;
    let conversion_journal = Arc::new(FilesystemExecutionJournal::new(
        stream.clone(),
        host.clone(),
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"), &acyclic_harness::core::SchemaRegistry::new(),
        &signed,
        65_536, None,
    )?);
    let converter = DurableToolRunner::new(&conversion_tools, conversion_journal, None)?;
    let conversion_operation = OperationId::from_bytes([117; 16]);
    let source = selected
        .first()
        .ok_or_else(|| Error::NotFound("conversion source".into()))?
        .0
        .clone();
    let conversion_input = explicit_conversion::ConvertImage {
        source: source.clone(),
        destination: "derived/image.img1".into(),
        mask: 255,
    };
    let mut retained_conversion = None;
    let mut fence = None;
    let mut first_capture = None;
    let mut completed = None;
    for pass in 0..3 {
        let runtime = FilesystemTaskRuntime::open(
            stream.clone(),
            host.clone(),
            &volume,
            &issuer.verifier(&mut None).expect("verifier copy"),
            &signed,
            &scope,
            &tasks,
            &machines,
            &ToolRegistry::default(),
            SessionLimits {
                active_tasks: 1,
                total_tasks: 1,
                depth: 1,
                model_steps: 1,
            },
            1,
            65_536, Arc::new(acyclic_stream::SystemUnixMillisClock), None, &[],
            &acyclic_harness::durable_tool::ResumableToolRegistry::default(), None,
        )
        .await?;
        if pass == 0 {
            assert!(matches!(
                runtime
                    .harness()
                    .admit(admission, &definition, 7, None)
                    .await?,
                Admission::Accepted(_)
            ));
            let worker = Worker {
                id: "native-replay".into(),
                available: ResourceSnapshot::default(),
                labels: BTreeMap::new(),
            };
            let lease = match runtime.task_host().pull_work(&worker).await? {
                WorkPull::Claimed(lease) => lease,
                WorkPull::Idle => return Err(Error::NotFound("original task lease".into())),
                WorkPull::Unresolved { error, .. } => return Err(error),
            };
            runtime.task_host().start_task(&lease, None).await?;
            fence = Some(LeaseFence::from(&lease.reservation));
        }
        let original = runtime.task_host().observe_admission(task).await?;
        assert_eq!(original.extensions.as_ref(), scope.extensions());
        let context = runtime.harness().durable_context(task, admission).await?;
        assert_eq!(context.scope().extensions(), scope.extensions());
        let writable = acyclic_harness::runtime::Bindings {
            tasks: tasks.clone(),
            tools: conversion_tools.clone(),
            scope: scope.clone(),
            concurrency: 1,
            durable_host: Some(runtime.task_host().clone()),
            interactions: None,
            content: Some(ContentBindings {
                reader: adapter.reader.clone(),
                writer: Some(FilesystemContentPublisher::new(host.clone(), &volume, &issuer.verifier(&mut None).expect("verifier copy"), &signed, 65_536, None)?),
            }),
            artifacts: None,
            policy: None,
            ..acyclic_harness::runtime::Bindings::local()
        }
        .build(None,
        )?
        .durable_context(task, admission)
        .await?;
        let arguments = serde_json::to_value(&conversion_input)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let outcome = converter
            .run(
                task,
                &conversion.definition,
                &acyclic_harness::tool::ToolInvocation {
                    operation_id: conversion_operation,
                    call_id: conversion_operation.to_string(),
                    name: conversion.definition.name.clone(),
                    arguments: arguments,
                },
                &ToolContext::new(std::sync::Arc::new(writable.clone()), conversion_operation, &(conversion_operation.to_string()), None)?,
                None,
                false,
            )?
            .await?.0;
        let Outcome::Succeeded(value) = outcome else {
            return Err(Error::Invalid(format!(
                "explicit conversion did not succeed: {outcome:?}"
            )));
        };
        let converted: explicit_conversion::ConvertedImage =
            serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(converted.source, source);
        assert_eq!(converted.mask, 255);
        let original_bytes = writable.read_file(&source).await?;
        let derived_bytes = writable.read_file(&converted.derived).await?;
        let mut expected_bytes = original_bytes.as_ref().to_vec();
        for pixel in expected_bytes.iter_mut().skip(8) {
            *pixel ^= 255;
        }
        assert_eq!(derived_bytes.as_ref(), expected_bytes.as_slice());
        if let Some(retained) = &retained_conversion {
            assert_eq!(&converted, retained);
        }
        retained_conversion = Some(converted.clone());
        let mut changed = conversion_input.clone();
        changed.mask = 1;
        assert!(
            converter
                .run(
                    task,
                    &conversion.definition,
                    &acyclic_harness::tool::ToolInvocation {
                        operation_id: conversion_operation,
                        call_id: conversion_operation.to_string(),
                        name: conversion.definition.name.clone(),
                        arguments: serde_json::to_value(changed)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                    },
                    &ToolContext::new(std::sync::Arc::new(writable.clone()), conversion_operation, &(conversion_operation.to_string()), None)?,
                    None,
                    false,
                )?
                .await.map(|result| result.0)
                .is_err()
        );
        let invocation = ToolInvocation {
            operation_id: conversion_operation,
            call_id: conversion_operation.to_string(),
            name: conversion.definition.name.clone(),
            arguments: serde_json::to_value(&conversion_input)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        };
        let projection: ToolResultContent = serde_json::from_value(
            conversion.projection.project(
                &invocation,
                &ToolResult {
                    value: serde_json::to_value(&converted)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                },
                None,
            )?.0,
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        let ToolResultContent::Parts { parts } = &projection else {
            return Err(Error::Invalid("derived parts".into()));
        };
        let Some(ModelDataPart::File {
            file,
            policy: FileProjectionPolicy::Native(policy),
        }) = parts.first()
        else {
            return Err(Error::Invalid("derived native file".into()));
        };
        let mut expected = selected.clone();
        expected.push((file.clone(), policy.as_ref().clone()));
        let mut converted_content = content.parts().to_vec();
        converted_content.push(ModelContentPart::File {
            file: file.clone(),
            policy: FileProjectionPolicy::Native(policy.clone()),
        });
        let execution = runtime
            .stock_execution(
                task,
                fence
                    .clone()
                    .ok_or_else(|| Error::NotFound("original lease".into()))?,
                turn,
                Model::new("fixture-native".into(), "all".into(), "mock-native-1".into(), Value::Null)?,
                adapter.clone(),
                ContextPipeline::default(), None,
            )
            .await?
            .with_max_output_tokens(16)?;
        let input = TurnInput {
            operation_id: execution.operation_id(),
            input: ModelContent::Parts(converted_content),
            selected_context: None,
            max_steps: 1,
        };
        let result = execution.execute(input).await;
        if pass == 0 {
            assert!(matches!(result, Err(Error::Storage(_))), "{result:?}");
            assert_eq!(adapter.generates.load(Ordering::SeqCst), 1);
            assert_eq!(adapter.reconciles.load(Ordering::SeqCst), 0);
            let capture = adapter
                .captures
                .lock()
                .map_err(|_| Error::Storage("capture lock".into()))?
                .first()
                .cloned()
                .ok_or_else(|| Error::NotFound("native dispatch capture".into()))?;
            let media = capture
                .parts
                .iter()
                .filter_map(|part| {
                    if let CapturedPart::Media(media) = part {
                        Some(media)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(media.len(), expected.len());
            assert_eq!(
                media
                    .last()
                    .ok_or_else(|| Error::NotFound("derived capture".into()))?
                    .bytes.as_ref(),
                derived_bytes.as_ref()
            );
            for (captured, (file, policy)) in media.iter().zip(&expected) {
                assert_eq!(&captured.file, file);
                assert_eq!(&captured.policy, policy);
                assert_eq!(
                    captured.options,
                    policy
                        .configuration
                        .as_ref()
                        .map(|_| json!({"mode":"strict","scale":2}))
                );
            }
            first_capture = Some(capture);
        } else {
            let output = result?;
            if let Some(previous) = &completed {
                assert_eq!(&output, previous);
            }
            completed = Some(output);
            assert_eq!(adapter.generates.load(Ordering::SeqCst), 1);
            assert_eq!(adapter.reconciles.load(Ordering::SeqCst), 1);
            let captures = adapter
                .captures
                .lock()
                .map_err(|_| Error::Storage("capture lock".into()))?;
            assert_eq!(captures.len(), 1);
            assert_eq!(captures.first(), first_capture.as_ref());
        }
        drop(execution);
        drop(runtime);
        if pass == 2 {
            exercise_media_compaction_fork_restart(
                host.clone(), volume.clone(), adapter.reader.clone(), &expected,
                invocation, projection, conversion.clone(),
            ).await?;
        }
        if pass == 0 {
            writer
                .stage(
                    OperationId::new(),
                    "media/0.bin",
                    b"replaced mutable media",
                    "application/octet-stream",
                    "replacement", None,
                )?.await?.0;
            writer
                .stage(
                    OperationId::new(),
                    "options.json",
                    br#"{"mode":"strict","scale":4}"#,
                    "application/json",
                    "replacement options", None,
                )?.await?.0;
        }
    }
    Ok(())
}


async fn exercise_media_compaction_fork_restart<A, O>(
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    reader: Arc<ReaderSpy>,
    selected: &[(FileRef, NativeMediaPolicy)],
    invocation: ToolInvocation,
    result: ToolResultContent,
    tool: Tool,
) -> Result<()>
where
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    use acyclic_harness::context::{
        CompactionPolicy, CompactionRetention, Context, ContextPlacement, SourceStage,
        ThresholdCompaction,
    };
    use acyclic_harness::executor::{ExecutionEvent, ExecutionJournal, Executor, ModelPurpose, StockExecutor};
    use acyclic_harness::filesystem::{FilesystemForkPreparer, workspace_ref};
    use acyclic_harness::fork::{ForkPreparation, ForkRequest, ForkSelection, ResourceRevision};
    use acyclic_harness::model::ModelPrefix;
    use acyclic_harness::resources::StreamRef;
    use acyclic_harness::store::StreamAggregate;
    let stream = Arc::new(MemoryStream::default());
    let stream_provider = ProviderRef::new("media-fork".into(), "stream".into(), "2".into(), &mut None)?;
    let parent = Authority {
        kind: AggregateKind::Conversation,
        id: "media-parent".into(),
    };
    let issuer = AuthorityIssuer::new("media-fork", [42; 32], parent.clone());
    let agent = match volume.owner() {
        VolumeOwner::Agent(agent) => *agent,
        _ => return Err(Error::Invalid("private media volume".into())),
    };
    let project = VolumeRef::new(
        volume.provider().clone(),
        "media-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("media".into()),
     &mut None)?;
    let project_head = host.create_volume(&project).await?;
    let grants = Capabilities::new([
        "conversation:bind".into(),
        "conversation:append".into(),
        "fork:publish".into(),
        "model:generate".into(),
        volume.capability(VolumeOperation::Read, &mut None)?,
        volume.capability(VolumeOperation::Write, &mut None)?,
        project.capability(VolumeOperation::Read, &mut None)?,
        project.capability(VolumeOperation::Write, &mut None)?,
    ]);
    let scope = issuer.issue("media-parent", &grants, Some(agent), None, &mut None).expect("scope proof");
    let journal = FilesystemExecutionJournal::new(
        stream.clone(),
        host.clone(),
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &SchemaRegistry::new(),
        &scope,
        65_536, None,
    )?
    .with_input_verifier(reader.clone());
    let adapter = Arc::new(MockAdapter::new(reader.clone()));
    let model = Model::new(
        "fixture-native".into(),
        "all".into(),
        "mock-native-1".into(),
        json!({"context_tokens":16_384}),
    )?;
    let exchange = vec![
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Parts(vec![ModelContentPart::ToolCall {
                call_id: invocation.call_id.clone(),
                name: invocation.name.clone(),
                arguments: invocation.arguments,
            }]),
        },
        ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Parts(vec![ModelContentPart::ToolResult {
                call_id: invocation.call_id,
                name: invocation.name,
                content: result,
            }]),
        },
    ];
    let mut messages = vec![ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text("old history ".repeat(1_100)),
    }];
    messages.extend(exchange.clone());
    let pipeline = ContextPipeline::new([Arc::new(SourceStage::new(
        "media-history",
        "1",
        Arc::new(Context {
            messages,
            metadata: BTreeMap::new(),
            current_input_index: None,
        }),
        ContextPlacement::Prepend,
    ))
        as Arc<dyn acyclic_harness::context::ContextStage>]);
    let tool_scope = RuntimeScope::new(Capabilities::new(["tool:call:example.convert_image"]), Limits::default())?;
    let mut tools = ToolRegistry::default();
    tools.register(tool)?;
    let executor = StockExecutor::new(
        model.clone(),
        adapter.clone(),
        pipeline,
        tools.clone(),
    )
    .with_tool_authority(tool_scope.clone(), None)?
    .with_max_output_tokens(16)?
    .with_compaction_policy(CompactionPolicy::Threshold(ThresholdCompaction {
        response_reserve_tokens: 4_096,
        recent_tokens: 1_024,
        retention: CompactionRetention::default(),
    }));
    let input = TurnInput {
        operation_id: OperationId::new(),
        selected_context: None,
        max_steps: 1,
        input: ModelContent::Parts(
            selected
                .iter()
                .map(|(file, policy)| ModelContentPart::File {
                    file: file.clone(),
                    policy: FileProjectionPolicy::Native(Box::new(policy.clone())),
                })
                .collect(),
        ),
    };
    executor.execute(input.clone(), &journal).await?;
    let records = journal.replay(input.operation_id, 0, 64).await?;
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(
                record.event,
                ExecutionEvent::ModelStarted {
                    purpose: ModelPurpose::Summary,
                    ..
                }
            ))
            .count(),
        1
    );
    let request = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ModelStarted {
                purpose: ModelPurpose::Response,
                request,
                ..
            } => Some(request),
            _ => None,
        })
        .ok_or_else(|| Error::NotFound("compacted native response".into()))?;
    let admitted =
        PreparedModelRequest::decode(&journal.load(request).await?, Limits::default(), None)?;
    assert!(
        admitted
            .request()
            .messages
            .windows(2)
            .any(|messages| messages == exchange)
    );
    assert!(!admitted.request().messages.iter().any(|message| matches!(&message.content, ModelContent::Text(text) if text.starts_with("old history"))));
    let prefix = journal.stage(OperationId::new(), "fork/media-prefix".into(),
        ModelPrefix::select(&admitted, None)?.canonical_bytes()?.to_vec(), ModelPrefix::MEDIA_TYPE).await?;
    let forks = acyclic_harness::fork::CompositeForkVerifier::new(&[
        acyclic_harness::filesystem::FilesystemForkVerifier::new(host.clone(), 65_536, None, None)?,
        acyclic_harness::fork::StreamHistoryForkVerifier::new(&stream_provider, &mut None)?,
    ], &mut None)?;
    let mut aggregate = StreamAggregate::open(
        &stream,
        parent.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(), None, acyclic_harness::store::default_projection_read_limits(), acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
    .await?
    .with_content_verifier(reader.clone())
    .with_fork_verifier(forks.clone());
    for action in [
        Action::BindConversation { agent },
        Action::AppendConversationMessage {
            message: Box::new(acyclic_harness::conversation::ConversationMessage {
                id: uuid::Uuid::new_v4(),
                sequence: 1,
                kind: acyclic_harness::conversation::MessageKind::User,
                content: prefix.clone(),
                attachments: acyclic_harness::conversation::ReferencedAttachments::Inline { items: admitted
                    .request()
                    .messages
                    .iter()
                    .flat_map(|message| message.content.file_refs())
                    .cloned()
                    .fold(Vec::new(), |mut files, file| {
                        if !files.contains(&file) {
                            files.push(file);
                        }
                        files
                    })
                    .into_iter()
                    .map(|file| acyclic_harness::conversation::Attachment { file, label: None })
                    .collect::<Vec<_>>() },
                reply_to: None,
                tool_call_id: None,
                extensions: BTreeMap::new().into(),
            }),
        },
    ] {
        aggregate
            .execute(Command {
                operation_id: OperationId::new(),
                idempotency_key: IdempotencyKey::new(format!(
                    "media:{}",
                    aggregate.reducer().revision()
                ))?,
                expected_revision: aggregate.reducer().revision(),
                scope: scope.clone(),
                causal_parent: None,
                action,
            }, #[cfg(feature = "filesystem")] None, &mut None)
            .await?;
    }
    let child_agent = AgentId::new();
    let child = Authority {
        kind: AggregateKind::Conversation,
        id: "media-child".into(),
    };
    let child_private = VolumeRef::new(
        volume.provider().clone(),
        "media-child-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
     &mut None)?;
    let child_project = VolumeRef::new(
        volume.provider().clone(),
        "media-child-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("media".into()),
     &mut None)?;
    let preparer = FilesystemForkPreparer::new(
        host.clone(),
        Reducer::restore(aggregate.reducer().snapshot(usize::MAX, &mut None)?.0,
            issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new(), &mut None)?,
        issuer.verifier(&mut None).expect("verifier copy"),
        scope.clone(),
        project.clone(),
        stream_provider.clone(),
        reader.clone(),
        None, None, #[cfg(not(target_arch = "wasm32"))] None,
    )?;
    let report = aggregate
        .prepare_fork(
            &preparer,
            ForkRequest {
                authorized_source: None,
                operation_id: OperationId::new(),
                parent: parent.clone(),
                parent_revision: aggregate.reducer().revision(),
                child: child.clone(),
                child_agent,
                attached_agents: Vec::new(),
                boundary: None,
                preparation: ForkPreparation {
                    child_result_volume: None,
                    summary: None,
                    child_project_volume: child_project.clone(),
                    child_private_volume: child_private.clone(),
                    inherited_through_sequence: 1,
                    maximum_inherited_messages: 4,
                    maximum_inherited_bytes: 65_536,
                    maximum_inherited_references: 32,
                },
                selections: vec![
                    ForkSelection {
                        required: true,
                        revision: ResourceRevision::History(StreamRef::new(
                            stream_provider,
                            parent.stream_path(&mut None)?.into_bytes(),
                            Some(aggregate.reducer().revision().to_string()),
                        )?),
                    },
                    ForkSelection {
                        required: true,
                        revision: ResourceRevision::Project {
                            volume: project,
                            generation: project_head.generation,
                        },
                    },
                    ForkSelection {
                        required: true,
                        revision: ResourceRevision::PrivateVolume {
                            volume: volume.clone(),
                            generation: host
                                .resolve(&workspace_ref(
                                    volume.provider().clone(),
                                    &volume.storage_name(&mut None)?,
                                )?)
                                .await?
                                .generation,
                            paths: Vec::new(),
                        },
                    },
                ],
            },
        )
        .await?;
    let child_issuer = AuthorityIssuer::new("media-fork-child", [43; 32], child.clone());
    let child_scope = child_issuer.issue("media-child", &Capabilities::new(
            [
                "conversation:bind".into(),
                child_private.capability(VolumeOperation::Read, &mut None)?,
                child_private.capability(VolumeOperation::Write, &mut None)?,
                child_project.capability(VolumeOperation::Read, &mut None)?,
                child_project.capability(VolumeOperation::Write, &mut None)?,
            ]
            .into_iter()
            .chain(
                report
                    .reference_grants
                    .iter()
                    .map(|grant| grant.capability())
                    .collect::<Result<Vec<_>>>()?,
            ),
        ), Some(child_agent), None, &mut None).expect("scope proof");
    let child_reader = FilesystemContentVerifier::new(host.clone(), &child_issuer.verifier(&mut None).expect("verifier copy"), &child_scope, 65_536, None)?;
    let mut child_aggregate = StreamAggregate::open(
        &stream,
        child,
        child_issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(), None, acyclic_harness::store::default_projection_read_limits(), acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
    .await?
    .with_content_verifier(child_reader.clone())
    .with_fork_verifier(forks);
    child_aggregate
        .spawn_from_report(&mut aggregate, report, scope, child_scope.clone())
        .await?;
    let child_executor = || {
        StockExecutor::new(
            model.clone(),
            adapter.clone(),
            ContextPipeline::default(),
            tools.clone(),
        )
        .with_tool_authority(tool_scope.clone(), None)?
        .with_compaction_policy(CompactionPolicy::Disabled)
        .with_inherited_prefix(prefix.clone(), child_reader.clone())
    };
    let child_input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("child continues".into()),
        selected_context: None,
        max_steps: 1,
    };
    let open_journal = || {
        FilesystemExecutionJournal::new(
            stream.clone(),
            host.clone(),
            &child_private,
            &child_issuer.verifier(&mut None).expect("verifier copy"),
            &SchemaRegistry::new(),
            &child_scope,
            65_536, None,
        )
        .map(|journal| journal.with_input_verifier(child_reader.clone()))
    };
    let child_output = child_executor()?
        .execute(child_input.clone(), &open_journal()?)
        .await?;
    let captures = adapter
        .captures
        .lock()
        .map_err(|_| Error::Storage("media fork capture".into()))?
        .clone();
    let parent_capture = captures
        .get(1)
        .ok_or_else(|| Error::NotFound("parent native capture".into()))?;
    let child_capture = captures
        .last()
        .ok_or_else(|| Error::NotFound("child native capture".into()))?;
    let media = |capture: &Capture| {
        capture
            .parts
            .iter()
            .filter_map(|part| match part {
                CapturedPart::Media(media) => Some(media.as_ref().clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(media(parent_capture), media(child_capture));
    drop(child_aggregate);
    drop(aggregate);
    let reopened = open_journal()?;
    assert_eq!(
        child_executor()?.execute(child_input, &reopened).await?,
        child_output
    );
    assert_eq!(
        *adapter
            .captures
            .lock()
            .map_err(|_| Error::Storage("media reopen capture".into()))?,
        captures
    );
    assert_eq!(
        PreparedModelRequest::decode(&journal.load(request).await?, Limits::default(), None)?
            .bytes(),
        admitted.bytes()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn live_original_options_and_actual_native_parts_reach_adapter_without_conversion()
-> Result<()> {
    exercise_live_native_boundary().await
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test::wasm_bindgen_test]
async fn live_original_options_and_actual_native_parts_reach_adapter_without_conversion()
-> std::result::Result<(), wasm_bindgen::JsValue> {
    tokio::task::LocalSet::new()
        .run_until(exercise_live_native_boundary())
        .await
        .map_err(|error| wasm_bindgen::JsValue::from_str(&error.to_string()))
}

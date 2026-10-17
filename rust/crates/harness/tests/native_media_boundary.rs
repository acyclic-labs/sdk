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
        FileProjectionPolicy, ImageDetail, Model, ModelContent, ModelContentPart, ModelDataPart,
        ModelDispatch, ModelEvent, ModelMessage, ModelProvider, ModelRequest, ModelRole,
        NativeConfigurationBinding, NativeMediaIntent, NativeMediaPolicy, PreparedModelRequest,
        ToolResultContent,
    },
    resources::ProviderRef,
    runtime::{ContentBindings, RuntimeScope, TaskDefinition, TaskRegistry, ToolContext},
    scheduler::{LeaseFence, ResourceRequest, SessionLimits},
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
    workflow::{
        MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, ResumableMachine,
    },
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream, MemoryStream};
use futures::{StreamExt as _, stream};
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
    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        maximum_bytes: u64,
        ceiling: Option<&'a acyclic_harness::Capabilities>,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>>
    {
        self.reads.fetch_add(1, Ordering::SeqCst);
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
            if self.missing.load(Ordering::SeqCst) {
                return Err(Error::NotFound(
                    "original native media is unavailable".into(),
                ));
            }
            let bytes = self.inner.read(file, maximum_bytes, ceiling, io)?.await?;
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
    Reasoning(acyclic_harness::model::ReasoningBlock),
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
        if *policy == FileProjectionPolicy::BoundedFull
            && file.descriptor().media_type() == "text/plain"
        {
            let bytes = self
                .reader
                .read(file, file.descriptor().byte_length(), None, None)?
                .await?;
            file.descriptor().verify(&bytes, &mut None)?;
            return Ok(CapturedPart::Text(
                std::str::from_utf8(&bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .into(),
            ));
        }
        let FileProjectionPolicy::Native(policy) = policy else {
            return Err(Error::Unsupported(
                "fixture accepts explicit native input only".into(),
            ));
        };
        policy.validate(file, Limits::default(), &mut None)?;
        let bytes = self
            .reader
            .read(file, file.descriptor().byte_length(), None, None)?
            .await?;
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
            let bytes = self
                .reader
                .read(
                    &binding.configuration.content,
                    binding.configuration.content.descriptor().byte_length(),
                    None,
                    None,
                )?
                .await?;
            binding
                .configuration
                .content
                .descriptor()
                .verify(&bytes, &mut None)?;
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
            context_tokens: model.options["context_tokens"]
                .as_u64()
                .and_then(|tokens| u32::try_from(tokens).ok())
                .unwrap_or(131_072),
            output_tokens: 4_096,
        })
    }
    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<ModelTokenCount>> {
        Box::pin(async move {
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
                    // The model receives payloads and options, not reference or admission metadata.
                    for binding in message.content.native_configurations() {
                        let metadata = serde_json::to_vec(binding)
                            .map_err(|error| Error::Invalid(error.to_string()))?
                            .len() as u64;
                        units = units.checked_sub(metadata).ok_or_else(|| {
                            Error::Invalid("fixture metadata count differs".into())
                        })?;
                    }
                    for file in message.content.file_refs() {
                        if !message
                            .content
                            .native_configurations()
                            .any(|binding| &binding.configuration.content == file)
                        {
                            let metadata = serde_json::to_vec(file)
                                .map_err(|error| Error::Invalid(error.to_string()))?
                                .len() as u64;
                            units = units.checked_sub(metadata).ok_or_else(|| {
                                Error::Invalid("fixture reference count differs".into())
                            })?;
                        }
                        units = units
                            .checked_add(file.descriptor().byte_length())
                            .ok_or_else(|| Error::Invalid("fixture count overflow".into()))?;
                    }
                    u32::try_from(units)
                        .map_err(|_| Error::Invalid("fixture count overflow".into()))
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
            Ok(ModelTokenCount::from_upper_bounds(
                if self.wrong_count_digest.load(Ordering::SeqCst) {
                    [99; 32]
                } else {
                    request.request_digest()
                },
                u32::try_from(fixed)
                    .map_err(|_| Error::Invalid("fixture framing count overflow".into()))?,
                message_tokens,
            ))
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
                        ModelContentPart::Reasoning { block } => {
                            parts.push(CapturedPart::Reasoning(block.clone()));
                        }
                        ModelContentPart::Text { text } => {
                            parts.push(CapturedPart::Text(text.clone()));
                        }
                        ModelContentPart::File { file, policy } => {
                            parts.push(self.file(file, policy).await?);
                        }
                        ModelContentPart::ToolCall(acyclic_harness::model::ToolCall { call_id, name, .. }) => {
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
    fn reconcile<'a>(
        &'a self,
        dispatch: acyclic_harness::model::ModelDispatch,
        observed: &'a [acyclic_harness::model::ModelEvent],
    ) -> BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>> {
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
            if recorded.request_digest != dispatch.request_digest || !events.starts_with(observed) {
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
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _context: Option<acyclic_harness::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "fixture projection does not execute a tool".into(),
            ))
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _context: Option<acyclic_harness::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "fixture projection does not reconcile a tool".into(),
            ))
        })
    }
}
impl ToolProjection for NativeProjector {
    fn project(
        &self,
        _: &ToolInvocation,
        result: &ToolResult,
        work: acyclic_harness::NativeWork<'_>,
    ) -> Result<(Value, acyclic_harness::NativeCustody)> {
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

#[path = "native_media_boundary_cases/live.rs"]
mod live;
use live::*;
#[path = "native_media_boundary_cases/retained.rs"]
mod retained;
use retained::*;
#[path = "native_media_boundary_cases/compaction.rs"]
mod compaction;
use compaction::*;

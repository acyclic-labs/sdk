//! Explicit credential-free functional provider for installed runtime qualification.
//! Results derive from admitted request bytes; this is not wire-event playback.

use acyclic_harness::{Error, Result,
    context::{ModelContextCapacity, ModelTokenCount},
    model::{Model, ModelAttempt, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use serde::Serialize;
use serde_json::json;
use std::{fs::File, io::{Read as _, Write as _}, path::Path, sync::Mutex};
use futures::StreamExt as _;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use acyclic_harness::{NativeFutureBundle, model::{NativeModelAttempt, NativeModelStream}};

const MAX_LEDGER_BYTES: u64 = 1024 * 1024;
const MAX_LEDGER_RECORDS: u64 = 128;

/// Independent finite diagnostic evidence of actual provider entry, never task
/// state, a model result cache, an effect receipt or replay input.
struct InvocationLedger {
    file: File,
    bytes: u64,
    records: u64,
}

#[derive(Serialize)]
struct InvocationRecord {
    format_version: u32,
    phase: &'static str,
    operation_id: acyclic_harness::OperationId,
    step: u32,
    request_digest: String,
}

pub struct Deterministic {
    delay_ms: u32,
    ledger: Mutex<InvocationLedger>,
}

impl Deterministic {
    pub fn new(delay_ms: u32, path: &Path) -> Result<Self> {
        if !path.is_absolute() || path.components().any(|part| matches!(part, std::path::Component::ParentDir)) {
            return Err(Error::Invalid("model invocation observer requires an absolute original path".into()));
        }
        let mut file = std::fs::OpenOptions::new().read(true).append(true).create(true).open(path)
            .map_err(|error| Error::Storage(format!("model invocation observer open: {error}")))?;
        let bytes = file.metadata().map_err(|error| Error::Storage(error.to_string()))?.len();
        if bytes > MAX_LEDGER_BYTES {
            return Err(Error::Invalid("model invocation observer exceeds byte bound".into()));
        }
        let mut records = 0_u64;
        let mut buffer = [0_u8; 4096];
        let mut final_byte = None;
        loop {
            let count = file.read(&mut buffer).map_err(|error| Error::Storage(error.to_string()))?;
            if count == 0 { break; }
            records += buffer[..count].iter().filter(|byte| **byte == b'\n').count() as u64;
            final_byte = Some(buffer[count - 1]);
            if records > MAX_LEDGER_RECORDS {
                return Err(Error::Invalid("model invocation observer exceeds record bound".into()));
            }
        }
        if final_byte.is_some_and(|byte| byte != b'\n') {
            return Err(Error::Storage("model invocation observer has an interrupted record".into()));
        }
        Ok(Self { delay_ms, ledger: Mutex::new(InvocationLedger { file, bytes, records }) })
    }

    fn observe_invocation(&self, phase: &'static str, dispatch: ModelDispatch) -> Result<()> {
        let record = InvocationRecord {
            format_version: 1, phase, operation_id: dispatch.operation_id,
            step: dispatch.step, request_digest: hex::encode(dispatch.request_digest),
        };
        let mut bytes = serde_json::to_vec(&record).map_err(|error| Error::Invalid(error.to_string()))?;
        bytes.push(b'\n');
        let mut ledger = self.ledger.lock().map_err(|_| Error::Storage("model invocation observer lock poisoned".into()))?;
        let next_bytes = ledger.bytes.checked_add(bytes.len() as u64)
            .ok_or_else(|| Error::Invalid("model invocation observer byte count overflow".into()))?;
        if ledger.records >= MAX_LEDGER_RECORDS || next_bytes > MAX_LEDGER_BYTES {
            return Err(Error::Invalid("model invocation observer finite allowance exhausted".into()));
        }
        ledger.file.write_all(&bytes)
            .and_then(|()| ledger.file.sync_all())
            .map_err(|error| Error::Storage(format!("model invocation observer append: {error}")))?;
        ledger.bytes = next_bytes;
        ledger.records += 1;
        Ok(())
    }

    fn context_capacity_engine(&self, selection: &Model) -> Result<ModelContextCapacity> {
        if selection.provider != "graphcoder-deterministic" || selection.name != "request-digest"
            || selection.revision != "1"
            || !selection.options.as_object().is_some_and(|options|
                options.len() == 1 && options.get("dispatch_delay_ms").and_then(serde_json::Value::as_u64) == Some(u64::from(self.delay_ms)))
        {
            return Err(Error::Unsupported("native terminal has no adapter for this model selection".into()));
        }
        Ok(ModelContextCapacity { context_tokens: 131_072, output_tokens: 4096 })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn observe_invocation_native(
        &self, phase: &'static str, dispatch: ModelDispatch, work: &NativeMaterializationWork,
    ) -> Result<()> {
        // The complete bounded diagnostic record is built on the stack. It is
        // independent evidence, never a semantic cache or a retry suppression.
        work.remaining()?;
        let mut storage = [0u8; 512];
        let digest = digest_hex(dispatch.request_digest);
        let digest = std::str::from_utf8(&digest).expect("hex encoder emits ASCII");
        let mut writer = std::io::Cursor::new(storage.as_mut_slice());
        writeln!(writer, "{{\"format_version\":1,\"phase\":\"{phase}\",\"operation_id\":\"{}\",\"step\":{},\"request_digest\":\"{digest}\"}}",
            dispatch.operation_id, dispatch.step)
            .map_err(|_| Error::Invalid("model invocation record exceeds stack bound".into()))?;
        let length = writer.position() as usize;
        let bytes = &storage[..length];
        let mut ledger = self.ledger.lock().map_err(|_| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Storage, reason: "model invocation observer lock poisoned" })?;
        let next_bytes = ledger.bytes.checked_add(bytes.len() as u64)
            .ok_or(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "model invocation observer byte count overflow" })?;
        if ledger.records >= MAX_LEDGER_RECORDS || next_bytes > MAX_LEDGER_BYTES {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "model invocation observer finite allowance exhausted" });
        }
        let mut written = 0;
        while written < bytes.len() {
            work.admit(WorkCounters { object_bytes_written: (bytes.len() - written) as u64, ..Default::default() })?;
            work.charge(WorkCounters { backend_write_operations: 1, ..Default::default() })?;
            let count = ledger.file.write(&bytes[written..])
                .map_err(acyclic_fs::NativeTreeMaterializationError::Io)?;
            work.spent(WorkCounters { object_bytes_written: count as u64, ..Default::default() })?;
            if count == 0 { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Storage,
                reason: "model invocation observer write made no progress" }); }
            written += count;
        }
        work.charge(WorkCounters { durability_operations: 1, ..Default::default() })?;
        ledger.file.sync_all().map_err(acyclic_fs::NativeTreeMaterializationError::Io)?;
        ledger.bytes = next_bytes;
        ledger.records += 1;
        Ok(())
    }
}

pub fn model(delay_ms: u32) -> Model {
    Model { provider: "graphcoder-deterministic".into(), name: "request-digest".into(),
        revision: "1".into(), options: json!({"dispatch_delay_ms":delay_ms}) }
}

fn events(dispatch: ModelDispatch) -> Vec<ModelEvent> {
    vec![ModelEvent::Content {
        delta: format!("deterministic request={} step={}", hex::encode(dispatch.request_digest), dispatch.step),
    }, ModelEvent::Completed { metadata: json!({"provider":"graphcoder-deterministic","output_tokens":128}) }]
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn digest_hex(digest: [u8; 32]) -> [u8; 64] {
    let mut encoded = [0; 64];
    hex::encode_to_slice(digest, &mut encoded).expect("exact digest hex capacity");
    encoded
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_events(
    dispatch: ModelDispatch, work: &NativeMaterializationWork,
) -> Result<[(ModelEvent, NativeAllocation); 2]> {
    work.remaining()?;
    let digest = digest_hex(dispatch.request_digest);
    let digest = std::str::from_utf8(&digest).expect("hex encoder emits ASCII");
    let mut storage = [0u8; 256];
    let mut writer = std::io::Cursor::new(storage.as_mut_slice());
    write!(writer, "{{\"kind\":\"content\",\"delta\":\"deterministic request={digest} step={}\"}}", dispatch.step)
        .map_err(|_| Error::Invalid("deterministic event exceeds stack bound".into()))?;
    let length = writer.position() as usize;
    let content = ModelEvent::decode_with_native_work(&storage[..length], work)?;
    let complete = ModelEvent::decode_with_native_work(
        br#"{"kind":"completed","metadata":{"provider":"graphcoder-deterministic","output_tokens":128}}"#, work,
    )?;
    Ok([content, complete])
}

#[derive(Default)]
struct ByteCount(usize);

impl std::io::Write for ByteCount {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("serialized token count overflows"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

impl ModelProvider for Deterministic {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_model(&self, work: &NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn context_capacity_with_native_work(&self, selection: &Model, work: &NativeMaterializationWork) -> Result<ModelContextCapacity> {
        let examined = (selection.provider.len() as u64).checked_add(selection.name.len() as u64)
            .and_then(|items| items.checked_add(selection.revision.len() as u64))
            .and_then(|items| items.checked_add(4))
            .ok_or_else(|| Error::Invalid("model selection traversal overflows".into()))?;
        work.charge(WorkCounters { items_examined: examined, ..Default::default() })?;
        self.context_capacity_engine(selection)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn count_tokens_with_native_work(&self, request: &PreparedModelRequest, work: &NativeMaterializationWork) -> Result<(ModelTokenCount, NativeAllocation)> {
        self.context_capacity_with_native_work(&request.request().model, work)?;
        request.serialized_token_upper_bound_with_native_work(work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn generate_with_native_work<'a>(
        &'a self, request: &'a PreparedModelRequest, dispatch: ModelDispatch,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, NativeModelStream<'a>>> {
        work.remaining()?;
        if request.manifest().request_digest != dispatch.request_digest {
            return Err(Error::Conflict("model dispatch request changed".into()));
        }
        self.context_capacity_with_native_work(&request.request().model, work)?;
        NativeFutureBundle::new(work, || async move {
            self.observe_invocation_native("generate", dispatch, work)?;
            if self.delay_ms > 0 {
                let digest = digest_hex(dispatch.request_digest);
                eprintln!("graphcoder deterministic-dispatch operation={} step={} request={}",
                    dispatch.operation_id, dispatch.step, std::str::from_utf8(&digest).expect("hex ASCII"));
            }
            tokio::time::sleep(std::time::Duration::from_millis(u64::from(self.delay_ms))).await;
            let events = native_events(dispatch, work)?;
            NativeModelStream::new(work, || futures::stream::iter(events.into_iter().map(Ok)))
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_work<'a>(
        &'a self, attempt: NativeModelAttempt<'a>, work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (Option<Vec<ModelEvent>>, NativeAllocation)>> {
        NativeFutureBundle::new(work, || async move {
            let dispatch = ModelDispatch {
                operation_id: attempt.operation_id, step: attempt.step, request_digest: attempt.request_digest,
            };
            self.observe_invocation_native("reconcile", dispatch, work)?;
            let complete = native_events(dispatch, work)?;
            work.charge(WorkCounters { items_examined: attempt.observed.len() as u64, ..Default::default() })?;
            if attempt.observed.len() > complete.len()
                || !attempt.observed.iter().zip(&complete).all(|(observed, item)| observed == &item.0)
            {
                return Err(Error::Conflict("deterministic provider retained prefix changed".into()));
            }
            let capacity = complete.len() - attempt.observed.len();
            let bytes = (capacity * std::mem::size_of::<ModelEvent>()) as u64;
            let mut custody = if bytes == 0 { work.retain_owned(0)? } else { work.claim(bytes)? };
            let mut suffix = Vec::with_capacity(capacity);
            for mut item in complete.into_iter().skip(attempt.observed.len()) {
                custody.absorb(&mut item.1)?;
                suffix.push(item.0);
            }
            Ok((Some(suffix), custody))
        })
    }

    fn context_capacity(&self, selection: &Model) -> Result<ModelContextCapacity> {
        self.context_capacity_engine(selection)
    }

    fn count_tokens(&self, request: &PreparedModelRequest) -> Result<ModelTokenCount> {
        self.context_capacity(&request.request().model)?;
        let message_tokens = request.request().messages.iter().map(|message| {
            let mut serialized = ByteCount::default();
            serde_json::to_writer(&mut serialized, message).map_err(|error| Error::Invalid(error.to_string()))?;
            let bytes = message.content.file_refs().iter().try_fold(
                serialized.0 as u64,
                |total, file| total.checked_add(file.descriptor().byte_length())
                    .ok_or_else(|| Error::Invalid("deterministic token count overflows".into())),
            )?;
            u32::try_from(bytes).map_err(|_| Error::Invalid("deterministic token count exceeds u32".into()))
        }).collect::<Result<Vec<_>>>()?;
        // Count the full serialized envelope as fixed overhead as well. This
        // conservative additive upper bound includes all model/tool framing.
        let fixed_tokens = u32::try_from(request.bytes().len())
            .map_err(|_| Error::Invalid("deterministic framing exceeds u32".into()))?;
        Ok(ModelTokenCount { request_digest: request.manifest().request_digest, fixed_tokens, message_tokens })
    }

    fn generate<'a>(&'a self, request: PreparedModelRequest, dispatch: ModelDispatch)
        -> BoxProviderStream<'a, Result<ModelEvent>>
    {
        if request.manifest().request_digest != dispatch.request_digest {
            return Box::pin(futures::stream::iter([Err(Error::Conflict("model dispatch request changed".into()))]));
        }
        if let Err(error) = self.context_capacity(&request.request().model) {
            return Box::pin(futures::stream::iter([Err(error)]));
        }
        let delay_ms = self.delay_ms;
        Box::pin(futures::stream::once(async move {
            self.observe_invocation("generate", dispatch)?;
            if delay_ms > 0 {
                eprintln!("graphcoder deterministic-dispatch operation={} step={} request={}",
                    dispatch.operation_id, dispatch.step, hex::encode(dispatch.request_digest));
            }
            tokio::time::sleep(std::time::Duration::from_millis(u64::from(delay_ms))).await;
            Ok(events(dispatch))
        }).flat_map(|result| match result {
            Ok(items) => futures::future::Either::Left(futures::stream::iter(items.into_iter().map(Ok))),
            Err(error) => futures::future::Either::Right(futures::stream::iter([Err(error)])),
        }))
    }

    fn reconcile<'a>(&'a self, attempt: ModelAttempt) -> BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async move {
            self.observe_invocation("reconcile", attempt.dispatch())?;
            let complete = events(attempt.dispatch());
            if !complete.starts_with(&attempt.observed) {
                return Err(Error::Conflict("deterministic provider retained prefix changed".into()));
            }
            Ok(Some(complete.into_iter().skip(attempt.observed.len()).collect()))
        })
    }
}

// This independent qualification oracle chooses semantic obligations from the
// real admitted exchange. Its receipts are actual generated responses, not the
// diagnostic invocation ledger or a prerecorded sequence of runtime events.
const SCENARIO_RECEIPT_BYTES: usize = 65_536;
const SCENARIO_RECEIPTS: usize = 32;
const SCENARIO_EVENTS: usize = 16;
const SCENARIO_HEADER: usize = 59;
const SCENARIO_MAGIC: &[u8; 6] = b"GCOR1\0";

#[derive(Clone, Copy)]
enum ScenarioRole { Root, Left, Right, Grandchild }

impl ScenarioRole {
    fn index(self) -> usize {
        match self { Self::Root => 0, Self::Left => 1, Self::Right => 2, Self::Grandchild => 3 }
    }
    fn directive(self) -> &'static str {
        match self {
            Self::Root => "graphcoder-recursive-fixture role=root",
            Self::Left => "graphcoder-recursive-fixture role=left",
            Self::Right => "graphcoder-recursive-fixture role=right",
            Self::Grandchild => "graphcoder-recursive-fixture role=grandchild",
        }
    }
    fn path(self) -> &'static str {
        match self {
            Self::Root => "graphcoder-root.txt", Self::Left => "graphcoder-left.txt",
            Self::Right => "graphcoder-right.txt", Self::Grandchild => "graphcoder-grandchild.txt",
        }
    }
    fn before(self) -> &'static str {
        match self {
            Self::Root => "graphcoder root before exact edit\n",
            Self::Left => "graphcoder left before exact edit\n",
            Self::Right => "graphcoder right before exact edit\n",
            Self::Grandchild => "graphcoder grandchild before exact edit\n",
        }
    }
    fn after(self) -> &'static str {
        match self {
            Self::Root => "graphcoder root after exact edit\n",
            Self::Left => "graphcoder left after exact edit\n",
            Self::Right => "graphcoder right after exact edit\n",
            Self::Grandchild => "graphcoder grandchild after exact edit\n",
        }
    }
    fn call(self, goal: usize) -> &'static str {
        const IDS: [[&str; 4]; 4] = [
            ["graphcoder-root-write", "graphcoder-root-read", "graphcoder-root-edit", "graphcoder-root-read-edited"],
            ["graphcoder-left-write", "graphcoder-left-read", "graphcoder-left-edit", "graphcoder-left-read-edited"],
            ["graphcoder-right-write", "graphcoder-right-read", "graphcoder-right-edit", "graphcoder-right-read-edited"],
            ["graphcoder-grandchild-write", "graphcoder-grandchild-read", "graphcoder-grandchild-edit", "graphcoder-grandchild-read-edited"],
        ];
        IDS[self.index()][goal]
    }
}

struct ScenarioSuccess {
    task: acyclic_harness::TaskId,
    actual_result: String,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _custody: Option<NativeAllocation>,
}

#[derive(Default)]
struct ScenarioChild {
    task: Option<acyclic_harness::TaskId>,
    success: Option<std::sync::Arc<ScenarioSuccess>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    original_work: Option<NativeMaterializationWork>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _binding: Option<NativeAllocation>,
}

/// Controlled external model for the distinct native recursive qualification.
/// The terminal supplies completion data only after observing real SDK success.
pub struct NativeScenarioOracle {
    diagnostic: Deterministic,
    actors: [acyclic_harness::AgentId; 4],
    receipts: std::path::PathBuf,
    receipt_count: Mutex<usize>,
    children: Mutex<[ScenarioChild; 3]>,
    completion: tokio::sync::Notify,
    last_receipt: Mutex<Option<std::sync::Arc<ScenarioReceiptPath>>>,
}

pub fn scenario_model(delay_ms: u32, actors: [acyclic_harness::AgentId; 4]) -> Model {
    Model { provider: "graphcoder-deterministic".into(), name: "graphcoder-recursive-fixture".into(),
        revision: "1".into(), options: json!({"dispatch_delay_ms":delay_ms,"actors":actors}) }
}

impl NativeScenarioOracle {
    pub fn new(delay_ms: u32, invocation_path: &Path, receipts: &Path,
        actors: [acyclic_harness::AgentId; 4]) -> Result<Self> {
        if !receipts.is_absolute()
            || receipts.components().any(|part| matches!(part, std::path::Component::ParentDir))
            || actors.iter().enumerate().any(|(index, actor)| actors[..index].contains(actor)) {
            return Err(Error::Invalid("recursive fixture requires an absolute receipt directory and four distinct actors".into()));
        }
        std::fs::create_dir_all(receipts).map_err(|error| Error::Storage(error.to_string()))?;
        let mut count = 0;
        for entry in std::fs::read_dir(receipts).map_err(|error| Error::Storage(error.to_string()))? {
            let entry = entry.map_err(|error| Error::Storage(error.to_string()))?;
            if !entry.file_type().map_err(|error| Error::Storage(error.to_string()))?.is_file()
                || entry.path().extension().and_then(|value| value.to_str()) != Some("response")
                || entry.metadata().map_err(|error| Error::Storage(error.to_string()))?.len() > SCENARIO_RECEIPT_BYTES as u64 {
                return Err(Error::Storage("recursive fixture receipt directory contains an invalid entry".into()));
            }
            count += 1;
            if count > SCENARIO_RECEIPTS {
                return Err(Error::Storage("recursive fixture receipt count exceeds its bound".into()));
            }
        }
        Ok(Self { diagnostic: Deterministic::new(delay_ms, invocation_path)?, actors,
            receipts: receipts.to_path_buf(), receipt_count: Mutex::new(count),
            children: Mutex::new(std::array::from_fn(|_| ScenarioChild::default())),
            completion: tokio::sync::Notify::new(), last_receipt: Mutex::new(None) })
    }

    pub fn pending_children(&self) -> Result<[Option<acyclic_harness::TaskId>; 3]> {
        let children = self.children.lock().map_err(|_| Error::Storage("recursive fixture completion lock poisoned".into()))?;
        Ok(std::array::from_fn(|index| if children[index].success.is_none() { children[index].task } else { None }))
    }

    /// The caller is the real terminal scheduler, never this model provider's
    /// hidden task-host reader. `actual_result` is the actual SDK-read TurnOutput.text.
    pub fn observe_child_succeeded(&self, task: acyclic_harness::TaskId, actual_result: &str) -> Result<()> {
        let mut children = self.children.lock().map_err(|_| Error::Storage("recursive fixture completion lock poisoned".into()))?;
        let child = children.iter_mut().find(|child| child.task == Some(task))
            .ok_or_else(|| Error::Conflict("recursive fixture received an unrequested child completion".into()))?;
        let budget = ScenarioBudget {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native: child.original_work.as_ref(),
            _lifetime: std::marker::PhantomData,
        };
        budget.check()?;
        budget.inspect(actual_result.len() as u64)?;
        if actual_result.is_empty() || actual_result.len() > SCENARIO_RECEIPT_BYTES {
            return Err(budget.reject(acyclic_harness::wire::ErrorCode::Invalid,
                "recursive fixture successful result is empty or oversized"));
        }
        if let Some(previous) = &child.success {
            if previous.actual_result != actual_result {
                return Err(budget.reject(acyclic_harness::wire::ErrorCode::Conflict,
                    "recursive fixture child success changed"));
            }
            return Ok(());
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = &child.original_work {
            work.charge(WorkCounters { bytes_copied: actual_result.len() as u64, ..Default::default() })?;
            let mut custody = work.claim(std::mem::size_of::<ScenarioSuccess>() as u64)?;
            custody.grow(actual_result.len() as u64)?;
            let mut copied = String::new();
            copied.try_reserve_exact(actual_result.len()).map_err(Error::Allocation)?;
            custody.resize_retained((std::mem::size_of::<ScenarioSuccess>() + copied.capacity()) as u64)?;
            copied.push_str(actual_result);
            work.remaining()?;
            child.success = Some(std::sync::Arc::new(ScenarioSuccess { task, actual_result: copied, _custody: Some(custody) }));
            self.completion.notify_waiters();
            return Ok(());
        }
        child.success = Some(std::sync::Arc::new(ScenarioSuccess { task, actual_result: actual_result.to_owned(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _custody: None }));
        self.completion.notify_waiters();
        Ok(())
    }

    fn capacity(&self, model: &Model, budget: &ScenarioBudget<'_>) -> Result<ModelContextCapacity> {
        budget.inspect((model.provider.len() + model.name.len() + model.revision.len()) as u64 + 8)?;
        let options = model.options.as_object();
        let selected = options.and_then(|options| options.get("actors")).and_then(serde_json::Value::as_array);
        if let Some(selected) = selected {
            if selected.len() != self.actors.len() {
                return Err(budget.reject(acyclic_harness::wire::ErrorCode::Unsupported,
                    "recursive fixture model actor inventory changed"));
            }
            for value in selected {
                budget.inspect(value.as_str().map_or(1, |text| text.len() as u64 + 1))?;
            }
        }
        if model.provider != "graphcoder-deterministic" || model.name != "graphcoder-recursive-fixture"
            || model.revision != "1" || options.is_none_or(|options| options.len() != 2)
            || options.and_then(|options| options.get("dispatch_delay_ms")).and_then(serde_json::Value::as_u64)
                != Some(u64::from(self.diagnostic.delay_ms))
            || selected.is_none_or(|selected| selected.len() != 4
                || selected.iter().zip(self.actors).any(|(value, actor)|
                    value.as_str().and_then(|text| text.parse::<acyclic_harness::AgentId>().ok()) != Some(actor))) {
            return Err(budget.reject(acyclic_harness::wire::ErrorCode::Unsupported,
                "recursive fixture model selection does not match its original configuration"));
        }
        Ok(ModelContextCapacity { context_tokens: 131_072, output_tokens: 4096 })
    }

    fn role(&self, request: &PreparedModelRequest, budget: &ScenarioBudget<'_>) -> Result<ScenarioRole> {
        use acyclic_harness::model::{ModelContent, ModelContentPart, ModelRole};
        for message in request.request().messages.iter().rev() {
            budget.inspect(1)?;
            if message.role != ModelRole::User { continue; }
            let directive = match &message.content {
                ModelContent::Text(text) => Some(text.as_str()),
                ModelContent::Part(ModelContentPart::Text { text }) => Some(text.as_str()),
                ModelContent::Parts(parts) => parts.iter().find_map(|part|
                    if let ModelContentPart::Text { text } = part { Some(text.as_str()) } else { None }),
                _ => None,
            };
            if let Some(directive) = directive {
                budget.inspect(directive.len() as u64)?;
                let first = directive.lines().next().unwrap_or("");
                for role in [ScenarioRole::Root, ScenarioRole::Left, ScenarioRole::Right, ScenarioRole::Grandchild] {
                    if first == role.directive() { return Ok(role); }
                }
            }
        }
        Err(budget.reject(acyclic_harness::wire::ErrorCode::Invalid,
            "recursive fixture has no original current-caller role directive"))
    }

    fn result<'a>(&self, request: &'a PreparedModelRequest, call: &str,
        budget: &ScenarioBudget<'_>) -> Result<Option<&'a serde_json::Value>> {
        use acyclic_harness::model::{ModelContentPart, ModelRole, ToolResultContent};
        let mut result = None;
        for message in &request.request().messages {
            budget.inspect(1)?;
            if message.role != ModelRole::Tool { continue; }
            for part in message.content.parts() {
                budget.inspect(1)?;
                if let ModelContentPart::ToolResult { call_id, content, .. } = part {
                    budget.inspect(call_id.len() as u64)?;
                    if call_id == call {
                        let ToolResultContent::Json { value } = content else {
                            return Err(budget.reject(acyclic_harness::wire::ErrorCode::Invalid,
                                "recursive fixture tool returned a non-JSON projection"));
                        };
                        if result.is_some_and(|previous| previous != value) {
                            return Err(budget.reject(acyclic_harness::wire::ErrorCode::Conflict,
                                "recursive fixture retained tool result changed"));
                        }
                        result = Some(value);
                    }
                }
            }
        }
        Ok(result)
    }

    fn file<'a>(&self, result: &'a serde_json::Value, role: ScenarioRole,
        budget: &ScenarioBudget<'_>) -> Result<&'a serde_json::Value> {
        budget.inspect(4)?;
        let file = result.get("file").ok_or_else(|| budget.reject(
            acyclic_harness::wire::ErrorCode::Invalid, "recursive fixture write did not return its actual file"))?;
        if file.get("volume").and_then(|value| value.get("class")).and_then(serde_json::Value::as_str) != Some("project")
            || file.get("path").and_then(serde_json::Value::as_str) != Some(role.path()) {
            return Err(budget.reject(acyclic_harness::wire::ErrorCode::Unauthorized,
                "recursive fixture writer is not bound to its original own project"));
        }
        Ok(file)
    }

    fn advertised(&self, request: &PreparedModelRequest, name: &str, budget: &ScenarioBudget<'_>) -> Result<()> {
        for tool in &request.request().tools {
            budget.inspect(tool.name.len() as u64 + 1)?;
            if tool.name == name { return Ok(()); }
        }
        Err(budget.reject(acyclic_harness::wire::ErrorCode::Unauthorized,
            "recursive fixture required tool is not advertised by the original request"))
    }

    async fn completed(&self, index: usize, task: acyclic_harness::TaskId,
        budget: &ScenarioBudget<'_>) -> Result<std::sync::Arc<ScenarioSuccess>> {
        {
            let mut children = self.children.lock().map_err(|_| budget.reject(
                acyclic_harness::wire::ErrorCode::Storage, "recursive fixture completion lock poisoned"))?;
            let child = &mut children[index];
            if child.task.is_some_and(|original| original != task) {
                return Err(budget.reject(acyclic_harness::wire::ErrorCode::Conflict,
                    "recursive fixture original child task changed"));
            }
            child.task = Some(task);
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = budget.native {
                if child.original_work.is_none() {
                    work.admit_retained(std::mem::size_of::<NativeMaterializationWork>() as u64)?;
                    child._binding = Some(work.retain_owned(std::mem::size_of::<NativeMaterializationWork>() as u64)?);
                    child.original_work = Some(work.clone());
                }
            }
        }
        loop {
            budget.check()?;
            let notified = self.completion.notified();
            {
                let children = self.children.lock().map_err(|_| budget.reject(
                    acyclic_harness::wire::ErrorCode::Storage, "recursive fixture completion lock poisoned"))?;
                if let Some(success) = &children[index].success { return Ok(std::sync::Arc::clone(success)); }
            }
            // A bounded external-oracle response wait, not a task-host poll.
            // Timer/Notify internals are opaque Tokio allocations; the actual
            // enclosing native future frame is admitted by NativeFutureBundle.
            tokio::select! {
                () = notified => {},
                () = tokio::time::sleep(std::time::Duration::from_millis(25)) => {},
            }
        }
    }

    async fn response(&self, request: &PreparedModelRequest, dispatch: ModelDispatch,
        budget: &ScenarioBudget<'_>) -> Result<ScenarioResponse> {
        use acyclic_harness::wire::ErrorCode;
        budget.check()?;
        if request.manifest().request_digest != dispatch.request_digest {
            return Err(budget.reject(ErrorCode::Conflict, "recursive fixture dispatch request changed"));
        }
        self.capacity(&request.request().model, budget)?;
        let role = self.role(request, budget)?;
        let mut response = ScenarioResponse::new(dispatch, budget)?;
        let write = self.result(request, role.call(0), budget)?;
        if write.is_none() {
            self.advertised(request, "acyclic.write_file", budget)?;
            response.call(role.call(0), "acyclic.write_file", &ScenarioArguments::Write {
                path: role.path(), text: role.before(), media_type: "text/plain", display_name: role.path() }, budget)?;
        } else {
            let original = self.file(write.expect("branch tested the result"), role, budget)?;
            let read = self.result(request, role.call(1), budget)?;
            if read.is_none() {
                self.advertised(request, "acyclic.read_file", budget)?;
                response.call(role.call(1), "acyclic.read_file", &ScenarioArguments::Read { file: original }, budget)?;
            } else {
                if read.and_then(serde_json::Value::as_str) != Some(role.before()) {
                    return Err(budget.reject(ErrorCode::Conflict, "recursive fixture original read is not the actual written text"));
                }
                let edited = self.result(request, role.call(2), budget)?;
                if edited.is_none() {
                    self.advertised(request, "acyclic.edit_file", budget)?;
                    response.call(role.call(2), "acyclic.edit_file", &ScenarioArguments::Edit {
                        file: original, old_text: role.before(), new_text: role.after() }, budget)?;
                } else {
                    let edited = self.file(edited.expect("branch tested the result"), role, budget)?;
                    let read_after = self.result(request, role.call(3), budget)?;
                    if read_after.is_none() {
                        self.advertised(request, "acyclic.read_file", budget)?;
                        response.call(role.call(3), "acyclic.read_file", &ScenarioArguments::Read { file: edited }, budget)?;
                    } else {
                        if read_after.and_then(serde_json::Value::as_str) != Some(role.after()) {
                            return Err(budget.reject(ErrorCode::Conflict, "recursive fixture edited read is not the actual exact-edit result"));
                        }
                        let descendants: &[usize] = match role {
                            ScenarioRole::Root => &[0, 1], ScenarioRole::Left => &[2], _ => &[],
                        };
                        let mut acknowledgements = [None; 3];
                        for &index in descendants {
                            let call = fork_call(index);
                            let fork = self.result(request, call, budget)?;
                            if let Some(fork) = fork {
                                budget.inspect(4)?;
                                let expected = self.actors[index + 1];
                                if fork.get("status").and_then(serde_json::Value::as_str) != Some("admitted")
                                    || fork.get("id").and_then(serde_json::Value::as_str)
                                        .and_then(|text| text.parse::<acyclic_harness::AgentId>().ok()) != Some(expected) {
                                    return Err(budget.reject(ErrorCode::Conflict, "recursive fixture fork lacks its actual new-agent admission"));
                                }
                                acknowledgements[index] = Some(fork.get("task").and_then(serde_json::Value::as_str)
                                    .and_then(|text| text.parse::<acyclic_harness::TaskId>().ok())
                                    .ok_or_else(|| budget.reject(ErrorCode::Invalid,
                                        "recursive fixture fork did not return its actual admitted child task"))?);
                            } else {
                                self.advertised(request, "acyclic.fork", budget)?;
                                response.call(call, "acyclic.fork", &ScenarioArguments::Fork {
                                    id: expected_actor(self.actors, index), msg: child_role(index).directive() }, budget)?;
                            }
                        }
                        if response.count == 0 {
                            let mut succeeded: [Option<std::sync::Arc<ScenarioSuccess>>; 3] = std::array::from_fn(|_| None);
                            for &index in descendants {
                                let task = acknowledgements[index].expect("all original fork acknowledgements were validated");
                                succeeded[index] = Some(self.completed(index, task, budget).await?);
                                let merged = self.result(request, merge_call(index), budget)?;
                                if let Some(merged) = merged {
                                    if merged.get("status").and_then(serde_json::Value::as_str) != Some("published")
                                        || merged.get("receipt").is_none() {
                                        return Err(budget.reject(ErrorCode::Conflict,
                                            "recursive fixture merge did not publish actual source-project changes"));
                                    }
                                } else {
                                    self.advertised(request, "acyclic.merge", budget)?;
                                    response.call(merge_call(index), "acyclic.merge", &ScenarioArguments::Merge { source: task }, budget)?;
                                }
                            }
                            if response.count == 0 {
                                let mut content = [0_u8; 1024];
                                let mut writer = std::io::Cursor::new(content.as_mut_slice());
                                write!(writer, "{} own-project-read={} ", role.directive(), role.after().trim_end())
                                    .map_err(|_| budget.reject(ErrorCode::Invalid, "recursive fixture completion exceeds its stack bound"))?;
                                for success in succeeded.iter().flatten() {
                                    budget.hash(success.actual_result.len() as u64)?;
                                    write!(writer, "child={} actual-result={} ", success.task, blake3::hash(success.actual_result.as_bytes()).to_hex())
                                        .map_err(|_| budget.reject(ErrorCode::Invalid, "recursive fixture completion exceeds its stack bound"))?;
                                }
                                let length = writer.position() as usize;
                                let content = std::str::from_utf8(&content[..length]).expect("static text and UUID/hash formatting emit UTF-8");
                                response.content(content, budget)?;
                            }
                        }
                    }
                }
            }
        }
        response.complete(budget)?;
        Ok(response)
    }
}

fn fork_call(index: usize) -> &'static str {
    ["graphcoder-fork-left", "graphcoder-fork-right", "graphcoder-fork-grandchild"][index]
}
fn merge_call(index: usize) -> &'static str {
    ["graphcoder-merge-left", "graphcoder-merge-right", "graphcoder-merge-grandchild"][index]
}
fn child_role(index: usize) -> ScenarioRole {
    [ScenarioRole::Left, ScenarioRole::Right, ScenarioRole::Grandchild][index]
}
fn expected_actor(actors: [acyclic_harness::AgentId; 4], index: usize) -> acyclic_harness::AgentId { actors[index + 1] }

#[derive(Serialize)]
#[serde(untagged)]
enum ScenarioArguments<'a> {
    Write { path: &'a str, text: &'a str, media_type: &'a str, display_name: &'a str },
    Read { file: &'a serde_json::Value },
    Edit { file: &'a serde_json::Value, old_text: &'a str, new_text: &'a str },
    Fork { id: acyclic_harness::AgentId, msg: &'a str },
    Merge { source: acyclic_harness::TaskId },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ScenarioWireEvent<'a> {
    ToolCall { call_id: &'a str, name: &'a str, arguments: &'a ScenarioArguments<'a> },
    Content { delta: &'a str },
    Completed { metadata: ScenarioUsage },
}
#[derive(Serialize)]
struct ScenarioUsage { provider: &'static str, output_tokens: u32 }

struct ScenarioBudget<'a> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native: Option<&'a NativeMaterializationWork>,
    _lifetime: std::marker::PhantomData<&'a ()>,
}
impl ScenarioBudget<'_> {
    fn ordinary() -> Self {
        Self {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native: None,
            _lifetime: std::marker::PhantomData,
        }
    }
    fn check(&self) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native { work.remaining()?; }
        Ok(())
    }
    fn inspect(&self, items: u64) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native { work.charge(WorkCounters { items_examined: items, ..Default::default() })?; }
        let _ = items;
        Ok(())
    }
    fn encode(&self, bytes: u64) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native { work.charge(WorkCounters { bytes_encoded: bytes, items_examined: 1, ..Default::default() })?; }
        let _ = bytes;
        Ok(())
    }
    fn hash(&self, bytes: u64) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native { work.charge(WorkCounters { bytes_hashed: bytes, ..Default::default() })?; }
        let _ = bytes;
        Ok(())
    }
    fn reject(&self, code: acyclic_harness::wire::ErrorCode, reason: &'static str) -> Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native.is_some() { return Error::NativeBoundary { code, reason }; }
        match code {
            acyclic_harness::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
            acyclic_harness::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
            acyclic_harness::wire::ErrorCode::Storage => Error::Storage(reason.into()),
            acyclic_harness::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
            _ => Error::Invalid(reason.into()),
        }
    }
    fn io(&self, error: std::io::Error) -> Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native.is_some() { return acyclic_fs::NativeTreeMaterializationError::Io(error).into(); }
        Error::Storage(error.to_string())
    }
}

struct ScenarioResponse { bytes: [u8; SCENARIO_RECEIPT_BYTES], length: usize, count: usize }
impl ScenarioResponse {
    fn new(dispatch: ModelDispatch, budget: &ScenarioBudget<'_>) -> Result<Self> {
        budget.encode(SCENARIO_HEADER as u64)?;
        let mut response = Self { bytes: [0; SCENARIO_RECEIPT_BYTES], length: SCENARIO_HEADER, count: 0 };
        response.bytes[..6].copy_from_slice(SCENARIO_MAGIC);
        response.bytes[6..22].copy_from_slice(&dispatch.operation_id.into_bytes());
        response.bytes[22..26].copy_from_slice(&dispatch.step.to_le_bytes());
        response.bytes[26..58].copy_from_slice(&dispatch.request_digest);
        Ok(response)
    }
    fn event(&mut self, event: &ScenarioWireEvent<'_>, budget: &ScenarioBudget<'_>) -> Result<()> {
        if self.count >= SCENARIO_EVENTS || self.length + 4 >= self.bytes.len() {
            return Err(budget.reject(acyclic_harness::wire::ErrorCode::Invalid, "recursive fixture response exceeds its finite bound"));
        }
        budget.encode(4)?;
        let start = self.length + 4;
        let mut writer = ScenarioWriter { bytes: &mut self.bytes[start..], length: 0, budget, cause: None };
        let result = serde_json::to_writer(&mut writer, event);
        if let Some(cause) = writer.cause.take() { return Err(cause); }
        if let Err(source) = result {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if budget.native.is_some() {
                return Err(Error::NativeEncode { code: acyclic_harness::wire::ErrorCode::Invalid, source, custody: None });
            }
            return Err(Error::Invalid(source.to_string()));
        }
        let length = writer.length;
        self.bytes[self.length..start].copy_from_slice(&(length as u32).to_le_bytes());
        self.length = start + length;
        self.count += 1;
        self.bytes[58] = self.count as u8;
        Ok(())
    }
    fn call(&mut self, call_id: &str, name: &str, arguments: &ScenarioArguments<'_>, budget: &ScenarioBudget<'_>) -> Result<()> {
        self.event(&ScenarioWireEvent::ToolCall { call_id, name, arguments }, budget)
    }
    fn content(&mut self, delta: &str, budget: &ScenarioBudget<'_>) -> Result<()> {
        self.event(&ScenarioWireEvent::Content { delta }, budget)
    }
    fn complete(&mut self, budget: &ScenarioBudget<'_>) -> Result<()> {
        self.event(&ScenarioWireEvent::Completed { metadata: ScenarioUsage {
            provider: "graphcoder-recursive-fixture", output_tokens: 128 } }, budget)
    }
    fn verify(&self, dispatch: ModelDispatch, budget: &ScenarioBudget<'_>) -> Result<()> {
        budget.inspect(self.length as u64)?;
        if self.length < SCENARIO_HEADER || &self.bytes[..6] != SCENARIO_MAGIC
            || self.bytes[6..22] != dispatch.operation_id.into_bytes()
            || self.bytes[22..26] != dispatch.step.to_le_bytes()
            || self.bytes[26..58] != dispatch.request_digest
            || self.count == 0 || self.count > SCENARIO_EVENTS || self.bytes[58] as usize != self.count {
            return Err(budget.reject(acyclic_harness::wire::ErrorCode::Storage, "recursive fixture response receipt identity is invalid"));
        }
        let mut end = SCENARIO_HEADER;
        for _ in 0..self.count {
            let length = self.frame_length(end, budget)?;
            end += 4 + length;
        }
        if end != self.length {
            return Err(budget.reject(acyclic_harness::wire::ErrorCode::Storage, "recursive fixture response receipt has trailing bytes"));
        }
        Ok(())
    }
    fn frame_length(&self, start: usize, budget: &ScenarioBudget<'_>) -> Result<usize> {
        let prefix = self.bytes.get(start..start + 4).filter(|_| start + 4 <= self.length)
            .ok_or_else(|| budget.reject(acyclic_harness::wire::ErrorCode::Storage, "recursive fixture response receipt is truncated"))?;
        let length = u32::from_le_bytes(prefix.try_into().expect("four-byte bounded frame length")) as usize;
        if length == 0 || length > self.length - start - 4 {
            return Err(budget.reject(acyclic_harness::wire::ErrorCode::Storage, "recursive fixture response receipt frame is invalid"));
        }
        Ok(length)
    }
    fn frame(&self, index: usize, budget: &ScenarioBudget<'_>) -> Result<&[u8]> {
        let mut start = SCENARIO_HEADER;
        for current in 0..self.count {
            let length = self.frame_length(start, budget)?;
            if current == index { return Ok(&self.bytes[start + 4..start + 4 + length]); }
            start += 4 + length;
        }
        Err(budget.reject(acyclic_harness::wire::ErrorCode::Storage, "recursive fixture response event index is invalid"))
    }
}

struct ScenarioWriter<'a, 'budget> {
    bytes: &'a mut [u8],
    length: usize,
    budget: &'a ScenarioBudget<'budget>,
    cause: Option<Error>,
}
impl std::io::Write for ScenarioWriter<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let result = self.budget.encode(bytes.len() as u64).and_then(|()| {
            if bytes.len() > self.bytes.len() - self.length {
                return Err(self.budget.reject(acyclic_harness::wire::ErrorCode::Invalid,
                    "recursive fixture response exceeds its byte bound"));
            }
            Ok(())
        });
        if let Err(cause) = result {
            if self.cause.is_none() { self.cause = Some(cause); }
            return Err(std::io::ErrorKind::Other.into());
        }
        self.bytes[self.length..self.length + bytes.len()].copy_from_slice(bytes);
        self.length += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

struct ScenarioReceiptPath {
    value: std::path::PathBuf,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _custody: Option<NativeAllocation>,
}

impl NativeScenarioOracle {
    /// Published only after the actual response receipt has been synced.
    pub fn pending_restart_receipt(&self) -> Result<Option<std::path::PathBuf>> {
        Ok(self.last_receipt.lock().map_err(|_| Error::Storage("recursive fixture receipt lock poisoned".into()))?
            .as_ref().map(|path| path.value.clone()))
    }

    fn receipt_path(&self, dispatch: ModelDispatch, budget: &ScenarioBudget<'_>) -> Result<std::sync::Arc<ScenarioReceiptPath>> {
        let mut name = [0_u8; 128];
        let mut digest = [0_u8; 64];
        hex::encode_to_slice(dispatch.request_digest, &mut digest).expect("exact digest hex capacity");
        let digest = std::str::from_utf8(&digest).expect("hex ASCII");
        let mut writer = std::io::Cursor::new(name.as_mut_slice());
        write!(writer, "{}-{}-{digest}.response", dispatch.operation_id, dispatch.step)
            .map_err(|_| budget.reject(acyclic_harness::wire::ErrorCode::Invalid, "recursive fixture response name exceeds its stack bound"))?;
        let length = writer.position() as usize;
        let name = std::str::from_utf8(&name[..length]).expect("UUID, decimal and hex ASCII");
        let capacity = self.receipts.as_os_str().len().checked_add(length + 1)
            .ok_or_else(|| budget.reject(acyclic_harness::wire::ErrorCode::Invalid, "recursive fixture receipt path overflows"))?;
        budget.inspect(capacity as u64)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut custody = if let Some(work) = budget.native {
            let mut custody = work.claim(std::mem::size_of::<ScenarioReceiptPath>() as u64)?;
            custody.grow(capacity as u64)?;
            work.charge(WorkCounters { bytes_copied: capacity as u64, ..Default::default() })?;
            Some(custody)
        } else { None };
        let mut path = std::path::PathBuf::with_capacity(capacity);
        path.push(&self.receipts);
        path.push(name);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(custody) = &mut custody {
            custody.resize_retained((std::mem::size_of::<ScenarioReceiptPath>() + path.capacity()) as u64)?;
        }
        budget.check()?;
        Ok(std::sync::Arc::new(ScenarioReceiptPath { value: path,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _custody: custody }))
    }

    fn load_response(&self, dispatch: ModelDispatch, budget: &ScenarioBudget<'_>) -> Result<Option<ScenarioResponse>> {
        let path = self.receipt_path(dispatch, budget)?;
        budget.read_operation()?;
        let mut file = match File::open(&path.value) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => { budget.check()?; return Ok(None); }
            Err(error) => return Err(budget.io(error)),
        };
        budget.read_operation()?;
        let length = file.metadata().map_err(|error| budget.io(error))?.len();
        if length < SCENARIO_HEADER as u64 || length > SCENARIO_RECEIPT_BYTES as u64 {
            return Err(budget.reject(acyclic_harness::wire::ErrorCode::Storage, "recursive fixture response receipt size is invalid"));
        }
        let mut response = ScenarioResponse { bytes: [0; SCENARIO_RECEIPT_BYTES], length: 0, count: 0 };
        while response.length < length as usize {
            let requested = length as usize - response.length;
            budget.admit_read(requested as u64)?;
            budget.read_operation()?;
            let count = file.read(&mut response.bytes[response.length..length as usize]).map_err(|error| budget.io(error))?;
            budget.read_completed(count as u64)?;
            if count == 0 {
                return Err(budget.reject(acyclic_harness::wire::ErrorCode::Storage, "recursive fixture response receipt was truncated while reading"));
            }
            response.length += count;
            budget.check()?;
        }
        response.count = response.bytes[58] as usize;
        response.verify(dispatch, budget)?;
        Ok(Some(response))
    }

    fn persist_response(&self, response: &ScenarioResponse, dispatch: ModelDispatch,
        budget: &ScenarioBudget<'_>) -> Result<()> {
        response.verify(dispatch, budget)?;
        let path = self.receipt_path(dispatch, budget)?;
        let mut count = self.receipt_count.lock().map_err(|_| budget.reject(
            acyclic_harness::wire::ErrorCode::Storage, "recursive fixture receipt counter lock poisoned"))?;
        budget.read_operation()?;
        match File::open(&path.value) {
            Ok(_) => {
                let existing = self.load_response(dispatch, budget)?.ok_or_else(|| budget.reject(
                    acyclic_harness::wire::ErrorCode::Storage, "recursive fixture response receipt disappeared"))?;
                if existing.bytes[..existing.length] != response.bytes[..response.length] {
                    return Err(budget.reject(acyclic_harness::wire::ErrorCode::Conflict,
                        "recursive fixture original generated response changed"));
                }
                *self.last_receipt.lock().map_err(|_| budget.reject(acyclic_harness::wire::ErrorCode::Storage,
                    "recursive fixture receipt lock poisoned"))? = Some(path);
                return Ok(());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => budget.check()?,
            Err(error) => return Err(budget.io(error)),
        }
        if *count >= SCENARIO_RECEIPTS {
            return Err(budget.reject(acyclic_harness::wire::ErrorCode::Invalid, "recursive fixture actual response receipt count exhausted"));
        }
        budget.write_operation()?;
        let mut file = std::fs::OpenOptions::new().write(true).create_new(true).open(&path.value)
            .map_err(|error| budget.io(error))?;
        *count += 1;
        let mut written = 0;
        while written < response.length {
            budget.admit_write((response.length - written) as u64)?;
            budget.write_operation()?;
            let actual = file.write(&response.bytes[written..response.length]).map_err(|error| budget.io(error))?;
            budget.write_completed(actual as u64)?;
            if actual == 0 {
                return Err(budget.reject(acyclic_harness::wire::ErrorCode::Storage, "recursive fixture response receipt write made no progress"));
            }
            written += actual;
            budget.check()?;
        }
        budget.durability_operation()?;
        file.sync_all().map_err(|error| budget.io(error))?;
        budget.check()?;
        *self.last_receipt.lock().map_err(|_| budget.reject(acyclic_harness::wire::ErrorCode::Storage,
            "recursive fixture receipt lock poisoned"))? = Some(path);
        Ok(())
    }

    async fn generate_response(&self, request: &PreparedModelRequest, dispatch: ModelDispatch,
        budget: &ScenarioBudget<'_>) -> Result<ScenarioResponse> {
        budget.check()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = budget.native { self.diagnostic.observe_invocation_native("generate", dispatch, work)?; }
        else { self.diagnostic.observe_invocation("generate", dispatch)?; }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        self.diagnostic.observe_invocation("generate", dispatch)?;
        let response = self.response(request, dispatch, budget).await?;
        self.persist_response(&response, dispatch, budget)?;
        // The independent terminal can kill the process during this real,
        // configured provider delay only after the actual durable response.
        budget.check()?;
        tokio::time::sleep(std::time::Duration::from_millis(u64::from(self.diagnostic.delay_ms))).await;
        budget.check()?;
        Ok(response)
    }

    fn reconcile_response(&self, dispatch: ModelDispatch, budget: &ScenarioBudget<'_>) -> Result<Option<ScenarioResponse>> {
        budget.check()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = budget.native { self.diagnostic.observe_invocation_native("reconcile", dispatch, work)?; }
        else { self.diagnostic.observe_invocation("reconcile", dispatch)?; }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        self.diagnostic.observe_invocation("reconcile", dispatch)?;
        self.load_response(dispatch, budget)
    }
}

impl ScenarioBudget<'_> {
    fn read_operation(&self) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native { work.charge(WorkCounters { backend_read_operations: 1, ..Default::default() })?; }
        Ok(())
    }
    fn write_operation(&self) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native { work.charge(WorkCounters { backend_write_operations: 1, ..Default::default() })?; }
        Ok(())
    }
    fn durability_operation(&self) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native { work.charge(WorkCounters { durability_operations: 1, ..Default::default() })?; }
        Ok(())
    }
    fn admit_read(&self, bytes: u64) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native { work.admit(WorkCounters { object_bytes_read: bytes, ..Default::default() })?; }
        let _ = bytes;
        Ok(())
    }
    fn read_completed(&self, bytes: u64) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native { work.spent(WorkCounters { object_bytes_read: bytes, ..Default::default() })?; }
        let _ = bytes;
        Ok(())
    }
    fn admit_write(&self, bytes: u64) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native { work.admit(WorkCounters { object_bytes_written: bytes, ..Default::default() })?; }
        let _ = bytes;
        Ok(())
    }
    fn write_completed(&self, bytes: u64) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native { work.spent(WorkCounters { object_bytes_written: bytes, ..Default::default() })?; }
        let _ = bytes;
        Ok(())
    }
}

impl ScenarioResponse {
    fn ordinary_suffix(&self, observed: &[ModelEvent], budget: &ScenarioBudget<'_>) -> Result<Vec<ModelEvent>> {
        if observed.len() > self.count {
            return Err(budget.reject(acyclic_harness::wire::ErrorCode::Conflict, "recursive fixture observed prefix exceeds its actual response"));
        }
        let mut suffix = Vec::new();
        suffix.try_reserve_exact(self.count - observed.len()).map_err(Error::Allocation)?;
        for index in 0..self.count {
            let event: ModelEvent = serde_json::from_slice(self.frame(index, budget)?)
                .map_err(|error| Error::Storage(error.to_string()))?;
            if let Some(previous) = observed.get(index) {
                if previous != &event {
                    return Err(budget.reject(acyclic_harness::wire::ErrorCode::Conflict,
                        "recursive fixture retained model prefix differs from its actual response"));
                }
            } else { suffix.push(event); }
        }
        Ok(suffix)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_suffix(&self, observed: &[ModelEvent], work: &NativeMaterializationWork)
        -> Result<(Vec<ModelEvent>, NativeAllocation)> {
        let budget = ScenarioBudget { native: Some(work), _lifetime: std::marker::PhantomData };
        work.remaining()?;
        if observed.len() > self.count {
            return Err(budget.reject(acyclic_harness::wire::ErrorCode::Conflict, "recursive fixture observed prefix exceeds its actual response"));
        }
        let count = self.count - observed.len();
        let mut custody = if count == 0 { work.retain_owned(0)? }
            else { work.claim((count * std::mem::size_of::<ModelEvent>()) as u64)? };
        let mut suffix = Vec::new();
        suffix.try_reserve_exact(count).map_err(Error::Allocation)?;
        custody.resize_retained((suffix.capacity() * std::mem::size_of::<ModelEvent>()) as u64)?;
        work.remaining()?;
        for index in 0..self.count {
            let mut produced = ModelEvent::decode_with_native_work(self.frame(index, &budget)?, work)?;
            if let Some(previous) = observed.get(index) {
                work.charge(WorkCounters { items_examined: 1, ..Default::default() })?;
                if previous != &produced.0 {
                    return Err(budget.reject(acyclic_harness::wire::ErrorCode::Conflict,
                        "recursive fixture retained model prefix differs from its actual response"));
                }
            } else {
                custody.absorb(&mut produced.1)?;
                suffix.push(produced.0);
            }
        }
        work.remaining()?;
        Ok((suffix, custody))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_stream<'a>(&self, work: &'a NativeMaterializationWork) -> Result<NativeModelStream<'a>> {
        let budget = ScenarioBudget { native: Some(work), _lifetime: std::marker::PhantomData };
        let mut events: [Option<(ModelEvent, NativeAllocation)>; SCENARIO_EVENTS] = std::array::from_fn(|_| None);
        for (index, event) in events.iter_mut().enumerate().take(self.count) {
            *event = Some(ModelEvent::decode_with_native_work(self.frame(index, &budget)?, work)?);
        }
        NativeModelStream::new(work, || futures::stream::iter(events.into_iter().flatten().map(Ok)))
    }
}

impl ModelProvider for NativeScenarioOracle {
    fn context_capacity(&self, model: &Model) -> Result<ModelContextCapacity> {
        self.capacity(model, &ScenarioBudget::ordinary())
    }
    fn count_tokens(&self, request: &PreparedModelRequest) -> Result<ModelTokenCount> {
        self.context_capacity(&request.request().model)?;
        let message_tokens = request.request().messages.iter().map(|message| {
            let mut serialized = ByteCount::default();
            serde_json::to_writer(&mut serialized, message).map_err(|error| Error::Invalid(error.to_string()))?;
            let bytes = message.content.file_refs().iter().try_fold(serialized.0 as u64, |total, file|
                total.checked_add(file.descriptor().byte_length()).ok_or_else(|| Error::Invalid("recursive fixture token count overflows".into())))?;
            u32::try_from(bytes).map_err(|_| Error::Invalid("recursive fixture token count exceeds u32".into()))
        }).collect::<Result<Vec<_>>>()?;
        Ok(ModelTokenCount { request_digest: request.manifest().request_digest,
            fixed_tokens: u32::try_from(request.bytes().len()).map_err(|_| Error::Invalid("recursive fixture framing exceeds u32".into()))?,
            message_tokens })
    }
    fn generate<'a>(&'a self, request: PreparedModelRequest, dispatch: ModelDispatch)
        -> BoxProviderStream<'a, Result<ModelEvent>> {
        Box::pin(futures::stream::once(async move {
            let budget = ScenarioBudget::ordinary();
            self.generate_response(&request, dispatch, &budget).await?.ordinary_suffix(&[], &budget)
        }).flat_map(|result| match result {
            Ok(events) => futures::future::Either::Left(futures::stream::iter(events.into_iter().map(Ok))),
            Err(error) => futures::future::Either::Right(futures::stream::iter([Err(error)])),
        }))
    }
    fn reconcile<'a>(&'a self, attempt: ModelAttempt) -> BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async move {
            let budget = ScenarioBudget::ordinary();
            self.reconcile_response(attempt.dispatch(), &budget)?
                .map(|response| response.ordinary_suffix(&attempt.observed, &budget)).transpose()
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_model(&self, work: &NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn context_capacity_with_native_work(&self, model: &Model, work: &NativeMaterializationWork) -> Result<ModelContextCapacity> {
        self.capacity(model, &ScenarioBudget { native: Some(work), _lifetime: std::marker::PhantomData })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn count_tokens_with_native_work(&self, request: &PreparedModelRequest, work: &NativeMaterializationWork)
        -> Result<(ModelTokenCount, NativeAllocation)> {
        self.context_capacity_with_native_work(&request.request().model, work)?;
        request.serialized_token_upper_bound_with_native_work(work)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn generate_with_native_work<'a>(&'a self, request: &'a PreparedModelRequest, dispatch: ModelDispatch,
        work: &'a NativeMaterializationWork) -> Result<NativeFutureBundle<'a, NativeModelStream<'a>>> {
        work.remaining()?;
        self.context_capacity_with_native_work(&request.request().model, work)?;
        if request.manifest().request_digest != dispatch.request_digest {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "recursive fixture dispatch request changed" });
        }
        NativeFutureBundle::new(work, || async move {
            let budget = ScenarioBudget { native: Some(work), _lifetime: std::marker::PhantomData };
            self.generate_response(request, dispatch, &budget).await?.native_stream(work)
        })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_work<'a>(&'a self, attempt: NativeModelAttempt<'a>, work: &'a NativeMaterializationWork)
        -> Result<NativeFutureBundle<'a, (Option<Vec<ModelEvent>>, NativeAllocation)>> {
        NativeFutureBundle::new(work, || async move {
            let budget = ScenarioBudget { native: Some(work), _lifetime: std::marker::PhantomData };
            let dispatch = ModelDispatch { operation_id: attempt.operation_id, step: attempt.step,
                request_digest: attempt.request_digest };
            if let Some(response) = self.reconcile_response(dispatch, &budget)? {
                let (events, custody) = response.native_suffix(attempt.observed, work)?;
                Ok((Some(events), custody))
            } else { Ok((None, work.retain_owned(0)?)) }
        })
    }
}

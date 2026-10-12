//! Explicit credential-free functional provider for installed runtime qualification.

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
use acyclic_harness::{NativeFutureBundle, model::NativeModelStream};

const MAX_LEDGER_BYTES: u64 = 1024 * 1024;
const MAX_LEDGER_RECORDS: u64 = 128;

/// Independent finite diagnostic evidence of actual provider entry, never task state, a model result cache, an effect receipt or replay input.
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
        // The complete bounded diagnostic record is built on the stack.
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
        &'a self, dispatch: ModelDispatch, observed: &'a [ModelEvent], work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (Option<Vec<ModelEvent>>, NativeAllocation)>> {
        NativeFutureBundle::new(work, || async move {
            self.observe_invocation_native("reconcile", dispatch, work)?;
            let complete = native_events(dispatch, work)?;
            work.charge(WorkCounters { items_examined: observed.len() as u64, ..Default::default() })?;
            if observed.len() > complete.len()
                || !observed.iter().zip(&complete).all(|(observed, item)| observed == &item.0)
            {
                return Err(Error::Conflict("deterministic provider retained prefix changed".into()));
            }
            let capacity = complete.len() - observed.len();
            let bytes = (capacity * std::mem::size_of::<ModelEvent>()) as u64;
            let mut custody = if bytes == 0 { work.retain_owned(0)? } else { work.claim(bytes)? };
            let mut suffix = Vec::with_capacity(capacity);
            for mut item in complete.into_iter().skip(observed.len()) {
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
            let bytes = message.content.file_refs().try_fold(
                serialized.0 as u64,
                |total, file| total.checked_add(file.descriptor().byte_length())
                    .ok_or_else(|| Error::Invalid("deterministic token count overflows".into())),
            )?;
            u32::try_from(bytes).map_err(|_| Error::Invalid("deterministic token count exceeds u32".into()))
        }).collect::<Result<Vec<_>>>()?;
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

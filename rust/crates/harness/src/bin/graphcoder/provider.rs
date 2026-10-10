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
    fn context_capacity(&self, selection: &Model) -> Result<ModelContextCapacity> {
        if selection.provider != "graphcoder-deterministic" || selection.name != "request-digest"
            || selection.revision != "1"
            || !selection.options.as_object().is_some_and(|options|
                options.len() == 1 && options.get("dispatch_delay_ms").and_then(serde_json::Value::as_u64) == Some(u64::from(self.delay_ms)))
        {
            return Err(Error::Unsupported("native terminal has no adapter for this model selection".into()));
        }
        Ok(ModelContextCapacity { context_tokens: 131_072, output_tokens: 4096 })
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

//! Explicit credential-free functional provider for installed runtime qualification.

use acyclic_harness::{
    Error, Result,
    context::{ModelContextCapacity, ModelTokenCount},
    model::{Model, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use futures::TryStreamExt as _;
use serde_json::json;
use std::{
    fs::File,
    io::{Read as _, Write as _},
    path::Path,
    sync::Mutex,
};

const MAX_LEDGER_BYTES: u64 = 1024 * 1024;
const MAX_LEDGER_RECORDS: u64 = 128;

struct InvocationLedger {
    file: File,
    bytes: u64,
    records: u64,
}

pub struct Deterministic {
    delay_ms: u32,
    ledger: Mutex<InvocationLedger>,
}

impl Deterministic {
    pub fn new(delay_ms: u32, path: &Path) -> Result<Self> {
        if !path.is_absolute()
            || path
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
        {
            return Err(Error::Invalid(
                "model invocation observer requires an absolute original path".into(),
            ));
        }
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
            .map_err(|error| Error::Storage(format!("model invocation observer open: {error}")))?;
        let mut contents = Vec::new();
        (&mut file)
            .take(MAX_LEDGER_BYTES + 1)
            .read_to_end(&mut contents)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let bytes = contents.len() as u64;
        let records = contents.iter().filter(|byte| **byte == b'\n').count() as u64;
        if bytes > MAX_LEDGER_BYTES || records > MAX_LEDGER_RECORDS {
            return Err(Error::Invalid(
                "model invocation observer exceeds finite bounds".into(),
            ));
        }
        if contents.last().is_some_and(|byte| *byte != b'\n') {
            return Err(Error::Storage(
                "model invocation observer has an interrupted record".into(),
            ));
        }
        Ok(Self {
            delay_ms,
            ledger: Mutex::new(InvocationLedger {
                file,
                bytes,
                records,
            }),
        })
    }

    fn observe_invocation(&self, phase: &'static str, dispatch: ModelDispatch) -> Result<()> {
        let failure = |reason: &str| Error::Invalid(reason.into());
        let digest = hex::encode(dispatch.request_digest);
        let record = format!(
            "{{\"format_version\":1,\"phase\":\"{phase}\",\"operation_id\":\"{}\",\"step\":{},\"request_digest\":\"{digest}\"}}\n",
            dispatch.operation_id, dispatch.step
        );
        let bytes = record.as_bytes();
        let mut ledger = self
            .ledger
            .lock()
            .map_err(|_| Error::Storage("model invocation observer lock poisoned".into()))?;
        // Both counters start bounded and only advance after this bound check.
        let next_bytes = ledger.bytes + bytes.len() as u64;
        if ledger.records >= MAX_LEDGER_RECORDS || next_bytes > MAX_LEDGER_BYTES {
            return Err(failure(
                "model invocation observer finite allowance exhausted",
            ));
        }
        ledger
            .file
            .write_all(bytes)
            .and_then(|()| ledger.file.sync_all())
            .map_err(|error| {
                Error::Storage(format!("model invocation observer append: {error}"))
            })?;
        ledger.bytes = next_bytes;
        ledger.records += 1;
        drop(ledger);
        if phase == "generate" && self.delay_ms > 0 {
            eprintln!(
                "graphcoder deterministic-dispatch operation={} step={} request={digest}",
                dispatch.operation_id, dispatch.step
            );
        }
        Ok(())
    }
}

pub fn model(delay_ms: u32) -> Model {
    Model {
        provider: "graphcoder-deterministic".into(),
        name: "request-digest".into(),
        revision: "1".into(),
        options: json!({"dispatch_delay_ms":delay_ms}),
    }
}

fn events(dispatch: ModelDispatch) -> [ModelEvent; 2] {
    [
        ModelEvent::Content {
            delta: format!(
                "deterministic request={} step={}",
                hex::encode(dispatch.request_digest),
                dispatch.step
            ),
        },
        ModelEvent::Completed {
            metadata: json!({"provider":"graphcoder-deterministic","output_tokens":128}),
        },
    ]
}

impl ModelProvider for Deterministic {
    fn context_capacity(&self, selection: &Model) -> Result<ModelContextCapacity> {
        if selection.provider != "graphcoder-deterministic"
            || selection.name != "request-digest"
            || selection.revision != "1"
            || !selection.options.as_object().is_some_and(|options| {
                options.len() == 1
                    && options
                        .get("dispatch_delay_ms")
                        .and_then(serde_json::Value::as_u64)
                        == Some(u64::from(self.delay_ms))
            })
        {
            return Err(Error::Unsupported(
                "native terminal has no adapter for this model selection".into(),
            ));
        }
        Ok(ModelContextCapacity {
            context_tokens: 131_072,
            output_tokens: 4096,
        })
    }

    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<ModelTokenCount>> {
        Box::pin(async move {
            self.context_capacity(&request.request().model)?;
            request
                .serialized_token_upper_bound(None)
                .map(|(count, _)| count)
        })
    }

    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxProviderStream<'a, Result<ModelEvent>> {
        Box::pin(
            futures::stream::once(async move {
                if request.request_digest() != dispatch.request_digest {
                    return Err(Error::Conflict("model dispatch request changed".into()));
                }
                self.context_capacity(&request.request().model)?;
                self.observe_invocation("generate", dispatch)?;
                tokio::time::sleep(std::time::Duration::from_millis(u64::from(self.delay_ms)))
                    .await;
                Ok(futures::stream::iter(
                    events(dispatch).into_iter().map(Ok::<_, Error>),
                ))
            })
            .try_flatten(),
        )
    }

    fn reconcile<'a>(
        &'a self,
        dispatch: ModelDispatch,
        observed: &'a [ModelEvent],
    ) -> BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async move {
            self.observe_invocation("reconcile", dispatch)?;
            let complete = events(dispatch);
            if !complete.starts_with(observed) {
                return Err(Error::Conflict(
                    "deterministic provider retained prefix changed".into(),
                ));
            }
            Ok(Some(complete.into_iter().skip(observed.len()).collect()))
        })
    }
}

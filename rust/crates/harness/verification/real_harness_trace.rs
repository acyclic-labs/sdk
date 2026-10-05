//! Test-only exporter for one real durable local swarm execution.
//!
//! This module is included below `filesystem::swarm_local::tests`, so it can
//! inspect the private registry and authenticated child journal without
//! widening the production API. It deliberately emits a small normalized
//! projection consumed by `check-trace.ps1`; the sidecar records the source
//! stream sequences and operation identities used to derive every event.

use super::super::*;
use crate::{
    Error, IdempotencyKey, OperationId, Result,
    executor::ExecutionEvent,
    model::{
        Model, ModelAttempt, ModelEvent, ModelOptionPolicy, ModelProvider, ModelRequest,
        ProviderDispatchContext,
    },
    model_input::PreparedModelInput,
    swarm_budget::{SwarmUsage, SwarmUsageSource},
};
use acyclic_stream::{LocalStream, StreamError};
use futures::{StreamExt as _, future::BoxFuture, stream::BoxStream};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tempfile::tempdir;
use tokio::sync::Notify;

struct TraceModel {
    child_operation: OperationId,
    child_release: Arc<Notify>,
    calls: AtomicUsize,
    usage: Arc<TraceUsage>,
}

#[derive(Default)]
struct TraceUsage {
    usage: Mutex<BTreeMap<(OperationId, String), SwarmUsage>>,
}

impl TraceUsage {
    fn begin(&self, dispatch: &ProviderDispatchContext) {
        if let Ok(mut usage) = self.usage.lock() {
            usage
                .entry((dispatch.operation_id, dispatch.dispatch_id.0.clone()))
                .or_default()
                .model_steps = 1;
        }
    }

    fn record(&self, dispatch: &ProviderDispatchContext, started: Instant, event: &ModelEvent) {
        let bytes = crate::contract::canonical_json_bytes(event)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or_default();
        if let Ok(mut usage) = self.usage.lock() {
            let entry = usage
                .entry((dispatch.operation_id, dispatch.dispatch_id.0.clone()))
                .or_default();
            entry.output_bytes = entry.output_bytes.saturating_add(bytes);
            entry.execution_time_ms = entry
                .execution_time_ms
                .max(started.elapsed().as_millis() as u64);
        }
    }
}

impl SwarmUsageSource for TraceUsage {
    fn provider_identity(&self) -> &str {
        "harness.verification.real-trace"
    }

    fn cumulative_usage(
        &self,
        operation_id: OperationId,
        dispatch_id: &IdempotencyKey,
    ) -> Result<SwarmUsage> {
        self.usage
            .lock()
            .map_err(|_| Error::Storage("trace usage lock poisoned".into()))
            .and_then(|usage| {
                usage
                    .get(&(operation_id, dispatch_id.0.clone()))
                    .copied()
                    .ok_or_else(|| Error::Indeterminate(operation_id))
            })
    }
}

impl ModelProvider for TraceModel {
    fn supports_dispatch_context(&self) -> bool {
        true
    }

    fn swarm_usage_source(&self) -> Option<Arc<dyn SwarmUsageSource>> {
        Some(self.usage.clone())
    }

    fn generate<'a>(&'a self, _prepared: PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            return Box::pin(futures::stream::iter([
                Ok(ModelEvent::ToolCall {
                    call_id: "formal-real-child".into(),
                    name: "acyclic.fork_child".into(),
                    arguments: json!({
                        "child_operation": self.child_operation.to_string(),
                        "task": "formal real child",
                        "prompt": "complete the real trace child"
                    }),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]));
        }
        let release = self.child_release.clone();
        let content = futures::stream::once(async move {
            release.notified().await;
            Ok::<ModelEvent, Error>(ModelEvent::Content {
                delta: "real child result".into(),
            })
        });
        let completed = futures::stream::once(async {
            Ok::<ModelEvent, Error>(ModelEvent::Completed {
                metadata: Value::Null,
            })
        });
        Box::pin(content.chain(completed))
    }

    fn generate_with_dispatch<'a>(
        &'a self,
        prepared: PreparedModelInput,
        dispatch: ProviderDispatchContext,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        self.usage.begin(&dispatch);
        let started = Instant::now();
        let usage = self.usage.clone();
        Box::pin(self.generate(prepared).map(move |event| {
            if let Ok(event) = &event {
                usage.record(&dispatch, started, event);
            }
            event
        }))
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }

    fn reconcile_admitted_with_dispatch<'a>(
        &'a self,
        prepared: PreparedModelInput,
        attempt: ModelAttempt,
        dispatch: ProviderDispatchContext,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        let usage = self.usage.clone();
        Box::pin(async move {
            if dispatch.operation_id != attempt.operation_id
                || dispatch.request_digest != attempt.request_digest
            {
                return Err(Error::Conflict(
                    "trace dispatch context does not match admitted attempt".into(),
                ));
            }
            let started = Instant::now();
            let events = self.reconcile_admitted(prepared, attempt).await?;
            if let Some(events) = &events {
                for event in events {
                    usage.record(&dispatch, started, event);
                }
            }
            Ok(events)
        })
    }
}

/// Captures the exact admitted request bytes while preserving the provider
/// boundary. The wrapper delegates policy, admission, authenticated usage,
/// and recovery to the inner provider; capture is observational only and
/// never changes the prepared model payload.
struct CaptureProvider {
    inner: Arc<dyn ModelProvider>,
    request_bytes: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
}

impl ModelProvider for CaptureProvider {
    fn model_option_policy(&self) -> Option<&ModelOptionPolicy> {
        self.inner.model_option_policy()
    }

    fn swarm_usage_source(&self) -> Option<Arc<dyn SwarmUsageSource>> {
        self.inner.swarm_usage_source()
    }

    fn supports_dispatch_context(&self) -> bool {
        self.inner.supports_dispatch_context()
    }

    fn admit(&self, request: &ModelRequest) -> Result<()> {
        self.inner.admit(request)
    }

    fn generate<'a>(&'a self, prepared: PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
        if let Ok(mut captured) = self.request_bytes.lock() {
            captured.push(prepared.bytes().to_vec());
        }
        self.inner.generate(prepared)
    }

    fn reconcile_admitted<'a>(
        &'a self,
        prepared: PreparedModelInput,
        attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        self.inner.reconcile_admitted(prepared, attempt)
    }

    fn generate_with_dispatch<'a>(
        &'a self,
        prepared: PreparedModelInput,
        dispatch: ProviderDispatchContext,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        if let Ok(mut captured) = self.request_bytes.lock() {
            captured.push(prepared.bytes().to_vec());
        }
        self.inner.generate_with_dispatch(prepared, dispatch)
    }

    fn reconcile_admitted_with_dispatch<'a>(
        &'a self,
        prepared: PreparedModelInput,
        attempt: ModelAttempt,
        dispatch: ProviderDispatchContext,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        self.inner
            .reconcile_admitted_with_dispatch(prepared, attempt, dispatch)
    }

    fn reconcile<'a>(
        &'a self,
        attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        self.inner.reconcile(attempt)
    }
}

async fn registry_events(
    stream: &acyclic_stream::Stream<LocalStream>,
) -> Result<Vec<(u64, StoredEvent, String)>> {
    let tail = match stream.tail().await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => 0,
        Err(error) => return Err(Error::Storage(error.to_string())),
    };
    let mut records = stream
        .read(
            0,
            u32::try_from(tail)
                .map_err(|_| Error::Storage("formal registry is too large".into()))?,
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let mut events = Vec::new();
    while let Some(record) = records.next().await {
        let record = record.map_err(|error| Error::Storage(error.to_string()))?;
        let stored: StoredRecord = serde_json::from_slice(&record.value)
            .map_err(|error| Error::Storage(format!("invalid formal registry record: {error}")))?;
        events.push((record.sequence, stored.event, hex_bytes(&record.value)));
    }
    Ok(events)
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_decode(value: &str) -> Result<Vec<u8>> {
    if value.len() % 2 != 0 {
        return Err(Error::Storage(
            "formal source bytes have odd hex length".into(),
        ));
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|error| Error::Storage(format!("invalid formal source hex: {error}")))
        })
        .collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_bytes(&Sha256::digest(bytes))
}

fn canonical_json_hex<T: serde::Serialize>(value: &T) -> Result<String> {
    Ok(hex_bytes(&crate::contract::canonical_json_bytes(value)?))
}

fn merge_json_objects(parts: impl IntoIterator<Item = Value>) -> Result<Value> {
    let mut merged = serde_json::Map::new();
    for part in parts {
        let object = part
            .as_object()
            .ok_or_else(|| Error::Storage("formal manifest part is not an object".into()))?;
        for (key, value) in object {
            if merged.insert(key.clone(), value.clone()).is_some() {
                return Err(Error::Storage(format!(
                    "formal manifest field is duplicated: {key}"
                )));
            }
        }
    }
    Ok(Value::Object(merged))
}

fn file_sha256(path: &Path) -> Result<String> {
    let bytes = fs::read(path).map_err(|error| Error::Storage(error.to_string()))?;
    Ok(sha256_hex(&bytes))
}

fn verification_provenance() -> Result<Value> {
    let verification = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("verification");
    let repo_root = verification
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .and_then(Path::parent)
        .ok_or_else(|| Error::Storage("formal verification path has no repository root".into()))?
        .to_path_buf();
    let executable = env::current_exe().map_err(|error| Error::Storage(error.to_string()))?;
    let source = verification.join("real_harness_trace.rs");
    let checker = verification.join("check-real-trace.ps1");
    let trace_checker = verification.join("check-trace.ps1");
    let qualification = verification.join("qualify-real-trace.ps1");
    Ok(json!({
        "source_commit": env::var("GRAPHCODER_REAL_TRACE_SOURCE_COMMIT").ok(),
        "source_tree": env::var("GRAPHCODER_REAL_TRACE_SOURCE_TREE").ok(),
        "source_clean": env::var("GRAPHCODER_REAL_TRACE_SOURCE_CLEAN")
            .ok()
            .and_then(|value| value.parse::<bool>().ok()),
        "workspace_manifest_path": repo_root.join("Cargo.toml"),
        "workspace_manifest_sha256": file_sha256(&repo_root.join("Cargo.toml"))?,
        "lockfile_path": repo_root.join("Cargo.lock"),
        "lockfile_sha256": file_sha256(&repo_root.join("Cargo.lock"))?,
        "exporter_source_path": source,
        "exporter_source_sha256": file_sha256(&source)?,
        "binary_path": executable,
        "binary_sha256": file_sha256(&executable)?,
        "real_checker_path": checker,
        "real_checker_sha256": file_sha256(&checker)?,
        "trace_checker_path": trace_checker,
        "trace_checker_sha256": file_sha256(&trace_checker)?,
        "qualification_path": qualification,
        "qualification_sha256": file_sha256(&qualification)?
    }))
}

fn generation_projection(report: &ForkReport) -> Result<(u64, Value)> {
    for (selection, capture) in report.request.selections.iter().zip(&report.captures) {
        if !matches!(&selection.revision, ResourceRevision::Project { .. }) {
            continue;
        }
        if let Capture::Captured(resource) = capture
            && let ResourceRevision::Project { generation, .. } = &resource.revision
        {
            // Filesystem generations are opaque provider identities. This
            // finite adapter retains the raw generation in the manifest and
            // maps the first observed immutable generation to ordinal zero.
            return serde_json::to_value(generation)
                .map(|value| (0, value))
                .map_err(|error| Error::Storage(format!("invalid formal generation: {error}")));
        }
    }
    Err(Error::Storage(
        "real fork admission has no captured project generation".into(),
    ))
}

fn operation_digest(event: &ExecutionEvent) -> Option<String> {
    match event {
        ExecutionEvent::ModelStarted { request_digest, .. } => Some(
            request_digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        ),
        _ => None,
    }
}

fn model_started_projection(event: &ExecutionEvent) -> Option<(u32, String)> {
    match event {
        ExecutionEvent::ModelStarted {
            step,
            request_digest,
        } => Some((*step, hex_bytes(request_digest))),
        _ => None,
    }
}

async fn export_real_trace(path: &Path) -> Result<()> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let child_operation = OperationId::from_bytes([2; 16]);
    let root_operation = OperationId::from_bytes([1; 16]);
    let child_release = Arc::new(Notify::new());
    let capture = Arc::new(CaptureProvider {
        inner: Arc::new(TraceModel {
            child_operation,
            child_release: child_release.clone(),
            calls: AtomicUsize::new(0),
            usage: Arc::new(TraceUsage::default()),
        }),
        request_bytes: Arc::new(std::sync::Mutex::new(Vec::new())),
    });
    let provider: Arc<dyn ModelProvider> = capture.clone();
    let model = Model::new("mock", "formal-real-trace", "1", json!({}))?;
    let swarm = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
        root.path(),
        model,
        provider,
        crate::conversation::Limits::default(),
    )
    .await?;
    let root_task = swarm.root_task().await?;
    let child_task = TaskId::from_bytes(child_operation.into_bytes());
    let message_id = OperationId::from_bytes([3; 16]);
    let message_body = b"formal runtime message";
    let mut root_run = Box::pin(swarm.run_root(root_operation, "formal real trace root"));
    let (sent_message, delivered_message) = loop {
        tokio::select! {
            result = &mut root_run => {
                result?;
                return Err(Error::Conflict(
                    "real child completed before the active-agent message could be admitted".into(),
                ));
            }
            _ = tokio::time::sleep(Duration::from_millis(1)) => {
                let Ok(session) = swarm.session(child_task).await else {
                    continue;
                };
                if !matches!(session.phase, LocalSessionPhase::Activating | LocalSessionPhase::Ready) {
                    continue;
                }
                let sent = swarm
                    .send_message(root_task, child_task, message_id, message_body)
                    .await?;
                let inbox = swarm.read_inbox(child_task, 0, 8).await?;
                let delivered = inbox
                    .iter()
                    .find(|item| item.message_id == message_id.to_string())
                    .cloned()
                    .ok_or_else(|| Error::Storage("real child inbox has no active-agent message".into()))?;
                // `notify_one` retains a permit if the provider has not yet
                // polled its gated child stream, avoiding a lost wake-up.
                child_release.notify_one();
                break (sent, delivered);
            }
        }
    };
    root_run.await?;
    let child_session = swarm.session(child_task).await?;
    if child_session.phase != LocalSessionPhase::Completed {
        return Err(Error::Storage("real child did not complete".into()));
    }

    let registry = swarm
        .registry
        .stream(REGISTRY_STREAM)
        .map_err(|error| Error::Storage(error.to_string()))?;
    let events = registry_events(&registry).await?;
    // ForkPrepared is the current durable admission record. ForkAdmitted is
    // retained only as a read-compatibility fallback for older registries;
    // the adapter never invents an admission record from a later event.
    let (
        admission_sequence,
        admission_kind,
        parent,
        parent_operation,
        parent_step,
        child,
        recorded_child_operation,
        child_authority,
        fork_operation,
        report,
        publication,
        admission_record_bytes,
        task,
        prompt,
        child_agent,
        seed,
        declaration,
    ) = events
        .iter()
        .find_map(|(sequence, event, record_bytes)| match event {
            StoredEvent::ForkPrepared {
                parent,
                parent_operation,
                parent_step,
                child,
                child_operation,
                child_authority,
                fork_operation,
                report,
                publication,
                declaration,
                task,
                prompt,
                child_agent,
                seed,
                ..
            } => Some((
                *sequence,
                "fork_prepared",
                *parent,
                *parent_operation,
                *parent_step,
                *child,
                *child_operation,
                child_authority.clone(),
                *fork_operation,
                report.clone(),
                publication.clone(),
                record_bytes.clone(),
                task.clone(),
                prompt.clone(),
                child_agent.clone(),
                seed.clone(),
                declaration.clone(),
            )),
            StoredEvent::ForkAdmitted {
                parent,
                parent_operation,
                parent_step,
                child,
                child_operation,
                child_authority,
                fork_operation,
                report,
                publication,
                declaration,
                task,
                prompt,
                child_agent,
                seed,
                ..
            } => Some((
                *sequence,
                "fork_admitted_legacy",
                *parent,
                *parent_operation,
                *parent_step,
                *child,
                *child_operation,
                child_authority.clone(),
                *fork_operation,
                report.clone(),
                publication.clone(),
                record_bytes.clone(),
                task.clone(),
                prompt.clone(),
                child_agent.clone(),
                seed.clone(),
                declaration.clone(),
            )),
            _ => None,
        })
        .ok_or_else(|| Error::Storage("real registry has no ForkPrepared record".into()))?;
    if parent != root_task
        || child != child_task
        || parent_operation != root_operation
        || recorded_child_operation != child_operation
    {
        return Err(Error::Conflict(
            "real registry fork identities do not match the executed operations".into(),
        ));
    }
    let child_authority = child_authority
        .ok_or_else(|| Error::Storage("real admission omitted child authority".into()))?;
    let fork_operation = fork_operation
        .ok_or_else(|| Error::Storage("real admission omitted fork operation".into()))?;
    let publication =
        publication.ok_or_else(|| Error::Storage("real admission omitted publication".into()))?;
    if fork_operation == publication.operation_id
        || fork_operation == child_operation
        || publication.operation_id == child_operation
    {
        return Err(Error::Conflict(
            "real fork, publication, and child operation identities are not distinct".into(),
        ));
    }
    let report =
        report.ok_or_else(|| Error::Storage("real admission omitted fork report".into()))?;
    let seed = seed.ok_or_else(|| Error::Storage("real admission omitted fork seed".into()))?;
    let declaration = declaration
        .ok_or_else(|| Error::Storage("real admission omitted fork declaration".into()))?;
    let publication_digest = crate::contract::canonical_json_digest(&publication)?;
    let (
        publication_completion_sequence,
        publication_completion_operation,
        publication_completion_digest,
        publication_completion_record_bytes,
    ) = events
        .iter()
        .find_map(|(sequence, event, record_bytes)| match event {
            StoredEvent::ForkPublicationCompleted { operation, digest }
                if *operation == publication.operation_id =>
            {
                Some((*sequence, *operation, *digest, record_bytes.clone()))
            }
            _ => None,
        })
        .ok_or_else(|| {
            Error::Storage(
                "real registry has no ForkPublicationCompleted receipt for the publication".into(),
            )
        })?;
    if publication_completion_digest != publication_digest {
        return Err(Error::Conflict(
            "ForkPublicationCompleted digest differs from the authenticated publication".into(),
        ));
    }
    if publication_completion_operation != publication.operation_id {
        return Err(Error::Conflict(
            "ForkPublicationCompleted operation differs from the authenticated publication".into(),
        ));
    }
    // Reopen the owner completion index from the same registry and submit the
    // exact receipt again. This is the production replay path: the existing
    // operation/digest must be accepted without appending a second receipt.
    let replay_plans = LocalModelForkPlans::new();
    replay_plans.bind_journal(swarm.registry.clone()).await?;
    if replay_plans.completed(publication.operation_id).await? != Some(publication_digest) {
        return Err(Error::Conflict(
            "reopened completion index lost the publication receipt".into(),
        ));
    }
    replay_plans
        .mark_completed(publication.operation_id, publication_digest)
        .await?;
    let replayed_events = registry_events(&registry).await?;
    let replay_count = replayed_events
        .iter()
        .filter(|(_, event, _)| {
            matches!(
                event,
                StoredEvent::ForkPublicationCompleted { operation, digest }
                    if *operation == publication.operation_id && *digest == publication_digest
            )
        })
        .count();
    if replay_count != 1 {
        return Err(Error::Conflict(
            "same-digest publication replay appended a duplicate receipt".into(),
        ));
    }
    let substitution_rejected = matches!(
        replay_plans
            .mark_completed(publication.operation_id, [0xA5; 32])
            .await,
        Err(Error::Conflict(_))
    );
    if !substitution_rejected {
        return Err(Error::Conflict(
            "publication replay accepted a substituted digest".into(),
        ));
    }
    let (generation, raw_generation) = generation_projection(&report)?;

    let parent_harness = swarm.open_session(root_task).await?;
    let parent_events = parent_harness
        .conversation_events(0, 1_024, crate::conversation::Limits::default())
        .await?;
    let publication_event = parent_events
        .iter()
        .find_map(|event| match &event.payload {
            crate::core::EventPayload::ForkPublished { seed }
                if seed.operation_id == fork_operation
                    && seed.child == child_authority
                    && event.operation_id == fork_operation =>
            {
                Some(event)
            }
            _ => None,
        })
        .ok_or_else(|| {
            Error::Storage("parent conversation has no matching ForkPublished event".into())
        })?;
    let publication_seed = match &publication_event.payload {
        crate::core::EventPayload::ForkPublished { seed } => seed.as_ref().clone(),
        _ => unreachable!("matching ForkPublished event changed payload"),
    };
    if publication_seed != seed {
        return Err(Error::Conflict(
            "parent ForkPublished seed differs from durable fork admission".into(),
        ));
    }
    let publication_revision = publication_event.revision;
    let publication_event_operation = publication_event.operation_id;
    let publication_event_bytes = crate::contract::canonical_json_bytes(publication_event)?;

    let child_harness = swarm.open_session(child_task).await?;
    let child_records = child_harness
        .storage()
        .journal()
        .replay(child_operation)
        .await?;
    let model_record = child_records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ModelStarted { .. } => Some(record),
            _ => None,
        })
        .ok_or_else(|| Error::Storage("real child journal has no ModelStarted record".into()))?;
    let model_sequence = model_record.sequence;
    let model_digest = operation_digest(&model_record.event);
    let (model_step, model_request_digest) = model_started_projection(&model_record.event)
        .ok_or_else(|| Error::Storage("real child ModelStarted projection disappeared".into()))?;
    let model_request_ref = child_records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ModelInputPrepared { step, request, .. } if *step == model_step => {
                Some(request.clone())
            }
            _ => None,
        })
        .ok_or_else(|| Error::Storage("real child journal has no prepared model request".into()))?;
    let journal_request_bytes = child_harness
        .storage()
        .journal()
        .load(&model_request_ref)
        .await?;
    let captured_requests = capture
        .request_bytes
        .lock()
        .map_err(|_| Error::Storage("real mock provider request capture was poisoned".into()))?
        .clone();
    let matching_requests = captured_requests
        .into_iter()
        .filter(|bytes| *bytes == journal_request_bytes)
        .collect::<Vec<_>>();
    if matching_requests.len() != 1 {
        return Err(Error::Conflict(format!(
            "real mock provider captured {} requests matching the child durable request",
            matching_requests.len()
        )));
    }
    let captured_request_bytes = matching_requests
        .into_iter()
        .next()
        .ok_or_else(|| Error::Storage("real mock provider did not capture child request".into()))?;
    if captured_request_bytes != journal_request_bytes {
        return Err(Error::Conflict(
            "real provider request bytes differ from the durable prepared request".into(),
        ));
    }
    let request_digest = *blake3::hash(&captured_request_bytes).as_bytes();
    let journal_request_digest = match &model_record.event {
        ExecutionEvent::ModelStarted { request_digest, .. } => *request_digest,
        _ => {
            return Err(Error::Storage(
                "real child ModelStarted record changed".into(),
            ));
        }
    };
    if request_digest != journal_request_digest {
        return Err(Error::Conflict(
            "real provider request bytes do not match ModelStarted request digest".into(),
        ));
    }
    let model_record_bytes = crate::contract::canonical_json_bytes(&model_record.event)?;
    let (
        completion_sequence,
        completion_operation,
        completion_record_bytes,
        completion_output,
        completion_output_ref,
        completion_output_digest,
    ) = events
        .iter()
        .find_map(|(sequence, event, record_bytes)| match event {
            StoredEvent::ForkCompleted {
                child: completed_child,
                operation,
                output,
                output_ref,
                output_digest,
                ..
            } if *completed_child == child_task && *operation == child_operation => Some((
                *sequence,
                *operation,
                record_bytes.clone(),
                output.clone(),
                output_ref.clone(),
                *output_digest,
            )),
            _ => None,
        })
        .ok_or_else(|| Error::Storage("real registry has no ForkCompleted record".into()))?;

    if completion_output_digest.is_none()
        || (completion_output.is_none() && completion_output_ref.is_none())
    {
        return Err(Error::Storage(
            "real completion has no durable output and digest".into(),
        ));
    }

    let post_message_events = registry_events(&registry).await?;
    let (
        message_admission_sequence,
        message_admission_record_bytes,
        message_admission_payload,
    ) = post_message_events
        .iter()
        .find_map(|(sequence, event, record_bytes)| match event {
            StoredEvent::MessageAdmitted {
                sender,
                recipient,
                message_id: recorded_id,
                payload,
            } if *sender == root_task
                && *recipient == child_task
                && *recorded_id == message_id =>
            {
                Some((*sequence, record_bytes.clone(), payload.clone()))
            }
            _ => None,
        })
        .ok_or_else(|| Error::Storage("real registry has no MessageAdmitted record".into()))?;
    if message_admission_payload != sent_message.payload {
        return Err(Error::Conflict(
            "message admission payload differs from the send result".into(),
        ));
    }

    let trace = vec![
        json!({
            "kind": "fork_admitted",
            "fork_operation_id": fork_operation.to_string(),
            "child_operation_id": child_operation.to_string(),
            "parent": 1,
            "child": 2,
            "depth": 1,
            "captured_generation": generation
        }),
        json!({
            "kind": "workspace_published",
            "fork_operation_id": fork_operation.to_string(),
            "publication_operation_id": publication.operation_id.to_string(),
            "publication_completion_sequence": publication_completion_sequence,
            "publication_completion_digest": hex_bytes(&publication_completion_digest),
            "parent": 1,
            "child": 2,
            "captured_generation": generation
        }),
        json!({
            "kind": "model_started",
            "child_operation_id": child_operation.to_string(),
            "agent": 2,
            "request_digest": model_request_digest,
            "request_bytes_sha256": sha256_hex(&captured_request_bytes)
        }),
        json!({
            "kind": "message_admitted",
            "message_id": message_id.to_string(),
            "sender": 1,
            "recipient": 2,
            "admission_sequence": message_admission_sequence
        }),
        json!({
            "kind": "message_delivered",
            "message_id": message_id.to_string(),
            "delivery_index": delivered_message.sequence
        }),
        json!({
            "kind": "agent_completed",
            "child_operation_id": child_operation.to_string(),
            "agent": 2,
            "outcome_durable": true
        }),
    ];
    let bytes = serde_json::to_vec_pretty(&trace)
        .map_err(|error| Error::Storage(format!("invalid formal trace: {error}")))?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| Error::Storage(error.to_string()))?;
    }
    fs::write(path, &bytes).map_err(|error| Error::Storage(error.to_string()))?;
    let trace_digest = sha256_hex(&bytes);

    let manifest_path = path.with_extension("manifest.json");
    let source = merge_json_objects([
        json!({
            "registry_stream": REGISTRY_STREAM,
            "admission_record": admission_kind,
            "fork_admitted_sequence": admission_sequence,
            "fork_completed_sequence": completion_sequence,
            "parent_conversation_event": "ForkPublished",
            "parent_conversation_revision": publication_revision,
            "parent_conversation_operation": publication_event_operation,
            "publication_completion_sequence": publication_completion_sequence,
            "publication_completion_operation": publication_completion_operation,
            "publication_completion_digest": publication_completion_digest,
            "publication_completion_record_bytes_hex": publication_completion_record_bytes,
            "publication_completion_record_sha256": sha256_hex(&hex_decode(&publication_completion_record_bytes)?),
            "publication_completion_replay_count": replay_count,
            "publication_completion_replay_same_digest": true,
            "publication_completion_replay_substitution_rejected": substitution_rejected,
            "child_execution_operation": child_operation,
            "child_execution_model_started_sequence": model_sequence,
            "child_execution_model_started_request_digest": model_digest,
            "child_execution_model_started_step": model_step,
            "child_execution_model_started_request_digest_hex": model_request_digest,
            "child_execution_model_started_request_bytes_hex": hex_bytes(&captured_request_bytes),
            "child_execution_model_started_request_bytes_sha256": sha256_hex(&captured_request_bytes)
        }),
        json!({
            "admission_record_bytes_hex": admission_record_bytes,
            "admission_record_sha256": sha256_hex(&hex_decode(&admission_record_bytes)?),
            "parent_event_canonical_bytes_hex": hex_bytes(&publication_event_bytes),
            "parent_event_sha256": sha256_hex(&publication_event_bytes),
            "child_model_event_canonical_bytes_hex": hex_bytes(&model_record_bytes),
            "child_model_event_sha256": sha256_hex(&model_record_bytes),
            "completion_record_bytes_hex": completion_record_bytes,
            "completion_record_sha256": sha256_hex(&hex_decode(&completion_record_bytes)?),
            "message_admission_sequence": message_admission_sequence,
            "message_admission_record_bytes_hex": message_admission_record_bytes,
            "message_admission_record_sha256": sha256_hex(&hex_decode(&message_admission_record_bytes)?),
            "message_id": message_id,
            "message_sender": root_task,
            "message_recipient": child_task,
            "message_admission_payload": message_admission_payload,
            "message_delivery_sequence": delivered_message.sequence,
            "message_delivery_message_id": delivered_message.message_id,
            "message_delivery_payload": delivered_message.payload
        }),
        json!({
            "root_task": root_task,
            "child_task": child_task,
            "child_depth": child_session.depth,
            "child_authority": child_authority,
            "parent_step": parent_step,
            "task": task,
            "prompt": prompt,
            "child_agent": child_agent
        }),
        json!({
            "seed": seed,
            "seed_canonical_bytes_hex": canonical_json_hex(&seed)?,
            "seed_sha256": sha256_hex(&crate::contract::canonical_json_bytes(&seed)?),
            "seed_digest": fork_seed_digest(&seed)?,
            "report": report,
            "report_canonical_bytes_hex": canonical_json_hex(&report)?,
            "report_sha256": sha256_hex(&crate::contract::canonical_json_bytes(&report)?),
            "publication": publication,
            "publication_canonical_bytes_hex": canonical_json_hex(&publication)?,
            "publication_sha256": sha256_hex(&crate::contract::canonical_json_bytes(&publication)?),
            "declaration": declaration,
            "declaration_canonical_bytes_hex": canonical_json_hex(&declaration)?,
            "declaration_sha256": sha256_hex(&crate::contract::canonical_json_bytes(&declaration)?)
        }),
        json!({
            "root_operation": root_operation,
            "fork_operation": fork_operation,
            "publication_operation": publication.operation_id,
            "publication_parent_operation": publication.parent_operation,
            "publication_step": publication.step,
            "parent_seed_authority": publication_seed.parent,
            "parent_seed_revision": publication_seed.parent_revision
        }),
        json!({
            "completion_operation": completion_operation,
            "completion_output": completion_output,
            "completion_output_ref": completion_output_ref,
            "completion_output_digest": completion_output_digest
        }),
    ])?;
    let identity_binding = json!({
        "fork_operation_id": fork_operation,
        "publication_operation_id": publication.operation_id,
        "publication_completion_operation_id": publication_completion_operation,
        "child_operation_id": child_operation,
        "parent_event_operation_id": publication_event_operation,
        "completion_operation_id": completion_operation
    });
    let ordering = json!({
        "basis": "causal projection across independently ordered durable streams",
        "registry": "registry sequence orders ForkPrepared, ForkPublicationCompleted, ForkCompleted, and MessageAdmitted",
        "mailbox": "mailbox sequence identifies the delivered MessageAdmitted payload",
        "parent_conversation": "conversation revision identifies ForkPublished",
        "child_execution": "child journal sequence identifies ModelStarted",
        "cross_stream_sequences_compared": false,
        "source_chronology_is_not_projected": true
    });
    let normalization = json!({
        "task_ids": {"1": root_task, "2": child_task},
        "agent_ids": {"2": child_agent},
        "generation": {
            "finite_ordinal": generation,
            "raw_captured_generation": raw_generation,
            "rule": "first observed immutable project generation maps to ordinal zero"
        }
    });
    let assumptions = json!([
        "The trace is one real local Filesystem-backed Harness run using a deterministic mock provider.",
        "The publication completion receipt was reopened by operation identity and replayed with the same digest; the replay must not append a second receipt, and a substituted digest is rejected.",
        "Task and opaque generation identities are normalized only at the adapter boundary.",
        "Current project generation is not independently observed by this trace, so publication freshness is not claimed.",
        "This trace does not prove approval handling, aggregate budget exhaustion, Rust refinement, liveness, OS confinement, project integration, or a total order across streams."
    ]);
    let manifest = json!({
        "kind": "real_harness_trace_manifest",
        "trace": path,
        "source": source,
        "identity_binding": identity_binding,
        "trace_binding": {
            "trace_path": path,
            "trace_sha256": trace_digest
        },
        "provenance": verification_provenance()?,
        "ordering": ordering,
        "normalization": normalization,
        "assumptions": assumptions
    });
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|error| Error::Storage(format!("invalid formal trace manifest: {error}")))?;
    fs::write(&manifest_path, manifest_bytes).map_err(|error| Error::Storage(error.to_string()))?;
    Ok(())
}

#[tokio::test]
#[ignore = "explicit real trace export; set GRAPHCODER_REAL_TRACE_PATH"]
async fn exports_real_harness_trace_for_canonical_checker() -> Result<()> {
    let path = env::var_os("GRAPHCODER_REAL_TRACE_PATH")
        .map(PathBuf::from)
        .ok_or_else(|| Error::Invalid("GRAPHCODER_REAL_TRACE_PATH is required".into()))?;
    export_real_trace(&path).await
}

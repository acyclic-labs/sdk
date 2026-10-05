//! Test-only exporter for one real durable local swarm execution.
//!
//! This module is included below `filesystem::swarm_local::tests`, so it can
//! inspect the private registry and authenticated child journal without
//! widening the production API. It deliberately emits a small normalized
//! projection consumed by `check-trace.ps1`; the sidecar records the source
//! stream sequences and operation identities used to derive every event.

use super::super::*;
use crate::{
    Error, OperationId, Result,
    executor::ExecutionEvent,
    model::{Model, ModelAttempt, ModelEvent, ModelProvider},
    model_input::PreparedModelInput,
};
use acyclic_stream::{LocalStream, StreamError};
use futures::{StreamExt as _, future::BoxFuture, stream::BoxStream};
use serde_json::{Value, json};
use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tempfile::tempdir;

struct TraceModel {
    child_operation: OperationId,
    calls: AtomicUsize,
}

impl ModelProvider for TraceModel {
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
        Box::pin(futures::stream::iter([
            Ok(ModelEvent::Content {
                delta: "real child result".into(),
            }),
            Ok(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]))
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

async fn registry_events(
    stream: &acyclic_stream::Stream<LocalStream>,
) -> Result<Vec<(u64, StoredEvent)>> {
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
        events.push((record.sequence, stored.event));
    }
    Ok(events)
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
            return Ok((0, serde_json::to_value(generation)?));
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

async fn export_real_trace(path: &Path) -> Result<()> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let child_operation = OperationId::from_bytes([2; 16]);
    let root_operation = OperationId::from_bytes([1; 16]);
    let provider = Arc::new(TraceModel {
        child_operation,
        calls: AtomicUsize::new(0),
    });
    let model = Model::new("mock", "formal-real-trace", "1", json!({}))?;
    let swarm = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
        root.path(),
        model,
        provider,
        crate::conversation::Limits::default(),
    )
    .await?;
    let root_task = swarm.root_task().await?;
    swarm
        .run_root(root_operation, "formal real trace root")
        .await?;
    let child_task = TaskId::from_bytes(child_operation.into_bytes());
    let child_session = swarm.session(child_task).await?;
    if child_session.phase != LocalSessionPhase::Completed {
        return Err(Error::Storage("real child did not complete".into()));
    }

    let registry = swarm
        .registry
        .stream(REGISTRY_STREAM)
        .map_err(|error| Error::Storage(error.to_string()))?;
    let events = registry_events(&registry).await?;
    let (admission_sequence, admitted) = events
        .iter()
        .find_map(|(sequence, event)| match event {
            StoredEvent::ForkAdmitted { .. } => Some((*sequence, event.clone())),
            _ => None,
        })
        .ok_or_else(|| Error::Storage("real registry has no ForkAdmitted record".into()))?;
    let StoredEvent::ForkAdmitted {
        parent,
        parent_operation,
        parent_step,
        child,
        child_operation: recorded_child_operation,
        fork_operation,
        report,
        publication,
        ..
    } = admitted
    else {
        unreachable!();
    };
    if parent != root_task
        || child != child_task
        || parent_operation != root_operation
        || recorded_child_operation != child_operation
    {
        return Err(Error::Conflict(
            "real registry fork identities do not match the executed operations".into(),
        ));
    }
    let fork_operation = fork_operation
        .ok_or_else(|| Error::Storage("real admission omitted fork operation".into()))?;
    let publication =
        publication.ok_or_else(|| Error::Storage("real admission omitted publication".into()))?;
    let report =
        report.ok_or_else(|| Error::Storage("real admission omitted fork report".into()))?;
    let (generation, raw_generation) = generation_projection(&report)?;

    let child_harness = swarm.open_session(child_task).await?;
    let child_records = child_harness
        .storage()
        .journal()
        .replay(child_operation)
        .await?;
    let (model_sequence, model_digest) = child_records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ModelStarted { .. } => {
                Some((record.sequence, operation_digest(&record.event)))
            }
            _ => None,
        })
        .ok_or_else(|| Error::Storage("real child journal has no ModelStarted record".into()))?;
    let (completion_sequence, completion_operation) = events
        .iter()
        .find_map(|(sequence, event)| match event {
            StoredEvent::ForkCompleted {
                child: completed_child,
                operation,
                ..
            } if *completed_child == child_task && *operation == child_operation => {
                Some((*sequence, *operation))
            }
            _ => None,
        })
        .ok_or_else(|| Error::Storage("real registry has no ForkCompleted record".into()))?;

    let trace = vec![
        json!({
            "kind": "fork_admitted",
            "operation_id": child_operation.to_string(),
            "parent": 1,
            "child": 2,
            "depth": 1,
            "captured_generation": generation
        }),
        json!({
            "kind": "model_started",
            "operation_id": child_operation.to_string(),
            "agent": 2
        }),
        json!({
            "kind": "agent_completed",
            "operation_id": child_operation.to_string(),
            "agent": 2,
            "outcome_durable": true
        }),
        json!({
            "kind": "workspace_published",
            "operation_id": child_operation.to_string(),
            "parent": 1,
            "child": 2,
            "captured_generation": generation,
            "current_generation": generation
        }),
    ];
    let bytes = serde_json::to_vec_pretty(&trace)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| Error::Storage(error.to_string()))?;
    }
    fs::write(path, &bytes).map_err(|error| Error::Storage(error.to_string()))?;

    let manifest_path = path.with_extension("manifest.json");
    let manifest = json!({
        "kind": "real_harness_trace_manifest",
        "trace": path,
        "source": {
            "registry_stream": REGISTRY_STREAM,
            "fork_admitted_sequence": admission_sequence,
            "fork_completed_sequence": completion_sequence,
            "child_execution_operation": child_operation,
            "child_execution_model_started_sequence": model_sequence,
            "child_execution_model_started_request_digest": model_digest,
            "root_task": root_task,
            "child_task": child_task,
            "root_operation": root_operation,
            "fork_operation": fork_operation,
            "publication_operation": publication.operation_id,
            "publication_parent_operation": publication.parent_operation,
            "publication_step": parent_step,
            "completion_operation": completion_operation
        },
        "normalization": {
            "agent_ids": {"1": root_task, "2": child_task},
            "generation": {
                "finite_ordinal": generation,
                "raw_captured_generation": raw_generation,
                "rule": "first observed immutable project generation maps to ordinal zero"
            }
        },
        "assumptions": [
            "The trace is one real local Filesystem-backed Harness run using a deterministic mock provider.",
            "Task and opaque generation identities are normalized only at the adapter boundary.",
            "The adapter checks event ordering and bindings; it does not prove Rust refinement, liveness, or OS confinement."
        ]
    });
    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)
        .map_err(|error| Error::Storage(error.to_string()))?;
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

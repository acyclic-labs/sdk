//! Lifecycle effects retain protected immutable dispatch and context records.
use super::*;
use crate::{
    OperationId, TaskId,
    executor::json::{JsonBounds, json_bytes_bounded, json_from_slice_bounded},
    task_volume::{Component, FileKey, FileValue, Query, TaskFile, TaskVolume},
};
use acyclic_stream::{CommitCondition, StreamProvider};

pub(crate) fn phase_operation(tool: OperationId, event: HookEvent) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness:tool-hook:v1\0");
    hasher.update(&tool.into_bytes());
    hasher.update(event.name().as_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(id)
}

pub(crate) fn approval_identity(manifest: [u8; 32]) -> crate::registry::ComponentIdentity {
    crate::registry::ComponentIdentity {
        name: "hooks.pre-tool".into(),
        version: "1".into(),
        digest: manifest,
    }
}

fn keys(task: TaskId, operation: OperationId, phase: &str) -> Result<(FileKey, FileKey)> {
    let task = Component::new(task.to_string())?;
    Ok((
        FileKey::Task {
            task: task.clone(),
            file: TaskFile::Context(Component::new(format!("hook-{operation}-{phase}-claim"))?),
        },
        FileKey::Task {
            task,
            file: TaskFile::Context(Component::new(format!("hook-{operation}-{phase}-outcome"))?),
        },
    ))
}

/// Claims without outcomes must recover before a preparation error can settle.
pub(crate) async fn unsettled_prompt<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    task: TaskId,
    operation: OperationId,
) -> Result<bool> {
    let pairs = HookEvent::at(HookSite::Prompt)
        .map(|event| keys(task, operation, event.name()))
        .collect::<Result<Vec<_>>>()?;
    let selected: Vec<_> = pairs
        .iter()
        .flat_map(|(a, b)| [a.clone(), b.clone()])
        .collect();
    let snapshot = volume
        .snapshot_files(&selected, &Query::Task(Component::new(task.to_string())?))
        .await?;
    Ok(pairs.iter().any(|(claim, outcome)| {
        snapshot.files.iter().any(|file| file.key == *claim)
            && !snapshot.files.iter().any(|file| file.key == *outcome)
    }))
}

#[doc(hidden)]
pub async fn run_once<P, A, AF, F, FF, const CLAIM: bool>(
    volume: &TaskVolume<P>,
    identity: (TaskId, OperationId, &str),
    maximum_bytes: u64,
    allow_start: bool,
    mut authorize: A,
    execute: F,
) -> Result<Option<HookOutput>>
where
    P: StreamProvider + ?Sized,
    A: FnMut() -> AF,
    AF: Future<Output = Result<(Vec<CommitCondition>, Option<u64>)>>,
    F: FnOnce() -> FF,
    FF: Future<Output = Result<HookOutput>>,
{
    let (task, operation, phase) = identity;
    if maximum_bytes == 0 || maximum_bytes == u64::MAX {
        return Err(Error::Invalid(
            "hook outcome requires a finite file bound".into(),
        ));
    }
    let (claim, result) = keys(task, operation, phase)?;
    let bounds = JsonBounds {
        maximum_bytes,
        maximum_items: maximum_bytes,
        maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
    };
    // Reject an unrepresentable failure before claiming an external effect.
    // A capture/merge overflow must always have room for its bounded outcome.
    super::budget::Budget::new(maximum_bytes)?;
    let mut failure = HookOutput::output_limit(false);
    // No additional lock: the task-volume generation CAS claims the effect.
    // Its index lock remains a leaf; authorization/provider IO happens outside it.
    let generation = loop {
        let cut = volume
            .snapshot_files(
                &[claim.clone(), result.clone()],
                &Query::File(result.clone()),
            )
            .await?;
        if let Some(file) = cut.files.iter().find(|file| file.key == result) {
            if !cut.files.iter().any(|file| file.key == claim) {
                return Err(Error::Invalid("hook outcome has no execution claim".into()));
            }
            authorize().await?;
            return json_from_slice_bounded(
                std::marker::PhantomData::<HookOutput>,
                &file.bytes,
                bounds,
                None,
                None,
                &mut |_| Ok(()),
            )
            .map(Some);
        }
        if !CLAIM {
            break cut.generation;
        }
        if cut.files.iter().any(|file| file.key == claim) {
            return Err(Error::Indeterminate(operation));
        }
        if !allow_start {
            return Ok(None);
        }
        let (conditions, deadline) = authorize().await?;
        // Creation is exclusive even for identical bytes. An idempotent append
        // acknowledgement is never permission for another caller to launch.
        match volume
            .create_if(
                cut.generation,
                &FileValue::new(
                    claim.clone(),
                    acyclic_stream::OwnedBytes::from_static(b"{}"),
                ),
                conditions,
                deadline,
                None,
            )
            .await
        {
            Ok(generation) => break generation,
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(error.into()),
        }
    };
    let output = execute().await?;
    let (output, bytes) = match json_bytes_bounded(&output, bounds, None, &mut |_| Ok(())) {
        Ok(bytes) => (output, bytes),
        Err(_) => {
            failure.diagnostics[0].blocking = output.blocks() || output.approval.is_some();
            let bytes = json_bytes_bounded(&failure, bounds, None, &mut |_| Ok(()))?;
            (failure, bytes)
        }
    };
    crate::task_volume::publish(
        volume,
        generation,
        &FileValue::new(result, bytes),
        authorize,
    )
    .await?;
    Ok(Some(output))
}

#[cfg(test)]
#[cfg(acyclic_harness_loom)]
mod models;
#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests;

//! Pi's pinned native command over the existing task/effect and namespace owners.
use acyclic_harness::runtime::TaskSpawner as _;

use super::{PiScratchAllocation, PiTaskBinding, pi_caller};
use acyclic_fs::{
    CancellationToken, NativeMaterializationWork, PublicationPermit, WorkBudget, WorkCounters,
};
#[cfg(test)]
use acyclic_harness::IdempotencyKey;
use acyclic_harness::JsonValueSchema;
use acyclic_harness::{
    Capabilities,
    executor::ExecutionJournal,
    interaction::{Interaction, InteractionResponse},
};
use acyclic_harness::{
    Error, InteractionId, OperationId, Result,
    conversation::{ContentGrant, FileRef, VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, EffectGuarantee, Scope},
    durable_host::{TaskJournalOwner, task_interaction_id},
    effect_host::TaskEffectPlan,
    filesystem::{
        BorrowedNativeVolumeBinding, EFFECT_TASK_COMMAND_KIND, NATIVE_PROCESS_EFFECT_KIND,
        NativeExistingLazyVolume, NativePreparationRoot, NativeProcessRequest, NativeViewManifest,
        NativeViewOptions, NativeVolumeMapping, NativeVolumeNamespace, NativeVolumeView,
    },
    runtime::{NativeVolumeCallbackGuard, TaskContext},
    workflow::WorkflowCommand,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(test)]
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

pub(crate) const PI_NATIVE_PROVIDER: &str = "acyclic.pi.native";
pub(crate) const PI_NATIVE_VERSION: &str = "1.1.0";

mod paths;
pub(crate) use paths::*;

mod source;
pub(crate) use source::*;

mod runtime;
pub(crate) use runtime::*;

mod intent;
pub(crate) use intent::*;

mod root;
pub(crate) use root::*;

fn result_schema() -> Result<&'static serde_json::value::RawValue> {
    serde_json::from_str(r#"{"type":"object","additionalProperties":false,"required":["success","exit_code","stdout","stderr","stop"],"properties":{"success":{"type":"boolean"},"exit_code":{"type":["integer","null"]},"stdout":{"type":"array","items":{"type":"integer","minimum":0,"maximum":255}},"stderr":{"type":"array","items":{"type":"integer","minimum":0,"maximum":255}},"stop":{"oneOf":[{"type":"null"},{"type":"object","additionalProperties":false,"required":["kind","message","cleanup_completed"],"properties":{"kind":{"enum":["output_limit","timeout","control_stopped","capture_failed"]},"message":{"type":"string"},"cleanup_completed":{"type":"boolean"}}}]}}}"#).map_err(decode_error)
}

fn pi_native_string(
    value: &str,
    work: &NativeMaterializationWork,
) -> Result<(String, acyclic_fs::NativeAllocation)> {
    let (bytes, custody) = work.copy_bytes(value.as_bytes())?;
    Ok((
        String::from_utf8(bytes).expect("SDK copied the UTF-8 source unchanged"),
        custody,
    ))
}

/// Funds vector backing before allocation and retains its actual capacity.
pub(crate) fn pi_native_vec<T>(
    capacity: usize,
    work: &NativeMaterializationWork,
) -> Result<(Vec<T>, acyclic_fs::NativeAllocation)> {
    let bytes = capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| acyclic_harness::Error::from(acyclic_fs::WorkError::Overflow.into()))?;
    let mut custody = if bytes == 0 {
        work.claim(0, 0, acyclic_fs::WorkCounters::default())
    } else {
        work.claim(
            bytes as u64,
            u64::from((bytes as u64) != 0),
            acyclic_fs::WorkCounters::default(),
        )
    }?;
    let mut value = Vec::new();
    if let Err(cause) = value.try_reserve_exact(capacity) {
        return Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause),
            custody: Some(custody),
        });
    }
    let actual = value
        .capacity()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| {
            acyclic_fs::NativeTreeMaterializationError::from(acyclic_fs::WorkError::Overflow)
        });
    if let Err(cause) = actual.and_then(|bytes| custody.reconcile_capacity(bytes as u64)) {
        drop(value);
        return Err(Error::NativeWork {
            cause,
            custody: Some(custody),
        });
    }
    Ok((value, custody))
}

fn has_parent(path: &Path) -> bool {
    path.components()
        .any(|part| matches!(part, Component::ParentDir))
}
fn invalid(message: &str) -> Error {
    Error::Invalid(message.to_owned())
}
fn decode_error(error: serde_json::Error) -> Error {
    invalid(&error.to_string())
}

#[cfg(test)]
#[path = "../pi_native_tests.rs"]
mod tests;

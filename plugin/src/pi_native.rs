//! Pi's pinned native command over the existing task/effect and namespace owners.
//! Native processes are trusted effects; a mount is not a process sandbox.

use super::{PiScratchAllocation, PiTaskBinding};
use acyclic_fs::{CancellationToken, LazyMount, MeasuredResult, NativeMaterializationWork, OperationReceipt, PublicationPermit, WorkBudget, WorkCounters};
#[cfg(test)]
use acyclic_fs::OperationFailure;
use acyclic_harness::{
    EffectAttemptId, Error, InteractionId, NativeFutureBundle, OperationId, Result,
    conversation::{ContentGrant, FileRef, VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, EffectGuarantee, EffectStatus, Scope},
    durable_host::{CoordinatorTaskHost, TaskJournalOwner, task_interaction_id},
    effect_host::{ConversationEffectHost, TaskEffectPlan, task_effect_id_with_native_work},
    effects::EffectProvider,
    filesystem::{
        BorrowedNativeVolumeBinding, EFFECT_TASK_COMMAND_KIND, FilesystemHost,
        NATIVE_PROCESS_EFFECT_KIND, NativePreparationReceipt, NativePreparationRoot,
        NativeProcessAuthority, NativeProcessProvider, NativeProcessReceipt, NativeReceiptError,
        NativeProcessRequest, NativeProcessView, NativePublicationFailure, NativeViewManifest, NativeViewOptions,
        NativeViewOwner, NativeVolumeNamespace, NativeVolumeView, workspace_ref,
        NativeVolumeMapping,
        NativeViewPublicationError,
    },
    runtime::{TaskContext, NativeTaskContext, NativeVolumeCallbackGuard},
    workflow::WorkflowCommand,
};
use acyclic_harness::JsonValueSchema;
#[cfg(test)]
use acyclic_harness::runtime::DurableTaskHost;
#[cfg(test)]
use acyclic_harness::IdempotencyKey;
use acyclic_harness::{core::Capabilities, interaction::{Interaction, InteractionResponse}, executor::ExecutionJournal};
use acyclic_stream::StreamProvider;
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(test)]
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::{Component, Path, PathBuf}, sync::{Arc, Mutex}};

pub(crate) const PI_NATIVE_PROVIDER: &str = "acyclic.pi.native";
pub(crate) const PI_NATIVE_VERSION: &str = "1.1.0";

/// References only. The authenticated factory, not these strings, allocates
/// or attests the actual original task's private native namespace.
pub(crate) struct PiNativePaths {
    pub(crate) cwd: PathBuf,
    pub(crate) session_file: PathBuf,
    pub(crate) session_directory: PathBuf,
    pub(crate) view_root: PathBuf,
    pub(crate) operation_root: PathBuf,
}

pub(crate) fn planned_native_paths(
    resources: &super::ServiceResources, root_session: &str, task: acyclic_harness::TaskId, root_key: Option<&str>,
) -> Result<PiNativePaths> {
    if root_session.is_empty() || !resources.data.is_absolute() {
        return Err(invalid("Pi native path references require their original absolute service root"));
    }
    let task_hash = blake3::hash(&task.into_bytes()).to_hex();
    let task_directory = resources.session_directory(root_session).join("agents").join(task_hash.as_str());
    let view_root = task_directory.join("view");
    let session_directory = view_root.join("scratch");
    let cwd = match root_key {
        Some(key) => view_root.join(native_root_route(key)?),
        None => session_directory.clone(),
    };
    Ok(PiNativePaths { cwd, session_file: session_directory.join("session.jsonl"), session_directory,
        view_root, operation_root: task_directory.join("preparation") })
}

pub(crate) fn planned_native_paths_with_native_work(
    resources: &super::ServiceResources, root_session: &str, task: acyclic_harness::TaskId,
    root_key: Option<&str>, work: &NativeMaterializationWork,
) -> Result<(PiNativePaths, acyclic_fs::NativeAllocation)> {
    work.remaining().map_err(native_error)?;
    let route = root_key.map(|key| native_root_route_with_work(key, work)).transpose()?;
    let task = native_task_directory_with_work(resources, root_session, task, work)?;
    let mut view = native_join(&task.path, Path::new("view"), work)?;
    let mut session = native_join(&view.path, Path::new("scratch"), work)?;
    let mut cwd = match &route {
        Some(route) => native_join(&view.path, Path::new(&route.0), work)?,
        None => {
            let (path, custody) = work.copy_path(&session.path).map_err(native_error)?;
            PiNativeOwnedPath { path, custody }
        }
    };
    let mut session_file = native_join(&session.path, Path::new("session.jsonl"), work)?;
    let mut operation = native_join(&task.path, Path::new("preparation"), work)?;
    let custody = work.retain_owned(std::mem::size_of::<PiNativePaths>() as u64).map_err(native_error)?;
    let mut planned = (PiNativePaths { cwd: cwd.path, session_file: session_file.path,
        session_directory: session.path, view_root: view.path, operation_root: operation.path }, custody);
    for incoming in [&mut cwd.custody, &mut session_file.custody, &mut session.custody,
        &mut view.custody, &mut operation.custody]
    {
        planned.1.absorb(incoming).map_err(native_error)?;
    }
    Ok(planned)
}

fn native_root_route_with_work(
    key: &str, work: &NativeMaterializationWork,
) -> Result<(String, acyclic_fs::NativeAllocation)> {
    use base64::Engine as _;
    work.remaining().map_err(native_error)?;
    work.charge(WorkCounters { items_examined: key.len() as u64, ..WorkCounters::default() }).map_err(native_error)?;
    let reject = || Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
        reason: "Pi native active root has no canonical identity" };
    let mut identity = [0; 16];
    hex::decode_to_slice(key, &mut identity).map_err(|_| reject())?;
    let mut canonical = [0; 32];
    hex::encode_to_slice(identity, &mut canonical).expect("exact root identity hexadecimal extent");
    if key.as_bytes() != canonical { return Err(reject()); }
    // Same existing route_name/compact_id encoding, without their intermediate
    // heap Strings. This reference is not resource or creator authority.
    let mut route = [0_u8; 23];
    route[0] = b'r';
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode_slice(identity, &mut route[1..])
        .expect("exact root identity route extent");
    pi_native_string(std::str::from_utf8(&route).expect("root route uses ASCII encoding"), work)
}

struct PiNativeOwnedPath { path: PathBuf, custody: acyclic_fs::NativeAllocation }

fn native_join(base: &Path, child: &Path, work: &NativeMaterializationWork) -> Result<PiNativeOwnedPath> {
    let (path, custody) = work.join_path(base, child).map_err(native_error)?;
    Ok(PiNativeOwnedPath { path, custody })
}

fn native_task_directory_with_work(
    resources: &super::ServiceResources, root_session: &str, task: acyclic_harness::TaskId, work: &NativeMaterializationWork,
) -> Result<PiNativeOwnedPath> {
    if root_session.is_empty() || !resources.data.is_absolute() {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi native path references require their original absolute service root" });
    }
    work.charge(WorkCounters { bytes_hashed: root_session.len() as u64 + 16, ..WorkCounters::default() }).map_err(native_error)?;
    // The existing service session_directory and planned_native_paths layout:
    // root session hash, then the original task hash. These are references,
    // never a creator receipt, filesystem identity or permission.
    let session_hash = blake3::hash(root_session.as_bytes()).to_hex();
    let task_hash = blake3::hash(&task.into_bytes()).to_hex();
    let sessions = native_join(&resources.data, Path::new("sessions"), work)?;
    let service = native_join(&sessions.path, Path::new(session_hash.as_str()), work)?;
    drop(sessions);
    let agents = native_join(&service.path, Path::new("agents"), work)?;
    drop(service);
    let task = native_join(&agents.path, Path::new(task_hash.as_str()), work)?;
    Ok(task)
}

pub(crate) fn native_view_options_with_work(
    resources: &super::ServiceResources, root_session: &str, task: acyclic_harness::TaskId,
    maximum_volumes: u32, work_per_volume: WorkBudget, work: &NativeMaterializationWork,
) -> Result<(NativeViewOptions, acyclic_fs::NativeAllocation)> {
    if maximum_volumes == 0 || !work_per_volume.has_finite_allowances() {
        return Err(invalid("Pi native options require their original finite composition allowances"));
    }
    let task = native_task_directory_with_work(resources, root_session, task, work)?;
    let view = native_join(&task.path, Path::new("view"), work)?;
    Ok((NativeViewOptions { root: view.path, maximum_volumes, work_per_volume }, view.custody))
}

fn native_service_root<'a>(
    data: &Path, root_session: &str, task: acyclic_harness::TaskId, view: &'a Path, work: &NativeMaterializationWork,
) -> Result<&'a Path> {
    work.charge(WorkCounters { bytes_hashed: root_session.len() as u64 + 16, ..WorkCounters::default() }).map_err(native_error)?;
    let session_hash = blake3::hash(root_session.as_bytes()).to_hex();
    let task_hash = blake3::hash(&task.into_bytes()).to_hex();
    let mut actual = view.strip_prefix(data).map_err(|_| invalid("Pi native root escaped its protected service"))?.components();
    for expected in ["sessions", session_hash.as_str(), "agents", task_hash.as_str(), "view"] {
        if actual.next() != Some(Component::Normal(std::ffi::OsStr::new(expected))) {
            return Err(invalid("Pi native preparation changed its original root session or task namespace"));
        }
    }
    if actual.next().is_some() { return Err(invalid("Pi native preparation selected a descendant rather than its original view")); }
    view.parent().and_then(Path::parent).and_then(Path::parent)
        .ok_or_else(|| invalid("Pi native preparation has no protected original session root"))
}

fn native_root_route(key: &str) -> Result<String> {
    let mut identity = [0; 16];
    hex::decode_to_slice(key, &mut identity).map_err(|_| invalid("Pi native active root has no canonical identity"))?;
    let root = acyclic_fs::WorkspaceRootId::from_bytes(identity);
    if super::root_key(root) != key { return Err(invalid("Pi native active root identity is not canonical")); }
    Ok(super::route_name(root))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PiNativeSourceMode { CallerTool, Scratch }

/// Exact native source selection retained in the original child admission.
/// This is provenance, never a recursive caller credential.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PiNativeSourceCut {
    pub(crate) mode: PiNativeSourceMode,
    pub(crate) parent_session_id: String,
    pub(crate) parent_session_file: Option<String>,
    pub(crate) parent_cwd: String,
    pub(crate) source_leaf_id: Option<String>,
    pub(crate) owner_entry_id: Option<String>,
    pub(crate) parent_leaf_id: Option<String>,
    pub(crate) captured_at: String,
    pub(crate) entries_json: String,
    pub(crate) sha256: String,
}

/// Closed launch graph diagnostic inventory: original derives, camel-case
/// source fields, snake-case mode tags and pinned serde primitive expectations.
/// No Binding/FileRef or arbitrary Value fields occur in this graph.
pub(crate) const PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES: u64 =
    super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "PiNativeLaunchIntent", "struct PiNativeLaunchIntent", "agent_alias", "child_session_id",
        "source", "invocation", "session_path", "session_bytes", "maximum_entries", "maximum_bytes",
        "PiNativeSourceCut", "struct PiNativeSourceCut", "mode", "parentSessionId", "parentSessionFile",
        "parentCwd", "sourceLeafId", "ownerEntryId", "parentLeafId", "capturedAt", "entriesJson", "sha256",
        "PiNativeSourceMode", "enum PiNativeSourceMode", "caller_tool", "scratch",
        "PiNativeInvocation", "struct PiNativeInvocation", "version", "executable", "executable_prefix",
        "extension", "provider_extension", "session_file", "cwd", "provider", "model", "prompt",
        "builtin_tools", "environment", "timeout_ms", "control_timeout_ms", "cancellation_poll_ms",
        "maximum_output_bytes", "maximum_result_bytes", "a string", "path string", "a sequence",
        "a map", "option", "u32", "u64",
    ]);

const NATIVE_HEADER_DIAGNOSTIC_LABEL_BYTES: u64 =
    super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "NativeHeader", "struct NativeHeader", "type", "version", "id", "timestamp", "cwd", "a string", "u32",
    ]);

const NATIVE_CLEANUP_DIAGNOSTIC_LABEL_BYTES: u64 =
    super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "PiNativeCleanupResult", "struct PiNativeCleanupResult", "success", "exit_code", "stdout", "stderr", "stop",
        "PiNativeCleanupStop", "struct PiNativeCleanupStop", "kind", "message", "cleanup_completed",
        "NativeProcessStopKind", "enum NativeProcessStopKind", "timeout", "output_limit", "control_stopped",
        "capture_failed", "a boolean", "i32", "option",
    ]);

const NATIVE_CAPABILITIES_DIAGNOSTIC_LABEL_BYTES: u64 =
    super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&["Capabilities", "a sequence", "a string"]);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeHeader {
    #[serde(rename = "type")]
    kind: String,
    version: u32,
    id: String,
    timestamp: String,
    cwd: String,
}

impl PiNativeSourceCut {
    /// Strictly checks the actual native bytes before Filesystem publication.
    /// Pi itself remains the owner of projection/compaction/tool-exchange rules.
    pub(crate) fn validate_session(
        &self, child: &str, cwd: &Path, bytes: &[u8], maximum_entries: u32, maximum_bytes: u64,
    ) -> Result<()> {
        self.validate_session_original(child, cwd, bytes, maximum_entries, maximum_bytes, None)
    }

    pub(crate) fn validate_session_with_work(
        &self, child: &str, cwd: &Path, bytes: &[u8], maximum_entries: u32, maximum_bytes: u64,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.validate_session_original(child, cwd, bytes, maximum_entries, maximum_bytes, Some(work))
    }

    fn validate_session_original(
        &self, child: &str, cwd: &Path, bytes: &[u8], maximum_entries: u32, maximum_bytes: u64,
        work: Option<&NativeMaterializationWork>,
    ) -> Result<()> {
        if let Some(work) = work { work.remaining().map_err(native_error)?; }
        if maximum_entries == 0 || maximum_bytes == 0 || bytes.len() as u64 > maximum_bytes
            || self.entries_json.len() as u64 > maximum_bytes || !cwd.is_absolute()
            || child.is_empty() || self.parent_session_id.is_empty() || child == self.parent_session_id
            || self.captured_at.is_empty() || !Path::new(&self.parent_cwd).is_absolute()
        {
            return Err(invalid("Pi native session differs from its original source cut or bounds"));
        }
        let mut digest = [0_u8; 64];
        if let Some(work) = work {
            work.charge(WorkCounters { bytes_hashed: self.entries_json.len() as u64, ..Default::default() }).map_err(native_error)?;
        }
        hex::encode_to_slice(Sha256::digest(self.entries_json.as_bytes()), &mut digest)
            .expect("SHA256 hexadecimal output has its fixed exact extent");
        if self.sha256.as_bytes() != digest {
            return Err(invalid("Pi native session differs from its original source digest"));
        }
        let selected = native_cut_decode::<Vec<Value>>(self.entries_json.as_bytes(), maximum_bytes,
            std::mem::size_of::<Vec<Value>>() as u64, JsonValueSchema::Root, 0, work)?;
        if selected.value.len() as u64 > u64::from(maximum_entries) {
            return Err(invalid("Pi native source cut exceeds its entry allowance"));
        }
        match self.mode {
            PiNativeSourceMode::CallerTool if self.owner_entry_id.as_deref().is_some_and(|owner| !owner.is_empty())
                && self.source_leaf_id.as_deref().is_some_and(|leaf| !leaf.is_empty()) => {}
            PiNativeSourceMode::Scratch if self.owner_entry_id.is_none() && self.source_leaf_id.is_none()
                && self.parent_leaf_id.is_none() && selected.value.is_empty() => {}
            _ => return Err(invalid("Pi native source mode differs from its authentic SDK selection")),
        }
        let mut previous: Option<&str> = None;
        let ids = match work {
            Some(work) => {
                let ids = pi_native_vec(selected.value.len(), work)?;
                NativeCutDecoded { value: ids.0, _custody: Some(ids.1) }
            }
            None => NativeCutDecoded { value: Vec::with_capacity(selected.value.len()), _custody: None },
        };
        let mut ids = ids;
        for entry in &selected.value {
            let id = entry.get("id").and_then(Value::as_str)
                .ok_or_else(|| invalid("Pi native source entry has no identity"))?;
            let parent = entry.get("parentId").ok_or_else(|| invalid("Pi native source entry has no parent"))?;
            if id.is_empty() || entry.get("type").and_then(Value::as_str).is_none_or(|kind| kind == "session")
                || (previous.is_none() && !parent.is_null())
                || (previous.is_some() && parent.as_str() != previous)
                || Some(id) == self.owner_entry_id.as_deref()
            {
                return Err(invalid("Pi native source is not the exact selected parent chain"));
            }
            previous = Some(id);
            ids.value.push(id);
        }
        match work {
            Some(work) => native_sort_entry_ids(&mut ids.value, work)?,
            None => ids.value.sort_unstable(),
        }
        if ids.value.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid("Pi native source repeats an entry identity"));
        }
        if previous != self.parent_leaf_id.as_deref() {
            return Err(invalid("Pi native source leaf differs"));
        }
        let text = std::str::from_utf8(bytes).map_err(|_| invalid("Pi native session is not UTF-8"))?;
        if !text.ends_with('\n') || text.contains('\r') {
            return Err(invalid("Pi child session must be complete native JSONL"));
        }
        let mut lines = text[..text.len() - 1].split('\n');
        let header = native_cut_decode::<NativeHeader>(
            lines.next().ok_or_else(|| invalid("Pi child header is absent"))?.as_bytes(), maximum_bytes,
            std::mem::size_of::<NativeHeader>() as u64, JsonValueSchema::None, NATIVE_HEADER_DIAGNOSTIC_LABEL_BYTES, work)?;
        if header.value.kind != "session" || header.value.version != 3 || header.value.id != child
            || header.value.timestamp != self.captured_at || Path::new(&header.value.cwd) != cwd
        {
            return Err(invalid("Pi child header differs from its prepared native identity"));
        }
        for entry in &selected.value {
            let actual = native_cut_decode::<Value>(
                lines.next().ok_or_else(|| invalid("Pi child lost a source entry"))?.as_bytes(), maximum_bytes,
                std::mem::size_of::<Value>() as u64, JsonValueSchema::Root, 0, work)?;
            if &actual.value != entry {
                return Err(invalid("Pi child entry differs from its original native source"));
            }
        }
        if lines.next().is_some() {
            return Err(invalid("Pi child session contains entries outside its source cut"));
        }
        Ok(())
    }
}

struct NativeCutDecoded<T> { value: T, _custody: Option<acyclic_fs::NativeAllocation> }


fn native_cut_decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8], maximum: u64, audited_fixed_bytes: u64, schema: JsonValueSchema,
    diagnostic_label_bytes: u64, work: Option<&NativeMaterializationWork>,
) -> Result<NativeCutDecoded<T>> {
    match work {
        Some(work) => {
            let decoded = super::pi_root_provider::guarded_pi_decode(
                bytes, maximum, audited_fixed_bytes, schema, diagnostic_label_bytes, work)?;
            Ok(NativeCutDecoded { value: decoded.0, _custody: Some(decoded.1) })
        }
        None => Ok(NativeCutDecoded { value: serde_json::from_slice(bytes).map_err(decode_error)?, _custody: None }),
    }
}

/// In-place heap ordering permits the original quota/cancellation error to stop
/// before the next comparison, without a fallible comparator lying to sort.
fn native_sort_entry_ids(ids: &mut [&str], work: &NativeMaterializationWork) -> Result<()> {
    fn sift(ids: &mut [&str], mut root: usize, end: usize, work: &NativeMaterializationWork) -> Result<()> {
        loop {
            let mut child = root.checked_mul(2).and_then(|value| value.checked_add(1))
                .ok_or_else(|| native_error(acyclic_fs::WorkError::Overflow.into()))?;
            if child >= end { return Ok(()); }
            if child + 1 < end {
                work.charge(WorkCounters { items_examined: 1, ..Default::default() }).map_err(native_error)?;
                if ids[child] < ids[child + 1] { child += 1; }
            }
            work.charge(WorkCounters { items_examined: 1, ..Default::default() }).map_err(native_error)?;
            if ids[root] >= ids[child] { return Ok(()); }
            ids.swap(root, child);
            root = child;
        }
    }
    let length = ids.len();
    for root in (0..length / 2).rev() { sift(ids, root, length, work)?; }
    for end in (1..length).rev() { ids.swap(0, end); sift(ids, 0, end, work)?; }
    Ok(())
}

/// Captured once by the original root SDK bootstrap and kept only in the
/// protected Pi owner file. It contains no task grant or journal signing key.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiNativeHostRuntime {
    pub(crate) version: String,
    pub(crate) executable: PathBuf,
    pub(crate) executable_prefix: Vec<String>,
    pub(crate) extension: PathBuf,
    pub(crate) provider_extension: Option<PathBuf>,
    pub(crate) environment: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiNativeCaptureLimits {
    pub(crate) timeout_ms: u32,
    pub(crate) control_timeout_ms: u32,
    pub(crate) cancellation_poll_ms: u32,
    pub(crate) maximum_output_bytes: u32,
    pub(crate) maximum_result_bytes: u32,
}

/// Fixed native policy captured in the immutable seed, never a spawn override.
/// Recursive startup/RPC has a three-minute wall allowance; capture remains
/// below both the original root's file admission and its canonical payload cap.
pub(crate) fn capture_limits(actual_root_limits: acyclic_harness::conversation::Limits, maximum_payload: u64) -> Result<PiNativeCaptureLimits> {
    actual_root_limits.validate()?;
    let result = actual_root_limits.file_bytes.min(maximum_payload)
        .min(super::MAXIMUM_ADAPTER_STATE_BYTES).min(1_048_576);
    let output = result.checked_sub(2048).map(|remaining| (remaining / 4).min(262_144))
        .filter(|bytes| *bytes > 0).ok_or_else(|| invalid("original Pi root bounds cannot admit a native process result"))?;
    Ok(PiNativeCaptureLimits { timeout_ms: 180_000, control_timeout_ms: 5_000,
        cancellation_poll_ms: 250, maximum_output_bytes: u32::try_from(output).map_err(|error| invalid(&error.to_string()))?,
        maximum_result_bytes: u32::try_from(result).map_err(|error| invalid(&error.to_string()))? })
}

fn runtime_validation_step(work: Option<&NativeMaterializationWork>, items: usize) -> Result<()> {
    if let Some(work) = work {
        work.charge(WorkCounters { items_examined: items as u64, ..WorkCounters::default() }).map_err(native_error)?;
    }
    Ok(())
}

impl PiNativeHostRuntime {
    pub(crate) fn sanitize(mut self) -> Result<Self> {
        self.environment.retain(|key, _| !is_caller_environment(key));
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.validate_original(None)
    }

    pub(crate) fn validate_with_native_work(&self, work: &NativeMaterializationWork) -> Result<()> {
        work.remaining().map_err(native_error)?;
        self.validate_original(Some(work))
    }

    fn validate_original(&self, work: Option<&NativeMaterializationWork>) -> Result<()> {
        let reject = |reason| match work {
            Some(_) => Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason },
            None => invalid(reason),
        };
        let mut environment_bytes = 0_u64;
        for (index, (key, value)) in self.environment.iter().enumerate() {
            runtime_validation_step(work, 1)?;
            runtime_validation_step(work, key.len())?;
            runtime_validation_step(work, value.len())?;
            environment_bytes = environment_bytes.checked_add(key.len() as u64).and_then(|bytes| bytes.checked_add(value.len() as u64))
                .ok_or_else(|| reject("Pi host environment size overflow"))?;
            if key.is_empty() || key.contains(['=', '\0']) || value.contains('\0') || is_caller_environment(key)
                || environment_bytes > super::MAXIMUM_ADAPTER_STATE_BYTES
                || (cfg!(windows) && {
                    let mut duplicate = false;
                    for prior in self.environment.keys().take(index) {
                        runtime_validation_step(work, prior.len())?;
                        runtime_validation_step(work, key.len())?;
                        if prior.eq_ignore_ascii_case(key) { duplicate = true; break; }
                    }
                    duplicate
                })
            {
                return Err(reject("Pi original host environment is invalid or contains a recursive credential"));
            }
        }
        runtime_validation_step(work, self.version.len())?;
        for path in [&self.executable, &self.extension] {
            runtime_validation_step(work, path.as_os_str().len())?;
        }
        for path in &self.executable_prefix { runtime_validation_step(work, path.len())?; }
        if let Some(path) = &self.provider_extension { runtime_validation_step(work, path.as_os_str().len())?; }
        if self.version != PI_NATIVE_VERSION || !self.executable.is_absolute() || !self.extension.is_absolute()
            || has_parent(&self.executable) || has_parent(&self.extension) || self.environment.is_empty()
            || self.executable_prefix.len() > 1
            || self.executable_prefix.first().is_some_and(|path| !Path::new(path).is_absolute() || has_parent(Path::new(path)) || path.contains('\0'))
            || self.provider_extension.as_ref().is_some_and(|path| !path.is_absolute() || has_parent(path))
        {
            return Err(reject("Pi original runtime must select the pinned absolute executable and extension"));
        }
        Ok(())
    }

    /// Copy the immutable protected runtime directly, paying known metadata and
    /// actual owned buffers before construction on the original ledger.
    pub(crate) fn clone_with_native_work(
        &self, work: &NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        work.remaining().map_err(native_error)?;
        self.validate_with_native_work(work)?;
        let mut version = pi_native_string(&self.version, work)?;
        let mut executable = work.copy_path(&self.executable).map_err(native_error)?;
        let mut extension = work.copy_path(&self.extension).map_err(native_error)?;
        let mut provider_extension = match &self.provider_extension {
            Some(path) => {
                let copied = work.copy_path(path).map_err(native_error)?;
                (Some(copied.0), copied.1)
            }
            None => (None, work.retain_owned(0).map_err(native_error)?),
        };
        let mut prefix = pi_native_vec::<String>(self.executable_prefix.len(), work)?;
        for argument in &self.executable_prefix {
            let mut copied = pi_native_string(argument, work)?;
            prefix.0.push(copied.0);
            prefix.1.absorb(&mut copied.1).map_err(native_error)?;
        }
        let mut environment = (BTreeMap::new(), work.retain_owned(0).map_err(native_error)?);
        for (key, value) in &self.environment {
            let mut slot = work.retain_owned(std::mem::size_of::<(String, String)>() as u64).map_err(native_error)?;
            let mut key = pi_native_string(key, work)?;
            let mut value = pi_native_string(value, work)?;
            environment.0.insert(key.0, value.0);
            environment.1.absorb(&mut slot).map_err(native_error)?;
            environment.1.absorb(&mut key.1).map_err(native_error)?;
            environment.1.absorb(&mut value.1).map_err(native_error)?;
        }
        let custody = work.retain_owned(std::mem::size_of::<Self>() as u64).map_err(native_error)?;
        let mut copied = (Self { version: version.0, executable: executable.0, executable_prefix: prefix.0,
            extension: extension.0, provider_extension: provider_extension.0, environment: environment.0 }, custody);
        for incoming in [&mut version.1, &mut executable.1, &mut extension.1,
            &mut provider_extension.1, &mut prefix.1, &mut environment.1]
        {
            copied.1.absorb(incoming).map_err(native_error)?;
        }
        Ok(copied)
    }



    #[allow(clippy::too_many_arguments, reason = "original immutable host and separately captured SDK model remain borrowed")]
    pub(crate) fn intent_invocation_with_work(
        &self, session_file: &Path, cwd: &Path, provider: &str, model: &str, prompt: &str,
        capture: PiNativeCaptureLimits, work: &NativeMaterializationWork,
    ) -> Result<(PiNativeInvocation, acyclic_fs::NativeAllocation)> {
        work.remaining().map_err(native_error)?;
        self.validate_with_native_work(work)?;
        let caller = BTreeMap::new();
        let projection = PiBoundInvocation { version: &self.version, executable: &self.executable,
            executable_prefix: &self.executable_prefix, extension: &self.extension,
            provider_extension: self.provider_extension.as_deref(), session_file, cwd, provider, model, prompt,
            builtin_tools: ["read", "write", "edit"], environment: PiCallerEnvironment {
                host: &self.environment, caller: &caller },
            timeout_ms: capture.timeout_ms, control_timeout_ms: capture.control_timeout_ms,
            cancellation_poll_ms: capture.cancellation_poll_ms, maximum_output_bytes: capture.maximum_output_bytes,
            maximum_result_bytes: capture.maximum_result_bytes };
        let bytes = super::pi_root_provider::guarded_pi_bytes(&projection, super::MAXIMUM_ADAPTER_STATE_BYTES, work)?;
        super::pi_root_provider::guarded_pi_decode(&bytes.0, super::MAXIMUM_ADAPTER_STATE_BYTES,
            std::mem::size_of::<PiNativeInvocation>() as u64, JsonValueSchema::None, PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES, work)
    }


    pub(crate) fn bind_invocation_with_work(
        &self, intent: &PiNativeLaunchIntent, caller_environment: &BTreeMap<String, String>,
        work: &NativeMaterializationWork,
    ) -> Result<(PiNativeInvocation, acyclic_fs::NativeAllocation)> {
        work.remaining().map_err(native_error)?;
        self.validate_with_native_work(work)?;
        intent.validate_with_work(work)?;
        require_caller_environment(caller_environment)?;
        let persisted = &intent.invocation;
        if persisted.version != self.version || persisted.executable != self.executable
            || persisted.executable_prefix != self.executable_prefix || persisted.extension != self.extension
            || persisted.provider_extension != self.provider_extension || persisted.environment != self.environment
            || !persisted.builtin_tools.iter().map(String::as_str).eq(["read", "write", "edit"])
        {
            return Err(invalid("Pi immutable child seed changed its original protected native host settings"));
        }
        let projection = PiBoundInvocation { version: &persisted.version, executable: &persisted.executable,
            executable_prefix: &persisted.executable_prefix, extension: &persisted.extension,
            provider_extension: persisted.provider_extension.as_deref(), session_file: &persisted.session_file,
            cwd: &persisted.cwd, provider: &persisted.provider, model: &persisted.model, prompt: &persisted.prompt,
            builtin_tools: &persisted.builtin_tools, environment: PiCallerEnvironment {
                host: &persisted.environment, caller: caller_environment },
            timeout_ms: persisted.timeout_ms, control_timeout_ms: persisted.control_timeout_ms,
            cancellation_poll_ms: persisted.cancellation_poll_ms, maximum_output_bytes: persisted.maximum_output_bytes,
            maximum_result_bytes: persisted.maximum_result_bytes };
        let bytes = super::pi_root_provider::guarded_pi_bytes(&projection, super::MAXIMUM_ADAPTER_STATE_BYTES, work)?;
        super::pi_root_provider::guarded_pi_decode(&bytes.0, super::MAXIMUM_ADAPTER_STATE_BYTES,
            std::mem::size_of::<PiNativeInvocation>() as u64, JsonValueSchema::None, PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES, work)
    }
}

#[derive(Serialize)]
struct PiBoundInvocation<'a, T> {
    version: &'a str, executable: &'a Path, executable_prefix: &'a [String], extension: &'a Path,
    provider_extension: Option<&'a Path>, session_file: &'a Path, cwd: &'a Path, provider: &'a str,
    model: &'a str, prompt: &'a str, builtin_tools: T, environment: PiCallerEnvironment<'a>,
    timeout_ms: u32, control_timeout_ms: u32, cancellation_poll_ms: u32,
    maximum_output_bytes: u32, maximum_result_bytes: u32,
}

struct PiCallerEnvironment<'a> { host: &'a BTreeMap<String, String>, caller: &'a BTreeMap<String, String> }
impl Serialize for PiCallerEnvironment<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.host.len() + self.caller.len()))?;
        for (key, value) in self.host.iter().chain(self.caller.iter()) { map.serialize_entry(key, value)?; }
        map.end()
    }
}

fn require_caller_environment(environment: &BTreeMap<String, String>) -> Result<()> {
    if environment.get("ACYCLIC_PI_CALLER").is_none_or(String::is_empty)
        || environment.iter().any(|(key, value)| !is_caller_environment(key) || value.is_empty() || value.contains('\0'))
    {
        return Err(invalid("Pi native child needs its separately authenticated original caller credential"));
    }
    Ok(())
}

fn is_caller_environment(key: &str) -> bool {
    let prefix = "ACYCLIC_PI_CALLER_";
    ["ACYCLIC_PI_CALLER", "ACYCLIC_PI_ROUTE", "ACYCLIC_PI_TASK", "ACYCLIC_PI_TOKEN", "ACYCLIC_PI_SCOPE", "ACYCLIC_PI_LEASE"]
        .iter().any(|reserved| key.eq_ignore_ascii_case(reserved))
        || key.get(..prefix.len()).is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

/// Host-selected complete invocation. The protected service authenticates the
/// scoped child caller before supplying its environment; IDs/cwd are not proof.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiNativeInvocation {
    pub(crate) version: String,
    pub(crate) executable: PathBuf,
    /// Empty for a native Pi executable; one absolute bundled CLI for Node.
    pub(crate) executable_prefix: Vec<String>,
    pub(crate) extension: PathBuf,
    pub(crate) provider_extension: Option<PathBuf>,
    pub(crate) session_file: PathBuf,
    pub(crate) cwd: PathBuf,
    pub(crate) provider: String,
    pub(crate) model: String,
    pub(crate) prompt: String,
    /// Exact host-approved native filesystem tools; never ambient defaults.
    pub(crate) builtin_tools: Vec<String>,
    pub(crate) environment: BTreeMap<String, String>,
    pub(crate) timeout_ms: u32,
    pub(crate) control_timeout_ms: u32,
    pub(crate) cancellation_poll_ms: u32,
    pub(crate) maximum_output_bytes: u32,
    pub(crate) maximum_result_bytes: u32,
}

impl PiNativeInvocation {
    pub(crate) fn request_with_work(&self, binding: &PiTaskBinding, manifest: &NativeViewManifest,
        work: &NativeMaterializationWork) -> Result<(NativeProcessRequest, acyclic_fs::NativeAllocation)> {
        work.remaining().map_err(native_error)?;
        binding.validate(Some(&binding.native_session_id), binding.project_volumes.keys().map(String::as_str), false)?;
        manifest.validate()?;
        let scratch = manifest.volumes.iter().find(|mapping| mapping.volume == binding.scratch.volume)
            .ok_or_else(|| invalid("Pi native namespace omits original scratch"))?;
        if !matches!(binding.scratch.allocation, PiScratchAllocation::Created(_))
            || !scratch.writable || !self.session_file.starts_with(&scratch.path)
            || self.session_file == scratch.path || !self.cwd.starts_with(&manifest.options.root)
            || !manifest.volumes.iter().any(|mapping| self.cwd.starts_with(&mapping.path))
            || self.version != PI_NATIVE_VERSION || !self.executable.is_absolute() || !self.extension.is_absolute()
            || !self.session_file.is_absolute() || !self.cwd.is_absolute()
            || self.provider_extension.as_ref().is_some_and(|path| !path.is_absolute())
            || self.executable_prefix.len() > 1
            || self.executable_prefix.first().is_some_and(|prefix| !Path::new(prefix).is_absolute())
            || [&self.executable, &self.extension, &self.session_file, &self.cwd].iter().any(|path| has_parent(path))
            || self.provider.is_empty() || self.model.is_empty() || self.prompt.is_empty()
            || self.environment.is_empty()
            || self.environment.iter().any(|(key, value)| key.is_empty() || key.contains(['=', '\0']) || value.contains('\0'))
            || self.builtin_tools.iter().enumerate().any(|(index, name)| !matches!(name.as_str(), "read" | "write" | "edit")
                || self.builtin_tools[..index].contains(name))
        {
            return Err(invalid("Pi invocation differs from the pinned executable, session or namespace"));
        }
        if manifest.volumes.len() != binding.project_volumes.len() + 1
            || binding.project_volumes.values().any(|volume| !manifest.volumes.iter().any(|mapping| &mapping.volume == volume
                && matches!(&mapping.namespace, NativeVolumeNamespace::LazyMount { .. })))
        {
            return Err(invalid("Pi native namespace differs from its admitted volume selection"));
        }
        let projection = PiNativeRequestProjection {
            executable: &self.executable, argv: PiNativeArgv(self), cwd: &self.cwd, environment: &self.environment,
            timeout_ms: self.timeout_ms, control_timeout_ms: self.control_timeout_ms,
            cancellation_poll_ms: self.cancellation_poll_ms, maximum_output_bytes: self.maximum_output_bytes,
            maximum_result_bytes: self.maximum_result_bytes, standard_input: Some(self.prompt.as_bytes()), view: manifest,
        };
        let encoded = super::pi_root_provider::guarded_pi_bytes(&projection, super::MAXIMUM_ADAPTER_STATE_BYTES, work)?;
        let request = NativeProcessRequest::decode_with_native_work(&encoded.0, work)?;
        if !request.0.has_valid_capture_allowances() || request.0.argv.iter().any(|value| value.contains('\0')) {
            return Err(invalid("Pi native capture allowances or argv are invalid"));
        }
        Ok(request)
    }
}

#[derive(Serialize)]
struct PiNativeRequestProjection<'a> {
    executable: &'a Path,
    argv: PiNativeArgv<'a>,
    cwd: &'a Path,
    environment: &'a BTreeMap<String, String>,
    timeout_ms: u32,
    control_timeout_ms: u32,
    cancellation_poll_ms: u32,
    maximum_output_bytes: u32,
    maximum_result_bytes: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    standard_input: Option<&'a [u8]>,
    view: &'a NativeViewManifest,
}

struct PiNativeArgv<'a>(&'a PiNativeInvocation);
impl Serialize for PiNativeArgv<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let invocation = self.0;
        let count = invocation.executable_prefix.len() + 13
            + if invocation.builtin_tools.is_empty() { 1 } else { 2 }
            + if invocation.provider_extension.is_some() { 2 } else { 0 };
        let mut sequence = serializer.serialize_seq(Some(count))?;
        for prefix in &invocation.executable_prefix { sequence.serialize_element(prefix)?; }
        for argument in ["--print", "--mode", "json", "--session", native_argv_path::<S::Error>(&invocation.session_file)?,
            "--provider", &invocation.provider, "--model", &invocation.model, "--no-extensions", "--no-mcp",
            "-e", native_argv_path::<S::Error>(&invocation.extension)?]
        { sequence.serialize_element(argument)?; }
        if invocation.builtin_tools.is_empty() {
            sequence.serialize_element("--no-builtin-tools")?;
        } else {
            let mut tools = [0_u8; 15];
            let mut length = 0;
            for (index, tool) in invocation.builtin_tools.iter().enumerate() {
                if index != 0 { tools[length] = b','; length += 1; }
                tools[length..length + tool.len()].copy_from_slice(tool.as_bytes());
                length += tool.len();
            }
            sequence.serialize_element("--tools")?;
            sequence.serialize_element(std::str::from_utf8(&tools[..length])
                .expect("validated builtin names are ASCII"))?;
        }
        if let Some(extension) = &invocation.provider_extension {
            sequence.serialize_element("-e")?;
            sequence.serialize_element(native_argv_path::<S::Error>(extension)?)?;
        }
        sequence.end()
    }
}

fn native_argv_path<E: serde::ser::Error>(path: &Path) -> std::result::Result<&str, E> {
    path.to_str().ok_or_else(|| E::custom("Pi invocation path is not UTF-8"))
}

/// Seeded immutable input consumed only through the actual created child's
/// TaskContext. Parent and child native IDs are intentionally separate fields.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiNativeLaunchIntent {
    /// Original caller-selected routing label, not Task/Agent/session authority.
    pub(crate) agent_alias: String,
    pub(crate) child_session_id: String,
    pub(crate) source: PiNativeSourceCut,
    pub(crate) invocation: PiNativeInvocation,
    pub(crate) session_path: String,
    pub(crate) session_bytes: String,
    pub(crate) maximum_entries: u32,
    pub(crate) maximum_bytes: u64,
}

impl PiNativeLaunchIntent {
    pub(crate) async fn load(context: &TaskContext, reference: &FileRef) -> Result<Self> {
        if context.durable_task_id().is_none() || reference.descriptor().media_type() != "application/json" {
            return Err(invalid("Pi launch input requires its actual durable child content binding"));
        }
        let bytes = context.read_file(reference).await?;
        let intent: Self = serde_json::from_slice(&bytes).map_err(decode_error)?;
        intent.validate()?;
        Ok(intent)
    }

    pub(crate) async fn load_with_work(
        context: &TaskContext, reference: &FileRef, work: &NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        if context.durable_task_id().is_none() || reference.descriptor().media_type() != "application/json" {
            return Err(invalid("Pi launch input requires its actual durable child content binding"));
        }
        let bytes = context.read_file_with_native_work(reference, work).await?;
        let maximum = context.scope().limits().file_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
        let intent = super::pi_root_provider::guarded_pi_decode::<Self>(
            bytes.as_ref(), maximum, std::mem::size_of::<Self>() as u64, JsonValueSchema::None, PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES, work)?;
        intent.0.validate_with_work(work)?;
        Ok(intent)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.validate_original(None)
    }

    pub(crate) fn validate_with_work(&self, work: &NativeMaterializationWork) -> Result<()> {
        self.validate_original(Some(work))
    }

    fn validate_original(&self, work: Option<&NativeMaterializationWork>) -> Result<()> {
        if self.agent_alias.is_empty() || self.agent_alias.len() > 128
            || !self.agent_alias.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
        {
            return Err(invalid("Pi original child alias must match [A-Za-z0-9_.-]{1,128}"));
        }
        if self.maximum_bytes > super::MAXIMUM_ADAPTER_STATE_BYTES || self.session_path.is_empty()
            || Path::new(&self.session_path).is_absolute()
            || Path::new(&self.session_path).components().any(|part| !matches!(part, Component::Normal(_)))
        {
            return Err(invalid("Pi session path or original context allowance is invalid"));
        }
        self.source.validate_session_original(&self.child_session_id, &self.invocation.cwd,
            self.session_bytes.as_bytes(), self.maximum_entries, self.maximum_bytes, work)
    }

    /// The approved CLI locator and staged SDK bytes must name the same leaf.
    pub(crate) fn request_with_work(&self, binding: &PiTaskBinding, manifest: &NativeViewManifest,
        work: &NativeMaterializationWork) -> Result<(NativeProcessRequest, acyclic_fs::NativeAllocation)> {
        self.validate_with_work(work)?;
        let scratch = manifest.volumes.iter().find(|mapping| mapping.volume == binding.scratch.volume)
            .ok_or_else(|| invalid("Pi launch has no original scratch mapping"))?;
        if self.invocation.session_file.strip_prefix(&scratch.path).ok() != Some(Path::new(&self.session_path)) {
            return Err(invalid("Pi staged native session differs from the approved CLI session leaf"));
        }
        self.invocation.request_with_work(binding, manifest, work)
    }

    /// Publishes on the caller's original work and retains the actual FileRef
    /// ownership through the prepared generation's final consumer.
    pub(crate) async fn publish_session_with_work(
        &self, context: &TaskContext, operation: OperationId, work: &NativeMaterializationWork,
    ) -> Result<(FileRef, acyclic_fs::NativeAllocation)> {
        self.validate_with_work(work)?;
        if context.durable_task_id().is_none() {
            return Err(invalid("Pi session publication requires its admitted child"));
        }
        context.stage_file_once_with_native_work(operation, &self.session_path, self.session_bytes.as_bytes(),
            "application/x-ndjson", "Pi child session", work).await
    }
}

/// Child task content is bound through created-resource access, not a root
/// signed scope pre-granted to guessed future child volume names.
pub(crate) async fn stage_context_command_with_work(
    context: &TaskContext, command: OperationId, request: &NativeProcessRequest, work: &NativeMaterializationWork,
) -> Result<(WorkflowCommand, TaskEffectPlan, acyclic_fs::NativeAllocation)> {
    if context.durable_task_id().is_none() { return Err(invalid("Pi native launch requires its original durable TaskContext")); }
    let maximum = context.scope().limits().file_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
    let mut staged = PiNativeStagedArtifacts {
        request: None, schema: None, plan: None, payload: None,
        custody: work.retain_owned(0).map_err(native_error)?,
    };
    let (reference, mut custody) = stage_context_artifact_with_work(
        context, command, "request", request, "application/json", maximum, work).await?;
    staged.request = Some(reference);
    staged.custody.absorb(&mut custody).map_err(native_error)?;
    let (reference, mut custody) = stage_context_artifact_with_work(
        context, command, "schema", &result_schema()?, "application/schema+json", maximum, work).await?;
    staged.schema = Some(reference);
    staged.custody.absorb(&mut custody).map_err(native_error)?;
    let mut provider = pi_native_string(PI_NATIVE_PROVIDER, work)?;
    staged.custody.absorb(&mut provider.1).map_err(native_error)?;
    let mut effect_kind = pi_native_string(NATIVE_PROCESS_EFFECT_KIND, work)?;
    staged.custody.absorb(&mut effect_kind.1).map_err(native_error)?;
    staged.plan = Some(TaskEffectPlan {
        provider: provider.0, guarantee: EffectGuarantee::AtMostOnce, effect_kind: effect_kind.0,
        request: staged.request.take().ok_or_else(|| invalid("Pi request custody was not staged"))?,
        result_schema: staged.schema.take().ok_or_else(|| invalid("Pi schema custody was not staged"))?,
    });
    let (reference, mut custody) = stage_context_artifact_with_work(
        context, command, "plan", staged.plan.as_ref().ok_or_else(|| invalid("Pi plan custody was not staged"))?,
        "application/json", maximum, work).await?;
    staged.payload = Some(reference);
    staged.custody.absorb(&mut custody).map_err(native_error)?;
    let mut kind = pi_native_string(EFFECT_TASK_COMMAND_KIND, work)?;
    staged.custody.absorb(&mut kind.1).map_err(native_error)?;
    let workflow = WorkflowCommand { operation_id: command, kind: kind.0,
        payload: staged.payload.take().ok_or_else(|| invalid("Pi command custody was not staged"))? };
    let plan = staged.plan.take().ok_or_else(|| invalid("Pi plan custody was not staged"))?;
    Ok((workflow, plan, staged.custody))
}

// Field order destroys every physical DTO before its original payload guard,
// including an interrupted request/schema/plan publication.
struct PiNativeStagedArtifacts {
    request: Option<FileRef>,
    schema: Option<FileRef>,
    plan: Option<TaskEffectPlan>,
    payload: Option<FileRef>,
    custody: acyclic_fs::NativeAllocation,
}

async fn stage_context_artifact_with_work(
    context: &TaskContext, command: OperationId, role: &str, value: &impl Serialize,
    media_type: &str, maximum: u64, work: &NativeMaterializationWork,
) -> Result<(FileRef, acyclic_fs::NativeAllocation)> {
    let encoded = guarded_native_bytes(value, maximum, work, false)?;
    let operation = native_artifact_operation_with_work(command, role, work)?;
    let mut path = PiNativeArtifactPath { bytes: [0; 96], length: 0 };
    std::fmt::write(&mut path, format_args!("pi-native/{command}/{role}.json"))
        .map_err(|_| invalid("Pi native artifact locator exceeds its fixed original bound"))?;
    let path = std::str::from_utf8(&path.bytes[..path.length])
        .map_err(|_| invalid("Pi native artifact locator is not UTF-8"))?;
    let result = context.stage_file_once_with_native_work(operation, path, &encoded.bytes, media_type, role, work).await;
    drop(encoded);
    result
}

struct PiNativeArtifactPath { bytes: [u8; 96], length: usize }
impl std::fmt::Write for PiNativeArtifactPath {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        let end = self.length.checked_add(value.len()).ok_or(std::fmt::Error)?;
        let destination = self.bytes.get_mut(self.length..end).ok_or(std::fmt::Error)?;
        destination.copy_from_slice(value.as_bytes());
        self.length = end;
        Ok(())
    }
}

fn native_artifact_operation_with_work(
    command: OperationId, role: &str, work: &NativeMaterializationWork,
) -> Result<OperationId> {
    let digest = native_digest_with_work(&("acyclic.pi/native-content/v1", command, role), 1024, work)?;
    let mut operation = [0; 16];
    operation.copy_from_slice(&digest[..16]);
    Ok(OperationId::from_bytes(operation))
}

fn native_digest_with_work(value: &impl Serialize, maximum: u64, work: &NativeMaterializationWork) -> Result<[u8; 32]> {
    let encoded = guarded_native_bytes(value, maximum, work, true)?;
    work.charge(WorkCounters { bytes_hashed: encoded.bytes.len() as u64, ..WorkCounters::default() }).map_err(native_error)?;
    let digest = *blake3::hash(&encoded.bytes).as_bytes();
    drop(encoded);
    Ok(digest)
}

struct PiNativeEncodedBytes { bytes: Vec<u8>, _custody: acyclic_fs::NativeAllocation }

fn guarded_native_bytes(
    value: &impl Serialize, maximum: u64, work: &NativeMaterializationWork, canonical: bool,
) -> Result<PiNativeEncodedBytes> {
    use acyclic_harness::{JsonBounds, JsonCodecCharge, canonical_json_bytes_bounded, json_bytes_bounded_trusted};
    let remaining = work.remaining().map_err(native_error)?;
    let mut custody = work.retain_owned(0).map_err(native_error)?;
    let mut parser_quota: Option<acyclic_fs::NativeAllocation> = None;
    let bounds = JsonBounds { maximum_bytes: maximum.min(remaining.bytes_encoded),
        maximum_items: remaining.items_examined, maximum_depth: 64 };
    let mut charge = |charge| {
        if let JsonCodecCharge::Release { bytes } = charge { return custody.release(bytes).map_err(native_error); }
        work.cancellation().check().map_err(|_| native_error(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        match charge {
            JsonCodecCharge::Work { bytes, items } => work.charge(WorkCounters {
                bytes_encoded: bytes, items_examined: items, ..WorkCounters::default()
            }).map_err(native_error),
            JsonCodecCharge::Copy { bytes } => work.charge(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() }).map_err(native_error),
            JsonCodecCharge::Read { bytes } => work.charge(WorkCounters { source_bytes_read: bytes, ..WorkCounters::default() }).map_err(native_error),
            JsonCodecCharge::Retain { bytes } => {
                if let Some(mut quota) = parser_quota.take() {
                    let retained = custody.bytes().checked_add(bytes)
                        .ok_or_else(|| native_error(acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)))?;
                    custody.absorb(&mut quota).map_err(native_error)?;
                    custody.substitute(retained).map_err(native_error)
                } else {
                    custody.resize_retained(custody.bytes().checked_add(bytes)
                        .ok_or_else(|| native_error(acyclic_fs::WorkError::Overflow.into()))?).map_err(native_error)
                }
            },
            JsonCodecCharge::Admission { maximum_owned_bytes } => {
                let mut quota = work.retain_owned(0).map_err(native_error)?;
                quota.resize_retained(maximum_owned_bytes).map_err(native_error)?;
                parser_quota = Some(quota);
                Ok(())
            },
            JsonCodecCharge::AllocationOperation { .. } => work.charge(WorkCounters {
                allocation_operations: 1, ..WorkCounters::default()
            }).map_err(native_error),
            JsonCodecCharge::Release { .. } => unreachable!(),
        }
    };
    let result = if canonical { canonical_json_bytes_bounded(value, bounds, &mut charge) }
        else { json_bytes_bounded_trusted(value, bounds, &mut charge) };
    drop(charge);
    drop(parser_quota);
    match result {
        Ok(bytes) => Ok(PiNativeEncodedBytes { bytes, _custody: custody }),
        Err(Error::NativeWork { cause, custody: None }) => Err(Error::NativeWork { cause, custody: Some(custody) }),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
fn native_artifact_operation(command: OperationId, role: &str) -> Result<OperationId> {
    let digest = acyclic_harness::contract::canonical_json_digest(&("acyclic.pi/native-content/v1", command, role))?;
    let mut operation = [0; 16];
    operation.copy_from_slice(&digest[..16]);
    Ok(OperationId::from_bytes(operation))
}

#[cfg(test)]
pub(crate) fn native_plan_operation(command: OperationId) -> Result<OperationId> {
    native_artifact_operation(command, "plan")
}


/// One real provider for one original admitted command. No process handle,
/// launch ledger, retry state or success-from-path inference is kept here.
pub(crate) struct PiNativeRunner<P: StreamProvider> {
    owner: Arc<TaskJournalOwner<P>>,
    command: OperationId,
    provider: Arc<NativeProcessProvider<P>>,
    launch_enabled: bool,
    work: NativeMaterializationWork,
}

/// Authentic observed process drainage, distinct from namespace disposal. This
/// is never constructed from a path, PID, cancellation ACK or caller JSON.
pub(crate) struct PiNativeDrainedProcess { task: acyclic_harness::TaskId, command: OperationId }


#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PiNativeCleanupResult {
    #[serde(rename = "success")]
    _success: bool,
    #[serde(rename = "exit_code", deserialize_with = "required_optional")]
    _exit_code: Option<i32>,
    #[serde(rename = "stdout")]
    _stdout: serde::de::IgnoredAny,
    #[serde(rename = "stderr")]
    _stderr: serde::de::IgnoredAny,
    #[serde(deserialize_with = "required_optional")]
    stop: Option<PiNativeCleanupStop>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PiNativeCleanupStop {
    #[serde(rename = "kind")]
    _kind: acyclic_harness::filesystem::NativeProcessStopKind,
    #[serde(rename = "message")]
    _message: serde::de::IgnoredAny,
    cleanup_completed: bool,
}

fn required_optional<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)
}

fn process_cleanup_completed(bytes: &[u8], maximum_bytes: u64, work: &NativeMaterializationWork) -> Result<bool> {
    let decoded = super::pi_root_provider::guarded_pi_decode::<PiNativeCleanupResult>(
        bytes, maximum_bytes, std::mem::size_of::<PiNativeCleanupResult>() as u64, JsonValueSchema::None, NATIVE_CLEANUP_DIAGNOSTIC_LABEL_BYTES, work,
    )?;
    Ok(decoded.0.stop.is_none_or(|stop| stop.cleanup_completed))
}

impl<P: StreamProvider + 'static> PiNativeRunner<P> {
    /// Borrow the retained original invocation owner; never reconstruct its
    /// scope, current fence or private signing custody for a recursive caller.
    pub(crate) fn original_owner(&self) -> &Arc<TaskJournalOwner<P>> { &self.owner }
    pub(crate) fn original_work(&self) -> &NativeMaterializationWork { &self.work }

    pub(crate) async fn bind_with_work(
        owner: Arc<TaskJournalOwner<P>>, binding: &PiTaskBinding, command: OperationId,
        plan: &TaskEffectPlan, invocation: &PiNativeInvocation, view: Arc<dyn NativeProcessView<P>>,
        approval: InteractionId, authority: NativeProcessAuthority, work: &NativeMaterializationWork,
    ) -> Result<Self> {
        if owner.task_id() != binding.task_id { return Err(invalid("Pi runner owner differs from its original task")); }
        let results = binding.results.as_ref().ok_or_else(|| invalid("Pi native child has no actually created unmounted result storage"))?;
        if !matches!(results.allocation, PiScratchAllocation::Created(_)) || authority.results.volume() != &results.volume {
            return Err(invalid("Pi native result publisher differs from its original created result volume"));
        }
        let expected = invocation.request_with_work(binding, view.manifest(), work)?;
        // Compare exactly the first-party staged descriptor on the unchanged
        // native ledger before the producer's authenticated request read.
        let bytes = guarded_native_bytes(&expected.0, plan.request.descriptor().byte_length(), work, false)?;
        plan.request.descriptor().verify(&bytes.bytes)?;
        if plan.provider != PI_NATIVE_PROVIDER {
            return Err(invalid("Pi native effect request differs from its original invocation"));
        }
        let provider = NativeProcessProvider::new_with_native_work(owner.clone(), command, plan, view, approval, authority, work)?
            .await?.into_arc_with_native_work(work)?;
        Ok(Self { owner, command, provider, launch_enabled: true, work: work.clone() })
    }

    /// Receipt-only construction uses the actual caller's shared ledger, including
    /// terminal read-only authorization. It cannot acquire a view or new lease.
    pub(crate) async fn recover(
        owner: Arc<TaskJournalOwner<P>>, command: OperationId, plan: &TaskEffectPlan,
        approval: InteractionId, authority: NativeProcessAuthority, work: &NativeMaterializationWork,
    ) -> std::result::Result<Self, NativeReceiptError> {
        if plan.provider != PI_NATIVE_PROVIDER { return Err(NativeReceiptError::Record("Pi recovery selected another native provider")); }
        let provider = NativeProcessProvider::recover_with_native_work(
            owner.clone(), command, plan, approval, authority, work).await?
            .into_arc_with_native_work(work).map_err(NativeReceiptError::Owner)?;
        Ok(Self { owner, command, provider, launch_enabled: false, work: work.clone() })
    }

    pub(crate) fn effect_provider(&self) -> Arc<dyn EffectProvider> { self.provider.clone() }

    /// MUST execute outside the session/catalog actor. Native child tool RPCs
    /// require that actor while this future waits for the provider supervisor.
    pub(crate) async fn launch(&self, effects: &ConversationEffectHost<P>) -> Result<(EffectStatus, acyclic_fs::NativeAllocation)> {
        if !self.launch_enabled { return self.reconcile(effects).await; }
        effects.run_task_effect_with_native_work(
            &self.owner, self.command, self.provider.original_plan(), &self.work).await
    }

    /// Lost ACK/restart is observation only. This cannot plan or dispatch.
    pub(crate) async fn reconcile(&self, effects: &ConversationEffectHost<P>) -> Result<(EffectStatus, acyclic_fs::NativeAllocation)> {
        effects.reconcile_task_effect_with_native_work(
            &self.owner, self.command, self.provider.original_plan(), &self.work).await
    }

    pub(crate) async fn receipt(&self) -> std::result::Result<Option<NativeProcessReceipt>, NativeReceiptError> {
        self.receipt_with_native_work(&self.work).await
    }

    fn original_attempt_with_native_work(
        &self, work: &NativeMaterializationWork,
    ) -> Result<EffectAttemptId> {
        let effect = task_effect_id_with_native_work(self.owner.task_id(), self.command, work)?;
        Ok(EffectAttemptId::from_bytes(effect.into_bytes()))
    }

    /// Read the original launch/observation journal on the caller's actual
    /// bounded ledger. Historical capture/publication work is not charged again.
    pub(crate) async fn receipt_with_native_work(
        &self, work: &NativeMaterializationWork,
    ) -> std::result::Result<Option<NativeProcessReceipt>, NativeReceiptError> {
        let attempt = self.original_attempt_with_native_work(work).map_err(NativeReceiptError::Owner)?;
        self.provider.receipt_with_native_work(attempt, work).await
    }

    /// Signal every original warm supervisor before lifecycle awaits any job.
    /// The producer authenticates current settlement; no task/view is cancelled.
    pub(crate) async fn signal_stop(
        &self, work: &NativeMaterializationWork, cleanup_cancellation: &CancellationToken,
    ) -> std::result::Result<(), NativeReceiptError> {
        let attempt = self.original_attempt_with_native_work(work).map_err(NativeReceiptError::Owner)?;
        self.provider.signal_stop(attempt, work, cleanup_cancellation).await
    }

    /// Join and authenticate the same producer's actual process/stdio drainage.
    /// Invoke outside the actor only after every original job has been signalled.
    pub(crate) async fn request_stop(
        &self, work: &NativeMaterializationWork, cleanup_cancellation: &CancellationToken,
    ) -> std::result::Result<NativeProcessReceipt, NativeReceiptError> {
        let attempt = self.original_attempt_with_native_work(work).map_err(NativeReceiptError::Owner)?;
        self.provider.request_stop(attempt, work, cleanup_cancellation).await
    }


    pub(crate) async fn confirm_process_cleanup_with_work(
        &self, binding: &PiTaskBinding, context: &TaskContext, maximum_bytes: u64, meter: &NativeMaterializationWork,
    ) -> std::result::Result<PiNativeDrainedProcess, NativeReceiptError> {
        if context.durable_task_id() != Some(self.owner.task_id()) {
            return Err(NativeReceiptError::Owner(Error::Unauthorized("Pi cleanup content context belongs to another original task".into())));
        }
        let result = async {
            let (terminal_outcome, _terminal_custody) = self.owner.outcome_with_native_work(&meter).await.map_err(NativeReceiptError::Owner)?;
            let terminal = terminal_outcome.is_some();
            if !terminal { self.owner.verify_with_native_work(true, &meter).await.map_err(NativeReceiptError::Owner)?; }
            if binding.task_id != self.owner.task_id() {
                return Err(NativeReceiptError::Owner(invalid("Pi process cleanup selected another task")));
            }
            let receipt = self.receipt_with_native_work(&meter).await?
                .ok_or(NativeReceiptError::Owner(Error::Indeterminate(self.command)))?;
            if !receipt.observed || receipt.task != binding.task_id || receipt.command != self.command {
                return Err(NativeReceiptError::Owner(Error::Indeterminate(self.command)));
            }
            let output = match &receipt.effect.status {
                EffectStatus::Succeeded { result } => result,
                _ => receipt.diagnostic.as_ref().ok_or(NativeReceiptError::Owner(Error::Indeterminate(self.command)))?,
            };
            let storage = binding.results.as_ref()
                .ok_or_else(|| NativeReceiptError::Owner(invalid("Pi cleanup has no original result storage")))?;
            if !matches!(storage.allocation, PiScratchAllocation::Created(_)) || output.volume() != &storage.volume {
                return Err(NativeReceiptError::Owner(Error::Unauthorized("Pi cleanup diagnostic belongs to another created result volume".into())));
            }
            self.owner.require_volume_grant_with_native_work(&storage.volume, VolumeOperation::Read, &meter)
                .map_err(NativeReceiptError::Owner)?;
            if output.descriptor().byte_length() > maximum_bytes {
                return Err(NativeReceiptError::Record("Pi cleanup result exceeds its original content allowance"));
            }
            let bytes = context.read_file_with_native_work(output, meter).await.map_err(NativeReceiptError::Owner)?;
            if !process_cleanup_completed(bytes.as_ref(), maximum_bytes, meter).map_err(NativeReceiptError::Owner)? {
                return Err(NativeReceiptError::Owner(Error::Indeterminate(self.command)));
            }
            if terminal {
                let (after, _after_custody) = self.owner.outcome_with_native_work(&meter).await.map_err(NativeReceiptError::Owner)?;
                if after.is_none() {
                    return Err(NativeReceiptError::Owner(Error::Conflict("Pi original terminal process observation changed".into())));
                }
            } else {
                self.owner.verify_with_native_work(true, &meter).await.map_err(NativeReceiptError::Owner)?;
            }
            Ok(PiNativeDrainedProcess { task: binding.task_id, command: self.command })
        }.await;
        result
    }

    /// Orders the original task's existing durable subtree cancellation first;
    /// the native supervisor sees that intent through its retained owner fence.
    /// A cancellation ACK never claims process exit or cleanup completion.
    pub(crate) async fn cancel(
        &self, host: &CoordinatorTaskHost<P>, effects: &ConversationEffectHost<P>,
    ) -> Result<(EffectStatus, acyclic_fs::NativeAllocation)> {
        host.cancel_with_native_work(self.owner.task_id(), &self.work).await?;
        self.reconcile(effects).await
    }
}

/// The production policy is the original admitted native-effect capability,
/// restricted to the immutable protected host and this exact request. Approval
/// is an ordinary resolved interaction, not a replacement approval ledger.
#[allow(clippy::too_many_arguments, reason = "original task, immutable request and existing interaction owner")]
pub(crate) async fn approve_command<P, A, O>(
    protected: &super::pi_owner::PiOwnerConfig, owner: &TaskJournalOwner<P>, context: &TaskContext,
    binding: &PiTaskBinding, invocation: &PiNativeInvocation, manifest: &NativeViewManifest, command: OperationId,
    journal: &acyclic_harness::filesystem::FilesystemExecutionJournal<P, A, O>, work: &NativeMaterializationWork,
) -> Result<InteractionId>
where P: StreamProvider, A: acyclic_fs::AsyncAuthorityStore + 'static, O: acyclic_fs::AsyncObjectStore + 'static,
{
    owner.verify_with_native_work(false, work).await?;
    let native_capability = "effect:provider:acyclic.pi.native";
    if context.durable_task_id() != Some(owner.task_id()) || binding.task_id != owner.task_id()
        || !context.scope().grants().contains(native_capability) || !protected.policy().grants.contains(native_capability)
    {
        return Err(Error::Unauthorized("Pi native approval exceeds the original admitted native policy".into()));
    }
    let runtime = protected.native_runtime()?;
    runtime.validate()?;
    if invocation.version != runtime.version || invocation.executable != runtime.executable
        || invocation.executable_prefix != runtime.executable_prefix || invocation.extension != runtime.extension
        || invocation.provider_extension != runtime.provider_extension
        || !invocation.builtin_tools.iter().map(String::as_str).eq(["read", "write", "edit"])
        || runtime.environment.iter().any(|(key, value)| invocation.environment.get(key) != Some(value))
        || invocation.environment.keys().any(|key| !is_caller_environment(key) && !runtime.environment.contains_key(key))
        || invocation.environment.get("ACYCLIC_PI_CALLER").is_none_or(String::is_empty)
    {
        return Err(Error::Unauthorized("Pi native approval changed original protected host settings".into()));
    }
    let request = invocation.request_with_work(binding, manifest, work)?;
    let digest = request.0.approval_digest_with_native_work(owner.task_id(), command, work)?;
    let approval = task_interaction_id(owner.task_id(), command);
    let title = native_fixed_text(format_args!("Run pinned Pi child {}", binding.native_session_id))?;
    let title = pi_native_string(title.as_str(), work)?;
    let interaction = (Interaction::approval(title.0, command, digest)?, title.1);
    journal.open_interaction_with_native_work(approval, &interaction.0, work)?.await?;
    owner.verify_with_native_work(false, work).await?;
    let respond = native_fixed_text(format_args!("interaction:respond:{approval}"))?;
    let encoded_grants = super::pi_root_provider::guarded_pi_bytes(
        &["interaction:resolve", respond.as_str()], 1024, work)?;
    let grants = super::pi_root_provider::guarded_pi_decode::<Capabilities>(
        &encoded_grants.0, 1024, std::mem::size_of::<Capabilities>() as u64, JsonValueSchema::None, NATIVE_CAPABILITIES_DIAGNOSTIC_LABEL_BYTES, work)?;
    let responder = protected.conversation_authority_with_native_work(owner.task_id(), binding.agent_id, &grants.0, work)?;
    journal.resolve_interaction_with_native_work(approval,
        &InteractionResponse::Approval { approved: true, reason: None }, &responder.0.2, work)?.await?;
    owner.verify_with_native_work(false, work).await?;
    Ok(approval)
}

struct NativeFixedText { bytes: [u8; 256], length: usize }
impl NativeFixedText {
    fn as_str(&self) -> &str { std::str::from_utf8(&self.bytes[..self.length]).expect("formatter copies valid UTF-8") }
}
impl std::fmt::Write for NativeFixedText {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        let end = self.length.checked_add(value.len()).filter(|end| *end <= self.bytes.len()).ok_or(std::fmt::Error)?;
        self.bytes[self.length..end].copy_from_slice(value.as_bytes());
        self.length = end;
        Ok(())
    }
}
fn native_fixed_text(arguments: std::fmt::Arguments<'_>) -> Result<NativeFixedText> {
    let mut text = NativeFixedText { bytes: [0; 256], length: 0 };
    std::fmt::Write::write_fmt(&mut text, arguments)
        .map_err(|_| native_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
    Ok(text)
}

/// Uses the actual existing scratch materializer and its original task journal.
/// Host allocation/witness is separate and must come from authenticated prepare.
pub(crate) fn prepare_scratch_with_work<'a, P, A, O>(
    owner: &'a TaskJournalOwner<P>, guards: &'a [NativeVolumeCallbackGuard<P>],
    allocation: &'a NativePreparationRoot, options: &'a NativeViewOptions,
    bindings: &'a [BorrowedNativeVolumeBinding<'a, A, O>], work: &'a NativeMaterializationWork,
) -> Result<NativeFutureBundle<'a, NativeVolumeView<A, O>>>
where P: StreamProvider, A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
      O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
{
    NativeVolumeView::prepare_with_native_work(owner, guards, allocation, options, bindings, work)
}

/// Created only by the original authenticated native preparation below. The
/// physical observations alone are not a directory-disposal capability.
pub(crate) struct PiPreparedScratch<A, O> {
    pub(crate) view: Arc<NativeVolumeView<A, O>>,
    pub(crate) allocation: NativePreparationRoot,
    pub(crate) operation: acyclic_fs::OperationId,
    task: acyclic_harness::TaskId,
    scratch: VolumeRef,
    service_root: PathBuf,
    service_identity: acyclic_fs::NativeRootIdentity,
    scratch_relative: PathBuf,
    scratch_identity: acyclic_fs::NativeRootIdentity,
    _creator_packet: acyclic_fs::materializer::NativeOwnedRootAllocation,
    _metadata_custody: acyclic_fs::NativeAllocation,
}

pub(crate) struct PiNativeRootSeed {
    pub(crate) allocation: Option<NativePreparationRoot>,
    packet: acyclic_fs::materializer::NativeOwnedRootAllocation,
    view_root: PathBuf,
    operation_root: PathBuf,
    operation: acyclic_fs::OperationId,
    task: acyclic_harness::TaskId,
    scratch: VolumeRef,
    service_root: PathBuf,
    service_identity: Option<acyclic_fs::NativeRootIdentity>,
    scratch_relative: PathBuf,
    _metadata_custody: acyclic_fs::NativeAllocation,
}

impl PiNativeRootSeed {
    fn witnessed_allocation(&self) -> Result<&NativePreparationRoot> {
        self.allocation.as_ref().ok_or(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Indeterminate,
            reason: "Pi native root has no completed original creator witness",
        })
    }

    pub(crate) fn preparation_root(&self) -> Option<&NativePreparationRoot> {
        self.allocation.as_ref()
    }

    pub(crate) fn creation_status(&self, index: usize) -> Option<acyclic_fs::materializer::NativeOwnedRootStatus> {
        self.packet.status(index)
    }

    pub(crate) fn creation_parent_count(&self) -> usize { self.packet.parent_count() }

    pub(crate) fn creation_parent_status(&self, index: usize) -> Option<acyclic_fs::materializer::NativeOwnedRootParentStatus> {
        self.packet.parent_status(index)
    }

    pub(crate) async fn create_directories_into_with_work<'a, P, A, O>(
        &mut self, owner: &'a Arc<TaskJournalOwner<P>>, shared: &Arc<NativeTaskContext>,
        binding: &PiTaskBinding, options: &NativeViewOptions, intent: &PiNativeLaunchIntent, session: &FileRef,
        bindings: &[BorrowedNativeVolumeBinding<'a, A, O>], meter: &NativeMaterializationWork,
    ) -> Result<()>
    where P: StreamProvider + 'static, A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
          O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
    {
        owner.verify_with_native_work(false, meter).await?;
        if owner.task_id() != self.task || binding.task_id != self.task
            || shared.context().durable_task_id() != Some(self.task) || binding.scratch.volume != self.scratch
            || options.root != self.view_root || self.operation != NativePreparationRoot::operation(owner)
            || self.packet.operation_id() != self.operation || !self._metadata_custody.matches_work(meter)
            || bindings.len() != 1 || bindings[0].volume != &self.scratch
            || bindings[0].path != "scratch" || !bindings[0].writable
            || !matches!(bindings[0].namespace, NativeVolumeNamespace::MaterializedDirectory)
            || session.volume() != &self.scratch || session.path() != intent.session_path
        {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native directory creation differs from its retained admitted seed" });
        }
        let creator = owner.child_scratch_creation_proof()?;
        if creator.original_task() != self.task || creator.volume() != &self.scratch {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native directory creation differs from its sealed Created scratch" });
        }
        creator.authorize_settlement_with_native_work(owner, meter).await?;
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            ContentGrant::verify(bindings[0].verifier, bindings[0].scope, &self.scratch, operation)?;
            owner.require_volume_grant_with_native_work(&self.scratch, operation, meter)?;
        }
        let bytes = shared.context().read_file_with_native_work(session, meter).await?;
        if bytes.as_ref() != intent.session_bytes.as_bytes() {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi original session publication differs from its immutable native cut" });
        }
        drop(bytes);
        let PiScratchAllocation::Created(creation) = &binding.scratch.allocation else {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native seed scratch has no actual filesystem creator" });
        };
        let actual = bindings[0].host.open_volume_with_native_work(&self.scratch, meter).await?;
        if actual.id().into_bytes() != creation.workspace_id {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi scratch workspace differs from its original filesystem creator" });
        }
        self.service_identity = Some(meter.root_identity(&self.service_root).map_err(native_error)?);
        if self.allocation.is_some() { return Ok(()); }
        let mut recovered = NativeVolumeView::<A, O>::recover_allocation_with_native_work(owner, options, meter)?.await?;
        if let Some(allocation) = recovered.0.as_ref() {
            if allocation.root != self.view_root || allocation.operation_directory != self.operation_root {
                return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
                    reason: "Pi recorded original preparation selected another native namespace" });
            }
            self._metadata_custody.absorb(&mut recovered.1).map_err(native_error)?;
            self.allocation = recovered.0;
            return Ok(());
        }
        for index in 0..4 {
            if matches!(self.packet.status(index), Some(acyclic_fs::materializer::NativeOwnedRootStatus::Known(_))) {
                continue;
            }
            self.packet.create_into_guarded(index, meter, || async {
                owner.verify_with_native_work(false, meter).await?;
                creator.authorize_settlement_with_native_work(owner, meter).await
            }).await?;
        }
        let identities = self.packet.known_identities().ok_or(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Indeterminate,
            reason: "Pi native creator has not confirmed every original directory identity",
        })?;
        creator.authorize_settlement_with_native_work(owner, meter).await?;
        let mut witnessed = NativePreparationRoot::witness_with_native_work(&self.view_root, &self.operation_root, meter)?.await?;
        let observed = witnessed.0.expected_preparation_identity;
        if witnessed.0.expected_identity != identities[0]
            || acyclic_fs::NativeRootIdentity::from_bytes(observed.operation) != identities[1]
            || acyclic_fs::NativeRootIdentity::from_bytes(observed.target) != identities[2]
            || acyclic_fs::NativeRootIdentity::from_bytes(observed.backup) != identities[3]
        {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi native witness differs from its actual original creation receipts" });
        }
        self._metadata_custody.absorb(&mut witnessed.1).map_err(native_error)?;
        self.allocation = Some(witnessed.0);
        Ok(())
    }
    /// Mounts the existing lazy child workspace without a copied project tree
    /// or another physical-root watcher. `retained` must remain with the task:
    /// a failed snapshot/pin check must not drop an acquired live mount owner.
    #[allow(clippy::too_many_arguments, reason = "original created project, native allocation, and scoped publication selection")]
    pub(crate) async fn mount_project_with_work<P, D, S>(
        &self, owner: &Arc<TaskJournalOwner<P>>, shared: &Arc<NativeTaskContext>, binding: &PiTaskBinding,
        root_key: &str, workspace: &acyclic_fs::LazyWorkspace<A, O, D, S>, filesystem: Arc<FilesystemHost<A, O>>,
        original_generation: &acyclic_harness::resources::GenerationRef,
        authority: (AuthorityVerifier, Scope, acyclic_fs::NativeAllocation),
        publication: PublicationPermit, index: usize, writable: bool, retained: &mut Option<Arc<LazyMount<A, O, D, S>>>,
        retained_allocation: &mut Option<acyclic_fs::NativeAllocation>,
        startup_owner: &mut Option<acyclic_fs::native_mount::NativeMountSession>,
        original_volume_work: &NativeMaterializationWork, meter: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(NativeVolumeMapping, NativeExistingLazyVolume, NativeVolumeCallbackGuard<P>, acyclic_fs::NativeAllocation)>
    where P: StreamProvider + 'static, A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
          O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
          D: acyclic_fs::DemandSource + 'static, S: acyclic_fs::LazyWorkspaceStore + 'static,
    {
        let context = shared.context();
        let result = async {
            if !authority.2.work().shares_ledger(meter) || !original_volume_work.shares_ledger(meter) {
                return Err(native_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            owner.verify_with_native_work(false, meter).await?;
            if owner.task_id() != self.task || binding.task_id != self.task || context.durable_task_id() != Some(self.task)
                || binding.scratch.volume != self.scratch
            {
                return Err(Error::Unauthorized("Pi lazy namespace belongs to another original task".into()));
            }
            let volume = binding.project_volumes.get(root_key).ok_or_else(|| invalid("Pi lazy project is not selected by the original child"))?;
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                if operation == VolumeOperation::Write && !writable { continue; }
                ContentGrant::verify(&verifier, &scope, volume, operation)?;
                owner.require_volume_grant_with_native_work(volume, operation, meter)?;
            }
            let allocation = self.witnessed_allocation()?;
            let route = native_root_route_with_work(root_key, meter)?;
            let mut path = native_join(&allocation.root, Path::new(&route.0), meter)?;
            if meter.root_identity(&allocation.root).map_err(native_error)? != allocation.expected_identity {
                return Err(Error::Conflict("Pi original native allocation was replaced".into()));
            }
            let callback_guard = shared.volume_callback_guard_with_native_work(owner, meter).await?;
            let actual = filesystem.open_volume_with_native_work(volume, original_volume_work).await?;
            if retained.is_none() {
                if startup_owner.is_some() || retained_allocation.is_some() {
                    return Err(Error::Indeterminate(OperationId::from_bytes(self.task.into_bytes())));
                }
                meter.create_owned_directory(&allocation.root, allocation.expected_identity, Path::new(&route.0), true)
                    .map_err(native_error)?;
                let options = if writable { acyclic_fs::MountOptions::read_write() } else { acyclic_fs::MountOptions::read_only() }
                    .publication(acyclic_fs::MountPublication::Manual);
                let source_guard = callback_guard.for_volume(owner, volume, writable, original_volume_work)?;
                let mut allocation = original_volume_work.claim(std::mem::size_of::<LazyMount<A, O, D, S>>() as u64)
                    .map_err(native_error)?;
                // Allocate the real Arc backing slot before native startup. A
                // later quota denial must not strand a successfully mounted
                // driver before it reaches the caller's retained owner slot.
                let mut slot = Arc::<LazyMount<A, O, D, S>>::new_uninit();
                let mut mount = match workspace.mount_with_work(path.path.as_path(), options,
                    Some(source_guard), &original_volume_work.native_mount_work(), cancellation).await {
                    Ok(mount) => mount,
                    Err(acyclic_fs::native_mount::MountLifecycleError::Native(
                        acyclic_fs::native_mount::NativeMountError::StartupFailed(failure),
                    )) => {
                        let (cause, owner) = failure.into_parts();
                        *startup_owner = Some(owner);
                        return Err(native_view_error(acyclic_fs::native_mount::MountLifecycleError::Native(cause)));
                    },
                    Err(cause) => return Err(native_view_error(cause)),
                };
                let custody_failure = mount.retain_payload_allocation(&mut allocation).err();
                Arc::get_mut(&mut slot).expect("new original mount slot is exclusive").write(mount);
                // SAFETY: the exclusive slot above contains exactly the actual
                // successful SDK mount; no uninitialized value is exposed.
                *retained = Some(unsafe { slot.assume_init() });
                if let Some(cause) = custody_failure {
                    // A failed graft changes neither ledger. Retain both the
                    // real live mount and its claimed backing slot until an
                    // explicit bounded shutdown has physically completed.
                    *retained_allocation = Some(allocation);
                    if retained.as_ref().expect("actual mount retained above")
                        .abandon_with_work(&original_volume_work.native_mount_work()).is_ok()
                    {
                        *retained = None;
                        *retained_allocation = None;
                    }
                    return Err(native_view_error(cause));
                }
            }
            let mount = retained.as_ref().ok_or_else(|| invalid("Pi lazy mount acquisition lost its actual owner"))?;
            if mount.path() != path.path.as_path() {
                return Err(Error::Conflict("Pi retained project mount belongs to another native namespace".into()));
            }
            let snapshot = mount.snapshot_with_work(&original_volume_work.native_mount_work(), cancellation).await.map_err(native_view_error)?;
            if snapshot.workspace_id != actual.id()
                || snapshot.authored_generation.digest().as_bytes().as_slice() != original_generation.as_resource().key()
            {
                return Err(Error::Conflict("Pi lazy mount differs from the original actual filesystem workspace or seed generation".into()));
            }
            owner.verify_with_native_work(false, meter).await?;
            let authority_revision = native_digest_with_work(&(authority.0.audience(), &authority.1),
                context.scope().limits().file_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES), meter)?;
            let mut volume_copy = acyclic_harness::filesystem::copy_native_volume_with_work(volume, meter)?;
            let mut generation_copy = acyclic_harness::filesystem::copy_native_generation_with_work(original_generation, meter)?;
            let namespace = NativeVolumeNamespace::LazyMount { snapshot };
            let declaration = BorrowedNativeVolumeBinding { host: filesystem.as_ref(), volume,
                generation: original_generation, namespace: &namespace, path: &route.0, writable,
                verifier: &authority.0, scope: &authority.1, publication };
            let existing = NativeExistingLazyVolume::capture_with_native_work(
                mount, owner, &declaration, &callback_guard, meter, original_volume_work)?.await?;
            let mut metadata = meter.retain_owned(0).map_err(native_error)?;
            metadata.absorb(&mut volume_copy.1).map_err(native_error)?;
            metadata.absorb(&mut generation_copy.1).map_err(native_error)?;
            metadata.absorb(&mut path.custody).map_err(native_error)?;
            let result = (
                NativeVolumeMapping { volume: volume_copy.0, generation: generation_copy.0,
                    namespace: NativeVolumeNamespace::LazyMount { snapshot }, path: path.path, writable, authority_revision },
                PiLazyMountBinding { index, mount: mount.clone(), filesystem, verifier: authority.0, scope: authority.1,
                    publication, _mapping_custody: metadata, _authority_custody: authority.2 },
            );
            result.1._mapping_custody.absorb(&mut volume_copy.1).map_err(native_error)?;
            result.1._mapping_custody.absorb(&mut generation_copy.1).map_err(native_error)?;
            result.1._mapping_custody.absorb(&mut path.custody).map_err(native_error)?;
            Ok(result)
        }.await;
        result
    }

    /// Keeps the exact creator witness in its owner slot until every fallible
    /// preparation, verification, and Arc publication has completed.
    pub(crate) async fn into_prepared_mixed_with_work<'a, P, A, O>(
        retained: &mut Option<Self>, owner: &'a Arc<TaskJournalOwner<P>>,
        options: &'a NativeViewOptions, bindings: &'a [BorrowedNativeVolumeBinding<'a, A, O>],
        guards: &'a [NativeVolumeCallbackGuard<P>],
        existing_lazy: &'a [Option<&'a NativeExistingLazyVolume>], meter: &'a NativeMaterializationWork,
    ) -> Result<PiPreparedScratch<A, O>>
    where P: StreamProvider + 'static, A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
          O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
    {
        let seed = retained.as_ref().ok_or(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi mixed preparation has no retained original creator witness",
        })?;
        owner.verify_with_native_work(false, meter).await?;
        let allocation = seed.witnessed_allocation()?;
        let service_identity = seed.service_identity.ok_or(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Indeterminate,
            reason: "Pi mixed preparation has no original service namespace witness",
        })?;
        if seed.task != owner.task_id() || allocation.root != options.root
            || bindings.len() != 2 || bindings[0].volume != &seed.scratch || bindings[0].path != "scratch"
            || !bindings[0].writable || !matches!(bindings[0].namespace, NativeVolumeNamespace::MaterializedDirectory)
            || !matches!(bindings[1].namespace, NativeVolumeNamespace::LazyMount { .. })
        {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi mixed declaration differs from its retained original creator" });
        }
        let view = NativeVolumeView::prepare_mixed_with_native_work(
            owner, guards, allocation, options, bindings, existing_lazy, meter)?.await?;
        let prepared = view.preparation_receipts().first().ok_or(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "Pi mixed view has no original preparation journal",
        })?;
        if prepared.task != seed.task || prepared.journal.plan.operation_id != seed.operation {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi mixed view belongs to another original preparation operation" });
        }
        let scratch_root = native_join(&seed.service_root, &seed.scratch_relative, meter)?;
        let scratch_identity = meter.root_identity(&scratch_root.path).map_err(native_error)?;
        owner.verify_with_native_work(false, meter).await?;
        let view = view.into_arc_with_native_work(meter)?;
        let seed = retained.take().expect("original creator slot was exclusively borrowed throughout preparation");
        Ok(PiPreparedScratch { view, allocation: seed.allocation.expect("completed original creator witness was verified above"),
            operation: seed.operation, task: seed.task, scratch: seed.scratch, service_root: seed.service_root, service_identity,
            scratch_relative: seed.scratch_relative, scratch_identity, _creator_packet: seed.packet,
            _metadata_custody: seed._metadata_custody })
    }

    /// Disposes this retained creator packet before launch. A recorded
    /// preparation must first be genuinely RolledBack under the same SDK
    /// operation and unchanged recorded/physical allocation identities.
    pub(crate) async fn cleanup_unprepared_with_work<P, A, O, D, S>(
        &mut self, owner: &TaskJournalOwner<P>, proof: &acyclic_harness::ChildScratchCreationProof,
        mounts: &[&LazyMount<A, O, D, S>],
        startup: &mut Option<acyclic_fs::native_mount::NativeMountSession>,
        source_volume_work: Option<&NativeMaterializationWork>, meter: &NativeMaterializationWork,
    ) -> Result<()>
    where P: StreamProvider + 'static, A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
          O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
          D: acyclic_fs::DemandSource + 'static, S: acyclic_fs::LazyWorkspaceStore + 'static,
    {
        if owner.task_id() != self.task || proof.original_task() != self.task || proof.volume() != &self.scratch
            || self.operation != NativePreparationRoot::operation(owner) || self.packet.operation_id() != self.operation
            || !self._metadata_custody.matches_work(meter)
        {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi seed disposal differs from its original creator and current child" });
        }
        proof.authorize_settlement_with_native_work(owner, meter).await?;
        if let Some(allocation) = self.allocation.as_ref() {
            if let Some(receipt) = NativeVolumeView::<A, O>::inspect_preparation_with_native_work(owner, allocation, meter)?.await? {
                verify_original_native_rollback(&receipt, self.task, self.operation, allocation)?;
            }
        }
        stop_unstarted_sources_with_work(owner, proof, &self.view_root, mounts, startup, source_volume_work, meter).await?;
        self.packet.release_guarded(meter, || async {
            proof.authorize_settlement_with_native_work(owner, meter).await
        }).await
    }
}

impl<A, O> PiPreparedScratch<A, O> {
    /// Used only by the host's authenticated Planned/no-receipt rollback path,
    /// never as a replacement for process cleanup after native dispatch.
    pub(crate) async fn cleanup_unstarted_rollback_with_work<P, D, S>(
        &mut self, owner: &TaskJournalOwner<P>, context: &TaskContext,
        proof: &acyclic_harness::ChildScratchCreationProof, mounts: &[&LazyMount<A, O, D, S>],
        startup: &mut Option<acyclic_fs::native_mount::NativeMountSession>,
        source_volume_work: Option<&NativeMaterializationWork>, meter: &NativeMaterializationWork,
    ) -> Result<()>
    where P: StreamProvider + 'static, A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
          O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
          D: acyclic_fs::DemandSource + 'static, S: acyclic_fs::LazyWorkspaceStore + 'static,
    {
        if owner.task_id() != self.task || context.durable_task_id() != Some(self.task)
            || proof.original_task() != self.task || proof.volume() != &self.scratch
            || self.operation != NativePreparationRoot::operation(owner)
            || self._creator_packet.operation_id() != self.operation || !self._metadata_custody.matches_work(meter)
        {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi prepared rollback differs from its original creator and current child" });
        }
        proof.authorize_settlement_with_native_work(owner, meter).await?;
        let receipt = NativeVolumeView::<A, O>::inspect_preparation_with_native_work(owner, &self.allocation, meter)?.await?
            .ok_or(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Indeterminate,
                reason: "Pi prepared rollback has no original recorded SDK rollback" })?;
        verify_original_native_rollback(&receipt, self.task, self.operation, &self.allocation)?;
        stop_unstarted_sources_with_work(owner, proof, &self.allocation.root, mounts, startup, source_volume_work, meter).await?;
        self._creator_packet.release_guarded(meter, || async {
            proof.authorize_settlement_with_native_work(owner, meter).await
        }).await
    }

    /// Explicit private-scratch disposal only. The caller has already stopped
    /// actual project drivers; no project/source/parent namespace is released.
    pub(crate) async fn release_scratch_with_work<P: StreamProvider + 'static>(
        &self, owner: &TaskJournalOwner<P>, context: &TaskContext, command: OperationId,
        drained: &PiNativeDrainedProcess, proof: &acyclic_harness::ChildScratchCreationProof,
        meter: &NativeMaterializationWork,
    ) -> Result<()> {
        if owner.task_id() != self.task || context.durable_task_id() != Some(self.task)
            || drained.task != self.task || drained.command != command || proof.original_task() != self.task
            || proof.volume() != &self.scratch || self.operation != NativePreparationRoot::operation(owner)
        {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi scratch disposal differs from its original created child and process" });
        }
        let target = native_join(&self.allocation.operation_directory, Path::new("target"), meter)?;
        let backup = native_join(&self.allocation.operation_directory, Path::new("backup"), meter)?;
        meter.release_owned_directory_guarded(&self.service_root, self.service_identity, &self.scratch_relative,
            self.scratch_identity, || async {
                proof.authorize_settlement_with_native_work(owner, meter).await?;
                if meter.root_identity(&self.allocation.root).map_err(native_error)? != self.allocation.expected_identity {
                    return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
                        reason: "Pi original native root was replaced before scratch disposal" });
                }
                let identities = self.allocation.expected_preparation_identity;
                for (path, identity) in [
                    (&self.allocation.operation_directory, identities.operation),
                    (&target.path, identities.target),
                    (&backup.path, identities.backup),
                ] {
                    if meter.root_identity(path).map_err(native_error)? != acyclic_fs::NativeRootIdentity::from_bytes(identity) {
                        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
                            reason: "Pi original preparation namespace was replaced before scratch disposal" });
                    }
                }
                Ok(())
            }).await
    }
}

fn verify_original_native_rollback(
    receipt: &NativePreparationReceipt, task: acyclic_harness::TaskId, operation: acyclic_fs::OperationId,
    allocation: &NativePreparationRoot,
) -> Result<()> {
    let expected = allocation.expected_preparation_identity;
    if receipt.task != task || receipt.manifest.options.root != allocation.root
        || !receipt.journal.is_valid_for(operation) || receipt.journal.phase != acyclic_fs::MaterializationPhase::RolledBack
        || receipt.journal.root_identity != Some(allocation.expected_identity.into_bytes())
        || receipt.journal.preparation_identity != Some(expected)
        || receipt.root != acyclic_fs::MaterializationRootObservation::Identical(allocation.expected_identity.into_bytes())
        || receipt.preparation != acyclic_fs::MaterializationPreparationObservation::Identical(expected)
    {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "Pi release has no identical original completed SDK rollback" });
    }
    Ok(())
}

async fn stop_unstarted_sources_with_work<P, A, O, D, S>(
    owner: &TaskJournalOwner<P>, proof: &acyclic_harness::ChildScratchCreationProof, root: &Path,
    mounts: &[&LazyMount<A, O, D, S>], startup: &mut Option<acyclic_fs::native_mount::NativeMountSession>,
    source_volume_work: Option<&NativeMaterializationWork>, metadata_work: &NativeMaterializationWork,
) -> Result<()>
where P: StreamProvider + 'static, A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
      O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
      D: acyclic_fs::DemandSource + 'static, S: acyclic_fs::LazyWorkspaceStore + 'static,
{
    let Some(volume_work) = source_volume_work else {
        if mounts.is_empty() && startup.is_none() { return Ok(()); }
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi source cleanup has no original preseeded volume work" });
    };
    if !volume_work.shares_ledger(metadata_work) {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi source cleanup work belongs to another original family" });
    }
    for mount in mounts {
        if mount.path() == root || !mount.path().starts_with(root) {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi source owner belongs to another original allocation" });
        }
        proof.authorize_settlement_with_native_work(owner, metadata_work).await?;
        mount.abandon_with_work(&volume_work.native_mount_work()).map_err(native_view_error)?;
    }
    if let Some(session) = startup.as_mut() {
        let state = session.cleanup_state();
        if state.destination == root || !state.destination.starts_with(root) {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi startup owner belongs to another original allocation" });
        }
        proof.authorize_settlement_with_native_work(owner, metadata_work).await?;
        session.stop_with_meter(&volume_work.native_mount_work())
            .map_err(|cause| native_view_error(acyclic_fs::native_mount::MountLifecycleError::Native(cause)))?;
        if !session.cleanup_state().cleanup_complete {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Indeterminate,
                reason: "Pi source startup cleanup has not physically completed" });
        }
        startup.take();
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, reason = "original protected creator and exact admitted scratch selection")]
pub(crate) async fn scratch_view_with_work<'a, P, A, O>(
    resources: &super::ServiceResources, root_session: &str, protected: &super::pi_owner::PiOwnerConfig,
    owner: &'a Arc<TaskJournalOwner<P>>, shared: &Arc<NativeTaskContext>, binding: &PiTaskBinding, options: &NativeViewOptions,
    intent: &PiNativeLaunchIntent, session: &FileRef,
    bindings: &[BorrowedNativeVolumeBinding<'a, A, O>], meter: &NativeMaterializationWork,
) -> Result<PiPreparedScratch<A, O>>
where P: StreamProvider + 'static, A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
      O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
{
    let context = shared.context();
    owner.verify_with_native_work(false, meter).await?;
    if context.durable_task_id() != Some(owner.task_id()) || owner.task_id() != binding.task_id || bindings.len() != 1
        || bindings[0].volume != &binding.scratch.volume || bindings[0].path != "scratch" || !bindings[0].writable
        || !matches!(bindings[0].namespace, NativeVolumeNamespace::MaterializedDirectory)
        || !options.work_per_volume.has_finite_allowances()
    {
        return Err(invalid("Pi physical scratch preparation differs from its actual admitted child"));
    }
    protected.authority_with_native_work(meter)?;
    let PiScratchAllocation::Created(creation) = &binding.scratch.allocation else {
        return Err(invalid("Pi scratch has not been created by its original filesystem producer"));
    };
    intent.validate_with_work(meter)?;
    let mut session_generation = [0; 32];
    hex::decode_to_slice(session.version(), &mut session_generation).map_err(|_| invalid("Pi session has no original filesystem generation"))?;
    if session.volume() != &binding.scratch.volume || session.path() != intent.session_path
        || bindings[0].generation.as_resource().key() != session_generation
        || session.descriptor().byte_length() > intent.maximum_bytes
    {
        return Err(Error::Conflict("Pi scratch selection differs from its original published SDK session".into()));
    }
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        ContentGrant::verify(bindings[0].verifier, bindings[0].scope, &binding.scratch.volume, operation)?;
        owner.require_volume_grant_with_native_work(&binding.scratch.volume, operation, meter)?;
    }
    let callback_guard = shared.mount_callback_guard_with_native_work(
        owner.clone(), &binding.scratch.volume, bindings[0].verifier, bindings[0].scope, true, meter)?.await?;
    let callback_guards = [callback_guard];
    let read = context.read_file_with_native_work(session, meter).await?;
    let bytes = PiNativeEncodedBytes { bytes: read.0, _custody: read.1 };
    if bytes.bytes.as_slice() != intent.session_bytes.as_bytes() {
        return Err(Error::Conflict("Pi original session publication differs from the immutable native cut".into()));
    }
    drop(bytes);
    let actual = pi_accept(bindings[0].host.open_measured(
        &workspace_ref(binding.scratch.volume.provider().clone(), &binding.scratch.volume.storage_name()?)?,
        meter.remaining().map_err(native_error)?, meter.cancellation()).await, meter)?;
    if actual.id().into_bytes() != creation.workspace_id {
        return Err(Error::Conflict("Pi scratch workspace differs from its original filesystem creator".into()));
    }
    let service = native_service_root(&resources.data, root_session, owner.task_id(), &options.root, meter)?;
    let service_identity = meter.root_identity(service).map_err(native_error)?;
    let operation = NativePreparationRoot::operation(owner);
    let task_directory = options.root.parent().ok_or_else(|| invalid("Pi native view has no original task namespace"))?;
    let operation_root = native_join(task_directory, Path::new("preparation"), meter)?;
    let scratch_root = native_join(&options.root, Path::new("scratch"), meter)?;
    fn relative<'p>(path: &'p Path, data: &Path) -> Result<&'p Path> {
        path.strip_prefix(data).map_err(|_| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi native seed escaped its held private service root",
        })
    }
    let packet = acyclic_fs::materializer::NativeOwnedRootAllocation::new_with_native_work(operation,
        &resources.private_state_root, [relative(&options.root, &resources.data)?, relative(&operation_root.path, &resources.data)?,
            relative(&target.path, &resources.data)?, relative(&backup.path, &resources.data)?], meter).map_err(native_error)?;
    let mut service_root = meter.copy_path(service).map_err(native_error)?;
    let scratch_relative = scratch_root.path.strip_prefix(service).ok().ok_or(Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi scratch escaped its original service namespace",
    })?;
    let mut scratch_relative = meter.copy_path(scratch_relative).map_err(native_error)?;
    let mut scratch = acyclic_harness::filesystem::copy_native_volume_with_work(&binding.scratch.volume, meter)?;
    let mut view_root = meter.copy_path(&options.root).map_err(native_error)?;
    let mut custody = meter.retain_owned(std::mem::size_of::<PiNativeRootSeed>() as u64).map_err(native_error)?;
    custody.absorb(&mut service_root.1).map_err(native_error)?;
    custody.absorb(&mut scratch_relative.1).map_err(native_error)?;
    custody.absorb(&mut scratch.1).map_err(native_error)?;
    custody.absorb(&mut view_root.1).map_err(native_error)?;
    custody.absorb(&mut operation_root.custody).map_err(native_error)?;
    Ok(PiNativeRootSeed { allocation: None, packet, view_root: view_root.0, operation_root: operation_root.path,
        operation, task: owner.task_id(), scratch: scratch.0, service_root: service_root.0,
        service_identity: None, scratch_relative: scratch_relative.0, _metadata_custody: custody })
}

struct PiNativeRootAllocation {
    allocation: NativePreparationRoot,
    custody: acyclic_fs::NativeAllocation,
}


pub(crate) fn cleanup_scratch_with_work<'a, P, A, O>(
    owner: &'a TaskJournalOwner<P>, allocation: &'a NativePreparationRoot, work: &'a NativeMaterializationWork,
) -> Result<NativeFutureBundle<'a, Option<NativePreparationReceipt>>>
where P: StreamProvider, A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
      O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
{
    NativeVolumeView::<A, O>::cleanup_preparation_with_native_work(owner, allocation, work)
}

/// Publishes original source-bound bytes through existing immutable FS content.
#[cfg(test)]
#[allow(clippy::too_many_arguments, reason = "exact native provenance and explicit finite provider authorities")]
pub(crate) async fn publish_session<A, O>(
    filesystem: &FilesystemHost<A, O>, volume: &VolumeRef, grant: &ContentGrant,
    source: &PiNativeSourceCut, child: &str, cwd: &Path, path: &str, bytes: &[u8],
    maximum_entries: u32, maximum_bytes: u64, operation: &IdempotencyKey,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<FileRef>, Error>
where A: acyclic_fs::AsyncAuthorityStore, O: acyclic_fs::AsyncObjectStore,
{
    source.validate_session(child, cwd, bytes, maximum_entries, maximum_bytes).map_err(OperationFailure::before_work)?;
    filesystem.put_content_measured(volume, grant, path, bytes, "application/x-ndjson", "Pi child session", maximum_bytes, operation, budget, cancellation).await
}

/// Three immutable artifacts use the existing operation-upload metadata engine.
/// A lost publication ACK is recovered by the same exact command/idempotency key.
#[cfg(test)]
pub(crate) async fn stage_command<A, O>(
    filesystem: &FilesystemHost<A, O>, volume: &VolumeRef, grant: &ContentGrant,
    command: OperationId, request: &NativeProcessRequest, maximum_bytes: u64,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<(WorkflowCommand, TaskEffectPlan)>, Error>
where A: acyclic_fs::AsyncAuthorityStore, O: acyclic_fs::AsyncObjectStore,
{
    let mut work = WorkCounters::default();
    let request_reference = stage_artifact(filesystem, volume, grant, command, "request", request, "application/json", maximum_bytes, budget, cancellation, &mut work).await?;
    let result_schema = stage_artifact(filesystem, volume, grant, command, "schema", &result_schema().map_err(|error| OperationFailure::new(error, work))?, "application/schema+json", maximum_bytes, budget, cancellation, &mut work).await?;
    let plan = TaskEffectPlan { provider: PI_NATIVE_PROVIDER.to_owned(), guarantee: EffectGuarantee::AtMostOnce,
        effect_kind: NATIVE_PROCESS_EFFECT_KIND.to_owned(), request: request_reference, result_schema };
    let payload = stage_artifact(filesystem, volume, grant, command, "plan", &plan, "application/json", maximum_bytes, budget, cancellation, &mut work).await?;
    Ok(OperationReceipt { value: (WorkflowCommand { operation_id: command, kind: EFFECT_TASK_COMMAND_KIND.to_owned(), payload }, plan), work })
}

#[cfg(test)]
#[allow(clippy::too_many_arguments, reason = "bounded immutable publication carries the original upload operation")]
async fn stage_artifact<A, O, T: Serialize>(
    filesystem: &FilesystemHost<A, O>, volume: &VolumeRef, grant: &ContentGrant, command: OperationId,
    role: &str, value: &T, media_type: &str, maximum_bytes: u64, budget: WorkBudget,
    cancellation: &CancellationToken, work: &mut WorkCounters,
) -> MeasuredResult<FileRef, Error>
where A: acyclic_fs::AsyncAuthorityStore, O: acyclic_fs::AsyncObjectStore,
{
    let bytes = encode_artifact(value, maximum_bytes).map_err(|error| OperationFailure::new(error, *work))?;
    if maximum_bytes == 0 || bytes.len() as u64 > maximum_bytes {
        return Err(OperationFailure::new(invalid("Pi native effect artifact exceeds its byte bound"), *work));
    }
    let operation = IdempotencyKey::new(format!("pi-native:{command}:{role}"))
        .map_err(|error| OperationFailure::new(error, *work))?;
    let remaining = work.remaining(budget).map_err(|error| OperationFailure::new(invalid(&error.to_string()), *work))?;
    let result = filesystem.put_content_measured(volume, grant, &format!("pi-native/{command}/{role}.json"), &bytes,
        media_type, role, maximum_bytes, &operation, remaining, cancellation).await;
    match result {
        Ok(receipt) => { *work = work.checked_add(receipt.work).map_err(|error| OperationFailure::new(invalid(&error.to_string()), *work))?; Ok(receipt.value) }
        Err(failure) => Err(failure.map_with_prior_work(*work, |error| error)),
    }
}

fn result_schema() -> Result<&'static serde_json::value::RawValue> {
    // A borrowed, immutable schema: the trusted codec owns only its admitted
    // output bytes, never an intermediate json! Value tree.
    serde_json::from_str(r#"{"type":"object","additionalProperties":false,"required":["success","exit_code","stdout","stderr","stop"],"properties":{"success":{"type":"boolean"},"exit_code":{"type":["integer","null"]},"stdout":{"type":"array","items":{"type":"integer","minimum":0,"maximum":255}},"stderr":{"type":"array","items":{"type":"integer","minimum":0,"maximum":255}},"stop":{"oneOf":[{"type":"null"},{"type":"object","additionalProperties":false,"required":["kind","message","cleanup_completed"],"properties":{"kind":{"enum":["output_limit","timeout","control_stopped","capture_failed"]},"message":{"type":"string"},"cleanup_completed":{"type":"boolean"}}}]}}}"#).map_err(decode_error)
}

#[cfg(test)]
fn encode_artifact(value: &impl Serialize, maximum_bytes: u64) -> Result<Vec<u8>> {
    if maximum_bytes == 0 { return Err(invalid("Pi artifact requires a positive byte bound")); }
    let maximum = maximum_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
    let mut output = super::BoundedJsonBuffer::with_limit(usize::try_from(maximum).map_err(|error| invalid(&error.to_string()))?);
    serde_json::to_writer(&mut output, value).map_err(decode_error)?;
    Ok(output.bytes)
}


/// One existing project mount. The index identifies its exact mapping in the
/// immutable combined manifest, rather than a second workspace/path registry.
pub(crate) struct PiLazyMountBinding<A, O, D, S> {
    pub(crate) index: usize,
    pub(crate) mount: Arc<LazyMount<A, O, D, S>>,
    pub(crate) filesystem: Arc<FilesystemHost<A, O>>,
    pub(crate) verifier: AuthorityVerifier,
    pub(crate) scope: Scope,
    pub(crate) publication: PublicationPermit,
    // PiNativeView drops its manifest before its project bindings. This guard
    // owns the actual copied mapping metadata through that final consumer.
    _mapping_custody: acyclic_fs::NativeAllocation,
    // Actual prefunded conversation metadata, resident with the values through
    // every clone of the enclosing live view, not an external host cache.
    _authority_custody: acyclic_fs::NativeAllocation,
}

/// Actual mixed namespace: existing lazy project mounts and the one original
/// scratch working set. Nothing here creates a mount, watcher or materializer.
pub(crate) struct PiNativeView<A, O, D, S> {
    manifest: NativeViewManifest,
    projects: Vec<PiLazyMountBinding<A, O, D, S>>,
    scratch: Arc<NativeVolumeView<A, O>>,
    root_identity: acyclic_fs::NativeRootIdentity,
    cancellation: CancellationToken,
    _manifest_custody: acyclic_fs::NativeAllocation,
}

impl<A, O, D, S> PiNativeView<A, O, D, S>
where A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
      O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
      D: acyclic_fs::DemandSource + 'static, S: acyclic_fs::LazyWorkspaceStore + 'static,
{
    pub(crate) fn manifest(&self) -> &NativeViewManifest { &self.manifest }

    /// Borrows the already authenticated original mount; it does not authorize
    /// a source checkpoint or selected destination write on its own.
    pub(crate) fn project_mount(&self, volume: &VolumeRef) -> Option<Arc<LazyMount<A, O, D, S>>> {
        self.projects.iter().find(|binding| {
            let mapping = &self.manifest.volumes[binding.index];
            &mapping.volume == volume && matches!(&mapping.namespace, NativeVolumeNamespace::LazyMount { .. })
        }).map(|binding| binding.mount.clone())
    }

    /// Copies the exact original SDK gate; this is not a new publication grant.
    pub(crate) fn project_publication(&self, volume: &VolumeRef) -> Option<PublicationPermit> {
        self.projects.iter().find(|binding| {
            let mapping = &self.manifest.volumes[binding.index];
            &mapping.volume == volume && matches!(&mapping.namespace, NativeVolumeNamespace::LazyMount { .. })
        }).map(|binding| binding.publication)
    }

    #[allow(clippy::too_many_arguments, reason = "original task, SDK call, mapped volume and owned output remain explicit")]
    pub(crate) async fn read_builtin_file<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, context: &TaskContext, operation: OperationId,
        volume: &VolumeRef, relative: &Path, maximum_bytes: usize, work: &NativeMaterializationWork,
    ) -> Result<(Vec<u8>, acyclic_fs::NativeAllocation)> {
        self.builtin_guard(owner, context, volume, VolumeOperation::Read, work).await?;
        let mapping = self.builtin_mapping(volume, false, work)?;
        if let Some(binding) = self.projects.iter().find(|binding| &self.manifest.volumes[binding.index].volume == volume) {
            let identity = binding.mount.native_root_identity()
                .ok_or_else(|| native_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
            return binding.mount.with_original_work(&work.native_mount_work(), |scope| {
                let scope = scope.try_clone_for_io();
                async move {
                    let scope = scope.map_err(|cause| native_view_error(acyclic_fs::native_mount::MountLifecycleError::Native(cause)))?;
                    work.read_working_file_guarded_with_scope(scope, &mapping.path, identity, relative, maximum_bytes,
                        || self.builtin_guard(owner, context, volume, VolumeOperation::Read, work)).await
                }
            }).await.map_err(native_view_error)?;
        }
        let locator = builtin_relative(mapping, &self.manifest.options.root, relative, work)?;
        self.scratch.read_builtin_file(owner.native_view_owner(), operation, Path::new(&locator.value), maximum_bytes, work).await
    }

    #[allow(clippy::too_many_arguments, reason = "original SDK call and exact held-leaf edit preimage remain explicit")]
    pub(crate) async fn write_builtin_file<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, context: &TaskContext, operation: OperationId,
        volume: &VolumeRef, relative: &Path, content: &str, create_parents: bool,
        expected_sha256: Option<[u8; 32]>, work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.builtin_guard(owner, context, volume, VolumeOperation::Write, work).await?;
        let mapping = self.builtin_mapping(volume, true, work)?;
        if let Some(binding) = self.projects.iter().find(|binding| &self.manifest.volumes[binding.index].volume == volume) {
            let identity = binding.mount.native_root_identity()
                .ok_or_else(|| native_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
            return binding.mount.with_original_work(&work.native_mount_work(), |scope| {
                let scope = scope.try_clone_for_io();
                async move {
                    let scope = scope.map_err(|cause| native_view_error(acyclic_fs::native_mount::MountLifecycleError::Native(cause)))?;
                    work.write_working_file_guarded_with_scope(scope, &mapping.path, identity, relative, content, create_parents, expected_sha256,
                        || self.builtin_guard(owner, context, volume, VolumeOperation::Write, work)).await
                }
            }).await.map_err(native_view_error)?;
        }
        let locator = builtin_relative(mapping, &self.manifest.options.root, relative, work)?;
        self.scratch.write_builtin_file(owner.native_view_owner(), operation, Path::new(&locator.value),
            content, create_parents, expected_sha256, work).await
    }

    fn builtin_mapping(&self, volume: &VolumeRef, writable: bool, work: &NativeMaterializationWork) -> Result<&NativeVolumeMapping> {
        for mapping in &self.manifest.volumes {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() }).map_err(native_error)?;
            if &mapping.volume != volume { continue; }
            if !matches!(mapping.volume.class(), acyclic_harness::conversation::VolumeClass::Project | acyclic_harness::conversation::VolumeClass::AgentPrivate)
                || writable && !mapping.writable
            {
                return Err(Error::Unauthorized("Pi builtin exceeds its actual mounted volume role".into()));
            }
            return Ok(mapping);
        }
        Err(Error::Unauthorized("Pi builtin volume is not in the original native namespace".into()))
    }

    async fn builtin_guard<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, context: &TaskContext, volume: &VolumeRef,
        operation: VolumeOperation, work: &NativeMaterializationWork,
    ) -> Result<()> {
        if context.durable_task_id() != Some(owner.task_id()) {
            return Err(Error::Unauthorized("Pi builtin caller belongs to another original task".into()));
        }
        self.cancellation.check().map_err(|_| native_error(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        owner.verify_with_native_work(false, work).await?;
        owner.require_volume_grant_with_native_work(volume, VolumeOperation::Read, work)?;
        if operation == VolumeOperation::Write {
            owner.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)?;
        }
        Ok(())
    }

    pub(crate) async fn bind_with_work<P: StreamProvider + 'static>(
        owner: &TaskJournalOwner<P>, context: &TaskContext, mapping: NativeVolumeMapping,
        project: PiLazyMountBinding<A, O, D, S>, scratch: Arc<NativeVolumeView<A, O>>, meter: &NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        owner.verify_with_native_work(false, meter).await?;
        if context.durable_task_id() != Some(owner.task_id()) || project.index != scratch.manifest().volumes.len() {
            return Err(invalid("Pi native namespace differs from its original task or project index"));
        }
        if !project._mapping_custody.work().shares_ledger(meter)
            || !project._authority_custody.work().shares_ledger(meter)
            || !<NativeVolumeView<A, O> as NativeProcessView<P>>::original_native_work(scratch.as_ref())
                .is_some_and(|original| original.shares_ledger(meter))
        {
            return Err(invalid("Pi mixed namespace does not retain its actual original preparation ledger"));
        }
        let root_identity = scratch.preparation_receipts().first()
            .and_then(|receipt| receipt.journal.root_identity)
            .map(acyclic_fs::NativeRootIdentity::from_bytes)
            .ok_or_else(|| invalid("Pi scratch preparation has no actual root witness"))?;
        let root = meter.copy_path(&scratch.manifest().options.root).map_err(native_error)?;
        let mut view = Self {
            manifest: NativeViewManifest { options: NativeViewOptions {
                root: root.0, maximum_volumes: scratch.manifest().options.maximum_volumes,
                work_per_volume: scratch.manifest().options.work_per_volume,
            }, volumes: Vec::new() },
            projects: Vec::new(), scratch, root_identity, cancellation: meter.cancellation().clone(),
            _manifest_custody: root.1,
        };
        let mut volumes = pi_native_vec(view.scratch.manifest().volumes.len().checked_add(1)
            .ok_or_else(|| native_error(acyclic_fs::WorkError::Overflow.into()))?, meter)?;
        view.manifest.volumes = volumes.0;
        view._manifest_custody.absorb(&mut volumes.1).map_err(native_error)?;
        let mut projects = pi_native_vec(1, meter)?;
        view.projects = projects.0;
        view._manifest_custody.absorb(&mut projects.1).map_err(native_error)?;
        view.projects.push(project);
        for original in &view.scratch.manifest().volumes {
            let (copy, mut custody) = acyclic_harness::filesystem::copy_native_mapping_with_work(original, meter)?;
            view.manifest.volumes.push(copy);
            view._manifest_custody.absorb(&mut custody).map_err(native_error)?;
        }
        view.manifest.volumes.push(mapping);
        let mut view = view.authenticate_bind(owner, context, meter).await?;
        let mut backing = meter.claim(std::mem::size_of::<Self>() as u64).map_err(native_error)?;
        view._manifest_custody.absorb(&mut backing).map_err(native_error)?;
        Ok(Arc::new(view))
    }

    async fn authenticate_bind<P: StreamProvider>(
        self, owner: &TaskJournalOwner<P>, context: &TaskContext, meter: &NativeMaterializationWork,
    ) -> Result<Self> {
            let manifest = &self.manifest;
            let projects = &self.projects;
            let scratch = &self.scratch;
            manifest.validate()?;
            let root_identity = scratch.preparation_receipts().first()
                .and_then(|receipt| receipt.journal.root_identity)
                .map(acyclic_fs::NativeRootIdentity::from_bytes)
                .ok_or_else(|| invalid("Pi scratch preparation has no actual root witness"))?;
            for mapping in &manifest.volumes {
                for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                    if operation == VolumeOperation::Write && !mapping.writable { continue; }
                    owner.require_volume_grant_with_native_work(&mapping.volume, operation, meter)?;
                }
            }
            for mapping in &scratch.manifest().volumes {
                if !manifest.volumes.contains(mapping) {
                    return Err(invalid("Pi scratch mapping differs from its actual prepared view"));
                }
            }
            for binding in projects {
                let mapping = manifest.volumes.get(binding.index).ok_or_else(|| invalid("Pi lazy mount index is outside the admitted selection"))?;
                let NativeVolumeNamespace::LazyMount { snapshot } = &mapping.namespace else {
                    return Err(invalid("Pi project mount was mislabeled as materialized storage"));
                };
                if mapping.path != binding.mount.path()
                    || mapping.authority_revision != native_digest_with_work(&(binding.verifier.audience(), &binding.scope),
                        context.scope().limits().file_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES), meter)?
                {
                    return Err(invalid("Pi lazy mount differs from its original path or authority"));
                }
                ContentGrant::verify(&binding.verifier, &binding.scope, &mapping.volume, VolumeOperation::Read)?;
                if mapping.writable { ContentGrant::verify(&binding.verifier, &binding.scope, &mapping.volume, VolumeOperation::Write)?; }
                let reference = workspace_ref(mapping.volume.provider().clone(), &mapping.volume.storage_name()?)?;
                let workspace = pi_accept(binding.filesystem.open_measured(&reference, meter.remaining().map_err(native_error)?, meter.cancellation()).await, meter)?;
                let actual = binding.mount.snapshot_with_work(&meter.native_mount_work(), meter.cancellation()).await.map_err(native_view_error)?;
                if workspace.id() != snapshot.workspace_id || &actual != snapshot {
                    return Err(Error::Conflict("Pi lazy mount does not own the selected filesystem volume or exact snapshot".into()));
                }
            }
            if meter.root_identity(&manifest.options.root).map_err(native_error)? != root_identity {
                return Err(Error::Conflict("Pi original native namespace root was replaced".into()));
            }
            owner.verify_with_native_work(false, meter).await?;
            Ok(self)
    }

    /// A live native caller is paused at its recursive tool RPC. Seal exactly
    /// its original authored project checkout before the existing FS fork
    /// snapshots it; scratch/session provenance remains the SDK's explicit cut.
    pub(crate) async fn checkpoint_projects_with_work<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, context: &TaskContext, meter: &NativeMaterializationWork,
    ) -> Result<(Vec<(usize, acyclic_fs::LazySnapshotRef)>, acyclic_fs::NativeAllocation)> {
        let cancellation = meter.cancellation();
        let result = async {
            owner.verify_with_native_work(false, meter).await?;
            if context.durable_task_id() != Some(owner.task_id()) {
                return Err(invalid("Pi source checkpoint differs from its original live task"));
            }
            if meter.root_identity(&self.manifest.options.root).map_err(native_error)? != self.root_identity {
                return Err(Error::Conflict("Pi source namespace root changed before its fork".into()));
            }
            let snapshots = pi_native_vec(self.projects.len(), meter)?;
            let mut snapshots = PiNativeSnapshots { value: snapshots.0, custody: snapshots.1 };
            for binding in &self.projects {
                let mapping = &self.manifest.volumes[binding.index];
                for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                    if operation == VolumeOperation::Write && !mapping.writable { continue; }
                    owner.require_volume_grant_with_native_work(&mapping.volume, operation, meter)?;
                }
                if mapping.writable {
                    let guard_error = Mutex::new(None);
                    let published = binding.mount.sync_with_permit_work_guarded(binding.publication,
                        &meter.native_mount_work(), cancellation, |work| lazy_publication_guard(
                            owner.native_view_owner(), &mapping.volume, &binding.verifier, &binding.scope,
                            work, &guard_error,
                        )).await.map_err(|error| {
                            guard_error.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                                .take().unwrap_or_else(|| native_view_error(error))
                        });
                    published?;
                }
                let snapshot = binding.mount.snapshot_with_work(&meter.native_mount_work(), cancellation).await.map_err(native_view_error)?;
                snapshots.value.push((binding.index, snapshot));
            }
            Ok((snapshots.value, snapshots.custody))
        }.await;
        result
    }

    /// Detaches actual project drivers only after authenticated process
    /// drainage. Session scratch is deliberately retained for explicit-source
    /// SDK cuts; deleting it requires the separate original-creator proof.
    pub(crate) async fn cleanup_mounts_with_work<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, context: &TaskContext, drained: &PiNativeDrainedProcess,
        meter: &NativeMaterializationWork,
    ) -> Result<()> {
        if context.durable_task_id() != Some(owner.task_id()) || drained.task != owner.task_id() {
            return Err(Error::Unauthorized("Pi native cleanup selected another task".into()));
        }
        if meter.root_identity(&self.manifest.options.root).map_err(native_error)? != self.root_identity {
            return Err(Error::Conflict("Pi original native root changed before cleanup".into()));
        }
        for binding in &self.projects {
            owner.verify_with_native_work(true, meter).await?;
            let volume = &self.manifest.volumes[binding.index].volume;
            owner.require_volume_grant_with_native_work(volume, VolumeOperation::Read, meter)?;
            binding.mount.abandon_with_work(&meter.native_mount_work()).map_err(native_view_error)?;
        }
        owner.verify_with_native_work(true, meter).await?;
        Ok(())
    }

    async fn validate<P: StreamProvider + 'static>(&self, meter: &NativeMaterializationWork) -> Result<()> {
        if meter.root_identity(&self.manifest.options.root).map_err(native_error)? != self.root_identity {
            return Err(Error::Conflict("Pi original native root was replaced".into()));
        }
        <NativeVolumeView<A, O> as NativeProcessView<P>>::validate_for_dispatch_with_native_work(self.scratch.as_ref(), meter)?.await?;
        for binding in &self.projects {
            let mapping = &self.manifest.volumes[binding.index];
            let actual = binding.mount.snapshot_with_work(&meter.native_mount_work(), &self.cancellation).await.map_err(native_view_error)?;
            let NativeVolumeNamespace::LazyMount { snapshot } = &mapping.namespace else {
                return Err(invalid("Pi lazy namespace kind changed"));
            };
            if &actual != snapshot { return Err(Error::Conflict("Pi original lazy source cut changed before launch".into())); }
        }
        if meter.root_identity(&self.manifest.options.root).map_err(native_error)? != self.root_identity {
            return Err(Error::Conflict("Pi native root changed during dispatch admission".into()));
        }
        Ok(())
    }

    async fn publish_original<P: StreamProvider + 'static>(
        &self, owner: NativeViewOwner<'_, P>, work: &NativeMaterializationWork,
        mut state: PiNativePublicationState,
    ) -> std::result::Result<acyclic_harness::filesystem::NativeViewPublicationReceipt, NativeViewPublicationError> {
        let mut failed_destination = None;
        let result = async {
            owner.verify_with_native_work(work).await?;
            if work.root_identity(&self.manifest.options.root).map_err(native_error)? != self.root_identity {
                return Err(Error::Conflict("Pi native root changed before publication".into()));
            }
            for binding in &self.projects {
                let mapping = &self.manifest.volumes[binding.index];
                if !mapping.writable { continue; }
                failed_destination = Some(binding.index);
                let guard_error = Mutex::new(None);
                binding.mount.sync_with_permit_work_guarded(binding.publication,
                    &work.native_mount_work(), &self.cancellation, |actual| lazy_publication_guard(
                        owner, &mapping.volume, &binding.verifier, &binding.scope, actual, &guard_error,
                    )).await.map_err(|cause| {
                        guard_error.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take().unwrap_or_else(|| native_view_error(cause))
                    })?;
                // The genuine SDK acknowledgement precedes every later
                // fallible check; move its preadmitted original mapping.
                state.completed(binding.index);
                failed_destination = None;
                work.remaining().map_err(native_error)?;
            }
            Ok(())
        }.await;
        if let Err(cause) = result { return Err(state.failure(cause, failed_destination)); }
        let scratch = match <NativeVolumeView<A, O> as NativeProcessView<P>>::publish_with_native_work(
            self.scratch.as_ref(), owner, work,
        ) {
            Ok(future) => match future.await {
                Ok(receipt) => receipt,
                Err(cause) => return Err(state.failure(cause, None)),
            },
            Err(cause) => return Err(state.failure(cause, None)),
        };
        match scratch {
            Ok(mut receipt) => {
                state.completed.append(&mut receipt.completed);
                state.custody.absorb(&mut receipt.custody).expect("same original publication ledger and preadmitted total");
                Ok(acyclic_harness::filesystem::NativeViewPublicationReceipt { completed: state.completed, custody: state.custody })
            }
            Err(mut failure) => {
                state.completed.append(&mut failure.completed);
                state.custody.absorb(&mut failure.custody).expect("same original publication ledger and preadmitted total");
                failure.completed = state.completed;
                failure.custody = state.custody;
                Err(failure)
            }
        }
    }
}

struct PiBuiltinRelative { value: String, _custody: acyclic_fs::NativeAllocation }
fn builtin_relative(
    mapping: &NativeVolumeMapping, root: &Path, relative: &Path, work: &NativeMaterializationWork,
) -> Result<PiBuiltinRelative> {
    let prefix = mapping.path.strip_prefix(root).map_err(|_| invalid("Pi builtin mapping escaped its original root"))?;
    let components = || prefix.components().chain(relative.components());
    let mut length = 0usize;
    let mut count = 0usize;
    if relative.as_os_str().is_empty() { return Err(invalid("Pi builtin requires an actual file leaf")); }
    for component in components() {
        let Component::Normal(value) = component else { return Err(invalid("Pi builtin locator contains a non-normal component")); };
        let value = value.to_str().ok_or_else(|| invalid("Pi builtin locator is not UTF-8"))?;
        if value.contains(['\\', '\0']) { return Err(invalid("Pi builtin locator contains a literal escape")); }
        length = length.checked_add(value.len()).and_then(|length| length.checked_add(usize::from(count > 0)))
            .ok_or_else(|| native_error(acyclic_fs::WorkError::Overflow.into()))?;
        count += 1;
    }
    let mut custody = work.claim(length as u64).map_err(native_error)?;
    let mut value = String::new();
    value.try_reserve_exact(length).map_err(|cause| native_error(acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause)))?;
    if let Err(cause) = custody.resize_retained(value.capacity() as u64) {
        drop(value);
        return Err(Error::NativeWork { cause, custody: Some(custody) });
    }
    work.charge(WorkCounters { bytes_copied: length as u64, source_path_components: count as u64,
        ..WorkCounters::default() }).map_err(native_error)?;
    for component in components() {
        if !value.is_empty() { value.push('/'); }
        let Component::Normal(component) = component else { unreachable!("validated borrowed components") };
        value.push_str(component.to_str().expect("validated borrowed UTF-8 component"));
    }
    Ok(PiBuiltinRelative { value, _custody: custody })
}

struct PiNativeSnapshots { value: Vec<(usize, acyclic_fs::LazySnapshotRef)>, custody: acyclic_fs::NativeAllocation }

struct PiNativeMappingPayload { mapping: NativeVolumeMapping, custody: acyclic_fs::NativeAllocation }
struct PiNativePublicationState {
    completed: Vec<NativeVolumeMapping>,
    copies: Vec<Option<PiNativeMappingPayload>>,
    custody: acyclic_fs::NativeAllocation,
    _copy_slots: acyclic_fs::NativeAllocation,
}
impl PiNativePublicationState {
    fn new(manifest: &NativeViewManifest, work: &NativeMaterializationWork) -> Result<Self> {
        let completed = pi_native_vec(manifest.volumes.len(), work)?;
        let copies = pi_native_vec(manifest.volumes.len(), work)?;
        let mut state = Self { completed: completed.0, copies: copies.0, custody: completed.1, _copy_slots: copies.1 };
        for mapping in &manifest.volumes {
            let (mapping, custody) = acyclic_harness::filesystem::copy_native_mapping_with_work(mapping, work)?;
            state.copies.push(Some(PiNativeMappingPayload { mapping, custody }));
        }
        Ok(state)
    }
    fn completed(&mut self, index: usize) {
        let PiNativeMappingPayload { mapping, mut custody } = self.copies[index].take().expect("actual preadmitted original destination");
        self.completed.push(mapping);
        self.custody.absorb(&mut custody).expect("same original publication ledger and preadmitted total");
    }
    fn failure(mut self, cause: Error, index: Option<usize>) -> NativeViewPublicationError {
        let destination = index.and_then(|index| self.copies.get_mut(index)).and_then(Option::take)
            .map(|PiNativeMappingPayload { mapping, mut custody }| {
                self.custody.absorb(&mut custody).expect("same original publication ledger and preadmitted total");
                mapping
            });
        NativeViewPublicationError { cause, destination, completed: self.completed, custody: self.custody,
            rendering_failure: None, diagnostic: None }
    }
}

fn pi_native_string(value: &str, work: &NativeMaterializationWork) -> Result<(String, acyclic_fs::NativeAllocation)> {
    let mut custody = if value.is_empty() { work.retain_owned(0) } else { work.claim(value.len() as u64) }.map_err(native_error)?;
    let mut copied = String::new();
    copied.try_reserve_exact(value.len()).map_err(|cause| native_error(acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause)))?;
    work.charge(WorkCounters { bytes_copied: value.len() as u64, ..WorkCounters::default() }).map_err(native_error)?;
    copied.push_str(value);
    if let Err(cause) = custody.resize_retained(copied.capacity() as u64) {
        drop(copied);
        return Err(Error::NativeWork { cause, custody: Some(custody) });
    }
    Ok((copied, custody))
}

fn pi_native_vec<T>(capacity: usize, work: &NativeMaterializationWork) -> Result<(Vec<T>, acyclic_fs::NativeAllocation)> {
    let bytes = capacity.checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| native_error(acyclic_fs::WorkError::Overflow.into()))?;
    let mut custody = if bytes == 0 { work.retain_owned(0) } else { work.claim(bytes as u64) }.map_err(native_error)?;
    let mut value = Vec::new();
    value.try_reserve_exact(capacity)
        .map_err(|cause| native_error(acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause)))?;
    if let Err(cause) = custody.resize_retained((value.capacity() * std::mem::size_of::<T>()) as u64) {
        drop(value);
        return Err(Error::NativeWork { cause, custody: Some(custody) });
    }
    Ok((value, custody))
}

impl<P, A, O, D, S> NativeProcessView<P> for PiNativeView<A, O, D, S>
where P: StreamProvider + 'static, A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
      O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
      D: acyclic_fs::DemandSource + 'static, S: acyclic_fs::LazyWorkspaceStore + 'static,
{
    fn manifest(&self) -> &NativeViewManifest { &self.manifest }
    fn operation_cancellation(&self) -> &CancellationToken { &self.cancellation }
    fn original_native_work(&self) -> Option<&NativeMaterializationWork> { Some(self._manifest_custody.work()) }

    fn validate_for_dispatch(&self, budget: WorkBudget) -> std::result::Result<NativeFutureBundle<'_, std::result::Result<OperationReceipt<()>, (Error, WorkCounters)>>, (Error, WorkCounters)> {
        let meter = pi_meter(budget, &self.cancellation).map_err(pi_constructor_failure)?;
        let nested = meter.clone();
        NativeFutureBundle::new(&meter, || async move { Ok(pi_inline_receipt(self.validate::<P>(&nested).await, &nested)) })
            .map_err(|error| (error, meter.receipt()))
    }

    fn validate_working_directory<'a>(&'a self, cwd: &'a Path, budget: WorkBudget) -> std::result::Result<NativeFutureBundle<'a, std::result::Result<OperationReceipt<()>, (Error, WorkCounters)>>, (Error, WorkCounters)> {
        let meter = pi_meter(budget, &self.cancellation).map_err(pi_constructor_failure)?;
        let nested = meter.clone();
        NativeFutureBundle::new(&meter, || async move {
            let result = cwd.strip_prefix(&self.manifest.options.root)
                .map_err(|_| invalid("Pi working directory escapes its original namespace"))
                .and_then(|relative| nested.validate_working_directory(&self.manifest.options.root, self.root_identity, relative).map_err(native_error));
            Ok(pi_inline_receipt(result, &nested))
        }).map_err(|error| (error, meter.receipt()))
    }

    fn publish<'a>(&'a self, owner: NativeViewOwner<'a, P>) -> std::result::Result<NativeFutureBundle<'a, std::result::Result<WorkCounters, NativePublicationFailure>>, (Error, WorkCounters)> {
        let budget = self.manifest.validation_budget().map_err(|error| (error, WorkCounters::default()))?;
        let meter = pi_meter(budget, &self.cancellation).map_err(pi_constructor_failure)?;
        let state = PiNativePublicationState::new(&self.manifest, &meter).map_err(|error| (error, meter.receipt()))?;
        let nested = meter.clone();
        NativeFutureBundle::new(&meter, || async move {
            Ok(self.publish_original(owner, &nested, state).await.map(|_| nested.receipt()).map_err(|failure| NativePublicationFailure {
                message: failure.cause.to_string(), destination: failure.destination,
                completed: failure.completed, work: nested.receipt(),
            }))
        }).map_err(|error| (error, meter.receipt()))
    }

    fn retain_native_namespace_scopes<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, acyclic_harness::filesystem::NativeProcessNamespaceScopes>> {
        NativeFutureBundle::new(work, || async {
            self.cancellation.check().map_err(|_| native_error(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
            if work.root_identity(&self.manifest.options.root).map_err(native_error)? != self.root_identity {
                return Err(Error::Conflict("Pi original native namespace was replaced before process scope acquisition".into()));
            }
            let admitted = pi_native_vec(self.projects.len(), work)?;
            let mut bundle = acyclic_harness::filesystem::NativeProcessNamespaceScopes { scopes: admitted.0, custody: admitted.1 };
            for binding in &self.projects {
                bundle.scopes.push(binding.mount.begin_original_work(&work.native_mount_work()).map_err(native_view_error)?);
            }
            Ok(bundle)
        })
    }

    fn validate_for_dispatch_with_native_work<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, ()>> {
        NativeFutureBundle::new(work, || self.validate::<P>(work))
    }

    fn validate_working_directory_with_native_work<'a>(
        &'a self, cwd: &'a Path, work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, ()>> {
        NativeFutureBundle::new(work, || async {
            self.cancellation.check().map_err(|_| native_error(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
            let relative = cwd.strip_prefix(&self.manifest.options.root)
                .map_err(|_| invalid("Pi working directory escapes its original namespace"))?;
            work.validate_working_directory(&self.manifest.options.root, self.root_identity, relative).map_err(native_error)
        })
    }

    fn publish_with_native_work<'a>(
        &'a self, owner: NativeViewOwner<'a, P>, work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, std::result::Result<acyclic_harness::filesystem::NativeViewPublicationReceipt, NativeViewPublicationError>>> {
        let state = PiNativePublicationState::new(&self.manifest, work)?;
        NativeFutureBundle::new(work, || async { Ok(self.publish_original(owner, work, state).await) })
    }
}

/// The SDK lends its actual invocation carrier outside the work mutex. Root
/// owner/grant IO uses that same complete ledger; no budget, receipt, allocation
/// loan, cancellation root or diagnostic is reconstructed at this boundary.
pub(crate) async fn lazy_publication_guard<P: StreamProvider>(
    owner: NativeViewOwner<'_, P>, volume: &VolumeRef, verifier: &AuthorityVerifier, scope: &Scope,
    work: acyclic_fs::native_mount::NativeMountMeterWork, original_error: &Mutex<Option<Error>>,
) -> std::result::Result<(), acyclic_fs::native_mount::MountSourceError> {
    let work = work.native_materialization_work();
    let result = async {
        owner.verify_with_native_work(&work).await?;
        ContentGrant::verify(verifier, scope, volume, VolumeOperation::Write)?;
        owner.require_volume_grant_with_native_work(volume, VolumeOperation::Write, &work)?;
        Ok(())
    }.await;
    result.map_err(|error| retain_publication_guard_error(error, original_error))
}

fn retain_publication_guard_error(
    error: Error, original_error: &Mutex<Option<Error>>,
) -> acyclic_fs::native_mount::MountSourceError {
    use acyclic_fs::{NativeTreeMaterializationError, native_mount::MountSourceError};
    let signal = match &error {
        Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, .. } => MountSourceError::Cancelled,
        Error::NativeWork { cause: NativeTreeMaterializationError::Work(work), .. } => MountSourceError::Work(*work),
        _ => MountSourceError::Stale,
    };
    // Stale is only the SDK callback's interruption signal. The public boundary
    // returns this entire original Root error, including any retained custody.
    let mut original = original_error.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if original.is_none() { *original = Some(error); }
    signal
}

fn pi_meter(budget: WorkBudget, cancellation: &CancellationToken) -> Result<NativeMaterializationWork> {
    if !budget.has_finite_allowances() { return Err(invalid("Pi native work requires finite allowances")); }
    NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}

fn pi_constructor_failure(error: Error) -> (Error, WorkCounters) {
    let work = match &error { Error::NativeConstruction { work, .. } => *work, _ => WorkCounters::default() };
    (error, work)
}

fn pi_inline_receipt<T>(result: Result<T>, meter: &NativeMaterializationWork) -> std::result::Result<OperationReceipt<T>, (Error, WorkCounters)> {
    result.map(|value| OperationReceipt { value, work: meter.receipt() }).map_err(|error| (error, meter.receipt()))
}

fn pi_accept<T>(result: MeasuredResult<OperationReceipt<T>, Error>, meter: &NativeMaterializationWork) -> Result<T> {
    match result {
        Ok(receipt) => { meter.spent(receipt.work).map_err(native_error)?; meter.remaining().map_err(native_error)?; Ok(receipt.value) }
        Err(failure) => { meter.spent(*failure.work).map_err(native_error)?; Err(failure.error) }
    }
}
fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error { Error::NativeWork { cause, custody: None } }
pub(crate) fn native_view_error(cause: acyclic_fs::native_mount::MountLifecycleError) -> Error { cause.into() }


fn has_parent(path: &Path) -> bool { path.components().any(|part| matches!(part, Component::ParentDir)) }
fn invalid(message: &str) -> Error { Error::Invalid(message.to_owned()) }
fn decode_error(error: serde_json::Error) -> Error { invalid(&error.to_string()) }

#[cfg(test)]
#[path = "pi_native_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "pi_native_process_tests.rs"]
mod process_tests;

//! Pi's pinned native command over the existing task/effect and namespace owners.

use super::{PiScratchAllocation, PiTaskBinding};
use acyclic_fs::{CancellationToken, LazyMount, NativeMaterializationWork, PublicationPermit, WorkBudget, WorkCounters};
use acyclic_harness::{
    Error, InteractionId, OperationId, Result,
    conversation::{ContentGrant, FileRef, VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, EffectGuarantee, Scope},
    durable_host::{TaskJournalOwner, task_interaction_id},
    effect_host::{TaskEffectPlan},
    filesystem::{
        BorrowedNativeVolumeBinding, EFFECT_TASK_COMMAND_KIND, FilesystemHost,
        NATIVE_PROCESS_EFFECT_KIND, NativePreparationReceipt, NativePreparationRoot,
        NativeProcessRequest, NativeViewManifest, NativeViewOptions,
        NativeVolumeNamespace, NativeVolumeView, NativeTaskViewDrainReceipt,
        NativeVolumeMapping, NativeExistingLazyVolume,
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
use std::{collections::BTreeMap, path::{Component, Path, PathBuf}, sync::Arc};

pub(crate) const PI_NATIVE_PROVIDER: &str = "acyclic.pi.native";
pub(crate) const PI_NATIVE_VERSION: &str = "1.1.0";

/// References only.
pub(crate) struct PiNativePaths {
    pub(crate) cwd: PathBuf,
    pub(crate) session_file: PathBuf,
    pub(crate) session_directory: PathBuf,
    pub(crate) view_root: PathBuf,
    pub(crate) operation_root: PathBuf,
}


pub(crate) fn planned_native_paths(
    resources: &super::ServiceResources, root_session: &str, task: acyclic_harness::TaskId,
    root_key: Option<&str>, work: &NativeMaterializationWork,
) -> Result<(PiNativePaths, acyclic_fs::NativeAllocation)> {
    work.remaining().map_err(native_error)?;
    let route = root_key.map(|key| native_root_route(key, work)).transpose()?;
    let task = native_task_directory(resources, root_session, task, work)?;
    let mut view = work.join_path(&task.0, Path::new("view")).map_err(native_error)?;
    let mut session = work.join_path(&view.0, Path::new("scratch")).map_err(native_error)?;
    let mut cwd = match &route {
        Some(route) => work.join_path(&view.0, Path::new(&route.0)).map_err(native_error)?,
        None => {
            let (path, custody) = work.copy_path(&session.0).map_err(native_error)?;
            (path, custody)
        }
    };
    let mut session_file = work.join_path(&session.0, Path::new("session.jsonl")).map_err(native_error)?;
    let mut operation = work.join_path(&task.0, Path::new("preparation")).map_err(native_error)?;
    let custody = work.retain_owned(std::mem::size_of::<PiNativePaths>() as u64).map_err(native_error)?;
    let mut planned = (PiNativePaths { cwd: cwd.0, session_file: session_file.0,
        session_directory: session.0, view_root: view.0, operation_root: operation.0 }, custody);
    for incoming in [&mut cwd.1, &mut session_file.1, &mut session.1,
        &mut view.1, &mut operation.1]
    {
        planned.1.absorb(incoming).map_err(native_error)?;
    }
    Ok(planned)
}

fn native_root_route(
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
    let mut route = [0_u8; 23];
    route[0] = b'r';
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode_slice(identity, &mut route[1..])
        .expect("exact root identity route extent");
    pi_native_string(std::str::from_utf8(&route).expect("root route uses ASCII encoding"), work)
}



fn native_task_directory(
    resources: &super::ServiceResources, root_session: &str, task: acyclic_harness::TaskId, work: &NativeMaterializationWork,
) -> Result<(PathBuf, acyclic_fs::NativeAllocation)> {
    if root_session.is_empty() || !resources.data.is_absolute() {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi native path references require their original absolute service root" });
    }
    work.charge(WorkCounters { bytes_hashed: root_session.len() as u64 + 16, ..WorkCounters::default() }).map_err(native_error)?;
    let session_hash = blake3::hash(root_session.as_bytes()).to_hex();
    let task_hash = blake3::hash(&task.into_bytes()).to_hex();
    let sessions = work.join_path(&resources.data, Path::new("sessions")).map_err(native_error)?;
    let service = work.join_path(&sessions.0, Path::new(session_hash.as_str())).map_err(native_error)?;
    drop(sessions);
    let agents = work.join_path(&service.0, Path::new("agents")).map_err(native_error)?;
    drop(service);
    let task = work.join_path(&agents.0, Path::new(task_hash.as_str())).map_err(native_error)?;
    Ok(task)
}

pub(crate) fn native_view_options(
    resources: &super::ServiceResources, root_session: &str, task: acyclic_harness::TaskId,
    maximum_volumes: u32, work_per_volume: WorkBudget, work: &NativeMaterializationWork,
) -> Result<(NativeViewOptions, acyclic_fs::NativeAllocation)> {
    if maximum_volumes == 0 || !work_per_volume.has_finite_allowances() {
        return Err(invalid("Pi native options require their original finite composition allowances"));
    }
    let task = native_task_directory(resources, root_session, task, work)?;
    let view = work.join_path(&task.0, Path::new("view")).map_err(native_error)?;
    Ok((NativeViewOptions { root: view.0, maximum_volumes, work_per_volume }, view.1))
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


#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PiNativeSourceMode { CallerTool, Scratch }

/// Exact native source selection retained in the original child admission.
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

/// Closed launch graph diagnostic inventory: original derives, camel-case source fields, snake-case mode tags and pinned serde primitive expectations.
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

    pub(crate) fn validate_session(
        &self, child: &str, cwd: &Path, bytes: &[u8], maximum_entries: u32, maximum_bytes: u64,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining().map_err(native_error)?;
        if maximum_entries == 0 || maximum_bytes == 0 || bytes.len() as u64 > maximum_bytes
            || self.entries_json.len() as u64 > maximum_bytes || !cwd.is_absolute()
            || child.is_empty() || self.parent_session_id.is_empty() || child == self.parent_session_id
            || self.captured_at.is_empty() || !Path::new(&self.parent_cwd).is_absolute()
        {
            return Err(invalid("Pi native session differs from its original source cut or bounds"));
        }
        let mut digest = [0_u8; 64];
        work.charge(WorkCounters { bytes_hashed: self.entries_json.len() as u64, ..Default::default() }).map_err(native_error)?;
        hex::encode_to_slice(Sha256::digest(self.entries_json.as_bytes()), &mut digest)
            .expect("SHA256 hexadecimal output has its fixed exact extent");
        if self.sha256.as_bytes() != digest {
            return Err(invalid("Pi native session differs from its original source digest"));
        }
        let selected = native_cut_decode::<Vec<Value>>(self.entries_json.as_bytes(), maximum_bytes,
            std::mem::size_of::<Vec<Value>>() as u64, JsonValueSchema::Root, 0, work)?;
        if selected.0.len() as u64 > u64::from(maximum_entries) {
            return Err(invalid("Pi native source cut exceeds its entry allowance"));
        }
        match self.mode {
            PiNativeSourceMode::CallerTool if self.owner_entry_id.as_deref().is_some_and(|owner| !owner.is_empty())
                && self.source_leaf_id.as_deref().is_some_and(|leaf| !leaf.is_empty()) => {}
            PiNativeSourceMode::Scratch if self.owner_entry_id.is_none() && self.source_leaf_id.is_none()
                && self.parent_leaf_id.is_none() && selected.0.is_empty() => {}
            _ => return Err(invalid("Pi native source mode differs from its authentic SDK selection")),
        }
        let mut previous: Option<&str> = None;
        let mut ids = pi_native_vec(selected.0.len(), work)?;
        for entry in &selected.0 {
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
            ids.0.push(id);
        }
        native_sort_entry_ids(&mut ids.0, work)?;
        if ids.0.windows(2).any(|pair| pair[0] == pair[1]) {
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
        if header.0.kind != "session" || header.0.version != 3 || header.0.id != child
            || header.0.timestamp != self.captured_at || Path::new(&header.0.cwd) != cwd
        {
            return Err(invalid("Pi child header differs from its prepared native identity"));
        }
        for entry in &selected.0 {
            let actual = native_cut_decode::<Value>(
                lines.next().ok_or_else(|| invalid("Pi child lost a source entry"))?.as_bytes(), maximum_bytes,
                std::mem::size_of::<Value>() as u64, JsonValueSchema::Root, 0, work)?;
            if &actual.0 != entry {
                return Err(invalid("Pi child entry differs from its original native source"));
            }
        }
        if lines.next().is_some() {
            return Err(invalid("Pi child session contains entries outside its source cut"));
        }
        Ok(())
    }
}

fn native_cut_decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8], maximum: u64, audited_fixed_bytes: u64, schema: JsonValueSchema,
    diagnostic_label_bytes: u64, work: &NativeMaterializationWork,
) -> Result<(T, acyclic_fs::NativeAllocation)> {
    super::pi_root_provider::guarded_pi_decode(bytes, maximum, audited_fixed_bytes, schema, diagnostic_label_bytes, work)
}

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

/// Captured once by the original root SDK bootstrap and kept only in the protected Pi owner file.
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

    /// Copy the immutable protected runtime directly, paying known metadata and actual owned buffers before construction on the original ledger.
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
    pub(crate) fn intent_invocation(
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

    pub(crate) fn bind_invocation(
        &self, intent: &PiNativeLaunchIntent, caller_environment: &BTreeMap<String, String>,
        work: &NativeMaterializationWork,
    ) -> Result<(PiNativeInvocation, acyclic_fs::NativeAllocation)> {
        work.remaining().map_err(native_error)?;
        self.validate_with_native_work(work)?;
        intent.validate(work)?;
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

/// Host-selected complete invocation.
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

/// Seeded immutable input consumed only through the actual created child's TaskContext.
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

    pub(crate) async fn load(
        context: &TaskContext, reference: &FileRef, work: &NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        if context.durable_task_id().is_none() || reference.descriptor().media_type() != "application/json" {
            return Err(invalid("Pi launch input requires its actual durable child content binding"));
        }
        let bytes = context.read_file_with_native_work(reference, work).await?;
        let maximum = context.scope().limits().file_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
        let intent = super::pi_root_provider::guarded_pi_decode::<Self>(
            bytes.as_ref(), maximum, std::mem::size_of::<Self>() as u64, JsonValueSchema::None, PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES, work)?;
        intent.0.validate(work)?;
        Ok(intent)
    }

    pub(crate) fn validate(&self, work: &NativeMaterializationWork) -> Result<()> {
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
        self.source.validate_session(&self.child_session_id, &self.invocation.cwd,
            self.session_bytes.as_bytes(), self.maximum_entries, self.maximum_bytes, work)
    }

    /// The approved CLI locator and staged SDK bytes must name the same leaf.
    pub(crate) fn request_with_work(&self, binding: &PiTaskBinding, manifest: &NativeViewManifest,
        work: &NativeMaterializationWork) -> Result<(NativeProcessRequest, acyclic_fs::NativeAllocation)> {
        self.validate(work)?;
        let scratch = manifest.volumes.iter().find(|mapping| mapping.volume == binding.scratch.volume)
            .ok_or_else(|| invalid("Pi launch has no original scratch mapping"))?;
        if self.invocation.session_file.strip_prefix(&scratch.path).ok() != Some(Path::new(&self.session_path)) {
            return Err(invalid("Pi staged native session differs from the approved CLI session leaf"));
        }
        self.invocation.request_with_work(binding, manifest, work)
    }

    /// Publishes on the caller's original work and retains the actual FileRef ownership through the prepared generation's final consumer.
    pub(crate) async fn publish_session(
        &self, context: &TaskContext, operation: OperationId, work: &NativeMaterializationWork,
    ) -> Result<(FileRef, acyclic_fs::NativeAllocation)> {
        self.validate(work)?;
        if context.durable_task_id().is_none() {
            return Err(invalid("Pi session publication requires its admitted child"));
        }
        context.stage_file_once_with_native_work(operation, &self.session_path, self.session_bytes.as_bytes(),
            "application/x-ndjson", "Pi child session", work).await
    }
}

/// Child task content is bound through created-resource access, not a root signed scope pre-granted to guessed future child volume names.
pub(crate) async fn stage_command(
    context: &TaskContext, command: OperationId, request: &NativeProcessRequest, work: &NativeMaterializationWork,
) -> Result<(WorkflowCommand, TaskEffectPlan, acyclic_fs::NativeAllocation)> {
    if context.durable_task_id().is_none() { return Err(invalid("Pi native launch requires its original durable TaskContext")); }
    let maximum = context.scope().limits().file_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
    // Declared first so every staged payload drops before its combined allocation.
    let mut custody = work.retain_owned(0).map_err(native_error)?;
    let mut request = stage_context_artifact(
        context, command, "request", request, "application/json", maximum, work).await?;
    custody.absorb(&mut request.1).map_err(native_error)?;
    let mut schema = stage_context_artifact(
        context, command, "schema", &result_schema()?, "application/schema+json", maximum, work).await?;
    custody.absorb(&mut schema.1).map_err(native_error)?;
    let mut provider = pi_native_string(PI_NATIVE_PROVIDER, work)?;
    custody.absorb(&mut provider.1).map_err(native_error)?;
    let mut effect_kind = pi_native_string(NATIVE_PROCESS_EFFECT_KIND, work)?;
    custody.absorb(&mut effect_kind.1).map_err(native_error)?;
    let plan = TaskEffectPlan {
        provider: provider.0, guarantee: EffectGuarantee::AtMostOnce, effect_kind: effect_kind.0,
        request: request.0, result_schema: schema.0,
    };
    let mut payload = stage_context_artifact(
        context, command, "plan", &plan, "application/json", maximum, work).await?;
    custody.absorb(&mut payload.1).map_err(native_error)?;
    let mut kind = pi_native_string(EFFECT_TASK_COMMAND_KIND, work)?;
    custody.absorb(&mut kind.1).map_err(native_error)?;
    Ok((WorkflowCommand { operation_id: command, kind: kind.0, payload: payload.0 }, plan, custody))
}

async fn stage_context_artifact(
    context: &TaskContext, command: OperationId, role: &str, value: &impl Serialize,
    media_type: &str, maximum: u64, work: &NativeMaterializationWork,
) -> Result<(FileRef, acyclic_fs::NativeAllocation)> {
    let encoded = super::pi_root_provider::guarded_pi_bytes(value, maximum, work)?;
    let operation = native_artifact_operation(command, role, work)?;
    let mut buffer = [0; 96];
    let mut writer = std::io::Cursor::new(buffer.as_mut_slice());
    std::io::Write::write_fmt(&mut writer, format_args!("pi-native/{command}/{role}.json"))
        .map_err(|_| invalid("Pi native artifact locator exceeds its fixed original bound"))?;
    let length = writer.position() as usize;
    let path = std::str::from_utf8(&buffer[..length])
        .map_err(|_| invalid("Pi native artifact locator is not UTF-8"))?;
    let result = context.stage_file_once_with_native_work(operation, path, &encoded.0, media_type, role, work).await;
    drop(encoded);
    result
}

fn native_artifact_operation(
    command: OperationId, role: &str, work: &NativeMaterializationWork,
) -> Result<OperationId> {
    let digest = native_digest(&("acyclic.pi/native-content/v1", command, role), 1024, work)?;
    let mut operation = [0; 16];
    operation.copy_from_slice(&digest[..16]);
    Ok(OperationId::from_bytes(operation))
}

fn native_digest(value: &impl Serialize, maximum: u64, work: &NativeMaterializationWork) -> Result<[u8; 32]> {
    let encoded = super::pi_root_provider::canonical_pi_value(value, maximum, work)?;
    work.charge(WorkCounters { bytes_hashed: encoded.0.len() as u64, ..WorkCounters::default() }).map_err(native_error)?;
    let digest = *blake3::hash(&encoded.0).as_bytes();
    drop(encoded);
    Ok(digest)
}





/// The production policy is the original admitted native-effect capability, restricted to the immutable protected host and this exact request.
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
    let title = pi_native_string(std::str::from_utf8(&title.0[..title.1]).expect("formatter copies UTF-8"), work)?;
    let interaction = (Interaction::approval(title.0, command, digest)?, title.1);
    journal.open_interaction_with_native_work(approval, &interaction.0, work)?.await?;
    owner.verify_with_native_work(false, work).await?;
    let respond = native_fixed_text(format_args!("interaction:respond:{approval}"))?;
    let encoded_grants = super::pi_root_provider::guarded_pi_bytes(
        &["interaction:resolve", std::str::from_utf8(&respond.0[..respond.1]).expect("formatter copies UTF-8")], 1024, work)?;
    let grants = super::pi_root_provider::guarded_pi_decode::<Capabilities>(
        &encoded_grants.0, 1024, std::mem::size_of::<Capabilities>() as u64, JsonValueSchema::None, NATIVE_CAPABILITIES_DIAGNOSTIC_LABEL_BYTES, work)?;
    let responder = protected.conversation_authority_with_native_work(owner.task_id(), binding.agent_id, &grants.0, work)?;
    journal.resolve_interaction_with_native_work(approval,
        &InteractionResponse::Approval { approved: true, reason: None }, &responder.0.2, work)?.await?;
    owner.verify_with_native_work(false, work).await?;
    Ok(approval)
}

fn native_fixed_text(arguments: std::fmt::Arguments<'_>) -> Result<([u8; 256], usize)> {
    let mut bytes = [0; 256];
    let mut writer = std::io::Cursor::new(bytes.as_mut_slice());
    std::io::Write::write_fmt(&mut writer, arguments)
        .map_err(|_| native_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
    let length = writer.position() as usize;
    Ok((bytes, length))
}

/// Created only by the original authenticated native preparation below.
pub(crate) struct PiPreparedScratch<A, O> {
    pub(crate) view: Arc<NativeVolumeView<A, O>>,
    pub(crate) allocation: NativePreparationRoot,
    pub(crate) operation: acyclic_fs::OperationId,
    task: acyclic_harness::TaskId,
    scratch: VolumeRef,
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
    /// Mounts the existing lazy child workspace without a copied project tree or another physical-root watcher.
    #[allow(clippy::too_many_arguments, reason = "original created project, native allocation, and scoped publication selection")]
    pub(crate) async fn mount_project_with_work<P, A, O, D, S>(
        &self, owner: &Arc<TaskJournalOwner<P>>, shared: &Arc<NativeTaskContext>, binding: &PiTaskBinding,
        root_key: &str, workspace: &acyclic_fs::LazyWorkspace<A, O, D, S>, filesystem: Arc<FilesystemHost<A, O>>,
        original_generation: &acyclic_harness::resources::GenerationRef,
        authority: &(AuthorityVerifier, Scope, acyclic_fs::NativeAllocation),
        publication: PublicationPermit, writable: bool, retained: &mut Option<Arc<LazyMount<A, O, D, S>>>,
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
                ContentGrant::verify(&authority.0, &authority.1, volume, operation)?;
                owner.require_volume_grant_with_native_work(volume, operation, meter)?;
            }
            let allocation = self.witnessed_allocation()?;
            let route = native_root_route(root_key, meter)?;
            let mut path = meter.join_path(&allocation.root, Path::new(&route.0)).map_err(native_error)?;
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
                let mut slot = Arc::<LazyMount<A, O, D, S>>::new_uninit();
                let mut mount = match workspace.mount_with_work(path.0.as_path(), options,
                    Some(source_guard), &original_volume_work, cancellation).await {
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
                // SAFETY: the exclusive slot above contains exactly the actual successful SDK mount; no uninitialized value is exposed.
                *retained = Some(unsafe { slot.assume_init() });
                if let Some(cause) = custody_failure {
                    *retained_allocation = Some(allocation);
                    if retained.as_ref().expect("actual mount retained above")
                        .abandon_with_work(&original_volume_work).is_ok()
                    {
                        *retained = None;
                        *retained_allocation = None;
                    }
                    return Err(native_view_error(cause));
                }
            }
            let mount = retained.as_ref().ok_or_else(|| invalid("Pi lazy mount acquisition lost its actual owner"))?;
            if mount.path() != path.0.as_path() {
                return Err(Error::Conflict("Pi retained project mount belongs to another native namespace".into()));
            }
            let snapshot = mount.snapshot_with_work(&original_volume_work, cancellation).await.map_err(native_view_error)?;
            if snapshot.workspace_id != actual.id()
                || snapshot.authored_generation.digest().as_bytes().as_slice() != original_generation.as_resource().key()
            {
                return Err(Error::Conflict("Pi lazy mount differs from the original actual filesystem workspace or seed generation".into()));
            }
            owner.verify_with_native_work(false, meter).await?;
            let authority_revision = native_digest(&(authority.0.audience(), &authority.1),
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
            metadata.absorb(&mut path.1).map_err(native_error)?;
            let result = (
                NativeVolumeMapping { volume: volume_copy.0, generation: generation_copy.0,
                    namespace, path: path.0, writable, authority_revision },
                existing, callback_guard, metadata,
            );
            Ok(result)
        }.await;
        result
    }

    /// Keeps the exact creator witness in its owner slot until every fallible preparation, verification, and Arc publication has completed.
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
        if seed.service_identity.is_none() {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Indeterminate,
                reason: "Pi mixed preparation has no original service namespace witness",
            });
        }
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
        owner.verify_with_native_work(false, meter).await?;
        let view = view.into_arc_with_native_work(meter)?;
        let seed = retained.take().expect("original creator slot was exclusively borrowed throughout preparation");
        Ok(PiPreparedScratch { view, allocation: seed.allocation.expect("completed original creator witness was verified above"),
            operation: seed.operation, task: seed.task, scratch: seed.scratch, _creator_packet: seed.packet,
            _metadata_custody: seed._metadata_custody })
    }

    /// Disposes this retained creator packet before launch.
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
    /// Used only by the host's authenticated Planned/no-receipt rollback path, never as a replacement for process cleanup after native dispatch.
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

    /// Disposal requires the SDK's whole physical drain receipt, followed by fresh original Created-Scratch WRITE for each retained creator deletion.
    pub(crate) async fn release_scratch_with_work<P: StreamProvider + 'static>(
        &mut self, owner: &TaskJournalOwner<P>, context: &TaskContext, command: OperationId,
        drained: &NativeTaskViewDrainReceipt, proof: &acyclic_harness::ChildScratchCreationProof,
        original: &NativeMaterializationWork, meter: &NativeMaterializationWork,
    ) -> Result<()>
    where
        A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
        O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
    {
        if owner.task_id() != self.task || context.durable_task_id() != Some(self.task)
            || proof.original_task() != self.task || proof.volume() != &self.scratch
            || self.operation != NativePreparationRoot::operation(owner)
            || self._creator_packet.operation_id() != self.operation
            || !self._metadata_custody.matches_work(original)
        {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi scratch disposal differs from its original created child and preparation" });
        }
        self.view.require_drained_for_disposal_with_native_work(
            owner.native_view_owner(), command, drained, original, meter)?.await?;
        self._creator_packet.release_guarded(meter, || async {
            proof.authorize_settlement_with_native_work(owner, meter).await
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
        mount.abandon_with_work(&volume_work).map_err(native_view_error)?;
    }
    if let Some(session) = startup.as_mut() {
        let state = session.cleanup_state();
        if state.destination == root || !state.destination.starts_with(root) {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi startup owner belongs to another original allocation" });
        }
        proof.authorize_settlement_with_native_work(owner, metadata_work).await?;
        session.stop_with_meter(&volume_work)
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
pub(crate) async fn native_root_seed<'a, P, A, O>(
    resources: &super::ServiceResources, root_session: &str, protected: &super::pi_owner::PiOwnerConfig,
    owner: &'a Arc<TaskJournalOwner<P>>, shared: &Arc<NativeTaskContext>, binding: &PiTaskBinding, options: &NativeViewOptions,
    intent: &PiNativeLaunchIntent, session: &FileRef,
    bindings: &[BorrowedNativeVolumeBinding<'a, A, O>], meter: &NativeMaterializationWork,
) -> Result<PiNativeRootSeed>
where P: StreamProvider + 'static, A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
      O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
{
    meter.admit(WorkCounters::UNCHARGED).map_err(native_error)?;
    if shared.context().durable_task_id() != Some(owner.task_id()) || owner.task_id() != binding.task_id || bindings.len() != 1
        || bindings[0].volume != &binding.scratch.volume || bindings[0].path != "scratch" || !bindings[0].writable
        || !matches!(bindings[0].namespace, NativeVolumeNamespace::MaterializedDirectory)
        || !options.work_per_volume.has_finite_allowances()
        || !matches!(binding.scratch.allocation, PiScratchAllocation::Created(_))
    {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi native seed metadata differs from its original admitted child" });
    }
    protected.authority_with_native_work(meter)?;
    intent.validate(meter)?;
    let mut session_generation = [0; 32];
    hex::decode_to_slice(session.version(), &mut session_generation).map_err(|_| Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi session has no original filesystem generation",
    })?;
    if session.volume() != &binding.scratch.volume || session.path() != intent.session_path
        || bindings[0].generation.as_resource().key() != session_generation
        || session.descriptor().byte_length() > intent.maximum_bytes
    {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "Pi seed session selection differs from its immutable native cut" });
    }
    let service = native_service_root(&resources.data, root_session, owner.task_id(), &options.root, meter)?;
    let operation = NativePreparationRoot::operation(owner);
    let task_directory = options.root.parent().ok_or(Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi native view has no original task namespace",
    })?;
    let mut operation_root = meter.join_path(task_directory, Path::new("preparation")).map_err(native_error)?;
    let target = meter.join_path(&operation_root.0, Path::new("target")).map_err(native_error)?;
    let backup = meter.join_path(&operation_root.0, Path::new("backup")).map_err(native_error)?;
    fn relative<'p>(path: &'p Path, data: &Path) -> Result<&'p Path> {
        path.strip_prefix(data).map_err(|_| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi native seed escaped its held private service root",
        })
    }
    let packet = acyclic_fs::materializer::NativeOwnedRootAllocation::new_with_native_work(operation,
        &resources.private_state_root, [relative(&options.root, &resources.data)?, relative(&operation_root.0, &resources.data)?,
            relative(&target.0, &resources.data)?, relative(&backup.0, &resources.data)?], meter).map_err(native_error)?;
    let mut service_root = meter.copy_path(service).map_err(native_error)?;
    let mut scratch = acyclic_harness::filesystem::copy_native_volume_with_work(&binding.scratch.volume, meter)?;
    let mut view_root = meter.copy_path(&options.root).map_err(native_error)?;
    let mut custody = meter.retain_owned(std::mem::size_of::<PiNativeRootSeed>() as u64).map_err(native_error)?;
    custody.absorb(&mut service_root.1).map_err(native_error)?;
    custody.absorb(&mut scratch.1).map_err(native_error)?;
    custody.absorb(&mut view_root.1).map_err(native_error)?;
    custody.absorb(&mut operation_root.1).map_err(native_error)?;
    Ok(PiNativeRootSeed { allocation: None, packet, view_root: view_root.0, operation_root: operation_root.0,
        operation, task: owner.task_id(), scratch: scratch.0, service_root: service_root.0,
        service_identity: None, _metadata_custody: custody })
}

fn result_schema() -> Result<&'static serde_json::value::RawValue> {
    serde_json::from_str(r#"{"type":"object","additionalProperties":false,"required":["success","exit_code","stdout","stderr","stop"],"properties":{"success":{"type":"boolean"},"exit_code":{"type":["integer","null"]},"stdout":{"type":"array","items":{"type":"integer","minimum":0,"maximum":255}},"stderr":{"type":"array","items":{"type":"integer","minimum":0,"maximum":255}},"stop":{"oneOf":[{"type":"null"},{"type":"object","additionalProperties":false,"required":["kind","message","cleanup_completed"],"properties":{"kind":{"enum":["output_limit","timeout","control_stopped","capture_failed"]},"message":{"type":"string"},"cleanup_completed":{"type":"boolean"}}}]}}}"#).map_err(decode_error)
}

fn pi_native_string(value: &str, work: &NativeMaterializationWork) -> Result<(String, acyclic_fs::NativeAllocation)> {
    let (bytes, custody) = work.copy_bytes(value.as_bytes()).map_err(native_error)?;
    Ok((String::from_utf8(bytes).expect("SDK copied the UTF-8 source unchanged"), custody))
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

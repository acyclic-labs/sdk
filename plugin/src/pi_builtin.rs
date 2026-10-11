//! Pi builtin IO is mediated by the original task owner, not by its cwd.
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use acyclic_harness::{Error, OperationId, Result, TaskId};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};

pub(crate) const PI_BUILTIN_COMMAND_KIND: &str = "acyclic.pi.builtin";
pub(crate) const MAXIMUM_BUILTIN_BYTES: u64 = 262_144;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PiBuiltinIo {
    Read { maximum_bytes: u64, #[serde(default)] variants: Vec<String> },
    Write { content: String },
    EditRead { maximum_bytes: u64 },
    EditWrite { content: String, expected_sha256: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiBuiltinRequest {
    pub(crate) caller: TaskId,
    pub(crate) sdk_operation: String,
    pub(crate) path: String,
    pub(crate) io: PiBuiltinIo,
}

impl PiBuiltinRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.sdk_operation.is_empty() || self.sdk_operation.len() > 512
            || self.sdk_operation.as_bytes().contains(&0) {
            return Err(invalid("builtin requires the original bounded SDK tool call identity"));
        }
        validate_path(&self.path)?;
        match &self.io {
            PiBuiltinIo::Read { maximum_bytes, variants } => {
                validate_maximum(*maximum_bytes)?;
                if variants.len() > 4 { return Err(invalid("builtin path variant bound exceeded")); }
                for path in variants { validate_path(path)?; }
            }
            PiBuiltinIo::EditRead { maximum_bytes } => validate_maximum(*maximum_bytes)?,
            PiBuiltinIo::Write { content } => validate_content(content)?,
            PiBuiltinIo::EditWrite { content, expected_sha256 } => {
                validate_content(content)?;
                expected_digest(expected_sha256)?;
            }
        }
        Ok(())
    }

    pub(crate) fn operation(&self, root: TaskId) -> OperationId {
        crate::pi_fork::derived(crate::pi_fork::original_operation(root, self.caller, &self.sdk_operation),
            match self.io {
                PiBuiltinIo::Read { .. } => "builtin-read",
                PiBuiltinIo::Write { .. } => "builtin-write",
                PiBuiltinIo::EditRead { .. } => "builtin-edit-read",
                PiBuiltinIo::EditWrite { .. } => "builtin-edit-write",
            })
    }

    pub(crate) fn require_root_source_read(&self, original_root: TaskId) -> Result<()> {
        if self.caller != original_root || !matches!(self.io, PiBuiltinIo::Read { .. }) {
            return Err(denied("model builtins cannot mutate the original physical Root source"));
        }
        Ok(())
    }

}

fn validate_maximum(maximum: u64) -> Result<()> {
    if maximum == 0 || maximum > MAXIMUM_BUILTIN_BYTES {
        return Err(invalid("builtin whole-file read exceeds its original byte allowance"));
    }
    Ok(())
}

fn validate_content(content: &str) -> Result<()> {
    if content.len() as u64 > MAXIMUM_BUILTIN_BYTES {
        return Err(invalid("builtin write exceeds its original byte allowance"));
    }
    Ok(())
}

pub(crate) fn expected_digest(value: &str) -> Result<[u8; 32]> {
    let mut digest = [0; 32];
    if value.len() != 64 || hex::decode_to_slice(value, &mut digest).is_err() {
        return Err(invalid("builtin edit requires its exact bounded preimage digest"));
    }
    Ok(digest)
}

fn validate_path(value: &str) -> Result<()> {
    // Reject foreign separators/drive syntax too: native Path parsing alone
    // must not turn a Windows escape into a Unix filename (or vice versa).
    if value.is_empty() || value.len() > 4096 || value.as_bytes().contains(&0)
        || (value.starts_with("\\\\") && !windows_local_verbatim(value)) || value.starts_with("//")
        || value.split(['/', '\\']).any(|part| part == "..")
        || (value.as_bytes().get(1) == Some(&b':') && !Path::new(value).is_absolute())
        || Path::new(value).components().any(|part| matches!(part, Component::ParentDir)) {
        return Err(denied("builtin path escapes its declared native volume"));
    }
    Ok(())
}

/// Lexical selection only. Success is NOT IO authority: the original held-root
/// SDK producer independently rejects symlinks and replaced native roots.
pub(crate) fn relative_path<'a>(path: &'a str, root: &Path) -> Result<&'a Path> {
    validate_path(path)?;
    if !root.is_absolute() {
        return Err(denied("builtin volume has no original absolute native mapping"));
    }
    let selected = Path::new(path);
    let relative = if selected.is_absolute() {
        strip_native_prefix(selected, root).ok_or_else(|| denied("builtin path is outside its original native view"))?
    } else { selected };
    if relative.as_os_str().is_empty()
        || relative.components().any(|part| !matches!(part, Component::Normal(_))) {
        return Err(denied("builtin path is not a regular leaf below its original native volume"));
    }
    Ok(relative)
}

#[cfg(not(windows))]
fn windows_local_verbatim(_: &str) -> bool { false }

#[cfg(windows)]
fn windows_local_verbatim(value: &str) -> bool {
    matches!(Path::new(value).components().next(),
        Some(Component::Prefix(prefix)) if matches!(prefix.kind(), std::path::Prefix::VerbatimDisk(_)))
}

#[cfg(not(windows))]
fn strip_native_prefix<'a>(path: &'a Path, root: &Path) -> Option<&'a Path> {
    path.strip_prefix(root).ok()
}

#[cfg(windows)]
fn strip_native_prefix<'a>(path: &'a Path, root: &Path) -> Option<&'a Path> {
    use std::path::Prefix;
    let mut selected = path.components();
    for expected in root.components() {
        let actual = selected.next()?;
        let equivalent = match (actual, expected) {
            (Component::Prefix(actual), Component::Prefix(expected)) => {
                let disk = |prefix| match prefix {
                    Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) => Some(drive.to_ascii_uppercase()),
                    _ => None,
                };
                disk(actual.kind()).is_some() && disk(actual.kind()) == disk(expected.kind())
            }
            _ => actual == expected,
        };
        if !equivalent { return None; }
    }
    Some(selected.as_path())
}

pub(crate) fn accept<T>(work: &NativeMaterializationWork,
    result: acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<T>, Error>) -> Result<T> {
    match result {
        Ok(receipt) => { work.spent(receipt.work).map_err(native_error)?; Ok(receipt.value) }
        Err(failure) => { work.spent(failure.work).map_err(native_error)?; Err(failure.error) }
    }
}

pub(crate) fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

pub(crate) fn invalid(reason: &'static str) -> Error {
    Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason }
}

fn denied(reason: &'static str) -> Error {
    Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized, reason }
}

pub(crate) fn read_response(bytes: &[u8], path: &str, volume_role: &str, work: &NativeMaterializationWork)
    -> Result<(serde_json::Value, NativeAllocation)> {
    use base64::Engine as _;
    use sha2::{Digest as _, Sha256};
    let size = bytes.len().checked_add(2).and_then(|size| size.checked_div(3))
        .and_then(|size| size.checked_mul(4)).ok_or_else(|| invalid("builtin response size overflow"))?;
    let _encoded = work.retain_owned(size as u64 + 64).map_err(native_error)?;
    work.charge(WorkCounters { bytes_hashed: bytes.len() as u64, bytes_encoded: bytes.len() as u64,
        ..WorkCounters::default() }).map_err(native_error)?;
    let content_base64 = base64::engine::general_purpose::STANDARD.encode(bytes);
    let sha256 = hex::encode(Sha256::digest(bytes));
    #[derive(Serialize)]
    struct Response<'a> { content_base64: &'a str, sha256: &'a str, path: &'a str, volume_role: &'a str }
    crate::pi_root_provider::guarded_pi_value(&Response { content_base64: &content_base64, sha256: &sha256, path, volume_role },
        MAXIMUM_BUILTIN_BYTES * 2, work)
}

type Owner = acyclic_harness::durable_host::TaskJournalOwner<acyclic_stream::LocalStream>;

/// Borrows original providers and creator handles; it owns no second journal,
/// route registry, root credential, or independent filesystem.
pub(crate) struct PiBuiltinHost<'a> {
    runtime: std::sync::Arc<crate::pi_native_host::PiNativeRuntime>,
    filesystem: std::sync::Arc<crate::pi_native_host::PiNativeFilesystem>,
    stream: acyclic_stream::StreamClient<acyclic_stream::LocalStream>,
    resources: crate::ServiceResources,
    protected: crate::pi_owner::PiOwnerConfig,
    retained: &'a crate::AdapterState,
    roots: &'a std::collections::BTreeMap<String, crate::pi_root_provider::PiRootHandle>,
    native: std::sync::Arc<crate::pi_native_host::PiNativeCommandHost>,
    work: NativeMaterializationWork,
}

impl<'a> PiBuiltinHost<'a> {
    #[allow(clippy::too_many_arguments, reason = "original providers and authentic creator handles")]
    pub(crate) fn new(
        runtime: std::sync::Arc<crate::pi_native_host::PiNativeRuntime>,
        filesystem: std::sync::Arc<crate::pi_native_host::PiNativeFilesystem>,
        stream: acyclic_stream::StreamClient<acyclic_stream::LocalStream>,
        resources: crate::ServiceResources, protected: crate::pi_owner::PiOwnerConfig,
        retained: &'a crate::AdapterState,
        roots: &'a std::collections::BTreeMap<String, crate::pi_root_provider::PiRootHandle>,
        native: std::sync::Arc<crate::pi_native_host::PiNativeCommandHost>,
        work: NativeMaterializationWork,
    ) -> Result<Self> {
        runtime.verify_owner(&protected)?;
        Ok(Self { runtime, filesystem, stream, resources, protected, retained, roots, native, work })
    }

    async fn root_read(&self, context: acyclic_harness::runtime::TaskContext,
        owner: std::sync::Arc<Owner>, request: &PiBuiltinRequest) -> Result<(serde_json::Value, NativeAllocation)> {
        request.validate()?;
        request.require_root_source_read(self.protected.policy().root_task)?;
        authorize_owner(&context, &owner, request.caller, &self.work).await?;
        let observed = self.runtime.runtime.task_host()
            .observe_admission_with_native_work(request.caller, &self.work).await?;
        let admission_custody = observed.1;
        let admission = observed.0;
        if admission.task_id() != request.caller {
            return Err(denied("builtin Root observation substituted its original task"));
        }
        let maximum = self.protected.policy().maximum_payload_bytes.min(crate::MAXIMUM_ADAPTER_STATE_BYTES);
        let encoded = crate::pi_root_provider::guarded_pi_bytes(&admission.input, maximum, &self.work)?;
        let decoded = crate::pi_root_provider::guarded_pi_decode::<crate::pi_task::PiTaskInput>(
            &encoded.0, maximum, maximum.saturating_mul(8), &self.work)?;
        drop(encoded.0);
        drop(encoded.1);
        drop(admission);
        drop(admission_custody);
        let _input_custody = decoded.1;
        let input = decoded.0;
        if input.binding.task_id != request.caller || input.binding.agent_id != self.protected.policy().agent {
            return Err(denied("builtin Root binding differs from its original admitted owner"));
        }
        let crate::pi_task::PiTaskExecution::Host { root, .. } = input.execution else {
            return Err(denied("builtin Root has no original host admission"));
        };
        let original_request = crate::pi_root_inputs::PiRootRequest { binding: input.binding, root };
        let crate::pi_root::PiRootIntent::Project { paths, root_id, .. } = &original_request.root else {
            return Err(denied("builtin Root has no original physical SOURCE"));
        };
        let context = accept(&self.work, crate::pi_root_provider::bind_root_output_context(context, &owner,
            self.protected.clone(), self.resources.clone(), self.filesystem.clone(), self.stream.clone(),
            &original_request, self.work.remaining().map_err(native_error)?, self.work.cancellation()).await)?;
        let (key, relative) = select_root_path(&request.path, paths, root_id)?;
        let root = Path::new(&paths[key]);
        let PiBuiltinIo::Read { maximum_bytes, variants } = &request.io else { unreachable!("read checked") };
        // Every alternative is proved inside the SAME root before any bytes are
        // read. A missing file cannot redirect fallback to another host root.
        let relatives = scoped_variants(variants, root)?;
        let selected = crate::pi_selected_host::PiSelectedHost::new(self.runtime.clone(), self.filesystem.clone(),
            self.stream.clone(), self.resources.clone(), self.protected.clone(), self.retained, self.roots,
            self.native.clone())?.with_work(self.work.clone());
        let (_, volume, workspace, frozen, inspection) = selected.selected(&context, &owner, request.caller,
            Some(key), None, request.operation(self.protected.policy().root_task),
            self.work.cancellation(), &self.work).await?;
        let reader = crate::pi_inspection::PiNativeSelectedSource::select(&inspection, request.caller,
            request.caller, &volume, &workspace, frozen)?;
        let mut last_absence = None;
        for (path, candidate) in std::iter::once(relative).chain(relatives.into_iter().flatten())
            .zip(std::iter::once(&request.path).chain(variants.iter())) {
            let converted = logical_path(path, &self.work)?;
            let _path_custody = converted.1;
            let path = converted.0;
            match reader.read_with_native_work(&context, &owner, &path, *maximum_bytes, &self.work).await {
                Ok((bytes, custody)) => {
                    let response = match authorize_owner(&context, &owner, request.caller, &self.work).await {
                        Ok(()) => read_response(bytes.as_ref(), candidate, "SOURCE", &self.work),
                        Err(error) => Err(error),
                    };
                    drop(bytes);
                    drop(custody);
                    return response;
                }
                Err(error) if is_absent(&error) => { last_absence = Some(error); }
                Err(error) => return Err(error),
            }
        }
        Err(last_absence.expect("the requested candidate is always checked"))
    }

    pub(crate) async fn child_io(&self, request: &PiBuiltinRequest) -> Result<(serde_json::Value, NativeAllocation)> {
        request.validate()?;
        if request.caller == self.protected.policy().root_task {
            return Err(denied("physical Root must use its original mediated source command"));
        }
        let live = self.native.live_source(request.caller, &self.work).await?
            .ok_or_else(|| denied("builtin caller has no current original native view"))?;
        authorize_owner(live.context.context(), &live.owner, request.caller, &self.work).await?;
        if live.binding.task_id != request.caller {
            return Err(denied("builtin native binding substituted its original task"));
        }
        let (mapping, relative) = select_native_path(&request.path, &live.binding, live.view.manifest())?;
        authorize_volume(&live.owner, &mapping.volume, mapping.writable, &request.io, &self.work)?;
        let variants = match &request.io { PiBuiltinIo::Read { variants, .. } => variants.as_slice(), _ => &[] };
        let relatives = scoped_variants(variants, &mapping.path)?;
        let operation = request.operation(self.protected.policy().root_task);
        let volume_role = if mapping.volume == live.binding.scratch.volume { "SCRATCH" } else { "PROJECT" };
        match &request.io {
            PiBuiltinIo::Read { maximum_bytes, .. } | PiBuiltinIo::EditRead { maximum_bytes } => {
                let mut last_absence = None;
                for (path, candidate) in std::iter::once(relative).chain(relatives.into_iter().flatten())
                    .zip(std::iter::once(&request.path).chain(variants.iter())) {
                    match live.view.read_builtin_file(&live.owner, live.context.context(), operation, &mapping.volume,
                        path, *maximum_bytes as usize, &self.work).await {
                        Ok((bytes, custody)) => {
                            let response = match authorize_owner(live.context.context(), &live.owner, request.caller, &self.work).await {
                                Ok(()) => read_response(&bytes, candidate, volume_role, &self.work),
                                Err(error) => Err(error),
                            };
                            drop(bytes);
                            drop(custody);
                            return response;
                        }
                        Err(error) if is_absent(&error) => { last_absence = Some(error); }
                        Err(error) => return Err(error),
                    }
                }
                Err(last_absence.expect("the requested candidate is always checked"))
            }
            PiBuiltinIo::Write { content } | PiBuiltinIo::EditWrite { content, .. } => {
                let expected = match &request.io {
                    PiBuiltinIo::EditWrite { expected_sha256, .. } => Some(expected_digest(expected_sha256)?),
                    _ => None,
                };
                live.view.write_builtin_file(&live.owner, live.context.context(), operation, &mapping.volume,
                    relative, content, matches!(request.io, PiBuiltinIo::Write { .. }), expected, &self.work).await?;
                authorize_owner(live.context.context(), &live.owner, request.caller, &self.work).await?;
                #[derive(Serialize)]
                struct Response<'a> { written: bool, path: &'a str, volume_role: &'a str }
                crate::pi_root_provider::guarded_pi_value(&Response { written: true, path: &request.path, volume_role },
                    8192, &self.work)
            }
        }
    }
}

fn authorize_volume(owner: &Owner, volume: &acyclic_harness::conversation::VolumeRef,
    writable: bool, io: &PiBuiltinIo, work: &NativeMaterializationWork) -> Result<()> {
    use acyclic_harness::conversation::VolumeOperation;
    owner.require_volume_grant_with_native_work(volume, VolumeOperation::Read, work)?;
    if !matches!(io, PiBuiltinIo::Read { .. }) {
        if !writable {
            return Err(denied("builtin native volume is not an original writable destination"));
        }
        owner.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)?;
    }
    Ok(())
}

fn select_native_path<'a, 'p>(path: &'p str, binding: &crate::PiTaskBinding,
    manifest: &'a acyclic_harness::filesystem::NativeViewManifest,
) -> Result<(&'a acyclic_harness::filesystem::NativeVolumeMapping, &'p Path)> {
    validate_path(path)?;
    manifest.validate()?;
    let allowed = |mapping: &&acyclic_harness::filesystem::NativeVolumeMapping| {
        mapping.volume == binding.scratch.volume || binding.project_volumes.values().any(|volume| volume == &mapping.volume)
    };
    let mut selected = None;
    for mapping in manifest.volumes.iter().filter(allowed) {
        if !Path::new(path).is_absolute() && mapping.volume == binding.scratch.volume
            && !binding.project_volumes.is_empty() { continue; }
        if let Ok(relative) = relative_path(path, &mapping.path) {
            if selected.is_some() {
                return Err(denied("builtin native path selection is ambiguous"));
            }
            selected = Some((mapping, relative));
        }
    }
    selected.ok_or_else(|| denied("builtin path is outside its actual PROJECT/SCRATCH view; RESULT is unmounted"))
}

pub(crate) async fn authorize_owner(context: &acyclic_harness::runtime::TaskContext,
    owner: &Owner, caller: TaskId, work: &NativeMaterializationWork) -> Result<()> {
    if owner.task_id() != caller || context.durable_task_id() != Some(caller) {
        return Err(denied("builtin caller differs from its original task owner/context"));
    }
    owner.verify_with_native_work(false, work).await
}

fn select_root_path<'p, 'r>(path: &'p str, roots: &'r std::collections::BTreeMap<String, String>,
    default: &[u8; 16]) -> Result<(&'r str, &'p Path)> {
    validate_path(path)?;
    if !Path::new(path).is_absolute() {
        let (key, root) = roots.iter().find(|(key, _)| {
            let mut identity = [0; 16];
            hex::decode_to_slice(key.as_bytes(), &mut identity).is_ok() && identity == *default
        }).ok_or_else(|| denied("builtin original Root cwd is absent"))?;
        return Ok((key, relative_path(path, Path::new(root))?));
    }
    let mut selected = None;
    for (key, root) in roots {
        if let Ok(relative) = relative_path(path, Path::new(root)) {
            if selected.is_some() {
                return Err(denied("builtin original source selection is ambiguous"));
            }
            selected = Some((key.as_str(), relative));
        }
    }
    selected.ok_or_else(|| denied("builtin path is outside the original physical source"))
}

fn scoped_variants<'a>(variants: &'a [String], root: &Path) -> Result<[Option<&'a Path>; 4]> {
    let mut selected = [None; 4];
    for (index, path) in variants.iter().enumerate() {
        let entry = selected.get_mut(index).ok_or_else(|| invalid("builtin variant bound exceeded"))?;
        *entry = Some(relative_path(path, root)?);
    }
    Ok(selected)
}

fn logical_path(path: &Path, work: &NativeMaterializationWork) -> Result<(String, NativeAllocation)> {
    let maximum = path.as_os_str().len();
    let custody = work.retain_owned(maximum as u64).map_err(native_error)?;
    let mut value = String::with_capacity(maximum);
    for component in path.components() {
        let part = component.as_os_str().to_str().ok_or_else(|| invalid("builtin leaf has no wire representation"))?;
        if !value.is_empty() { value.push('/'); }
        value.push_str(part);
    }
    work.charge(WorkCounters { bytes_copied: value.len() as u64, ..WorkCounters::default() }).map_err(native_error)?;
    acyclic_harness::conversation::validate_content_path(&value)?;
    Ok((value, custody))
}

fn is_absent(error: &Error) -> bool {
    match error {
        Error::NotFound(_) => true,
        Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Io(source), .. } =>
            source.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}

impl acyclic_harness::filesystem::TaskCommandHost for PiBuiltinHost<'_> {
    fn execute(&self, context: acyclic_harness::runtime::TaskContext,
        fence: acyclic_harness::scheduler::LeaseFence, command: acyclic_harness::workflow::WorkflowCommand,
        payload: serde_json::Value,
    ) -> acyclic_stream::BoxProviderFuture<'_, Result<acyclic_harness::filesystem::TaskCommandProgress>> {
        Box::pin(async move {
            let encoded = crate::pi_root_provider::guarded_pi_bytes(&payload, MAXIMUM_BUILTIN_BYTES * 2, &self.work)?;
            let decoded = crate::pi_root_provider::guarded_pi_decode::<PiBuiltinRequest>(
                &encoded.0, MAXIMUM_BUILTIN_BYTES * 2, MAXIMUM_BUILTIN_BYTES * 4, &self.work)?;
            drop(encoded.0);
            drop(encoded.1);
            let _request_custody = decoded.1;
            let request = decoded.0;
            request.validate()?;
            if command.kind != PI_BUILTIN_COMMAND_KIND
                || command.operation_id != request.operation(self.protected.policy().root_task) {
                return Err(denied("builtin command changed its original SDK operation"));
            }
            let owner = std::sync::Arc::new(self.runtime.runtime.task_host()
                .journal_owner(request.caller, fence).await?
                .with_invocation_cancellation(self.work.cancellation()));
            let (value, custody) = self.root_read(context, owner, &request).await?;
            Ok(acyclic_harness::filesystem::TaskCommandProgress::ReadyWithNativeCustody { value, custody })
        })
    }

    fn reconcile<'a>(&'a self, context: acyclic_harness::runtime::TaskContext,
        fence: acyclic_harness::scheduler::LeaseFence, command: acyclic_harness::workflow::WorkflowCommand,
        payload: serde_json::Value,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<acyclic_harness::filesystem::TaskCommandProgress>> {
        // Root builtins are READ-only; a re-entered original command has no
        // filesystem mutation to redispatch. The shared workflow owns replay.
        self.execute(context, fence, command, payload)
    }
}

#[cfg(test)]
#[path = "pi_builtin_tests.rs"]
mod tests;

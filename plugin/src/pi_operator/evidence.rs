//! Bounded file evidence from the pinned imported generation.
use super::*;
use crate::{
    AdapterState, PluginTaskBinding,
    pi_owner::PiOwnerConfig,
    pi_root_provider::PiRootHandle,
    pi_selected_host::{denied, invalid, native_workspace_error},
};
use acyclic_harness::{
    conversation::{ContentGrant, VolumeOperation},
    filesystem::PreparationIo,
    store::HistoryWork,
};
use base64::Engine as _;
use std::collections::BTreeMap;

pub(crate) async fn read_imported_selected_files(
    protected: &PiOwnerConfig,
    retained: &AdapterState,
    roots: &BTreeMap<String, PiRootHandle>,
    descriptor: &PiPreparedApproval,
    evidence: Option<&PiOperatorFileEvidence>,
    work: &NativeMaterializationWork,
) -> Result<(serde_json::Value, NativeAllocation)> {
    let PluginTaskBinding::Pi(binding) = &retained.task_binding else {
        return Err(denied("file evidence has no original Pi Root binding"));
    };
    let key = descriptor
        .command
        .root
        .as_deref()
        .ok_or_else(|| denied("file evidence lost its explicit Root"))?;
    let volume = binding
        .project_volumes
        .get(key)
        .ok_or_else(|| denied("file evidence lost its Root volume"))?;
    let root = roots
        .get(key)
        .ok_or_else(|| denied("file evidence lost its Root workspace"))?;
    let created = retained
        .roots
        .get(key)
        .ok_or_else(|| denied("file evidence lost its creator"))?
        .created()
        .map_err(Error::Unauthorized)?;
    let mut io = HistoryWork::new(PreparationIo::from_native_work(work)?);
    let storage_name = volume.storage_name(&mut Some(&mut io))?;
    if binding.task_id != protected.policy().root_task
        || descriptor.command.caller != binding.task_id
        || descriptor.session != binding.native_session_id
        || descriptor.source_snapshot.workspace_id != root.workspace.workspace().id()
        || created.repository_workspace_id != root.workspace.workspace().id().into_bytes()
        || created.native_root_identity != root.physical.source.inner().root_identity().to_bytes()
        || storage_name != root.workspace.workspace().name().as_str()
    {
        return Err(denied("file evidence changed its original selected import"));
    }
    let capability = volume.capability(VolumeOperation::Read, &mut Some(&mut io))?;
    let grants = acyclic_harness::Capabilities::new_with_native_work([capability.as_str()], work)?;
    let authorization = protected.conversation_authority_with_native_work(
        binding.task_id,
        binding.agent_id,
        &grants.0,
        work,
    )?;
    let verifier = authorization.0.1.verifier(&mut Some(&mut io))?;
    let scope = &authorization.0.2;
    let _grant = ContentGrant::verify(
        &verifier,
        scope,
        volume,
        VolumeOperation::Read,
        protected.policy().limits.file_bytes,
        &mut Some(&mut io),
    )?;
    let generation = root
        .workspace
        .workspace()
        .generation(descriptor.imported, work, work.cancellation())
        .await
        .map_err(|error| native_workspace_error((error, None)))?;
    let capacity = descriptor
        .journal
        .plan
        .edits
        .len()
        .checked_mul(2)
        .and_then(|count| count.checked_add(descriptor.command.paths.len()))
        .ok_or_else(|| invalid("file evidence path count overflow"))?;
    let (_paths_custody, mut paths) = {
        let (paths, custody) = crate::pi_native::pi_native_vec::<&str>(capacity, work)?;
        (custody, paths)
    };
    work.charge(WorkCounters {
        items_examined: (descriptor.command.paths.len() + descriptor.journal.plan.edits.len())
            as u64,
        ..WorkCounters::default()
    })?;
    paths.extend(descriptor.command.paths.iter().map(String::as_str));
    for edit in &descriptor.journal.plan.edits {
        match edit {
            acyclic_fs::MaterializationEdit::Install { path, .. }
            | acyclic_fs::MaterializationEdit::Remove { path }
            | acyclic_fs::MaterializationEdit::SetMetadata { path, .. } => paths.push(path),
            acyclic_fs::MaterializationEdit::Rename { from, to } => {
                paths.push(from);
                paths.push(to);
            }
            _ => return Err(denied("file evidence has an unsupported edit")),
        }
    }
    paths.sort_unstable();
    paths.dedup();
    let maximum = evidence.map_or(262_144, |request| request.maximum_bytes);
    if maximum == 0 || maximum > 262_144 {
        return Err(invalid("file evidence exceeds its range limit"));
    }
    if let Some(request) = evidence {
        if paths.binary_search(&request.path.as_str()).is_err() {
            return Err(denied("file evidence names an unprepared path"));
        }
        if request.cursor.as_ref().is_some_and(|cursor| {
            cursor.operation != descriptor.command.sdk_operation
                || cursor.root != key
                || cursor.generation != descriptor.imported
                || cursor.path != request.path
        }) {
            return Err(denied(
                "file evidence cursor changed its operation, Root, generation or path",
            ));
        }
    }
    let mut payload = {
        let (files, custody) = crate::pi_native::pi_native_vec(paths.len(), work)?;
        (files, None, custody)
    };
    let (files, cursor, custody) = &mut payload;
    let mut remaining = u64::from(maximum);
    for path in paths {
        if evidence.is_some_and(|request| path < request.path.as_str()) {
            continue;
        }
        acyclic_harness::conversation::validate_content_path(path, &mut Some(&mut io))?;
        if !descriptor.command.paths.iter().any(|prefix| {
            path == prefix
                || path
                    .strip_prefix(prefix)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        }) {
            return Err(denied("file evidence escaped the selected paths"));
        }
        work.cancellation()
            .check()
            .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
        let _grant = ContentGrant::verify(
            &verifier,
            scope,
            volume,
            VolumeOperation::Read,
            protected.policy().limits.file_bytes,
            &mut Some(&mut io),
        )?;
        let absolute = work.display(&format_args!("/{path}"))?;
        let stat = match generation
            .stat(&absolute.0, work, work.cancellation())
            .await
        {
            Ok(stat) => stat,
            Err(acyclic_fs::WorkspaceError::NotFound) => {
                files.push((path, None));
                continue;
            }
            Err(error) => return Err(native_workspace_error((error, None))),
        };
        if stat.kind != acyclic_fs::kernel::FileKind::Regular {
            continue;
        }
        let offset = evidence
            .filter(|request| request.path == path)
            .and_then(|request| request.cursor.as_ref())
            .map_or(0, |cursor| cursor.offset);
        let length = stat
            .logical_bytes
            .ok_or_else(|| denied("file evidence lacks its immutable length"))?;
        if offset > length {
            return Err(invalid("file evidence cursor exceeds its immutable file"));
        }
        let count = (length - offset).min(remaining);
        if count != 0 || offset == length {
            let bytes = generation
                .read_range(&absolute.0, offset, count, work, work.cancellation())
                .await
                .map_err(|error| native_workspace_error((error, None)))?
                .bytes;
            if bytes.len() as u64 != count {
                return Err(denied("file evidence range changed its immutable length"));
            }
            let length = count
                .checked_add(2)
                .and_then(|length| length.checked_div(3))
                .and_then(|length| length.checked_mul(4))
                .ok_or_else(|| invalid("file evidence size overflow"))?;
            custody.grow(length)?;
            work.charge(WorkCounters {
                bytes_encoded: length,
                allocation_operations: u64::from(length != 0),
                ..WorkCounters::default()
            })?;
            files.push((
                path,
                Some(base64::engine::general_purpose::STANDARD.encode(&bytes)),
            ));
            remaining -= count;
        }
        if offset + count < length {
            *cursor = Some(PiOperatorFileCursor {
                operation: descriptor.command.sdk_operation.as_str(),
                root: key,
                generation: descriptor.imported,
                path,
                offset: offset + count,
            });
            break;
        }
    }
    crate::pi_root_provider::guarded_pi_value(
        &FileEvidence {
            files: files.as_slice(),
            complete: cursor.is_none(),
            cursor: cursor.as_ref(),
        },
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
        work,
    )
}

#[derive(serde::Serialize)]
struct FileEvidence<'a> {
    #[serde(serialize_with = "serialize_files")]
    files: &'a [(&'a str, Option<String>)],
    cursor: Option<&'a PiOperatorFileCursor<&'a str>>,
    complete: bool,
}
fn serialize_files<S: serde::Serializer>(
    files: &&[(&str, Option<String>)],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    use serde::ser::SerializeMap as _;
    let mut entries = serializer.serialize_map(Some(files.len()))?;
    for (path, value) in *files {
        entries.serialize_entry(path, value)?;
    }
    entries.end()
}

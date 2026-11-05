use super::*;
use crate::kernel::Owned;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

pub(super) fn binding_conflict_view<A, O>(
    plan: &JoinPlan<A, O>,
    directory_id: FileId,
    name: &LogicalName,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<crate::ConflictView>, WorkspaceError> {
    let name = canonical_name_key(name, accounting)?;
    let mut view = Owned::new(
        crate::ConflictView {
            key: ConflictKey::Binding {
                directory_id,
                name: name.value,
            },
            path: None,
            kind: ConflictKind::Binding,
            base: ConflictValue::Absent,
            ours: ConflictValue::Absent,
            theirs: ConflictValue::Absent,
        },
        name.allocation,
        name.bytes,
    );
    let ConflictKey::Binding { name, .. } = &view.value.key else {
        unreachable!("binding view retains its original binding key")
    };
    for (value, generation) in [
        &mut view.value.base,
        &mut view.value.ours,
        &mut view.value.theirs,
    ]
    .into_iter()
    .zip([plan.base.id, plan.target_head.id, plan.source_head.id])
    {
        let copy = Owned::<Vec<u8>>::copy_from_slice(name, accounting)?;
        *value = ConflictValue::BindingReference {
            generation,
            directory_id,
            name: copy.transfer_to(&mut view.allocation, &mut view.bytes, accounting)?,
        };
    }
    Ok(view)
}

pub(super) fn record_conflict_view<A, O>(
    plan: &JoinPlan<A, O>,
    file_id: FileId,
    path: Option<String>,
    kind: ConflictKind,
) -> crate::ConflictView {
    let value = |generation| ConflictValue::Record {
        generation,
        file_id,
    };
    crate::ConflictView {
        key: ConflictKey::File(file_id),
        path,
        kind,
        base: value(plan.base.id),
        ours: value(plan.target_head.id),
        theirs: value(plan.source_head.id),
    }
}

fn canonical_name_key(
    name: &LogicalName,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<Vec<u8>>, crate::FsError> {
    let length = name
        .as_bytes()
        .len()
        .checked_add(1)
        .ok_or(crate::WorkError::Overflow)?;
    let mut key = Owned::<Vec<u8>>::allocate(length, accounting)?;
    let result = accounting.perform(
        crate::WorkCounters {
            bytes_copied: crate::foundation::usize_to_u64(name.as_bytes().len()),
            output_bytes: crate::foundation::usize_to_u64(length),
            ..crate::WorkCounters::UNCHARGED
        },
        || {
            key.value.push(name.encoding().tag());
            key.value.extend_from_slice(name.as_bytes());
        },
    );
    if let Err(error) = result {
        key.retire(accounting)?;
        return Err(error);
    }
    Ok(key)
}

pub(super) fn namespace_path_text(path: &NamespacePath) -> Result<String, WorkspaceError> {
    if path.is_root() {
        return Ok("/".to_owned());
    }
    let cancellation = crate::CancellationToken::new();
    namespace_path_text_with_cancellation(path, &cancellation)
}

pub(super) fn namespace_path_text_with_cancellation(
    path: &NamespacePath,
    cancellation: &CancellationToken,
) -> Result<String, WorkspaceError> {
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation);
    path.unicode_text_with_accounting(&mut accounting, cancellation)
        .map(|(text, _)| text)
        .map_err(|_| {
            if cancellation.is_cancelled() {
                WorkspaceError::Cancelled(crate::CancellationError)
            } else {
                WorkspaceError::path("non-Unicode path cannot be projected as a portable string")
            }
        })
}

pub(super) fn kernel_conflict(key: &ConflictKey) -> Result<MergeConflict, DrivenJoinError> {
    match key {
        ConflictKey::File(file_id)
        | ConflictKey::Metadata(file_id)
        | ConflictKey::HardLinks(file_id)
        | ConflictKey::Directory(file_id)
        | ConflictKey::SpecialPayload(file_id)
        | ConflictKey::ContentRange { file_id, .. }
        | ConflictKey::Rename { file_id, .. } => Ok(MergeConflict::File(*file_id)),
        ConflictKey::Binding { directory_id, name } => {
            let (&tag, bytes) = name.split_first().ok_or(DrivenJoinError::StaleCandidate)?;
            let encoding = crate::kernel::NameEncoding::from_tag(tag)
                .map_err(|_| DrivenJoinError::StaleCandidate)?;
            let name = LogicalName::new(encoding, bytes.to_vec())
                .map_err(|_| DrivenJoinError::StaleCandidate)?;
            Ok(MergeConflict::Binding {
                directory_id: *directory_id,
                name,
            })
        }
    }
}

pub(super) fn conflict_file_id(key: &ConflictKey) -> Option<FileId> {
    match key {
        ConflictKey::File(file_id)
        | ConflictKey::Metadata(file_id)
        | ConflictKey::HardLinks(file_id)
        | ConflictKey::Directory(file_id)
        | ConflictKey::SpecialPayload(file_id)
        | ConflictKey::ContentRange { file_id, .. }
        | ConflictKey::Rename { file_id, .. } => Some(*file_id),
        ConflictKey::Binding { .. } => None,
    }
}

pub(super) fn resolution_matches_conflict(key: &ConflictKey, resolution: &MergeResolution) -> bool {
    match resolution {
        MergeResolution::Select(_) | MergeResolution::Unresolved => true,
        MergeResolution::Text(_) | MergeResolution::Binary(_) => {
            matches!(key, ConflictKey::File(_) | ConflictKey::ContentRange { .. })
        }
        MergeResolution::Metadata(_) => matches!(key, ConflictKey::Metadata(_)),
        MergeResolution::Binding(_) => matches!(key, ConflictKey::Binding { .. }),
    }
}

pub(super) fn side_resolution(side: ConflictSide) -> crate::kernel::MergeConflictResolution {
    crate::kernel::MergeConflictResolution::Select(match side {
        ConflictSide::Base => crate::kernel::MergeConflictSide::Base,
        ConflictSide::Ours => crate::kernel::MergeConflictSide::Ours,
        ConflictSide::Theirs => crate::kernel::MergeConflictSide::Theirs,
    })
}

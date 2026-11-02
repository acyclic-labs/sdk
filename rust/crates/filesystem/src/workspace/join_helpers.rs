use super::*;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

pub(super) fn binding_conflict_view<A, O>(
    plan: &JoinPlan<A, O>,
    conflict: MergeConflict,
) -> crate::ConflictView {
    match conflict {
        MergeConflict::File(file_id) => crate::ConflictView {
            key: ConflictKey::File(file_id),
            path: None,
            kind: ConflictKind::Record,
            base: ConflictValue::RecordReference {
                generation: plan.base.id,
                file_id,
            },
            ours: ConflictValue::RecordReference {
                generation: plan.target_head.id,
                file_id,
            },
            theirs: ConflictValue::RecordReference {
                generation: plan.source_head.id,
                file_id,
            },
        },
        MergeConflict::Binding { directory_id, name } => {
            let name = canonical_name_key(&name);
            let value = |generation| ConflictValue::BindingReference {
                generation,
                directory_id,
                name: name.clone(),
            };
            crate::ConflictView {
                key: ConflictKey::Binding {
                    directory_id,
                    name: name.clone(),
                },
                path: None,
                kind: ConflictKind::Binding,
                base: value(plan.base.id),
                ours: value(plan.target_head.id),
                theirs: value(plan.source_head.id),
            }
        }
    }
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

fn canonical_name_key(name: &LogicalName) -> Vec<u8> {
    let mut key = Vec::with_capacity(name.as_bytes().len().saturating_add(1));
    key.push(name.encoding().tag());
    key.extend_from_slice(name.as_bytes());
    key
}

pub(super) fn logical_name_text(name: &LogicalName) -> Result<String, WorkspaceError> {
    name.unicode_text(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .ok()
    .map(|receipt| receipt.value)
    .ok_or_else(|| {
        WorkspaceError::path("non-Unicode path cannot be projected as a portable string")
    })
}

pub(super) fn namespace_path_text(path: &NamespacePath) -> Result<String, WorkspaceError> {
    if path.is_root() {
        return Ok("/".to_owned());
    }
    let cancellation = crate::CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    path.unicode_text_with_accounting(&mut accounting, &cancellation)
        .map(|(text, _)| text)
        .map_err(|_| {
            WorkspaceError::path("non-Unicode path cannot be projected as a portable string")
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

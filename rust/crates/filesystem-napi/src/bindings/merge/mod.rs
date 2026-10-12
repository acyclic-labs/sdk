use super::*;

compat_json!(
    encode_merge_plan_json,
    encode_merge_plan_payload,
    "Encodes a merge-plan payload in the versioned compatibility envelope."
);

compat_json!(
    decode_merge_plan_json,
    decode_merge_plan_payload,
    "Decodes a versioned merge-plan envelope to its canonical payload."
);

compat_json!(
    encode_merge_candidate_json,
    encode_merge_candidate_payload,
    "Encodes a merge-candidate payload in the versioned compatibility envelope."
);

compat_json!(
    decode_merge_candidate_json,
    decode_merge_candidate_payload,
    "Decodes a versioned merge-candidate envelope to its canonical payload."
);

/// One exact name binding generation change.
#[napi(object)]
pub struct NativeBindingChange {
    /// Stable 16-byte parent-directory identity.
    pub directory_id: Buffer,
    /// Exact changed logical name.
    pub name: NativePathComponent,
    /// Binding before the change, absent for creation.
    pub before: Option<NativeTreeEntry>,
    /// Binding after the change, absent for deletion.
    pub after: Option<NativeTreeEntry>,
}

/// One complete directory entry.
#[napi(object)]
pub struct NativeTreeEntry {
    /// Exact logical entry name.
    pub name: NativePathComponent,
    /// Stable 16-byte target file identity.
    pub file_id: Buffer,
    /// Canonical target file-kind name.
    pub file_kind: FileKind,
}

/// Bounded generation diff result.
#[napi(object)]
pub struct NativeGenerationDiff {
    /// Stable-file-record changes.
    pub files: Vec<NativeFileRecordChange>,
    /// Exact directory-binding changes.
    pub bindings: Vec<NativeBindingChange>,
    /// Whether the configured change bound truncated the result.
    pub truncated: bool,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One typed merge conflict.
#[napi(object)]
pub struct NativeMergeConflict {
    /// `file` or `binding`.
    pub kind: String,
    /// Conflicting stable file identity for a file conflict.
    pub file_id: Option<Buffer>,
    /// Parent-directory identity for a binding conflict.
    pub directory_id: Option<Buffer>,
    /// Exact logical name for a binding conflict.
    pub name: Option<NativePathComponent>,
}

/// Caller-selected immutable side for one exact merge conflict.
#[napi(object)]
pub struct NativeMergeSelection {
    /// `file` or `binding`.
    pub kind: String,
    /// Conflicting stable file identity for a file conflict.
    pub file_id: Option<Buffer>,
    /// Parent-directory identity for a binding conflict.
    pub directory_id: Option<Buffer>,
    /// Exact logical name for a binding conflict.
    pub name: Option<NativePathComponent>,
    /// `base`, `ours`, or `theirs`.
    pub side: String,
}

/// Terminal merge preparation result.
#[napi(object)]
pub struct NativeMergePreparation {
    /// `prepared` or `conflicted`.
    pub status: String,
    /// Prepared two-parent generation identity on success.
    pub generation_id: Option<Buffer>,
    /// Bounded exact conflicts when preparation cannot proceed.
    pub conflicts: Vec<NativeMergeConflict>,
    /// Whether the conflict bound truncated the result.
    pub truncated: bool,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

#[napi]
impl NativeCheckout {
    /// Prepares a bounded two-parent merge against the current authority head.
    #[napi]
    pub async fn prepare_merge(
        &self,
        theirs: Buffer,
        maximum_changes: NapiU32,
        maximum_conflicts: NapiU32,
    ) -> Result<NativeMergePreparation> {
        let maximum_changes = maximum_changes.into_inner();
        let maximum_conflicts = maximum_conflicts.into_inner();

        let theirs = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &theirs,
            "merge generation identity",
        )?));
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .prepare_merge(
                theirs,
                maximum_changes,
                maximum_conflicts,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        encode_merge_preparation(receipt.value, receipt.work)
    }
}

pub(in crate::bindings) fn native_merge_selection_conflict(
    selection: &NativeMergeSelection,
) -> Result<MergeConflict> {
    match selection.kind.as_str() {
        "file" => Ok(MergeConflict::File(FileId::from_bytes(fixed_16(
            selection
                .file_id
                .as_deref()
                .ok_or_else(|| Error::new(Status::InvalidArg, "file selection lacks fileId"))?,
        )?))),
        "binding" => {
            let directory_id =
                FileId::from_bytes(fixed_16(selection.directory_id.as_deref().ok_or_else(
                    || Error::new(Status::InvalidArg, "binding selection lacks directoryId"),
                )?)?);
            let name = selection
                .name
                .as_ref()
                .ok_or_else(|| Error::new(Status::InvalidArg, "binding selection lacks name"))?;
            let encoding = NameEncoding::from_public_str(&name.encoding).ok_or_else(|| {
                Error::new(
                    Status::InvalidArg,
                    "binding selection has an invalid name encoding",
                )
            })?;
            let name =
                LogicalName::new(encoding, name.bytes.to_vec(), u32::MAX).map_err(napi_error)?;
            Ok(MergeConflict::Binding { directory_id, name })
        }
        _ => Err(Error::new(
            Status::InvalidArg,
            "merge selection kind must be file or binding",
        )),
    }
}

pub(in crate::bindings) fn encode_generation_diff(
    diff: acyclic_fs::GenerationDiff,
    work: acyclic_fs::WorkCounters,
) -> Result<NativeGenerationDiff> {
    Ok(NativeGenerationDiff {
        files: diff
            .files
            .into_iter()
            .map(|change| NativeFileRecordChange {
                file_id: Buffer::from(change.file_id.into_bytes().to_vec()),
                before: change.before.map(encode_file_record),
                after: change.after.map(encode_file_record),
            })
            .collect(),
        bindings: diff
            .bindings
            .into_iter()
            .map(|change| NativeBindingChange {
                directory_id: Buffer::from(change.directory_id.into_bytes().to_vec()),
                name: encode_name_component(&change.name),
                before: change.before.as_ref().map(encode_tree_entry),
                after: change.after.as_ref().map(encode_tree_entry),
            })
            .collect(),
        truncated: diff.truncated,
        work_json: work_json(&work)?,
    })
}

pub(in crate::bindings) fn encode_merge_preparation(
    preparation: MergePreparation,
    work: acyclic_fs::WorkCounters,
) -> Result<NativeMergePreparation> {
    let work_json = work_json(&work)?;
    Ok(match preparation {
        MergePreparation::Prepared { generation_id } => NativeMergePreparation {
            status: "prepared".to_owned(),
            generation_id: Some(Buffer::from(generation_id.digest().into_bytes().to_vec())),
            conflicts: Vec::new(),
            truncated: false,
            work_json,
        },
        MergePreparation::Conflicted {
            conflicts,
            truncated,
        } => NativeMergePreparation {
            status: "conflicted".to_owned(),
            generation_id: None,
            conflicts: conflicts.into_iter().map(encode_merge_conflict).collect(),
            truncated,
            work_json,
        },
    })
}

pub(in crate::bindings) fn encode_merge_conflict(conflict: MergeConflict) -> NativeMergeConflict {
    match conflict {
        MergeConflict::File(file_id) => NativeMergeConflict {
            kind: "file".to_owned(),
            file_id: Some(Buffer::from(file_id.into_bytes().to_vec())),
            directory_id: None,
            name: None,
        },
        MergeConflict::Binding { directory_id, name } => NativeMergeConflict {
            kind: "binding".to_owned(),
            file_id: None,
            directory_id: Some(Buffer::from(directory_id.into_bytes().to_vec())),
            name: Some(encode_name_component(&name)),
        },
    }
}

use super::*;

/// One exact name binding generation change.
#[napi(object)]
pub struct NativeBindingChange {
    pub directory_id: Buffer,
    pub name: NativePathComponent,
    pub before: Option<NativeTreeEntry>,
    pub after: Option<NativeTreeEntry>,
}

/// One complete directory entry.
#[napi(object)]
pub struct NativeTreeEntry {
    pub name: NativePathComponent,
    pub file_id: Buffer,
    pub file_kind: FileKind,
}

/// Bounded generation diff result.
#[napi(object)]
pub struct NativeGenerationDiff {
    pub files: Vec<NativeFileRecordChange>,
    pub bindings: Vec<NativeBindingChange>,
    pub truncated: bool,
    pub work_json: String,
}

/// One typed merge conflict.
#[napi(object)]
pub struct NativeMergeConflict {
    pub kind: String,
    pub file_id: Option<Buffer>,
    pub directory_id: Option<Buffer>,
    pub name: Option<NativePathComponent>,
}

/// Caller-selected immutable side for one exact merge conflict.
#[napi(object)]
pub struct NativeMergeSelection {
    pub kind: String,
    pub file_id: Option<Buffer>,
    pub directory_id: Option<Buffer>,
    pub name: Option<NativePathComponent>,
    pub side: String,
}

/// Terminal merge preparation result.
#[napi(object)]
pub struct NativeMergePreparation {
    pub status: String,
    pub generation_id: Option<Buffer>,
    pub conflicts: Vec<NativeMergeConflict>,
    pub truncated: bool,
    pub work_json: String,
}

#[napi]
impl NativeCheckout {
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
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout_call!(
            self,
            checkout,
            prepare_merge(theirs, maximum_changes, maximum_conflicts,)
        )?;
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
    let (status, generation, conflicts, truncated) = match preparation {
        MergePreparation::Prepared { generation_id } => {
            ("prepared", Some(generation_id), Vec::new(), false)
        }
        MergePreparation::Conflicted {
            conflicts,
            truncated,
        } => ("conflicted", None, conflicts, truncated),
    };
    Ok(NativeMergePreparation {
        status: status.to_owned(),
        generation_id: generation.map(native_generation_buffer),
        conflicts: conflicts.into_iter().map(encode_merge_conflict).collect(),
        truncated,
        work_json,
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

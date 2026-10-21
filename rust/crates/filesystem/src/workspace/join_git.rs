//! Git resolves content independently of native metadata.
use super::*;
use crate::facade::FsWork as _;
use crate::kernel::{FileRecord, MergeConflictResolution, MergeConflictSide};

fn metadata(base: FileMetadata, mut ours: FileMetadata, theirs: FileMetadata) -> FileMetadata {
    let executable = |mode| matches!(mode, MetadataField::Value(mode) if mode & 0o100 != 0);
    if executable(ours.posix_mode) == executable(base.posix_mode)
        && executable(ours.posix_mode) != executable(theirs.posix_mode)
    {
        let mode = match ours.posix_mode {
            MetadataField::Value(mode) => Some(mode),
            MetadataField::Unavailable => match theirs.posix_mode {
                MetadataField::Value(mode) => Some(mode),
                MetadataField::Unavailable => None,
            },
        };
        if let Some(mode) = mode {
            ours.posix_mode = MetadataField::Value(
                (mode & !0o111)
                    | if executable(theirs.posix_mode) {
                        0o111
                    } else {
                        0
                    },
            );
        }
    }
    ours
}

/// A conflicted file one side replaced under its path. The kernel merges a
/// replacement as the deleting side's value of the base identity; these are
/// the three records it merges, so drivers see the same content.
pub(super) struct ReplacedFile {
    /// Base, ours and theirs records, the replacement carrying its own identity.
    pub(super) records: [FileRecord; 3],
}

/// The record that `name` in directory `directory` binds in `view`, if any.
/// The directory is found by identity, so a renamed or moved directory still
/// names the same binding in every generation, as the kernel sees it.
async fn bound_in_directory<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    view: &Generation<A, O>,
    directory: FileId,
    name: &str,
) -> Result<Option<WorkspaceStat>, WorkspaceError> {
    let paths = view
        .paths_for_file_id(
            directory,
            1,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    let Some(parent) = paths.into_iter().next() else {
        return Ok(None);
    };
    let path = if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    };
    bound_record(view, &path).await
}

/// The record `path` binds in `view`, if any.
async fn bound_record<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    view: &Generation<A, O>,
    path: &str,
) -> Result<Option<WorkspaceStat>, WorkspaceError> {
    match view
        .stat(path, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
    {
        Ok(receipt) => Ok(Some(receipt.value)),
        Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => Ok(None),
        Err(failure) => Err(failure.error),
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> JoinPlan<A, O> {
    /// Finds a conflicted file whose identity exactly one side replaced at
    /// `path` with a single-link regular file it created.
    ///
    /// This is the kernel's replacement rule seen through names: `path` is
    /// where the editing side binds `file_id`; the base binds the same name in
    /// the same directory identity (wherever that directory now lives) to
    /// `file_id`, and the deleting side binds it to a file absent from the
    /// base. Only then does the kernel bind the name back to `file_id`, so a
    /// resolution supplied for anything else (a file moved onto the path, say)
    /// would be left unbound and dropped.
    pub(super) async fn replaced_file(
        &self,
        file_id: FileId,
        path: &str,
    ) -> Result<Option<ReplacedFile>, WorkspaceError> {
        let volume = &self.target_head.workspace.volume;
        let fs = &volume.fs;
        let mut records = [None; 3];
        for (slot, generation) in
            records
                .iter_mut()
                .zip([self.base.id, self.target_head.id, self.source_head.id])
        {
            *slot = fs
                .workspace_conflict_record(volume, generation, file_id)
                .await?;
        }
        let (side, editing, replaced) = match records {
            [Some(_), None, Some(_)] => (&self.target_head, &self.source_head, 1),
            [Some(_), Some(_), None] => (&self.source_head, &self.target_head, 2),
            _ => return Ok(None),
        };
        // The common ancestor is readable through the target's volume.
        let base = Generation {
            workspace: self.target_head.workspace.clone(),
            id: self.base.id,
            publication_closure: None,
        };
        let Some((parent, name)) = path.rsplit_once('/') else {
            return Ok(None);
        };
        let parent = if parent.is_empty() { "/" } else { parent };
        if bound_record(editing, path)
            .await?
            .is_none_or(|record| record.file_id != file_id)
        {
            return Ok(None);
        }
        let Some(directory) = bound_record(editing, parent).await? else {
            return Ok(None);
        };
        if directory.kind != FileKind::Directory
            || bound_in_directory(&base, directory.file_id, name)
                .await?
                .is_none_or(|record| record.file_id != file_id)
        {
            return Ok(None);
        }
        let Some(replacement) = bound_in_directory(side, directory.file_id, name).await? else {
            return Ok(None);
        };
        if replacement.kind != FileKind::Regular
            || replacement.file_id == file_id
            || fs
                .workspace_conflict_record(volume, self.base.id, replacement.file_id)
                .await?
                .is_some()
        {
            return Ok(None);
        }
        records[replaced] = fs
            .workspace_conflict_record(volume, side.id, replacement.file_id)
            .await?;
        let [Some(base), Some(ours), Some(theirs)] = records else {
            return Ok(None);
        };
        let records = [base, ours, theirs];
        if records
            .iter()
            .any(|record| record.kind != FileKind::Regular || record.link_count != 1)
        {
            return Ok(None);
        }
        Ok(Some(ReplacedFile { records }))
    }

    /// Resolves a replaced file to the selected content under the base
    /// identity; the kernel binds the path back to it and drops the replacement.
    pub(super) async fn replaced_file_resolution(
        &self,
        file_id: FileId,
        replaced: ReplacedFile,
        resolution: &MergeResolution,
    ) -> Result<MergeConflictResolution, DrivenJoinError> {
        let volume = &self.target_head.workspace.volume;
        let records = replaced.records;
        let template = FileRecord {
            file_id,
            ..records[1]
        };
        let mut record = match resolution {
            MergeResolution::Select(side) => FileRecord {
                file_id,
                ..records[match side {
                    ConflictSide::Base => 0,
                    ConflictSide::Ours => 1,
                    ConflictSide::Theirs => 2,
                }]
            },
            MergeResolution::Text(text) => {
                volume
                    .fs
                    .stage_merge_regular_record(
                        volume,
                        template,
                        Bytes::copy_from_slice(text.as_bytes()),
                    )
                    .await?
            }
            MergeResolution::Binary(bytes) => {
                volume
                    .fs
                    .stage_merge_regular_record(volume, template, Bytes::copy_from_slice(bytes))
                    .await?
            }
            MergeResolution::Metadata(_)
            | MergeResolution::Binding(_)
            | MergeResolution::Unresolved => {
                return Err(DrivenJoinError::UnsupportedResolution(ConflictKey::File(
                    file_id,
                )));
            }
        };
        if self.git_metadata {
            record.metadata = self.git_merged_metadata(&records).await?;
        }
        Ok(MergeConflictResolution::File(Some(record)))
    }

    async fn binding_conflict(
        &self,
        view: &Generation<A, O>,
        path: &str,
    ) -> Result<MergeConflict, WorkspaceError> {
        let config = self.target_head.workspace.volume.config;
        let namespace = customer_path(path, config)?;
        let (_, name) = namespace
            .split_last()
            .ok_or(WorkspaceError::InvalidMergeResolution)?;
        let (parent, _) = path
            .rsplit_once('/')
            .ok_or(WorkspaceError::InvalidMergeResolution)?;
        let parent = view
            .stat(
                if parent.is_empty() { "/" } else { parent },
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        Ok(MergeConflict::Binding {
            directory_id: parent.file_id,
            name: name.clone(),
        })
    }

    pub(super) async fn git_delete_binding(
        &self,
        conflict: &crate::ConflictView,
        resolution: &MergeConflictResolution,
        resolutions: &mut BTreeMap<MergeConflict, MergeConflictResolution>,
    ) -> Result<(), DrivenJoinError> {
        if !self.git_metadata
            || !matches!(conflict.kind, ConflictKind::Record)
            || (matches!(conflict.ours, ConflictValue::Absent)
                == matches!(conflict.theirs, ConflictValue::Absent))
        {
            return Ok(());
        }
        let Some(path) = &conflict.path else {
            return Ok(());
        };
        let view = if matches!(conflict.ours, ConflictValue::Absent) {
            &self.source_head
        } else {
            &self.target_head
        };
        let key = self.binding_conflict(view, path).await?;
        let binding = match resolution {
            MergeConflictResolution::Select(side) => MergeConflictResolution::Select(*side),
            MergeConflictResolution::File(Some(_)) => {
                MergeConflictResolution::Select(if matches!(conflict.ours, ConflictValue::Absent) {
                    MergeConflictSide::Theirs
                } else {
                    MergeConflictSide::Ours
                })
            }
            MergeConflictResolution::File(None) => MergeConflictResolution::Binding(None),
            _ => return Ok(()),
        };
        resolutions.entry(key).or_insert(binding);
        Ok(())
    }

    pub(super) async fn git_resolution(
        &self,
        key: &ConflictKey,
        resolution: MergeConflictResolution,
    ) -> Result<MergeConflictResolution, DrivenJoinError> {
        if !self.git_metadata {
            return Ok(resolution);
        }
        let Some(file_id) = conflict_file_id(key) else {
            return Ok(resolution);
        };
        let volume = &self.target_head.workspace.volume;
        let fs = &volume.fs;
        let mut records = [None; 3];
        for (slot, generation) in
            records
                .iter_mut()
                .zip([self.base.id, self.target_head.id, self.source_head.id])
        {
            let Some(record) = fs
                .workspace_conflict_record(volume, generation, file_id)
                .await?
            else {
                return Ok(resolution);
            };
            *slot = Some(record);
        }
        let [Some(base), Some(ours), Some(theirs)] = records else {
            unreachable!()
        };
        let records = [base, ours, theirs];
        let mut record: FileRecord = match resolution {
            MergeConflictResolution::File(Some(record)) => record,
            MergeConflictResolution::Select(side) => {
                records[match side {
                    MergeConflictSide::Base => 0,
                    MergeConflictSide::Ours => 1,
                    MergeConflictSide::Theirs => 2,
                }]
            }
            other => return Ok(other),
        };
        if record.kind != FileKind::Regular {
            return Ok(MergeConflictResolution::File(Some(record)));
        }
        if records
            .iter()
            .all(|original| original.metadata == record.metadata)
        {
            return Ok(MergeConflictResolution::File(Some(record)));
        }
        record.metadata = self.git_merged_metadata(&records).await?;
        Ok(MergeConflictResolution::File(Some(record)))
    }

    /// Target metadata with the executable bit merged as Git merges it.
    async fn git_merged_metadata(
        &self,
        records: &[FileRecord; 3],
    ) -> Result<crate::ObjectId, DrivenJoinError> {
        let volume = &self.target_head.workspace.volume;
        let fs = &volume.fs;
        let cancellation = CancellationToken::new();
        let mut work = WorkBudget::UNBOUNDED.accounting(&cancellation);
        let mut values = [FileMetadata::default(); 3];
        for (slot, original) in values.iter_mut().zip(records) {
            *slot = crate::facade::read_record_metadata_to_file_root_with_accounting(
                volume,
                *original,
                &mut work,
                &cancellation,
            )
            .await
            .map_err(WorkspaceError::from)?;
        }
        let merged = metadata(values[0], values[1], values[2]);
        Ok(if merged == values[1] {
            records[1].metadata
        } else {
            fs.stage_merge_metadata_value(merged).await?
        })
    }
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(8))]
        #[test]
        fn git_delete_modify_keeps_the_selected_binding(ours_deleted in any::<bool>(), text in "[a-z]{1,24}") {
            futures::executor::block_on(async {
                let text = format!("modified:{text}");
                let fs = crate::Fs::memory();
                let stop = CancellationToken::new();
                let main = fs.create_workspace("main", WorkBudget::UNBOUNDED, &stop).await.unwrap().value;
                main.write_text("/note", "base").await.unwrap();
                let generation = main.head(WorkBudget::UNBOUNDED, &stop).await.unwrap().value;
                let child = main.fork::<_, _, WorkspaceError>("child", crate::ForkOptions::from_generation(generation, IdempotencyKey::new()),
                    crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &stop, ()).await.unwrap().value;
                if ours_deleted { main.remove("/note").await.unwrap(); child.write_text("/note", &text).await.unwrap(); }
                else { child.remove("/note").await.unwrap(); main.write_text("/note", &text).await.unwrap(); }
                child.write_text("/clean", "source").await.unwrap();
                let plan = child.join_into(&main).git_metadata().plan(WorkBudget::UNBOUNDED, &stop).await.unwrap().value;
                let options = ApplyOptions { if_target: plan.target_head(), idempotency_key: IdempotencyKey::new() };
                let JoinOutcome::Conflicted { conflicts, truncated } = plan.apply(options, BTreeMap::new(), crate::PublicationPermit::Unrestricted).await.unwrap()
                    else { return Err(TestCaseError::fail("expected delete/modify conflict")); };
                let typed = plan.describe_conflicts(conflicts, truncated).await.unwrap();
                let resolutions = typed.conflicts.iter().map(|conflict| (conflict.key.clone(),
                    MergeResolution::Select(if matches!(conflict.ours, ConflictValue::Absent) { ConflictSide::Theirs } else { ConflictSide::Ours }))).collect();
                let candidate = UnpublishedMergeCandidate { plan: typed, resolutions };
                let outcome = plan.apply_candidate(options, &candidate, crate::PublicationPermit::Unrestricted).await.unwrap();
                prop_assert!(matches!(outcome, JoinOutcome::Applied(_) | JoinOutcome::NoChanges(_)));
                let head = main.head(WorkBudget::UNBOUNDED, &stop).await.unwrap().value;
                let bytes = head.read("/note", 64, WorkBudget::UNBOUNDED, &stop).await.unwrap().value;
                prop_assert_eq!(bytes.as_ref(), text.as_bytes());
                let clean = head.read("/clean", 64, WorkBudget::UNBOUNDED, &stop).await.unwrap().value;
                prop_assert_eq!(clean.as_ref(), b"source");
                prop_assert_eq!(head.stat("/note", WorkBudget::UNBOUNDED, &stop).await.unwrap().value.link_count, 1);
                Ok(())
            })?;
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        // Timestamps are always ours; the executable bit follows the only side
        // changing it, independently of all other mode and native metadata bits.
        #[test]
        fn git_metadata_ignores_native_times_and_merges_executable_bits(
            modes in any::<[u32; 3]>(), times in any::<[i64; 3]>(),
        ) {
            let value = |i: usize| FileMetadata { posix_mode: MetadataField::Value(modes[i]),
                modified_ns: MetadataField::Value(times[i]), changed_ns: MetadataField::Value(times[i]),
                ..FileMetadata::default() };
            let merged = metadata(value(0), value(1), value(2));
            prop_assert_eq!(merged.modified_ns, value(1).modified_ns);
            prop_assert_eq!(merged.changed_ns, value(1).changed_ns);
            let expected = if (modes[0] ^ modes[1]) & 0o100 != 0 { modes[1] } else { modes[2] } & 0o100;
            let MetadataField::Value(mode) = merged.posix_mode else { unreachable!() };
            prop_assert_eq!(mode & 0o100, expected);
            prop_assert_eq!(mode & !0o111, modes[1] & !0o111);
        }
    }
}

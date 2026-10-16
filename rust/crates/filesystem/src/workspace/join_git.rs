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

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> JoinPlan<A, O> {
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
        let cancellation = CancellationToken::new();
        let mut work = WorkBudget::UNBOUNDED.accounting(&cancellation);
        if records
            .iter()
            .all(|original| original.metadata == record.metadata)
        {
            return Ok(MergeConflictResolution::File(Some(record)));
        }
        let mut values = [FileMetadata::default(); 3];
        for (slot, original) in values.iter_mut().zip(&records) {
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
        record.metadata = if merged == values[1] {
            records[1].metadata
        } else {
            fs.stage_merge_metadata_value(merged).await?
        };
        Ok(MergeConflictResolution::File(Some(record)))
    }
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

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

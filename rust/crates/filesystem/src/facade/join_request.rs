use super::*;

pub(super) const WORKSPACE_JOIN_OPERATION_DOMAIN: &[u8] =
    b"acyclic-fs-workspace-join-operation-v1\0";

pub(crate) enum WorkspaceJoinOutcome {
    Applied(GenerationId),
    AlreadyApplied(GenerationId),
    Joined(GenerationId, Digest),
    AlreadyJoined(GenerationId, Digest),
    NoChanges(GenerationId),
    Stale(GenerationId),
    Conflicted(Vec<MergeConflict>, bool),
    Fenced,
    IdempotencyConflict,
}

pub(crate) struct WorkspaceJoinRequest<'a, A, O> {
    pub target: &'a Volume<A, O>,
    pub base: WorkspaceCommonAncestor,
    pub source: &'a crate::Generation<A, O>,
    pub expected_target: GenerationId,
    pub expected_head: Head,
    pub history: crate::workspace::JoinHistory,
    pub operation_id: OperationId,
    pub permit: PublicationPermit,
    pub maximum_generations: u32,
    pub maximum_changes: u32,
    pub maximum_conflicts: u32,
    pub resolutions: WorkspaceJoinResolutions<'a>,
}

impl<A, O> WorkspaceJoinRequest<'_, A, O> {
    pub(super) fn operation_context(&self, resolutions: Digest) -> Digest {
        workspace_join_context(
            self.source.workspace.volume.id,
            self.source.id,
            self.target.id,
            self.expected_target,
            self.base,
            self.history,
            self.maximum_generations,
            self.maximum_changes,
            self.maximum_conflicts,
            resolutions,
        )
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "join digest binds every checked Filesystem generation and limit"
)]
pub(super) fn workspace_join_context(
    source_workspace: VolumeId,
    source_generation: GenerationId,
    target_workspace: VolumeId,
    expected_target: GenerationId,
    base: WorkspaceCommonAncestor,
    history: crate::workspace::JoinHistory,
    maximum_generations: u32,
    maximum_changes: u32,
    maximum_conflicts: u32,
    resolutions: Digest,
) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(WORKSPACE_JOIN_OPERATION_DOMAIN);
    hasher.update(&source_workspace.into_bytes());
    hasher.update(source_generation.digest().as_bytes());
    hasher.update(&target_workspace.into_bytes());
    hasher.update(expected_target.digest().as_bytes());
    hasher.update(&base.volume_id.into_bytes());
    hasher.update(base.id.digest().as_bytes());
    hasher.update(&[match history {
        crate::workspace::JoinHistory::Merge => 1,
        crate::workspace::JoinHistory::Rebase => 2,
        crate::workspace::JoinHistory::Squash => 3,
        crate::workspace::JoinHistory::CherryPick => 4,
    }]);
    hasher.update(&maximum_generations.to_le_bytes());
    hasher.update(&maximum_changes.to_le_bytes());
    hasher.update(&maximum_conflicts.to_le_bytes());
    hasher.update(resolutions.as_bytes());
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

pub(crate) enum WorkspaceJoinResolutions<'a> {
    Map(BTreeMap<MergeConflict, MergeConflictResolution>),
    Slice(&'a [(MergeConflict, MergeConflictResolution)]),
}

impl WorkspaceJoinResolutions<'_> {
    pub(super) fn visit(
        &self,
        mut visit: impl FnMut(&MergeConflict, &MergeConflictResolution) -> Result<(), FsError>,
    ) -> Result<(), FsError> {
        match self {
            Self::Map(map) => {
                for (key, value) in map {
                    visit(key, value)?;
                }
            }
            Self::Slice(slice) => {
                for (key, value) in *slice {
                    visit(key, value)?;
                }
            }
        }
        Ok(())
    }
}

pub(super) fn hash_join_resolutions(
    resolutions: &WorkspaceJoinResolutions<'_>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Digest, FsError> {
    let mut hasher = blake3::Hasher::new();
    let domain = b"acyclic-fs-workspace-join-resolutions-v1\0";
    let prefix = WorkCounters {
        bytes_hashed: domain.len() as u64,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(prefix)?;
    hasher.update(domain);
    accounting.record_completed(prefix)?;
    resolutions.visit(|conflict, resolution| {
        let key_bytes = match conflict {
            MergeConflict::File(_) => 17,
            MergeConflict::Binding { name, .. } => 26_u64
                .checked_add(name.as_bytes().len() as u64)
                .ok_or(WorkError::Overflow)?,
        };
        let resolution_bytes = match resolution {
            MergeConflictResolution::Select(_) => 2,
            MergeConflictResolution::File(None) | MergeConflictResolution::Binding(None) => 2,
            MergeConflictResolution::Binding(Some(_)) => 18,
            MergeConflictResolution::File(Some(record)) => {
                let payload_bytes = match record.payload {
                    FilePayload::InlineRegular(data) => 9 + data.as_bytes().len() as u64,
                    FilePayload::Regular { .. }
                    | FilePayload::SymbolicLink { .. }
                    | FilePayload::ReparsePoint { .. } => 42,
                    FilePayload::Directory { .. } => 34,
                    FilePayload::Empty => 1,
                    FilePayload::Device { .. } => 9,
                };
                60 + payload_bytes
            }
        };
        let hashing = WorkCounters {
            bytes_hashed: key_bytes
                .checked_add(resolution_bytes)
                .ok_or(WorkError::Overflow)?,
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(hashing)?;
        match conflict {
            MergeConflict::File(file_id) => {
                hasher.update(&[0]);
                hasher.update(&file_id.into_bytes());
            }
            MergeConflict::Binding { directory_id, name } => {
                hasher.update(&[1]);
                hasher.update(&directory_id.into_bytes());
                hasher.update(&[match name.encoding() {
                    NameEncoding::Utf8 => 0,
                    NameEncoding::PosixBytes => 1,
                    NameEncoding::WindowsUtf16Le => 2,
                }]);
                hasher.update(&(name.as_bytes().len() as u64).to_le_bytes());
                hasher.update(name.as_bytes());
            }
        }
        hash_merge_resolution(&mut hasher, resolution);
        accounting.record_completed(hashing)
    })?;
    Ok(Digest::from_bytes(*hasher.finalize().as_bytes()))
}

pub(super) fn workspace_join_conflict_bytes(
    conflicts: &Vec<MergeConflict>,
) -> Result<u64, FsError> {
    let slots = conflicts
        .capacity()
        .checked_mul(size_of::<MergeConflict>())
        .map(usize_to_u64)
        .ok_or(WorkError::Overflow)?;
    conflicts.iter().try_fold(slots, |bytes, conflict| {
        let name = match conflict {
            MergeConflict::File(_) => 0,
            MergeConflict::Binding { name, .. } => name.retained_bytes() as u64,
        };
        bytes
            .checked_add(name)
            .ok_or_else(|| WorkError::Overflow.into())
    })
}

pub(super) fn workspace_join_scalar(
    outcome: WorkspaceJoinOutcome,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<WorkspaceJoinOutcome>, WorkspaceError> {
    Ok(Owned::new(outcome, accounting.claim_owned(0, 0)?, 0))
}

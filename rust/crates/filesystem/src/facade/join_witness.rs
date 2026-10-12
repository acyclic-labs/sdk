use super::*;

/// Restart-safe proof inputs for one committed workspace join. Verification
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinCommitWitness {
    /// Exact source workspace recorded by the join.
    pub source_workspace: VolumeId,
    /// Exact source generation recorded by the join.
    pub source_generation: GenerationId,
    /// Exact target workspace recorded by the join.
    pub target_workspace: VolumeId,
    /// CAS target observed when the join was planned.
    pub expected_target: GenerationId,
    pub(crate) base_workspace: VolumeId,
    pub(crate) base_generation: GenerationId,
    /// Exact history form used to publish the result.
    pub history: crate::workspace::JoinHistory,
    pub(crate) maximum_generations: u32,
    pub(crate) maximum_changes: u32,
    pub(crate) maximum_conflicts: u32,
    pub(crate) resolutions_digest: Digest,
    pub(crate) expected_head: Head,
    /// Stable Filesystem retry identity bound to the committed join.
    pub operation_id: OperationId,
    /// Immutable generation published by the join.
    pub result_generation: GenerationId,
}

impl JoinCommitWitness {
    pub(super) fn operation_context(self) -> Digest {
        workspace_join_context(
            self.source_workspace,
            self.source_generation,
            self.target_workspace,
            self.expected_target,
            WorkspaceCommonAncestor {
                volume_id: self.base_workspace,
                id: self.base_generation,
            },
            self.history,
            self.maximum_generations,
            self.maximum_changes,
            self.maximum_conflicts,
            self.resolutions_digest,
        )
    }
}

pub(super) fn hash_merge_resolution(
    hasher: &mut blake3::Hasher,
    resolution: &MergeConflictResolution,
) {
    let hash_side = |hasher: &mut blake3::Hasher, side: MergeConflictSide| {
        hasher.update(&[match side {
            MergeConflictSide::Base => 0,
            MergeConflictSide::Ours => 1,
            MergeConflictSide::Theirs => 2,
        }]);
    };
    match resolution {
        MergeConflictResolution::Select(side) => {
            hasher.update(&[0]);
            hash_side(hasher, *side);
        }
        MergeConflictResolution::File(record) => {
            hasher.update(&[1]);
            if let Some(record) = record {
                hasher.update(&[1]);
                hasher.update(&record.file_id.into_bytes());
                hasher.update(&[match record.kind {
                    FileKind::Regular => 0,
                    FileKind::Directory => 1,
                    FileKind::SymbolicLink => 2,
                    FileKind::Fifo => 3,
                    FileKind::Socket => 4,
                    FileKind::CharacterDevice => 5,
                    FileKind::BlockDevice => 6,
                    FileKind::ReparsePoint => 7,
                    FileKind::MountBoundary => 8,
                }]);
                hasher.update(&record.link_count.to_le_bytes());
                hash_object_id(hasher, record.metadata);
                hash_file_payload(hasher, record.payload);
            } else {
                hasher.update(&[0]);
            }
        }
        MergeConflictResolution::Binding(file_id) => {
            hasher.update(&[2]);
            if let Some(file_id) = file_id {
                hasher.update(&[1]);
                hasher.update(&file_id.into_bytes());
            } else {
                hasher.update(&[0]);
            }
        }
    }
}

pub(super) fn hash_object_id(hasher: &mut blake3::Hasher, object: ObjectId) {
    hasher.update(&[object.kind.canonical_tag()]);
    hasher.update(object.digest.as_bytes());
}

pub(super) fn hash_file_payload(hasher: &mut blake3::Hasher, payload: FilePayload) {
    match payload {
        FilePayload::InlineRegular(data) => {
            hasher.update(&[0]);
            hasher.update(&(data.as_bytes().len() as u64).to_le_bytes());
            hasher.update(data.as_bytes());
        }
        FilePayload::Regular {
            logical_bytes,
            extents,
        } => {
            hasher.update(&[1]);
            hasher.update(&logical_bytes.to_le_bytes());
            hash_object_id(hasher, extents);
        }
        FilePayload::Directory { entries } => {
            hasher.update(&[2]);
            hash_object_id(hasher, entries);
        }
        FilePayload::SymbolicLink {
            target_bytes,
            target,
        } => {
            hasher.update(&[3]);
            hasher.update(&target_bytes.to_le_bytes());
            hash_object_id(hasher, target);
        }
        FilePayload::Empty => {
            hasher.update(&[4]);
        }
        FilePayload::Device { major, minor } => {
            hasher.update(&[5]);
            hasher.update(&major.to_le_bytes());
            hasher.update(&minor.to_le_bytes());
        }
        FilePayload::ReparsePoint {
            payload_bytes,
            payload,
        } => {
            hasher.update(&[6]);
            hasher.update(&payload_bytes.to_le_bytes());
            hash_object_id(hasher, payload);
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn workspace_head_state(
        &self,
        workspace: &Volume<A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(GenerationId, Head), WorkspaceError> {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let (root, head, _) = workspace
                .resolve_head_generation(original, cancellation)
                .await?;
            return Ok((GenerationId::new(root.digest), head));
        }
        let result = workspace
            .resolve_head_generation(accounting.remaining()?, cancellation)
            .await
            .map(|receipt| receipt.value);
        let backend = match &result {
            Ok((_, _, work)) => *work,
            Err(failure) => *failure.work,
        };
        accounting.record_completed(backend)?;
        let (root, head, _) = result.map_err(|failure| failure.error)?;
        Ok((GenerationId::new(root.digest), head))
    }

    pub(crate) async fn workspace_operation_generation(
        &self,
        workspace: &Volume<A, O>,
        operation_id: OperationId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Option<(GenerationId, DurableCommit)>, KernelOwnedAllocation), WorkspaceError>
    {
        accounting.check()?;
        let mut work = authority_work(accounting, cancellation)?;
        let result = self
            .inner
            .authority
            .find_operation(volume_authority_id(workspace.id), operation_id, &mut work)
            .await;
        record_authority_work(accounting, &work)?;
        let record = result.map_err(FsError::from)?;
        let allocation = KernelOwnedAllocation::Standalone;
        let generation = record
            .as_ref()
            .map(|record| {
                accounting.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                if record.operation_id != operation_id {
                    return Err(FsError::InvalidAuthorityHistory);
                }
                generation_from_record(record, workspace.id, accounting)
                    .map(|root| GenerationId::new(root.digest))
            })
            .transpose()?;
        Ok((generation.zip(record), allocation))
    }

    /// Verifies the exact publication identity of a committed workspace join.
    pub(crate) async fn verify_workspace_join_commit<W: FsWork>(
        &self,
        target: &Volume<A, O>,
        witness: &JoinCommitWitness,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<bool, WorkspaceError> {
        let mut accounting = budget.accounting(cancellation);
        let standalone = readonly_standalone_live(&accounting).is_some();
        let result = async {
            if !standalone {
                accounting.check()?;
            }
            if target.id != witness.target_workspace
                || witness.maximum_generations == 0
                || witness.maximum_changes == 0
                || witness.maximum_conflicts == 0
                || witness.source_workspace == witness.target_workspace
                || witness.expected_target == witness.result_generation
            {
                return Ok(false);
            }
            let mut native = None;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                let observed = ({
                    let mut authority = crate::AuthorityWork::native((original), (cancellation));
                    self.inner
                        .authority
                        .find_operation(
                            (volume_authority_id(target.id)),
                            (witness.operation_id),
                            &mut authority,
                        )
                        .await
                        .map(|value| (value, KernelOwnedAllocation::Standalone))
                })
                .map_err(FsError::from)?;
                native = Some((
                    observed.0,
                    observed.1,
                ));
            }
            let observed = match native {
                Some(observed) => observed,
                None => {
                    let observed = self
                        .observe_volume_operation(
                            target.id,
                            witness.operation_id,
                            accounting.remaining()?,
                            cancellation,
                        )
                        .await;
                    let completed = match &observed {
                        Ok(receipt) => receipt.work,
                        Err(failure) => *failure.work,
                    };
                    *accounting
                        .standalone_parts()
                        .expect("standalone join verification")
                        .1 = completed;
                    (
                        observed.map_err(WorkspaceError::engine)?.value,
                        KernelOwnedAllocation::Standalone,
                    )
                }
            };
            let Some(commit) = observed.0.as_ref() else {
                return Ok(false);
            };
            let generation = generation_from_record(commit, target.id, &mut accounting)?;
            if generation.digest != witness.result_generation.digest() {
                return Ok(false);
            }
            let hashing = WorkCounters {
                bytes_hashed: WORKSPACE_JOIN_OPERATION_DOMAIN.len() as u64
                    + 3 * 16
                    + 4 * 32
                    + 1
                    + 3 * 4,
                ..WorkCounters::UNCHARGED
            };
            if !standalone {
                accounting.admit(hashing)?;
            }
            let context = witness.operation_context();
            if !standalone {
                accounting.record_completed(hashing)?;
            }
            let request = PublishGenerationRequest {
                authority_id: volume_authority_id(target.id),
                volume_id: target.id,
                epoch: witness.expected_head.epoch,
                expected: witness.expected_head,
                operation_id: witness.operation_id,
                generation_root: ObjectId {
                    kind: ObjectKind::GenerationRoot,
                    digest: witness.result_generation.digest(),
                },
            };
            let expected = if standalone {
                contextual_publication_fingerprint(request, context)
            } else {
                contextual_publication_fingerprint_with_accounting(
                    request,
                    context,
                    &mut accounting,
                )?
            };
            Ok(commit.fingerprint == expected)
        }
        .await;
        budget.finish(result, &accounting)
    }
}

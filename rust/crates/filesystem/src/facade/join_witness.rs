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
    hasher.update(object.to_bytes().as_slice());
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
        let (root, head) = workspace
            .resolve_head_generation(accounting, cancellation)
            .await?;
        Ok((GenerationId::from_root(root), head))
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
                    .map(|root| GenerationId::from_root(root))
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
            let mut authority = authority_work(&accounting, cancellation)?;
            let observed = self
                .inner
                .authority
                .find_operation(
                    volume_authority_id(target.id),
                    witness.operation_id,
                    &mut authority,
                )
                .await;
            record_authority_work(&mut accounting, &authority)?;
            let observed = observed.map_err(FsError::from)?;
            let Some(commit) = observed.as_ref() else {
                return Ok(false);
            };
            let generation = generation_from_record(commit, target.id, &mut accounting)?;
            if generation != witness.result_generation.object_id() {
                return Ok(false);
            }
            let hashing = WorkCounters {
                bytes_hashed: super::join_request::workspace_join_context_bytes(
                    witness.source_generation,
                    witness.expected_target,
                    witness.base_generation,
                ),
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
                generation_root: witness.result_generation.object_id(),
            };
            let mut unbounded;
            let fingerprint_accounting = if standalone {
                unbounded = crate::kernel::allocation::KernelAccounting::standalone(
                    WorkBudget::UNBOUNDED,
                    cancellation,
                );
                &mut unbounded
            } else {
                &mut accounting
            };
            let expected =
                contextual_publication_fingerprint(request, context, fingerprint_accounting)?;
            Ok(commit.fingerprint == expected)
        }
        .await;
        budget.finish(result, &accounting)
    }
}

#[cfg(test)]
mod tests {
    use super::super::join_request::{WorkspaceJoinResolutions, hash_join_resolutions};
    use super::*;

    #[test]
    fn join_resolutions_bind_every_sealed_reference_and_admit_exact_hash_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let plain = ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([1; 32]),
            storage: None,
        };
        let storage = crate::ObjectStorage {
            locator: Digest::from_bytes([2; 32]),
            root_file_id: FileId::from_bytes([3; 16]),
            origin_generation: Digest::from_bytes([4; 32]),
        };
        let references = [
            plain,
            ObjectId {
                storage: Some(storage),
                ..plain
            },
            ObjectId {
                storage: Some(crate::ObjectStorage {
                    locator: Digest::from_bytes([5; 32]),
                    ..storage
                }),
                ..plain
            },
            ObjectId {
                storage: Some(crate::ObjectStorage {
                    root_file_id: FileId::from_bytes([6; 16]),
                    ..storage
                }),
                ..plain
            },
            ObjectId {
                storage: Some(crate::ObjectStorage {
                    origin_generation: Digest::from_bytes([7; 32]),
                    ..storage
                }),
                ..plain
            },
        ];
        let cancellation = CancellationToken::new();
        for position in 0..5 {
            let record = |reference: ObjectId| {
                let (kind, payload) = match position {
                    0 => (FileKind::Fifo, FilePayload::Empty),
                    1 => (
                        FileKind::Regular,
                        FilePayload::Regular {
                            logical_bytes: 1,
                            extents: ObjectId {
                                kind: ObjectKind::ExtentPage,
                                ..reference
                            },
                        },
                    ),
                    2 => (
                        FileKind::Directory,
                        FilePayload::Directory {
                            entries: ObjectId {
                                kind: ObjectKind::TreePage,
                                ..reference
                            },
                        },
                    ),
                    3 => (
                        FileKind::SymbolicLink,
                        FilePayload::SymbolicLink {
                            target_bytes: 1,
                            target: ObjectId {
                                kind: ObjectKind::Blob,
                                ..reference
                            },
                        },
                    ),
                    _ => (
                        FileKind::ReparsePoint,
                        FilePayload::ReparsePoint {
                            payload_bytes: 1,
                            payload: ObjectId {
                                kind: ObjectKind::Blob,
                                ..reference
                            },
                        },
                    ),
                };
                FileRecord {
                    file_id: FileId::from_bytes([8; 16]),
                    kind,
                    link_count: 1,
                    metadata: if position == 0 { reference } else { plain },
                    payload,
                }
            };
            let mut digests = Vec::new();
            for reference in references {
                let resolutions = BTreeMap::from([(
                    MergeConflict::File(FileId::from_bytes([8; 16])),
                    MergeConflictResolution::File(Some(record(reference))),
                )]);
                let resolutions = WorkspaceJoinResolutions::Map(&resolutions);
                let mut accounting =
                    KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
                let digest = hash_join_resolutions(&resolutions, &mut accounting)?;
                assert!(
                    !digests.contains(&digest),
                    "reference alias at position {position}"
                );
                digests.push(digest);

                let KernelAccounting::Standalone { work, .. } = accounting else {
                    unreachable!()
                };
                let reference_bytes = if reference.storage.is_some() { 113 } else { 33 };
                let (metadata_bytes, payload_bytes) = match position {
                    0 => (reference_bytes, 1),
                    2 => (33, 1 + reference_bytes),
                    _ => (33, 9 + reference_bytes),
                };
                assert_eq!(
                    work.bytes_hashed,
                    b"acyclic-fs-workspace-join-resolutions-v1\0".len() as u64
                        + 17
                        + 27
                        + metadata_bytes
                        + payload_bytes
                );
                let exact = WorkBudget {
                    bytes_hashed: work.bytes_hashed,
                    ..WorkBudget::UNBOUNDED
                };
                assert_eq!(
                    hash_join_resolutions(
                        &resolutions,
                        &mut KernelAccounting::standalone(exact, &cancellation)
                    )?,
                    digest
                );
                let insufficient = WorkBudget {
                    bytes_hashed: exact.bytes_hashed - 1,
                    ..exact
                };
                assert!(
                    hash_join_resolutions(
                        &resolutions,
                        &mut KernelAccounting::standalone(insufficient, &cancellation)
                    )
                    .is_err()
                );

                // Rebuilding a plain identity reproduces the former intent collision.
                let mut rebuilt = blake3::Hasher::new();
                hash_merge_resolution(
                    &mut rebuilt,
                    &MergeConflictResolution::File(Some(record(ObjectId {
                        storage: None,
                        ..reference
                    }))),
                );
                let mut original = blake3::Hasher::new();
                hash_merge_resolution(
                    &mut original,
                    &MergeConflictResolution::File(Some(record(plain))),
                );
                assert_eq!(rebuilt.finalize(), original.finalize());
            }
        }
        cancellation.cancel();
        let empty = BTreeMap::new();
        assert!(
            hash_join_resolutions(
                &WorkspaceJoinResolutions::Map(&empty),
                &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation)
            )
            .is_err()
        );
        Ok(())
    }
}

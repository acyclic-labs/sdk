use super::*;

#[derive(Clone, Copy, Eq, PartialEq)]
enum ConflictContents {
    Driver,
    Files,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> JoinPlan<A, O> {
    pub(super) async fn stage_regular_resolution(
        &self,
        key: &ConflictKey,
        bytes: Bytes,
    ) -> Result<crate::kernel::FileRecord, DrivenJoinError> {
        let file_id = conflict_file_id(key)
            .ok_or_else(|| DrivenJoinError::UnsupportedResolution(key.clone()))?;
        let record = self
            .target_head
            .workspace
            .volume
            .fs
            .workspace_conflict_record(
                &self.target_head.workspace.volume,
                self.target_head.id,
                file_id,
            )
            .await?
            .ok_or(WorkspaceError::InvalidMergeResolution)?;
        self.target_head
            .workspace
            .volume
            .fs
            .stage_merge_regular_record(&self.target_head.workspace.volume, record, bytes)
            .await
            .map_err(Into::into)
    }

    /// Expands exact kernel conflicts into immutable, driver-ready values.
    pub async fn describe_conflicts(
        &self,
        conflicts: Vec<MergeConflict>,
        truncated: bool,
    ) -> Result<MergePlan, WorkspaceError> {
        let mut typed = Vec::with_capacity(conflicts.len());
        for conflict in conflicts {
            typed.push(self.describe_conflict(conflict).await?);
        }
        Ok(MergePlan {
            base: self.base.id,
            ours: self.target_head.id,
            theirs: self.source_head.id,
            conflicts: typed,
            truncated,
        })
    }

    /// Describes one exact conflict from this plan's retained generations.
    /// Directory projections can load a selected entry without expanding every
    /// conflict or reading the contents of unrelated conflicting files.
    pub async fn describe_conflict(
        &self,
        conflict: MergeConflict,
    ) -> Result<crate::ConflictView, WorkspaceError> {
        self.describe_conflict_contents(conflict, ConflictContents::Driver)
            .await
    }

    pub(super) async fn describe_conflict_file(
        &self,
        conflict: MergeConflict,
    ) -> Result<crate::ConflictView, WorkspaceError> {
        self.describe_conflict_contents(conflict, ConflictContents::Files)
            .await
    }

    async fn describe_conflict_contents(
        &self,
        conflict: MergeConflict,
        contents: ConflictContents,
    ) -> Result<crate::ConflictView, WorkspaceError> {
        let MergeConflict::File(file_id) = conflict else {
            let mut view = binding_conflict_view(self, conflict.clone());
            if let MergeConflict::Binding { directory_id, name } = conflict
                && let Some(directory) = self.conflict_path(directory_id).await?
                && let Ok(name) = logical_name_text(&name)
            {
                view.path = Some(if directory == "/" {
                    format!("/{name}")
                } else {
                    format!("{directory}/{name}")
                });
            }
            return Ok(view);
        };
        self.file_conflict_view(file_id, contents).await
    }

    /// The driver-ready view of a conflict on one file identity's record.
    async fn file_conflict_view(
        &self,
        file_id: FileId,
        contents: ConflictContents,
    ) -> Result<crate::ConflictView, WorkspaceError> {
        let path = self.conflict_path(file_id).await?;
        let fs = &self.target_head.workspace.volume.fs;
        let base = fs
            .workspace_conflict_record(&self.target_head.workspace.volume, self.base.id, file_id)
            .await?;
        let ours = fs
            .workspace_conflict_record(
                &self.target_head.workspace.volume,
                self.target_head.id,
                file_id,
            )
            .await?;
        let theirs = fs
            .workspace_conflict_record(
                &self.target_head.workspace.volume,
                self.source_head.id,
                file_id,
            )
            .await?;
        let fallback = |generation, record: Option<crate::kernel::FileRecord>| match record {
            Some(_) => ConflictValue::Record {
                generation,
                file_id,
            },
            None => ConflictValue::Absent,
        };
        let Some((base_record, ours_record, theirs_record)) = base
            .zip(ours)
            .zip(theirs)
            .map(|((base, ours), theirs)| (base, ours, theirs))
        else {
            if let Some(text) = path.as_deref()
                && let Some(replaced) = self.replaced_file(file_id, text).await?
                && let Some(view) = self
                    .regular_conflict_view(file_id, path.clone(), replaced.records, contents)
                    .await?
            {
                return Ok(view);
            }
            return Ok(crate::ConflictView {
                key: ConflictKey::File(file_id),
                path,
                kind: ConflictKind::Record,
                base: fallback(self.base.id, base),
                ours: fallback(self.target_head.id, ours),
                theirs: fallback(self.source_head.id, theirs),
            });
        };
        if base_record.link_count != ours_record.link_count
            || base_record.link_count != theirs_record.link_count
        {
            return Ok(record_conflict_view(
                self,
                file_id,
                path,
                ConflictKind::HardLink,
            ));
        }
        if self.git_metadata
            && let Some(view) = self
                .regular_conflict_view(
                    file_id,
                    path.clone(),
                    [base_record, ours_record, theirs_record],
                    contents,
                )
                .await?
        {
            return Ok(view);
        }
        if base_record.metadata != ours_record.metadata
            || base_record.metadata != theirs_record.metadata
        {
            if contents == ConflictContents::Files {
                let mut view = record_conflict_view(self, file_id, path, ConflictKind::Metadata);
                view.key = ConflictKey::Metadata(file_id);
                return Ok(view);
            }
            return self
                .metadata_conflict_view(file_id, path, [base_record, ours_record, theirs_record])
                .await;
        }
        if let Some(view) = self
            .regular_conflict_view(
                file_id,
                path.clone(),
                [base_record, ours_record, theirs_record],
                contents,
            )
            .await?
        {
            return Ok(view);
        }
        let kind = match base_record.kind {
            FileKind::Directory => ConflictKind::Directory,
            FileKind::SymbolicLink => ConflictKind::SymbolicLink,
            _ => ConflictKind::Special,
        };
        Ok(record_conflict_view(self, file_id, path, kind))
    }

    /// The view of a conflict on one file identity's metadata.
    async fn metadata_conflict_view(
        &self,
        file_id: FileId,
        path: Option<String>,
        records: [crate::kernel::FileRecord; 3],
    ) -> Result<crate::ConflictView, WorkspaceError> {
        let volume = &self.target_head.workspace.volume;
        let [base, ours, theirs] = records;
        Ok(crate::ConflictView {
            key: ConflictKey::Metadata(file_id),
            path,
            kind: ConflictKind::Metadata,
            base: ConflictValue::Metadata(
                volume.fs.workspace_conflict_metadata(volume, base).await?,
            ),
            ours: ConflictValue::Metadata(
                volume.fs.workspace_conflict_metadata(volume, ours).await?,
            ),
            theirs: ConflictValue::Metadata(
                volume
                    .fs
                    .workspace_conflict_metadata(volume, theirs)
                    .await?,
            ),
        })
    }

    pub(super) async fn conflict_path(
        &self,
        file_id: FileId,
    ) -> Result<Option<String>, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut path = self
            .target_head
            .workspace
            .first_path_at_with_accounting(
                self.target_head.id,
                file_id,
                &mut accounting,
                &cancellation,
            )
            .await?;
        if path.is_none() {
            path = self
                .source_head
                .workspace
                .first_path_at_with_accounting(
                    self.source_head.id,
                    file_id,
                    &mut accounting,
                    &cancellation,
                )
                .await?;
        }
        let Some(path) = path else {
            return Ok(None);
        };
        let text = path
            .value
            .unicode_text_with_accounting(&mut accounting, &cancellation);
        path.retire(&mut accounting)?;
        text.map(|(text, _allocation)| Some(text))
            .map_err(Into::into)
    }

    async fn regular_conflict_view(
        &self,
        file_id: FileId,
        path: Option<String>,
        records: [crate::kernel::FileRecord; 3],
        contents: ConflictContents,
    ) -> Result<Option<crate::ConflictView>, WorkspaceError> {
        if records
            .iter()
            .any(|record| record.kind != FileKind::Regular)
        {
            return Ok(None);
        }
        let mut bodies: [Option<acyclic_stream::OwnedBytes>; 3] = std::array::from_fn(|_| None);
        let reference = || {
            let mut view = record_conflict_view(self, file_id, path.clone(), ConflictKind::Binary);
            [view.base, view.ours, view.theirs] =
                std::array::from_fn(|index| ConflictValue::Record {
                    generation: [self.base.id, self.target_head.id, self.source_head.id][index],
                    file_id: records[index].file_id,
                });
            view
        };
        let limits = crate::text_merge::ByteMergeLimits {
            max_bytes: self
                .target_head
                .workspace
                .volume
                .config
                .limits
                .maximum_read_bytes,
        };
        for (record, body) in records.into_iter().zip(&mut bodies) {
            let Some(bytes) = self
                .target_head
                .workspace
                .volume
                .fs
                .workspace_conflict_bytes(&self.target_head.workspace.volume, record)
                .await?
            else {
                return Ok(Some(reference()));
            };
            *body = Some(bytes);
        }
        let [Some(base), Some(ours), Some(theirs)] = bodies else {
            return Err(WorkspaceError::InvalidMergeResolution);
        };
        let bodies = [base, ours, theirs];
        let (kind, [base, ours, theirs]) = match bodies
            .each_ref()
            .map(|bytes| crate::text_merge::text_from_bytes(bytes, limits))
        {
            [Ok(base), Ok(ours), Ok(theirs)] => (
                ConflictKind::Text,
                [base, ours, theirs].map(|text| ConflictValue::Text(text.to_owned())),
            ),
            _ if contents == ConflictContents::Files => return Ok(Some(reference())),
            _ => (
                ConflictKind::Binary,
                bodies
                    .each_ref()
                    .map(|bytes| ConflictValue::Binary(bytes.to_vec())),
            ),
        };
        Ok(Some(crate::ConflictView {
            key: ConflictKey::File(file_id),
            path,
            kind,
            base,
            ours,
            theirs,
        }))
    }

    /// Applies exact resolutions under the supplied writer permit.
    pub async fn apply(
        &self,
        options: ApplyOptions,
        resolutions: BTreeMap<MergeConflict, crate::kernel::MergeConflictResolution>,
        permit: crate::PublicationPermit,
    ) -> Result<JoinOutcome<A, O>, WorkspaceError> {
        if options.if_target != self.target_head.id {
            return Ok(JoinOutcome::StaleTarget(
                self.target_head
                    .workspace
                    .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| failure.error)?
                    .value,
            ));
        }
        let outcome = self
            .target_head
            .workspace
            .volume
            .fs
            .apply_workspace_join(self.join_request(options, &resolutions, permit))
            .await?;
        self.map_join_outcome(outcome)
    }

    pub(crate) async fn prepare_with_accounting(
        &self,
        options: ApplyOptions,
        resolutions: &BTreeMap<MergeConflict, crate::kernel::MergeConflictResolution>,
        permit: crate::PublicationPermit,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<JoinPreparation, WorkspaceError> {
        if options.if_target != self.target_head.id {
            return Err(WorkspaceError::StaleTarget);
        }
        self.target_head
            .workspace
            .volume
            .fs
            .prepare_workspace_join_with_accounting(
                self.join_request(options, resolutions, permit),
                accounting,
                cancellation,
            )
            .await
    }

    /// Prepares a retained read projection of the original three-way merge.
    /// Unresolved regions stay separate from the clean checkpoint and cannot
    /// enter the publication path as implicit target-side selections.
    pub(crate) async fn preview_with_accounting(
        &self,
        options: ApplyOptions,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<JoinPreparation, WorkspaceError> {
        if options.if_target != self.target_head.id {
            return Err(WorkspaceError::StaleTarget);
        }
        self.target_head
            .workspace
            .volume
            .fs
            .preview_workspace_join_with_accounting(
                self.join_request(
                    options,
                    &BTreeMap::new(),
                    crate::PublicationPermit::Unrestricted,
                ),
                accounting,
                cancellation,
            )
            .await
    }

    fn join_request<'a>(
        &'a self,
        options: ApplyOptions,
        resolutions: &'a BTreeMap<MergeConflict, crate::kernel::MergeConflictResolution>,
        permit: crate::PublicationPermit,
    ) -> crate::facade::WorkspaceJoinRequest<'a, A, O> {
        crate::facade::WorkspaceJoinRequest {
            target: &self.target_head.workspace.volume,
            base: self.base,
            source: &self.source_head,
            expected_target: options.if_target,
            expected_head: self.target_authority_head,
            history: self.history,
            operation_id: options.idempotency_key.operation_id(),
            permit,
            maximum_generations: self.maximum_generations,
            maximum_changes: self.maximum_changes,
            maximum_conflicts: self.maximum_conflicts,
            resolutions: crate::facade::WorkspaceJoinResolutions::Map(resolutions),
        }
    }

    pub(crate) fn map_join_outcome(
        &self,
        outcome: crate::facade::WorkspaceJoinOutcome,
    ) -> Result<JoinOutcome<A, O>, WorkspaceError> {
        let generation = |id| Generation {
            workspace: self.target_head.workspace.clone(),
            id,
            publication_closure: None,
        };
        Ok(match outcome {
            crate::facade::WorkspaceJoinOutcome::Joined(id, resolutions_digest) => {
                JoinOutcome::Applied(JoinApplication {
                    generation: generation(id),
                    resolutions_digest,
                })
            }
            crate::facade::WorkspaceJoinOutcome::AlreadyJoined(id, resolutions_digest) => {
                JoinOutcome::AlreadyApplied(JoinApplication {
                    generation: generation(id),
                    resolutions_digest,
                })
            }
            crate::facade::WorkspaceJoinOutcome::Applied(_)
            | crate::facade::WorkspaceJoinOutcome::AlreadyApplied(_) => {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            crate::facade::WorkspaceJoinOutcome::NoChanges(id) => {
                JoinOutcome::NoChanges(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::Stale(id) => {
                JoinOutcome::StaleTarget(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::Conflicted(conflicts, truncated) => {
                JoinOutcome::Conflicted {
                    conflicts,
                    truncated,
                }
            }
            crate::facade::WorkspaceJoinOutcome::Fenced => JoinOutcome::Fenced,
            crate::facade::WorkspaceJoinOutcome::IdempotencyConflict => {
                JoinOutcome::IdempotencyConflict
            }
        })
    }
}

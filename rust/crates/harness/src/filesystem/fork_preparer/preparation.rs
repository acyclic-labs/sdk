//! Journaled captures of the selected history and volumes.
use super::*;
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> PreparationAttempt<'_, A, O> {
    #[allow(
        clippy::too_many_lines,
        clippy::cognitive_complexity,
        reason = "one journaled fork preparation must bind each side effect to the same request"
    )]
    pub(super) async fn prepare(&self, request: ForkRequest) -> Result<ForkReport> {
        if request.authorized_source.is_some() && self.task_preparation.is_none() {
            return Err(bounded_io::failure(
                self.io.as_ref(),
                crate::wire::ErrorCode::Unauthorized,
                "authorized workspace capture requires its original preparation lease",
            ));
        }
        self.verify_task_lease(false).await?;
        let summary_proof = self
            .preparer
            .prepare_bound_summary(&request, self.io.as_ref())?;
        let journal = self.claim(&request).await?;
        if let Some(report) = read_report(
            &self.preparer.host,
            &journal,
            &request,
            self.journal_limit(MAX_REPORT_BYTES),
            self.io.as_ref(),
        )
        .await?
        {
            self.verify_task_allocations(&request).await?;
            return Ok(report);
        }
        let (source_project, source_generation) = request
            .selections
            .iter()
            .find_map(|selection| match &selection.revision {
                ResourceRevision::Project { volume, generation } => Some((volume, generation)),
                _ => None,
            })
            .ok_or_else(|| {
                bounded_io::failure(
                    self.io.as_ref(),
                    crate::wire::ErrorCode::Invalid,
                    "fork has no project selection",
                )
            })?;
        let request_digest = blake3::Hash::from(record_digest(
            &request,
            self.journal_limit(MAX_REQUEST_BYTES),
            self.io.as_ref(),
        )?);
        for (volume, role) in creation_volumes(&request) {
            self.verify_task_lease(false).await?;
            self.preparer
                .host
                .claim_fork_allocation(
                    AllocationClaim {
                        operation_id: request.operation_id,
                        preparation_digest: *request_digest.as_bytes(),
                        task_origin: self
                            .task_preparation
                            .map(|task| self.clone_origin(task.origin()))
                            .transpose()?,
                        parent: self.clone_authority(&request.parent)?,
                        child: self.clone_authority(&request.child)?,
                        volume: crate::executor::with_history_io(self.io.as_ref(), |work| {
                            volume.copy(work)
                        })?,
                        role,
                    },
                    self.journal_limit(MAX_REQUEST_BYTES),
                    self.io.as_ref(),
                    self.task_preparation,
                    #[cfg(not(target_arch = "wasm32"))]
                    self.native_creation_guard.as_ref(),
                )
                .await?;
        }
        let private = crate::executor::with_history_io(self.io.as_ref(), |work| {
            request.preparation.child_private_volume.copy(work)
        })?;
        let (source_private, private_generation, paths) = request
            .selections
            .iter()
            .find_map(|selection| match &selection.revision {
                ResourceRevision::PrivateVolume {
                    volume,
                    generation,
                    paths,
                } => Some((volume, generation, paths)),
                _ => None,
            })
            .ok_or_else(|| {
                bounded_io::failure(
                    self.io.as_ref(),
                    crate::wire::ErrorCode::Invalid,
                    "fork has no parent scratch selection",
                )
            })?;
        let private_key = IdempotencyKey::new(crate::contract::preparation_format(
            format_args!("fork:{}:{request_digest}:scratch", request.operation_id),
            self.io.as_ref(),
        )?)?;
        let private_grant = self.read_grant(source_private)?;
        let project_grant = if request.authorized_source.is_some() {
            std::borrow::Cow::Borrowed(self.preparer.sealed_project_read(
                &request,
                source_project,
                source_generation,
                self.io.as_ref(),
                self.task_preparation,
            )?)
        } else {
            std::borrow::Cow::Owned(self.read_grant(source_project)?)
        };
        self.verify_task_lease(false).await?;
        let private_observation = bounded_io::fork(
            &self.preparer.host,
            source_private,
            &private_grant,
            private_generation,
            &private,
            Some(self.clone_paths(paths)?),
            &private_key,
            self.io.as_ref(),
            self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        )
        .await?;
        let controller = self.preparer.controller(self.io.as_ref())?;
        let project_key = IdempotencyKey::new(crate::contract::preparation_format(
            format_args!("fork:{}:{request_digest}:project", request.operation_id),
            self.io.as_ref(),
        )?)?;
        self.verify_task_lease(false).await?;
        let child_project = bounded_io::fork(
            &self.preparer.host,
            source_project,
            &project_grant,
            source_generation,
            &request.preparation.child_project_volume,
            None,
            &project_key,
            self.io.as_ref(),
            self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        )
        .await?;
        let (child_result_volume, child_result_generation) =
            if let Some(volume) = &request.preparation.child_result_volume {
                self.verify_task_lease(false).await?;
                let key = IdempotencyKey::new(crate::contract::preparation_format(
                    format_args!("fork:{}:{request_digest}:result", request.operation_id),
                    self.io.as_ref(),
                )?)?;
                let result = bounded_io::fork(
                    &self.preparer.host,
                    source_private,
                    &private_grant,
                    private_generation,
                    volume,
                    Some(Vec::new()),
                    &key,
                    self.io.as_ref(),
                    self.task_preparation,
                    #[cfg(not(target_arch = "wasm32"))]
                    self.native_creation_guard.as_ref(),
                )
                .await?;
                (
                    Some(crate::executor::with_history_io(
                        self.io.as_ref(),
                        |work| volume.copy(work),
                    )?),
                    Some(result.generation),
                )
            } else {
                (None, None)
            };
        let (
            mut child_private_generation,
            mut inherited_context,
            mut reference_grants,
            attachment_manifests,
        ) = if request.preparation.inherited_through_sequence == 0 {
            (
                self.io.as_ref().map_or_else(
                    || Ok(private_observation.generation.clone()),
                    |io| private_observation.generation.clone_with_io(io),
                )?,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
        } else {
            let context_key = IdempotencyKey::new(crate::contract::preparation_format(
                format_args!("fork:{}:{request_digest}:context", request.operation_id),
                self.io.as_ref(),
            )?)?;
            let continuation_keys = if request.preparation.summary.is_some() {
                let keys =
                    crate::filesystem::summary_fork::summary_keys(&context_key, self.io.as_ref())?;
                let mut values = bounded_io::vector(keys.len(), self.io.as_ref())?;
                values.extend(keys);
                values
            } else {
                Vec::new()
            };
            let inherited = controller
                .materialize_inherited_conversation_continued(
                    &self.preparer.parent,
                    &private,
                    &private_observation.generation,
                    request.child_agent,
                    &request.attached_agents,
                    request.preparation.inherited_through_sequence,
                    usize::try_from(request.preparation.maximum_inherited_messages).map_err(
                        |_| {
                            bounded_io::failure(
                                self.io.as_ref(),
                                crate::wire::ErrorCode::Invalid,
                                "fork message bound is too large",
                            )
                        },
                    )?,
                    request.preparation.maximum_inherited_bytes,
                    request.preparation.maximum_inherited_references as usize,
                    self.preparer.resolver.as_ref(),
                    &context_key,
                    &continuation_keys,
                    self.preparer
                        .history
                        .as_ref()
                        .map(PreparedForkHistory::conversation),
                    self.io.as_ref(),
                    self.task_preparation,
                    #[cfg(not(target_arch = "wasm32"))]
                    self.native_creation_guard.as_ref(),
                )
                .await?;
            (
                inherited.generation,
                {
                    let mut files = bounded_io::vector(1, self.io.as_ref())?;
                    files.push(inherited.file);
                    files
                },
                inherited.reference_grants,
                inherited.attachment_manifests,
            )
        };
        let summary = if request.preparation.summary.is_some() {
            let bound = self.preparer.summary.as_ref().ok_or_else(|| {
                bounded_io::failure(
                    self.io.as_ref(),
                    crate::wire::ErrorCode::Unsupported,
                    "summary fork provider is not installed",
                )
            })?;
            let prepared = summary_proof.as_ref().ok_or_else(|| {
                bounded_io::failure(
                    self.io.as_ref(),
                    crate::wire::ErrorCode::Storage,
                    "summary source proof is missing",
                )
            })?;
            let key = IdempotencyKey::new(crate::contract::preparation_format(
                format_args!("fork:{}:{request_digest}:context", request.operation_id),
                self.io.as_ref(),
            )?)?;
            let prefix = inherited_context.first().ok_or_else(|| {
                bounded_io::failure(
                    self.io.as_ref(),
                    crate::wire::ErrorCode::Invalid,
                    "summary fork omitted its logical prefix",
                )
            })?;
            let (_, summary_references) =
                crate::filesystem::summary_fork::split_references(prepared, self.io.as_ref())?;
            add_summary_readers(
                &request,
                &mut reference_grants,
                summary_references.values(),
                self.io.as_ref(),
            )?;
            let (capture, generation) = controller
                .materialize_summary_context(
                    &self.preparer.parent,
                    prepared,
                    bound.journal.as_ref(),
                    &private,
                    &child_private_generation,
                    prefix,
                    request.preparation.maximum_inherited_bytes,
                    request.preparation.maximum_inherited_references as usize,
                    &key,
                    self.preparer.resolver.as_ref(),
                    self.io.as_ref(),
                    self.task_preparation,
                    #[cfg(not(target_arch = "wasm32"))]
                    self.native_creation_guard.as_ref(),
                )
                .await?;
            let capacity = inherited_context
                .len()
                .checked_add(capture.inherited_files().count())
                .ok_or_else(|| {
                    bounded_io::failure(
                        self.io.as_ref(),
                        crate::wire::ErrorCode::Invalid,
                        "summary context count overflows",
                    )
                })?;
            bounded_io::reserve(&mut inherited_context, capacity, self.io.as_ref())?;
            for file in capture.inherited_files() {
                inherited_context.push(crate::executor::with_history_io(
                    self.io.as_ref(),
                    |work| file.copy(work),
                )?);
            }
            child_private_generation = generation;
            Some(capture)
        } else {
            None
        };
        let mut captures = bounded_io::vector(request.selections.len(), self.io.as_ref())?;
        let shared_count = request.selections.iter().filter(|selection|
            matches!(&selection.revision, ResourceRevision::SharedVolume(volume) if volume.provider() == &self.preparer.host.provider))
            .count().checked_mul(request.attached_agents.len() + 1)
            .ok_or_else(|| bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Invalid, "shared reader count overflows"))?;
        let mut shared_grants = bounded_io::vector(shared_count, self.io.as_ref())?;
        for (index, selection) in request.selections.iter().enumerate() {
            self.verify_task_lease(false).await?;
            let revision = match &selection.revision {
                ResourceRevision::History(_) => {
                    crate::executor::with_history_io(self.io.as_ref(), |work| {
                        selection.revision.copy(work).map(|(value, _)| value)
                    })?
                }
                ResourceRevision::Project { .. } => ResourceRevision::Project {
                    volume: crate::executor::with_history_io(self.io.as_ref(), |work| {
                        request.preparation.child_project_volume.copy(work)
                    })?,
                    generation: self.io.as_ref().map_or_else(
                        || Ok(child_project.generation.clone()),
                        |io| child_project.generation.clone_with_io(io),
                    )?,
                },
                ResourceRevision::PrivateVolume { paths, .. } => ResourceRevision::PrivateVolume {
                    volume: crate::executor::with_history_io(self.io.as_ref(), |work| {
                        private.copy(work)
                    })?,
                    generation: self.io.as_ref().map_or_else(
                        || Ok(private_observation.generation.clone()),
                        |io| private_observation.generation.clone_with_io(io),
                    )?,
                    paths: self.clone_paths(paths)?,
                },
                ResourceRevision::SharedVolume(volume)
                    if volume.provider() == &self.preparer.host.provider =>
                {
                    self.read_grant(volume)?;
                    let reference = bounded_io::content_workspace(
                        &self.preparer.host.provider,
                        volume,
                        self.io.as_ref(),
                    )?;
                    bounded_io::resolve(&self.preparer.host, &reference, self.io.as_ref()).await?;
                    for reader in std::iter::once(request.child_agent)
                        .chain(request.attached_agents.iter().copied())
                    {
                        bounded_io::item(
                            self.io.as_ref(),
                            std::mem::size_of::<VolumeOperation>() as u64,
                        )?;
                        shared_grants.push(SharedGrant {
                            volume: crate::executor::with_history_io(self.io.as_ref(), |work| {
                                volume.copy(work)
                            })?,
                            child_agent: reader,
                            operations: BTreeSet::from([VolumeOperation::Read]),
                        });
                    }
                    crate::executor::with_history_io(self.io.as_ref(), |work| {
                        selection.revision.copy(work).map(|(value, _)| value)
                    })?
                }
                _ => {
                    let capture = if let Some(provider) = self
                        .preparer
                        .capture_providers
                        .iter()
                        .find(|provider| provider.provider() == selection.revision.provider())
                    {
                        if let Some(committed) =
                            self.capture_result(&journal, index, selection).await?
                        {
                            committed
                        } else {
                            let captured = if self.start_capture(&journal, &request, index).await? {
                                self.verify_task_lease(false).await?;
                                provider.capture(&request, selection).await?
                            } else {
                                provider
                                    .reconcile(&request, selection)
                                    .await?
                                    .ok_or(Error::Indeterminate(request.operation_id))?
                            };
                            self.commit_capture_result(
                                &journal, &request, index, selection, captured,
                            )
                            .await?
                        }
                    } else {
                        Capture::Unsupported(bounded_io::clone_text(
                            "resource capture provider is not installed",
                            self.io.as_ref(),
                        )?)
                    };
                    captures.push(capture);
                    continue;
                }
            };
            captures.push(Capture::Captured(CapturedResource {
                source: crate::executor::with_history_io(self.io.as_ref(), |work| {
                    selection.revision.copy(work).map(|(value, _)| value)
                })?,
                revision,
            }));
        }
        let inherited_through_sequence = request.preparation.inherited_through_sequence;
        let mut report = ForkReport {
            request,
            captures,
            child_private_volume: private,
            child_private_generation,
            child_result_volume,
            child_result_generation,
            inherited_context,
            summary,
            inherited_through_sequence,
            shared_grants,
            reference_grants,
            history_witness: None,
            attachment_manifests,
        };
        let mut history = self.io.as_ref().map(PreparationIo::history_io);
        report.validate(
            &mut history
                .as_mut()
                .map(|history| history as &mut dyn crate::store::history::HistoryIo),
        )?;
        if let Some(preparation) = &self.task_preparation {
            for grant in &report.reference_grants {
                require_reference_donor(preparation.grants(), grant, self.io.as_ref())?;
                if grant.reader == report.request.child_agent
                    && !{
                        let key = bounded_io::reference_key(&grant.file, self.io.as_ref())?;
                        preparation.request().child.grants.contains(&key)
                    }
                {
                    return Err(bounded_io::failure(
                        self.io.as_ref(),
                        crate::wire::ErrorCode::Unauthorized,
                        "prepared child omitted an exact inherited-file read grant",
                    ));
                }
            }
        }
        let required_captures_ready =
            report
                .request
                .selections
                .iter()
                .zip(&report.captures)
                .all(|(selection, capture)| {
                    !selection.required || matches!(capture, Capture::Captured(_))
                });
        if required_captures_ready {
            self.verify_task_lease(false).await?;
            let mut seed = if let Some(io) = &self.io {
                let mut observer = io.history_io();
                report
                    .copy(&mut Some(&mut observer))?
                    .0
                    .into_seed(&mut Some(&mut observer))?
            } else {
                report.clone().into_seed(&mut None)?
            };
            if let Some(history) = &self.preparer.history {
                let mut observer = self.io.as_ref().map(PreparationIo::history_io);
                self.preparer.parent.attest_fork_history_with_work(
                    &mut seed,
                    history.conversation(),
                    &mut observer
                        .as_mut()
                        .map(|observer| observer as &mut dyn crate::store::history::HistoryIo),
                )?;
                report.history_witness = seed.history_witness;
            }
            self.preparer
                .host
                .bind_fork_seed(
                    &seed,
                    self.journal_limit(MAX_REQUEST_BYTES),
                    self.io.as_ref(),
                    self.task_preparation,
                    #[cfg(not(target_arch = "wasm32"))]
                    self.native_creation_guard.as_ref(),
                )
                .await?;
        }
        self.verify_task_lease(false).await?;
        self.commit_report(&journal, &report).await
    }
}

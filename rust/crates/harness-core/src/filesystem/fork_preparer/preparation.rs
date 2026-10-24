//! Journaled capture of a selected parent revision.
use super::*;
use crate::executor::{stock_boundary, stock_string_copy, with_history_io};

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> PreparationAttempt<'_, A, O> {
    pub(super) async fn prepare(&self, request: ForkRequest) -> Result<ForkReport> {
        let preparer = self.preparer;
        let host = &preparer.host;
        let io = self.io;
        let task = self.task_preparation;
        let invalid = |reason| stock_boundary(io, crate::wire::ErrorCode::Invalid, reason);
        if request.authorized_source.is_some() && task.is_none() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "authorized workspace capture requires its original preparation lease",
            ));
        }
        self.verify_task_lease(false).await?;
        let journal = self.claim(&request).await?;
        if let Some(report) = read_report(
            host,
            &journal,
            &request,
            self.journal_limit(MAX_REPORT_BYTES),
            io,
        )
        .await?
        {
            self.verify_task_allocations(&request).await?;
            return Ok(report);
        }
        let policy = &request.preparation;
        let (source_project, source_generation) = request
            .selections
            .iter()
            .find_map(|selection| match &selection.revision {
                ResourceRevision::Project { volume, generation } => Some((volume, generation)),
                _ => None,
            })
            .ok_or_else(|| invalid("fork has no project selection"))?;
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
            .ok_or_else(|| invalid("fork has no parent scratch selection"))?;
        let digest = blake3::Hash::from(record_digest(
            &request,
            self.journal_limit(MAX_REQUEST_BYTES),
            io,
        )?);
        let operation = request.operation_id;
        let key = |suffix| {
            IdempotencyKey::new(with_history_io(io, |work| {
                crate::executor::preparation_format(
                    format_args!("fork:{operation}:{digest}:{suffix}"),
                    work,
                )
            })?)
        };
        let generation_copy = |generation: &crate::resources::GenerationRef| {
            io.map_or_else(|| Ok(generation.clone()), |io| generation.clone_with_io(io))
        };
        let mut observer = io;
        let mut work = observer
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        for volume in creation_volumes(&request) {
            self.verify_task_lease(false).await?;
            host.claim_fork_allocation(
                AllocationClaim {
                    operation_id: operation,
                    preparation_digest: *digest.as_bytes(),
                    task_origin: task
                        .map(|task| self.clone_origin(task.origin()))
                        .transpose()?,
                    parent: self.clone_authority(&request.parent)?,
                    child: self.clone_authority(&request.child)?,
                    volume: volume.copy(&mut work)?,
                },
                self.journal_limit(MAX_REQUEST_BYTES),
                io,
                task,
            )
            .await?;
        }
        let private = policy.child_private_volume.copy(&mut work)?;
        let private_key = key("scratch")?;
        let private_grant = self.read_grant(source_private)?;
        let project_grant = if request.authorized_source.is_some() {
            std::borrow::Cow::Borrowed(preparer.sealed_project_read(
                &request,
                source_project,
                source_generation,
                io,
                task.map(Arc::as_ref),
            )?)
        } else {
            std::borrow::Cow::Owned(self.read_grant(source_project)?)
        };
        self.verify_task_lease(false).await?;
        let private_observation = (host)
            .fork_volume_pinned(
                source_private,
                &private_grant,
                private_generation,
                &private,
                Some(self.clone_paths(paths)?),
                &private_key,
                io,
                task,
            )
            .await?;
        let controller = (request.parent.kind == crate::core::AggregateKind::Conversation)
            .then(|| preparer.controller(io))
            .transpose()?;
        let project_key = key("project")?;
        self.verify_task_lease(false).await?;
        let child_project = (host)
            .fork_volume_pinned(
                source_project,
                &project_grant,
                source_generation,
                &policy.child_project_volume,
                None,
                &project_key,
                io,
                task,
            )
            .await?;
        let (child_result_volume, child_result_generation) =
            if let Some(volume) = &policy.child_result_volume {
                self.verify_task_lease(false).await?;
                let result = (host)
                    .fork_volume_pinned(
                        source_private,
                        &private_grant,
                        private_generation,
                        volume,
                        Some(Vec::new()),
                        &key("result")?,
                        io,
                        task,
                    )
                    .await?;
                (Some(volume.copy(&mut work)?), Some(result.generation))
            } else {
                (None, None)
            };
        let context_key = if policy.inherited_through_sequence == 0 {
            None
        } else {
            Some(key("context")?)
        };
        let (
            mut child_private_generation,
            mut inherited_context,
            mut reference_grants,
            attachment_manifests,
        ) = if let Some(context_key) = &context_key {
            let controller = controller
                .as_ref()
                .ok_or_else(|| invalid("task fork cannot inherit a conversation"))?;
            let mut continuation_keys = Vec::new();
            if policy.summary.is_some() {
                let keys = crate::filesystem::summary_fork::summary_keys(context_key, io)?;
                PreparationIo::reserve(&mut continuation_keys, keys.len(), io)?;
                continuation_keys.extend(keys);
            }
            let inherited = controller
                .materialize_inherited_conversation(
                    &preparer.parent,
                    &private,
                    &private_observation.generation,
                    request.child_agent,
                    &request.attached_agents,
                    policy.inherited_through_sequence,
                    usize::try_from(policy.maximum_inherited_messages)
                        .map_err(|_| invalid("fork message bound is too large"))?,
                    policy.maximum_inherited_bytes,
                    policy.maximum_inherited_references as usize,
                    preparer.resolver.as_ref(),
                    context_key,
                    &continuation_keys,
                    preparer
                        .history
                        .as_ref()
                        .map(PreparedForkHistory::conversation),
                    io,
                    task,
                )
                .await?;
            let mut files = crate::model::vec_with_capacity(1, io)?;
            files.push(inherited.file);
            (
                inherited.generation,
                files,
                inherited.reference_grants,
                inherited.attachment_manifests,
            )
        } else {
            (
                generation_copy(&private_observation.generation)?,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
        };
        let summary = if policy.summary.is_some() {
            let controller = controller
                .as_ref()
                .ok_or_else(|| invalid("task fork cannot summarize a conversation"))?;
            let bound = preparer
                .summary
                .as_ref()
                .ok_or_else(|| invalid("summary fork has no prepared Summary context"))?;
            let prefix = inherited_context
                .first()
                .ok_or_else(|| invalid("summary fork omitted its logical prefix"))?;
            let (_, references) =
                crate::filesystem::summary_fork::split_references(&bound.prepared, io)?;
            add_summary_readers(&request, &mut reference_grants, &references, io)?;
            let (capture, generation) = controller
                .materialize_summary_context(
                    &preparer.parent,
                    &bound.prepared,
                    bound.journal.as_ref(),
                    &private,
                    &child_private_generation,
                    prefix,
                    policy.maximum_inherited_bytes,
                    policy.maximum_inherited_references as usize,
                    context_key
                        .as_ref()
                        .ok_or_else(|| invalid("summary fork has no inherited prefix key"))?,
                    preparer.resolver.as_ref(),
                    io,
                    task,
                )
                .await?;
            let capacity = inherited_context
                .len()
                .checked_add(capture.inherited_files().count())
                .ok_or_else(|| invalid("summary context count overflows"))?;
            PreparationIo::reserve(&mut inherited_context, capacity, io)?;
            for file in capture.inherited_files() {
                inherited_context.push(file.copy(&mut work)?);
            }
            child_private_generation = generation;
            Some(capture)
        } else {
            None
        };
        let mut captures = crate::model::vec_with_capacity(request.selections.len(), io)?;
        let shared_count = request.selections.iter().filter(|selection|
            matches!(&selection.revision, ResourceRevision::SharedVolume(volume) if volume.provider() == &host.provider))
            .count().checked_mul(request.attached_agents.len() + 1)
            .ok_or_else(|| invalid("shared reader count overflows"))?;
        let mut shared_grants = crate::model::vec_with_capacity(shared_count, io)?;
        for (index, selection) in request.selections.iter().enumerate() {
            self.verify_task_lease(false).await?;
            let revision = match &selection.revision {
                ResourceRevision::History(_) => selection.revision.copy(&mut work)?.0,
                ResourceRevision::Project { .. } => ResourceRevision::Project {
                    volume: (policy.child_project_volume.copy(&mut work)?).into(),
                    generation: (generation_copy(&child_project.generation)?).into(),
                },
                ResourceRevision::PrivateVolume { paths, .. } => ResourceRevision::PrivateVolume {
                    volume: (private.copy(&mut work)?).into(),
                    generation: (generation_copy(&private_observation.generation)?).into(),
                    paths: self.clone_paths(paths)?,
                },
                ResourceRevision::SharedVolume(volume) if volume.provider() == &host.provider => {
                    self.read_grant(volume)?;
                    let reference = bounded_io::content_workspace(&host.provider, volume, io)?;
                    (host).resolve(&reference, io).await?;
                    for reader in std::iter::once(request.child_agent)
                        .chain(request.attached_agents.iter().copied())
                    {
                        crate::model::charge_items(io, 1)?;
                        if let Some(io) = io {
                            io.retain_owned(std::mem::size_of::<VolumeOperation>() as u64)?;
                        }
                        shared_grants.push(SharedGrant {
                            volume: volume.copy(&mut work)?,
                            child_agent: reader,
                            operations: BTreeSet::from([VolumeOperation::Read]),
                        });
                    }
                    selection.revision.copy(&mut work)?.0
                }
                _ => {
                    let capture = match preparer
                        .capture_providers
                        .iter()
                        .find(|provider| provider.provider() == selection.revision.provider())
                    {
                        Some(provider) => {
                            self.capture(&journal, &request, index, selection, provider.as_ref())
                                .await?
                        }
                        None => Capture::Unsupported(stock_string_copy(
                            "resource capture provider is not installed",
                            io,
                        )?),
                    };
                    captures.push(capture);
                    continue;
                }
            };
            captures.push(Capture::Captured(Box::new(CapturedResource {
                source: selection.revision.copy(&mut work)?.0,
                revision,
            })));
        }
        let inherited_through_sequence = policy.inherited_through_sequence;
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
        report.validate(&mut work)?;
        if let Some(task) = task {
            for grant in &report.reference_grants {
                require_reference_donor(&task.origin().source_grants, grant, io)?;
                if grant.reader == report.request.child_agent {
                    let capability = match io {
                        Some(io) => grant
                            .file
                            .read_capability(&mut Some(&mut io.history_io()))?,
                        None => grant.file.read_capability(&mut work)?,
                    };
                    if !task.request().child.grants.contains(&capability) {
                        return Err(stock_boundary(
                            io,
                            crate::wire::ErrorCode::Unauthorized,
                            "prepared child omitted an exact inherited-file read grant",
                        ));
                    }
                }
            }
        }
        if report
            .request
            .selections
            .iter()
            .zip(&report.captures)
            .all(|(selection, capture)| {
                !selection.required || matches!(capture, Capture::Captured(_))
            })
        {
            self.verify_task_lease(false).await?;
            let mut seed = report.copy(&mut work)?.0.into_seed(&mut work)?;
            if let Some(history) = &preparer.history {
                preparer.parent.attest_fork_history_with_work(
                    &mut seed,
                    history.conversation(),
                    &mut work,
                )?;
                report.history_witness = seed.history_witness.clone();
            }
            host.bind_fork_seed(&seed, self.journal_limit(MAX_REQUEST_BYTES), io, task)
                .await?;
        }
        self.verify_task_lease(false).await?;
        self.commit_report(&journal, &report).await
    }
}

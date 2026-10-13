use super::*;
impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemForkPreparer<A, O> {
    pub(super) async fn prepare_inner(&self, request: ForkRequest) -> Result<ForkReport> {
        self.check_request(&request)?;
        let journal = self.claim(&request).await?;
        if let Some(report) = self.read_report(&journal, &request).await? {
            return Ok(report);
        }
        let source_generation = request
            .selections
            .iter()
            .find_map(|selection| match &selection.revision {
                ResourceRevision::Project { generation, .. } => Some(generation),
                _ => None,
            })
            .ok_or_else(|| Error::Invalid("fork has no project selection".into()))?;
        let request_bytes = encode_record(&request, MAX_REQUEST_BYTES)?;
        let request_digest = blake3::hash(&request_bytes);
        for volume in [
            &request.preparation.child_private_volume,
            &request.preparation.child_project_volume,
        ] {
            self.host
                .claim_fork_allocation(AllocationClaim {
                    operation_id: request.operation_id,
                    preparation_digest: *request_digest.as_bytes(),
                    parent: request.parent.clone(),
                    child: request.child.clone(),
                    volume: volume.clone(),
                })
                .await?;
        }
        let private = request.preparation.child_private_volume.clone();
        let (source_private, private_generation, paths) = request
            .selections
            .iter()
            .find_map(|selection| {
                if let ResourceRevision::PrivateVolume {
                    volume,
                    generation,
                    paths,
                } = &selection.revision
                {
                    Some((volume, generation, paths))
                } else {
                    None
                }
            })
            .ok_or_else(|| Error::Invalid("fork has no parent scratch selection".into()))?;
        let private_key = IdempotencyKey::new(format!(
            "fork:{}:{request_digest}:scratch",
            request.operation_id
        ))?;
        let private_observation = self
            .host
            .fork_volume_pinned(
                source_private,
                private_generation,
                &private,
                Some(paths.clone()),
                &private_key,
            )
            .await?;
        let controller = ParentProjectController::new(
            &self.host,
            &self.parent,
            &self.verifier,
            &self.scope,
            &self.project,
            None,
        )?;
        let project_key = IdempotencyKey::new(format!(
            "fork:{}:{request_digest}:project",
            request.operation_id
        ))?;
        let child_project = controller
            .fork_project(
                source_generation,
                &request.preparation.child_project_volume,
                &project_key,
            )
            .await?;
        let (
            mut child_private_generation,
            mut inherited_context,
            mut reference_grants,
            attachment_manifests,
        ) = if request.preparation.inherited_through_sequence == 0 {
            (
                private_observation.generation.clone(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
        } else {
            let context_key = IdempotencyKey::new(format!(
                "fork:{}:{request_digest}:context",
                request.operation_id
            ))?;
            let continuation_keys = if request.preparation.summary.is_some() {
                crate::filesystem::summary_fork::summary_keys(&context_key)?.to_vec()
            } else {
                Vec::new()
            };
            let inherited = controller
                .materialize_inherited_conversation_continued(
                    &self.parent,
                    &private,
                    &private_observation.generation,
                    request.child_agent,
                    &request.attached_agents,
                    request.preparation.inherited_through_sequence,
                    usize::try_from(request.preparation.maximum_inherited_messages)
                        .map_err(|_| Error::Invalid("fork message bound is too large".into()))?,
                    request.preparation.maximum_inherited_bytes,
                    request.preparation.maximum_inherited_references as usize,
                    self.resolver.as_ref(),
                    &context_key,
                    &continuation_keys,
                )
                .await?;
            (
                inherited.generation,
                vec![inherited.file],
                inherited.reference_grants,
                inherited.attachment_manifests,
            )
        };
        let summary = if request.preparation.summary.is_some() {
            let bound = self.summary.as_ref().ok_or_else(|| {
                Error::Unsupported("summary fork provider is not installed".into())
            })?;
            let key = IdempotencyKey::new(format!(
                "fork:{}:{request_digest}:context",
                request.operation_id
            ))?;
            let prefix = inherited_context
                .first()
                .ok_or_else(|| Error::Invalid("summary fork omitted its logical prefix".into()))?;
            // Count the exact additional reader grants before either Summary
            // publication phase. Reuse original prefix grants when identical.
            let (_, summary_references) =
                crate::filesystem::summary_fork::split_references(&bound.prepared)?;
            let mut planned_grants = reference_grants
                .iter()
                .map(|grant| Ok((grant.capability()?, grant.reader)))
                .collect::<Result<BTreeSet<_>>>()?;
            for reader in
                std::iter::once(request.child_agent).chain(request.attached_agents.iter().copied())
            {
                for file in summary_references.values() {
                    if file.volume().owner() != &crate::conversation::VolumeOwner::Agent(reader) {
                        planned_grants.insert((file.read_capability()?, reader));
                    }
                }
            }
            if planned_grants.len() > request.preparation.maximum_inherited_references as usize {
                return Err(Error::Invalid(
                    "summary fork reader grants exceed capture allowance".into(),
                ));
            }
            let (capture, generation) = controller
                .materialize_summary_context(
                    &self.parent,
                    &bound.prepared,
                    bound.journal.as_ref(),
                    &private,
                    &child_private_generation,
                    prefix,
                    request.preparation.maximum_inherited_bytes,
                    request.preparation.maximum_inherited_references as usize,
                    &key,
                )
                .await?;
            let mut granted = reference_grants
                .iter()
                .map(|grant| Ok((grant.capability()?, grant.reader)))
                .collect::<Result<BTreeSet<_>>>()?;
            for reader in
                std::iter::once(request.child_agent).chain(request.attached_agents.iter().copied())
            {
                for file in &capture.references {
                    if file.volume().owner() != &crate::conversation::VolumeOwner::Agent(reader)
                        && granted.insert((file.read_capability()?, reader))
                    {
                        reference_grants.push(ReferenceGrant {
                            file: file.clone(),
                            reader,
                            attachment_manifest: None,
                        });
                    }
                }
            }
            inherited_context.extend(capture.inherited_files().cloned());
            child_private_generation = generation;
            Some(capture)
        } else {
            None
        };
        let mut captures = Vec::with_capacity(request.selections.len());
        let mut shared_grants = Vec::new();
        for (index, selection) in request.selections.iter().enumerate() {
            let capture = match &selection.revision {
                ResourceRevision::History(_) => Capture::Captured(CapturedResource {
                    source: selection.revision.clone(),
                    revision: selection.revision.clone(),
                }),
                ResourceRevision::Project { .. } => Capture::Captured(CapturedResource {
                    source: selection.revision.clone(),
                    revision: ResourceRevision::Project {
                        volume: request.preparation.child_project_volume.clone(),
                        generation: child_project.generation.clone(),
                    },
                }),
                ResourceRevision::PrivateVolume { paths, .. } => {
                    Capture::Captured(CapturedResource {
                        source: selection.revision.clone(),
                        revision: ResourceRevision::PrivateVolume {
                            volume: private.clone(),
                            generation: private_observation.generation.clone(),
                            paths: paths.clone(),
                        },
                    })
                }
                ResourceRevision::SharedVolume(volume)
                    if volume.provider() == &self.host.provider =>
                {
                    ContentGrant::verify(
                        &self.verifier,
                        &self.scope,
                        volume,
                        VolumeOperation::Read,
                    )?;
                    let reference =
                        workspace_ref(self.host.provider.clone(), &volume.storage_name()?)?;
                    self.host.resolve(&reference).await?;
                    for reader in std::iter::once(request.child_agent)
                        .chain(request.attached_agents.iter().copied())
                    {
                        shared_grants.push(SharedGrant {
                            volume: volume.clone(),
                            child_agent: reader,
                            operations: BTreeSet::from([VolumeOperation::Read]),
                        });
                    }
                    Capture::Captured(CapturedResource {
                        source: selection.revision.clone(),
                        revision: selection.revision.clone(),
                    })
                }
                _ => {
                    if let Some(provider) = self
                        .capture_providers
                        .iter()
                        .find(|provider| provider.provider() == selection.revision.provider())
                    {
                        if let Some(committed) = self
                            .capture_result(&journal, &request, index, selection)
                            .await?
                        {
                            committed
                        } else {
                            let fresh = self
                                .start_capture(&journal, &request, index, selection)
                                .await?;
                            let captured = if fresh {
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
                        Capture::Unsupported("resource capture provider is not installed".into())
                    }
                }
            };
            captures.push(capture);
        }
        let inherited_through_sequence = request.preparation.inherited_through_sequence;
        let report = ForkReport {
            request,
            captures,
            child_private_volume: private.clone(),
            child_private_generation,
            inherited_context,
            summary,
            inherited_through_sequence,
            shared_grants,
            reference_grants,
            attachment_manifests,
        };
        report.validate(&mut None)?;
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
            let seed = report.clone().into_seed()?;
            self.host.bind_fork_seed(&seed).await?;
        }
        self.commit_report(&journal, &report).await
    }
}
impl<A, O> ForkPreparer for FilesystemForkPreparer<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn parent_snapshot(&self) -> (&Authority, u64) {
        (self.parent.authority(), self.parent.revision())
    }

    fn prepare<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<ForkReport>> {
        Box::pin(async move { self.prepare_inner(request).await })
    }

    fn reconcile<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<Option<ForkReport>>> {
        Box::pin(async move {
            self.authorize_request(&request)?;
            let journal = self.journal_ref(&request)?;
            match self.host.resolve(&journal).await {
                Ok(_) => {}
                Err(Error::NotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            }
            if !self.read_claim(&journal, &request).await? {
                return Ok(None);
            }
            self.read_report(&journal, &request).await
        })
    }
}

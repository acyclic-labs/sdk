use super::*;
use acyclic_harness::runtime::TaskStateProvider as _;

impl NativeSession {
    pub async fn publication(
        &self,
        command: &str,
        request: &NativePublicationRequest,
    ) -> Result<NativePublicationResult> {
        let binding = self.publication_binding(request).await?;
        let host = self.runtime.task_host();
        let fence = LeaseFence::from(&request.caller_lease.reservation);
        if command == "/publication-open" {
            host.verify_dispatch_owner(binding.caller, &fence, None)?
                .await?;
        }
        let (journal, interactions) = self.publication_journal(binding, request).await?;
        let state = acyclic_fs::LocalCoreStateStore::new(&binding.state_root);
        let cancellation = acyclic_fs::CancellationToken::new();
        let receipt = match acyclic_fs::load_native_materialization_journal(
            &state,
            fs_operation(binding.operation_id),
            binding.budget,
            &cancellation,
        )
        .await
        {
            Ok(receipt) => receipt,
            Err((error, work)) => return Ok(publication_failure(binding, error.to_string(), work)),
        };
        let observed = self
            .publication_result(binding, &interactions, receipt.value, receipt.work)
            .await?;
        let command = match command {
            "/publication-observe" => return Ok(observed),
            "/publication-reconcile"
                if observed.publication.as_ref().is_some_and(|journal| {
                    matches!(
                        journal.phase,
                        MaterializationPhase::Preparing | MaterializationPhase::Prepared
                    )
                }) && observed.interaction.is_none() =>
            {
                "/publication-open"
            }
            "/publication-reconcile"
                if matches!(observed.status, "approved" | "recovering")
                    && observed
                        .resolution
                        .as_ref()
                        .is_some_and(|value| value.outcome == InteractionOutcome::Approved) =>
            {
                "/publication-apply"
            }
            "/publication-open" | "/publication-apply" => command,
            _ => return Ok(observed),
        };
        match command {
            "/publication-open" => {
                if let Some(original) = &observed.publication
                    && original.phase != MaterializationPhase::Preparing
                {
                    if original.phase == MaterializationPhase::Prepared {
                        self.publication_open_approval(binding, &journal, original)
                            .await?;
                        return self
                            .publication_result(
                                binding,
                                &interactions,
                                observed.publication,
                                observed.work,
                            )
                            .await;
                    }
                    return Ok(observed);
                }
            }
            _ => {
                let original = observed.publication.as_ref().ok_or_else(|| {
                    Error::Conflict("original publication has not been prepared".into())
                })?;
                let digest = publication_digest(binding, original)?;
                journal
                    .verify(interaction_id(binding), binding.operation_id, digest)
                    .await?;
                host.verify_dispatch_owner(binding.caller, &fence, None)?
                    .await?;
                if original.phase == MaterializationPhase::Applied {
                    return Ok(observed);
                }
                if matches!(
                    original.phase,
                    MaterializationPhase::Preparing
                        | MaterializationPhase::RollingBack
                        | MaterializationPhase::RolledBack
                ) {
                    return Err(Error::Conflict(
                        "publication is not an approved prepared forward operation".into(),
                    ));
                }
            }
        }
        let (from, to) = self.publication_generations(binding).await?;
        let excluded = binding
            .excluded_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let options = publication_options(binding);
        host.verify_dispatch_owner(binding.caller, &fence, None)?
            .await?;
        let result = if command == "/publication-open" {
            acyclic_fs::prepare_native_generation_transition(
                &from,
                &to,
                &state,
                publication(binding, &excluded, &options, &cancellation),
            )
            .await
        } else {
            let digest = publication_digest(
                binding,
                observed.publication.as_ref().expect("checked original"),
            )?;
            let owner = host.journal_owner(binding.caller, &fence, None).await?;
            let work =
                match acyclic_fs::NativeMaterializationWork::new(binding.budget, &cancellation) {
                    Ok(work) => work,
                    Err((error, work)) => {
                        return Ok(publication_failure(binding, error.to_string(), work));
                    }
                };
            let guard_work = work.clone();
            let guard_journal = &journal;
            let guard_owner = &owner;
            let before_effect = || {
                let work = guard_work.clone();
                async move {
                    let result: Result<()> = async {
                        let mut observer = acyclic_harness::store::HistoryWork::new(
                            acyclic_harness::filesystem::PreparationIo::from_native_work(&work)?,
                        );
                        observer.preflight()?;
                        for (volume, operation) in [
                            (&binding.from_project, VolumeOperation::Read),
                            (&binding.to_project, VolumeOperation::Read),
                            (&binding.to_project, VolumeOperation::Write),
                        ] {
                            guard_owner.require_volume_grant(
                                volume,
                                operation,
                                &mut Some(&mut observer),
                            )?;
                        }
                        guard_journal
                            .verify(interaction_id(binding), binding.operation_id, digest)
                            .await?;
                        guard_owner.verify(false, &mut Some(&mut observer)).await
                    }
                    .await;
                    result.map_err(|error| {
                        acyclic_fs::NativeWorkspacePublicationError::Native(
                            acyclic_fs::NativeTreeMaterializationError::Io(std::io::Error::other(
                                error,
                            )),
                        )
                    })
                }
            };

            acyclic_fs::publish_native_generation_transition(
                &from,
                &to,
                &state,
                publication(binding, &excluded, &options, &cancellation),
                work,
                before_effect,
            )
            .await
        };
        match result {
            Ok(receipt) => {
                if command == "/publication-open" {
                    self.publication_open_approval(binding, &journal, &receipt.value)
                        .await?;
                }
                self.publication_result(binding, &interactions, Some(receipt.value), receipt.work)
                    .await
            }
            Err((error, work)) => Ok(publication_failure(binding, error.to_string(), work)),
        }
    }
    pub(super) async fn publication_generations(
        &self,
        binding: &NativePublicationConfig,
    ) -> Result<(
        acyclic_fs::Generation<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>,
        acyclic_fs::Generation<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>,
    )> {
        let filesystem =
            acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&self.config.filesystem_root))
                .await
                .map_err(storage)?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut generations = Vec::with_capacity(2);
        for (project, generation) in [
            (&binding.from_project, binding.from),
            (&binding.to_project, binding.to),
        ] {
            let workspace = filesystem
                .open_workspace(
                    project.storage_name(&mut None)?,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(|failure| storage(failure.error))?
                .value;
            generations.push(
                workspace
                    .generation(generation, WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| storage(failure.error))?
                    .value,
            );
        }
        let [from, to] = <[_; 2]>::try_from(generations)
            .map_err(|_| Error::Conflict("publication generations missing".into()))?;
        Ok((from, to))
    }
}

use super::*;

impl NativeSession {
    pub async fn publication(
        &self,
        command: &str,
        request: &NativePublicationRequest,
    ) -> Result<serde_json::Value> {
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
        if let Some(original) = &receipt.value {
            publication_digest(binding, original)?;
        }
        let mut approval = self
            .publication_approval(binding, &interactions, receipt.value.as_ref())
            .await?;
        let status = publication_status(receipt.value.as_ref(), approval.as_ref());
        let command = match command {
            "/publication-reconcile"
                if receipt.value.as_ref().is_some_and(|original| {
                    matches!(
                        original.phase,
                        MaterializationPhase::Preparing | MaterializationPhase::Prepared
                    )
                }) && approval.is_none() =>
            {
                "/publication-open"
            }
            "/publication-reconcile"
                if matches!(status, "approved" | "recovering")
                    && approval
                        .as_ref()
                        .and_then(|(_, resolution)| resolution.as_ref())
                        .is_some_and(|resolution| {
                            resolution.outcome == (InteractionOutcome::Approved {})
                        }) =>
            {
                "/publication-apply"
            }
            "/publication-open" | "/publication-apply" => command,
            _ => {
                return self.publication_report(binding, receipt, approval).await;
            }
        };
        if command == "/publication-open" {
            if let Some(original) = &receipt.value
                && original.phase != MaterializationPhase::Preparing
            {
                if original.phase == MaterializationPhase::Prepared {
                    self.publication_open_approval(binding, &journal, original)
                        .await?;
                    approval = self
                        .publication_approval(binding, &interactions, Some(original))
                        .await?;
                }
                return self.publication_report(binding, receipt, approval).await;
            }
        } else {
            let original = receipt.value.as_ref().ok_or_else(|| {
                Error::Conflict("original publication has not been prepared".into())
            })?;
            journal
                .verify(
                    interaction_id(binding),
                    binding.operation_id,
                    publication_digest(binding, original)?,
                )
                .await?;
            host.verify_dispatch_owner(binding.caller, &fence, None)?
                .await?;
            if original.phase == MaterializationPhase::Applied {
                return self.publication_report(binding, receipt, approval).await;
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
        let filesystem =
            acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&self.config.filesystem_root))
                .await
                .map_err(storage)?;
        let generation = |name: String, id| {
            let filesystem = &filesystem;
            let cancellation = &cancellation;
            async move {
                filesystem
                    .open_workspace(name, WorkBudget::UNBOUNDED, cancellation)
                    .await
                    .map_err(|failure| storage(failure.error))?
                    .value
                    .generation(id, WorkBudget::UNBOUNDED, cancellation)
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| storage(failure.error))
            }
        };
        let from = generation(binding.from_project.storage_name(&mut None)?, binding.from).await?;
        let to = generation(binding.to_project.storage_name(&mut None)?, binding.to).await?;
        let excluded = binding
            .excluded_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let options = acyclic_fs::MaterializeOptions {
            destination: binding.operation_directory.join("target"),
            expected_root_identity: None,
            maximum_directory_entries: binding.maximum_directory_entries,
            maximum_extent_spans: binding.maximum_extent_spans,
            transfer_bytes: binding.transfer_bytes,
        };
        let publication = acyclic_fs::NativeWorkspacePublication {
            root: &binding.root,
            operation_directory: &binding.operation_directory,
            source_root_identity: acyclic_fs::NativeRootIdentity::from_bytes(
                binding.source_root_identity,
            ),
            operation_id: fs_operation(binding.operation_id),
            from: binding.from,
            to: binding.to,
            excluded_names: &excluded,
            options: &options,
            budget: binding.budget,
            cancellation: &cancellation,
        };
        host.verify_dispatch_owner(binding.caller, &fence, None)?
            .await?;
        let result = if command == "/publication-open" {
            acyclic_fs::prepare_native_generation_transition(&from, &to, &state, publication).await
        } else {
            let digest =
                publication_digest(binding, receipt.value.as_ref().expect("checked original"))?;
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
                publication,
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
                let approval = self
                    .publication_approval(binding, &interactions, Some(&receipt.value))
                    .await?;
                self.publication_report(
                    binding,
                    acyclic_fs::OperationReceipt {
                        value: Some(receipt.value),
                        work: receipt.work,
                    },
                    approval,
                )
                .await
            }
            Err((error, work)) => Ok(publication_failure(binding, error.to_string(), work)),
        }
    }
}

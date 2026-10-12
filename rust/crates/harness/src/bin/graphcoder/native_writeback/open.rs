use super::*;
use acyclic_harness::runtime::TaskStateProvider;
impl NativeSession {
    pub async fn publication_open(
        &self,
        request: &NativePublicationRequest,
    ) -> Result<NativePublicationResult> {
        let binding = self.publication_binding(request).await?;
        self.runtime
            .task_host()
            .verify_dispatch_owner(
                binding.caller,
                LeaseFence::from(&request.caller_lease.reservation),
            )
            .await?;
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
        if let Some(original) = receipt.value {
            if original.phase != MaterializationPhase::Preparing {
                if original.phase == MaterializationPhase::Prepared {
                    self.publication_open_approval(binding, &journal, &original)
                        .await?;
                }
                return self
                    .publication_result(binding, &interactions, Some(original), receipt.work)
                    .await;
            }
        }
        let (from, to) = self.publication_generations(binding).await?;
        let excluded = binding
            .excluded_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let options = publication_options(binding);
        self.runtime
            .task_host()
            .verify_dispatch_owner(
                binding.caller,
                LeaseFence::from(&request.caller_lease.reservation),
            )
            .await?;
        match acyclic_fs::prepare_native_generation_transition(
            &from,
            &to,
            &state,
            publication(binding, &excluded, &options, &cancellation),
        )
        .await
        {
            Ok(receipt) => {
                self.publication_open_approval(binding, &journal, &receipt.value)
                    .await?;
                self.publication_result(binding, &interactions, Some(receipt.value), receipt.work)
                    .await
            }
            Err((error, work)) => Ok(publication_failure(binding, error.to_string(), work)),
        }
    }
    pub async fn publication_observe(
        &self,
        request: &NativePublicationRequest,
    ) -> Result<NativePublicationResult> {
        let binding = self.publication_binding(request).await?;
        let (_, interactions) = self.publication_journal(binding, request).await?;
        let state = acyclic_fs::LocalCoreStateStore::new(&binding.state_root);
        let cancellation = acyclic_fs::CancellationToken::new();
        match acyclic_fs::load_native_materialization_journal(
            &state,
            fs_operation(binding.operation_id),
            binding.budget,
            &cancellation,
        )
        .await
        {
            Ok(receipt) => {
                self.publication_result(binding, &interactions, receipt.value, receipt.work)
                    .await
            }
            Err((error, work)) => Ok(publication_failure(binding, error.to_string(), work)),
        }
    }
    pub async fn publication_reconcile(
        &self,
        request: &NativePublicationRequest,
    ) -> Result<NativePublicationResult> {
        let observed = self.publication_observe(request).await?;
        if observed.publication.as_ref().is_some_and(|journal| {
            matches!(
                journal.phase,
                MaterializationPhase::Preparing | MaterializationPhase::Prepared
            )
        }) && observed.interaction.is_none()
        {
            return self.publication_open(request).await;
        }
        if matches!(observed.status, "approved" | "recovering")
            && observed
                .resolution
                .as_ref()
                .is_some_and(|value| value.outcome == InteractionOutcome::Approved)
        {
            return self.publication_apply(request).await;
        }
        Ok(observed)
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
        let from_workspace = filesystem
            .open_workspace(
                binding.from_project.storage_name(&mut None)?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(storage)?;
        let to_workspace = filesystem
            .open_workspace(
                binding.to_project.storage_name(&mut None)?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(storage)?;
        let from = from_workspace
            .generation(binding.from, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(storage)?;
        let to = to_workspace
            .generation(binding.to, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(storage)?;
        Ok((from, to))
    }
}

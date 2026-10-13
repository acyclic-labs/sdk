use super::*;
use acyclic_harness::runtime::{DurableTaskHost as _, TaskStateProvider};
impl NativeSession {
    pub async fn publication_apply(
        &self,
        request: &NativePublicationRequest,
    ) -> Result<NativePublicationResult> {
        let binding = self.publication_binding(request).await?;
        let (journal, interactions) = self.publication_journal(binding, request).await?;
        let observed = self.publication_observe(request).await?;
        let original = observed
            .publication
            .as_ref()
            .ok_or_else(|| Error::Conflict("original publication has not been prepared".into()))?;
        let digest = publication_digest(binding, original)?;
        journal
            .verify(interaction_id(binding), binding.operation_id, digest)
            .await?;
        self.runtime
            .task_host()
            .verify_dispatch_owner(
                binding.caller,
                LeaseFence::from(&request.caller_lease.reservation),
            )
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
        let (from, to) = self.publication_generations(binding).await?;
        let excluded = binding
            .excluded_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let options = publication_options(binding);
        let state = acyclic_fs::LocalCoreStateStore::new(&binding.state_root);
        let owner = self
            .runtime
            .task_host()
            .journal_owner(binding.caller, &(LeaseFence::from(&request.caller_lease.reservation)), None)
            .await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = match acyclic_fs::NativeMaterializationWork::new(binding.budget, &cancellation) {
            Ok(work) => work,
            Err((error, work)) => return Ok(publication_failure(binding, error.to_string(), work)),
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
                        .verify_with_native_work(
                            interaction_id(binding),
                            binding.operation_id,
                            digest,
                            &work,
                        )?
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
        match acyclic_fs::publish_native_generation_transition(
            &from,
            &to,
            &state,
            publication(binding, &excluded, &options, &cancellation),
            work,
            before_effect,
        )
        .await
        {
            Ok(receipt) => {
                self.publication_result(binding, &interactions, Some(receipt.value), receipt.work)
                    .await
            }
            Err((error, work)) => Ok(publication_failure(binding, error.to_string(), work)),
        }
    }
}

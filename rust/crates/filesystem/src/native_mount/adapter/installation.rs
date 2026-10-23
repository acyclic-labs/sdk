impl<A: AsyncAuthorityStore, O: AsyncObjectStore> SharedCheckout<A, O> {
    async fn install_change(
        &self,
        candidate: CheckoutCandidate<'_, A, O>,
        change: ViewChange<'_>,
        work: &crate::NativeMaterializationWork,
        lifetime: &CancellationToken,
        guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    ) -> Result<(), MountSourceError> {
        let cancellation = work.cancellation();
        let rename = match &change {
            ViewChange::Moved { from, to, .. } => Some([*from, *to]),
            _ => None,
        };
        let publication = self
            .install_candidate(
                (candidate, rename),
                &mut NativeDemandCarrier::Native(work),
                cancellation,
                Some(lifetime),
                |carrier, _| self.ledger.record_deferred(&change, carrier),
                |checkout, candidate, prepared| {
                    cancellation
                        .commit_if_both_live(lifetime, || {
                            checkout.adopt_prepared_candidate(
                                &candidate.base,
                                candidate.checkout,
                                prepared,
                            )
                        })
                        .map_err(|_| MountSourceError::Cancelled)
                },
            )
            .await?
            .ok_or(MountSourceError::Stale)?;
        if publication == MountPublication::PerMutation {
            self.publish(
                crate::PublicationPermit::Unrestricted,
                crate::kernel::KernelAccounting::native(work, cancellation),
                Some(lifetime),
                &mut source_publication_guard(guard, Some(lifetime.clone())),
                None,
                &mut None,
            )
            .await
            .map_err(|(error, _)| error)?;
        }
        Ok(())
    }

    pub(super) async fn install_candidate<'a, N: FnOnce()>(
        &self,
        (candidate, rename): (CheckoutCandidate<'a, A, O>, Option<[&NamespacePath; 2]>),
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        record: impl FnOnce(
            &mut NativeDemandCarrier<'_>,
            ViewStamp,
        ) -> Result<(Result<(), MountSourceError>, N), MountSourceError>,
        commit: impl FnOnce(
            &mut Checkout<A, O>,
            CheckoutCandidate<'a, A, O>,
            crate::facade::PreparedCandidateAdoption,
        ) -> Result<bool, MountSourceError>,
    ) -> Result<Option<MountPublication>, MountSourceError> {
        carrier.check(cancellation, lifetime)?;
        if self.view_gate.generation() != candidate.epoch {
            return Ok(None);
        }
        let (recorded, notify) = record(carrier, candidate.stamp)?;
        // Record invalidation before adoption, then dispatch only after every lease drops.
        let result = async move {
            recorded?;
            let mut state = await_lock(self.lock(), carrier, cancellation, lifetime).await?;
            if self.view_gate.generation() != candidate.epoch
                || state.publication_operation.is_some()
                    && state.publication_operation != candidate.operation
            {
                return Ok(None);
            }
            let original = carrier
                .original_native_work()
                .ok_or(MountSourceError::NativeWork(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                ))?;
            let CheckoutCandidate {
                base,
                checkout,
                operation,
                epoch,
                stamp,
                deadline,
                _publication,
                _file_mutation,
            } = candidate;
            let Some((base, checkout)) = state
                .checkout
                .rebase_native_candidate(
                    base,
                    checkout,
                    rename,
                    MAXIMUM_EXACT_INSTALL_CHANGES,
                    &mut crate::kernel::allocation::KernelAccounting::native(
                        &original,
                        cancellation,
                    ),
                    cancellation,
                )
                .await?
            else {
                return Ok(None);
            };
            let candidate = CheckoutCandidate {
                base,
                checkout,
                operation,
                epoch,
                stamp,
                deadline,
                _publication,
                _file_mutation,
            };
            let prepared = state.checkout.prepare_candidate_adoption(
                &candidate.base,
                &candidate.checkout,
                &mut crate::kernel::allocation::KernelAccounting::native(&original, cancellation),
            )?;
            let Some(prepared) = prepared else {
                return Ok(None);
            };
            carrier.check(cancellation, lifetime)?;
            let operation = candidate.operation;
            let deadline = candidate.deadline;
            check_mutation_deadline(deadline, &self.clock)?;
            if !commit(&mut state.checkout, candidate, prepared)? {
                return Ok(None);
            }
            state.pending_deadline = earliest_deadline(state.pending_deadline, deadline);
            if let Some(operation) = operation {
                state.retain_operation_id(operation)?;
            }
            state.recorded_view = state.checkout.root().file_table;
            Ok(Some(state.publication))
        }
        .await;
        notify();
        result
    }
}

pub(in crate::native_mount) fn earliest_deadline(
    first: Option<u64>,
    second: Option<u64>,
) -> Option<u64> {
    match (first, second) {
        (Some(first), Some(second)) => Some(first.min(second)),
        (first, second) => first.or(second),
    }
}

fn check_mutation_deadline(
    deadline: Option<u64>,
    clock: &crate::native_mount::clock::Clock,
) -> Result<(), MountSourceError> {
    #[cfg(test)]
    if tests::return_fence::omitted_deadline() {
        return Ok(());
    }
    if let Some(deadline) = deadline
        && access::access_time(clock)? >= deadline
    {
        return Err(MountSourceError::AccessDenied);
    }
    Ok(())
}

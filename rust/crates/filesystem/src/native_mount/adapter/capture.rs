use super::*;

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> CheckoutMountSource<A, O> {
    pub(super) fn capture_host_with_identity(
        &self, source_root: &Path, paths: &[MountPath],
        identity: NativeRootIdentity, scope: HostCaptureScope,
    ) -> Result<(), MountSourceError> {
        if paths.is_empty() { return Ok(()); }
        let work = capture_meter(crate::performance::BOUNDARY_BUDGET, &self.cancellation)
            .map_err(|failure| failure.error)?;
        self.runtime.block_on(
            || tokio::time::timeout(CALLBACK_TIMEOUT,
                self.capture_host(source_root, paths, identity, scope, None, &work)),
            Some(work.runtime_meter()),
        ).map_err(|error| MountSourceError::NativeWork(work.runtime_error(error)))?
            .map_err(|_| MountSourceError::Stale)?
    }

    /// Captures host changes using the caller's work and baseline.
    pub(in crate::native_mount) async fn capture_host(
        &self, source_root: &Path, paths: &[MountPath], identity: NativeRootIdentity,
        scope: HostCaptureScope, baseline: Option<Arc<NativeViewBaseline>>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        work.native_read_meter().lock().check().map_err(super::super::lazy::mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        if paths.is_empty() { return Ok(()); }
        let inputs = self.capture_inputs(source_root, paths, identity, scope, work)?;
        self.capture_candidate(&inputs.0, &inputs.1, scope, baseline.as_deref(), work).await
    }

    async fn capture_candidate(
        &self, paths: &[NamespacePath], options: &CaptureOptions,
        scope: HostCaptureScope, baseline: Option<&NativeViewBaseline>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let cancellation = work.cancellation();
        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let mut credit = self.reserve_capture_poll()?;
        let _producer = {
            let mut gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            if gate.cancelled { return Err(MountSourceError::Stale); }
            self.accept_capture_poll(&mut credit)?;
            gate.producers = gate.producers.checked_add(1)
                .ok_or(MountSourceError::Work(WorkError::Overflow))?;
            gate.ever_used = true;
            CaptureProducer {
                gate: Arc::clone(&self.capture_gate), quiesced: Arc::clone(&self.capture_quiesced),
                original: None, _owner: None, _allocation: None,
            }
        };
        let mut view = super::super::view_ledger::ViewLedgerAdmission::default();
        for _ in 0..3 {
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut candidate = {
                let checkout = self.checkout.lock_with_carrier(
                    &mut carrier, cancellation, Some(&self.cancellation),
                ).await?;
                checkout.candidate_for_operation_with_carrier(None, &mut carrier, cancellation)?
            };
            match scope {
                HostCaptureScope::ExactPaths => capture_paths_batched(
                    &mut candidate.checkout, paths, options, 64, cancellation, baseline, work,
                ).await,
                HostCaptureScope::Subtree => capture_subtrees_with_native_work(
                    &mut candidate.checkout, std::slice::from_ref(paths.first().ok_or(MountSourceError::Stale)?),
                    options, &crate::CapturePolicy::default(), cancellation, baseline, work,
                ).await,
            }.map_err(|failure| capture_mount_error(failure.error))?;
            let scope = match scope {
                HostCaptureScope::ExactPaths => InstallScope::Paths(paths),
                HostCaptureScope::Subtree => InstallScope::Subtree(paths.first().ok_or(MountSourceError::Stale)?),
            };
            if self.install_capture(candidate, scope, &mut carrier, &mut view, cancellation).await? {
                return Ok(());
            }
        }
        Err(MountSourceError::Stale)
    }

    async fn install_capture(
        &self, candidate: CheckoutCandidate<A, O>, scope: InstallScope<'_>,
        carrier: &mut NativeDemandCarrier<'_>, view: &mut super::super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken,
    ) -> Result<bool, MountSourceError> {
        let (effect, _allocation) = installed_changes(
            &candidate.base, &candidate.checkout, scope, carrier, cancellation,
        ).await?;
        let (epoch, stamp) = {
            let checkout = self.checkout.lock_with_carrier(
                carrier, cancellation, Some(&self.cancellation),
            ).await?;
            checkout.ensure_publication_resolved()?;
            (self.checkout.view_gate.generation(), self.checkout.ledger.stamp())
        };
        let publication = self.checkout.install_candidate(
            candidate, epoch, stamp, carrier, cancellation, Some(&self.cancellation),
            |carrier| self.with_capture_observer_dispatch(||
                self.checkout.ledger.record(&ViewChange::Effect(&effect), carrier, view)),
            |checkout, candidate, prepared| {
                let gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
                if gate.cancelled { return Err(MountSourceError::Stale); }
                cancellation.commit_if_both_live(&self.cancellation, ||
                    checkout.adopt_prepared_candidate(&candidate.base, candidate.checkout, prepared))
                    .map_err(|_| MountSourceError::Cancelled)
            },
            None, |publication, _| publication,
        ).await?;
        let Some(publication) = publication else { return Ok(false); };
        if publication != MountPublication::Manual {
            self.checkout.seal_original_with_carrier_guarded(
                crate::PublicationPermit::Unrestricted, false, carrier, cancellation,
                Some(&self.cancellation), &mut self.publication_guard(),
            ).await?;
        }
        Ok(true)
    }
}

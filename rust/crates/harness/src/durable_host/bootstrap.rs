//! Bootstrap.
use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl RootBootstrapBindingWitness {
    pub(crate) fn binding(&self) -> &crate::store::VerifiedRootBinding {
        &self.proof.get().expect("issued canonical root binding").0
    }
}

#[cfg(feature = "filesystem")]
impl<P: StreamProvider> TaskJournalOwner<P> {
    pub(crate) fn authenticated_binding_agent(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<crate::AgentId>> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if !self.input_grants.contains(capability::CONVERSATION_BIND) {
            return Err(guarded_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "task scope lacks conversation:bind",
            ));
        }
        Ok(self.input_agent)
    }

    pub(crate) fn authenticated_fork_agent(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::AgentId> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
            io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
        }
        if !self.input_grants.contains(capability::FORK_PUBLISH) {
            return Err(guarded_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "task scope lacks fork:publish",
            ));
        }
        self.input_agent.ok_or_else(|| {
            guarded_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "task has no authenticated current agent",
            )
        })
    }

    pub(crate) fn binding_origin(&self) -> (TaskId, [u8; 32], &crate::scheduler::LeaseFence) {
        (self.task_id, self.workflow.request_digest, &self.fence)
    }

    /// Serializes root bind replay/commit/cache on this same owner handle only.
    pub(crate) async fn lock_root_binding(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        Some(self.root_binding.as_ref()?.gate.lock().await)
    }

    pub(crate) fn check_root_binding(
        &self,
        audience: &Authority,
        operation: OperationId,
        intent: [u8; 32],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if let Some(proof) = self
            .root_binding
            .as_ref()
            .and_then(|state| state.proof.get())
            .map(|retained| &retained.0)
        {
            if proof.audience() != audience
                || proof.bind_operation() != operation
                || proof.intent_digest() != intent
            {
                return Err(guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Conflict,
                    "root binding audience, operation or intent changed",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn pin_root_binding_choice(
        &self,
        operation: OperationId,
        intent: [u8; 32],
        audience_digest: [u8; 32],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        let Some(state) = &self.root_binding else {
            return Ok(());
        };
        let choice = (operation, intent, audience_digest);
        if let Err(choice) = state.pending.set(choice) {
            if state.pending.get() != Some(&choice) {
                return Err(guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Conflict,
                    "root binding publication choice changed",
                ));
            }
        }
        Ok(())
    }

    /// Borrows the canonical root-bind proof without exposing a scope or minting volume permission.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn authenticated_root_binding_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&crate::store::VerifiedRootBinding> {
        let proof = self
            .root_binding
            .as_ref()
            .and_then(|state| state.proof.get())
            .map(|retained| &retained.0)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "root creation lacks its canonical committed binding",
            })?;
        self.verify_root_binding_use_with_native_work(proof, false, work)
            .await?;
        Ok(proof)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn verify_root_binding_identity_with_native_work(
        &self,
        proof: &crate::store::VerifiedRootBinding,
        readonly: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.charge(acyclic_fs::WorkCounters::default())?;
        if self.root_binding.is_none() || (!readonly && self.is_readonly()) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "root creation requires its original live root owner",
            });
        }
        if proof.original_task() != self.task_id
            || proof.admission_digest() != self.original_admission_digest
            || proof.fence() != &self.fence
            || Some(proof.agent()) != self.input_agent
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "root binding differs from its original owner",
            });
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn verify_root_binding_use_with_native_work(
        &self,
        proof: &crate::store::VerifiedRootBinding,
        readonly: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.verify_root_binding_identity_with_native_work(proof, readonly, work)?;
        if readonly {
            self.verify_read_access(&mut Some(
                &mut crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)?,
            ))
            .await
        } else {
            self.verify(
                false,
                &mut Some(
                    &mut crate::filesystem::PreparationIo::from_native_work(work)
                        .map(crate::store::HistoryWork::new)?,
                ),
            )
            .await
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn root_bootstrap_binding_witness_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<RootBootstrapBindingWitness>> {
        self.authenticated_root_binding_with_native_work(work)
            .await?;
        Ok(Arc::clone(
            self.root_binding.as_ref().expect("verified root cache"),
        ))
    }

    /// Uses only the original committed binding and original READ witness; expired historical custody never becomes a fresh creator or writer.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn authenticated_root_binding_for_read_with_native_work<'a>(
        &'a self,
        witness: &'a RootBootstrapBindingWitness,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&'a crate::store::VerifiedRootBinding> {
        let proof = witness.binding();
        self.verify_root_binding_use_with_native_work(proof, true, work)
            .await?;
        Ok(proof)
    }

    pub(crate) fn root_bootstrap_owner(&self) -> &Authority {
        self.host.verifier.audience()
    }

    /// Authenticates the configured Task creator's existing MAC.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn authenticate_root_bootstrap_creator_with_native_work(
        &self,
        verifier: &AuthorityVerifier,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.charge(acyclic_fs::WorkCounters::default())?;
        if self.root_binding.is_none() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "bootstrap creator requires an original root admission",
            });
        }
        verifier.verify_audience(
            self.host.verifier.audience(),
            &mut Some(
                &mut crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)?,
            ),
        )?;
        verifier.verify(
            &self.host.owner_scope,
            &mut Some(
                &mut crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)?,
            ),
        )?;
        self.verify_read_access(&mut Some(
            &mut crate::filesystem::PreparationIo::from_native_work(work)
                .map(crate::store::HistoryWork::new)?,
        ))
        .await
    }

    /// Retains the existing coordinator-only publication guard, never this host or task context.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn root_bootstrap_publication_lease_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<dyn crate::resource_preparation::CreatedTaskLease>> {
        self.authenticated_root_binding_with_native_work(work)
            .await?;
        let mut io = crate::filesystem::PreparationIo::from_native_work(work)
            .map(crate::store::HistoryWork::new)?;
        crate::store::history::HistoryIo::retain_owned(
            &mut io,
            std::mem::size_of::<CreatedTaskPublicationLease<P>>() as u64,
        )?;
        let fence = self.fence.clone_with_work(&mut Some(&mut io))?;
        self.host.created_publication_lease_with_work(
            self.operation_id(),
            Some(fence),
            self.deadline_epoch_ms,
            &mut Some(&mut io),
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn original_root_admission_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskAdmissionRecord, acyclic_fs::NativeAllocation)> {
        if self.root_binding.is_none() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "bootstrap source requires an original root admission",
            });
        }
        self.verify_read_access(&mut Some(
            &mut crate::filesystem::PreparationIo::from_native_work(work)
                .map(crate::store::HistoryWork::new)?,
        ))
        .await?;
        self.host
            .observe_admission_with_native_work(self.task_id, work)
            .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn verify_root_bootstrap_context_with_native_work(
        &self,
        root: &TaskAdmissionRecord,
        binding: &crate::store::VerifiedRootBinding,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.verify_root_binding_identity_with_native_work(binding, true, work)?;
        if root.parent.is_some()
            || crate::runtime::task_id_for_admission(root.operation_id) != self.task_id
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "bootstrap admission differs from its actual root",
            });
        }
        let mut io = crate::filesystem::PreparationIo::from_native_work(work)
            .map(crate::store::HistoryWork::new)?;
        let digest = crate::contract::canonical_json_digest_with_work(
            root,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut Some(&mut io),
        )?;
        if digest != self.original_admission_digest {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "bootstrap admission differs from protected original workflow",
            });
        }
        self.verify_read_access(&mut Some(
            &mut crate::filesystem::PreparationIo::from_native_work(work)
                .map(crate::store::HistoryWork::new)?,
        ))
        .await
    }

    pub(crate) fn retain_root_binding(
        &self,
        proof: crate::store::VerifiedRootBinding,
        custody: Option<crate::runtime::NativeCustody>,
    ) -> Result<bool> {
        let retained = (proof, custody);
        let proof = &retained.0;
        #[cfg(not(target_arch = "wasm32"))]
        let native = retained.1.as_ref().map(|custody| custody.work().clone());
        #[cfg(not(target_arch = "wasm32"))]
        let mut native_io = native
            .as_ref()
            .map(|work| {
                crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)
            })
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut work = native_io
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(target_arch = "wasm32")]
        let mut work: Option<&mut dyn crate::store::history::HistoryIo> = None;
        let agent = self.authenticated_binding_agent(&mut work)?;
        if self.root_binding.is_none()
            || proof.original_task() != self.task_id
            || proof.admission_digest() != self.workflow.request_digest
            || proof.fence() != &self.fence
            || agent != Some(proof.agent())
        {
            return Err(guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "root binding proof belongs to another owner or lease",
            ));
        }
        let state = self.root_binding.as_ref().ok_or_else(|| {
            guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "owner has no root binding cache",
            )
        })?;
        if let Err(incoming) = state.proof.set(retained) {
            let proof = &incoming.0;
            let retained = &state
                .proof
                .get()
                .ok_or_else(|| {
                    guarded_boundary(
                        &work,
                        crate::wire::ErrorCode::Conflict,
                        "root binding cache is unavailable",
                    )
                })?
                .0;
            if retained.audience() != proof.audience()
                || retained.bind_operation() != proof.bind_operation()
                || retained.intent_digest() != proof.intent_digest()
            {
                return Err(guarded_boundary(
                    &work,
                    crate::wire::ErrorCode::Conflict,
                    "root binding audience, operation or intent changed",
                ));
            }
            return Ok(false);
        }
        Ok(true)
    }

    pub(crate) fn authenticate_fork_parent(
        &self,
        parent: &Authority,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.authenticated_fork_agent(work)?;
        let audience = match &self.input_created {
            Some(access) => access
                .current_content(work)?
                .and_then(|binding| binding.authority.as_ref()),
            None if self.root_binding.is_some() => self
                .root_binding
                .as_ref()
                .and_then(|state| state.proof.get())
                .map(|retained| retained.0.audience()),
            None => None,
        }
        .ok_or_else(|| {
            guarded_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "current caller has no authenticated publication audience",
            )
        })?;
        crate::core::inspect_metadata(&(audience, parent), work)?;
        if audience != parent {
            return Err(guarded_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "fork parent differs from the current caller audience",
            ));
        }
        Ok(())
    }

    pub(crate) fn authenticate_binding_scope(
        &self,
        audience: &Authority,
        scope: &Scope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let agent = self
            .authenticated_binding_agent(&mut None)?
            .ok_or_else(|| {
                Error::Unauthorized("binding owner has no authenticated agent".into())
            })?;
        if scope.agent() != Some(agent)
            || !scope.capabilities().contains(capability::CONVERSATION_BIND)
        {
            return Err(Error::Unauthorized(
                "binding scope differs from the original task actor or grant".into(),
            ));
        }
        if self.root_binding.is_none() {
            let binding = match &self.input_created {
                Some(access) => access.current_content(&mut None)?,
                None => None,
            };
            if binding.and_then(|binding| binding.authority.as_ref()) != Some(audience) {
                return Err(Error::Unauthorized(
                    "binding audience differs from the current sealed child".into(),
                ));
            }
        }
        self.host
            .verifier
            .verify_scope_for_with_work(audience, scope, work)
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn verify_original_root_bootstrap_source_with_native_work(
        &self,
        original: &(OperationSpec, &LeaseFence),
        root: &TaskAdmissionRecord,
        binding: &crate::store::VerifiedRootBinding,
        native: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut io = crate::filesystem::PreparationIo::from_native_work(native)
            .map(crate::store::HistoryWork::new)?;
        let mut observer = Some(&mut io as &mut dyn crate::store::history::HistoryIo);
        let declaration = &original.0;
        declaration.verify_owner(self.verifier.audience(), &self.verifier, &mut observer)?;
        if root.parent.is_some()
            || !root.resource_proofs.is_empty()
            || declaration.operation_id != root.operation_id
            || binding.original_task() != crate::runtime::task_id_for_admission(root.operation_id)
            || binding.fence() != original.1
            || declaration.owner_scope.agent() != Some(binding.agent())
            || self.owner_scope.agent() != Some(binding.agent())
        {
            return Err(guarded_boundary(
                &mut observer,
                crate::wire::ErrorCode::Unauthorized,
                "bootstrap binding differs from its canonical original root reservation",
            ));
        }
        let digest = crate::contract::canonical_json_digest_with_work(
            root,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut observer,
        )?;
        if digest != binding.admission_digest() {
            return Err(guarded_boundary(
                &mut observer,
                crate::wire::ErrorCode::Unauthorized,
                "bootstrap root input differs from its protected original workflow",
            ));
        }
        Ok(())
    }

    /// Restores only configured, originally allocated ROOT content.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) async fn verify_root_bootstrap_access_with_native_work(
        &self,
        root: &TaskAdmissionRecord,
        native: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<crate::resource_preparation::CreatedResourceAccess>> {
        use crate::resource_preparation::{CreatedResourceAccess, RootBootstrapContentContext};
        let mut access: Option<CreatedResourceAccess> = None;
        for provider in &self.resource_creation {
            for source in provider.root_bootstrap_sources(root.operation_id) {
                native.charge(acyclic_fs::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
                let Some((request, witness)) = source else {
                    continue;
                };
                let binding = witness.binding();
                let mut io = crate::filesystem::PreparationIo::from_native_work(native)
                    .map(crate::store::HistoryWork::new)?;
                let original = crate::distributed::original_reservation_proof(
                    &self.stream,
                    self.verifier.audience(),
                    &self.owner_scope,
                    &self.verifier,
                    root.operation_id,
                    binding.fence(),
                    &mut Some(&mut io),
                )
                .await?;
                let context =
                    RootBootstrapContentContext::from_verified_original_root_with_native_work(
                        self, &original, root, binding, native,
                    )
                    .await?;
                let received = provider
                    .verify_root_bootstrap_with_native_work(context, request, native)?
                    .await?;
                let next = CreatedResourceAccess::consume_root_bootstrap_with_native_work(
                    context, request, received.0, received.1, native,
                )?;
                access = Some(match access.take() {
                    Some(current) => current.merge_root_bootstrap_with_native_work(next, native)?,
                    None => next,
                });
                drop(original);
                drop(io);
            }
        }
        Ok(access)
    }

    pub(super) async fn resume_scope_with_work(
        &self,
        task: TaskId,
        operation: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(RuntimeScope, u64)> {
        if task.into_bytes() != operation.into_bytes() {
            return Err(guarded_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "task identity does not match its admission",
            ));
        }
        let admission = self.admission_with_work(operation, work).await?;
        let (scope, scope_bytes) = self.authenticated_scope_with_work(&admission, work).await?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let (scope, scope_bytes) = if admission.parent.is_none()
            && admission.resource_proofs.is_empty()
            && let Some(native) = work
                .as_deref()
                .and_then(|io| io.original_native_work())
                .cloned()
        {
            match self
                .verify_root_bootstrap_access_with_native_work(&admission, &native)
                .await?
            {
                Some(access) => {
                    let restored =
                        scope.with_created_access_with_work(&admission, Some(access), work)?;
                    drop(scope);
                    if let Some(io) = work.as_deref_mut() {
                        io.release_owned(scope_bytes)?;
                    }
                    restored
                }
                None => (scope, scope_bytes),
            }
        } else {
            (scope, scope_bytes)
        };
        let deadline_epoch_ms = admission.run_limits.deadline_epoch_ms;
        let has_created = !admission.resource_proofs.is_empty();
        let admission_bytes = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
            .checked_add(admission.logical_owned_bytes()?)
            .ok_or_else(|| {
                guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "resumed admission custody overflows",
                )
            })?;
        drop(admission);
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(admission_bytes)?;
        }
        if !has_created {
            return Ok((scope, scope_bytes));
        }
        if let Some(io) = work.as_deref_mut() {
            io.retain_owned(std::mem::size_of::<CreatedTaskPublicationLease<P>>() as u64)?;
        }
        let fence = {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(work).await?;
            let observed = coordinator.observe_operation(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                operation,
                work,
            )?;
            match observed.reservation.as_ref() {
                Some(reservation) => Some(crate::scheduler::LeaseFence {
                    reservation_id: match work.as_deref_mut() {
                        Some(io) => io.clone_text(&reservation.id)?,
                        None => reservation.id.clone(),
                    },
                    placement: match work.as_deref_mut() {
                        Some(io) => io.clone_text(&reservation.placement)?,
                        None => reservation.placement.clone(),
                    },
                }),
                None => None,
            }
        };
        let lease =
            self.created_publication_lease_with_work(operation, fence, deadline_epoch_ms, work)?;
        let scope = scope.with_created_publication_lease(lease, work)?;
        Ok((scope, scope_bytes))
    }
}

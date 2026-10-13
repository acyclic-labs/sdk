//! Authentication of the original committed ROOT binding.
use super::*;
use crate::store::guarded_boundary;

impl RootBootstrapBindingWitness {
    pub(crate) fn binding(&self) -> &crate::store::VerifiedRootBinding {
        &self.proof.get().expect("issued canonical root binding").0
    }
}
impl<P: StreamProvider> TaskJournalOwner<P> {
    fn current_agent(&self, work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<Option<crate::AgentId>> {
        match &self.created_access {
            Some(access) => access.current_content(work)?.map(|binding| match binding.volume.owner() {
                crate::conversation::VolumeOwner::Agent(agent) => Ok(*agent),
                _ => Err(guarded_boundary(work, crate::wire::ErrorCode::Unauthorized, "created scratch has no authenticated agent")),
            }).transpose(),
            None if self.root_binding.is_some() => Ok(self.host.owner_scope.agent()),
            None => Ok(None),
        }
    }
    pub(crate) fn authenticated_binding_agent(&self, work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<Option<crate::AgentId>> {
        crate::core::inspect_metadata(&self.admission.grants, work)?;
        if !self.admission.grants.contains(capability::CONVERSATION_BIND) {
            return Err(guarded_boundary(work, crate::wire::ErrorCode::Unauthorized, "task scope lacks conversation:bind"));
        }
        self.current_agent(work)
    }
    pub(crate) fn authenticated_fork_agent(&self, work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<crate::AgentId> {
        crate::core::inspect_metadata(&self.admission.grants, work)?;
        if !self.admission.grants.contains(capability::FORK_PUBLISH) {
            return Err(guarded_boundary(work, crate::wire::ErrorCode::Unauthorized, "task scope lacks fork:publish"));
        }
        self.current_agent(work)?.ok_or_else(|| guarded_boundary(work, crate::wire::ErrorCode::Unauthorized, "task has no authenticated current agent"))
    }
    pub(crate) fn binding_origin(&self, work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<(TaskId, [u8; 32], &crate::scheduler::LeaseFence)> {
        Ok((self.task_id(), self.admission.canonical_digest_with_work(work)?, &self.fence))
    }
    pub(crate) async fn lock_root_binding(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        Some(self.root_binding.as_ref()?.gate.lock().await)
    }
    pub(crate) fn check_root_binding(&self, audience: &Authority, operation: OperationId, intent: [u8; 32], work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<()> {
        crate::core::inspect_metadata(audience, work)?;
        if let Some(proof) = self.root_binding.as_ref().and_then(|state| state.proof.get()).map(|pair| &pair.0) {
            if proof.audience() != audience || proof.bind_operation() != operation || proof.intent_digest() != intent {
                return Err(guarded_boundary(work, crate::wire::ErrorCode::Conflict, "root binding audience, operation or intent changed"));
            }
        }
        Ok(())
    }
    pub(crate) fn pin_root_binding_choice(&self, operation: OperationId, intent: [u8; 32], audience: [u8; 32], work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if let Some(state) = &self.root_binding {
            let choice = (operation, intent, audience);
            if state.pending.set(choice).is_err() && state.pending.get() != Some(&choice) {
                return Err(guarded_boundary(work, crate::wire::ErrorCode::Conflict, "root publication choice changed"));
            }
        }
        Ok(())
    }
    pub(crate) fn retain_root_binding(&self, proof: crate::store::VerifiedRootBinding, custody: crate::NativeCustody) -> Result<bool> {
        #[cfg(not(target_arch = "wasm32"))]
        let io = custody.as_ref().map(|custody| crate::filesystem::PreparationIo::from_native_work(custody.work())).transpose()?;
        #[cfg(target_arch = "wasm32")]
        let io = None;
        crate::executor::with_history_io(io.as_ref(), |work| {
            if proof.original_task() != self.task_id() || proof.admission_digest() != self.admission.canonical_digest_with_work(work)?
                || proof.fence() != &self.fence || self.authenticated_binding_agent(work)? != Some(proof.agent()) {
                return Err(guarded_boundary(work, crate::wire::ErrorCode::Unauthorized, "root proof belongs to another original owner or lease"));
            }
            let state = self.root_binding.as_ref().ok_or_else(|| guarded_boundary(work, crate::wire::ErrorCode::Unauthorized, "owner has no root binding cache"))?;
            match state.proof.set((proof, custody)) {
                Ok(()) => Ok(true),
                Err((proof, _)) => { self.check_root_binding(proof.audience(), proof.bind_operation(), proof.intent_digest(), work)?; Ok(false) }
            }
        })
    }
    pub(crate) fn authenticate_fork_parent(&self, parent: &Authority, work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<()> {
        self.authenticated_fork_agent(work)?;
        let audience = match &self.created_access {
            Some(access) => access.current_content(work)?.and_then(|binding| binding.authority.as_ref()),
            None => self.root_binding.as_ref().and_then(|state| state.proof.get()).map(|pair| pair.0.audience()),
        };
        crate::core::inspect_metadata(parent, work)?;
        if audience != Some(parent) { return Err(guarded_boundary(work, crate::wire::ErrorCode::Unauthorized, "fork parent differs from current authenticated audience")); }
        Ok(())
    }
    pub(crate) fn authenticate_binding_scope(&self, audience: &Authority, scope: &Scope, work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<()> {
        let agent = self.authenticated_binding_agent(work)?;
        if agent.is_none() || scope.agent() != agent || !scope.capabilities().contains(capability::CONVERSATION_BIND) {
            return Err(guarded_boundary(work, crate::wire::ErrorCode::Unauthorized, "binding scope differs from original actor or grant"));
        }
        if self.root_binding.is_none() && self.created_access.as_ref().map(|access| access.current_content(work)).transpose()?.flatten()
            .and_then(|binding| binding.authority.as_ref()) != Some(audience) {
            return Err(guarded_boundary(work, crate::wire::ErrorCode::Unauthorized, "binding audience differs from sealed child"));
        }
        self.host.verifier.verify_scope_for(audience, scope, work)
    }
    pub(crate) fn root_bootstrap_owner(&self) -> &Authority { self.host.verifier.audience() }
}

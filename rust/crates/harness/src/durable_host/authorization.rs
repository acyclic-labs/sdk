//! Authenticated actor and retained ROOT publication.
use super::*;
use crate::store::HistoryIo;

impl<P: StreamProvider> TaskJournalOwner<P> {
    fn root(&self) -> bool {
        self.admission.parent.is_none() && self.admission.resource_proofs.is_empty()
    }

    fn actor(
        &self,
        grant: &str,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<crate::AgentId>> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if !self.admission.grants.contains(grant) {
            return Err(Error::Unauthorized("task lacks publication grant".into()));
        }
        Ok(self.input_agent)
    }

    pub(crate) fn authenticated_binding_agent(
        &self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<crate::AgentId>> {
        self.actor(capability::CONVERSATION_BIND, work)
    }

    pub(crate) fn authenticated_fork_agent(
        &self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<crate::AgentId> {
        self.actor(capability::FORK_PUBLISH, work)?
            .ok_or_else(|| Error::Unauthorized("task lacks authenticated actor".into()))
    }

    pub(crate) fn authenticate_fork_parent(
        &self,
        parent: &Authority,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.authenticated_fork_agent(work)?;
        if !self.root() {
            let audience = self
                .created
                .as_ref()
                .map(|access| access.current_content(work))
                .transpose()?
                .flatten()
                .and_then(|binding| binding.authority.as_ref())
                .ok_or_else(|| {
                    Error::Unauthorized("created task lacks its authenticated audience".into())
                })?;
            crate::core::inspect_metadata(&(audience, parent), work)?;
            if audience != parent {
                return Err(Error::Unauthorized(
                    "fork parent differs from created audience".into(),
                ));
            }
            return Ok(());
        }
        let binding = self.root_binding.get().ok_or_else(|| {
            Error::Unauthorized("task lacks committed publication audience".into())
        })?;
        crate::core::inspect_metadata(&(binding.0.audience(), parent), work)?;
        if binding.0.audience() != parent {
            return Err(Error::Unauthorized(
                "fork parent differs from authenticated audience".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn authenticate_binding_scope(
        &self,
        audience: &Authority,
        scope: &Scope,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if !self.root()
            || scope.agent() != self.authenticated_binding_agent(work)?
            || scope.agent().is_none()
            || !scope.capabilities().contains(capability::CONVERSATION_BIND)
        {
            return Err(Error::Unauthorized(
                "binding scope differs from original ROOT actor".into(),
            ));
        }
        self.host
            .verifier
            .verify_scope_for_with_work(audience, scope, work)
    }

    pub(crate) fn binding_origin(&self) -> (TaskId, [u8; 32], &crate::scheduler::LeaseFence) {
        (self.task_id(), self.workflow.request_digest, &self.fence)
    }

    pub(crate) async fn lock_root_binding(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        if !self.root() {
            return None;
        }
        Some(self.root_gate.lock().await)
    }

    pub(crate) fn check_root_binding(
        &self,
        audience: &Authority,
        operation: OperationId,
        intent: [u8; 32],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if let Some(retained) = self.root_binding.get() {
            let proof = &retained.0;
            if proof.audience() != audience
                || proof.bind_operation() != operation
                || proof.intent_digest() != intent
            {
                return Err(Error::Conflict("ROOT binding choice changed".into()));
            }
        }
        Ok(())
    }

    pub(crate) fn pin_root_binding_choice(
        &self,
        operation: OperationId,
        intent: [u8; 32],
        audience: [u8; 32],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if !self.root() {
            return Err(Error::Unauthorized(
                "child cannot publish ROOT binding".into(),
            ));
        }
        let choice = (operation, intent, audience);
        if self.root_choice.get_or_init(|| choice) != &choice {
            return Err(Error::Conflict("ROOT publication choice changed".into()));
        }
        Ok(())
    }

    pub(crate) fn retain_root_binding(
        &self,
        proof: crate::store::VerifiedRootBinding,
        custody: Option<crate::runtime::NativeCustody>,
    ) -> Result<bool> {
        if !self.root()
            || proof.original_task() != self.task_id()
            || proof.admission_digest() != self.workflow.request_digest
            || proof.fence() != &self.fence
            || Some(proof.agent()) != self.authenticated_binding_agent(&mut None)?
        {
            return Err(Error::Unauthorized(
                "ROOT proof differs from original owner".into(),
            ));
        }
        if self.root_choice.get().is_none_or(|choice| {
            choice.0 != proof.bind_operation() || choice.1 != proof.intent_digest()
        }) {
            return Err(Error::Conflict(
                "ROOT proof differs from pinned publication".into(),
            ));
        }
        self.check_root_binding(
            proof.audience(),
            proof.bind_operation(),
            proof.intent_digest(),
            &mut None,
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut custody = custody;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(held) = &mut custody {
            if !self.native_shared_work_matches(held.work()) {
                return Err(Error::Unauthorized(
                    "ROOT proof uses another allocation context".into(),
                ));
            }
            let mut allocation = held.work().claim(
                (std::mem::size_of::<(
                    crate::store::VerifiedRootBinding,
                    Option<crate::runtime::NativeCustody>,
                )>() + 2 * std::mem::size_of::<usize>()) as u64,
                1,
                acyclic_fs::WorkCounters::default(),
            )?;
            held.absorb(&mut allocation)?;
        }
        match self.root_binding.set(Arc::new((proof, custody))) {
            Ok(()) => Ok(true),
            Err(retained) => {
                let proof = &retained.0;
                self.check_root_binding(
                    proof.audience(),
                    proof.bind_operation(),
                    proof.intent_digest(),
                    &mut None,
                )?;
                Ok(false)
            }
        }
    }

    pub(crate) fn invocation_cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        self.invocation_cancellation.as_ref()
    }

    /// Binds the original owner's live invocation cancellation.
    pub fn with_invocation_cancellation(
        mut self,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Self {
        self.invocation_cancellation = Some(cancellation.clone());
        self
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native_shared_work_matches(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> bool {
        self.native_custody
            .as_ref()
            .is_some_and(|held| held.work().shares_allocation_context(work))
    }

    /// Shares the original paid owner without copying admission metadata.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(
        mut self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        let custody = self
            .native_custody
            .as_mut()
            .filter(|held| held.work().shares_allocation_context(work))
            .ok_or_else(|| Error::Unauthorized("owner lacks original metadata custody".into()))?;
        let mut header = work.claim(
            (2 * std::mem::size_of::<usize>()) as u64,
            1,
            acyclic_fs::WorkCounters::default(),
        )?;
        custody.absorb(&mut header)?;
        Ok(Arc::new(self))
    }
}

impl<P: StreamProvider> crate::resource_preparation::CreatedTaskLease for TaskJournalOwner<P> {
    fn fence(&self) -> Result<crate::scheduler::LeaseFence> {
        Ok(self.fence.clone())
    }
    fn verify<'a>(
        &'a self,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        #[cfg(not(target_arch = "wasm32"))]
        if io
            .and_then(|io| io.original_native_work())
            .is_some_and(|work| !self.native_shared_work_matches(work))
        {
            return Err(Error::Unauthorized(
                "publication lease uses another ledger".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            match io {
                Some(io) => self.verify(false, &mut Some(&mut io.history_io())).await,
                None => self.verify(false, &mut None).await,
            }
        })
    }
}

#[cfg(test)]
#[path = "tests/root_bootstrap.rs"]
mod test_helpers;

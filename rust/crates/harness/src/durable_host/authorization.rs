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
        crate::core::inspect_metadata(&self.admission.grants, work)?;
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
        mut custody: Option<crate::runtime::NativeCustody>,
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
        self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !self.native_shared_work_matches(work) {
            return Err(Error::Unauthorized(
                "owner lacks original metadata custody".into(),
            ));
        }
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 1,
            ..Default::default()
        })?;
        Ok(Arc::new(self))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn authenticated_root_binding_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        Arc<(
            crate::store::VerifiedRootBinding,
            Option<crate::runtime::NativeCustody>,
        )>,
    > {
        if !self.native_shared_work_matches(work) {
            return Err(Error::Unauthorized("ROOT owner uses another ledger".into()));
        }
        self.verify(
            false,
            &mut Some(&mut crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(work)?,
            )),
        )
        .await?;
        let proof = self
            .root_binding
            .get()
            .ok_or_else(|| Error::Unauthorized("ROOT lacks committed binding".into()))?;
        Ok(Arc::clone(proof))
    }
    pub(crate) fn root_bootstrap_owner(&self) -> &Authority {
        &self.host.owner
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn authenticate_root_bootstrap_creator_with_native_work(
        &self,
        verifier: &AuthorityVerifier,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        if !self.root() {
            return Err(Error::Unauthorized(
                "creator requires original ROOT admission".into(),
            ));
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let mut history = io.history_io();
        verifier.verify_audience(&self.host.owner, &mut Some(&mut history))?;
        verifier.verify(&self.host.owner_scope, &mut Some(&mut history))?;
        self.verify(true, &mut Some(&mut history)).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn original_root_admission_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskAdmissionRecord, acyclic_fs::NativeAllocation)> {
        if !self.root() {
            return Err(Error::Unauthorized(
                "source requires original ROOT admission".into(),
            ));
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        self.verify(true, &mut Some(&mut io.history_io())).await?;
        let (record, custody) = self
            .host
            .readonly_admission(self.operation_id(), &io)
            .await?;
        if record != *self.admission {
            return Err(Error::Conflict("ROOT admission changed".into()));
        }
        Ok((record, custody))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn root_bootstrap_context<'a>(
        &'a self,
        root: &'a TaskAdmissionRecord,
        binding: &'a crate::store::VerifiedRootBinding,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::resource_preparation::RootBootstrapContentContext<'a>> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let mut history = io.history_io();
        let digest = crate::contract::canonical_json_digest(
            root,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut Some(&mut history),
        )?;
        if !self.root()
            || root != self.admission.as_ref()
            || binding.original_task() != self.task_id()
            || binding.admission_digest() != digest
            || digest != self.workflow.request_digest
            || binding.fence() != &self.fence
            || Some(binding.agent()) != self.input_agent
        {
            return Err(Error::Unauthorized(
                "bootstrap differs from committed ROOT origin".into(),
            ));
        }
        self.check_root_binding(
            binding.audience(),
            binding.bind_operation(),
            binding.intent_digest(),
            &mut Some(&mut history),
        )?;
        if self.root_binding.get().is_none() {
            return Err(Error::Unauthorized("ROOT binding is not retained".into()));
        }
        self.verify(true, &mut Some(&mut history)).await?;
        Ok(
            crate::resource_preparation::RootBootstrapContentContext::authenticated(
                root,
                &self.host.owner,
                binding,
                digest,
            ),
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn root_bootstrap_publication_lease_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<dyn crate::resource_preparation::CreatedTaskLease>> {
        self.authenticated_root_binding_with_native_work(work)
            .await?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        self.host
            .journal_owner(self.task_id(), &self.fence, Some(&io))
            .await?
            .into_native_shared(work)
            .map(|owner| owner as Arc<dyn crate::resource_preparation::CreatedTaskLease>)
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

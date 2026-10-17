use super::*;

impl<P: StreamProvider> TaskJournalOwner<P> {
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
    #[cfg(not(target_arch = "wasm32"))]
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
        let record = self
            .host
            .admission(self.operation_id(), &mut Some(&mut io.history_io()))
            .await?;
        let custody = io.take_native_custody(
            std::mem::size_of::<TaskAdmissionRecord>() as u64 + record.logical_owned_bytes()?,
        )?;
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
        let digest = crate::executor::json::canonical_json_digest(
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

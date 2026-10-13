//! Original task context and historical read authorization.
use super::*;
use crate::store::HistoryIo;

impl<P: StreamProvider> TaskJournalOwner<P> {
    pub(crate) fn original_host_binding_matches(&self, host: &CoordinatorTaskHost<P>) -> bool {
        std::ptr::eq(self.host.as_ref(), host)
    }

    pub(crate) fn require_task_context_binding(
        &self,
        context: &crate::runtime::TaskContext,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(|io| io.original_native_work()) {
            context.require_original_native_work(work)?;
            if !self.native_shared_work_matches(work) || !context.native_metadata_matches(work) {
                return Err(Error::Unauthorized(
                    "context differs from original owner ledger".into(),
                ));
            }
        }
        crate::executor::with_history_io(io, |work| {
            crate::core::inspect_metadata(
                &(
                    context.scope().grants(),
                    &self.admission.grants,
                    context.scope().resource_proofs(),
                    &self.admission.resource_proofs,
                    context.scope().extensions(),
                    &self.admission.extensions,
                ),
                work,
            )
        })?;
        if !context.original_task_binding_matches(self.task_id(), &self.fence)
            || context.id() != self.operation_id()
            || !context
                .original_host_binding_matches(self.host.as_ref(), self.host.policy.as_deref())
            || context.scope().grants() != &self.admission.grants
            || context.scope().limits() != self.admission.limits
            || context.scope().run_limits() != self.admission.run_limits
            || context.scope().resource_proofs() != self.admission.resource_proofs
            || context.scope().extensions() != self.admission.extensions.as_ref()
        {
            return Err(Error::Unauthorized(
                "context differs from original owner admission".into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn verify_read_access(
        &self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if work.is_none() {
            return self.verify(true, work).await;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if work
            .as_deref()
            .and_then(|io| io.original_native_work())
            .is_some_and(|work| !self.native_shared_work_matches(work))
        {
            return Err(Error::Unauthorized("read owner uses another ledger".into()));
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let (declaration, _) = crate::distributed::original_reservation_proof(
            &self.host.stream,
            &self.host.owner,
            &self.host.owner_scope,
            &self.host.verifier,
            self.operation_id(),
            &self.fence,
            quota.work(),
        )
        .await?;
        quota.adopt(declaration.logical_owned_bytes()?)?;
        if declaration.owner_scope.agent() != self.input_agent && self.admission.parent.is_none() {
            return Err(Error::Unauthorized(
                "historical root actor differs from original declaration".into(),
            ));
        }
        drop(declaration);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn require_native_workflow_validation(&self) -> Result<()> {
        self.host
            .machines
            .require_native_work(&self.workflow.initial.machine)
    }

    pub(crate) fn require_mail_scope(
        &self,
        scope: &RuntimeScope,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if io
            .and_then(|io| io.original_native_work())
            .is_some_and(|work| !self.native_shared_work_matches(work))
        {
            return Err(Error::Unauthorized("mail scope uses another ledger".into()));
        }
        crate::executor::with_history_io(io, |work| {
            crate::core::inspect_metadata(
                &(
                    scope.grants(),
                    &self.admission.grants,
                    scope.resource_proofs(),
                    &self.admission.resource_proofs,
                    scope.extensions(),
                    &self.admission.extensions,
                ),
                work,
            )
        })?;
        if scope.grants() != &self.admission.grants
            || scope.limits() != self.admission.limits
            || scope.run_limits() != self.admission.run_limits
            || scope.resource_proofs() != self.admission.resource_proofs
            || scope.extensions() != self.admission.extensions.as_ref()
        {
            return Err(Error::Unauthorized(
                "mail scope differs from original admission".into(),
            ));
        }
        Ok(())
    }
}

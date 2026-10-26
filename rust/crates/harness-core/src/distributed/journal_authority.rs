//! Immutable authentication for one declaration in one coordinator.
use super::*;
use crate::core::VerifiedScope;

pub(crate) struct JournalAuthority<P> {
    client: Arc<P>,
    path: String,
    operation: OperationId,
    observer: VerifiedScope,
    declarer: VerifiedScope,
    _custody: Option<crate::filesystem::PreparationCustody>,
}

impl<P: StreamProvider> DistributedCoordinator<P> {
    pub(crate) fn authenticate_journal(
        &self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<JournalAuthority<P>> {
        let declaration = self.observe_operation(owner, scope, verifier, operation, work)?;
        let mut quota = CodecQuota::new(work, 0)?;
        let observer = verifier.authenticate_scope(scope, quota.work())?;
        quota.adopt(observer.logical_owned_bytes()?)?;
        let declarer = verifier.authenticate_scope(&declaration.spec.owner_scope, quota.work())?;
        quota.adopt(declarer.logical_owned_bytes()?)?;
        let path = crate::core::copy_text(self.stream.0.path().as_str(), &mut quota)?;
        let custody = quota.take_owned_custody()?;
        quota.transfer();
        Ok(JournalAuthority {
            client: self.client.clone(),
            path,
            operation,
            observer,
            declarer,
            _custody: custody,
        })
    }
}

impl<P: StreamProvider> JournalAuthority<P> {
    pub(crate) fn declared_agent(&self) -> Option<crate::AgentId> {
        self.declarer.scope().agent()
    }

    pub(super) const fn operation(&self) -> OperationId {
        self.operation
    }

    pub(super) fn recheck(
        &self,
        coordinator: &DistributedCoordinator<P>,
        operation: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        // The declaration cannot be replaced: Scheduler rejects a second
        // declaration of the same operation. Private proofs own their scopes,
        // so callers cannot mutate wire bytes after authentication.
        if !Arc::ptr_eq(&self.client, &coordinator.client)
            || self.path != coordinator.stream.0.path().as_str()
            || self.operation != operation
        {
            return Err(guarded_boundary(
                work,
                wire::ErrorCode::Unauthorized,
                "journal authentication belongs to another coordinator or operation",
            ));
        }
        self.observer.recheck(work)?;
        self.declarer.recheck(work)
    }
}

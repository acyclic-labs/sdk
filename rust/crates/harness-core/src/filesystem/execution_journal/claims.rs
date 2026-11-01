//! Retain the original provider lifetime without reacquiring an execution.
use super::*;
use std::borrow::Borrow;

struct ClaimOwner<A: 'static, O: 'static>(Arc<FilesystemHost<A, O>>);

impl<A, O> Borrow<acyclic_fs::OperationClaims> for ClaimOwner<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn borrow(&self) -> &acyclic_fs::OperationClaims {
        self.0.filesystem.operation_claims()
    }
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Moves an already acquired claim into its original filesystem owner.
    /// The retained guard borrows no journal, turn or preparation observer.
    /// It retains exclusion only; original journal and operation admission
    /// still govern replay, request construction and every publication.
    ///
    /// A claim from another provider refuses before handoff. No second claim,
    /// allocation, read or event is created, and the refused guard retires as
    /// `ProducerDropped` through its own original registry.
    pub fn retain_execution_claim(
        &self,
        claim: crate::executor::ExecutionClaim<'_>,
        io: Option<&PreparationIo>,
    ) -> Result<
        crate::executor::RetainedExecutionClaim<
            impl Borrow<acyclic_fs::OperationClaims>
            + acyclic_stream::ProviderPlatform
            + 'static
            + use<P, A, O>,
        >,
    > {
        claim
            .retain(ClaimOwner(Arc::clone(&self.host)))
            .map_err(|(_original, _proposed)| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "execution claim belongs to another provider owner",
                )
            })
    }
}

//! Exact-head publication preparation shared by composed transactions.
use super::*;

/// Keeps closure authentication, flush admission and projection fencing alive
/// through the caller's single authoritative commit decision.
pub(crate) struct PreparedPublication(preparation::PreparedGeneration);

impl PreparedPublication {
    pub(crate) fn append(&self) -> &GuardedAppend {
        &self.0.append
    }

    pub(crate) fn recorded(&mut self) {
        if let Some(proof) = self.0.authentication.pending.take() {
            proof.created();
        }
    }
}

pub(crate) async fn prepare_publication<P: PublicationOwner>(
    owner: &P,
    request: PublishGenerationRequest,
    permit: PublicationPermit,
    limits: ClosureLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<PreparedPublication, crate::FsError> {
    let prepared = preparation::Preparation {
        owner,
        request,
        intent: PublicationIntent::guarded(None, permit),
        limits,
        cache: None,
        cancellation,
        resolved: None,
    }
    .prepare(accounting, &mut |_, _, _| Ok::<_, crate::FsError>(()))
    .await?;
    match prepared {
        preparation::Prepared::Ready(prepared) => Ok(PreparedPublication(prepared)),
        preparation::Prepared::Complete(_) => Err(crate::FsError::Authority(
            AuthorityStoreError::RejectedStatic("composed publication unexpectedly resolved"),
        )),
    }
}

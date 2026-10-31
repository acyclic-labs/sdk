use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// The agent approvals that own this journal's interactions.
    pub(super) fn approvals(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<&crate::approvals::AgentApprovals> {
        self.agent_approvals.as_deref().ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "execution has no interaction owner",
            )
        })
    }

    pub(super) async fn open_interaction_with_io(
        &self,
        id: InteractionId,
        interaction: &Interaction,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        crate::approvals::until_stopped(io, self.approvals(io)?.open_with_io(id, interaction, io))
            .await
    }

    pub(super) async fn interaction_outcome_with_io(
        &self,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<InteractionOutcome>> {
        crate::approvals::until_stopped(io, self.approvals(io)?.dispatch_with_io(id, io)).await
    }
}

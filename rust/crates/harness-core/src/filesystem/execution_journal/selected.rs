use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn verify_selected_context_with_io(
        &self,
        operation_id: OperationId,
        selected: &SelectedModelContext,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        tracing::trace!(target: "acyclic_harness::read_profile", phase = "selected:begin");
        self.require_operation(operation_id, io)?;
        if let Some(bound) = &self.bound_selection {
            if bound == selected {
                return Ok(());
            }
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "selection differs from the agent conversation",
            ));
        }
        Err(canonical_boundary(
            io,
            crate::wire::ErrorCode::Conflict,
            "turn has no committed model-context selection",
        ))
    }
}

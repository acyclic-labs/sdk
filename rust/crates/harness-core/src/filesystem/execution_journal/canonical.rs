use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Admits a compaction publication. Its projection must stand alone: a
    /// canonical checkpoint names committed conversation history that agent
    /// streams do not keep, so no journal can prove its source.
    pub(super) async fn verify_canonical_publication_with_io(
        &self,
        event: &ExecutionEvent,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        let ExecutionEvent::ContextCompacted { projection, .. } = event else {
            return Ok(());
        };
        crate::executor::with_history_io(io, |work| limits.validate_file(projection, work))?;
        let projection =
            crate::executor::load_response_projection(self, projection, limits, io).await?;
        crate::context::validate_projected_context(&projection.0.context, limits, io)?;
        if projection.0.checkpoint.is_some() || projection.0.canonical.is_some() {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "checkpoint source selection is not the committed admission",
            ));
        }
        Ok(())
    }
}

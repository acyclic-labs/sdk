use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Resolves the first publication of a complete generation within this
    /// view's observed prefix. Cold lookup uses the backend's original index;
    /// neither a later identical root nor a current head replaces that position.
    pub async fn generation_position<W: FsWork>(
        &self,
        generation: GenerationId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(Sequence, Sequence), FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .generation_position_accounted(generation, &mut accounting, cancellation)
            .await;
        work.finish(result, &accounting)
    }

    pub(super) async fn generation_position_accounted(
        &self,
        generation: GenerationId,
        accounting: &mut KernelAccounting<'_>,
        _cancellation: &CancellationToken,
    ) -> Result<(Sequence, Sequence), FsError> {
        accounting.check()?;
        let head = self
            .authority_head
            .ok_or(FsError::InvalidAuthorityHistory)?
            .sequence;
        // A first-record receipt proves first occurrence without a provider
        // operation. A later head or commit receipt proves only its own offset.
        if self.observed_generation.position(generation) == Some(Sequence::new(1)) {
            return Ok((Sequence::new(1), head));
        }
        let mut authority = crate::async_storage::AuthorityFrame::borrow(accounting)?;
        let result = self
            .volume
            .fs
            .inner
            .authority
            .first_generation(
                volume_authority_id(self.volume.id),
                generation,
                head,
                &mut authority,
            )
            .await;
        finish_authority_work(authority)?;
        let position = result?.ok_or(FsError::NotFound)?;
        if position == Sequence::GENESIS || position > head {
            return Err(FsError::InvalidAuthorityHistory);
        }
        Ok((position, head))
    }
}

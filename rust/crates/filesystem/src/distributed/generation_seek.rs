use super::*;

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    pub(super) async fn seek_generation(
        &self,
        authority: AuthorityId,
        generation: GenerationId,
        through: Sequence,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<Sequence>, AuthorityStoreError> {
        work.check()?;
        let before = through
            .get()
            .checked_add(1)
            .ok_or(crate::WorkError::Overflow)?;
        self.authority_require_live(authority, work).await?;
        let path = work.path(&self.placement, authority, "/records", None)?;
        let retained_heads = self
            .authority_heads(
                acyclic_stream::HeadProjection::Tail,
                std::slice::from_ref(&path),
                work,
            )
            .await?;
        let heads = &retained_heads.0;
        let tail = heads
            .heads
            .first()
            .and_then(Option::as_ref)
            .ok_or(AuthorityStoreError::Missing)?
            .tail;
        if tail < before {
            return Err(authority_corrupt(
                "generation lookup prefix is not retained",
                work,
            ));
        }
        // The cut itself is request storage. Admit it through the same original
        // frame before constructing the finite sparse read.
        let mut revisions = work.vec(1)?;
        revisions.push(heads.snapshot.clone());
        let at_least = acyclic_stream::RevisionCut::new(revisions).map_err(map_stream_error)?;
        let reference = generation.to_bytes();
        let key = work.stream_bytes(GENERATION_INDEX_DOMAIN, &[reference.as_slice()])?;
        if !work.is_native() {
            work.charge(WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::UNCHARGED
            })?;
        }
        let mut stream = self
            .authority_read(
                acyclic_stream::ReadRequest {
                    key: Some(acyclic_stream::ReadKeySeek {
                        key: key.clone(),
                        before,
                    }),
                    path: path.clone(),
                    from: 1,
                    limit: 1,
                    at_least,
                },
                work,
            )
            .await?;
        let Some(retained_record) = stream.next(work).await? else {
            work.check()?;
            return Ok(None);
        };
        let record = &retained_record.0;
        if record.origin != path
            || record
                .headers
                .keys()
                .binary_search_by(|indexed| indexed.as_ref().cmp(key.as_ref()))
                .is_err()
        {
            return Err(authority_corrupt(
                "generation index changed its original path or key",
                work,
            ));
        }
        let durable = authority_decode_borrowed(authority, &record.value, work)?;
        if record.sequence == 0
            || record.sequence >= before
            || durable.sequence.get() != record.sequence
            || generation_from_payload(authority, durable.sequence, durable.payload, work)?
                != Some(generation)
        {
            return Err(authority_corrupt(
                "generation index does not bind its authority record",
                work,
            ));
        }
        work.check()?;
        Ok(Some(durable.sequence))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

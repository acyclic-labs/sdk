use super::*;

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    pub(super) async fn head_payload_checked<const RECHECK: bool>(
        &self,
        authority: AuthorityId,
        maximum_payload_bytes: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<((Head, Option<Bytes>), AuthorityAllocation), AuthorityStoreError> {
        let mut scope = work.scope()?;
        let work = &mut *scope;
        async {
            let (snapshot, latest) = self
                .fork_snapshot_payload::<true>(authority, work)
                .await
                .and_then(|value| work.finish(value))?;
            let Some((storage, allocation, offset)) = latest else {
                return Ok(((snapshot.head, None), None));
            };
            // Bind storage after funding so errors also destroy it first.
            let record = storage;
            if maximum_payload_bytes == 0 {
                return Err(AuthorityStoreError::InvalidReplayLimit);
            }
            work.check()?;
            // Preserve the replay boundary's retirement and admission checks
            // even though its authenticated record no longer needs another read.
            if RECHECK || !cfg!(all(test, loom)) {
                self.authority_require_live(authority, work).await?;
            }
            work.check()?;
            if snapshot.head.sequence.get() != record.sequence {
                return Err(authority_corrupt(
                    "Stream sequence does not match filesystem authority sequence",
                    work,
                ));
            }
            #[cfg(all(test, loom, loom))]
            let replacement = if super::head_payload_loom_tests::resample_payload() {
                Some(
                    Box::pin(super::head_payload_loom_tests::resampled_payload(
                        self, authority, work,
                    ))
                    .await?,
                )
            } else {
                None
            };
            #[cfg(all(test, loom, loom))]
            let payload = replacement.as_deref().unwrap_or(&record.value[offset..]);
            #[cfg(not(all(test, loom, loom)))]
            let payload = &record.value[offset..];
            if payload.len() as u64 > maximum_payload_bytes {
                return Err(AuthorityStoreError::ReplayRecordTooLarge {
                    observed: payload.len() as u64,
                    maximum: maximum_payload_bytes,
                });
            }
            let payload = work.bytes(payload)?;
            // Retain provider storage and native funding through the copy.
            drop((record, allocation));
            work.finish(((snapshot.head, Some(payload)), None))
        }
        .await
    }
}

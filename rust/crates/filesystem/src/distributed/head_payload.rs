use super::*;

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    pub(super) async fn head_payload_checked<const RECHECK: bool>(
        &self,
        authority: AuthorityId,
        maximum_payload_bytes: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<((Head, Option<Bytes>), AuthorityAllocation), AuthorityStoreError> {
        let mut child = work.operation()?;
        let result = async {
            let (snapshot, latest) = self
                .fork_snapshot_payload::<true>(authority, &mut child)
                .await
                .and_then(|value| child.finish(value))?;
            let Some((storage, allocation, offset)) = latest else {
                return Ok(((snapshot.head, None), None));
            };
            // Bind storage after funding so errors also destroy it first.
            let record = storage;
            if maximum_payload_bytes == 0 {
                return Err(AuthorityStoreError::InvalidReplayLimit);
            }
            child.check()?;
            // Preserve the replay boundary's retirement and admission checks
            // even though its authenticated record no longer needs another read.
            if RECHECK || !cfg!(all(test, acyclic_fs_loom)) {
                self.authority_require_live(authority, &mut child).await?;
            }
            child.check()?;
            if snapshot.head.sequence.get() != record.sequence {
                return Err(authority_corrupt(
                    "Stream sequence does not match filesystem authority sequence",
                    &child,
                ));
            }
            #[cfg(test)]
            #[cfg(all(acyclic_fs_loom, acyclic_loom))]
            let replacement = if super::head_payload_loom_tests::resample_payload() {
                Some(
                    Box::pin(super::head_payload_loom_tests::resampled_payload(
                        self, authority, &mut child,
                    ))
                    .await?,
                )
            } else {
                None
            };
            #[cfg(test)]
            #[cfg(all(acyclic_fs_loom, acyclic_loom))]
            let payload = replacement.as_deref().unwrap_or(&record.value[offset..]);
            #[cfg(not(all(test, acyclic_fs_loom, acyclic_loom)))]
            let payload = &record.value[offset..];
            if payload.len() as u64 > maximum_payload_bytes {
                return Err(AuthorityStoreError::ReplayRecordTooLarge {
                    observed: payload.len() as u64,
                    maximum: maximum_payload_bytes,
                });
            }
            let payload = child.bytes(payload)?;
            // Retain provider storage and native funding through the copy.
            drop((record, allocation));
            child.finish(((snapshot.head, Some(payload)), None))
        }
        .await;
        work.record(child.spent())?;
        result
    }
}

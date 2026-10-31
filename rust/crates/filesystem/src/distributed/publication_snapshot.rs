use super::*;

impl<P: AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    pub(super) async fn publication_snapshot(
        &self,
        volume: crate::VolumeId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<crate::PublicationSnapshot, AuthorityStoreError> {
        let mut child = work.operation()?;
        let result = async {
            let path = super::changes::changes_path(&self.placement, volume, &mut child)?;
            let path_bytes = path.as_str().len() as u64;
            // Freeze the exclusive cut first. The exact immutable record below
            // belongs to this cut even if a later publication advances the tail.
            let next = self.tail(path.clone(), &mut child).await?;
            let sequence = next
                .checked_sub(1)
                .ok_or(AuthorityStoreError::CorruptStatic(
                    "filesystem changes have no publication record",
                ))?;
            let (record, allocation) = self.record(path, sequence, &mut child).await?;
            child.release(path_bytes)?;
            let encoded = acyclic_stream::changes_context(&record.value, &record.headers)
                .map_err(map_stream_error)?;
            let context =
                crate::PublicationContext::from_bytes(encoded).map_err(map_stream_error)?;
            context
                .validate_record(&record, self.placement.actor_scope())
                .map_err(map_stream_error)?;
            if context.records_after != 0 {
                return Err(AuthorityStoreError::CorruptStatic(
                    "filesystem changes end inside a publication",
                ));
            }
            let snapshot = crate::PublicationSnapshot {
                volume,
                context,
                next,
                preceding: record.commit_id,
            };
            drop((record, allocation));
            child.finish(snapshot)
        }
        .await;
        work.record(child.spent())?;
        result
    }
}

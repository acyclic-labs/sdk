use super::*;

impl<P: StreamProvider> DistributedCoordinator<P> {
    pub(super) async fn read_indexed_intent(
        &self,
        key: &str,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<([u8; 32], SchedulerEvent, u64, acyclic_stream::CommitId)>> {
        validate_idempotency_key(key, work)?;
        #[cfg(all(test, not(acyclic_harness_loom)))]
        if super::tests::intent_index::restore_scan() {
            return read_intent(&self.client, key, Some(self.revision), work).await;
        }
        let Some(sequence) = self.intents.get(key) else {
            return Ok(None);
        };
        self.read_intent_at(key, sequence, self.revision, work)
            .await
    }

    pub(super) async fn read_intent_at(
        &self,
        key: &str,
        sequence: u64,
        maximum_revision: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<([u8; 32], SchedulerEvent, u64, acyclic_stream::CommitId)>> {
        let mut quota = CodecQuota::new(work, 0)?;
        let mut source =
            read_records(&self.client, &self.stream.0, sequence, 1, quota.work()).await?;
        let mut next = sequence;
        let record = next_record(&mut source, &mut next, next_revision(sequence)?, &mut quota)
            .await?
            .ok_or_else(|| {
                guarded_boundary(
                    quota.work(),
                    wire::ErrorCode::Storage,
                    "coordinator intent prefix is incomplete",
                )
            })?;
        let raw_bytes = operations::sdk_record_retained_bytes(&record, quota.work());
        let (revision, source_key, digest, _, event) = decode(&record, quota.work())?;
        let key_bytes = if quota.work().is_some() {
            source_key.capacity() as u64
        } else {
            0
        };
        let event_bytes = if quota.work().is_some() {
            event.logical_owned_bytes()?
        } else {
            0
        };
        quota.adopt(key_bytes + event_bytes)?;
        if source_key != key || revision > maximum_revision {
            return Err(guarded_boundary(
                quota.work(),
                wire::ErrorCode::Storage,
                "coordinator intent index differs from its canonical record",
            ));
        }
        let commit_id = record.commit_id;
        drop((source_key, record));
        quota.relinquish(key_bytes + raw_bytes)?;
        quota.transfer();
        Ok(Some((digest, event, revision, commit_id)))
    }
}

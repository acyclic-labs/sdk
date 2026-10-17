use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Resolves a published generation's sequence and this view's observed head.
    /// Verified historical positions are cached; cold traversal remains paged
    /// and charged. Generations after this view are refused.
    pub async fn generation_position<W: FsWork>(
        &self,
        generation: GenerationId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(Sequence, Sequence), FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            accounting.check()?;
            let head = self
                .authority_head
                .ok_or(FsError::InvalidAuthorityHistory)?
                .sequence;
            if let Some(position) = self.observed_generation.position(generation) {
                return Ok((position, head));
            }
            let positions = &self.volume.fs.inner.generation_positions;
            let key = (self.volume.id, generation);
            accounting.charge(WorkCounters {
                items_examined: super::generation_cache::CAPACITY as u64,
                bytes_copied: size_of::<Sequence>() as u64,
                ..WorkCounters::UNCHARGED
            })?;
            if let Some(position) = positions.get(&key, head) {
                return Ok((position, head));
            }
            let mut after = Sequence::GENESIS;
            while after < head {
                let count = (head.get() - after.get()).min(128) as u32;
                let mut authority = authority_work(&accounting, cancellation)?;
                let page = self
                    .volume
                    .fs
                    .inner
                    .authority
                    .replay(
                        volume_authority_id(self.volume.id),
                        after,
                        ReplayLimit {
                            records: count,
                            payload_bytes: u64::from(count) * MAXIMUM_VOLUME_EVENT_BYTES,
                        },
                        &mut authority,
                    )
                    .await;
                record_authority_work(&mut accounting, &authority)?;
                let (records, _allocation) = page?;
                if records.is_empty() {
                    return Err(FsError::InvalidAuthorityHistory);
                }
                for record in &records {
                    accounting.charge(WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    })?;
                    if record.sequence.get() != after.get() + 1 || record.sequence > head {
                        return Err(FsError::InvalidAuthorityHistory);
                    }
                    if generation_from_record(record, self.volume.id, &mut accounting)?.digest
                        == generation.digest()
                    {
                        // Only the first occurrence found from genesis enters
                        // the cache. A restored head may repeat an older root.
                        accounting.charge(WorkCounters {
                            items_examined: super::generation_cache::CAPACITY as u64,
                            bytes_copied: size_of::<Option<((VolumeId, GenerationId), Sequence)>>()
                                as u64,
                            ..WorkCounters::UNCHARGED
                        })?;
                        positions.record(key, record.sequence);
                        return Ok((record.sequence, head));
                    }
                    after = record.sequence;
                }
            }
            Err(FsError::NotFound)
        }
        .await;
        work.finish(result, &accounting)
    }
}

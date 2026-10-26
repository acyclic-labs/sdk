//! Authenticated changes at every authority step, including reverted changes.
use super::allocation::KernelAccounting;
use super::{CommitFootprint, DecodeLimits, GenerationDiff, GenerationRoot, Owned};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, FsError, Head, ObjectId, VolumeId,
    WorkError,
};

pub(crate) struct FrontierRequest<'a> {
    pub(crate) volume: VolumeId,
    pub(crate) base: (Head, &'a GenerationRoot),
    pub(crate) head: (Head, ObjectId),
    pub(crate) limits: DecodeLimits,
}

pub(crate) struct GenerationFrontier {
    changes: Owned<Vec<Owned<GenerationDiff>>>,
}

impl GenerationFrontier {
    pub(crate) fn independent(
        &self,
        proposal: CommitFootprint<'_>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, FsError> {
        for changes in &self.changes.value {
            let query = proposal.independent(
                CommitFootprint {
                    reads: None,
                    writes: &changes.value,
                },
                accounting.remaining()?.items_examined,
            );
            accounting.charge_items(query.items_examined)?;
            if !query.independent? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn retire(mut self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        while let Some(change) = self.changes.value.pop() {
            change.retire(accounting)?;
        }
        self.changes.retire(accounting)
    }
}

pub(crate) async fn generation_frontier<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    authority: &A,
    objects: &O,
    request: FrontierRequest<'_>,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<GenerationFrontier, FsError> {
    let mut frontier = GenerationFrontier {
        changes: Owned::<Vec<Owned<GenerationDiff>>>::allocate(0, accounting)?,
    };
    let result = async {
        let mut position = request.base.0;
        let mut table = request.base.1.file_table;
        let mut last_root = None;
        if position.sequence > request.head.0.sequence || position.epoch > request.head.0.epoch {
            return Err(FsError::InvalidAuthorityHistory);
        }
        while position.sequence < request.head.0.sequence {
            let facts =
                replay_facts(authority, &request, position, accounting, cancellation).await?;
            let result = async {
                for (head, object) in &facts.value {
                    accounting.charge_items(1)?;
                    let (root, allocation) =
                        super::read_generation_root_with_limits_and_accounting(
                            objects,
                            *object,
                            request.limits,
                            accounting,
                            cancellation,
                        )
                        .await?;
                    let bytes = crate::foundation::usize_to_u64(
                        root.parents.capacity() * std::mem::size_of::<crate::GenerationId>(),
                    );
                    let root = Owned::new(root, allocation, bytes);
                    let result = async {
                        if root.value.volume_id != request.volume
                            || root.value.root_file_id != request.base.1.root_file_id
                        {
                            return Err(FsError::VolumeMismatch);
                        }
                        let maximum = u32::try_from(accounting.remaining()?.items_examined)
                            .unwrap_or(u32::MAX);
                        let (change, allocation) = super::diff_generation_file_tables(
                            objects,
                            table,
                            root.value.file_table,
                            maximum,
                            request.limits,
                            accounting,
                            cancellation,
                        )
                        .await?;
                        let bytes = change.retained_bytes()?;
                        let change = Owned::new(change, allocation, bytes);
                        if change.value.truncated {
                            change.retire(accounting)?;
                            return Err(FsError::InvalidDiff);
                        }
                        let maximum = usize::try_from(
                            request.head.0.sequence.get() - request.base.0.sequence.get(),
                        )
                        .map_err(|_| WorkError::Overflow)?;
                        if let Err(error) = frontier.changes.ensure_for_push(maximum, accounting) {
                            change.retire(accounting)?;
                            return Err(error);
                        }
                        frontier.changes.value.push(change);
                        table = root.value.file_table;
                        position = *head;
                        last_root = Some(*object);
                        Ok(())
                    }
                    .await;
                    root.retire(accounting).and(result)?;
                }
                Ok(())
            }
            .await;
            facts.retire(accounting).and(result)?;
        }
        if position.digest != request.head.0.digest || last_root != Some(request.head.1) {
            return Err(FsError::InvalidAuthorityHistory);
        }
        Ok(())
    }
    .await;
    match result {
        Ok(()) => Ok(frontier),
        Err(error) => {
            frontier.retire(accounting)?;
            Err(error)
        }
    }
}

async fn replay_facts<A: AsyncAuthorityStore>(
    authority: &A,
    request: &FrontierRequest<'_>,
    mut position: Head,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Owned<Vec<(Head, ObjectId)>>, FsError> {
    let count = (request.head.0.sequence.get() - position.sequence.get())
        .min(accounting.remaining()?.items_examined)
        .min(acyclic_stream::MAX_ITEMS as u64);
    let mut work = match accounting {
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work, .. } => {
            crate::AuthorityWork::native(work, cancellation)
        }
        _ => crate::AuthorityWork::new(accounting.remaining()?, cancellation),
    };
    let page = authority
        .replay(
            super::volume_authority_id(request.volume),
            position.sequence,
            crate::ReplayLimit {
                records: u32::try_from(count).map_err(|_| WorkError::Overflow)?,
                payload_bytes: count
                    .checked_mul(acyclic_stream::MAX_RECORD_BYTES as u64)
                    .ok_or(WorkError::Overflow)?,
            },
            &mut work,
        )
        .await;
    let spent = work.spent();
    drop(work);
    let retained = if let Some((allocations, work, budget, _)) = accounting.standalone_parts() {
        *work = work.with_backend(spent, allocations.live_bytes())?;
        work.verify(budget)?;
        Some((
            accounting.claim_owned(spent.peak_allocation_bytes, 0)?,
            spent.peak_allocation_bytes,
        ))
    } else {
        None
    };
    let result = async {
        let (records, _allocation) = page?;
        if records.is_empty() {
            return Err(FsError::InvalidAuthorityHistory);
        }
        let mut facts = Owned::<Vec<(Head, ObjectId)>>::allocate(records.len(), accounting)?;
        let result = (|| {
            for record in &records {
                accounting.charge_items(1)?;
                if record.sequence
                    != position
                        .sequence
                        .checked_next()
                        .map_err(|_| FsError::InvalidAuthorityHistory)?
                    || record.sequence > request.head.0.sequence
                    || record.previous_digest != position.digest
                    || record.epoch < position.epoch
                    || record.epoch > request.head.0.epoch
                {
                    return Err(FsError::InvalidAuthorityHistory);
                }
                let decoded = super::decode_publication_evidence_with_accounting(
                    &record.payload,
                    acyclic_stream::MAX_RECORD_BYTES as u64,
                    accounting,
                )?;
                if decoded.publication.volume_id != request.volume {
                    return Err(FsError::VolumeMismatch);
                }
                position = Head {
                    epoch: record.epoch,
                    sequence: record.sequence,
                    digest: record.digest,
                };
                facts
                    .value
                    .push((position, decoded.publication.generation_root));
            }
            Ok(())
        })();
        match result {
            Ok(()) => Ok(facts),
            Err(error) => {
                facts.retire(accounting)?;
                Err(error)
            }
        }
    }
    .await;
    if let Some((allocation, bytes)) = retained {
        super::persistent_point::retire_allocation(allocation, bytes, accounting)?;
    }
    result
}

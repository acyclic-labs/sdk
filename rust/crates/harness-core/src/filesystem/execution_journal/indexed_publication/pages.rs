//! Oversized initial snapshots stage bounded, unreachable pages before the cut.
use super::*;

#[cfg(test)]
mod tests;

pub(super) struct Cut<'a> {
    pub(super) operation: OperationId,
    pub(super) execution: &'a StreamPath,
    pub(super) head: &'a StreamPath,
    pub(super) pages: &'a StreamPath,
    pub(super) execution_condition: CommitCondition,
    pub(super) head_condition: CommitCondition,
    pub(super) owner: Option<CommitCondition>,
    pub(super) execution_tail: u64,
    pub(super) head_tail: u64,
    pub(super) base: u64,
    pub(super) digest: [u8; 32],
}

impl Cut<'_> {
    fn request<'a, 'b>(
        &self,
        page_condition: CommitCondition,
        records: &[OwnedBytes],
        publication: Option<(&OwnedBytes, &OwnedBytes)>,
        work: &'a mut Option<&'b mut dyn crate::store::HistoryIo>,
    ) -> Result<(
        acyclic_stream::CommitRequest,
        crate::wire_codec::CodecQuota<'a, 'b>,
    )> {
        let digest = if publication.is_some() {
            self.digest
        } else {
            let mut input = [0; 48];
            input[..32].copy_from_slice(&self.digest);
            let expected = match &page_condition {
                CommitCondition::Tail { expected, .. } => *expected,
                CommitCondition::Absent { .. } => 0,
                _ => return Err(invalid(None)),
            };
            input[32..40].copy_from_slice(&expected.to_le_bytes());
            input[40..].copy_from_slice(&(records.len() as u64).to_le_bytes());
            if let Some(work) = work.as_deref_mut() {
                work.hashed_bytes(input.len() as u64)?;
            }
            *blake3::hash(&input).as_bytes()
        };
        let (key, held) = crate::distributed::digest_key(&digest, work)?;
        let mut request = crate::distributed::commit_request(key, held, work);
        if let Some(owner) = &self.owner {
            crate::distributed::add_condition(&mut request, owner.clone())?;
        }
        crate::distributed::add_condition(&mut request, self.head_condition.clone())?;
        crate::distributed::add_condition(&mut request, self.execution_condition.clone())?;
        crate::distributed::add_condition(&mut request, page_condition)?;
        if let Some((observation, checkpoint)) = publication {
            add_records(
                &mut request,
                self.execution,
                std::slice::from_ref(observation),
            )?;
            add_records(&mut request, self.head, std::slice::from_ref(checkpoint))?;
        }
        add_records(&mut request, self.pages, records)?;
        Ok(request)
    }

    async fn stage<P: StreamProvider>(
        &self,
        provider: &Arc<P>,
        offset: usize,
        records: &[OwnedBytes],
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Option<usize>> {
        // Keep one page for the final journal/head commit. Each candidate is
        // checked against the actual wire size, rather than a raised limit.
        let mut count = (records.len() - 1).min(acyclic_stream::MAX_ITEMS);
        let expected = self
            .base
            .checked_add(offset as u64)
            .ok_or_else(|| invalid(None))?;
        loop {
            if count == 0 {
                return Err(invalid(None));
            }
            let condition = condition(provider, self.pages, expected, work).await?;
            let batch = records.get(..count).ok_or_else(|| invalid(None))?;
            let mut request = self.request(condition, batch, None, work)?;
            if let Some(work) = request.1.work().as_deref_mut() {
                work.codec_charge(crate::executor::json::JsonCodecCharge::Inspect {
                    items: count as u64,
                })?;
            }
            if request.0.wire_encoded_len() > acyclic_stream::MAX_COMMAND_BYTES {
                drop(request);
                count /= 2;
                continue;
            }
            let (outcome, retained) =
                crate::distributed::commit(request, provider, Some(self.operation), None).await?;
            let held = crate::wire_codec::CodecQuota::from_retained(work, retained);
            let committed = match &outcome {
                CommitOutcome::Conflict(_) => Ok(false),
                CommitOutcome::Committed(envelope) => {
                    if envelope.mutations.len() != 1
                        || !confirms(envelope, self.pages, expected, batch)
                    {
                        Err(Error::Indeterminate(self.operation))
                    } else {
                        Ok(true)
                    }
                }
            };
            drop(outcome);
            drop(held);
            return committed.map(|committed| committed.then_some(count));
        }
    }

    pub(super) async fn publish<P: StreamProvider>(
        &self,
        provider: &Arc<P>,
        records: &[OwnedBytes],
        observation: &OwnedBytes,
        checkpoint: &OwnedBytes,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<bool> {
        let mut offset = 0;
        loop {
            let pending = records
                .get(offset..)
                .filter(|records| !records.is_empty())
                .ok_or_else(|| invalid(None))?;
            if pending.len() <= acyclic_stream::MAX_ITEMS {
                let expected = self
                    .base
                    .checked_add(offset as u64)
                    .ok_or_else(|| invalid(None))?;
                let condition = condition(provider, self.pages, expected, work).await?;
                let mut request =
                    self.request(condition, pending, Some((observation, checkpoint)), work)?;
                if let Some(work) = request.1.work().as_deref_mut() {
                    work.codec_charge(crate::executor::json::JsonCodecCharge::Inspect {
                        items: pending.len() as u64,
                    })?;
                }
                if request.0.wire_encoded_len() <= acyclic_stream::MAX_COMMAND_BYTES {
                    let (outcome, retained) =
                        crate::distributed::commit(request, provider, Some(self.operation), None)
                            .await?;
                    let held = crate::wire_codec::CodecQuota::from_retained(work, retained);
                    let result = self.confirm(&outcome, expected, pending, observation, checkpoint);
                    drop(outcome);
                    drop(held);
                    return result;
                }
                drop(request);
            }
            let Some(count) = self.stage(provider, offset, pending, work).await? else {
                return Ok(false);
            };
            offset += count;
        }
    }

    fn confirm(
        &self,
        outcome: &CommitOutcome,
        pages_tail: u64,
        records: &[OwnedBytes],
        observation: &OwnedBytes,
        checkpoint: &OwnedBytes,
    ) -> Result<bool> {
        match outcome {
            CommitOutcome::Conflict(_) => Ok(false),
            CommitOutcome::Committed(envelope) => {
                if envelope.mutations.len() != 3
                    || !confirms(
                        envelope,
                        self.execution,
                        self.execution_tail,
                        std::slice::from_ref(observation),
                    )
                    || !confirms(
                        envelope,
                        self.head,
                        self.head_tail,
                        std::slice::from_ref(checkpoint),
                    )
                    || !confirms(envelope, self.pages, pages_tail, records)
                {
                    return Err(Error::Indeterminate(self.operation));
                }
                Ok(true)
            }
        }
    }
}

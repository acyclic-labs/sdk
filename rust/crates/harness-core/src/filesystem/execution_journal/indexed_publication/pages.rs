//! Oversized initial snapshots stage bounded, unreachable pages before the cut.
use super::*;

#[cfg(not(target_arch = "wasm32"))]
mod retained;
#[cfg(test)]
mod tests;
#[cfg(not(target_arch = "wasm32"))]
pub(super) use retained::Publication;

pub(super) struct Cut<'a> {
    pub(super) operation: OperationId,
    pub(super) execution: &'a StreamPath,
    pub(super) head: &'a StreamPath,
    pub(super) pages: &'a StreamPath,
    pub(super) execution_condition: CommitCondition,
    pub(super) head_condition: CommitCondition,
    pub(super) execution_tail: u64,
    pub(super) head_tail: u64,
    pub(super) base: u64,
    pub(super) digest: [u8; 32],
    pub(super) contributions: &'a [Contribution<'a>],
}

/// One already-encoded record and its original optimistic publication fence.
/// This adds no submission, index or authority; its producer owns the payload.
pub(in crate::filesystem) struct Contribution<'a> {
    pub(in crate::filesystem) condition: CommitCondition,
    pub(in crate::filesystem) record: &'a OwnedBytes,
}

pub(in crate::filesystem::execution_journal) fn contribution_identity(
    contributions: &[Contribution<'_>],
    io: Option<&PreparationIo>,
) -> Result<Option<[u8; 32]>> {
    if contributions.is_empty() {
        return Ok(None);
    }
    let domain = b"harness/v1/contributed-records\0";
    let digests = (contributions.len() as u64)
        .checked_mul(32)
        .ok_or_else(|| invalid(io))?;
    let bytes = crate::core::owned_sum([domain.len() as u64, 8, digests])?;
    if let Some(io) = io {
        io.charge(acyclic_fs::WorkCounters {
            bytes_hashed: bytes,
            ..Default::default()
        })?;
    }
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    hash.update(&(contributions.len() as u64).to_le_bytes());
    let mut previous = None;
    for contribution in contributions {
        let (path, _) = contribution.target()?;
        if previous.is_some_and(|prior| prior >= path) {
            return Err(invalid(io));
        }
        previous = Some(path);
        let digest = acyclic_stream::request::conditioned_record_digest_with(
            &contribution.condition,
            contribution.record,
            |bytes_hashed| {
                if let Some(io) = io {
                    io.charge(acyclic_fs::WorkCounters {
                        bytes_hashed,
                        ..Default::default()
                    })?;
                }
                Ok::<(), Error>(())
            },
        )?;
        hash.update(&digest);
    }
    Ok(Some(*hash.finalize().as_bytes()))
}

impl Contribution<'_> {
    fn target(&self) -> Result<(&StreamPath, u64)> {
        match &self.condition {
            CommitCondition::Head { path, expected, .. }
            | CommitCondition::Tail { path, expected } => Ok((path, *expected)),
            CommitCondition::Absent { path } => Ok((path, 0)),
            CommitCondition::Present { .. } => Err(invalid(None)),
        }
    }
}

impl Cut<'_> {
    pub(super) fn request<'a, 'b>(
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
        self.append_to(
            &mut request.0,
            &mut request.1,
            page_condition,
            records,
            publication,
        )?;
        Ok(request)
    }

    /// Consumes the entire unsent batch on refusal. Its original request,
    /// filesystem holds and construction quota are dropped together on error.
    pub(super) fn contribute<'a, 'b>(
        &self,
        mut prepared: (
            acyclic_fs::FilesystemCommit,
            crate::wire_codec::CodecQuota<'a, 'b>,
        ),
        page_condition: CommitCondition,
        records: &[OwnedBytes],
        observation: &OwnedBytes,
        checkpoint: &OwnedBytes,
    ) -> Result<(
        acyclic_fs::FilesystemCommit,
        crate::wire_codec::CodecQuota<'a, 'b>,
    )> {
        let request = prepared.0.request_mut().ok_or_else(|| invalid(None))?;
        self.append_to(
            request,
            &mut prepared.1,
            page_condition,
            records,
            Some((observation, checkpoint)),
        )?;
        Ok(prepared)
    }

    fn append_to(
        &self,
        request: &mut acyclic_stream::CommitRequest,
        custody: &mut crate::wire_codec::CodecQuota<'_, '_>,
        page_condition: CommitCondition,
        records: &[OwnedBytes],
        publication: Option<(&OwnedBytes, &OwnedBytes)>,
    ) -> Result<()> {
        // Both standalone and composed publication use this one funded builder.
        for condition in [
            self.head_condition.clone(),
            self.execution_condition.clone(),
            page_condition,
        ] {
            crate::distributed::add_condition(request, custody, condition)?;
        }
        if let Some((observation, checkpoint)) = publication {
            add_records(
                request,
                custody,
                self.execution,
                std::slice::from_ref(observation),
            )?;
            add_records(
                request,
                custody,
                self.head,
                std::slice::from_ref(checkpoint),
            )?;
        }
        add_records(request, custody, self.pages, records)?;
        if publication.is_some() {
            let mut previous = None;
            for contribution in self.contributions {
                let (path, _) = contribution.target()?;
                if previous.is_some_and(|prior| prior >= path)
                    || path == self.execution
                    || path == self.head
                    || path == self.pages
                {
                    return Err(invalid(None));
                }
                previous = Some(path);
                crate::distributed::add_condition(
                    request,
                    custody,
                    contribution.condition.clone(),
                )?;
                add_records(
                    request,
                    custody,
                    path,
                    std::slice::from_ref(contribution.record),
                )?;
            }
        }
        Ok(())
    }

    async fn stage<P: crate::store::JournalProvider + ?Sized>(
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
                crate::distributed::commit(request, provider, Some(self.operation), None, None)
                    .await?;
            let held = crate::wire_codec::CodecQuota::from_retained(work, retained);
            let committed = match &outcome {
                CommitOutcome::Conflict(_) => Ok(false),
                CommitOutcome::Committed(receipt) => {
                    let valid = matches!(receipt.appends.as_slice(), [append]
                        if confirms(append, self.pages, expected, batch));
                    if !valid || !receipt.forks.is_empty() {
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

    pub(super) async fn publish<P: crate::store::JournalProvider + ?Sized>(
        &self,
        provider: &Arc<P>,
        records: &[OwnedBytes],
        observation: &OwnedBytes,
        checkpoint: &OwnedBytes,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Option<acyclic_stream::CommitId>> {
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
                    #[cfg(not(target_arch = "wasm32"))]
                    if request
                        .1
                        .work()
                        .as_deref()
                        .and_then(|io| io.original_native_work())
                        .is_some()
                    {
                        let (prepared, producer) = self.prepare_native(
                            provider,
                            request,
                            Publication {
                                pages_tail: expected,
                                records: pending,
                                observation,
                                checkpoint,
                            },
                        )?;
                        crate::distributed::native_call_result(
                            prepared.original(),
                            producer.await,
                        )?
                        .map_err(|error| {
                            crate::store::operations::stream_error_with_work(error, work)
                        })?;
                        return prepared.finish(work).await;
                    }
                    let (outcome, retained) = crate::distributed::commit(
                        request,
                        provider,
                        Some(self.operation),
                        None,
                        None,
                    )
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
                return Ok(None);
            };
            offset += count;
        }
    }

    pub(super) fn confirm(
        &self,
        outcome: &CommitOutcome,
        pages_tail: u64,
        records: &[OwnedBytes],
        observation: &OwnedBytes,
        checkpoint: &OwnedBytes,
    ) -> Result<Option<acyclic_stream::CommitId>> {
        self.confirm_members(outcome, pages_tail, records, observation, checkpoint, false)
    }

    /// The original normalized batch authenticates caller members before any
    /// journal member can advance its cache or checkpoint.
    pub(super) fn confirm_composed(
        &self,
        prepared: &acyclic_fs::FilesystemCommit,
        outcome: &CommitOutcome,
        pages_tail: u64,
        records: &[OwnedBytes],
        observation: &OwnedBytes,
        checkpoint: &OwnedBytes,
    ) -> Result<Option<acyclic_stream::CommitId>> {
        if let CommitOutcome::Committed(receipt) = outcome {
            receipt
                .validate_request(prepared.request())
                .map_err(|_| Error::Indeterminate(self.operation))?;
        }
        self.confirm_members(outcome, pages_tail, records, observation, checkpoint, true)
    }

    fn confirm_members(
        &self,
        outcome: &CommitOutcome,
        pages_tail: u64,
        records: &[OwnedBytes],
        observation: &OwnedBytes,
        checkpoint: &OwnedBytes,
        composed: bool,
    ) -> Result<Option<acyclic_stream::CommitId>> {
        match outcome {
            CommitOutcome::Conflict(_) => Ok(None),
            CommitOutcome::Committed(receipt) => {
                if !composed && receipt.appends.len() != 3 + self.contributions.len() {
                    return Err(Error::Indeterminate(self.operation));
                }
                if !composed && !receipt.forks.is_empty() {
                    return Err(Error::Indeterminate(self.operation));
                }
                // Stream canonicalizes mutations by path before publication.
                // Visit every member once; contributed paths are admitted in
                // that same order, independently of the three journal paths.
                let mut previous = None;
                let mut seen = 0_u8;
                let mut contributions = self.contributions.iter().peekable();
                for append in &receipt.appends {
                    if previous.is_some_and(|path| path >= &append.path) {
                        return Err(Error::Indeterminate(self.operation));
                    }
                    previous = Some(&append.path);
                    let (bit, valid) = if append.path == *self.execution {
                        (
                            1,
                            confirms(
                                append,
                                self.execution,
                                self.execution_tail,
                                std::slice::from_ref(observation),
                            ),
                        )
                    } else if append.path == *self.head {
                        (
                            2,
                            confirms(
                                append,
                                self.head,
                                self.head_tail,
                                std::slice::from_ref(checkpoint),
                            ),
                        )
                    } else if append.path == *self.pages {
                        (4, confirms(append, self.pages, pages_tail, records))
                    } else {
                        let contribution = contributions.peek();
                        let target = contribution.map(|item| item.target()).transpose()?;
                        if composed && target.is_none_or(|(path, _)| path != &append.path) {
                            continue;
                        }
                        let Some(contribution) = contributions.next() else {
                            return Err(Error::Indeterminate(self.operation));
                        };
                        let (path, start) = contribution.target()?;
                        (
                            0,
                            confirms(
                                append,
                                path,
                                start,
                                std::slice::from_ref(contribution.record),
                            ),
                        )
                    };
                    if !valid || seen & bit != 0 {
                        return Err(Error::Indeterminate(self.operation));
                    }
                    seen |= bit;
                }
                if seen != 7 || contributions.next().is_some() {
                    return Err(Error::Indeterminate(self.operation));
                }
                Ok(Some(receipt.commit_id))
            }
        }
    }
}

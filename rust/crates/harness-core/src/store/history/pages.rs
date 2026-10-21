use super::*;

impl<P: StreamProvider> HistoryReader<P> {
    /// Binds the canonical path to its authenticated verifier.
    pub fn new(
        client: &std::sync::Arc<P>,
        authority: &Authority,
        verifier: &AuthorityVerifier,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        verifier.verify_audience(authority, work)?;
        let verifier = verifier.copy(work)?;
        let held = if work.is_some() {
            verifier.logical_owned_bytes()?
        } else {
            0
        };
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, held);
        let (path, _) = crate::store::operations::publication_path(
            authority,
            "harness/v2/",
            None,
            quota.work(),
        )?;
        quota.transfer();
        Ok(Self {
            client: client.clone(),
            path,
            verifier,
        })
    }

    /// Resolves an attested operation under a finite encoded-byte allowance.
    pub async fn operation_event(
        &self,
        operation: crate::OperationId,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Option<Event>, u64)> {
        crate::store::operations::find_operation_bounded_with_work(
            &self.client,
            self.verifier.audience(),
            &self.verifier,
            operation,
            maximum_bytes,
            true,
            work,
        )
        .await
    }

    /// Reconstructs an effect at the pinned cut and returns its retained bytes.
    pub async fn effect(
        &self,
        cursor: &HistoryCursor,
        effect: crate::EffectId,
        limits: HistoryReadLimits,
        mut work: Option<&mut dyn HistoryIo>,
    ) -> Result<Option<(crate::core::EffectState, u64)>> {
        project_effect(
            &self.client,
            &self.verifier,
            cursor,
            effect,
            limits,
            &mut work,
        )
        .await
    }

    /// Captures a committed boundary for archival traversal.
    pub async fn pin(
        &self,
        after_revision: u64,
        mut work: Option<&mut dyn HistoryIo>,
    ) -> Result<HistoryCursor> {
        let through_revision = self.committed_tail_with_work(&mut work).await?;
        if after_revision > through_revision {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Invalid,
                "history cursor exceeds committed tail",
            ));
        }
        let mut quota = crate::wire_codec::CodecQuota::from_retained(&mut work, 0);
        let authority = crate::core::copy_authority(self.verifier.audience(), &mut quota)?;
        if let Some(io) = quota.work().as_deref() {
            io.check_cancellation()?;
        }
        quota.transfer();
        Ok(HistoryCursor {
            authority,
            after_revision,
            through_revision,
        })
    }

    /// Reads one bounded page without changing the supplied cursor.
    pub async fn read_page(
        &self,
        cursor: &HistoryCursor,
        limits: HistoryReadLimits,
        mut work: Option<&mut dyn HistoryIo>,
    ) -> Result<HistoryPage> {
        use crate::{
            store::guarded_boundary as boundary, wire::ErrorCode::Invalid, wire_codec::CodecQuota,
        };
        let mut quota = CodecQuota::new(&mut work, 0)?;
        self.verifier
            .verify_audience(&cursor.authority, quota.work())?;
        if limits.maximum_events == 0
            || limits.maximum_bytes == 0
            || cursor.after_revision > cursor.through_revision
        {
            return Err(boundary(
                quota.work(),
                Invalid,
                "history read bounds or cursor are invalid",
            ));
        }
        if cursor.through_revision > self.committed_tail_with_work(quota.work()).await? {
            return Err(boundary(
                quota.work(),
                Invalid,
                "history cursor exceeds committed tail",
            ));
        }
        let mut next = HistoryCursor {
            authority: match quota.work().as_deref_mut() {
                Some(io) => io.clone_authority(&cursor.authority)?,
                None => cursor.authority.clone(),
            },
            after_revision: cursor.after_revision,
            through_revision: cursor.through_revision,
        };
        quota.adopt(next.authority.id.capacity() as u64)?;
        let count = u32::try_from(
            (cursor.through_revision - cursor.after_revision).min(u64::from(limits.maximum_events)),
        )
        .map_err(|_| {
            boundary(
                quota.work(),
                Invalid,
                "history page exceeds event allowance",
            )
        })?;
        let mut events = Vec::new();
        self.read_page_records(cursor, limits, count, &mut next, &mut events, &mut quota)
            .await?;
        quota.transfer();
        Ok(HistoryPage {
            events,
            cursor: next,
        })
    }

    pub(super) async fn committed_tail_with_work(
        &self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<u64> {
        let tail =
            crate::store::operations::tail_optional_with_work(&self.client, &self.path, work)
                .await?;
        if let Some(work) = work.as_deref() {
            work.check_cancellation()?;
        }
        Ok(tail.unwrap_or(0))
    }

    async fn read_page_records(
        &self,
        cursor: &HistoryCursor,
        limits: HistoryReadLimits,
        count: u32,
        next: &mut HistoryCursor,
        events: &mut Vec<Event>,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<()> {
        use crate::{
            store::guarded_boundary as boundary,
            wire::ErrorCode::{Invalid, Storage},
        };
        if count != 0 {
            let mut records = crate::store::operations::read_records(
                &self.client,
                &self.path,
                cursor.after_revision,
                count,
                quota.work(),
            )
            .await?
            .map_err(|error| {
                crate::store::operations::stream_error_with_work(error, quota.work())
            })?;
            let mut bytes = 0_u64;
            loop {
                if let Some(io) = quota.work().as_deref() {
                    io.check_cancellation()?;
                }
                let Some(record) = records.next(quota.work()).await? else {
                    break;
                };
                let raw =
                    crate::store::operations::sdk_record_retained_bytes(&record, quota.work());
                quota.adopt(raw)?;
                if let Some(io) = quota.work().as_deref() {
                    io.check_cancellation()?;
                }
                if events.len() >= count as usize || record.sequence != next.after_revision {
                    return Err(boundary(
                        quota.work(),
                        Storage,
                        "history page is not bounded and contiguous",
                    ));
                }
                bytes = bytes
                    .checked_add(record.value.len() as u64)
                    .filter(|bytes| *bytes <= limits.maximum_bytes)
                    .ok_or_else(|| {
                        boundary(quota.work(), Invalid, "history page exceeds byte bound")
                    })?;
                let (event, held) = verify_history_record_with_retained_quota(
                    &self.verifier,
                    &cursor.authority,
                    next.after_revision,
                    &record,
                    quota.work(),
                )?;
                quota.adopt(held)?;
                if let Some(io) = quota.work().as_deref_mut() {
                    io.admit_output(bytes, events.len() as u64 + 1)?;
                    let before = events.capacity();
                    ensure_slot(events, count as usize, io)?;
                    quota.adopt(
                        ((events.capacity() - before) * std::mem::size_of::<Event>()) as u64,
                    )?;
                }
                next.after_revision = event.revision;
                events.push(event);
                drop(record);
                quota.relinquish(raw)?;
            }
            if events.is_empty() {
                return Err(boundary(
                    quota.work(),
                    Storage,
                    "history page stopped before its pinned boundary",
                ));
            }
            if let Some(io) = quota.work().as_deref_mut() {
                io.check_cancellation()?;
                io.stage_output(bytes, events.len() as u64)?;
            }
        } else if let Some(io) = quota.work().as_deref() {
            io.check_cancellation()?;
        }
        Ok(())
    }
}

pub(in crate::store) async fn project_effect<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    verifier: &AuthorityVerifier,
    cursor: &HistoryCursor,
    effect: crate::EffectId,
    limits: HistoryReadLimits,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Option<(crate::core::EffectState, u64)>> {
    let mut quota = crate::wire_codec::CodecQuota::from_retained(work, 0);
    let (events, held) = super::super::effects::effect_events(
        client,
        verifier,
        cursor,
        effect,
        limits,
        quota.work(),
    )
    .await?;
    quota.adopt(held)?;
    if events.is_empty() {
        return Ok(None);
    }
    let authority = crate::core::copy_authority(&cursor.authority, &mut quota)?;
    let verifier = verifier.copy(quota.work())?;
    // The reducer takes ownership of these metadata claims.
    quota.transfer();
    quota.adopt(held)?;
    let result = crate::core::Reducer::project_effect_with_retained_quota(
        authority,
        verifier,
        effect,
        &events,
        quota.work(),
    );
    drop(events);
    let released = quota.relinquish(held);
    let result = result?;
    released?;
    Ok(Some(result))
}

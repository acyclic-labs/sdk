use super::*;

impl<P: StreamProvider> HistoryReader<P> {
    pub(crate) fn logical_owned_bytes(&self, work: &Option<&mut dyn HistoryIo>) -> Result<u64> {
        let path = if work
            .as_deref()
            .is_some_and(crate::store::operations::original_native_decode_mode)
        {
            0
        } else {
            self.path.as_str().len() as u64
        };
        self.verifier
            .logical_owned_bytes()?
            .checked_add(path)
            .ok_or_else(|| Error::Invalid("history reader metadata byte count overflows".into()))
    }

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
        let (events, event_owned) = crate::store::effects::effect_events(
            &self.client,
            &self.verifier,
            cursor,
            effect,
            limits,
            &mut work,
        )
        .await?;
        if events.is_empty() {
            return Ok(None);
        }
        let authority = match work.as_deref_mut() {
            Some(work) => work.clone_authority(&cursor.authority)?,
            None => cursor.authority.clone(),
        };
        let verifier = self.verifier.copy(&mut work)?;
        let result = crate::core::Reducer::project_effect_with_retained_quota(
            authority, verifier, effect, &events, &mut work,
        );
        drop(events);
        if let Some(work) = work.as_deref_mut() {
            let released = work.release_owned(event_owned);
            if result.is_ok() {
                released?;
            }
        }
        result.map(Some)
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
        let authority = match work.as_deref_mut() {
            Some(work) => work.clone_authority(self.verifier.audience())?,
            None => self.verifier.audience().clone(),
        };
        if let Some(work) = work.as_deref_mut() {
            work.check_cancellation()?;
        }
        Ok(HistoryCursor {
            authority,
            after_revision,
            through_revision,
        })
    }

    /// Reads at most one caller-bounded page; a failure leaves the supplied cursor intact.
    pub async fn read_page(
        &self,
        cursor: &HistoryCursor,
        limits: HistoryReadLimits,
        work: Option<&mut dyn HistoryIo>,
    ) -> Result<HistoryPage> {
        self.read_page_with_work(cursor, limits, false, work)
            .await
            .map(|(page, _, _, _)| page)
    }

    pub(crate) async fn read_page_with_work(
        &self,
        cursor: &HistoryCursor,
        limits: HistoryReadLimits,
        retain_records: bool,
        mut work: Option<&mut dyn HistoryIo>,
    ) -> Result<(
        HistoryPage,
        Vec<crate::store::operations::ObservedRecord>,
        u64,
        u64,
    )> {
        self.verifier
            .verify_audience(&cursor.authority, &mut work)?;
        if limits.maximum_events == 0
            || limits.maximum_bytes == 0
            || cursor.after_revision > cursor.through_revision
        {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Invalid,
                "history read bounds or cursor are invalid",
            ));
        }
        if cursor.through_revision > self.committed_tail_with_work(&mut work).await? {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Invalid,
                "history cursor exceeds committed tail",
            ));
        }
        let mut next = match work.as_deref_mut() {
            Some(work) => HistoryCursor {
                authority: work.clone_authority(&cursor.authority)?,
                after_revision: cursor.after_revision,
                through_revision: cursor.through_revision,
            },
            None => cursor.clone(),
        };
        let count = u32::try_from(
            (cursor.through_revision - cursor.after_revision).min(u64::from(limits.maximum_events)),
        )
        .map_err(|_| {
            crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Invalid,
                "history page count exceeds platform bounds",
            )
        })?;
        let mut events = Vec::new();
        let mut events_retained_bytes = 0_u64;
        if count == 0 {
            if let Some(work) = work.as_deref_mut() {
                work.check_cancellation()?;
            }
            return Ok((
                HistoryPage {
                    events,
                    cursor: next,
                },
                Vec::new(),
                0,
                0,
            ));
        }
        let mut records = crate::store::operations::read_records(
            &self.client,
            &self.path,
            cursor.after_revision,
            count,
            &mut work,
        )
        .await?
        .map_err(|error| crate::distributed::sdk_error(error, &work))?;
        let mut retained_records = Vec::new();
        let mut bytes = 0_u64;
        loop {
            if let Some(work) = work.as_deref_mut() {
                work.check_cancellation()?;
            }
            let Some(record) = records.next(&mut work).await? else {
                break;
            };
            let record_bytes = crate::store::operations::sdk_record_retained_bytes(&record, &work);
            if let Some(work) = work.as_deref_mut() {
                work.check_cancellation()?;
            }
            if events.len() >= count as usize || record.sequence != next.after_revision {
                return Err(crate::store::guarded_boundary(
                    &work,
                    crate::wire::ErrorCode::Storage,
                    "history page is not bounded and contiguous",
                ));
            }
            bytes = bytes
                .checked_add(record.value.len() as u64)
                .filter(|total| *total <= limits.maximum_bytes)
                .ok_or_else(|| {
                    crate::store::guarded_boundary(
                        &work,
                        crate::wire::ErrorCode::Invalid,
                        "history page exceeds byte bound",
                    )
                })?;
            let (event, event_retained) = verify_history_record_with_retained_quota(
                &self.verifier,
                &cursor.authority,
                next.after_revision,
                &record,
                &mut work,
            )?;
            events_retained_bytes = events_retained_bytes
                .checked_add(event_retained)
                .ok_or_else(|| {
                    crate::store::guarded_boundary(
                        &work,
                        crate::wire::ErrorCode::Storage,
                        "history event custody overflows",
                    )
                })?;
            if let Some(work) = work.as_deref_mut() {
                work.admit_output(bytes, events.len() as u64 + 1)?;
                work.reserve_events(&mut events, count as usize)?;
            }
            next.after_revision = event.revision;
            events.push(event);
            if retain_records {
                if let Some(work) = work.as_deref_mut() {
                    work.reserve_records(&mut retained_records, count as usize)?;
                }
                retained_records.push(record);
            } else {
                drop(record);
                if let Some(work) = work.as_deref_mut() {
                    work.release_owned(record_bytes)?;
                }
            }
        }
        if events.is_empty() {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Storage,
                "history page stopped before its pinned boundary",
            ));
        }
        if let Some(work) = work.as_deref_mut() {
            work.check_cancellation()?;
            work.stage_output(bytes, events.len() as u64)?;
        }
        Ok((
            HistoryPage {
                events,
                cursor: next,
            },
            retained_records,
            bytes,
            events_retained_bytes,
        ))
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
}

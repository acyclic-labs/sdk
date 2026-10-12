use super::*;

impl<P: StreamProvider> StreamAggregate<P> {
    /// Restores an authenticated aggregate at a bounded committed cut.
    #[allow(
        clippy::too_many_arguments,
        reason = "canonical restore inputs and observer"
    )]
    pub async fn open(
        client: &std::sync::Arc<P>,
        authority: Authority,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        snapshot: Option<Snapshot>,
        limits: HistoryReadLimits,
        maximum_terminal_effects: usize,
        original_revision: Option<u64>,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<Self> {
        authority_verifier.verify_audience(&authority, &mut None)?;
        if limits.maximum_events == 0 || limits.maximum_bytes == 0 || maximum_terminal_effects == 0
        {
            return Err(Error::Invalid(
                "projection read bounds must be positive".into(),
            ));
        }
        let reader = HistoryReader::new(client, &authority, &authority_verifier, work)?;
        let reader_bytes = if work.is_some() {
            reader.verifier.logical_owned_bytes()?
        } else {
            0
        };
        let loaded = match snapshot {
            Some(snapshot) => {
                checkpoints::supplied(
                    client,
                    &authority,
                    snapshot,
                    limits,
                    original_revision,
                    work,
                )
                .await?
            }
            None => {
                checkpoints::load_with_work(
                    client,
                    &authority,
                    &authority_verifier,
                    limits,
                    original_revision,
                    work,
                )
                .await?
            }
        };
        let path = reader.path.clone();
        let mut reducer = if let Some(snapshot) = loaded.snapshot {
            if snapshot.authority != authority {
                return Err(Error::Invalid(
                    "snapshot authority does not match requested aggregate".into(),
                ));
            }
            Reducer::restore(snapshot, authority_verifier, schemas, work)?
        } else {
            Reducer::new(authority, authority_verifier, schemas)
        };
        reducer.set_resident_event_limit(1024)?;
        reducer.set_resident_terminal_effect_limit(maximum_terminal_effects)?;
        let start = reducer.revision();
        let remaining_events = limits.maximum_events - loaded.consumed_events;
        if loaded.through_revision < start
            || loaded.through_revision - start > u64::from(remaining_events)
        {
            return Err(Error::Invalid(
                "projection suffix exceeds event allowance".into(),
            ));
        }
        if start < loaded.through_revision {
            Self::restore_suffix(
                client,
                &reader,
                &mut reducer,
                loaded.through_revision,
                remaining_events,
                limits.maximum_bytes.saturating_sub(loaded.consumed_bytes),
                work,
            )
            .await?;
        }
        drop(reader);
        if let Some(work) = work.as_deref_mut() {
            work.release_owned(reader_bytes)?;
        }
        if let Some(work) = work.as_deref_mut() {
            work.check_cancellation()?;
        }

        Ok(Self {
            client: client.clone(),
            path,
            reducer,
            content_verifier: None,
            fork_verifier: None,
            merge_verifier: None,
            extension_migrations: None,
            limits: Limits::default(),
            effect_history_limits: default_projection_read_limits(),
        })
    }

    pub(super) async fn restore_suffix(
        client: &std::sync::Arc<P>,
        reader: &HistoryReader<P>,
        reducer: &mut Reducer,
        through: u64,
        maximum_events: u32,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<()> {
        use crate::{
            wire::ErrorCode::{Invalid, Storage},
            wire_codec::CodecQuota,
        };
        let start = reducer.revision();
        if maximum_bytes == 0 || through < start || through - start > u64::from(maximum_events) {
            return Err(guarded_boundary(
                work,
                Invalid,
                "projection suffix exceeds read allowance",
            ));
        }
        if through
            > operations::tail_optional_with_work(client, &reader.path, work)
                .await?
                .unwrap_or(0)
        {
            return Err(guarded_boundary(
                work,
                Invalid,
                "history cursor exceeds committed tail",
            ));
        }
        let mut records =
            operations::read_records(client, &reader.path, start, (through - start) as u32, work)
                .await?
                .map_err(|error| crate::store::operations::stream_error_with_work(error, work))?;
        let mut used = 0_u64;
        let mut output_bytes = 0_u64;
        loop {
            let mut quota = CodecQuota::from_retained(work, 0);
            if let Some(io) = quota.work().as_deref() {
                io.check_cancellation()?;
            }
            let Some(record) = records.next(quota.work()).await? else {
                break;
            };
            let raw = operations::sdk_record_retained_bytes(&record, quota.work());
            quota.adopt(raw)?;
            if reducer.revision() >= through || record.sequence != reducer.revision() {
                return Err(guarded_boundary(
                    quota.work(),
                    Storage,
                    "history page is not bounded and contiguous",
                ));
            }
            output_bytes = output_bytes
                .checked_add(record.value.len() as u64)
                .ok_or_else(|| {
                    guarded_boundary(
                        quota.work(),
                        Invalid,
                        "projection suffix exceeds byte allowance",
                    )
                })?;
            used = used
                .checked_add(record.value.len() as u64)
                .filter(|total| *total <= maximum_bytes)
                .ok_or_else(|| {
                    guarded_boundary(
                        quota.work(),
                        Invalid,
                        "projection suffix exceeds byte allowance",
                    )
                })?;
            let (event, held) = history::verify_history_record_with_retained_quota(
                &reader.verifier,
                reducer.authority(),
                reducer.revision(),
                &record,
                quota.work(),
            )?;
            quota.adopt(held)?;
            if let Some(io) = quota.work().as_deref_mut() {
                io.admit_output(output_bytes, reducer.revision() - start + 1)?;
            }
            let (indexed, bytes) = effects::retirement_proof_with_work(
                client,
                reducer.authority(),
                &event,
                &record,
                maximum_bytes.saturating_sub(used),
                quota.work(),
            )
            .await?;
            used = used
                .checked_add(bytes)
                .filter(|total| *total <= maximum_bytes)
                .ok_or_else(|| {
                    guarded_boundary(
                        quota.work(),
                        Invalid,
                        "projection suffix exceeds byte allowance",
                    )
                })?;
            let applied = reducer.apply_committed(event, quota.work())?;
            if indexed {
                let ApplyResult::Applied { event } = &applied else {
                    return Err(guarded_boundary(
                        quota.work(),
                        Storage,
                        "projection suffix replayed its successor",
                    ));
                };
                reducer.mark_indexed_terminal_effect_with_work(event, quota.work())?;
            }
            reducer.enforce_terminal_effect_limit()?;
            drop(applied);
            drop(record);
        }
        if reducer.revision() != through {
            return Err(guarded_boundary(
                work,
                Storage,
                "history page stopped before its pinned boundary",
            ));
        }
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
            io.stage_output(output_bytes, through - start)?;
        }
        Ok(())
    }
}

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
        if maximum_bytes == 0 {
            return Err(Error::Invalid(
                "projection suffix exceeds byte allowance".into(),
            ));
        }
        let cursor = HistoryCursor {
            authority: match work.as_deref_mut() {
                Some(work) => work.clone_authority(reducer.authority())?,
                None => reducer.authority().clone(),
            },
            after_revision: reducer.revision(),
            through_revision: through,
        };
        let (page, records, mut used, events_retained_bytes) = reader
            .read_page_with_work(
                &cursor,
                HistoryReadLimits {
                    maximum_events,
                    maximum_bytes,
                },
                true,
                work.as_mut()
                    .map(|observer| &mut **observer as &mut dyn history::HistoryIo),
            )
            .await?;
        let cursor_bytes = cursor.authority.id.capacity() as u64;
        drop(cursor);
        if let Some(work) = work.as_deref_mut() {
            work.release_owned(cursor_bytes)?;
        }
        if page.cursor.after_revision != through {
            return Err(Error::Invalid(
                "projection suffix exceeds byte allowance".into(),
            ));
        }
        let event_bytes =
            (page.events.capacity() * std::mem::size_of::<crate::core::Event>()) as u64;
        let record_capacity =
            (records.capacity() * std::mem::size_of::<operations::ObservedRecord>()) as u64;
        let page_cursor_bytes = page.cursor.authority.id.capacity() as u64;
        drop(page.cursor);
        if let Some(work) = work.as_deref_mut() {
            work.release_owned(page_cursor_bytes)?;
        }
        for (event, record) in page.events.into_iter().zip(records) {
            let (indexed, bytes) = effects::retirement_proof_with_work(
                client,
                reducer.authority(),
                &event,
                &record,
                maximum_bytes.saturating_sub(used),
                work,
            )
            .await?;
            used = used
                .checked_add(bytes)
                .filter(|total| *total <= maximum_bytes)
                .ok_or_else(|| Error::Invalid("projection suffix exceeds byte allowance".into()))?;
            let (cloned, clone_owned) = crate::core::native_event_clone_with_custody(&event, work)?;
            drop(reducer.apply_committed(cloned, work)?);
            if let Some(work) = work.as_deref_mut() {
                work.release_owned(clone_owned)?;
            }
            if indexed {
                reducer.mark_indexed_terminal_effect_with_work(&event, work)?;
            }
            reducer.enforce_terminal_effect_limit()?;
            drop(event);
            let record_bytes = operations::sdk_record_retained_bytes(&record, work);
            drop(record);
            if let Some(work) = work.as_deref_mut() {
                work.release_owned(record_bytes)?;
            }
        }
        if let Some(work) = work.as_deref_mut() {
            work.release_owned(events_retained_bytes)?;
            work.release_owned(event_bytes)?;
            work.release_owned(record_capacity)?;
            work.check_cancellation()?;
        }
        Ok(())
    }
}

use super::*;
use futures::TryStreamExt as _;

impl<P: StreamProvider> StreamAggregate<P> {
    /// Reads an effect at the current cut and returns its retained bytes.
    pub async fn effect(
        &self,
        effect: crate::EffectId,
        limits: HistoryReadLimits,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<Option<(crate::core::EffectState, u64)>> {
        if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work
                .as_ref()
                .is_some_and(|work| work.original_native_work().is_some())
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "effect read bounds must be positive",
                });
            }
            return Err(Error::Invalid("effect read bounds must be positive".into()));
        }
        if let Some(state) = self.reducer.effect(effect) {
            crate::contract::validate_trusted_json_byte_bound(state, limits.maximum_bytes, work)?;
            return crate::core::native_effect_clone_with_custody(state, work).map(Some);
        }
        let reader = HistoryReader::new(
            &self.client,
            self.reducer.authority(),
            self.reducer.event_verifier(),
            work,
        )?;
        let reader_bytes = if work.is_some() {
            reader.logical_owned_bytes(work)?
        } else {
            0
        };
        let authority = match work.as_deref_mut() {
            Some(observer) => observer.clone_authority(self.reducer.authority()),
            None => Ok(self.reducer.authority().clone()),
        };
        let authority = match authority {
            Ok(authority) => authority,
            Err(error) => {
                drop(reader);
                if let Some(observer) = work.as_deref_mut() {
                    let _ = observer.release_owned(reader_bytes);
                }
                return Err(error);
            }
        };
        let cursor = HistoryCursor {
            authority,
            after_revision: 0,
            through_revision: self.reducer.revision(),
        };
        let cursor_bytes = if work.is_some() {
            cursor.authority.id.capacity() as u64
        } else {
            0
        };
        let result = reader
            .effect(
                &cursor,
                effect,
                limits,
                work.as_mut()
                    .map(|observer| &mut **observer as &mut dyn history::HistoryIo),
            )
            .await;
        drop(cursor);
        drop(reader);
        let released = match work.as_deref_mut() {
            Some(observer) => observer.release_owned(cursor_bytes + reader_bytes),
            None => Ok(()),
        };
        match result {
            Ok(state) => {
                released?;
                Ok(state)
            }
            Err(error) => {
                let _ = released;
                Err(error)
            }
        }
    }

    /// Resolves one archived operation through its atomically published canonical location.
    pub async fn operation_event(
        &self,
        operation: OperationId,
    ) -> Result<Option<crate::core::Event>> {
        operations::find_operation_bounded_with_work(
            &self.client,
            self.reducer.authority(),
            self.reducer.event_verifier(),
            operation,
            2 * acyclic_stream::MAX_RECORD_BYTES as u64,
            true,
            &mut None,
        )
        .await
        .map(|(event, _)| event)
    }

    /// Exact committed model selection, including operations archived from resident state.
    pub async fn context_selection_for_operation(
        &self,
        operation: OperationId,
    ) -> Result<Option<crate::conversation::ModelContextSelection>> {
        Ok(self
            .operation_event(operation)
            .await?
            .and_then(|event| match event.payload {
                EventPayload::ModelContextSelected { selection } => Some(selection),
                _ => None,
            }))
    }

    pub(super) async fn replay(
        &mut self,
        command: &Command,
        return_event: bool,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<Option<(ApplyResult, u64)>> {
        let mut owned = crate::wire_codec::CodecQuota::from_retained(work, 0);
        self.reducer
            .verify_command_scope_with_work(command, owned.work())?;
        let (event, _) = operations::bounded_operation_with_retained_quota(
            &self.client,
            self.reducer.authority(),
            self.reducer.event_verifier(),
            command.operation_id,
            maximum_bytes,
            return_event,
            owned.work(),
        )
        .await?;
        let Some((event, original_owned)) = event else {
            if self
                .reducer
                .operation_revision(command.operation_id)
                .is_some()
            {
                return Err(guarded_boundary(
                    owned.work(),
                    crate::wire::ErrorCode::Storage,
                    "resident operation has no atomic canonical location",
                ));
            }
            return Ok(None);
        };
        owned.adopt(original_owned)?;
        self.reducer
            .verify_retained_command_with_work(command, &event, owned.work())?;
        if event.revision > self.reducer.revision() {
            if event.revision != crate::contract::next_revision(self.reducer.revision())? {
                return Err(guarded_boundary(
                    owned.work(),
                    crate::wire::ErrorCode::Conflict,
                    "aggregate must refresh to the indexed operation boundary",
                ));
            }
            let indexed = effects::retirement_proof_for_event_with_work(
                &self.client,
                self.reducer.authority(),
                &event,
                owned.work(),
            )
            .await?;
            let (cloned, returned_owned) =
                crate::core::native_event_clone_with_custody(&event, owned.work())?;
            owned.adopt(returned_owned)?;
            let result = self.reducer.apply_committed(cloned, owned.work())?;
            if indexed {
                self.reducer
                    .mark_indexed_terminal_effect_with_work(&event, owned.work())?;
            }
            self.reducer.enforce_terminal_effect_limit()?;
            drop(event);
            owned.relinquish(original_owned)?;
            owned.transfer();
            return Ok(Some((result, returned_owned)));
        }
        owned.transfer();
        Ok(Some((ApplyResult::Replayed { event }, original_owned)))
    }

    /// Opens an archival reader without replaying or cloning the resident event history.
    pub fn history_reader(&self) -> Result<HistoryReader<P>> {
        HistoryReader::new(
            &self.client,
            self.reducer.authority(),
            self.reducer.event_verifier(),
            &mut None,
        )
    }

    /// Resolves one attested event within this projection's committed boundary.
    pub async fn read_event_at(
        &self,
        revision: u64,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<(crate::core::Event, u64)> {
        let after_revision = revision.checked_sub(1).ok_or_else(|| {
            guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "event revision must be positive",
            )
        })?;
        let mut owned = crate::wire_codec::CodecQuota::from_retained(work, 0);
        let cursor = HistoryCursor {
            authority: match owned.work().as_deref_mut() {
                Some(work) => work.clone_authority(self.reducer.authority())?,
                None => self.reducer.authority().clone(),
            },
            after_revision,
            through_revision: self.reducer.revision(),
        };
        let cursor_bytes = if owned.work().is_some() {
            cursor.authority.id.capacity() as u64
        } else {
            0
        };
        owned.adopt(cursor_bytes)?;
        let reader = HistoryReader::new(
            &self.client,
            self.reducer.authority(),
            self.reducer.event_verifier(),
            owned.work(),
        )?;
        if owned.work().is_some() {
            let reader_bytes = reader.logical_owned_bytes(owned.work())?;
            owned.adopt(reader_bytes)?;
        }
        let (page, _, _, retained) = reader
            .read_page_with_work(
                &cursor,
                HistoryReadLimits {
                    maximum_events: 1,
                    maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                },
                false,
                owned
                    .work()
                    .as_mut()
                    .map(|work| &mut **work as &mut dyn history::HistoryIo),
            )
            .await?;
        if owned.work().is_some() {
            owned.adopt(
                retained
                    + page.cursor.authority.id.capacity() as u64
                    + (page.events.capacity() * std::mem::size_of::<crate::core::Event>()) as u64,
            )?;
        }
        let event = page.events.into_iter().next();
        drop(page.cursor);
        drop(cursor);
        drop(reader);
        let event = event.ok_or_else(|| {
            guarded_boundary(
                owned.work(),
                crate::wire::ErrorCode::NotFound,
                "committed event is unavailable",
            )
        })?;
        let temporary = owned.retained() - retained;
        owned.relinquish(temporary)?;
        owned.transfer();
        Ok((event, retained))
    }

    /// Captures the authoritative tail for a finite incremental refresh.
    pub async fn tail_revision(&self) -> Result<u64> {
        Ok(self.client.tail(self.path.clone()).await?)
    }

    /// Advances the existing verified projection by at most one caller-sized page.
    pub async fn refresh_through(
        &mut self,
        through_revision: u64,
        maximum_events: u32,
    ) -> Result<bool> {
        let current = self.reducer.revision();
        if maximum_events == 0 || through_revision < current {
            return Err(Error::Invalid("aggregate refresh cursor is invalid".into()));
        }
        if through_revision == current {
            return Ok(true);
        }
        if through_revision > self.client.tail(self.path.clone()).await? {
            return Err(Error::Invalid(
                "aggregate refresh exceeds committed tail".into(),
            ));
        }
        let count = u32::try_from((through_revision - current).min(u64::from(maximum_events)))
            .map_err(|_| Error::Invalid("aggregate refresh page exceeds platform size".into()))?;
        let mut records = self
            .client
            .read(acyclic_stream::ReadRequest {
                path: self.path.clone(),
                from: current,
                limit: count,
            })
            .await?;
        let mut consumed = 0;
        while let Some(record) = records.try_next().await? {
            if consumed >= count || record.sequence != self.reducer.revision() {
                return Err(Error::Storage(
                    "aggregate refresh page is not contiguous".into(),
                ));
            }
            let ((authority, event), _) = decode_event(&record.value, &mut None)?;
            if &authority != self.reducer.authority()
                || event.revision
                    != record.sequence.checked_add(1).ok_or_else(|| {
                        Error::Storage("aggregate refresh revision overflows".into())
                    })?
            {
                return Err(Error::Storage(
                    "aggregate refresh event binding is invalid".into(),
                ));
            }
            let (indexed, _) = effects::retirement_proof_with_work(
                &self.client,
                self.reducer.authority(),
                &event,
                &record,
                2 * acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut None,
            )
            .await?;
            self.reducer.apply_committed(event.clone(), &mut None)?;
            if indexed {
                self.reducer
                    .mark_indexed_terminal_effect_with_work(&event, &mut None)?;
            }
            self.reducer.enforce_terminal_effect_limit()?;
            consumed += 1;
        }
        if consumed == 0 {
            return Err(Error::Storage(
                "aggregate refresh stopped before captured tail".into(),
            ));
        }
        Ok(self.reducer.revision() == through_revision)
    }
}

use super::*;

impl<P, A, O> FilesystemWorkflowJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    #[doc(hidden)]
    pub async fn observe_latest_record(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<Option<WorkflowRecord>> {
        let owner = self.owner.as_ref().ok_or_else(|| {
            observation_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "task outbox observation requires its original owner",
            )
        })?;
        self.verify_observation_owner(owner, true, io).await?;
        let (_path, stream) = Self::observation_stream(&self.stream, &self.path, io)?;
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history.as_mut().map(|io| io as &mut dyn HistoryIo);
        let bounds = match crate::store::operations::bounds_with_work(
            &self.stream,
            stream.path(),
            &mut work,
        )
        .await?
        {
            Ok(bounds) => bounds,
            Err(StreamError::NotFound) => {
                if let Some(io) = io {
                    io.check_cancellation()?;
                }
                return Ok(None);
            }
            Err(error) => return Err(Self::observation_stream_error(io, error)),
        };
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        if bounds.tail == 0 {
            return Ok(None);
        }
        if self.read_admission(io).await?.is_none() {
            return Ok(None);
        }
        let record = self
            .replay_records(bounds.tail - 1, 1, true, false, io)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| {
                observation_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "original outbox record is absent",
                )
            })?;
        self.verify_observation_owner(owner, true, io).await?;
        Ok(Some(record))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn prove_release_with_io(
        &self,
        io: &PreparationIo,
    ) -> Result<Option<WorkflowReleaseWitness<'_>>> {
        let Some(record) = self.observe_latest_record(Some(io)).await? else {
            return Ok(None);
        };
        if !matches!(record.transition.status, MachineStatus::Suspended) {
            return Ok(None);
        }
        let owner = self
            .owner
            .as_ref()
            .ok_or_else(|| io.invalid("observed outbox has no original owner"))?;
        Ok(Some(WorkflowReleaseWitness {
            binding: owner.task_binding(),
            path: &self.path,
            tail: record.checkpoint()?.1,
        }))
    }

    pub(super) async fn verify_observation_owner(
        &self,
        owner: &TaskJournalOwner<P>,
        allow_terminal: bool,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|history| history as &mut dyn crate::store::history::HistoryIo);
        if allow_terminal {
            owner.verify_read_access(&mut work).await
        } else {
            owner.verify(false, &mut work).await
        }
    }

    pub(super) fn observation_stream<'a>(
        stream: &std::sync::Arc<P>,
        path: &str,
        io: Option<&'a PreparationIo>,
    ) -> Result<(
        Option<super::super::DecodeReservation<'a>>,
        acyclic_stream::Stream<P>,
    )> {
        let held = if let Some(io) = io {
            let work = acyclic_fs::WorkCounters {
                bytes_copied: path.len() as u64,
                items_examined: path.len() as u64,
                ..Default::default()
            };
            io.preflight(work)?;
            let hold = io.reserve_decode(path.len() as u64)?;
            io.allocation_attempt(path.len() as u64)?;
            io.charge(work)?;
            Some(hold)
        } else {
            None
        };
        Ok((
            held,
            acyclic_stream::Stream::new(std::sync::Arc::clone(stream), path)
                .map_err(|error| Self::observation_stream_error(io, error))?,
        ))
    }

    pub(super) fn observation_stream_error(
        _native_io: Option<&PreparationIo>,
        source: StreamError,
    ) -> Error {
        #[cfg(not(target_arch = "wasm32"))]
        if _native_io
            .and_then(PreparationIo::original_native_work)
            .is_some()
        {
            return Error::NativeStream {
                code: crate::wire::ErrorCode::Storage,
                source,
                custody: None,
            };
        }
        Error::Storage(source.to_string())
    }

    pub(super) async fn next_observation_record<'a>(
        entries: &mut crate::store::operations::ObservedRecords,
        io: Option<&'a PreparationIo>,
    ) -> Result<
        Option<(
            Option<super::super::DecodeReservation<'a>>,
            crate::store::operations::ObservedRecord,
        )>,
    > {
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history.as_mut().map(|io| io as &mut dyn HistoryIo);
        let (held, record) = {
            let Some(record) = entries.next(&mut work).await? else {
                if let Some(io) = io {
                    io.check_cancellation()?;
                }
                return Ok(None);
            };
            let bytes = crate::store::operations::sdk_record_retained_bytes(&record, &work);
            let held = io
                .filter(|_| bytes != 0)
                .map(|io| io.guard_owned(bytes))
                .transpose()?;
            (held, record)
        };
        if record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(observation_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "workflow Stream record exceeds protocol bound",
            ));
        }
        Ok(Some((held, record)))
    }

    pub(super) async fn read_bound_content(
        &self,
        reference: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<OwnedBytes> {
        if let Some(io) = io {
            if io.original_native_work().is_none() {
                reference.validate(&mut Some(&mut io.history_io()))?;
            }
            if reference.volume() != &self.volume {
                return Err(observation_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unauthorized,
                    "workflow content belongs to another private volume",
                ));
            }
            ContentGrant::authorize_volume(
                &self.verifier,
                &self.scope,
                &self.volume,
                VolumeOperation::Read,
                io.remaining()?.bytes_encoded,
                &mut Some(&mut io.history_io()),
            )?;
            self.host
                .read_pinned_io(reference, self.maximum_payload_bytes, Some(io))
                .await
        } else {
            let read = ContentGrant::verify(
                &self.verifier,
                &self.scope,
                &self.volume,
                VolumeOperation::Read,
                u64::MAX,
                &mut None,
            )?;
            self.host
                .read_content(reference, &read, self.maximum_payload_bytes, None)
                .await
        }
    }
    pub(super) async fn verified_history(
        &self,
        admission: &WorkflowAdmission,
        candidate: &WorkflowRecord,
        require_next: bool,
        io: Option<&PreparationIo>,
    ) -> Result<(u64, Option<WorkflowRecord>)> {
        let operation_id = candidate.operation_id;
        let mut identities = WorkflowIdentities {
            reserved: BTreeSet::new(),
            keys: BTreeSet::new(),
            hold: io.map(|io| io.guard_owned(0)).transpose()?,
        };
        let mut previous_page_owned = 0;
        let mut matched_page_owned = 0;
        let mut previous: Option<WorkflowRecord> = None;
        let mut matched = None;
        loop {
            let revision = previous
                .as_ref()
                .map(WorkflowRecord::checkpoint)
                .transpose()?
                .map_or(0, |(_, revision, _)| revision);
            let before_page = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let page = self
                .replay_records(revision, WORKFLOW_REPLAY_PAGE_RECORDS, false, true, io)
                .await?;
            let page_owned = io
                .map(|io| {
                    io.owned_bytes()?
                        .checked_sub(before_page)
                        .ok_or_else(|| Error::Invalid("workflow replay ownership decreased".into()))
                })
                .transpose()?
                .unwrap_or(0);
            if let Some(hold) = &mut identities.hold {
                hold.adopt(page_owned)?;
            }
            let mut page_has_match = false;
            if page.is_empty() {
                break;
            }
            for record in page {
                let checkpoint = previous
                    .as_ref()
                    .map(WorkflowRecord::checkpoint)
                    .transpose()?
                    .unwrap_or((
                        &admission.initial.machine,
                        admission.initial.revision,
                        &admission.initial.state,
                    ));
                let terminal = previous.as_ref().is_some_and(|prior| {
                    !matches!(prior.transition.status, MachineStatus::Suspended)
                });
                validate_workflow_next(
                    checkpoint,
                    terminal,
                    &identities.reserved,
                    &identities.keys,
                    &record,
                    io,
                )
                .map_err(|error| {
                    if error.is_native_failure() {
                        error
                    } else {
                        Error::Storage(
                            "workflow history diverges from admission or reuses an identity".into(),
                        )
                    }
                })?;
                identities.advance(&record)?;
                if let Some(prior) = previous.take() {
                    if prior.operation_id == operation_id {
                        matched = Some(prior);
                        if previous_page_owned != 0 {
                            matched_page_owned = previous_page_owned;
                        } else {
                            page_has_match = true;
                            matched_page_owned = page_owned;
                        }
                    } else {
                        drop(prior);
                        if previous_page_owned != 0
                            && let Some(hold) = &mut identities.hold
                        {
                            hold.substitute(
                                hold.bytes()
                                    .checked_sub(previous_page_owned)
                                    .ok_or_else(|| {
                                        hold.io().invalid("workflow replay ownership underflow")
                                    })?,
                            )?;
                        }
                    }
                    previous_page_owned = 0;
                }
                previous = Some(record);
            }
            previous_page_owned = if page_has_match { 0 } else { page_owned };
        }
        let revision = previous
            .as_ref()
            .map(WorkflowRecord::checkpoint)
            .transpose()?
            .map_or(0, |(_, revision, _)| revision);
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        for identity in candidate.operation_ids() {
            if let Some(io) = io {
                io.charge(acyclic_fs::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
            }
            if identities.reserved.contains(&identity)
                && matched.is_none()
                && previous
                    .as_ref()
                    .is_none_or(|prior| prior.operation_id != operation_id)
            {
                return Err(Error::Conflict(
                    "workflow command identity is already bound".into(),
                ));
            }
        }
        if require_next && revision == candidate.prior.revision {
            let checkpoint = previous
                .as_ref()
                .map(WorkflowRecord::checkpoint)
                .transpose()?
                .unwrap_or((
                    &admission.initial.machine,
                    admission.initial.revision,
                    &admission.initial.state,
                ));
            let terminal = previous
                .as_ref()
                .is_some_and(|prior| !matches!(prior.transition.status, MachineStatus::Suspended));
            validate_workflow_next(
                checkpoint,
                terminal,
                &identities.reserved,
                &identities.keys,
                candidate,
                io,
            )?;
        }
        if let Some(prior) = previous
            && prior.operation_id == operation_id
        {
            if previous_page_owned != 0 {
                matched_page_owned = previous_page_owned;
            }
            matched = Some(prior);
        }
        if let Some(hold) = &mut identities.hold {
            hold.retain_part(matched_page_owned)?;
        }
        Ok((revision, matched))
    }
}

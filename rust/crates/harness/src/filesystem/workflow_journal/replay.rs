use super::*;

impl<P, A, O> FilesystemWorkflowJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn read_record(
        &self,
        reference: &FileRef,
        verify_payloads: bool,
        verify_machine: bool,
        io: Option<&PreparationIo>,
    ) -> Result<WorkflowRecord> {
        let _prefix;
        let prefix = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(
                format_args!(".system/workflows/{}/", self.id()),
                work,
            )
        })?;
        _prefix = io
            .map(|io| io.guard_owned(prefix.capacity() as u64))
            .transpose()?;
        if reference.volume() != &self.volume
            || reference.descriptor().media_type() != "application/json"
            || !reference.path().starts_with(&prefix)
        {
            return Err(observation_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "workflow record belongs to another private volume",
            ));
        }
        let _bytes;
        let bytes = self.read_bound_content(reference, io).await?;
        _bytes = external_read_hold(&bytes, io)?;
        let record: WorkflowRecord = match io {
            Some(io) => crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData::<WorkflowRecord>,
                &bytes,
                crate::wire::ErrorCode::Storage,
            )?),
            None => crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Storage(format!("workflow record is invalid: {error}")))?,
        };
        record.validate(io)?;
        if let Some(owner) = &self.owner {
            if verify_machine {
                owner.validate_workflow_record(&record, io)?;
            } else if record.prior.machine != owner.workflow_admission().initial.machine {
                return Err(observation_boundary(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "observed workflow changed its pinned machine",
                ));
            }
        }
        if verify_payloads {
            self.verify_command_payloads(&record, io).await?;
        }
        Ok(record)
    }

    pub(super) fn require_machine_io(&self, io: Option<&PreparationIo>) -> Result<()> {
        if io.is_some() {
            #[cfg(not(target_arch = "wasm32"))]
            if io.and_then(PreparationIo::original_native_work).is_some() {
                return self
                    .owner
                    .as_ref()
                    .map_or(Ok(()), |owner| owner.require_native_workflow_validation());
            }
            return Err(observation_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "finite workflow replay requires a native machine callback",
            ));
        }
        Ok(())
    }

    pub(super) async fn replay_records(
        &self,
        after: u64,
        maximum: u32,
        verify_payloads: bool,
        verify_machine: bool,
        io: Option<&PreparationIo>,
    ) -> Result<Vec<WorkflowRecord>> {
        if verify_machine {
            self.require_machine_io(io)?;
        }
        if maximum == 0 {
            return Err(observation_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "workflow page bounds are invalid",
            ));
        }
        let _path;
        let (stream, held) = Self::observation_stream(&self.stream, &self.path, io)?;
        _path = held;
        if let Some(io) = io {
            io.history_io().begin_call()?;
        }
        let _request_path = Self::retain_observation_request_path(&self.path, io)?;
        let entries = match stream
            .read(after, maximum.min(WORKFLOW_REPLAY_PAGE_RECORDS))
            .await
        {
            Ok(entries) => entries,
            Err(StreamError::NotFound) => {
                if let Some(io) = io {
                    io.check_cancellation()?;
                }
                return Ok(Vec::new());
            }
            Err(error) => return Err(Self::observation_stream_error(io, error)),
        };
        let mut entries = entries;
        let capacity = maximum.min(WORKFLOW_REPLAY_PAGE_RECORDS) as usize;
        let vector_bytes = capacity
            .checked_mul(std::mem::size_of::<WorkflowRecord>())
            .ok_or_else(|| {
                observation_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "workflow replay vector ownership overflow",
                )
            })? as u64;
        let vector_hold = if let Some(io) = io {
            io.retain_owned(vector_bytes)?;
            let hold = io.guard_owned(vector_bytes)?;
            io.allocation_attempt(vector_bytes)?;
            Some(hold)
        } else {
            None
        };
        let mut records = Vec::with_capacity(capacity);
        loop {
            let _entry;
            let Some((entry, held)) = Self::next_observation_record(&mut entries, io).await? else {
                break;
            };
            _entry = held;
            let sequence = after + records.len() as u64;
            if records.len() >= capacity || entry.sequence != sequence {
                return Err(observation_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "workflow sequence or page bound is invalid",
                ));
            }
            let reference_hold;
            let reference_before = io.map(PreparationIo::owned_bytes).transpose()?;
            let reference: FileRef = match io {
                Some(io) => crate::filesystem::retain_decoded(io.decode(
                    std::marker::PhantomData::<FileRef>,
                    &entry.value,
                    crate::wire::ErrorCode::Storage,
                )?),
                None => crate::contract::json_from_slice(&entry.value).map_err(|error| {
                    Error::Storage(format!("workflow reference is invalid: {error}"))
                })?,
            };
            reference_hold = match (io, reference_before) {
                (Some(io), Some(before)) => Some(io.guard_owned(
                    io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                        Error::Invalid("workflow reference custody underflow".into())
                    })?,
                )?),
                _ => None,
            };
            let record = self
                .read_record(&reference, verify_payloads, verify_machine, io)
                .await?;
            if record.prior.revision != sequence {
                return Err(observation_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "workflow checkpoint revision is discontinuous",
                ));
            }
            records.push(record);
            drop(reference);
            drop(reference_hold);
        }
        if let Some(hold) = vector_hold {
            hold.into_retained();
        }
        Ok(records)
    }

    /// Reads one committed transition for passive observation.
    pub(crate) async fn observe_transition(
        &self,
        revision: u64,
        io: Option<&PreparationIo>,
    ) -> Result<WorkflowRecord> {
        let after = revision
            .checked_sub(1)
            .ok_or_else(|| Error::Invalid("zero observation revision".into()))?;
        self.replay_records(after, 1, false, false, io)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Storage("observed workflow checkpoint is absent".into()))
    }
}

use super::*;

impl<P, A, O> FilesystemWorkflowJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(crate) async fn has_transition_with_io(&self, io: Option<&PreparationIo>) -> Result<bool> {
        let (_path, stream) = Self::observation_stream(&self.stream, &self.path, io)?;
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history.as_mut().map(|io| io as &mut dyn HistoryIo);
        let mut entries = match crate::store::operations::read_records(
            &self.stream,
            stream.path(),
            0,
            1,
            &mut work,
        )
        .await?
        {
            Ok(entries) => entries,
            Err(StreamError::NotFound) => {
                if let Some(io) = io {
                    io.check_cancellation()?;
                }
                return Ok(false);
            }
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        let Some((_entry, entry)) = Self::next_observation_record(&mut entries, io).await? else {
            return Ok(false);
        };
        if entry.sequence != 0
            || Self::next_observation_record(&mut entries, io)
                .await?
                .is_some()
        {
            return Err(Error::Storage(
                "workflow transition stream is invalid".into(),
            ));
        }
        Ok(true)
    }

    pub(crate) async fn read_admission(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<Option<WorkflowAdmission>> {
        if let Some(owner) = &self.owner {
            self.verify_observation_owner(owner, true, io).await?;
            let admission = owner.workflow_admission().copy(io)?;
            admission.validate(io)?;
            return Ok(Some(admission));
        }
        let reject = |code, reason| observation_boundary(io, code, reason);
        let (_path, path) = {
            let path = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(
                    format_args!("harness/v2/workflow-admissions/{}", self.id()),
                    work,
                )
            })?;
            let held = io
                .map(|io| io.guard_owned(path.capacity() as u64))
                .transpose()?;
            (held, path)
        };
        let (_stream_path, stream) = Self::observation_stream(&self.stream, &path, io)?;
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history.as_mut().map(|io| io as &mut dyn HistoryIo);
        let mut entries = match crate::store::operations::read_records(
            &self.stream,
            stream.path(),
            0,
            2,
            &mut work,
        )
        .await?
        {
            Ok(entries) => entries,
            Err(StreamError::NotFound) => {
                if let Some(io) = io {
                    io.check_cancellation()?;
                }
                return Ok(None);
            }
            Err(error) => return Err(Self::observation_stream_error(io, error)),
        };
        let Some((_entry, entry)) = Self::next_observation_record(&mut entries, io).await? else {
            return Ok(None);
        };
        if Self::next_observation_record(&mut entries, io)
            .await?
            .is_some()
        {
            return Err(reject(
                crate::wire::ErrorCode::Storage,
                "workflow admission stream is invalid",
            ));
        }
        if entry.sequence != 0 {
            return Err(reject(
                crate::wire::ErrorCode::Storage,
                "workflow admission sequence is invalid",
            ));
        }
        let (_reference_hold, reference) = {
            let reference_before = io.map(PreparationIo::owned_bytes).transpose()?;
            let reference: FileRef = crate::filesystem::decode_record(&entry.value, io)?;
            let held = match (io, reference_before) {
                (Some(io), Some(before)) => Some(io.guard_owned(io.owned_since(before)?)?),
                _ => None,
            };
            (held, reference)
        };
        let (_expected_path, expected_path) = {
            let expected_path = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(
                    format_args!(".system/workflows/{}/admission.json", self.id()),
                    work,
                )
            })?;
            let held = io
                .map(|io| io.guard_owned(expected_path.capacity() as u64))
                .transpose()?;
            (held, expected_path)
        };
        if reference.volume() != &self.volume
            || reference.path() != expected_path
            || reference.descriptor().media_type() != "application/json"
        {
            return Err(reject(
                crate::wire::ErrorCode::Unauthorized,
                "workflow admission belongs to another volume",
            ));
        }
        let _bytes;
        let bytes = self.read_bound_content(&reference, io).await?;
        _bytes = external_read_hold(&bytes, io)?;
        let admission: WorkflowAdmission = crate::filesystem::decode_record(&bytes, io)?;
        admission.validate(io)?;
        Ok(Some(admission))
    }

    pub(crate) async fn admit_with_io(
        &self,
        admission: WorkflowAdmission,
        io: Option<&PreparationIo>,
    ) -> Result<WorkflowAdmission> {
        admission.validate(io)?;
        if let Some(owner) = &self.owner {
            if &admission != owner.workflow_admission() {
                return Err(Error::Unauthorized(
                    "workflow differs from retained task admission".into(),
                ));
            }
            self.verify_observation_owner(owner, false, io).await?;
            // Task admission already durably retained this exact workflow.
            // Its original owner is the authority for both live and cold reads.
            return Ok(admission);
        }
        if let Some(existing) = self.read_admission(io).await? {
            return if existing == admission {
                Ok(existing)
            } else {
                Err(Error::Conflict(
                    "workflow admission identity is already bound".into(),
                ))
            };
        }
        if self.has_transition_with_io(io).await? {
            return Err(Error::Conflict(
                "workflow transitions exist without admission".into(),
            ));
        }
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let (_reference, reference) = {
            let reference = self.stage_payload(&admission, None, io).await?;
            let held = io
                .map(|io| io.guard_owned(io.owned_since(before)?))
                .transpose()?;
            (held, reference)
        };
        {
            let (_pinned_bytes, pinned_bytes) = {
                let pinned_bytes = self.read_bound_content(&reference, io).await?;
                let held = external_read_hold(&pinned_bytes, io)?;
                (held, pinned_bytes)
            };
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let (_pinned, pinned) = {
                let pinned: WorkflowAdmission =
                    crate::filesystem::decode_record(&pinned_bytes, io)?;
                let held = io
                    .map(|io| io.guard_owned(io.owned_since(before)?))
                    .transpose()?;
                (held, pinned)
            };
            pinned.validate(io)?;
            if pinned != admission {
                return Err(Error::Conflict(
                    "staged workflow admission disagrees with its identity".into(),
                ));
            }
        }
        let (_path, path) = {
            let path = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(
                    format_args!("harness/v2/workflow-admissions/{}", self.id()),
                    work,
                )
            })?;
            let held = io
                .map(|io| io.guard_owned(path.capacity() as u64))
                .transpose()?;
            (held, path)
        };
        let appended = self
            .append_reference(
                &path,
                "admission",
                0,
                admission.operation_id,
                &reference,
                io,
            )
            .await;
        match appended {
            Ok(true) => return Ok(admission),
            Err(error) if self.owner.is_some() => return Err(error),
            Ok(false) | Err(Error::Conflict(_) | Error::Indeterminate(_)) => {}
            Err(error) => return Err(error),
        }
        match self.read_admission(io).await {
            Ok(Some(existing)) if existing == admission => Ok(existing),
            Ok(Some(_)) => Err(Error::Conflict(
                "workflow admission identity is already bound".into(),
            )),
            Ok(None) if self.owner.is_some() => Err(Error::Conflict(
                "workflow admission ownership changed".into(),
            )),
            Ok(None) => Err(Error::Indeterminate(admission.operation_id)),
            Err(Error::Storage(_)) if matches!(appended, Err(Error::Indeterminate(_))) => {
                Err(Error::Indeterminate(admission.operation_id))
            }
            Err(error) => Err(error),
        }
    }
}

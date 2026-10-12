//! Original owner checks and fenced journal publication.
use super::*;

#[cfg(feature = "filesystem")]
impl<P: StreamProvider> TaskJournalOwner<P> {
    pub(crate) const fn input_limits(&self) -> crate::conversation::Limits {
        self.input_limits
    }

    pub(crate) fn validate_input_file(&self, file: &FileRef) -> Result<()> {
        self.input_limits.validate_file(file)?;
        if !read_granted(&self.input_grants, file)? {
            return Err(Error::Unauthorized(
                "task cannot read the execution input file".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_native_configuration(
        &self,
        binding: &crate::model::NativeConfigurationBinding,
    ) -> Result<(&crate::core::SchemaRegistry, [u8; 32])> {
        let (schemas, expected) = binding.verify_original(
            self.extensions.as_ref(),
            self.extension_schemas.as_ref(),
            self.extension_runtime.as_deref(),
        )?;
        let file = &binding.configuration.content;
        self.validate_input_file(file)?;
        if file.descriptor().byte_length() > self.input_limits.render_bytes {
            return Err(Error::Invalid(
                "native options exceed render byte limit".into(),
            ));
        }
        Ok((schemas, expected))
    }

    /// Verifies options against the original authenticated admission and linked code.
    /// This is read authority, so it remains valid during original-fence settlement.
    pub(crate) async fn verify_native_configuration(
        &self,
        binding: &crate::model::NativeConfigurationBinding,
    ) -> Result<()> {
        let (schemas, expected) = self.validate_native_configuration(binding)?;
        let bytes = self
            .host
            .reader
            .read(&binding.configuration.content)
            .await?;
        schemas.verify_configuration_binding(
            &binding.configuration,
            expected,
            &bytes,
            self.input_limits,
        )
    }

    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    pub(crate) fn require_volume_grant(
        &self,
        volume: &crate::conversation::VolumeRef,
        operation: crate::conversation::VolumeOperation,
    ) -> Result<()> {
        let grant = volume.capability(operation)?;
        if !self.input_grants.contains(&grant) {
            return Err(Error::Unauthorized(format!("task scope lacks {grant}")));
        }
        Ok(())
    }

    pub(crate) fn require_interaction_grant(&self) -> Result<()> {
        if !self.input_grants.contains(capability::INTERACTION_ROUTE) {
            return Err(Error::Unauthorized(
                "task scope lacks interaction:route".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn task_binding(&self) -> (TaskId, crate::scheduler::LeaseFence) {
        (self.task_id, self.fence.clone())
    }

    pub(crate) async fn suspend_quiescent_execution(
        &self,
        execution: OperationId,
        tail: u64,
        revision: u64,
        command: OperationId,
    ) -> Result<()> {
        self.host
            .suspend_workflow_command_guarded(
                self.task_id,
                self.fence.clone(),
                revision,
                Some(command),
                Some((execution, tail)),
            )
            .await
    }

    pub(crate) fn validate_storage(
        &self,
        verifier: &AuthorityVerifier,
        maximum_payload_bytes: u64,
    ) -> Result<()> {
        if verifier.audience() != &self.host.owner
            || maximum_payload_bytes > self.maximum_payload_bytes
        {
            return Err(Error::Unauthorized(
                "journal storage exceeds admitted task authority or limits".into(),
            ));
        }
        Ok(())
    }

    /// Returns the initial workflow identity derived from the retained task
    /// admission and its registered machine, rather than caller-selected state.
    #[must_use]
    pub const fn workflow_admission(&self) -> &crate::workflow::WorkflowAdmission {
        &self.workflow
    }

    pub(crate) fn validate_workflow_record(
        &self,
        record: &crate::workflow::WorkflowRecord,
    ) -> Result<()> {
        if record.prior.machine != self.workflow.initial.machine {
            return Err(Error::Conflict(
                "workflow implementation differs from task admission".into(),
            ));
        }
        let (next, transition, _, _) = self.host.machines.step(
            &record.prior,
            std::borrow::Cow::Borrowed(&record.input),
            None,
        )?;
        if record.checkpoint()? != (&next.machine, next.revision, &next.state) || transition != record.transition {
            return Err(Error::Conflict(
                "workflow transition differs from pinned machine".into(),
            ));
        }
        if let crate::workflow::MachineStatus::Completed { value } = &transition.status {
            crate::contract::validate_json_schema_value(
                &self.output_schema,
                value,
                "task output",
                &mut None,
            )?;
        }
        Ok(())
    }

    pub(crate) fn stream(&self) -> Arc<P> {
        self.stream.clone()
    }

    pub(crate) fn operation_id(&self) -> OperationId {
        OperationId::from_bytes(self.task_id.into_bytes())
    }

    pub(super) async fn condition(
        &self,
        write: crate::distributed::JournalWrite,
    ) -> Result<acyclic_stream::CommitCondition> {
        self.host
            .coordinator
            .lock()
            .await
            .journal_condition(
                &self.host.owner,
                &self.host.owner_scope,
                &self.host.verifier,
                self.operation_id(),
                &self.fence,
                write,
            )
            .await
    }

    pub(crate) async fn verify(&self, settlement: bool) -> Result<()> {
        use crate::distributed::JournalWrite;
        self.condition(if settlement {
            JournalWrite::Settlement
        } else {
            JournalWrite::Fresh
        })
        .await
        .map(|_| ())
    }

    pub(super) fn execution_write(
        operation: OperationId,
        event: &crate::executor::ExecutionEvent,
    ) -> crate::distributed::JournalWrite {
        use crate::{distributed::JournalWrite, executor::ExecutionEvent};
        match event {
            ExecutionEvent::ModelStarted {
                step,
                purpose,
                request_digest,
                ..
            } => JournalWrite::Model {
                attempt_id: purpose.attempt_operation(operation),
                step: *step,
                request_digest: *request_digest,
            },
            ExecutionEvent::Started { .. }
            | ExecutionEvent::ContextPrepared { .. }
            | ExecutionEvent::ContextCompacted { .. }
            | ExecutionEvent::ToolStarted { .. } => JournalWrite::Fresh,
            ExecutionEvent::Model { .. }
            | ExecutionEvent::ToolCompleted { .. }
            | ExecutionEvent::ToolFailed { .. } => JournalWrite::Settlement,
        }
    }

    pub(crate) async fn verify_execution(
        &self,
        operation: OperationId,
        event: &crate::executor::ExecutionEvent,
    ) -> Result<()> {
        self.condition(Self::execution_write(operation, event))
            .await
            .map(|_| ())
    }

    pub(crate) async fn verify_model_history(
        &self,
        operation: OperationId,
        records: &[crate::executor::ExecutionRecord],
    ) -> Result<()> {
        self.host
            .verify_model_history(self.task_id, operation, records)
            .await
    }

    pub(crate) fn require_effect_grants(&self, provider: &str, planning: bool) -> Result<()> {
        for grant in [
            capability::EFFECT_RUN.to_owned(),
            capability::effect_provider(provider),
        ] {
            if !self.input_grants.contains(&grant) {
                return Err(Error::Unauthorized(format!("task scope lacks {grant}")));
            }
        }
        if planning && !self.input_grants.contains(capability::EFFECT_PLAN) {
            return Err(Error::Unauthorized("task scope lacks effect:plan".into()));
        }
        Ok(())
    }

    pub(crate) async fn append_conversation(
        &self,
        publication: &crate::store::operations::IndexedPublication,
        key: &StreamKey,
        write: crate::distributed::JournalWrite,
    ) -> Result<bool> {
        let mut request = self
            .prepare_append(
                &publication.path,
                publication.expected_tail,
                key,
                &publication.bytes,
                write,
            )
            .await?;
        publication.add_index(&mut request);
        match crate::distributed::commit_keyed(&self.stream, request, publication.operation_id)
            .await?
        {
            acyclic_stream::CommitOutcome::Conflict(_) => Ok(false),
            acyclic_stream::CommitOutcome::Committed(envelope) => {
                publication
                    .verify(&envelope)
                    .map_err(|_| Error::Indeterminate(publication.operation_id))?;
                Ok(true)
            }
        }
    }

    pub(crate) async fn append_execution(
        &self,
        operation: OperationId,
        expected_tail: u64,
        key: &StreamKey,
        bytes: OwnedBytes,
        event: &crate::executor::ExecutionEvent,
    ) -> Result<bool> {
        let path = crate::distributed::execution_path(operation)?;
        self.append(
            path,
            expected_tail,
            key,
            bytes,
            Self::execution_write(operation, event),
        )
        .await
    }

    pub(crate) async fn append_workflow(
        &self,
        admission: bool,
        expected_tail: u64,
        key: &StreamKey,
        bytes: OwnedBytes,
    ) -> Result<bool> {
        let class = if admission {
            "workflow-admissions"
        } else {
            "workflows"
        };
        let path =
            acyclic_stream::StreamPath::new(format!("harness/v2/{class}/{}", self.operation_id()))?;
        self.append(
            path,
            expected_tail,
            key,
            bytes,
            crate::distributed::JournalWrite::Fresh,
        )
        .await
    }

    pub(super) async fn prepare_append(
        &self,
        path: &acyclic_stream::StreamPath,
        expected_tail: u64,
        key: &StreamKey,
        bytes: &OwnedBytes,
        write: crate::distributed::JournalWrite,
    ) -> Result<acyclic_stream::CommitRequest> {
        use acyclic_stream::{CommitCondition, CommitMutation};
        let owner = self.condition(write).await?;
        let target = match self.stream.bounds(path.as_str()).await {
            Ok(_) => CommitCondition::Tail {
                path: path.clone(),
                expected: expected_tail,
            },
            Err(StreamError::NotFound) if expected_tail == 0 => {
                CommitCondition::Absent { path: path.clone() }
            }
            Err(error) => return Err(error.into()),
        };
        // Conflicting transactions are retained by Stream too. A refreshed owner
        // revision gets its own recovery key; the journal's CAS and retry index
        // still prevent duplicate logical records.
        let mut hash = blake3::Hasher::new();
        hash.update(b"harness/v2/owned-journal\0");
        hash.update(key.as_bytes());
        hash.update(&self.task_id.into_bytes());
        hash.update(&crate::contract::canonical_json_bytes(&self.fence)?);
        let CommitCondition::Tail { expected, .. } = &owner else {
            return Err(Error::Invalid("coordinator tail condition required".into()));
        };
        hash.update(&expected.to_le_bytes());
        hash.update(path.as_str().as_bytes());
        hash.update(&expected_tail.to_le_bytes());
        hash.update(&[u8::from(matches!(target, CommitCondition::Absent { .. }))]);
        hash.update(blake3::hash(bytes).as_bytes());
        let transaction_key =
            StreamKey::new(OwnedBytes::copy_from_slice(hash.finalize().as_bytes()))?;
        Ok(acyclic_stream::CommitRequest {
            conditions: vec![owner, target],
            mutations: vec![CommitMutation::Append {
                path: path.clone(),
                records: vec![bytes.clone()],
            }],
            idempotency_key: transaction_key,
        })
    }

    pub(crate) async fn append(
        &self,
        path: acyclic_stream::StreamPath,
        expected_tail: u64,
        key: &StreamKey,
        bytes: OwnedBytes,
        write: crate::distributed::JournalWrite,
    ) -> Result<bool> {
        use acyclic_stream::{CommitOutcome, CommittedMutation};
        let request = self
            .prepare_append(&path, expected_tail, key, &bytes, write)
            .await?;
        let outcome =
            crate::distributed::commit_keyed(&self.stream, request, self.operation_id()).await?;
        match outcome {
            CommitOutcome::Conflict(_) => Ok(false),
            CommitOutcome::Committed(envelope) => {
                let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
                    return Err(Error::Storage("invalid owned journal envelope".into()));
                };
                if append.path != path
                    || append.start != expected_tail
                    || append.end != expected_tail + 1
                    || append.tail != append.end
                    || append.records.len() != 1
                    || append.records.first().is_none_or(|record| {
                        record.sequence != expected_tail
                            || record.value != bytes
                            || record.commit_id != envelope.commit_id
                    })
                {
                    return Err(Error::Storage("invalid owned journal append".into()));
                }
                Ok(true)
            }
        }
    }
}

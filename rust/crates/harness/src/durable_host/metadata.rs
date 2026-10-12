//! Metadata.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(super) async fn read_json(
        &self,
        reference: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Value, u64)> {
        reference.validate(work)?;
        if reference.descriptor().media_type() != "application/json" {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "durable task payload is not JSON",
            ));
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let (bytes, byte_claim) = match quota.work().as_deref_mut() {
            Some(io) => {
                crate::store::history::read_content_with_work(io, self.reader.as_ref(), reference)
                    .await?
            }
            None => (
                self.reader
                    .read(reference, reference.descriptor().byte_length(), None)?
                    .await?,
                0,
            ),
        };
        quota.adopt(byte_claim)?;
        reference.descriptor().verify(&bytes, quota.work())?;
        let (value, value_claim) = match quota.work().as_deref_mut() {
            Some(io) => io.decode_json_value(&bytes)?,
            None => (
                crate::contract::json_from_slice(&bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                0,
            ),
        };
        quota.adopt(value_claim)?;
        let canonical = crate::store::history::canonical_bytes_with_work(
            &value,
            reference.descriptor().byte_length(),
            quota.work(),
        )?;
        let capacity = canonical.capacity() as u64;
        quota.adopt(capacity)?;
        let matches = canonical.as_slice() == bytes.as_ref();
        drop(canonical);
        quota.relinquish(capacity)?;
        drop(bytes);
        quota.relinquish(byte_claim)?;
        if !matches {
            return Err(admission_boundary(
                quota.work(),
                crate::wire::ErrorCode::Invalid,
                "durable task payload is not canonical JSON",
            ));
        }
        quota.transfer();
        Ok((value, value_claim))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Reads an authenticated admission on its original native ledger.
    pub async fn observe_admission_with_native_work(
        &self,
        task: TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskAdmissionRecord, acyclic_fs::NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work)?;
        let admission = self
            .admission_with_work(
                OperationId::from_bytes(task.into_bytes()),
                &mut Some(&mut io),
            )
            .await?;
        let owned = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
            .checked_add(admission.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "task admission custody overflows",
            })?;
        let custody = io.take_native_custody(owned)?;
        Ok((admission, custody))
    }

    pub(super) async fn stage_payload_with_work(
        &self,
        operation: OperationId,
        key: &str,
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<FileRef> {
        let Some(io) = work.as_deref_mut() else {
            return self.payloads.stage(operation, key, bytes, None)?.await;
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            let original = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "payload staging requires the original native work observer",
            })?;
            let content_io = crate::filesystem::PreparationIo::from_native_work(original)?;
            let reference = self
                .payloads
                .stage(operation, key, bytes, Some(&content_io))?
                .await?;
            let custody = content_io.into_native_custody(reference.logical_owned_bytes()?)?;
            io.absorb_native_custody(custody)?;
            Ok(reference)
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        {
            let _ = io;
            Err(Error::Unsupported(
                "native payload staging is unavailable on this platform".into(),
            ))
        }
    }

    pub(super) async fn retained_declaration_with_work(
        &self,
        operation_id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<OperationSpec>> {
        let mut declaration_key_capacity = 0;
        let declaration_key = if let Some(io) = work.as_deref_mut() {
            const PREFIX: &str = "task-admission:";
            let mut uuid = [0; 36];
            let operation = uuid::Uuid::from_bytes(operation_id.into_bytes())
                .hyphenated()
                .encode_lower(&mut uuid);
            let mut text = String::new();
            io.admit_copy((PREFIX.len() + operation.len()) as u64)?;
            io.reserve_text(&mut text, PREFIX.len() + operation.len())?;
            io.copy_bytes((PREFIX.len() + operation.len()) as u64)?;
            text.push_str(PREFIX);
            text.push_str(operation);
            declaration_key_capacity = text.capacity() as u64;
            IdempotencyKey::new(text)?
        } else {
            IdempotencyKey::new(format!("task-admission:{operation_id}"))?
        };
        let indexed = crate::distributed::observe_declaration(
            &self.stream,
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            operation_id,
            &declaration_key,
            work,
        )
        .await;
        drop(declaration_key);
        let released = match work.as_deref_mut() {
            Some(io) => io.release_owned(declaration_key_capacity),
            None => Ok(()),
        };
        let indexed = match indexed {
            Ok(indexed) => {
                released?;
                indexed
            }
            Err(error) => return Err(error),
        };
        if indexed.is_some() {
            return Ok(indexed);
        }
        {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(work).await?;
            match coordinator.observe_operation(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                operation_id,
                work,
            ) {
                Ok(operation) => Ok(Some(operation.spec.clone_with_work(work)?)),
                Err(Error::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        }
    }

    pub(super) async fn read_retained_admission_counted(
        &self,
        operation_id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(TaskAdmissionRecord, u64)> {
        let spec = self
            .retained_declaration_with_work(operation_id, work)
            .await?
            .ok_or_else(|| {
                admission_boundary(work, crate::wire::ErrorCode::NotFound, "task admission")
            })?;
        let bytes = if work.is_some() {
            spec.logical_owned_bytes()?
        } else {
            0
        };
        let admission = self.read_declared_admission_counted(&spec, work).await;
        drop(spec);
        let release = match work.as_deref_mut() {
            Some(io) => io.release_owned(bytes),
            None => Ok(()),
        };
        match admission {
            Ok(admission) => {
                release?;
                Ok(admission)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn read_declared_admission_counted(
        &self,
        spec: &OperationSpec,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(TaskAdmissionRecord, u64)> {
        spec.verify_owner(self.verifier.audience(), &self.verifier, work)?;
        let operation_id = spec.operation_id;
        if let Some(limits) = self.session_limits {
            let mut coordinator = self.coordinator.lock().await;
            let retained = match coordinator.scheduler().session_limits(operation_id, work) {
                Err(error) if error.code() == crate::wire::ErrorCode::NotFound => {
                    coordinator.refresh(work).await?;
                    coordinator.scheduler().session_limits(operation_id, work)?
                }
                other => other?,
            };
            if retained != limits {
                return Err(admission_boundary(
                    work,
                    crate::wire::ErrorCode::Conflict,
                    "retained session ceilings differ from owner bindings",
                ));
            }
        }
        let (value, held) =
            self.read_json(&spec.state, work)
                .await
                .map_err(|error| match error {
                    Error::NotFound(_) => admission_boundary(
                        work,
                        crate::wire::ErrorCode::Storage,
                        "committed task admission content is missing",
                    ),
                    other => other,
                })?;
        let (admission, added) = TaskAdmissionRecord::from_canonical_value(value, work)?;
        let held = held.checked_add(added).ok_or_else(|| {
            admission_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "task admission custody overflows",
            )
        })?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let held = if work
            .as_deref()
            .is_some_and(|io| io.original_native_work().is_some())
        {
            let actual = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
                .checked_add(admission.logical_owned_bytes()?)
                .ok_or_else(|| {
                    admission_boundary(
                        work,
                        crate::wire::ErrorCode::Invalid,
                        "task admission custody overflows",
                    )
                })?;
            let retired = held.checked_sub(actual).ok_or_else(|| {
                admission_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "task admission producer custody is undersized",
                )
            })?;
            work.as_deref_mut()
                .expect("native admission observer")
                .release_owned(retired)?;
            actual
        } else {
            held
        };
        let policy_matches = if work.is_some() {
            admission.policy.as_ref() == self.policy_binding.as_ref()
        } else {
            admission.policy == self.policy.as_ref().map(|policy| policy.identity())
        };
        if !policy_matches {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "durable task policy differs from its admission",
            ));
        }
        if let Some(extensions) = &admission.extensions {
            extensions.validate(work)?;
        }
        if admission.extensions.as_ref() != self.root_scope.extensions() {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "retained task extensions differ from owner bindings",
            ));
        }
        let mut operation_text = [0; 36];
        let operation_text: &str = uuid::Uuid::from_bytes(operation_id.into_bytes())
            .hyphenated()
            .encode_lower(&mut operation_text);
        let parent_matches = match (&spec.parent, admission.parent) {
            (None, None) => true,
            (Some(link), Some(parent)) => {
                link.operation_id == OperationId::from_bytes(parent.into_bytes())
                    && link.slot == operation_text
            }
            _ => false,
        };
        if admission.operation_id != operation_id
            || admission.task.name != spec.entrypoint.name
            || admission.task.version != spec.entrypoint.version
            || admission.task.digest != spec.entrypoint.digest
            || admission.output_schema != spec.entrypoint.result_schema
            || !parent_matches
            || admission.machine.name != admission.task.name
            || admission.machine.version != admission.task.version
            || self
                .machines
                .resolve(&admission.machine, &mut None)?
                .is_none()
        {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "durable task admission disagrees with its declaration",
            ));
        }
        self.tasks.validate_durable_admission(
            &admission.task,
            &admission.machine,
            Some(&admission.input_schema),
            &admission.output_schema,
            Some(&admission.input),
            work,
        )?;
        Ok((admission, held))
    }

    pub(crate) async fn admission_with_work(
        &self,
        operation_id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<TaskAdmissionRecord> {
        let admission = self
            .read_retained_admission_counted(operation_id, work)
            .await
            .map(|(record, _)| record)?;
        let (scope, held) = self.authenticated_scope_with_work(&admission, work).await?;
        drop(scope);
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(held)?;
        }
        Ok(admission)
    }
}

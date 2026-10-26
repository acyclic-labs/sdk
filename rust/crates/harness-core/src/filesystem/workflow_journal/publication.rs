use super::*;

impl<P, A, O> FilesystemWorkflowJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    #[inline(never)]
    pub(super) fn stage_payload<'a, T: serde::Serialize + Sync + 'a>(
        &'a self,
        payload: &'a T,
        transition: Option<(u64, crate::OperationId)>,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let _bytes;
            let bytes = match io {
                Some(io) => io.encode(payload, self.maximum_payload_bytes, false)?,
                None => serde_json::to_vec(payload)
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .into(),
            };
            _bytes = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
            if bytes.len() as u64 > self.maximum_payload_bytes {
                return Err(Error::Invalid(
                    "workflow payload exceeds private-file limit".into(),
                ));
            }
            let (_path, path) = {
                let path = match transition {
                    Some((revision, operation)) => crate::executor::with_history_io(io, |work| {
                        crate::executor::preparation_format(
                            format_args!(
                                ".system/workflows/{}/{revision:020}-{operation}.json",
                                self.id()
                            ),
                            work,
                        )
                    })?,
                    None => crate::executor::with_history_io(io, |work| {
                        crate::executor::preparation_format(
                            format_args!(".system/workflows/{}/admission.json", self.id()),
                            work,
                        )
                    })?,
                };
                let held = io
                    .map(|io| io.guard_owned(path.capacity() as u64))
                    .transpose()?;
                (held, path)
            };
            let (_key, key) = {
                let key = match transition {
                    Some((_, operation)) => crate::executor::with_history_io(io, |work| {
                        crate::executor::preparation_format(
                            format_args!("workflow:{}:{operation}", self.id()),
                            work,
                        )
                    })?,
                    None => crate::executor::with_history_io(io, |work| {
                        crate::executor::preparation_format(
                            format_args!("workflow:{}:admission", self.id()),
                            work,
                        )
                    })?,
                };
                let held = io
                    .map(|io| io.guard_owned(key.capacity() as u64))
                    .transpose()?;
                (held, key)
            };
            let key = IdempotencyKey::new(key)?;
            let maximum = io
                .map(|io| io.remaining().map(|remaining| remaining.bytes_encoded))
                .transpose()?
                .unwrap_or(u64::MAX);
            let (_grant, grant) = {
                let grant = crate::executor::with_history_io(io, |work| {
                    ContentGrant::verify(
                        &self.verifier,
                        &self.scope,
                        &self.volume,
                        VolumeOperation::Write,
                        maximum,
                        work,
                    )
                })?;
                let held = io
                    .map(|io| io.guard_owned(grant.logical_owned_bytes()?))
                    .transpose()?;
                (held, grant)
            };
            self.host
                .put_content_impl(
                    &self.volume,
                    &grant,
                    &path,
                    &bytes,
                    "application/json",
                    if transition.is_some() {
                        "transition.json"
                    } else {
                        "admission.json"
                    },
                    self.maximum_payload_bytes,
                    &key,
                    Some(InternalContentClass::Workflow),
                    None,
                    None,
                    io,
                )?
                .await
        })
    }

    #[inline(never)]
    pub(super) fn append_reference<'a>(
        &'a self,
        path: &'a str,
        retry: &'a str,
        revision: u64,
        operation: crate::OperationId,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let admission = path != self.path;
            let _bytes;
            let bytes = match io {
                Some(io) => io.encode(reference, acyclic_stream::MAX_RECORD_BYTES as u64, false)?,
                None => serde_json::to_vec(reference)
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .into(),
            };
            _bytes = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
            if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
                return Err(Error::Invalid(
                    "workflow reference exceeds Stream limit".into(),
                ));
            }
            let digest = {
                let (_input, input) = {
                    let input = crate::executor::with_history_io(io, |work| {
                        crate::executor::preparation_format(format_args!("{path}:{retry}"), work)
                    })?;
                    let held = io
                        .map(|io| io.guard_owned(input.capacity() as u64))
                        .transpose()?;
                    (held, input)
                };
                blake3::Hash::from(crate::model::hash_bytes(input.as_bytes(), io)?)
            };
            let _key;
            let key_bytes = super::super::fork_preparer_io::clone_bytes(digest.as_bytes(), io)?;
            _key = io
                .map(|io| io.guard_bytes(&key_bytes))
                .transpose()?
                .flatten();
            let key = StreamKey::new(key_bytes).map_err(|error| {
                #[cfg(not(target_arch = "wasm32"))]
                if io.is_some() {
                    return Error::NativeStream {
                        code: crate::wire::ErrorCode::Invalid,
                        source: error,
                        custody: None,
                    };
                }
                Error::Invalid(error.to_string())
            })?;
            if let Some(owner) = &self.owner {
                let mut borrowed = io.map(PreparationIo::history_io);
                let mut work = borrowed
                    .as_mut()
                    .map(|work| work as &mut dyn crate::store::history::HistoryIo);
                return owner
                    .append_workflow(admission, revision, &key, bytes, &mut work)
                    .await;
            }
            let (_path, stream) = Self::observation_stream(&self.stream, path, io)?;
            let identity = if admission {
                "workflow admission identity"
            } else {
                "workflow retry identity"
            };
            let appended = match io {
                Some(io) => {
                    let mut borrowed = io.history_io();
                    crate::distributed::append(
                        (&stream, &self.stream),
                        [bytes],
                        0,
                        Some(revision),
                        key,
                        operation,
                        &mut Some(&mut borrowed),
                    )
                    .await
                }
                None => stream
                    .append_batch(vec![bytes.into()], Some(revision), key)
                    .await
                    .map_err(|error| match error {
                        StreamError::Unavailable => Error::Indeterminate(operation),
                        StreamError::IdempotencyMismatch => {
                            Error::Conflict(format!("{identity} reused"))
                        }
                        error => Error::Storage(error.to_string()),
                    }),
            }?;
            match appended {
                AppendOutcome::Committed(receipt)
                    if receipt.start == revision
                        && Some(receipt.end) == revision.checked_add(1) =>
                {
                    Ok(true)
                }
                AppendOutcome::Committed(_) => {
                    Err(Error::Storage("workflow append receipt is invalid".into()))
                }
                AppendOutcome::TailConflict { .. } => Ok(false),
            }
        })
    }

    #[inline(never)]
    pub(super) fn verify_command_payloads<'a>(
        &'a self,
        record: &'a WorkflowRecord,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            for command in &record.transition.commands {
                if command.payload.descriptor().byte_length() > self.maximum_payload_bytes {
                    return Err(Error::Invalid(
                        "workflow command exceeds payload limit".into(),
                    ));
                }
                if let Some(io) = io {
                    let step = acyclic_fs::WorkCounters {
                        items_examined: 1,
                        ..Default::default()
                    };
                    io.preflight(step)?;
                    io.charge(step)?;
                }
                if let Some(owner) = &self.owner {
                    owner.validate_input_file(&command.payload, io)?;
                }
                if command.payload.volume() != &self.volume {
                    return Err(observation_boundary(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "workflow command payload belongs to another volume",
                    ));
                }
                let _bytes;
                let bytes = self.read_bound_content(&command.payload, io).await?;
                _bytes = external_read_hold(&bytes, io)?;
            }
            Ok(())
        })
    }

    #[inline(never)]
    pub(super) fn reconcile<'a>(
        &'a self,
        record: &'a WorkflowRecord,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<WorkflowRecord>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let admission = self.read_admission(io)?.await?.ok_or_else(|| {
                Error::Conflict("workflow transition has no retained admission".into())
            })?;
            let (_, existing) = self
                .verified_history(&admission, record, false, io)?
                .await?;
            let Some(existing) = existing else {
                return Ok(None);
            };
            if existing != *record {
                return Err(Error::Conflict("workflow operation identity reused".into()));
            }
            self.verify_command_payloads(&existing, io)?.await?;
            Ok(Some(existing))
        })
    }

    #[inline(never)]
    fn resolve_commit_append<'a>(
        &'a self,
        appended: Result<bool>,
        record: &'a WorkflowRecord,
        io: Option<&'a PreparationIo>,
    ) -> Result<Option<crate::conversation::BoundedContentFuture<'a, ()>>> {
        // The physical append already determined this outcome. Only an
        // unresolved append starts another admitted reconciliation operation.
        let indeterminate = match appended {
            Ok(true) => return Ok(None),
            Ok(false) => false,
            Err(Error::Indeterminate(_)) if self.owner.is_none() => true,
            Err(error) => return Err(error),
        };
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if !indeterminate {
                return match self.reconcile(record, io)?.await? {
                    Some(_) => Ok(()),
                    None => Err(Error::Conflict(
                        "workflow checkpoint or ownership changed".into(),
                    )),
                };
            }
            match self.reconcile(record, io)?.await {
                Ok(Some(_)) => Ok(()),
                Ok(None) | Err(Error::Storage(_)) => Err(Error::Indeterminate(record.operation_id)),
                #[cfg(not(target_arch = "wasm32"))]
                Err(Error::NativeStream {
                    source: StreamError::Unavailable,
                    ..
                }) => Err(Error::Indeterminate(record.operation_id)),
                Err(error) => Err(error),
            }
        })
        .map(Some)
    }

    pub(crate) async fn commit_with_io(
        &self,
        expected_revision: u64,
        idempotency_key: IdempotencyKey,
        record: WorkflowRecord,
        io: Option<&PreparationIo>,
    ) -> Result<WorkflowRecord> {
        self.commit_borrowed_with_io(expected_revision, idempotency_key, &record, io)?
            .await?;
        Ok(record)
    }

    #[inline(never)]
    pub(crate) fn commit_borrowed_with_io<'a>(
        &'a self,
        expected_revision: u64,
        idempotency_key: IdempotencyKey,
        record: &'a WorkflowRecord,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.require_machine_io(io)?;
            if let Some(io) = io {
                io.check_cancellation()?;
            }
            if let Some(owner) = &self.owner {
                self.verify_observation_owner(owner, false, io).await?;
            }
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let (_admission, admission) = {
                let admission = self.read_admission(io)?.await?.ok_or_else(|| {
                    Error::Conflict("workflow transition has no retained admission".into())
                })?;
                let held = io
                    .map(|io| io.guard_owned(io.owned_since(before)?))
                    .transpose()?;
                (held, admission)
            };
            crate::workflow::validate_workflow_commit(
                expected_revision,
                &idempotency_key,
                record,
                io,
            )?;
            if let Some(owner) = &self.owner {
                owner.validate_workflow_record(record, io)?;
            }
            {
                let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                let (history_hold, (revision, existing)) = {
                    let (revision, existing) =
                        self.verified_history(&admission, record, true, io)?.await?;
                    let history_hold = io
                        .map(|io| io.guard_owned(io.owned_since(before)?))
                        .transpose()?;
                    (history_hold, (revision, existing))
                };
                if let Some(existing) = existing {
                    if existing != *record {
                        return Err(Error::Conflict(
                            "workflow operation identity is already bound".into(),
                        ));
                    }
                    self.verify_command_payloads(&existing, io)?.await?;
                    drop(history_hold);
                    return Ok(());
                }
                if revision != expected_revision {
                    return Err(Error::Conflict(
                        "workflow checkpoint revision is stale".into(),
                    ));
                }
            }
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            self.verify_command_payloads(record, io)?.await?;
            let (_reference, reference) = {
                let reference = self
                    .stage_payload(
                        record,
                        Some((record.prior.revision, record.operation_id)),
                        io,
                    )?
                    .await?;
                let held = io
                    .map(|io| io.guard_owned(io.owned_since(before)?))
                    .transpose()?;
                (held, reference)
            };
            {
                let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                let (_staged, staged) = {
                    let staged = self.read_record(&reference, false, false, io)?.await?;
                    let held = io
                        .map(|io| io.guard_owned(io.owned_since(before)?))
                        .transpose()?;
                    (held, staged)
                };
                if staged != *record {
                    return Err(Error::Conflict(
                        "staged workflow record disagrees with its transition".into(),
                    ));
                }
            }
            let appended = self
                .append_reference(
                    &self.path,
                    idempotency_key.as_str(),
                    expected_revision,
                    record.operation_id,
                    &reference,
                    io,
                )?
                .await;
            if let Some(reconciliation) = self.resolve_commit_append(appended, record, io)? {
                reconciliation.await?;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod polling_graph_storage_tests {
    use super::*;
    type Journal = FilesystemWorkflowJournal<
        acyclic_stream::MemoryStream,
        acyclic_fs::MemoryAuthorityBackend,
        acyclic_fs::MemoryObjectBackend,
    >;

    use crate::{AgentId, conversation::VolumeOwner, resources::ProviderRef};
    struct Clock;
    impl acyclic_stream::UnixMillisClock for Clock {
        fn now_unix_millis(&self) -> u64 {
            1234
        }
    }

    async fn fixture() -> Result<Arc<Journal>> {
        fixture_with_filesystem(acyclic_fs::Fs::memory()).await
    }

    async fn fixture_with_filesystem(filesystem: acyclic_fs::MemoryFs) -> Result<Arc<Journal>> {
        let provider =
            ProviderRef::new("future".into(), "filesystem".into(), "2".into(), &mut None)?;
        let host = FilesystemHost::new(filesystem, &provider, None)?;
        let agent = AgentId::from_bytes([1; 16]);
        let volume = VolumeRef::new(
            provider,
            "future".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        host.create_volume(&volume).await?;
        let issuer = crate::core::AuthorityIssuer::new(
            "future",
            [2; 32],
            crate::core::Authority {
                kind: crate::core::AggregateKind::Conversation,
                id: crate::ConversationId::from_bytes([3; 16]).to_string(),
            },
        );
        let scope = issuer.issue(
            "owner",
            &crate::Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )?;
        FilesystemWorkflowJournal::new(
            Arc::new(acyclic_stream::MemoryStream::new_with_clock(
                Default::default(),
                Arc::new(Clock),
            )),
            host,
            "confirmed",
            crate::filesystem::FilesystemJournalBinding {
                volume: &volume,
                verifier: &issuer.verifier(&mut None)?,
                scope: &scope,
                maximum_bytes: 4096,
                io: None,
            },
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn confirmed_append_survives_cancelled_completion_admission() -> Result<()> {
        use crate::workflow::{MachineCheckpoint, MachineIdentity, MachineTransition};
        let journal = fixture().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let record = WorkflowRecord {
            operation_id: crate::OperationId::from_bytes([1; 16]),
            idempotency_key: IdempotencyKey::new("confirmed")?,
            prior: MachineCheckpoint {
                machine: MachineIdentity {
                    name: "confirmed".into(),
                    version: "1".into(),
                    digest: [1; 32],
                },
                revision: 0,
                state: serde_json::Value::Null,
            },
            input: serde_json::Value::Null,
            transition: MachineTransition {
                state: serde_json::Value::Null,
                commands: vec![],
                status: MachineStatus::Suspended,
            },
        };
        cancellation.cancel();
        assert!(io.check_cancellation().is_err());
        let result = journal.resolve_commit_append(Ok(true), &record, Some(&io))?;
        assert!(
            result.is_none(),
            "confirmed publication must not start reconciliation"
        );
        assert!(
            journal
                .resolve_commit_append(Ok(false), &record, Some(&io))
                .is_err()
        );
        assert!(matches!(
            journal.resolve_commit_append(
                Err(Error::Conflict("confirmed error".into())),
                &record,
                Some(&io)
            ),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    type Input = (
        &'static Journal,
        &'static WorkflowAdmission,
        &'static WorkflowRecord,
        &'static FileRef,
    );
    fn retained<Args, T>(_: impl FnOnce(Args) -> T) -> usize {
        std::mem::size_of::<T>()
    }
    #[test]
    fn workflow_children_retain_only_owned_polling_handles() {
        let measured = [
            (
                "verified_history",
                retained(|(j, a, r, _): Input| j.verified_history(a, r, true, None)),
            ),
            (
                "read_admission",
                retained(|(j, _, _, _): Input| j.read_admission(None)),
            ),
            (
                "read_record",
                retained(|(j, _, _, f): Input| j.read_record(f, true, true, None)),
            ),
            (
                "replay_records",
                retained(|(j, _, _, _): Input| j.replay_records(0, 1, true, true, None)),
            ),
            (
                "reconcile",
                retained(|(j, _, r, _): Input| j.reconcile(r, None)),
            ),
            (
                "resolve_commit_append",
                retained(|(j, _, r, _): Input| j.resolve_commit_append(Ok(true), r, None)),
            ),
            (
                "verify_command_payloads",
                retained(|(j, _, r, _): Input| j.verify_command_payloads(r, None)),
            ),
            (
                "stage_payload",
                retained(|(j, _, r, _): Input| j.stage_payload(r, None, None)),
            ),
            (
                "append_reference",
                retained(|(j, _, _, f): Input| {
                    j.append_reference(
                        "path",
                        "retry",
                        0,
                        crate::OperationId::from_bytes([1; 16]),
                        f,
                        None,
                    )
                }),
            ),
        ];
        eprintln!("workflow polling graph retained bytes: {measured:?}");
        assert!(
            measured.into_iter().all(|(_, bytes)| bytes <= 512),
            "workflow children embed concrete polling graphs"
        );
    }
}

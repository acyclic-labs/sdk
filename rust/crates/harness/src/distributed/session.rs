use super::*;

impl<P: StreamProvider> DistributedCoordinator<P> {
    /// Pins immutable session ceilings under the root owner's authority.
    pub async fn configure_session(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        limits: crate::scheduler::SessionLimits,
        idempotency_key: IdempotencyKey,
    ) -> Result<bool> {
        self.refresh(&mut None).await?;
        authorize_operation_scope(
            owner,
            scope,
            verifier,
            capability::OPERATION_DECLARE,
            &mut None,
        )?;
        self.authorize_owner(owner, verifier, operation_id, &mut None)?;
        self.apply_internal(
            idempotency_key,
            SchedulerEvent::SessionConfigured {
                operation_id,
                limits,
            },
            None,
            None,
            None,
            None,
            None,
            &mut None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(crate) async fn declare_session(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        spec: OperationSpec,
        limits: crate::scheduler::SessionLimits,
        idempotency_key: IdempotencyKey,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        authorize_operation_scope(owner, scope, verifier, capability::OPERATION_DECLARE, work)?;
        spec.verify_owner(owner, verifier, work)?;
        if spec.parent.is_some() {
            return Err(coordinator_boundary(
                work,
                wire::ErrorCode::Invalid,
                "session declaration must be a root",
            ));
        }
        limits.validate(work)?;
        validate_idempotency_key(idempotency_key.as_str(), work)?;

        let mut custody = CodecQuota::new(work, 0)?;
        let operation_id = spec.operation_id;
        if let Some(io) = custody.work().as_deref_mut() {
            io.hashed_bytes(idempotency_key.as_str().len() as u64)?;
        }
        let session_key = formatted_text(
            "session:".len() + 36 + 1 + 64,
            format_args!(
                "session:{operation_id}:{}",
                blake3::hash(idempotency_key.as_str().as_bytes()).to_hex()
            ),
            custody.work(),
        )?;
        if custody.work().is_some() {
            custody.adopt(session_key.capacity() as u64)?;
        }
        let keys = [idempotency_key.as_str(), session_key.as_str()];
        let box_bytes = std::mem::size_of::<OperationSpec>() as u64;
        custody.replace(custody.retained() + box_bytes)?;
        if let Some(io) = custody.work().as_deref_mut() {
            io.admit_allocation(box_bytes)?;
        }
        let events = [
            SchedulerEvent::Declared {
                spec: Box::new(spec),
            },
            SchedulerEvent::SessionConfigured {
                operation_id,
                limits,
            },
        ];
        self.refresh(custody.work()).await?;
        if self
            .session_intent(&keys, &events, custody.work())
            .await?
            .is_some()
        {
            return Ok(false);
        }

        let expected_start = self.revision;
        let mut request = CodecQuota::new(custody.work(), 0)?;
        let mut records = [OwnedBytes::new(), OwnedBytes::new()];
        let mut revision = expected_start;
        let mut committed_at_ms = self.next_commit_time(request.work())?;
        for (index, (key, event)) in keys.iter().zip(&events).enumerate() {
            if let SchedulerEvent::Declared { spec } = event {
                self.verify_content(&spec.state, request.work()).await?;
                self.scheduler
                    .validate_session(spec, &limits, request.work())?;
            }
            revision = revision.checked_add(1).ok_or_else(|| {
                coordinator_boundary(
                    request.work(),
                    wire::ErrorCode::Invalid,
                    "coordinator revision exhausted",
                )
            })?;
            let canonical = crate::store::history::canonical_bytes_with_work(
                event,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                request.work(),
            )?;
            let held = if request.work().is_some() {
                canonical.capacity() as u64
            } else {
                0
            };
            let mut encoded = CodecQuota::from_retained(request.work(), held);
            if let Some(io) = encoded.work().as_deref_mut() {
                io.hashed_bytes(canonical.len() as u64)?;
            }
            let digest = *blake3::hash(&canonical).as_bytes();
            encoded.transfer();
            let bytes = encode(
                revision,
                operation_id,
                key,
                digest,
                canonical,
                committed_at_ms,
                encoded.work(),
            )?;
            drop(encoded);
            let (record, retained) =
                operations::owned_bytes_from_retained_vec(bytes, request.work())?;
            request.adopt(retained)?;
            records[index] = record;
            committed_at_ms = committed_at_ms.checked_add(1).ok_or_else(|| {
                coordinator_boundary(
                    request.work(),
                    wire::ErrorCode::Invalid,
                    "coordinator commit time exhausted",
                )
            })?;
        }
        let (key, held) = stream_key(
            idempotency_key.as_str().len(),
            format_args!("{}", idempotency_key.as_str()),
            request.work(),
        )?;
        let retained = request.retained();
        request.transfer();
        let mut key_custody = CodecQuota::from_retained(request.work(), held);
        let appended = append(
            (&self.stream.0, &self.client),
            records,
            retained,
            Some(expected_start),
            key,
            operation_id,
            key_custody.work(),
        )
        .await;
        drop(key_custody);
        let receipt = match appended {
            Ok(AppendOutcome::Committed(receipt)) => {
                if receipt.end.checked_sub(receipt.start) != Some(2) || receipt.tail < receipt.end {
                    return Err(coordinator_boundary(
                        request.work(),
                        wire::ErrorCode::Storage,
                        "invalid session append receipt",
                    ));
                }
                Some(receipt)
            }
            Ok(AppendOutcome::TailConflict { .. }) => None,
            Err(error) if coordinator_retry_conflict(&error) => None,
            Err(error) => return Err(error),
        };
        self.refresh(request.work()).await?;
        let Some((revision, commit_id)) =
            self.session_intent(&keys, &events, request.work()).await?
        else {
            return Err(coordinator_boundary(
                request.work(),
                wire::ErrorCode::Conflict,
                "session append conflicted without its complete intent",
            ));
        };
        if let Some(receipt) = &receipt
            && (commit_id != receipt.commit_id || revision.checked_sub(2) != Some(receipt.start))
        {
            return Err(coordinator_boundary(
                request.work(),
                wire::ErrorCode::Storage,
                "session append receipt differs from its committed envelope",
            ));
        }
        Ok(receipt.is_some_and(|receipt| receipt.start == expected_start))
    }

    async fn session_intent(
        &mut self,
        keys: &[&str; 2],
        events: &[SchedulerEvent; 2],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<(u64, acyclic_stream::CommitId)>> {
        let mut positions = [None; 2];
        for (index, (key, event)) in keys.iter().zip(events).enumerate() {
            let mut custody = CodecQuota::new(work, 0)?;
            let Some((digest, existing, revision, commit_id)) =
                read_intent(&self.client, key, Some(self.revision), custody.work()).await?
            else {
                continue;
            };
            if custody.work().is_some() {
                custody.adopt(existing.logical_owned_bytes()?)?;
            }
            if &existing != event
                || digest
                    != crate::executor::json::canonical_json_digest(
                        event,
                        acyclic_stream::MAX_RECORD_BYTES as u64,
                        custody.work(),
                    )?
            {
                return Err(coordinator_boundary(
                    custody.work(),
                    wire::ErrorCode::Conflict,
                    "session retry identity reused",
                ));
            }
            positions[index] = Some((revision, commit_id));
        }
        match positions {
            [None, None] => Ok(None),
            [Some((prior, first)), Some((revision, second))]
                if prior.checked_add(1) == Some(revision) && first == second =>
            {
                Ok(Some((revision, second)))
            }
            _ => Err(coordinator_boundary(
                work,
                wire::ErrorCode::Conflict,
                "session declaration is not atomic",
            )),
        }
    }
}

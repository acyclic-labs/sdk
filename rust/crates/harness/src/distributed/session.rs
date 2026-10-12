use super::*;

impl<P: StreamProvider> DistributedCoordinator<P> {
    /// Pins immutable session ceilings under the existing root owner's authority.
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
        authorize_operation_scope(owner, scope, verifier, capability::OPERATION_DECLARE, &mut None)?;
        self.authorize_owner(owner, verifier, operation_id, &mut None)?;
        self.apply_internal(
            idempotency_key,
            SchedulerEvent::SessionConfigured {
                operation_id,
                limits,
            }, None, None, None, None, None, &mut None
        )
        .await
    }

    #[allow(clippy::too_many_lines,         clippy::too_many_arguments)]
    pub(crate) async fn declare_session(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        spec: OperationSpec,
        limits: crate::scheduler::SessionLimits,
        idempotency_key: IdempotencyKey, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        let mut custody = CodecQuota::new(work, 0)?;
        verifier.verify_audience(owner, custody.work())?;
        verifier.verify(scope, custody.work())?;
        if !scope.capabilities().contains("operation:declare") || spec.owner.authority() != owner {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Unauthorized,
                "session declaration owner is invalid"));
        }
        spec.verify_owner(owner, verifier, custody.work())?;
        if spec.parent.is_some() {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Invalid,
                "session declaration must be a root"));
        }
        limits.validate(custody.work())?;
        validate_idempotency_key(idempotency_key.as_str(), custody.work())?;
        let operation_id = spec.operation_id;
        let declaration_key = crate::core::copy_text(idempotency_key.as_str(), &mut custody)?;
        if let Some(work) = custody.work().as_deref_mut() {
            work.hashed_bytes(idempotency_key.as_str().len() as u64)?;
        }
        let session_key = formatted_text(
            "session:".len() + 36 + 1 + 64,
            format_args!("session:{operation_id}:{}",
                blake3::hash(idempotency_key.as_str().as_bytes()).to_hex()),
            custody.work(),
        )?;
        if custody.work().is_some() { custody.adopt(session_key.capacity() as u64)?; }
        let keys = [declaration_key, session_key];
        let box_bytes = std::mem::size_of::<OperationSpec>() as u64;
        if let Some(work) = custody.work().as_deref_mut() {
            work.admit_owned(box_bytes)?;
            work.admit_allocation(box_bytes)?;
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
        if let Some(work) = custody.work().as_deref_mut() {
            work.retain_owned(box_bytes)?;
            custody.adopt(box_bytes)?;
        }
        self.refresh(custody.work()).await?;
        if self.session_intent(&keys, &events, custody.work()).await?.is_some() {
            return Ok(false);
        }
        let mut request = CodecQuota::new(custody.work(), 0)?;
        let mut records = [OwnedBytes::new(), OwnedBytes::new()];
        let mut revision = self.revision;
        let mut committed_at_ms = self.next_commit_time(request.work())?;
        for (index, (key, event)) in keys.iter().zip(&events).enumerate() {
            if let SchedulerEvent::Declared { spec } = event {
                self.verify_content(&spec.state, request.work()).await?;
                self.scheduler.validate_session(spec, &limits, request.work())?;
            }
            revision = revision.checked_add(1).ok_or_else(|| {
                coordinator_boundary(request.work(), wire::ErrorCode::Invalid,
                    "coordinator revision exhausted")
            })?;
            let canonical = crate::store::history::canonical_bytes_with_work(
                event, acyclic_stream::MAX_RECORD_BYTES as u64, request.work(),
            )?;
            let canonical_held = if request.work().is_some() { canonical.capacity() as u64 } else { 0 };
            let mut canonical_custody = CodecQuota::from_retained(
                request.work(), canonical_held,
            );
            if let Some(work) = canonical_custody.work().as_deref_mut() {
                work.hashed_bytes(canonical.len() as u64)?;
            }
            let digest = *blake3::hash(&canonical).as_bytes();
            canonical_custody.transfer();
            let encoded = encode(
                revision, operation_id, key, digest, canonical, committed_at_ms, canonical_custody.work(),
            )?;
            drop(canonical_custody);
            let (encoded, encoded_held) = operations::owned_bytes_from_retained_vec(
                encoded, request.work(),
            )?;
            request.adopt(encoded_held)?;
            records[index] = encoded;
            committed_at_ms = committed_at_ms.checked_add(1).ok_or_else(|| {
                coordinator_boundary(request.work(), wire::ErrorCode::Invalid,
                    "coordinator commit time exhausted")
            })?;
        }
        let (stream_key, key_held) = stream_key(idempotency_key.as_str().len(), format_args!("{}", idempotency_key.as_str()), request.work())?;
        let retained_request = request.retained();
        request.transfer();
        let mut key_custody = CodecQuota::from_retained(request.work(), key_held);
        let appended = append(
            (&self.stream.0, &self.client), records, retained_request, Some(self.revision),
            stream_key, operation_id, key_custody.work(),
        ).await;
        drop(key_custody);
        let outcome = match appended {
            Ok(outcome) => outcome,
            Err(error) if coordinator_retry_conflict(&error) => {
                self.refresh(request.work()).await?;
                if self.session_intent(&keys, &events, request.work()).await?.is_some() {
                    return Ok(false);
                }
                return Err(coordinator_boundary(request.work(), wire::ErrorCode::Conflict,
                    "session retry identity reused"));
            }
            Err(error) => return Err(error),
        };
        let (result, receipt) = match outcome {
            AppendOutcome::Committed(receipt) => {
                if receipt.end.checked_sub(receipt.start) != Some(2) || receipt.tail < receipt.end {
                    return Err(coordinator_boundary(request.work(), wire::ErrorCode::Storage,
                        "invalid session append receipt"));
                }
                let result = receipt.start == revision - 2;
                (result, Some((receipt.commit_id, receipt.start)))
            }
            AppendOutcome::TailConflict { .. } => (false, None),
        };
        self.refresh(request.work()).await?;
        let Some((revision, commit_id)) = self.session_intent(&keys, &events, request.work()).await? else {
            return Err(coordinator_boundary(request.work(), wire::ErrorCode::Conflict,
                "session append conflicted without its complete intent"));
        };
        if let Some((envelope, start)) = receipt
            && (commit_id != envelope || revision.checked_sub(2) != Some(start))
        {
            return Err(coordinator_boundary(request.work(), wire::ErrorCode::Storage,
                "session append receipt differs from its committed envelope"));
        }
        Ok(result)
    }

    async fn session_intent(
        &mut self, keys: &[String; 2], events: &[SchedulerEvent; 2],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<(u64, acyclic_stream::CommitId)>> {
        let mut retained = 0;
        let mut previous: Option<(u64, acyclic_stream::CommitId)> = None;
        for (key, event) in keys.iter().zip(events) {
            let mut custody = CodecQuota::new(work, 0)?;
            if let Some(intent) = read_intent(&self.client, key, Some(self.revision), custody.work()).await? {
                if custody.work().is_some() {
                    custody.adopt(intent.1.logical_owned_bytes()?)?;
                }
                let (digest, existing, revision, commit_id) = &intent;
                if existing != event {
                    return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
                        "session retry identity reused"));
                }
                let canonical = crate::store::history::canonical_bytes_with_work(
                    event, acyclic_stream::MAX_RECORD_BYTES as u64, custody.work(),
                )?;
                if custody.work().is_some() { custody.adopt(canonical.capacity() as u64)?; }
                if let Some(work) = custody.work().as_deref_mut() { work.hashed_bytes(canonical.len() as u64)?; }
                if digest != blake3::hash(&canonical).as_bytes() {
                    return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
                        "session retry identity reused"));
                }
                if let Some((prior, envelope)) = previous
                    && (prior.checked_add(1) != Some(*revision) || envelope != *commit_id)
                {
                    return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
                        "session declaration is not one atomic envelope"));
                }
                previous = Some((*revision, *commit_id));
                retained += 1;
            }
        }
        match retained {
            0 => Ok(None),
            2 => Ok(previous),
            _ => Err(coordinator_boundary(work, wire::ErrorCode::Conflict,
                "session declaration is not atomic")),
        }
    }
}

use super::*;

impl crate::OperationWindowStore for IndexedDbAuthorityStore {
    type Error = AuthorityStoreError;
    async fn load(
        &self,
        workspace: crate::WorkspaceId,
    ) -> Result<Option<crate::OperationWindowSnapshot>, Self::Error> {
        let cancellation = CancellationToken::new();
        let mut io = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let tx = transaction(
            &self.database,
            &[OPERATION_WINDOWS],
            TransactionMode::Readonly,
        )
        .map_err(backend)?;
        self.window(&mut io, &tx, workspace).await
    }
    async fn compare_and_swap(
        &self,
        workspace: crate::WorkspaceId,
        expected: u64,
        replacement: crate::OperationWindowSnapshot,
    ) -> Result<bool, Self::Error> {
        if !replacement.is_valid_for(workspace)
            || expected.checked_add(1) != Some(replacement.revision)
        {
            return Err(AuthorityStoreError::Corrupt(
                "invalid window replacement".to_owned(),
            ));
        }
        let mut bytes = vec![0; self.maximum_payload_bytes.min(MAXIMUM_WINDOW_BYTES) as usize];
        let mut writer = std::io::Cursor::new(bytes.as_mut_slice());
        serde_json::to_writer(&mut writer, &replacement).map_err(backend)?;
        let length = writer.position() as usize;
        bytes.truncate(length);
        let cancellation = CancellationToken::new();
        let mut io = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let tx = transaction(
            &self.database,
            &[OPERATION_WINDOWS],
            TransactionMode::Readwrite,
        )
        .map_err(backend)?;
        if self
            .window(&mut io, &tx, workspace)
            .await?
            .map_or(0, |s| s.revision)
            != expected
        {
            return Ok(false);
        }
        io.set(
            &tx,
            OPERATION_WINDOWS,
            &authority_key(AuthorityId::from_bytes(workspace.into_bytes())),
            &bytes,
        )
        .await?;
        io.commit(tx).await?;
        Ok(true)
    }
}
impl AsyncAuthorityStore for IndexedDbAuthorityStore {
    async fn create_first_record<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        context: &mut crate::AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<bool, crate::AuthorityError<G::Error>> {
        if guard.is_some() {
            return crate::async_storage::first_record(self, authority, commit, context, guard)
                .await;
        }

        let io = &mut *context;
        io.check()?;
        let tx = transaction(&self.database, AUTHORITY_STORES, TransactionMode::Readwrite)
            .map_err(backend)?;
        if io
            .fixed(&tx, AUTHORITY_HEADS, &authority_key(authority), HEAD_BYTES)
            .await?
            .is_some()
        {
            drop(tx);
            return Ok(crate::async_storage::first_record(
                self,
                authority,
                commit,
                io,
                None::<&mut ()>,
            )
            .await
            .map_err(|e| e.into_error(std::convert::identity))?);
        }
        self.first(io, &tx, authority, commit).await?;
        io.commit(tx).await?;
        Ok(true)
    }
    async fn fork_workspace<G: crate::AuthorityGuard>(
        &self,
        fork: WorkspaceForkCommit,
        permit: PublicationPermit,
        context: &mut crate::AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<WorkspaceForkOutcome, crate::AuthorityError<G::Error>> {
        if guard.is_some() || permit != PublicationPermit::Unrestricted {
            return crate::async_storage::workspace_fork(self, fork, permit, context, guard).await;
        }

        let io = &mut *context;
        io.check()?;
        let tx = transaction(&self.database, AUTHORITY_STORES, TransactionMode::Readwrite)
            .map_err(backend)?;
        if fork
            .lineage
            .is_some_and(|s| s.lineage == GenerationFork::PublishedPrefix)
            || io
                .fixed(
                    &tx,
                    AUTHORITY_HEADS,
                    &authority_key(fork.retention),
                    HEAD_BYTES,
                )
                .await?
                .is_some()
            || io
                .fixed(
                    &tx,
                    AUTHORITY_HEADS,
                    &authority_key(fork.destination),
                    HEAD_BYTES,
                )
                .await?
                .is_some()
        {
            drop(tx);
            return Ok(crate::async_storage::workspace_fork(
                self,
                fork,
                permit,
                io,
                None::<&mut ()>,
            )
            .await
            .map_err(|e| e.into_error(std::convert::identity))?);
        }
        self.first(io, &tx, fork.retention, fork.retained).await?;
        self.first(io, &tx, fork.destination, fork.creation).await?;
        io.commit(tx).await?;
        Ok(WorkspaceForkOutcome::Committed)
    }

    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        context: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        crate::async_storage::seal(guard.as_deref_mut(), context).await?;
        let io = &mut *context;
        io.check()?;
        let tx = transaction(&self.database, AUTHORITY_STORES, TransactionMode::Readwrite)
            .map_err(backend)?;
        let key = authority_key(authority);
        let value = if let Some(bytes) = io.fixed(&tx, AUTHORITY_HEADS, &key, HEAD_BYTES).await? {
            let head = decode_head(&bytes).map_err(corrupt)?;
            io.gate(&tx, authority).await?;
            drop(tx);
            CreateAuthorityOutcome::Existing(head)
        } else {
            let head = Head::genesis(epoch);
            work::charge(io, authority_fixed_write_work(HEAD_BYTES + GATE_BYTES, 2))?;
            io.insert(
                &tx,
                AUTHORITY_HEADS,
                &key,
                Uint8Array::from(encode_head(head).as_slice()).into(),
            )
            .await?;
            io.insert(
                &tx,
                AUTHORITY_GATES,
                &key,
                Uint8Array::from(encode_publication_gate(free_publication_gate()).as_slice())
                    .into(),
            )
            .await?;
            io.commit(tx).await?;
            CreateAuthorityOutcome::Created(head)
        };
        if matches!(value, CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard.as_deref_mut(), context).await?;
        }
        Ok(value)
    }
    async fn head(
        &self,
        authority: AuthorityId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        let io = context;
        io.check()?;
        let tx = transaction(
            &self.database,
            &[AUTHORITY_HEADS],
            TransactionMode::Readonly,
        )
        .map_err(backend)?;
        let head = io.head(&tx, authority).await?;
        Ok(head)
    }
    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: GuardedAppend,
        context: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<AppendOutcome, crate::AuthorityError<G::Error>> {
        crate::async_storage::seal(guard.as_deref_mut(), context).await?;

        let GuardedAppend {
            authority_id: authority,
            epoch,
            expected,
            commit,
            permit,
        } = request;
        let io = &mut *context;
        io.check()?;
        self.validate(&commit)?;
        let tx = transaction(&self.database, AUTHORITY_STORES, TransactionMode::Readwrite)
            .map_err(backend)?;
        let actual = io.head(&tx, authority).await?;
        if epoch != actual.epoch {
            return Ok(AppendOutcome::Fenced {
                    actual_epoch: actual.epoch,
                });
        }
        if let Some(bytes) = io
            .fixed(
                &tx,
                AUTHORITY_OPERATIONS,
                &operation_key(authority, commit.operation_id),
                OPERATION_BYTES,
            )
            .await?
        {
            let op = decode_operation(&bytes).map_err(corrupt)?;
            if op.fingerprint != commit.fingerprint {
                return Ok(AppendOutcome::IdempotencyConflict {
                        committed_fingerprint: op.fingerprint,
                    });
            }
            work::charge(io, authority_fixed_read_work(0))?;
            let blob = io
                .blob(&tx, AUTHORITY_COMMITS, &commit_key(authority, op.sequence))
                .await?;
            drop(tx);
            let durable = self.decode(io, authority, &blob, 0).await?;
            if durable.operation_id != commit.operation_id
                || durable.sequence != op.sequence
                || durable.fingerprint != op.fingerprint
            {
                return Err(corrupt("operation index does not match commit").into());
            }
            return Ok(AppendOutcome::AlreadyCommitted(durable));
        }
        if expected != actual {
            return Ok(AppendOutcome::Conflict { actual });
        }
        let gate = io.gate(&tx, authority).await?;
        let admitted = match permit {
            PublicationPermit::Unrestricted => gate.active.is_none(),
            PublicationPermit::Reservation {
                operation_id,
                gate_tail,
                expected: reserved,
            } => {
                gate.tail == gate_tail
                    && gate.active == Some(OperationId::from_bytes(operation_id))
                    && reserved == expected
            }
            PublicationPermit::Lease {
                authority_id,
                workspace_id,
                lease_id,
                expires_at_millis,
            } => {
                let workspace = crate::WorkspaceId::from_bytes(workspace_id);
                let snapshot = self.window(io, &tx, workspace).await?;
                let active = match snapshot {
                    Some(crate::OperationWindowSnapshot {
                        phase: crate::OperationWindowPhase::Active { leases, .. },
                        ..
                    }) => leases
                        .get(&crate::OperationLeaseId::from_bytes(lease_id))
                        .is_some_and(|lease| lease.expires_at_millis == expires_at_millis),
                    _ => false,
                };
                let now = js_sys::Date::now()
                    .to_string()
                    .parse::<u64>()
                    .map_err(backend)?;
                gate.active.is_none()
                    && authority_id == authority.into_bytes()
                    && crate::kernel::volume_authority_id(workspace.volume_id()) == authority
                    && active
                    && now < expires_at_millis
            }
        };
        if !admitted {
            return Ok(AppendOutcome::Fenced {
                    actual_epoch: actual.epoch,
                });
        }
        let (durable, bytes) = self.prepare(io, authority, actual, commit)?;
        self.write(&io, &tx, authority, &durable, &bytes).await?;
        io.commit(tx).await?;
        Ok(AppendOutcome::Committed(durable))
    }
    async fn reserve_publication(
        &self,
        authority: AuthorityId,
        expected: Head,
        operation: OperationId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<ReservationOutcome, AuthorityStoreError> {
        let io = context;
        io.check()?;
        let tx = transaction(
            &self.database,
            &[AUTHORITY_HEADS, AUTHORITY_GATES],
            TransactionMode::Readwrite,
        )
        .map_err(backend)?;
        let actual = io.head(&tx, authority).await?;
        let gate = io.gate(&tx, authority).await?;
        if actual != expected || gate.active.is_some_and(|active| active != operation) {
            return Ok(ReservationOutcome::Conflict { actual });
        }
        let reservation = PublicationReservation {
            authority_id: authority,
            operation_id: operation,
            expected,
            gate_tail: gate.tail,
        };
        if gate.active == Some(operation) {
            return Ok(ReservationOutcome::AlreadyReserved(reservation));
        }
        let tail = gate
            .tail
            .checked_add(1)
            .ok_or_else(|| backend("publication gate exhausted"))?;
        work::charge(io, authority_fixed_write_work(GATE_BYTES, 1))?;
        io.set(
            &tx,
            AUTHORITY_GATES,
            &authority_key(authority),
            &encode_publication_gate(PublicationGateRecord {
                tail,
                active: Some(operation),
                released: None,
            }),
        )
        .await?;
        io.commit(tx).await?;
        Ok(ReservationOutcome::Reserved(PublicationReservation {
            gate_tail: tail,
            ..reservation
        }))
    }
    async fn release_publication(
        &self,
        reservation: PublicationReservation,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let io = context;
        io.check()?;
        let tx = transaction(
            &self.database,
            &[AUTHORITY_GATES],
            TransactionMode::Readwrite,
        )
        .map_err(backend)?;
        let gate = io.gate(&tx, reservation.authority_id).await?;
        if gate.released == Some((reservation.operation_id, reservation.gate_tail)) {
            return Ok(());
        }
        if gate.tail != reservation.gate_tail || gate.active != Some(reservation.operation_id) {
            return Err(backend("publication reservation is no longer owned"));
        }
        let tail = gate
            .tail
            .checked_add(1)
            .ok_or_else(|| backend("publication gate exhausted"))?;
        work::charge(io, authority_fixed_write_work(GATE_BYTES, 1))?;
        io.set(
            &tx,
            AUTHORITY_GATES,
            &authority_key(reservation.authority_id),
            &encode_publication_gate(PublicationGateRecord {
                tail,
                active: None,
                released: Some((reservation.operation_id, reservation.gate_tail)),
            }),
        )
        .await?;
        io.commit(tx).await?;
        Ok(())
    }
    async fn fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<FenceOutcome, AuthorityStoreError> {
        let io = context;
        io.check()?;
        let tx = transaction(
            &self.database,
            &[AUTHORITY_HEADS],
            TransactionMode::Readwrite,
        )
        .map_err(backend)?;
        let actual = io.head(&tx, authority).await?;
        if actual != expected {
            return Ok(FenceOutcome::Conflict { actual });
        }
        let epoch = actual
            .epoch
            .get()
            .checked_add(1)
            .ok_or_else(|| AuthorityStoreError::EpochExhausted)?;
        let next = Head {
            epoch: Epoch::new(epoch).map_err(backend)?,
            ..actual
        };
        work::charge(
            io,
            WorkCounters {
                authority_records_appended: 1,
                ..authority_fixed_write_work(HEAD_BYTES, 1)
            },
        )?;
        io.set(
            &tx,
            AUTHORITY_HEADS,
            &authority_key(authority),
            &encode_head(next),
        )
        .await?;
        io.commit(tx).await?;
        Ok(FenceOutcome::Advanced(next))
    }
    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        let io = &mut *context;
        io.check()?;
        let tx = transaction(&self.database, AUTHORITY_STORES, TransactionMode::Readonly)
            .map_err(backend)?;
        io.head(&tx, authority).await?;
        let Some(bytes) = io
            .fixed(
                &tx,
                AUTHORITY_OPERATIONS,
                &operation_key(authority, operation),
                OPERATION_BYTES,
            )
            .await?
        else {
            return Ok(None);
        };
        let op = decode_operation(&bytes).map_err(corrupt)?;
        work::charge(io, authority_fixed_read_work(0))?;
        let blob = io
            .blob(&tx, AUTHORITY_COMMITS, &commit_key(authority, op.sequence))
            .await?;
        drop(tx);
        let durable = self.decode(io, authority, &blob, 0).await?;
        if durable.operation_id != operation
            || durable.sequence != op.sequence
            || durable.fingerprint != op.fingerprint
        {
            return Err(corrupt("operation index does not match commit"));
        }
        Ok(Some(durable))
    }
    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(AuthorityStoreError::InvalidReplayLimit);
        }
        if limit.records > 4096 {
            return Err(AuthorityStoreError::Rejected(
                "browser replay page exceeds 4096 records".to_owned(),
            ));
        }
        let io = &mut *context;
        io.check()?;
        let tx = transaction(
            &self.database,
            &[AUTHORITY_HEADS, AUTHORITY_COMMITS],
            TransactionMode::Readonly,
        )
        .map_err(backend)?;
        let head = io.head(&tx, authority).await?;
        if after > head.sequence {
            return Err(backend("replay cursor is ahead of head"));
        }
        if after == head.sequence {
            return Ok((Vec::new(), None));
        }
        let count = (head.sequence.get() - after.get()).min(u64::from(limit.records))
            + u64::from(after != Sequence::GENESIS);
        let start = after.get() + u64::from(after == Sequence::GENESIS);
        let handles = count * size_of::<(Sequence, Blob)>() as u64;
        work::charge(
            io,
            WorkCounters {
                allocation_operations: 1,
                peak_allocation_bytes: handles,
                ..Default::default()
            },
        )?;
        let mut blobs = Vec::new();
        blobs.try_reserve_exact(count as usize).map_err(backend)?;
        let mut payload_bytes = 0;
        for offset in 0..count {
            let sequence = Sequence::new(start + offset);
            work::charge(io, authority_fixed_read_work(0))?;
            let blob = io
                .blob(&tx, AUTHORITY_COMMITS, &commit_key(authority, sequence))
                .await?;
            let payload = self.payload(&blob)?;
            if sequence != after {
                if payload_bytes + payload > limit.payload_bytes {
                    if payload_bytes == 0 {
                        return Err(AuthorityStoreError::ReplayRecordTooLarge {
                            observed: payload,
                            maximum: limit.payload_bytes,
                        });
                    }
                    break;
                }
                payload_bytes += payload;
            }
            blobs.push((sequence, blob));
        }
        drop(tx);
        let mut retained = blobs.capacity() as u64 * size_of::<(Sequence, Blob)>() as u64
            + blobs.len() as u64 * size_of::<DurableCommit>() as u64;
        work::charge(
            io,
            WorkCounters {
                allocation_operations: 1,
                peak_allocation_bytes: retained,
                ..Default::default()
            },
        )?;
        let mut output = Vec::new();
        output.try_reserve_exact(blobs.len()).map_err(backend)?;
        let mut previous = crate::Digest::ZERO;
        for (sequence, blob) in blobs {
            let encoded = self.payload(&blob)? + COMMIT_PREFIX_BYTES as u64;
            let commit = self.decode(io, authority, &blob, retained).await?;
            if commit.sequence != sequence {
                return Err(corrupt("commit sequence does not match key"));
            }
            if sequence == after {
                previous = commit.digest;
                continue;
            }
            if commit.previous_digest != previous {
                return Err(corrupt("discontinuous authority chain"));
            }
            previous = commit.digest;
            retained += encoded;
            output.push(commit);
        }
        if output.last().is_some_and(|c| c.sequence == head.sequence) && previous != head.digest {
            return Err(corrupt("head digest does not match terminal commit"));
        }
        Ok((output, None))
    }
}

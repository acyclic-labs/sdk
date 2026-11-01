use super::*;

pub(in crate::distributed) struct PreparedGroup<'a> {
    pub(in crate::distributed) anchor: AuthoritySnapshot,
    pub(in crate::distributed) receipts: crate::kernel::Owned<Vec<AppendReceipt<'a>>>,
    pub(in crate::distributed) outcomes: crate::kernel::Owned<Vec<FsAppendOutcome>>,
    pub(in crate::distributed) transaction: CommitRequest,
    pub(in crate::distributed) deadline: Option<u64>,
    deadline_owner: usize,
}

impl PreparedGroup<'_> {
    pub(in crate::distributed) fn project(
        &self,
        placement: &AuthorityPlacement,
        projection: crate::distributed::actor::Projection,
        work: &mut AuthorityWork<'_>,
    ) -> Result<crate::distributed::actor::PreparedProjection, AuthorityStoreError> {
        let first = self
            .receipts
            .value
            .first()
            .ok_or(AuthorityStoreError::RejectedStatic("empty prepared group"))?;
        let last = self
            .receipts
            .value
            .last()
            .ok_or(AuthorityStoreError::RejectedStatic("empty prepared group"))?;
        projection.prepare(
            Head {
                epoch: last.epoch,
                sequence: last.durable.sequence,
                digest: last.durable.digest,
            },
            (first.commit.operation_id, last.commit.publication.as_ref()),
            &last.records,
            &self.transaction,
            (placement, first.authority_id),
            work,
        )
    }
}

pub(in crate::distributed) enum GroupPreparation<'a> {
    Ready(PreparedGroup<'a>),
    Rejected {
        index: usize,
        outcome: FsAppendOutcome,
        observed: Head,
    },
}

impl GroupPreparation<'_> {
    fn rejected(index: usize, outcome: FsAppendOutcome, observed: Head) -> Self {
        // Preparation may have advanced a provisional prefix. No member of a
        // rejected group can advertise that prefix as an authority head.
        let outcome = match outcome {
            FsAppendOutcome::Conflict { .. } => FsAppendOutcome::Conflict { actual: observed },
            outcome => outcome,
        };
        Self::Rejected {
            index,
            outcome,
            observed,
        }
    }
}

pub(in crate::distributed) fn group_first<'a>(
    requests: &'a [crate::GuardedAppend],
    cuts: &[crate::PolicyCut],
) -> Result<&'a crate::GuardedAppend, AuthorityStoreError> {
    if cuts.len() > requests.len() {
        return Err(AuthorityStoreError::RejectedStatic(
            "authority group has more policy cuts than members",
        ));
    }
    requests
        .first()
        .ok_or(AuthorityStoreError::RejectedStatic("empty authority group"))
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    /// Appends a precomposed serial history in one atomic Streams transaction.
    /// Every request must name the same authority and epoch; each expected head
    /// must be exactly its predecessor's authenticated durable head. This API
    /// does not certify namespace independence: composition must do that before
    /// constructing these requests. All leases, locators, changes and lineage
    /// fences still go through the ordinary append preparation implementation.
    /// Native completion storage is funded before submission. After an
    /// authenticated native commit, caller cancellation or a busy projection
    /// cache cannot replace or delay delivery of its original member receipts.
    pub async fn compare_and_append_group<G: AuthorityGuard>(
        &self,
        requests: &[crate::GuardedAppend],
        cuts: &[crate::PolicyCut],
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<AppendGroupOutcome, AuthorityError<G::Error>> {
        let first = group_first(requests, cuts)?;
        let actor = self.actor(first.authority_id, work)?;
        let mut journal = actor.lease(work).await?;
        work.check()?;
        let projection = self
            .activate(
                first.authority_id,
                first.commit.publication.as_ref().map(|seal| seal.volume()),
                &mut journal,
                work,
            )
            .await?;
        let revision = actor.begin_publication(journal)?;
        let prepared = self
            .prepare_group(requests, work, guard.as_deref_mut(), Some(&projection))
            .await?;
        let prepared = match prepared {
            GroupPreparation::Ready(prepared) => prepared,
            GroupPreparation::Rejected {
                index,
                outcome,
                observed,
            } => {
                // Preparation issued no durability request. Restore the
                // verified frontier only if no newer actor revision won
                // while admission or a caller guard was in flight.
                actor.complete(revision, projection, work).await?;
                return Ok(AppendGroupOutcome::Rejected {
                    index,
                    outcome,
                    observed,
                });
            }
        };
        let next_projection = prepared.project(&self.placement, projection, work)?;
        let PreparedGroup {
            anchor,
            receipts: prepared,
            mut outcomes,
            transaction,
            deadline,
            deadline_owner,
        } = prepared;
        // Once I/O starts, only its authenticated receipt can reactivate
        // this projection. Dropping or failing a continuation cannot leave
        // the previous head available for a new writer.
        let output = self
            .publish_with_policy_cuts(transaction, deadline, cuts, work, guard.as_deref_mut())
            .await;
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                let index = error_owner(&error, deadline_owner);
                let member =
                    prepared
                        .value
                        .get(index)
                        .ok_or(AuthorityStoreError::RejectedStatic(
                            "missing group error owner",
                        ))?;
                let outcome = self
                    .recover_append_error(member, error, work, guard.as_deref_mut())
                    .await?;
                return Ok(AppendGroupOutcome::Rejected {
                    index,
                    outcome,
                    observed: anchor.head,
                });
            }
        };
        self.settle_group(
            &prepared.value,
            anchor,
            &output.0,
            &mut outcomes.value,
            work,
            guard.as_deref_mut(),
        )
        .await?;
        if let CommitOutcome::Committed(envelope) = &output.0 {
            // The receipt is final. A busy cache cannot delay its delivery
            // or require another admission after physical publication.
            actor.complete_if_available(revision, next_projection.seal(envelope.commit_id));
        }
        let (receipts, allocation) = crate::async_storage::authority_parts(outcomes);
        Ok(AppendGroupOutcome::Published {
            receipts,
            allocation,
        })
    }

    pub(in crate::distributed) async fn settle_group<G: AuthorityGuard>(
        &self,
        prepared: &[AppendReceipt<'_>],
        anchor: AuthoritySnapshot,
        output: &CommitOutcome,
        receipts: &mut Vec<FsAppendOutcome>,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<(), AuthorityError<G::Error>> {
        let conflicts = match output {
            CommitOutcome::Committed(envelope) => {
                let first = prepared.first().ok_or_else(|| {
                    authority_corrupt("committed authority group has no members", work)
                })?;
                let last = prepared.last().ok_or_else(|| {
                    authority_corrupt("committed authority group has no members", work)
                })?;
                // Preparation proved a dense history on this one authority.
                // Authenticate its whole range once, then project each member.
                if !envelope.appends.iter().any(|append| {
                    append.path == first.records
                        && append.start <= first.durable.sequence.get()
                        && last.durable.sequence.get() < append.tail
                }) {
                    return Err(authority_corrupt(
                        "commit envelope omits its authority record",
                        work,
                    )
                    .into());
                }
                for member in prepared {
                    receipts.push(FsAppendOutcome::Committed(durable_commit(
                        member.durable,
                        envelope.committed_at_micros,
                        member.commit.payload.clone(),
                    )));
                }
                return Ok(());
            }
            CommitOutcome::Conflict(conflicts) => conflicts,
        };
        let mut recovered = work.vec_owned(prepared.len())?;
        recovered.value.resize_with(prepared.len(), || None);
        let actual = self
            .recover_conflict_head(
                prepared
                    .first()
                    .ok_or(AuthorityStoreError::RejectedStatic("empty prepared group"))?
                    .authority_id,
                anchor,
                conflicts,
                work,
                |durable, record, work| {
                    for (member, recovered) in prepared.iter().zip(&mut recovered.value) {
                        visit(work)?;
                        if durable.operation_id != member.commit.operation_id {
                            continue;
                        }
                        if recovered.is_some() {
                            return Err(authority_corrupt(
                                "authority suffix duplicates an operation",
                                work,
                            ));
                        }
                        let payload = work.bytes(durable.payload)?;
                        *recovered = Some((
                            durable_commit(durable, record.committed_at_micros, payload),
                            super::super::operations::OperationRecordProof::new(
                                durable, record, work,
                            )?,
                        ));
                    }
                    Ok(())
                },
            )
            .await?;
        for (index, member) in prepared.iter().enumerate() {
            let recovered = match recovered.value.get_mut(index).and_then(Option::take) {
                Some((commit, proof)) => {
                    self.verify_recovered_operation(
                        member.authority_id,
                        commit.operation_id,
                        &proof,
                        work,
                    )
                    .await?;
                    Some(commit)
                }
                None => {
                    let operation = work.operation_path(
                        &self.placement,
                        member.authority_id,
                        member.commit.operation_id,
                    )?;
                    if conflicts.iter().any(|conflict| {
                        matches!(conflict, acyclic_stream::CommitConflict::Exists { path } if path == &operation)
                    }) {
                        Some(
                            self.authority_find_operation(
                                member.authority_id,
                                member.commit.operation_id,
                                work,
                            )
                            .await?
                            .ok_or_else(|| {
                                authority_corrupt(
                                    "rejected operation locator has no authenticated receipt",
                                    work,
                                )
                            })?,
                        )
                    } else {
                        None
                    }
                }
            };
            receipts.push(
                Self::settle_conflicted_append(
                    member,
                    conflicts,
                    actual,
                    recovered,
                    work,
                    guard.as_deref_mut(),
                )
                .await?,
            );
        }
        drop(recovered);
        work.check()?;
        Ok(())
    }

    pub(in crate::distributed) async fn prepare_group<'a, G: AuthorityGuard>(
        &self,
        requests: &'a [crate::GuardedAppend],
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
        projection: Option<&super::super::actor::Projection>,
    ) -> Result<GroupPreparation<'a>, AuthorityError<G::Error>> {
        let first = requests
            .first()
            .ok_or(AuthorityStoreError::RejectedStatic("empty authority group"))?;
        // Include each member's policy fences in the one shared provider bound.
        if requests.len() > crate::async_storage::MAX_AUTHORITY_GROUP_MEMBERS {
            return Err(AuthorityStoreError::NativeStream(
                acyclic_stream::StreamError::LimitExceeded,
            )
            .into());
        }
        let anchor = match projection {
            Some(projection) => projection.snapshot,
            None => self.fork_snapshot(first.authority_id, work).await?,
        };
        let mut cursor = AppendCursor::Durable(anchor);
        let mut receipts = work.vec_owned(requests.len())?;
        let mut transaction = None;
        let mut deadline: Option<u64> = None;
        let mut deadline_owner = 0;
        for (index, request) in requests.iter().enumerate() {
            visit(work)?;
            if request.authority_id != first.authority_id || request.epoch != first.epoch {
                return Err(AuthorityStoreError::RejectedStatic(
                    "authority group crosses writer fences",
                )
                .into());
            }
            let view = AppendView {
                cursor: Some(cursor),
                projection,
                transaction: transaction.as_ref(),
            };
            reject_duplicate_operation(&receipts.value, request.commit.operation_id, work)?;
            let prepared = match self
                .prepare_append(request, &view, work, guard.as_deref_mut(), Publication)
                .await?
            {
                Preparation::Ready(prepared) => prepared,
                Preparation::Outcome(outcome) => {
                    return Ok(GroupPreparation::rejected(index, outcome, anchor.head));
                }
            };
            cursor = cursor.advance(Head {
                epoch: prepared.receipt.epoch,
                sequence: prepared.receipt.durable.sequence,
                digest: prepared.receipt.durable.digest,
            })?;
            if prepared
                .deadline
                .is_some_and(|next| deadline.is_none_or(|prior| next < prior))
            {
                deadline_owner = index;
            }
            deadline = match (deadline, prepared.deadline) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            match &mut transaction {
                None => transaction = Some(prepared.request),
                Some(transaction) => extend_transaction(transaction, prepared.request, work)?,
            }
            receipts.value.push(prepared.receipt);
        }
        Ok(GroupPreparation::Ready(PreparedGroup {
            anchor,
            receipts,
            // Terminal reply storage belongs to the original preparation and
            // is admitted before the provider can accept physical publication.
            outcomes: work.vec_owned(requests.len())?,
            transaction: transaction
                .ok_or(AuthorityStoreError::RejectedStatic("empty prepared group"))?,
            deadline,
            deadline_owner,
        }))
    }
}

fn error_owner<E>(error: &AuthorityError<E>, deadline_owner: usize) -> usize {
    if matches!(
        error,
        AuthorityError::Authority(AuthorityStoreError::NativeStream(
            acyclic_stream::StreamError::DeadlineElapsed
        ))
    ) {
        deadline_owner
    } else {
        0
    }
}

fn reject_duplicate_operation(
    receipts: &[AppendReceipt<'_>],
    operation: OperationId,
    work: &mut AuthorityWork<'_>,
) -> Result<(), AuthorityStoreError> {
    for receipt in receipts {
        visit(work)?;
        if receipt.commit.operation_id == operation {
            return Err(AuthorityStoreError::RejectedStatic(
                "duplicate operation in authority group",
            ));
        }
    }
    Ok(())
}

fn extend_transaction(
    prefix: &mut CommitRequest,
    next: CommitRequest,
    work: &mut AuthorityWork<'_>,
) -> Result<(), AuthorityStoreError> {
    let key = work.folded_key(
        b"authority-group-attempt\0",
        &prefix.idempotency_key,
        &next.idempotency_key,
    )?;
    merge_transaction(prefix, next, work)?;
    prefix.idempotency_key = key;
    Ok(())
}

// Both authority groups and caller compositions use the same ordered append
// merge. The caller's original key is retained; a group's key is folded above.
pub(in crate::distributed) fn merge_transaction(
    prefix: &mut CommitRequest,
    next: CommitRequest,
    work: &mut AuthorityWork<'_>,
) -> Result<(), AuthorityStoreError> {
    let view = AppendView {
        cursor: None,
        projection: None,
        transaction: Some(prefix),
    };
    let mut conditions = work.vec(prefix.conditions.len() + next.conditions.len())?;
    for condition in next.conditions {
        let path = super::super::policy::condition_path(&condition);
        if let Some((anchor, records)) = view.staged(path, work)? {
            let tail = anchor
                .checked_add(crate::foundation::usize_to_u64(records.len()))
                .ok_or(crate::WorkError::Overflow)?;
            if !matches!(&condition, CommitCondition::Tail { expected, .. } if *expected == tail) {
                return Err(AuthorityStoreError::RejectedStatic(
                    "group fence disagrees with prepared prefix",
                ));
            }
        } else {
            conditions.push(condition);
        }
    }
    let mut merged = work.vec(prefix.conditions.len() + conditions.len())?;
    merged.append(&mut prefix.conditions);
    for condition in conditions {
        super::super::policy::merge_condition(&mut merged, condition, work)?;
    }
    prefix.conditions = merged;
    let mut mutations = work.vec(prefix.mutations.len() + next.mutations.len())?;
    mutations.append(&mut prefix.mutations);
    for mutation in next.mutations {
        merge_append(&mut mutations, mutation, work)?;
    }
    prefix.mutations = mutations;
    prefix.deadline_unix_millis = match (prefix.deadline_unix_millis, next.deadline_unix_millis) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    Ok(())
}

fn merge_append(
    mutations: &mut Vec<CommitMutation>,
    mutation: CommitMutation,
    work: &mut AuthorityWork<'_>,
) -> Result<(), AuthorityStoreError> {
    let CommitMutation::Append { path, mut records } = mutation else {
        return Err(AuthorityStoreError::RejectedStatic(
            "authority group contains a non-append mutation",
        ));
    };
    for existing in mutations.iter_mut() {
        visit(work)?;
        if let CommitMutation::Append {
            path: destination,
            records: prior,
        } = existing
            && *destination == path
        {
            let mut merged = work.vec(prior.len() + records.len())?;
            merged.append(prior);
            merged.append(&mut records);
            *prior = merged;
            return Ok(());
        }
    }
    mutations.push(CommitMutation::Append { path, records });
    Ok(())
}

fn visit(work: &mut AuthorityWork<'_>) -> Result<(), AuthorityStoreError> {
    super::view::visit(work)
}

#[cfg(test)]
mod quotation_tests {
    use super::*;

    #[tokio::test]
    async fn additive_quotes_cover_the_canonical_merged_command()
    -> Result<(), Box<dyn std::error::Error>> {
        for count in [1, 4, 16, 64] {
            let (store, authority) = super::super::group_tests::fixture().await?;
            let members = super::super::group_tests::requests(authority, count)?;
            let cancellation = CancellationToken::new();
            let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let mut capacity = crate::AuthorityGroupCost::default();
            for member in &members {
                let mut quote = member.clone();
                quote.expected = Head::genesis(Epoch::GENESIS);
                let cost = store
                    .quote_group_append(&quote, 0, &mut work)
                    .await?
                    .ok_or("missing quote")?;
                assert!(capacity.admit(cost));
            }
            let GroupPreparation::Ready(group) = store
                .prepare_group(&members, &mut work, None::<&mut ()>, None)
                .await?
            else {
                return Err("prepared group was rejected".into());
            };
            assert!(
                acyclic_stream::request::commit_encoded_len(&group.transaction) <= capacity.bytes,
                "additive admission underestimated the actual provider command"
            );
        }
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod identity_tests;

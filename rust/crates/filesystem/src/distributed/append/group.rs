use super::*;

struct PreparedGroup<'a> {
    anchor: AuthoritySnapshot,
    receipts: crate::kernel::Owned<Vec<AppendReceipt<'a>>>,
    transaction: CommitRequest,
    deadline: Option<u64>,
    deadline_owner: usize,
}

impl PreparedGroup<'_> {
    fn project(
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

enum GroupPreparation<'a> {
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

fn group_first<'a>(
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
        let mut child = work.operation()?;
        let result = async {
            let projection = self
                .activate(
                    first.authority_id,
                    first.commit.publication.as_ref().map(|seal| seal.volume()),
                    &mut journal,
                    &mut child,
                )
                .await?;
            let revision = actor.begin_publication(journal)?;
            let prepared = self
                .prepare_group(
                    requests,
                    &mut child,
                    guard.as_deref_mut(),
                    Some(&projection),
                )
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
                    actor.complete(revision, projection, &mut child).await?;
                    return Ok(AppendGroupOutcome::Rejected {
                        index,
                        outcome,
                        observed,
                    });
                }
            };
            let next_projection = prepared.project(&self.placement, projection, &mut child)?;
            let PreparedGroup {
                anchor,
                receipts: prepared,
                transaction,
                deadline,
                deadline_owner,
            } = prepared;
            // Once I/O starts, only its authenticated receipt can reactivate
            // this projection. Dropping or failing a continuation cannot leave
            // the previous head available for a new writer.
            let output = self
                .publish_with_policy_cuts(
                    transaction,
                    deadline,
                    cuts,
                    &mut child,
                    guard.as_deref_mut(),
                )
                .await;
            let output =
                match output {
                    Ok(output) => output,
                    Err(error) => {
                        let index = error_owner(&error, deadline_owner);
                        let member = prepared.value.get(index).ok_or(
                            AuthorityStoreError::RejectedStatic("missing group error owner"),
                        )?;
                        let outcome = self
                            .recover_append_error(member, error, &mut child, guard.as_deref_mut())
                            .await?;
                        return Ok(AppendGroupOutcome::Rejected {
                            index,
                            outcome,
                            observed: anchor.head,
                        });
                    }
                };
            let result = self
                .settle_group(
                    &prepared.value,
                    anchor,
                    &output.0,
                    &mut child,
                    guard.as_deref_mut(),
                )
                .await?;
            if let CommitOutcome::Committed(envelope) = &output.0 {
                actor
                    .complete(
                        revision,
                        next_projection.seal(envelope.commit_id),
                        &mut child,
                    )
                    .await?;
            }
            Ok(result)
        }
        .await;
        work.record(child.spent())?;
        result
    }

    async fn settle_group<G: AuthorityGuard>(
        &self,
        prepared: &[AppendReceipt<'_>],
        anchor: AuthoritySnapshot,
        output: &CommitOutcome,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<AppendGroupOutcome, AuthorityError<G::Error>> {
        let recovery_count = if matches!(output, CommitOutcome::Conflict(_)) {
            prepared.len()
        } else {
            0
        };
        let mut recovered = work.vec_owned(recovery_count)?;
        recovered.value.resize_with(recovery_count, || None);
        let actual = match output {
            CommitOutcome::Committed(_) => None,
            CommitOutcome::Conflict(conflicts) => Some(
                self.recover_conflict_head(
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
                .await?,
            ),
        };
        let mut receipts = work.vec_owned(prepared.len())?;
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
                    if let CommitOutcome::Conflict(conflicts) = output {
                        let operation = work.operation_path(
                            &self.placement,
                            member.authority_id,
                            member.commit.operation_id,
                        )?;
                        if conflicts.iter().any(|conflict| matches!(conflict, acyclic_stream::CommitConflict::Exists { path } if path == &operation)) {
                            Some(self.authority_find_operation(member.authority_id, member.commit.operation_id, work).await?
                                .ok_or_else(|| authority_corrupt("rejected operation locator has no authenticated receipt", work))?)
                        } else { None }
                    } else {
                        None
                    }
                }
            };
            receipts.value.push(
                Self::settle_append(
                    member,
                    output,
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
        let (receipts, allocation) = crate::async_storage::authority_parts(receipts);
        Ok(AppendGroupOutcome::Published {
            receipts,
            allocation,
        })
    }

    async fn prepare_group<'a, G: AuthorityGuard>(
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
        super::super::policy::merge_policy_condition(&mut merged, condition, work)?;
    }
    prefix.conditions = merged;
    let mut mutations = work.vec(prefix.mutations.len() + next.mutations.len())?;
    mutations.append(&mut prefix.mutations);
    for mutation in next.mutations {
        merge_append(&mut mutations, mutation, work)?;
    }
    prefix.mutations = mutations;
    prefix.idempotency_key = work.folded_key(
        b"authority-group-attempt\0",
        &prefix.idempotency_key,
        &next.idempotency_key,
    )?;
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

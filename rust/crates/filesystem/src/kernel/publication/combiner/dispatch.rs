use super::*;

impl Ticket {
    fn restore_completed_work(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        let completed = self
            .proposal
            .prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .completed_work
            .take();
        if let Some(completed) = completed {
            completed.restore(accounting)?;
        }
        Ok(())
    }

    pub(in crate::kernel::publication) async fn submit<P: PublicationOwner>(
        &mut self,
        owner: &P,
        append: GuardedAppend,
        seal: Option<SharedSeal>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<AppendOutcome, crate::FsError> {
        let cancelled = accounting.cancellation_wait(cancellation)?;
        tokio::pin!(cancelled);
        {
            let mut prepared = self
                .proposal
                .prepared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            prepared.append = Some(append);
            prepared.seal = seal;
        }
        let leader = self.index.is_some_and(|index| {
            self.group
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .ready(index)
        });
        if leader {
            let result = seal_group(owner, &self.group, &self.proposal, accounting, cancellation);
            if let Err(error) = result {
                self.abort();
                self.restore_completed_work(accounting)?;
                return Err(error);
            }
            self.group.signal.notify_waiters();
        }
        loop {
            let notified = self.group.signal.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            accounting.check()?;
            let (phase, abort_cause) = {
                let state = self
                    .group
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (state.phase, state.abort_cause)
            };
            if matches!(phase, Phase::Complete | Phase::Aborted) {
                let mut prepared = self
                    .proposal
                    .prepared
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(completed) = prepared.completed_work.take() {
                    completed.restore(accounting)?;
                }
                if phase == Phase::Aborted {
                    return Err(prepared
                        .error
                        .take()
                        .unwrap_or_else(|| PublicationError::GroupAborted(abort_cause).into()));
                }
                let outcome = prepared
                    .outcome
                    .take()
                    .ok_or(crate::FsError::InvalidAuthorityHistory)?;
                self.settled = true;
                return Ok(outcome);
            }
            wait_for_notification(notified.as_mut(), cancelled.as_mut()).await?;
        }
    }
}

fn seal_group<P: PublicationOwner>(
    owner: &P,
    group: &RetainedArc<Group>,
    leader: &Arc<Proposal>,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), crate::FsError> {
    let count = group
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .selected()
        .count();
    let requests = Owned::<Vec<GuardedAppend>>::allocate(count, accounting)?;
    let mut requests = Retained::from_owned(requests, accounting)?;
    let cuts = Owned::<Vec<crate::PolicyCut>>::allocate(count, accounting)?;
    let mut cuts = Retained::from_owned(cuts, accounting)?;
    let mut preparation_work = backend_work(accounting, cancellation)?;
    let result = prepare_requests(group, &mut requests.value, &mut preparation_work);
    let completed = preparation_work.spent();
    drop(preparation_work);
    if accounting.standalone_parts().is_some() {
        accounting.accept_backend(completed)?;
    }
    let head = result?;
    {
        let state = group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for proposal in state.selected() {
            if let Some(cut) = proposal.core.cut {
                cuts.value.push(cut);
            }
        }
    }
    let pending = PendingBatch::prepare(group, head, accounting)?;
    #[cfg(not(target_arch = "wasm32"))]
    if accounting.native_work().is_some() {
        let guard = crate::NativeAuthorityGuard(native_seal(group.clone()));
        return handoff(
            owner,
            group,
            leader,
            DispatchInput {
                requests,
                cuts,
                pending,
                guard: Some(guard),
                owner_error: std::convert::identity,
            },
            accounting,
            cancellation,
        );
    }
    handoff(
        owner,
        group,
        leader,
        DispatchInput {
            requests,
            cuts,
            pending,
            guard: None::<()>,
            owner_error: crate::FsError::Authority,
        },
        accounting,
        cancellation,
    )
}

struct DispatchInput<G: crate::OwnedAuthorityGuard> {
    requests: Retained<Vec<GuardedAppend>>,
    cuts: Retained<Vec<crate::PolicyCut>>,
    pending: PendingBatch,
    guard: Option<G>,
    owner_error: fn(G::Error) -> crate::FsError,
}

fn handoff<P: PublicationOwner, G: crate::OwnedAuthorityGuard>(
    owner: &P,
    group: &RetainedArc<Group>,
    leader: &Arc<Proposal>,
    input: DispatchInput<G>,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), crate::FsError> {
    let DispatchInput {
        requests,
        cuts,
        pending,
        guard,
        owner_error,
    } = input;
    let dispatch_group = group.clone();
    let leader = Arc::clone(leader);
    let storage = owner.clone();
    let batch = crate::OwnedAuthorityGroup::new(
        requests,
        cuts,
        accounting,
        cancellation,
        guard,
        move |outcome, completed_work| {
            let _storage = storage;
            let result = outcome
                .map_err(|error| match error {
                    crate::AuthorityError::Authority(error) => crate::FsError::Authority(error),
                    crate::AuthorityError::Owner(error) => owner_error(error),
                })
                .and_then(|outcome| {
                    distribute(&dispatch_group, outcome)?;
                    let sealed = dispatch_group
                        .sealed
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .committed();
                    if let Some(sealed) = sealed {
                        pending.commit(&dispatch_group.volume, sealed);
                    }
                    Ok(())
                });
            let failed = result.is_err();
            {
                let mut prepared = leader
                    .prepared
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                prepared.completed_work = Some(completed_work);
                if let Err(error) = result {
                    prepared.error = Some(error);
                }
            }
            {
                let mut state = dispatch_group
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if failed || !state.complete() {
                    state.abort();
                }
            }
            dispatch_group.signal.notify_waiters();
        },
    )?;
    {
        let mut state = group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.handoff() {
            return Err(PublicationError::GroupAborted(GroupAbort::PhaseEnded).into());
        }
    }
    if let Some(task) = owner.authority().own_group(batch) {
        acyclic_stream::runtime::PlatformExecutor.spawn_owned(task);
    }
    Ok(())
}

fn prepare_requests(
    group: &Group,
    requests: &mut Vec<GuardedAppend>,
    work: &mut crate::AuthorityWork<'_>,
) -> Result<Head, crate::FsError> {
    let state = group
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if state.phase != Phase::Sealing {
        return Err(PublicationError::GroupAborted(GroupAbort::PhaseEnded).into());
    }
    let mut expected = None;
    for proposal in state.selected() {
        work.admit(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let mut request = proposal
            .prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .append
            .take()
            .ok_or(crate::FsError::InvalidDiff)?;
        let predecessor = expected.unwrap_or(request.expected);
        if let Some(changes) = request.commit.publication.as_mut() {
            changes.compose_base(request.expected, predecessor)?;
        }
        request.expected = predecessor;
        let sequence = predecessor
            .sequence
            .checked_next()
            .map_err(|_| crate::FsError::InvalidAuthorityHistory)?;
        let digest = work.durable_digest(
            request.authority_id,
            &crate::streams_record::BorrowedStreamsDurableRecord {
                epoch: request.epoch,
                sequence,
                operation_id: request.commit.operation_id,
                fingerprint: request.commit.fingerprint,
                previous_digest: predecessor.digest,
                digest: Digest::ZERO,
                payload: &request.commit.payload,
            },
        )?;
        expected = Some(Head {
            epoch: request.epoch,
            sequence,
            digest,
        });
        requests.push(request);
        work.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
    }
    expected.ok_or(crate::FsError::InvalidAuthorityHistory)
}

fn distribute(group: &Group, outcome: crate::AppendGroupOutcome) -> Result<(), crate::FsError> {
    let state = group
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if state.phase != Phase::Appending {
        return Err(PublicationError::GroupAborted(GroupAbort::PhaseEnded).into());
    }
    match outcome {
        crate::AppendGroupOutcome::Published {
            mut receipts,
            allocation,
        } => {
            if receipts.len() != state.selected().count() {
                return Err(crate::FsError::InvalidAuthorityHistory);
            }
            if receipts
                .iter()
                .all(|outcome| matches!(outcome, AppendOutcome::Committed(_)))
            {
                let Some(AppendOutcome::Committed(last)) = receipts.last() else {
                    return Err(crate::FsError::InvalidAuthorityHistory);
                };
                let root = state
                    .tail()
                    .ok_or(crate::FsError::InvalidAuthorityHistory)?;
                group
                    .sealed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .commit(Anchor {
                        head: Head {
                            epoch: last.epoch,
                            sequence: last.sequence,
                            digest: last.digest,
                        },
                        base: root,
                        root,
                    })?;
            }
            for (proposal, outcome) in state.selected().zip(receipts.drain(..)) {
                proposal
                    .prepared
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .outcome = Some(outcome);
            }
            drop(receipts);
            drop(allocation);
        }
        crate::AppendGroupOutcome::Rejected {
            index,
            outcome,
            observed,
        } => {
            let mut rejected = Some(outcome);
            let actual = observed;
            for (position, proposal) in state.selected().enumerate() {
                let outcome = if position == index {
                    rejected
                        .take()
                        .ok_or(crate::FsError::InvalidAuthorityHistory)?
                } else if proposal.core.request.epoch != actual.epoch {
                    AppendOutcome::Fenced {
                        actual_epoch: actual.epoch,
                    }
                } else {
                    AppendOutcome::Conflict { actual }
                };
                proposal
                    .prepared
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .outcome = Some(outcome);
            }
            if rejected.is_some() {
                return Err(crate::FsError::InvalidAuthorityHistory);
            }
        }
    }
    Ok(())
}

pub(super) fn backend_work<'a>(
    accounting: &KernelAccounting<'a>,
    cancellation: &'a CancellationToken,
) -> Result<crate::AuthorityWork<'a>, crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        return Ok(crate::AuthorityWork::native(original, cancellation));
    }
    Ok(crate::AuthorityWork::new(
        accounting.remaining()?,
        cancellation,
    ))
}

pub(super) fn merge_work(
    accounting: &mut KernelAccounting<'_>,
    work: &crate::AuthorityWork<'_>,
) -> Result<(), crate::FsError> {
    if accounting.standalone_parts().is_some() {
        accept_backend(accounting, work.spent())?;
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn native_seal(
    group: RetainedArc<Group>,
) -> impl FnMut(&crate::NativeMaterializationWork) -> SealFuture + Clone + Send {
    move |work| {
        let group = group.clone();
        match crate::heap_future::native_future(work, || async move { seal_members(&group).await })
            .map(crate::heap_future::OwnedFuture::erase_send)
            .map_err(crate::FsError::from)
        {
            Ok(future) => futures::future::Either::Right(future),
            Err(error) => futures::future::Either::Left(std::future::ready(Err(error))),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn seal_members(group: &Group) -> Result<(), crate::FsError> {
    let count = group
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .members
        .len();
    for index in 0..count {
        let notified = group.signal.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let proposal = {
            let state = group
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.phase != Phase::Appending {
                return Err(PublicationError::GroupAborted(GroupAbort::PhaseEnded).into());
            }
            match state.selected_at(index) {
                Some((_, true)) => continue,
                Some((proposal, false)) => Arc::clone(proposal),
                None => continue,
            }
        };
        let mut seal = proposal
            .prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .seal
            .take()
            .ok_or(crate::FsError::InvalidDiff)?;
        let result = {
            let future = (seal.callback)();
            tokio::pin!(future);
            loop {
                tokio::select! {
                    biased;
                    () = notified.as_mut() => {
                        notified.set(group.signal.notified());
                        notified.as_mut().enable();
                        let state = group
                            .state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if state.phase != Phase::Appending {
                            break Err(PublicationError::GroupAborted(GroupAbort::PhaseEnded).into());
                        }
                        // The authority task already owns the immutable append.
                        // Caller loss ends only this caller's pending seal.
                        if state.selected_at(index).is_some_and(|(_, withdrawn)| withdrawn) {
                            break Ok(());
                        }
                    }
                    result = future.as_mut() => break result,
                }
            }
        };
        proposal
            .prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .seal = Some(seal);
        result?;
    }
    Ok(())
}

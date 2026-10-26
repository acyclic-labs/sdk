//! One funded continuation owns every selected prefix and its collection pins.
use super::*;

type Prefix = (ObjectId, Option<ObjectId>);

pub(super) struct Funding {
    pub(super) work: crate::kernel::allocation::retained::OwnedAccounting,
    pub(super) limits: ClosureLimits,
}
pub(super) type PrefixResult = Result<Option<Prefix>, crate::FsError>;

struct Generation {
    root: ObjectId,
    authentication: Retained<super::super::authenticate::Authenticated>,
    pin: Option<crate::collection::Hold>,
}

struct Job<P> {
    group: RetainedArc<Group>,
    sponsor: Arc<Proposal>,
    finished: bool,
    // Release prefix payloads and pins before the actual store lifecycle.
    owner: P,
}

impl<P> Job<P> {
    fn return_work(proposal: &Proposal, result: Result<Option<Prefix>, crate::FsError>) {
        let mut prepared = proposal
            .prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(funding) = prepared.prefix_work.take() {
            prepared.completed_work = Some(funding.work);
            if prepared.prefix_result.is_none() {
                prepared.prefix_result = Some(result);
            }
        }
    }
}

impl<P> Drop for Job<P> {
    fn drop(&mut self) {
        let mut state = self
            .group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.finished {
            state.abort();
        }
        for member in &state.members {
            Self::return_work(&member.value, Err(PublicationError::GroupAborted.into()));
        }
        drop(state);
        self.group.signal.notify_waiters();
    }
}

impl<P: PublicationOwner> Job<P> {
    fn collect(&self) -> Result<(), crate::FsError> {
        let mut state = self
            .group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase == Phase::Aborted {
            return Err(PublicationError::GroupAborted.into());
        }
        let mut prepared = self
            .sponsor
            .prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let funding = prepared
            .prefix_work
            .as_mut()
            .ok_or(crate::FsError::AccountingHandoffActive)?;
        let mut accounting = funding.work.borrow();
        let mut capacity = crate::AuthorityGroupCost::default();
        state.collect(
            |a, b| independent(a, b, &mut accounting),
            |proposal| Ok(capacity.admit(proposal.admission()?.cost)),
        )?;
        drop(prepared);
        self.group.signal.notify_waiters();
        Ok(())
    }

    async fn prepare_member(&self, index: usize) -> Result<(), crate::FsError> {
        let (proposal, predecessor) = {
            let state = self
                .group
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let member = state
                .members
                .get(index)
                .ok_or(crate::FsError::InvalidDiff)?;
            match state.plan(index) {
                Plan::Prefix(predecessor) => (Arc::clone(&member.value), predecessor),
                Plan::Waiting | Plan::Deferred => {
                    Self::return_work(&member.value, Ok(None));
                    return Ok(());
                }
                Plan::Aborted => return Err(PublicationError::GroupAborted.into()),
            }
        };
        let funding = proposal
            .prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .prefix_work
            .take()
            .ok_or(crate::FsError::AccountingHandoffActive)?;
        let Funding { mut work, limits } = funding;
        let cancellation = work.cancellation();
        let result = self
            .prepare_root(
                &proposal,
                predecessor,
                limits,
                &mut work.borrow(),
                &cancellation,
            )
            .await;
        let mut state = self
            .group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut prepared = proposal
            .prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        prepared.completed_work = Some(work);
        prepared.prefix_result = Some(match result {
            Ok(generation) if state.planned(index, generation.root) => {
                prepared.authentication = Some(generation.authentication);
                prepared.publication_pin = generation.pin;
                Ok(Some((generation.root, predecessor)))
            }
            Ok(_) => Err(PublicationError::GroupAborted.into()),
            Err(error) => Err(error),
        });
        let failed = matches!(prepared.prefix_result, Some(Err(_)));
        if failed {
            state.abort();
        }
        drop(prepared);
        drop(state);
        self.group.signal.notify_waiters();
        if failed {
            Err(PublicationError::GroupAborted.into())
        } else {
            Ok(())
        }
    }

    async fn prepare_root(
        &self,
        proposal: &Proposal,
        predecessor: Option<ObjectId>,
        limits: ClosureLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Generation, crate::FsError> {
        let anchor = proposal.admission()?.anchor;
        let root = if let Some(predecessor) = predecessor {
            super::plan::compose_metadata(
                self.owner.objects(),
                predecessor,
                Composition {
                    delta: &proposal.core.delta.value,
                    retained_merge: proposal.core.retained_merge,
                },
                limits.decode,
                accounting,
                cancellation,
            )
            .await?
        } else {
            anchor.root
        };
        let proven_at = self.owner.objects().collection_sweeps();
        let (previous_authentication, previous_pin) = {
            let mut prepared = proposal
                .prepared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                prepared.authentication.take(),
                prepared.publication_pin.take(),
            )
        };
        let previous_authentication = match previous_authentication {
            Some(authentication)
                if authentication.value.proof.generation_id.digest() == root.digest =>
            {
                return Ok(Generation {
                    root,
                    authentication,
                    pin: previous_pin,
                });
            }
            previous => previous,
        };
        let authenticated = super::super::authenticate::prove(
            self.owner.objects(),
            root,
            Some(proposal.original_anchor().base),
            limits,
            self.owner.publication_cache().map(|cache| &cache.inner),
            accounting,
            cancellation,
        )
        .await?;
        let pinned = super::super::authenticate::retain_and_pin(
            self.owner.objects(),
            authenticated,
            proposal.core.preconditions.config,
            proven_at,
            accounting,
        )
        .await?;
        drop(previous_authentication);
        drop(previous_pin);
        Ok(Generation {
            root,
            authentication: pinned.authentication,
            pin: pinned.pin,
        })
    }

    async fn run(mut self) {
        if let Err(error) = self.collect() {
            Self::return_work(&self.sponsor, Err(error));
            return;
        }
        let count = self
            .group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .members
            .len();
        for index in 0..count {
            if self.prepare_member(index).await.is_err() {
                return;
            }
        }
        self.finished = true;
    }
}

impl Ticket {
    pub(super) async fn prepare_prefix<P: PublicationOwner>(
        &mut self,
        owner: P,
        limits: ClosureLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Prefix>, crate::FsError> {
        if self.index.is_none() {
            self.wait_deferred(accounting, cancellation).await?;
            return Ok(None);
        }
        let cancelled = accounting.cancellation_wait(cancellation)?;
        tokio::pin!(cancelled);
        self.fund_prefix(limits, accounting, cancellation)?;
        loop {
            let notified = self.group.signal.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(task) = self.start_prefix(&owner) {
                acyclic_stream::runtime::PlatformExecutor.spawn_owned(task);
            }
            if let Some(result) = self.prefix_completion(accounting)? {
                if matches!(result, Ok(None)) {
                    self.settled = true;
                }
                return result;
            }
            wait_for_notification(notified.as_mut(), cancelled.as_mut()).await?;
        }
    }

    fn fund_prefix(
        &self,
        limits: ClosureLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::FsError> {
        let state = self
            .group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase == Phase::Aborted {
            return Err(PublicationError::GroupAborted.into());
        }
        self.proposal
            .prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .prefix_work = Some(Funding {
            work: crate::kernel::allocation::retained::OwnedAccounting::take_branch(
                accounting,
                cancellation,
            )?,
            limits,
        });
        self.group.signal.notify_waiters();
        Ok(())
    }

    fn start_prefix<P: PublicationOwner>(&self, owner: &P) -> Option<crate::OwnedAuthorityTask> {
        let leader = {
            let mut state = self
                .group
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.can_collect(|proposal| {
                proposal
                    .admission
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .ready()
                    .is_some()
                    && proposal
                        .prepared
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .prefix_work
                        .is_some()
            }) && state.own_preparation()
        };
        let task = if leader {
            let job = Job {
                owner: owner.clone(),
                group: self.group.clone(),
                sponsor: Arc::clone(&self.proposal),
                finished: false,
            };
            let task = crate::OwnedAuthorityTask::admit(
                job,
                |job, bytes| {
                    let mut prepared = job
                        .sponsor
                        .prepared
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let result = prepared
                        .prefix_work
                        .as_mut()
                        .ok_or(crate::FsError::AccountingHandoffActive)
                        .and_then(|work| {
                            RetainedAllocation::claim(
                                bytes,
                                u64::from(bytes != 0),
                                &mut work.work.borrow(),
                            )
                        });
                    match result {
                        Ok(allocation) => Ok(allocation),
                        Err(error) => {
                            prepared.prefix_result = Some(Err(error));
                            Err(())
                        }
                    }
                },
                Job::run,
            );
            task.ok()
        } else {
            None
        };
        if leader {
            self.group.signal.notify_waiters();
        }
        task
    }

    fn prefix_completion(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<PrefixResult>, crate::FsError> {
        let state = self
            .group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut prepared = self
            .proposal
            .prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(state.phase, Phase::Complete | Phase::Aborted)
            && !matches!(prepared.prefix_result, Some(Ok(Some(_))) | Some(Err(_)))
        {
            return Ok(None);
        }
        if let Some(completed) = prepared.completed_work.take() {
            completed.restore(accounting)?;
        }
        let result = prepared
            .prefix_result
            .take()
            .ok_or_else(|| crate::FsError::Publication(PublicationError::GroupAborted))?;
        Ok(Some(result))
    }
}

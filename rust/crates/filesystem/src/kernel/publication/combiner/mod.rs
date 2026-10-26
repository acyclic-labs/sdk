//! Runtime-neutral, per-owner volume proposal collection and atomic sealing.
use super::*;
use crate::kernel::allocation::retained::{
    Retained, RetainedAllocation, RetainedArc, RetainedWeak,
};
use crate::kernel::{CommitFootprint, GenerationDiff};
use crate::{AsyncAuthorityStore, AsyncObjectStore};
use std::sync::{Arc, Mutex};
mod state;
use crate::kernel::allocation::fixed_index::FixedIndex;
mod composition;
mod pipeline;
mod preparation;
mod volume;
use state::{Phase, Plan, State};
use volume::PendingBatch;
pub(crate) use volume::Volume;

// Seals are funded by the native original only; browser builds never construct
// one, so their frame carries no allocation.
#[cfg(not(target_arch = "wasm32"))]
type SealAllocation = crate::NativeAllocation;
#[cfg(target_arch = "wasm32")]
type SealAllocation = ();
type SealFuture = futures::future::Either<
    std::future::Ready<Result<(), crate::FsError>>,
    crate::heap_future::OwnedFuture<
        dyn std::future::Future<Output = Result<(), crate::FsError>> + Send,
        SealAllocation,
    >,
>;
type SealCallback = Box<dyn FnMut() -> SealFuture + Send>;

pub(super) struct SharedSeal {
    pub(super) callback: SealCallback,
    pub(super) _allocation: KernelOwnedAllocation,
}

struct Prepared {
    authentication: Option<Retained<super::authenticate::Authenticated>>,
    publication_pin: Option<crate::collection::Hold>,
    append: Option<GuardedAppend>,
    outcome: Option<AppendOutcome>,
    completed_work: Option<crate::kernel::allocation::retained::OwnedAccounting>,
    prefix_work: Option<preparation::Funding>,
    prefix_result: Option<preparation::PrefixResult>,
    error: Option<crate::FsError>,
    seal: Option<SharedSeal>,
}

struct ProposalCore {
    request: PublishGenerationRequest,
    preconditions: PublicationPreconditions,
    base: Retained<crate::kernel::GenerationRoot>,
    delta: Retained<GenerationDiff>,
    retained_merge: Option<ObjectId>,
    quote: GuardedAppend,
    growth: usize,
    cost: Mutex<Option<crate::AuthorityGroupCost>>,
    native: bool,
    cut: Option<crate::PolicyCut>,
    _allocation: RetainedAllocation,
}

struct Proposal {
    core: Arc<ProposalCore>,
    admission: Mutex<AdmissionState>,
    prepared: Mutex<Prepared>,
    _allocation: RetainedAllocation,
}

#[derive(Clone, Copy)]
struct Admission {
    anchor: Anchor,
    cost: crate::AuthorityGroupCost,
}

enum AdmissionState {
    Vacant,
    Provisional(Admission),
    Ready(Admission),
}

impl AdmissionState {
    fn ready(&self) -> Option<Admission> {
        match self {
            Self::Ready(admission) => Some(*admission),
            Self::Vacant | Self::Provisional(_) => None,
        }
    }

    fn settle(&mut self, actual: Option<Anchor>) -> bool {
        let prepared = std::mem::replace(self, Self::Vacant);
        if let (Self::Provisional(prepared), Some(actual)) = (prepared, actual)
            && prepared.anchor.head == actual.head
            && prepared.anchor.base == actual.root
        {
            *self = Self::Ready(prepared);
            return true;
        }
        false
    }
}

/// A planned filesystem head has no Streams commit identity. Only its exact
/// durable receipt can make it a certificate for a successor.
#[derive(Default)]
enum BatchAnchor {
    #[default]
    Vacant,
    Provisional(Anchor),
    Committed(Anchor),
}

impl BatchAnchor {
    fn committed(&self) -> Option<Anchor> {
        match self {
            Self::Committed(anchor) => Some(*anchor),
            Self::Vacant | Self::Provisional(_) => None,
        }
    }

    fn commit(&mut self, actual: Anchor) -> Result<(), crate::FsError> {
        if !matches!(self, Self::Provisional(planned) if *planned == actual) {
            return Err(crate::FsError::InvalidAuthorityHistory);
        }
        *self = Self::Committed(actual);
        Ok(())
    }
}

struct Group {
    volume: Arc<Volume>,
    state: Mutex<State<Arc<Proposal>, ObjectId>>,
    sealed: Mutex<BatchAnchor>,
    signal: tokio::sync::Notify,
    predecessor: Mutex<Option<RetainedArc<Group>>>,
    _members_allocation: RetainedAllocation,
}

struct Registry {
    entries: Mutex<FixedIndex<Arc<Volume>, 32>>,
    _allocation: RetainedAllocation,
}

impl Registry {
    fn new(accounting: &mut KernelAccounting<'_>) -> Result<Arc<Self>, crate::FsError> {
        let bytes = crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<Self>())
            .map_err(|_| WorkError::Overflow)? as u64;
        let allocation = RetainedAllocation::claim(bytes, 1, accounting)?;
        Ok(Arc::new(Self {
            entries: Mutex::new(FixedIndex::default()),
            _allocation: allocation,
        }))
    }
}

pub(super) struct Ticket {
    group: RetainedArc<Group>,
    proposal: Arc<Proposal>,
    index: Option<usize>,
    settled: bool,
}

impl Ticket {
    pub(super) async fn enter<P: PublicationOwner>(
        owner: &P,
        input: (
            PublishGenerationRequest,
            PublicationPreconditions,
            &PublicationIntent,
        ),
        limits: super::ClosureLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Self>, crate::FsError> {
        let objects = owner.objects();
        let (request, preconditions, intent) = input;
        let (base, allocation) = crate::kernel::read_generation_root_with_limits_and_accounting(
            objects,
            preconditions.base,
            limits.decode,
            accounting,
            cancellation,
        )
        .await?;
        let bytes =
            usize_to_u64(base.parents.capacity() * std::mem::size_of::<crate::GenerationId>());
        let base = Retained::from_owned(Owned::new(base, allocation, bytes), accounting)?;
        let result = prepare_delta(
            objects,
            request,
            &base.value,
            (limits.decode, preconditions.config),
            accounting,
            cancellation,
        )
        .await;
        let PreparedDelta {
            delta,
            changes,
            retained_merge,
        } = result?;
        let delta = Retained::from_owned(delta, accounting)?;
        let quoted = capture::quote(request, intent, changes, accounting, cancellation)?;
        let core = ProposalCore::new(
            request,
            preconditions,
            base,
            delta,
            retained_merge,
            quoted,
            accounting,
        )?;
        Self::enter_core(owner, core, (limits, None), accounting, cancellation).await
    }

    async fn enter_core<P: PublicationOwner>(
        owner: &P,
        core: Arc<ProposalCore>,
        admission: (ClosureLimits, Option<Anchor>),
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Self>, crate::FsError> {
        let authority = owner.authority();
        let volume = authority_actor(
            authority.operation_claims(),
            authority.actor_scope(),
            core.request.authority_id,
            accounting,
        )?;
        // Enqueue immutable metadata before waiting for durability or asking
        // the provider for a quote. The bounded successor fills while the
        // previous append is in flight; no caller resamples that pending head.
        let group = volume.group(accounting)?;
        let proposal = Proposal::new(Arc::clone(&core), accounting)?;
        let index = group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .enqueue(Arc::clone(&proposal))
            .ok();
        let mut ticket = Self {
            group,
            proposal,
            index,
            settled: false,
        };
        if index.is_none() {
            return Ok(Some(ticket));
        }
        if index == Some(0) {
            ticket.collect_window(accounting).await?;
            ticket
                .group
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .close();
            ticket.group.signal.notify_waiters();
        }
        if ticket
            .prepare_successor(owner, admission.0, accounting, cancellation)
            .await?
        {
            ticket.group.signal.notify_waiters();
            return Ok(Some(ticket));
        }
        let certified = match admission.1 {
            Some(certificate) => Some(certificate),
            None => volume.certificate(&core, accounting)?,
        };
        let Some(anchor) = normalize::prepare(
            owner,
            normalize::Input {
                core: Arc::clone(&core),
                limits: admission.0,
                certified,
            },
            accounting,
            cancellation,
        )
        .await?
        else {
            ticket.abort();
            ticket.settled = true;
            return Ok(None);
        };
        let Some(cost) = core
            .capacity(authority, anchor, accounting, cancellation)
            .await?
        else {
            ticket.abort();
            ticket.settled = true;
            return Ok(None);
        };
        *ticket
            .proposal
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            AdmissionState::Ready(Admission { anchor, cost });
        ticket.group.signal.notify_waiters();
        Ok(Some(ticket))
    }

    async fn collect_window(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        // Fund at most one host turn per admitted member slot. The shared
        // runtime adapter also lets browser storage events progress; self-wakes
        // alone only yield among microtasks and can starve IndexedDB.
        let turns = self
            .group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .members
            .capacity();
        for _ in 0..turns {
            accounting.check()?;
            {
                let state = self
                    .group
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.phase == Phase::Aborted {
                    return Err(PublicationError::GroupAborted.into());
                }
                if state.members.len() == state.members.capacity() {
                    return Ok(());
                }
            }
            accounting.charge_items(1)?;
            acyclic_stream::runtime::yield_now()
                .await
                .map_err(crate::AuthorityStoreError::NativeStream)?;
        }
        Ok(())
    }

    fn abort(&mut self) {
        let mut state = self
            .group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.withdraw(self.index);
        drop(state);
        self.group.signal.notify_waiters();
    }

    async fn wait_predecessor(
        &self,
        predecessor: &Group,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Anchor>, crate::FsError> {
        let cancelled = accounting.cancellation_wait(cancellation)?;
        tokio::pin!(cancelled);
        loop {
            let notified = predecessor.signal.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            accounting.check()?;
            let phase = predecessor
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .phase;
            if matches!(phase, Phase::Complete | Phase::Aborted) {
                // Completion installs the receipt and recent-change witness
                // before notifying us. Discard the dependency only afterwards.
                self.group
                    .predecessor
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                return Ok(if phase == Phase::Complete {
                    predecessor
                        .sealed
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .committed()
                } else {
                    None
                });
            }
            wait_for_notification(notified.as_mut(), cancelled.as_mut()).await?;
        }
    }

    async fn wait_deferred(
        &mut self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::FsError> {
        let cancelled = accounting.cancellation_wait(cancellation)?;
        tokio::pin!(cancelled);
        loop {
            let notified = self.group.signal.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            accounting.check()?;
            let phase = self
                .group
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .phase;
            match phase {
                Phase::Complete => {
                    self.settled = true;
                    return Ok(());
                }
                Phase::Aborted => return Err(PublicationError::GroupAborted.into()),
                _ => wait_for_notification(notified.as_mut(), cancelled.as_mut()).await?,
            }
        }
    }

    pub(super) fn authority_root(&self) -> ObjectId {
        self.proposal.original_anchor().base
    }
    fn anchor(&self) -> Result<Anchor, crate::FsError> {
        Ok(self.proposal.admission()?.anchor)
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        if !self.settled {
            self.abort();
        }
    }
}

pub(crate) fn authority_actor(
    owner: &acyclic_stream::OperationClaims,
    scope: [u8; 16],
    key: AuthorityId,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Arc<Volume>, crate::FsError> {
    // Stores sharing one owner keep separate actors per scope: distinct
    // placements never share cached heads or publication groups.
    let mut slot_key = [0; 32];
    slot_key[..16].copy_from_slice(&scope);
    slot_key[16..].copy_from_slice(&key.into_bytes());
    let owned = owner.shared_resource(
        accounting,
        |accounting| accounting.charge_items(1),
        |bytes, accounting| RetainedAllocation::claim(bytes, 1, accounting),
        Registry::new,
    )?;
    let mut registry = owned
        .entries
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(volume) = registry.get(slot_key, accounting)? {
        return Ok(Arc::clone(volume));
    }
    let slot = registry.get_or_try_insert_with(slot_key, accounting, Volume::new)?;
    Ok(Arc::clone(slot))
}

fn new_group(
    volume: Arc<Volume>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<RetainedArc<Group>, crate::FsError> {
    let maximum = (crate::async_storage::MAX_AUTHORITY_GROUP_MEMBERS)
        .min(usize::try_from(accounting.remaining()?.items_examined).unwrap_or(usize::MAX));
    if maximum == 0 {
        return Err(WorkError::BudgetExceeded {
            counter: "items_examined",
            observed: 1,
            maximum: 0,
        }
        .into());
    }
    let members =
        Owned::<Vec<state::Member<Arc<Proposal>, ObjectId>>>::allocate(maximum, accounting)?;
    let members = Retained::from_owned(members, accounting)?;
    RetainedArc::new(
        Group {
            volume,
            state: Mutex::new(State::new(members.value)),
            _members_allocation: members.allocation,
            sealed: Mutex::new(BatchAnchor::Vacant),
            signal: tokio::sync::Notify::new(),
            predecessor: Mutex::new(None),
        },
        accounting,
    )
}

fn independent(
    a: &Proposal,
    b: &Proposal,
    accounting: &mut KernelAccounting<'_>,
) -> Result<bool, crate::FsError> {
    accounting.charge_items(1)?;
    if a.core.native != b.core.native
        || a.admission()?.anchor.head != b.admission()?.anchor.head
        || a.admission()?.anchor.base != b.admission()?.anchor.base
        || a.core.request.epoch != b.core.request.epoch
        || a.core.request.operation_id == b.core.request.operation_id
    {
        return Ok(false);
    }
    independent_footprints(&a.core, &b.core, accounting)
}

fn independent_footprints(
    a: &ProposalCore,
    b: &ProposalCore,
    accounting: &mut KernelAccounting<'_>,
) -> Result<bool, crate::FsError> {
    let query = CommitFootprint {
        reads: Some(&a.preconditions.reads),
        writes: &a.delta.value,
    }
    .independent(
        CommitFootprint {
            reads: Some(&b.preconditions.reads),
            writes: &b.delta.value,
        },
        accounting.remaining()?.items_examined,
    );
    accounting.charge_items(query.items_examined)?;
    query.independent.map_err(Into::into)
}

/// Tree composition must retain a proposal's explicit merge ancestry. Keeping
/// the original merge root as the second parent preserves both of its parents,
/// including unpublished local generations, across any number of compositions.
#[derive(Clone, Copy)]
struct Composition<'a> {
    delta: &'a GenerationDiff,
    retained_merge: Option<ObjectId>,
}

struct PreparedDelta {
    delta: Owned<GenerationDiff>,
    changes: crate::PublicationChanges,
    retained_merge: Option<ObjectId>,
}

async fn prepare_delta<O: crate::AsyncObjectStore>(
    objects: &O,
    request: PublishGenerationRequest,
    base: &crate::kernel::GenerationRoot,
    admission: (crate::kernel::DecodeLimits, crate::VolumeConfig),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<PreparedDelta, crate::FsError> {
    let (limits, config) = admission;
    let (root, allocation) = crate::kernel::read_generation_root_with_limits_and_accounting(
        objects,
        request.generation_root,
        limits,
        accounting,
        cancellation,
    )
    .await?;
    let bytes = usize_to_u64(root.parents.capacity() * std::mem::size_of::<crate::GenerationId>());
    let root = Owned::new(root, allocation, bytes);
    let result = async {
        if root.value.volume_id != request.volume_id
            || base.volume_id != request.volume_id
            || root.value.root_file_id != base.root_file_id
        {
            return Err(crate::FsError::VolumeMismatch);
        }
        let maximum = u32::try_from(accounting.remaining()?.items_examined).unwrap_or(u32::MAX);
        let (delta, allocation) = crate::kernel::diff_generation_file_tables(
            objects,
            base.file_table,
            root.value.file_table,
            maximum,
            limits,
            accounting,
            cancellation,
        )
        .await?;
        let bytes = delta.retained_bytes()?;
        let delta = Owned::new(delta, allocation, bytes);
        if delta.value.truncated {
            delta.retire(accounting)?;
            return Err(crate::FsError::InvalidDiff);
        }
        let changes = crate::PublicationChanges::proposed(
            objects,
            base,
            &root.value,
            request,
            config,
            accounting,
            cancellation,
        )
        .await;
        match changes {
            Ok(changes) => Ok(PreparedDelta {
                delta,
                changes,
                retained_merge: (root.value.parents.len() > 1).then_some(request.generation_root),
            }),
            Err(error) => {
                delta.retire(accounting)?;
                Err(error)
            }
        }
    }
    .await;
    root.retire(accounting).and(result)
}

mod capture;
mod dispatch;
mod normalize;
use normalize::Anchor;
mod plan;
pub(super) use plan::{Candidate, prepare};

async fn wait_for_notification<N, C>(
    notified: std::pin::Pin<&mut N>,
    cancelled: std::pin::Pin<&mut C>,
) -> Result<(), crate::FsError>
where
    N: Future<Output = ()>,
    C: Future<Output = ()>,
{
    match futures::future::select(notified, cancelled).await {
        futures::future::Either::Left(_) => Ok(()),
        futures::future::Either::Right(_) => Err(crate::CancellationError.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FsError, WorkBudget};

    pub(super) fn anchor() -> Anchor {
        let root = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([1; 32]),
        };
        Anchor {
            head: Head {
                epoch: Epoch::GENESIS,
                sequence: crate::Sequence::new(4),
                digest: Digest::from_bytes([2; 32]),
            },
            base: root,
            root,
        }
    }

    fn alternatives(expected: Anchor) -> Result<[Anchor; 7], FsError> {
        Ok([
            Anchor {
                head: Head {
                    epoch: Epoch::new(2).map_err(|_| FsError::InvalidDiff)?,
                    ..expected.head
                },
                ..expected
            },
            Anchor {
                head: Head {
                    sequence: crate::Sequence::new(5),
                    ..expected.head
                },
                ..expected
            },
            Anchor {
                head: Head {
                    digest: Digest::ZERO,
                    ..expected.head
                },
                ..expected
            },
            Anchor {
                base: ObjectId {
                    digest: Digest::ZERO,
                    ..expected.base
                },
                ..expected
            },
            Anchor {
                root: ObjectId {
                    digest: Digest::ZERO,
                    ..expected.root
                },
                ..expected
            },
            Anchor {
                base: ObjectId {
                    kind: ObjectKind::Metadata,
                    ..expected.base
                },
                ..expected
            },
            Anchor {
                root: ObjectId {
                    kind: ObjectKind::Metadata,
                    ..expected.root
                },
                ..expected
            },
        ])
    }

    #[test]
    fn provisional_receipt_validation_binds_every_anchor_coordinate() -> Result<(), FsError> {
        let expected = anchor();
        for wrong in alternatives(expected)? {
            let mut batch = BatchAnchor::Provisional(expected);
            assert!(batch.committed().is_none());
            assert!(
                batch.commit(wrong).is_err(),
                "a mismatched receipt promoted a planned batch"
            );
            assert!(batch.committed().is_none());
            batch.commit(expected)?;
            assert_eq!(batch.committed(), Some(expected));
            assert!(
                batch.commit(expected).is_err(),
                "a receipt promoted the same batch twice"
            );
        }
        Ok(())
    }

    #[test]
    fn provisional_admission_rejects_mismatched_or_revoked_predecessors() -> Result<(), FsError> {
        let expected = anchor();
        for wrong in alternatives(expected)?
            .into_iter()
            .filter(|wrong| wrong.head != expected.head || wrong.root != expected.root)
        {
            let mut admission = AdmissionState::Provisional(Admission {
                anchor: expected,
                cost: crate::AuthorityGroupCost::default(),
            });
            assert!(admission.ready().is_none());
            assert!(
                !admission.settle(Some(wrong)),
                "a mismatched predecessor admitted prepared metadata"
            );
            assert!(matches!(admission, AdmissionState::Vacant));
        }
        let mut missing = AdmissionState::Provisional(Admission {
            anchor: expected,
            cost: crate::AuthorityGroupCost::default(),
        });
        assert!(!missing.settle(None));
        assert!(
            !missing.settle(Some(expected)),
            "a revoked plan was resurrected by a late receipt"
        );
        let mut ready = AdmissionState::Provisional(Admission {
            anchor: expected,
            cost: crate::AuthorityGroupCost::default(),
        });
        assert!(ready.settle(Some(expected)));
        assert_eq!(
            ready.ready().map(|admission| admission.anchor),
            Some(expected)
        );
        Ok(())
    }

    #[test]
    fn failed_group_control_admission_retires_its_member_capacity() -> Result<(), FsError> {
        let cancel = CancellationToken::new();
        let budget = WorkBudget {
            allocation_operations: 1,
            ..crate::performance::BOUNDARY_BUDGET
        };
        let mut owner = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
        let volume = Volume::new(&mut owner)?;
        let mut accounting = KernelAccounting::standalone(budget, &cancel);
        assert!(new_group(volume, &mut accounting).is_err());
        let (allocations, _, _, _) = accounting.standalone_parts().ok_or(FsError::InvalidDiff)?;
        assert_eq!(
            allocations.live_bytes(),
            0,
            "rejected group retained unused member capacity"
        );
        Ok(())
    }
}

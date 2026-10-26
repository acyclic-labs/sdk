//! Provider-owned volume state outlives receipt delivery and transient rounds.
//! Only authenticated committed batches enter the recent-change witness. A
//! witness certifies a bounded interval; it never claims to be the current head.
use super::*;

pub(crate) struct Volume {
    #[cfg(any(test, feature = "distributed"))]
    pub(crate) journal: tokio::sync::Mutex<crate::distributed::actor::Journal>,
    #[cfg(any(test, feature = "distributed"))]
    pub(crate) revision: std::sync::atomic::AtomicU64,
    state: Mutex<VolumeState>,
    _allocation: RetainedAllocation,
}

struct VolumeState {
    active: RetainedWeak<Group>,
    appending: RetainedWeak<Group>,
    recent: Option<RecentBatch>,
}

struct RecentBatch {
    from: Anchor,
    through: Anchor,
    members: Retained<Vec<Arc<ProposalCore>>>,
}

impl Volume {
    #[cfg(test)]
    pub(crate) fn provisional_successors(&self) -> usize {
        let active = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .upgrade();
        active.map_or(0, |group| {
            let state = group
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .members
                .iter()
                .filter(|member| {
                    matches!(
                        &*member
                            .value
                            .admission
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner),
                        AdmissionState::Provisional(_)
                    )
                })
                .count()
        })
    }

    pub(crate) fn publication_root(
        &self,
        request: PublishGenerationRequest,
    ) -> Option<(Head, ObjectId)> {
        #[cfg(any(test, feature = "distributed"))]
        {
            self.journal
                .try_lock()
                .ok()?
                .writable()?
                .publication_root(request)
        }
        #[cfg(not(any(test, feature = "distributed")))]
        {
            let _ = request;
            None
        }
    }

    pub(super) fn new(accounting: &mut KernelAccounting<'_>) -> Result<Arc<Self>, crate::FsError> {
        let bytes = crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<Self>())
            .map_err(|_| WorkError::Overflow)? as u64;
        let allocation = RetainedAllocation::claim(bytes, 1, accounting)?;
        Ok(Arc::new(Self {
            #[cfg(any(test, feature = "distributed"))]
            journal: tokio::sync::Mutex::new(crate::distributed::actor::Journal::Vacant),
            #[cfg(any(test, feature = "distributed"))]
            revision: std::sync::atomic::AtomicU64::new(0),
            state: Mutex::new(VolumeState {
                active: RetainedWeak::default(),
                appending: RetainedWeak::default(),
                recent: None,
            }),
            _allocation: allocation,
        }))
    }

    pub(super) fn group(
        self: &Arc<Self>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<RetainedArc<Group>, crate::FsError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let selected = state
            .active
            .upgrade()
            .into_iter()
            .chain(state.appending.upgrade())
            .find_map(|group| {
                let phase = group
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .phase;
                (!matches!(phase, Phase::Complete | Phase::Aborted)).then_some((group, phase))
            });
        if let Some((group, phase)) = selected {
            if phase == Phase::Appending {
                // An empty successor can disappear while its predecessor is
                // still owned by the append task. Preserve that dependency
                // independently of the weak collection-window handle.
                state.appending = group.downgrade();
                // One in-flight append and one bounded collecting successor.
                // Metadata may prepare against this immutable predecessor;
                // only its exact receipt can admit the successor to sealing.
                let next = new_group(Arc::clone(self), accounting)?;
                *next
                    .predecessor
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(group);
                state.active = next.downgrade();
                return Ok(next);
            }
            return Ok(group);
        }
        let group = new_group(Arc::clone(self), accounting)?;
        state.active = group.downgrade();
        Ok(group)
    }

    pub(super) fn certificate(
        &self,
        proposal: &ProposalCore,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<Anchor>, crate::FsError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(recent) = &state.recent else {
            return Ok(None);
        };
        if recent.from.head != proposal.request.expected
            || recent.from.base != proposal.preconditions.base
        {
            return Ok(None);
        }
        for member in &recent.members.value {
            let query = CommitFootprint {
                reads: Some(&proposal.preconditions.reads),
                writes: &proposal.delta.value,
            }
            .independent(
                CommitFootprint {
                    reads: None,
                    writes: &member.delta.value,
                },
                accounting.remaining()?.items_examined,
            );
            accounting.charge_items(query.items_examined)?;
            if !query.independent? {
                return Ok(None);
            }
        }
        Ok(Some(recent.through))
    }
}

/// Admission happens before publication. Failed or provisional batches cannot
/// install a witness, and the retained members contain no caller seal closure.
pub(super) struct PendingBatch {
    from: Anchor,
    members: Retained<Vec<Arc<ProposalCore>>>,
}

impl PendingBatch {
    pub(super) fn prepare(
        group: &Group,
        head: Head,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, crate::FsError> {
        let state = group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let from = state
            .selected()
            .next()
            .ok_or(crate::FsError::InvalidAuthorityHistory)?
            .admission()?
            .anchor;
        let mut members =
            Owned::<Vec<Arc<ProposalCore>>>::allocate(state.selected().count(), accounting)?;
        members
            .value
            .extend(state.selected().map(|proposal| Arc::clone(&proposal.core)));
        let prepared = Self {
            from,
            members: Retained::from_owned(members, accounting)?,
        };
        let root = state
            .tail()
            .ok_or(crate::FsError::InvalidAuthorityHistory)?;
        *group
            .sealed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            BatchAnchor::Provisional(Anchor {
                head,
                base: root,
                root,
            });
        Ok(prepared)
    }

    pub(super) fn commit(self, volume: &Volume, through: Anchor) {
        let mut state = volume
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A delayed receipt from an older round cannot regress the witness.
        if state
            .recent
            .as_ref()
            .is_none_or(|recent| recent.through.head.sequence < through.head.sequence)
        {
            let retired = state.recent.replace(RecentBatch {
                from: self.from,
                through,
                members: self.members,
            });
            drop(state);
            drop(retired);
        }
    }
}

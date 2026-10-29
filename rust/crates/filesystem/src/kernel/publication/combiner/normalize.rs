use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Anchor {
    pub(super) head: Head,
    pub(super) base: ObjectId,
    pub(super) root: ObjectId,
}

pub(super) struct Input {
    pub(super) core: Arc<ProposalCore>,
    pub(super) limits: ClosureLimits,
    pub(super) certified: Option<Anchor>,
}

pub(super) async fn prepare<P: PublicationOwner>(
    owner: &P,
    input: Input,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<Anchor>, crate::FsError> {
    let current = crate::PublicationChanges::current_root(
        owner.authority(),
        input.core.request,
        accounting,
        cancellation,
    )
    .await?;
    prepare_at(owner, input, current, accounting, cancellation).await
}

pub(super) async fn prepare_at<P: PublicationOwner>(
    owner: &P,
    input: Input,
    current: (Head, ObjectId),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<Anchor>, crate::FsError> {
    let core = &input.core;
    let original = Anchor {
        head: core.request.expected,
        base: core.preconditions.base,
        root: core.request.generation_root,
    };
    let (head, root) = current;
    if head == original.head {
        return Ok(Some(original));
    }
    if !core.preconditions.rebase
        || head.epoch != original.head.epoch
        || head.sequence <= original.head.sequence
    {
        return Ok(None);
    }
    if input
        .certified
        .is_none_or(|certified| certified.head != head || certified.base != root)
    {
        let frontier = crate::kernel::generation_frontier(
            owner.authority(),
            owner.objects(),
            crate::kernel::FrontierRequest {
                volume: core.request.volume_id,
                base: (original.head, &core.base.value),
                head: (head, root),
                limits: input.limits.decode,
            },
            accounting,
            cancellation,
        )
        .await?;
        let independent = frontier.independent(
            CommitFootprint {
                reads: Some(&core.preconditions.reads),
                writes: &core.delta.value,
            },
            accounting,
        );
        if !frontier.retire(accounting).and(independent)? {
            return Ok(None);
        }
    }
    let candidate = composition::compose(
        owner.clone(),
        input.core,
        root,
        input.limits.decode,
        accounting,
        cancellation,
    )
    .await?;
    Ok(Some(Anchor {
        head,
        base: root,
        root: candidate,
    }))
}

impl Ticket {
    pub(super) fn certificate(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<Anchor>, crate::FsError> {
        let state = self
            .group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase != Phase::Complete
            || state.selected().next().is_none_or(|first| {
                first.original_anchor().head != self.proposal.original_anchor().head
                    || first.original_anchor().base != self.proposal.original_anchor().base
            })
        {
            return Ok(None);
        }
        let certified = self
            .group
            .sealed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .committed();
        if certified.is_none() {
            return Ok(None);
        }
        for committed in state.selected() {
            let query = CommitFootprint {
                reads: Some(&self.proposal.core.preconditions.reads),
                writes: &self.proposal.core.delta.value,
            }
            .independent(
                CommitFootprint {
                    reads: None,
                    writes: &committed.core.delta.value,
                },
                accounting.remaining()?.items_examined,
            );
            accounting.charge_items(query.items_examined)?;
            if !query.independent? {
                return Ok(None);
            }
        }
        Ok(certified)
    }
}

impl ProposalCore {
    pub(super) fn new(
        request: PublishGenerationRequest,
        preconditions: PublicationPreconditions,
        base: Retained<crate::kernel::GenerationRoot>,
        delta: Retained<GenerationDiff>,
        retained_merge: Option<ObjectId>,
        quoted: (GuardedAppend, usize),
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Arc<Self>, crate::FsError> {
        let control = crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<Self>())
            .map_err(|_| WorkError::Overflow)? as u64;
        let allocation = RetainedAllocation::claim(control, 1, accounting)?;
        #[cfg(not(target_arch = "wasm32"))]
        let native = accounting.native_work().is_some();
        #[cfg(target_arch = "wasm32")]
        let native = false;
        Ok(Arc::new(Self {
            request,
            preconditions,
            base,
            delta,
            retained_merge,
            quote: quoted.0,
            growth: quoted.1,
            cost: Mutex::new(None),
            native,
            cut: crate::PolicyCut::current(),
            _allocation: allocation,
        }))
    }
}

impl Proposal {
    pub(super) fn new(
        core: Arc<ProposalCore>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Arc<Self>, crate::FsError> {
        let bytes = crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<Self>())
            .map_err(|_| WorkError::Overflow)? as u64;
        let allocation = RetainedAllocation::claim(bytes, 1, accounting)?;
        Ok(Arc::new(Self {
            core,
            admission: Mutex::new(AdmissionState::Vacant),
            prepared: Mutex::new(Prepared {
                authentication: None,
                publication_pin: None,
                append: None,
                outcome: None,
                completed_work: None,
                prefix_work: None,
                prefix_result: None,
                error: None,
                seal: None,
            }),
            _allocation: allocation,
        }))
    }

    pub(super) fn original_anchor(&self) -> Anchor {
        self.admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .ready()
            .map_or(
                Anchor {
                    head: self.core.request.expected,
                    base: self.core.preconditions.base,
                    root: self.core.request.generation_root,
                },
                |admission| admission.anchor,
            )
    }

    pub(super) fn admission(&self) -> Result<Admission, crate::FsError> {
        self.admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .ready()
            .ok_or(crate::FsError::InvalidAuthorityHistory)
    }
}

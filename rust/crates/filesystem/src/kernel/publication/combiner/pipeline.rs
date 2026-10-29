//! Prepare against an immutable predecessor; only its real receipt admits use.
use super::*;

impl Ticket {
    pub(super) async fn prepare_successor<P: PublicationOwner>(
        &self,
        owner: &P,
        limits: ClosureLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool, crate::FsError> {
        let objects = owner.objects();
        let authority = owner.authority();
        let predecessor = self
            .group
            .predecessor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(predecessor) = predecessor else {
            return Ok(false);
        };
        let provisional = predecessor.successor_basis(&self.proposal.core, accounting)?;
        if let Some((durable, pending)) = provisional {
            // Capacity is quoted against the preceding real durable projection.
            // The canonical quote pads dense integer growth; a planned head is
            // never passed to an append builder as a writable cursor.
            if let Some(cost) = self
                .proposal
                .core
                .capacity(authority, durable, accounting, cancellation)
                .await?
            {
                let core = &self.proposal.core;
                let anchor = normalize::prepare_at(
                    owner,
                    normalize::Input {
                        core: Arc::clone(core),
                        limits,
                        certified: Some(pending),
                    },
                    (pending.head, pending.root),
                    accounting,
                    cancellation,
                )
                .await?;
                if let Some(anchor) = anchor {
                    let proven_at = objects.collection_sweeps();
                    let authenticated = super::super::authenticate::prove(
                        objects,
                        anchor.root,
                        Some(pending.root),
                        limits,
                        None,
                        accounting,
                        cancellation,
                    )
                    .await?;
                    let pinned = super::super::authenticate::retain_and_pin(
                        objects,
                        authenticated,
                        core.preconditions.config,
                        proven_at,
                        accounting,
                    )
                    .await?;
                    {
                        let mut prepared = self
                            .proposal
                            .prepared
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        prepared.authentication = Some(pinned.authentication);
                        prepared.publication_pin = pinned.pin;
                    }
                    *self
                        .proposal
                        .admission
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        AdmissionState::Provisional(Admission { anchor, cost });
                }
            }
        }
        let actual = self
            .wait_predecessor(&predecessor, accounting, cancellation)
            .await?;
        Ok(self.proposal.settle_preparation(actual))
    }

    pub(in crate::kernel::publication) fn take_authentication(
        &self,
        root: ObjectId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<super::super::authenticate::Authenticated>, crate::FsError> {
        let authenticated = self
            .proposal
            .prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .authentication
            .take();
        let Some(authenticated) = authenticated else {
            return Ok(None);
        };
        if authenticated.value.proof.generation_id.object_id() != root {
            return Ok(None);
        }
        let Retained { value, allocation } = authenticated;
        drop(allocation);
        super::super::retain(accounting, value.guard.retained_bytes())?;
        Ok(Some(value))
    }
}

impl Proposal {
    /// Consume the predecessor dependency before releasing any rejected roots.
    fn settle_preparation(&self, actual: Option<Anchor>) -> bool {
        let ready = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .settle(actual);
        if !ready {
            let mut prepared = self
                .prepared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            prepared.authentication.take();
            prepared.publication_pin.take();
        }
        ready
    }
}

impl Group {
    fn successor_basis(
        &self,
        proposal: &ProposalCore,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<(Anchor, Anchor)>, crate::FsError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase != Phase::Appending {
            return Ok(None);
        }
        let Some(first) = state.selected().next() else {
            return Ok(None);
        };
        let durable = first.admission()?.anchor;
        if durable.head != proposal.request.expected || durable.base != proposal.preconditions.base
        {
            return Ok(None);
        }
        for member in state.selected() {
            accounting.charge_items(1)?;
            if member.core.request.epoch != proposal.request.epoch
                || member.core.request.operation_id == proposal.request.operation_id
                || !independent_footprints(proposal, &member.core, accounting)?
            {
                return Ok(None);
            }
        }
        let sealed = self
            .sealed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*sealed {
            BatchAnchor::Provisional(pending) => Ok(Some((durable, *pending))),
            BatchAnchor::Vacant | BatchAnchor::Committed(_) => Ok(None),
        }
    }
}

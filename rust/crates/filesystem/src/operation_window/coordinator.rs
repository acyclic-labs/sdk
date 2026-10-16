//! Lease transitions and final-close workspace reconciliation.
use super::*;

impl<S: OperationWindowStore> OperationWindowCoordinator<S> {
    /// Opens an overlapping lease. The first lease pins `parent`.
    pub async fn begin(
        &self,
        workspace_id: WorkspaceId,
        parent: GenerationId,
        owner: impl Into<String>,
        now_millis: u64,
        expires_at_millis: u64,
    ) -> Result<OperationWindowLease, OperationWindowError<S::Error>> {
        self.begin_with_lease_id(
            workspace_id,
            parent,
            owner,
            now_millis,
            expires_at_millis,
            OperationLeaseId::new(),
        )
        .await
    }

    /// Opens a lease with a caller-supplied deterministic identity.
    pub async fn begin_with_lease_id(
        &self,
        workspace_id: WorkspaceId,
        parent: GenerationId,
        owner: impl Into<String>,
        now_millis: u64,
        expires_at_millis: u64,
        lease_id: OperationLeaseId,
    ) -> Result<OperationWindowLease, OperationWindowError<S::Error>> {
        if expires_at_millis <= now_millis {
            return Err(OperationWindowError::InvalidExpiry);
        }
        let owner = owner.into();
        if owner.trim().is_empty() || lease_id.into_bytes() == [0; 16] {
            return Err(OperationWindowError::LeaseIdentityConflict);
        }
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(workspace_id).await?;
            if expire_to_reconcile(
                &mut current.phase,
                now_millis,
                OperationId::from_bytes(lease_id.into_bytes()),
            )
            .is_some()
            {
                let expected = next_revision(&mut current.revision);
                if self.cas(workspace_id, expected, current).await? {
                    return Err(OperationWindowError::Reconciling);
                }
                continue;
            }
            let pinned_parent = match &mut current.phase {
                OperationWindowPhase::Idle => {
                    let mut leases = BTreeMap::new();
                    leases.insert(
                        lease_id,
                        OperationLease {
                            id: lease_id,
                            owner: owner.clone(),
                            expires_at_millis,
                        },
                    );
                    current.phase = OperationWindowPhase::Active {
                        pinned_parent: parent,
                        leases,
                        pending_parent: None,
                    };
                    parent
                }
                OperationWindowPhase::Active {
                    pinned_parent,
                    leases,
                    ..
                } => {
                    if let Some(existing) = leases.get(&lease_id) {
                        if existing.owner == owner
                            && existing.expires_at_millis == expires_at_millis
                        {
                            return Ok(OperationWindowLease {
                                workspace_id,
                                lease_id,
                                pinned_parent: *pinned_parent,
                                expires_at_millis,
                            });
                        }
                        return Err(OperationWindowError::LeaseIdentityConflict);
                    }
                    leases.insert(
                        lease_id,
                        OperationLease {
                            id: lease_id,
                            owner: owner.clone(),
                            expires_at_millis,
                        },
                    );
                    *pinned_parent
                }
                OperationWindowPhase::Reconciling { .. } => {
                    return Err(OperationWindowError::Reconciling);
                }
            };
            let expected = next_revision(&mut current.revision);
            if self.cas(workspace_id, expected, current).await? {
                return Ok(OperationWindowLease {
                    workspace_id,
                    lease_id,
                    pinned_parent,
                    expires_at_millis,
                });
            }
        }
        Err(OperationWindowError::Contended)
    }

    /// Coalesces a newly authenticated parent generation without changing the mount.
    pub async fn observe_parent(
        &self,
        workspace_id: WorkspaceId,
        parent: GenerationId,
    ) -> Result<bool, OperationWindowError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(workspace_id).await?;
            match &mut current.phase {
                OperationWindowPhase::Idle => return Ok(false),
                OperationWindowPhase::Active { pending_parent, .. } => {
                    *pending_parent = Some(parent);
                }
                OperationWindowPhase::Reconciling {
                    subsequent_parent, ..
                } => *subsequent_parent = Some(parent),
            }
            let expected = next_revision(&mut current.revision);
            if self.cas(workspace_id, expected, current).await? {
                return Ok(true);
            }
        }
        Err(OperationWindowError::Contended)
    }

    /// Reconciles an idle child after its parent advances, or coalesces the
    /// advance into an active/reconciling operation window. An idle claim is
    /// durable and fences a new tool window until reconciliation completes.
    ///
    /// This also covers a parent tool that finishes after the child's last
    /// overlapping tool: the child need not open another tool to catch up.
    pub async fn parent_advanced_workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        workspace: &Workspace<A, O>,
        parent: GenerationId,
        limits: OperationReconcileLimits,
    ) -> Result<Option<WorkspaceRebase<A, O>>, OperationWindowError<S::Error>> {
        validate_reconcile_limits(limits).map_err(OperationWindowError::Workspace)?;
        let workspace_id = workspace.id();
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(workspace_id).await?;
            let reconcile = match &mut current.phase {
                OperationWindowPhase::Idle => {
                    let ticket = OperationId::new();
                    current.phase = OperationWindowPhase::Reconciling {
                        ticket,
                        pinned_parent: parent,
                        pending_parent: Some(parent),
                        subsequent_parent: None,
                    };
                    Some(OperationWindowReconcile {
                        ticket,
                        pinned_parent: parent,
                        pending_parent: Some(parent),
                    })
                }
                OperationWindowPhase::Active { pending_parent, .. } => {
                    *pending_parent = Some(parent);
                    None
                }
                OperationWindowPhase::Reconciling {
                    subsequent_parent, ..
                } => {
                    *subsequent_parent = Some(parent);
                    None
                }
            };
            let expected = next_revision(&mut current.revision);
            if self.cas(workspace_id, expected, current).await? {
                return match reconcile {
                    Some(reconcile) => self
                        .reconcile_workspace(workspace, reconcile, limits)
                        .await
                        .map(Some),
                    None => Ok(None),
                };
            }
        }
        Err(OperationWindowError::Contended)
    }

    /// Extends one still-active exact lease without changing the pinned mount.
    /// The returned lease replaces the caller's old publication permit; using
    /// the old expiry after renewal is fenced by durable state. A renewal must
    /// strictly increase the expiry, so a superseded token cannot be reissued.
    pub async fn renew(
        &self,
        lease: &OperationWindowLease,
        now_millis: u64,
        expires_at_millis: u64,
    ) -> Result<OperationWindowLease, OperationWindowError<S::Error>> {
        if expires_at_millis <= now_millis || expires_at_millis <= lease.expires_at_millis {
            return Err(OperationWindowError::InvalidExpiry);
        }
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(lease.workspace_id).await?;
            let OperationWindowPhase::Active {
                pinned_parent,
                leases,
                ..
            } = &mut current.phase
            else {
                return Err(OperationWindowError::StaleLease);
            };
            let Some(existing) = leases.get(&lease.lease_id) else {
                return Err(OperationWindowError::StaleLease);
            };
            if existing.expires_at_millis != lease.expires_at_millis
                || existing.expires_at_millis <= now_millis
            {
                return Err(OperationWindowError::StaleLease);
            }
            let pinned_parent = *pinned_parent;
            leases
                .get_mut(&lease.lease_id)
                .ok_or(OperationWindowError::StaleLease)?
                .expires_at_millis = expires_at_millis;
            let expected = next_revision(&mut current.revision);
            if self.cas(lease.workspace_id, expected, current).await? {
                return Ok(OperationWindowLease {
                    workspace_id: lease.workspace_id,
                    lease_id: lease.lease_id,
                    pinned_parent,
                    expires_at_millis,
                });
            }
        }
        Err(OperationWindowError::Contended)
    }

    /// Closes one lease and atomically claims reconciliation after the last close.
    pub async fn finish(
        &self,
        lease: &OperationWindowLease,
        now_millis: u64,
    ) -> Result<OperationWindowFinish, OperationWindowError<S::Error>> {
        self.finish_with_ticket(lease, now_millis, OperationId::new())
            .await
    }

    /// Closes a lease with a caller-supplied deterministic reconciliation ticket.
    pub async fn finish_with_ticket(
        &self,
        lease: &OperationWindowLease,
        now_millis: u64,
        reconciliation_ticket: OperationId,
    ) -> Result<OperationWindowFinish, OperationWindowError<S::Error>> {
        self.close(lease, now_millis, reconciliation_ticket, || {
            std::future::ready(Ok(FinalClose::Reconcile))
        })
        .await
        .map(|(finish, _)| finish)
    }

    /// Closes a lease in one transition. The final close asks `final_close`
    /// what reconciliation owes the parent; the returned flag says that the
    /// close settled the window, with nothing left to reconcile.
    async fn close<F: Future<Output = Result<FinalClose, OperationWindowError<S::Error>>>>(
        &self,
        lease: &OperationWindowLease,
        now_millis: u64,
        reconciliation_ticket: OperationId,
        final_close: impl Fn() -> F,
    ) -> Result<(OperationWindowFinish, bool), OperationWindowError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(lease.workspace_id).await?;
            let before = current.phase.clone();
            let mut result = close_lease(
                &mut current.phase,
                lease.lease_id,
                lease.expires_at_millis,
                now_millis,
                reconciliation_ticket,
            );
            if result.is_none() && current.phase == before {
                return Ok((OperationWindowFinish::AlreadyClosed, false));
            }
            let mut settled = false;
            if let Some(OperationWindowFinish::Reconcile(reconcile)) = &mut result {
                match final_close().await? {
                    FinalClose::Reconcile => {}
                    FinalClose::Settle => {
                        current.phase = OperationWindowPhase::Idle;
                        settled = true;
                    }
                    FinalClose::Rebase(parent) => {
                        reconcile.pending_parent = Some(parent);
                        if let OperationWindowPhase::Reconciling { pending_parent, .. } =
                            &mut current.phase
                        {
                            *pending_parent = Some(parent);
                        }
                    }
                }
            }
            let expected = next_revision(&mut current.revision);
            if self.cas(lease.workspace_id, expected, current).await? {
                return Ok((
                    result.unwrap_or(OperationWindowFinish::AlreadyClosed),
                    settled,
                ));
            }
        }
        Err(OperationWindowError::Contended)
    }

    /// Closes a lease and runs exactly one core live rebase after the last close.
    ///
    /// The last close rebases the fork onto its parent's head, whether or not
    /// the window observed the advance. A fork already based on that head needs
    /// no rebase, so its last close releases the window in the same transition.
    /// Successful, current, and conflicted outcomes are terminal and release
    /// the window. Stale, fenced, and idempotency-conflicted outcomes retain the
    /// durable reconciliation ticket so recovery can retry or inspect it.
    pub async fn finish_workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        workspace: &Workspace<A, O>,
        lease: &OperationWindowLease,
        now_millis: u64,
        limits: OperationReconcileLimits,
    ) -> Result<WorkspaceOperationFinish<A, O>, OperationWindowError<S::Error>> {
        if workspace.id() != lease.workspace_id {
            return Err(OperationWindowError::IncompatibleState);
        }
        validate_reconcile_limits(limits).map_err(OperationWindowError::Workspace)?;
        let final_close = || {
            let workspace = workspace.clone();
            async move {
                match workspace
                    .volume
                    .fs
                    .parent_advance(&workspace.volume, limits.maximum_generations)
                    .await
                {
                    Ok(Some(parent)) => Ok(FinalClose::Rebase(parent)),
                    // A workspace that is not a fork has no parent to follow.
                    Ok(None) | Err(WorkspaceError::NotFork) => Ok(FinalClose::Settle),
                    Err(error) => Err(OperationWindowError::Workspace(error)),
                }
            }
        };
        let reconcile = match self
            .close(lease, now_millis, OperationId::new(), final_close)
            .await?
        {
            (OperationWindowFinish::StillActive { remaining }, _) => {
                return Ok(WorkspaceOperationFinish::StillActive { remaining });
            }
            (OperationWindowFinish::AlreadyClosed, _) => {
                return Ok(WorkspaceOperationFinish::AlreadyClosed);
            }
            (OperationWindowFinish::Reconcile(_), true) => {
                return workspace
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| {
                        WorkspaceOperationFinish::Reconciled(WorkspaceRebase::Current(
                            receipt.value,
                        ))
                    })
                    .map_err(|failure| OperationWindowError::Workspace(failure.error));
            }
            (OperationWindowFinish::Reconcile(reconcile), false) => reconcile,
        };
        self.reconcile_workspace(workspace, reconcile, limits)
            .await
            .map(WorkspaceOperationFinish::Reconciled)
    }

    /// Runs one previously claimed reconciliation ticket.
    ///
    /// This split form lets adapters fence a lease before capturing its mount,
    /// then publish the capture and finish the core rebase without reopening a
    /// race for another tool window.
    pub async fn reconcile_workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        workspace: &Workspace<A, O>,
        reconcile: OperationWindowReconcile,
        limits: OperationReconcileLimits,
    ) -> Result<WorkspaceRebase<A, O>, OperationWindowError<S::Error>> {
        validate_reconcile_limits(limits).map_err(OperationWindowError::Workspace)?;
        let snapshot = self.snapshot(workspace.id()).await?;
        if !matches!(
            snapshot.phase,
            OperationWindowPhase::Reconciling { ticket, .. } if ticket == reconcile.ticket
        ) {
            return Err(OperationWindowError::StaleTicket);
        }
        let outcome = if reconcile.pending_parent.is_some() {
            workspace
                .live_rebase(
                    IdempotencyKey::from_bytes(reconcile.ticket.into_bytes()),
                    limits.maximum_generations,
                    limits.maximum_changes,
                    limits.maximum_conflicts,
                )
                .await
                .map_err(OperationWindowError::Workspace)?
        } else {
            let generation = workspace
                .head(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .await
                .map_err(|failure| OperationWindowError::Workspace(failure.error))?
                .value;
            WorkspaceRebase::Current(generation)
        };
        if matches!(
            outcome,
            WorkspaceRebase::Rebased(_)
                | WorkspaceRebase::AlreadyRebased(_)
                | WorkspaceRebase::Current(_)
                | WorkspaceRebase::Conflicted { .. }
        ) {
            self.complete_reconcile(workspace.id(), reconcile.ticket)
                .await?;
        }
        Ok(outcome)
    }

    /// Claims and completes reconciliation left behind by expired leases or a
    /// crashed adapter. Returns `None` when no recovery work exists.
    pub async fn recover_workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        workspace: &Workspace<A, O>,
        now_millis: u64,
        limits: OperationReconcileLimits,
    ) -> Result<Option<WorkspaceRebase<A, O>>, OperationWindowError<S::Error>> {
        validate_reconcile_limits(limits).map_err(OperationWindowError::Workspace)?;
        let reconcile = self.claim_reconcile(workspace.id(), now_millis).await?;
        match reconcile {
            Some(reconcile) => self
                .reconcile_workspace(workspace, reconcile, limits)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    /// Claims an expired active window or returns the durable retry ticket for
    /// an interrupted reconciliation.
    pub async fn claim_reconcile(
        &self,
        workspace_id: WorkspaceId,
        now_millis: u64,
    ) -> Result<Option<OperationWindowReconcile>, OperationWindowError<S::Error>> {
        self.claim_reconcile_with_ticket(workspace_id, now_millis, OperationId::new())
            .await
    }

    /// Claims recovery with a caller-supplied deterministic ticket.
    pub async fn claim_reconcile_with_ticket(
        &self,
        workspace_id: WorkspaceId,
        now_millis: u64,
        ticket: OperationId,
    ) -> Result<Option<OperationWindowReconcile>, OperationWindowError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(workspace_id).await?;
            match current.phase {
                OperationWindowPhase::Idle => return Ok(None),
                OperationWindowPhase::Reconciling {
                    ticket,
                    pinned_parent,
                    pending_parent,
                    ..
                } => {
                    return Ok(Some(OperationWindowReconcile {
                        ticket,
                        pinned_parent,
                        pending_parent,
                    }));
                }
                OperationWindowPhase::Active { .. } => {}
            }
            let Some(reconcile) = expire_to_reconcile(&mut current.phase, now_millis, ticket)
            else {
                return Ok(None);
            };
            let expected = next_revision(&mut current.revision);
            if self.cas(workspace_id, expected, current).await? {
                return Ok(Some(reconcile));
            }
        }
        Err(OperationWindowError::Contended)
    }

    /// Completes the exact reconciliation ticket and permits the next window.
    pub async fn complete_reconcile(
        &self,
        workspace_id: WorkspaceId,
        ticket: OperationId,
    ) -> Result<(), OperationWindowError<S::Error>> {
        self.complete_reconcile_with_next_ticket(workspace_id, ticket, OperationId::new())
            .await
    }

    /// Completes reconciliation using a caller-supplied ticket if a newer
    /// parent was observed while reconciliation was active.
    pub async fn complete_reconcile_with_next_ticket(
        &self,
        workspace_id: WorkspaceId,
        ticket: OperationId,
        next_ticket: OperationId,
    ) -> Result<(), OperationWindowError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(workspace_id).await?;
            match current.phase {
                OperationWindowPhase::Reconciling {
                    ticket: current_ticket,
                    pinned_parent,
                    subsequent_parent,
                    ..
                } if current_ticket == ticket => {
                    current.phase = match subsequent_parent {
                        Some(parent) => OperationWindowPhase::Reconciling {
                            ticket: next_ticket,
                            pinned_parent,
                            pending_parent: Some(parent),
                            subsequent_parent: None,
                        },
                        None => OperationWindowPhase::Idle,
                    };
                }
                _ => return Err(OperationWindowError::StaleTicket),
            }
            let expected = next_revision(&mut current.revision);
            if self.cas(workspace_id, expected, current).await? {
                return Ok(());
            }
        }
        Err(OperationWindowError::Contended)
    }

    /// Returns validated durable state for diagnostics and recovery.
    pub async fn inspect(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<OperationWindowSnapshot, OperationWindowError<S::Error>> {
        self.snapshot(workspace_id).await
    }

    /// Loads and validates the current durable window state for inspection.
    pub async fn snapshot(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<OperationWindowSnapshot, OperationWindowError<S::Error>> {
        let stored = self
            .store
            .load(workspace_id)
            .await
            .map_err(OperationWindowError::Store)?;
        match stored {
            Some(state) if state.is_valid_for(workspace_id) => Ok(state),
            Some(_) => Err(OperationWindowError::IncompatibleState),
            None => Ok(OperationWindowSnapshot::idle(workspace_id)),
        }
    }

    async fn cas(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: OperationWindowSnapshot,
    ) -> Result<bool, OperationWindowError<S::Error>> {
        if !replacement.is_valid_for(workspace_id)
            || replacement.revision != expected_revision.saturating_add(1)
        {
            return Err(OperationWindowError::IncompatibleState);
        }
        self.store
            .compare_and_swap(workspace_id, expected_revision, replacement)
            .await
            .map_err(OperationWindowError::Store)
    }
}

fn expire_to_reconcile(
    phase: &mut OperationWindowPhase,
    now_millis: u64,
    ticket: OperationId,
) -> Option<OperationWindowReconcile> {
    let OperationWindowPhase::Active {
        pinned_parent,
        leases,
        pending_parent,
    } = phase
    else {
        return None;
    };
    leases.retain(|_, lease| lease.expires_at_millis > now_millis);
    if !leases.is_empty() {
        return None;
    }
    let reconcile = OperationWindowReconcile {
        ticket,
        pinned_parent: *pinned_parent,
        pending_parent: *pending_parent,
    };
    *phase = OperationWindowPhase::Reconciling {
        ticket: reconcile.ticket,
        pinned_parent: reconcile.pinned_parent,
        pending_parent: reconcile.pending_parent,
        subsequent_parent: None,
    };
    Some(reconcile)
}

fn close_lease(
    phase: &mut OperationWindowPhase,
    lease_id: OperationLeaseId,
    expected_expiry_millis: u64,
    now_millis: u64,
    reconciliation_ticket: OperationId,
) -> Option<OperationWindowFinish> {
    let OperationWindowPhase::Active {
        pinned_parent,
        leases,
        pending_parent,
    } = phase
    else {
        return None;
    };
    let requested_live = leases.get(&lease_id).is_some_and(|lease| {
        lease.expires_at_millis == expected_expiry_millis && lease.expires_at_millis > now_millis
    });
    if leases
        .get(&lease_id)
        .is_some_and(|lease| lease.expires_at_millis != expected_expiry_millis)
    {
        return None;
    }
    leases.remove(&lease_id);
    leases.retain(|_, lease| lease.expires_at_millis > now_millis);
    if !leases.is_empty() {
        if !requested_live {
            return None;
        }
        return Some(OperationWindowFinish::StillActive {
            remaining: u32::try_from(leases.len()).unwrap_or(u32::MAX),
        });
    }
    let reconcile = OperationWindowReconcile {
        ticket: reconciliation_ticket,
        pinned_parent: *pinned_parent,
        pending_parent: *pending_parent,
    };
    *phase = OperationWindowPhase::Reconciling {
        ticket: reconcile.ticket,
        pinned_parent: reconcile.pinned_parent,
        pending_parent: reconcile.pending_parent,
        subsequent_parent: None,
    };
    requested_live.then_some(OperationWindowFinish::Reconcile(reconcile))
}

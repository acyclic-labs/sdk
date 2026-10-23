use super::*;
use crate::access::{Action, Capability, Context, PathTrie, Policy, Principal, Resource};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Authorize and publish under one process-owned policy cut. The operation
    /// must reject clock-expiring authority, which this publication lease cannot freeze.
    pub async fn with_stable_policy<F, T>(
        &self,
        operation: impl FnOnce() -> F,
    ) -> Result<T, WorkspaceError>
    where
        A: 'static,
        F: Future<Output = T>,
    {
        if !self.volume.fs.has_process_policy_owner() {
            return Err(WorkspaceError::AccessDenied);
        }
        let policy = crate::access::projection::enter(self.id.volume_id())?;
        let result = operation().await;
        drop(policy);
        Ok(result)
    }

    /// Intersect a deployment-verified coarse action ceiling with persisted
    /// policy. Adapters lacking path enforcement must reject any restriction.
    pub async fn bind_coarse_access(
        self,
        actions: &[Action],
        cancellation: &CancellationToken,
    ) -> Result<Self, WorkspaceError> {
        let principal = Principal {
            kind: crate::access::PrincipalKind::Anonymous,
            id: String::new(),
            lineage: Vec::new(),
            groups: BTreeSet::new(),
            roles: BTreeSet::new(),
            tags: BTreeSet::new(),
            attributes: BTreeMap::new(),
        };
        let capability = Capability {
            layers: vec![vec![crate::access::Grant {
                actions: actions.iter().copied().collect(),
                resource: crate::access::ResourcePattern {
                    lineage_root: None,
                    volume: Some(
                        uuid::Uuid::from_bytes(self.id.volume_id().into_bytes()).to_string(),
                    ),
                    generations: crate::access::Generations::All,
                    path: "**".into(),
                },
            }]],
            expires_at: None,
        };
        self.unrestricted_access(&principal, &capability, actions, cancellation)
            .await
    }

    /// Compile persisted policy for a host-authenticated caller. Missing policy
    /// and claims this adapter cannot authenticate are errors, never defaults.
    async fn compile_access(
        &self,
        principal: &Principal,
        capability: &Capability,
        cancellation: &CancellationToken,
    ) -> Result<PathTrie, WorkspaceError> {
        let mut checkout = self
            .volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                WorkBudget::UNBOUNDED,
                cancellation,
            )
            .await
            .map_err(|failure| WorkspaceError::from(failure.error))?
            .value;
        let (bytes, revision) = checkout
            .read_access_policy(WorkBudget::UNBOUNDED, cancellation)
            .await
            .map_err(|failure| WorkspaceError::from(failure.error))?
            .value;
        let bytes = bytes.ok_or(WorkspaceError::AccessDenied)?;
        let policy: Policy =
            serde_json::from_slice(&bytes.bytes).map_err(|_| WorkspaceError::AccessDenied)?;
        // These coarse adapters have no ordinal or authenticated task ancestry.
        // Never filter a conditional deny using invented facts.
        if policy.layers.iter().flatten().any(|rule| {
            rule.principal != crate::access::PrincipalPattern::default()
                || rule.resource.lineage_root.is_some()
                || rule.resource.generations != crate::access::Generations::All
                || rule.conditions.generations.is_some()
                || rule.conditions.relation.is_some()
                || rule.conditions.role.is_some()
                || rule.conditions.tag.is_some()
                || rule.conditions.approvals.is_some()
        }) {
            return Err(WorkspaceError::AccessDenied);
        }
        let volume = uuid::Uuid::from_bytes(self.id.volume_id().into_bytes()).to_string();
        PathTrie::compile(
            &policy,
            principal,
            Resource {
                config: self.volume.config,
                lineage: &[],
                volume: &volume,
                generation: 0,
                head: 0,
                path: "",
            },
            Context {
                now: access_time()?,
                approvals: 0,
            },
            capability,
        )
        .map(|view| view.with_policy_revision(checkout.generation_id(), revision))
        .map_err(|_| WorkspaceError::AccessDenied)
    }

    /// Bind a coarse adapter only when its entire exposed operation set is
    /// authorized. Every later checkout fences this view against its own root.
    async fn unrestricted_access(
        mut self,
        principal: &Principal,
        capability: &Capability,
        actions: &[Action],
        cancellation: &CancellationToken,
    ) -> Result<Self, WorkspaceError> {
        let view = self
            .compile_access(principal, capability, cancellation)
            .await?;
        if !view.allows_every_path(actions, access_time()?) {
            return Err(WorkspaceError::AccessDenied);
        }
        self.access = Some((
            Arc::new(view),
            actions
                .iter()
                .fold(0, |mask, action| mask | 1 << *action as u16),
        ));
        Ok(self)
    }

    pub(super) async fn check_access(
        &self,
        checkout: &mut Checkout<A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        if self.access.is_none() {
            return Ok(());
        }
        let mut frame = None;
        let (mut future, bytes) = crate::heap_future::in_heap_admitted(
            (&mut *accounting, &mut frame),
            |(accounting, frame), bytes| {
                **frame = Some(accounting.claim_owned(bytes, u64::from(bytes != 0))?);
                Ok::<_, WorkspaceError>(())
            },
            |(accounting, _)| self.check_access_revision(checkout, accounting, cancellation),
        )?;
        let result = future.as_mut().await;
        drop(future);
        if let Some(mut frame) = frame {
            accounting.release_owned(&mut frame, bytes)?;
        }
        result
    }

    pub(crate) async fn check_current_access(
        &self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let Some((view, actions)) = &self.access else {
            return Ok(());
        };
        let observed = self
            .current_access_policy_revision_accounted(accounting, cancellation)
            .await?;
        let now = access_time()?;
        if !observed.is_some_and(|(generation, revision)| {
            view.matches_policy_revision(generation, revision)
        }) || !Action::ALL
            .into_iter()
            .filter(|action| *actions & (1 << *action as u16) != 0)
            .all(|action| view.allows_every_path(&[action], now))
        {
            return Err(WorkspaceError::AccessDenied);
        }
        Ok(())
    }

    async fn check_access_revision(
        &self,
        checkout: &mut Checkout<A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let Some((view, actions)) = &self.access else {
            return Ok(());
        };
        if checkout.mode().access != crate::model::AccessMode::ReadOnly
            && *actions & (1 << Action::Write as u16) == 0
        {
            return Err(WorkspaceError::AccessDenied);
        }
        let now = access_time()?;
        let metadata = checkout
            .read_metadata_with_accounting(checkout.root().root_file_id, accounting, cancellation)
            .await?;
        if !view.matches_policy_revision(checkout.generation_id(), metadata.named_attributes)
            || !Action::ALL
                .into_iter()
                .filter(|action| *actions & (1 << *action as u16) != 0)
                .all(|action| view.allows_every_path(&[action], now))
        {
            return Err(WorkspaceError::AccessDenied);
        }
        Ok(())
    }
}

pub(crate) fn access_time() -> Result<u64, WorkspaceError> {
    use acyclic_stream::UnixMillisClock as _;
    let millis = acyclic_stream::SystemUnixMillisClock.now_unix_millis();
    if millis == 0 {
        return Err(WorkspaceError::AccessDenied);
    }
    Ok(millis / 1000)
}

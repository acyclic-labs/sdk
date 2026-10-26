use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Transfers the configured issuer into the runtime. The harness package
    /// receives only workload scopes; it cannot issue a grant or choose an agent.
    pub fn with_authority_custodian(mut self, issuer: &crate::core::AuthorityRoot) -> Result<Self> {
        if self.authority_custodian.is_some() {
            return Err(Error::Conflict("runtime authority is already bound".into()));
        }
        if issuer.verifier(&mut None)? != self.verifier {
            return Err(Error::Unauthorized(
                "runtime custodian belongs to another authority".into(),
            ));
        }
        self.authority_custodian = Some(issuer.copy(&mut None)?);
        Ok(self)
    }

    /// Derives source authority from the current retained task and lease. No
    /// caller-supplied capabilities or agent identity enter the signing boundary.
    pub async fn fork_source_scope(&self, task: TaskId, fence: LeaseFence) -> Result<Scope> {
        self.derive_fork_source_scope(task, fence, None).await
    }

    /// Authorizes a workspace fork for an exact registered descendant task.
    /// Native hosts need task-spawn authority, without harness-tool authority.
    pub async fn fork_source_scope_for_task<I: 'static, Output: 'static>(
        &self,
        task: TaskId,
        fence: LeaseFence,
        definition: &crate::runtime::TaskDefinition<I, Output>,
    ) -> Result<Scope> {
        let identity = definition.identity();
        let registered = self
            .harness
            .task::<I, Output>(&format!("{}@{}", identity.name, identity.version))?;
        if registered.identity() != identity {
            return Err(Error::Unauthorized(
                "fork descendant differs from its registered definition".into(),
            ));
        }
        self.derive_fork_source_scope(task, fence, Some(identity))
            .await
    }

    async fn derive_fork_source_scope(
        &self,
        task: TaskId,
        fence: LeaseFence,
        descendant: Option<&crate::registry::ComponentIdentity>,
    ) -> Result<Scope> {
        let issuer = self
            .authority_custodian
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("runtime has no authority custodian".into()))?;
        let context = self.task_context(task, fence).await?;
        let scope = context.scope();
        if scope
            .run_limits()
            .deadline_epoch_ms
            .is_some_and(|deadline| self.host.clock.now_unix_millis() >= deadline)
        {
            return Err(Error::Unauthorized("source workload has expired".into()));
        }
        if let Some(identity) = descendant {
            crate::runtime::require_descendant_grant(scope.grants(), identity)?;
        } else if !scope.grants().contains("tool:call:spawn") {
            return Err(Error::Unauthorized(
                "source task lacks spawn authority".into(),
            ));
        }
        let agent = if let Some(created) = &scope.created_access {
            let mut private = created.content.iter().filter(|binding| {
                binding.volume.class() == crate::conversation::VolumeClass::AgentPrivate
            });
            let binding = private.next().ok_or_else(|| {
                Error::Unauthorized("source task has no created private volume".into())
            })?;
            if private.next().is_some() {
                return Err(Error::Unauthorized(
                    "source task has ambiguous private volumes".into(),
                ));
            }
            let crate::conversation::VolumeOwner::Agent(agent) = binding.volume.owner() else {
                return Err(Error::Unauthorized(
                    "source private volume has no agent owner".into(),
                ));
            };
            *agent
        } else {
            if !scope.grants().is_subset_of(self.signed.capabilities()) {
                return Err(Error::Unauthorized(
                    "source task widens its runtime workload".into(),
                ));
            }
            self.signed.agent().ok_or_else(|| {
                Error::Unauthorized("runtime workload has no agent identity".into())
            })?
        };
        self.verifier.verify(&self.signed, &mut None)?;
        let derived = if self.signed.agent() == Some(agent)
            && scope.grants().is_subset_of(self.signed.capabilities())
        {
            issuer.attenuate(&self.signed, "child-source", scope.grants())?
        } else {
            issuer.issue_derived(
                &self.signed,
                "child-source",
                scope.grants(),
                Some(agent),
                &mut None,
            )?
        };
        if let Some(deadline) = scope.run_limits().deadline_epoch_ms {
            let remaining = deadline
                .checked_sub(self.host.clock.now_unix_millis())
                .ok_or_else(|| Error::Unauthorized("source workload has expired".into()))?;
            let now = acyclic_stream::UnixMillisClock::now_unix_millis(
                &acyclic_stream::SystemUnixMillisClock,
            ) / 1000;
            let expires = now
                .checked_add(remaining.div_ceil(1000))
                .ok_or_else(|| Error::Unauthorized("source workload deadline overflows".into()))?;
            derived.with_workload_expiry(
                expires,
                &issuer.public_workload_trust()?,
                issuer.audience(),
                now,
            )
        } else {
            Ok(derived)
        }
    }
}

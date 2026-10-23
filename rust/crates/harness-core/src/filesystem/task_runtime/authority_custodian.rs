use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Transfers the configured issuer into the runtime. The harness package
    /// receives only workload scopes; it cannot issue a grant or choose an agent.
    pub fn with_authority_custodian(
        mut self,
        issuer: &crate::core::AuthorityIssuer,
    ) -> Result<Self> {
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
        if !scope.grants().contains("tool:call:spawn") {
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
                &issuer.public_workload_trust(),
                issuer.audience(),
                now,
            )
        } else {
            Ok(derived)
        }
    }
}

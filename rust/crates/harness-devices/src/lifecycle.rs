//! Archive releases the original grant, through the agent's closing participant.
use crate::DeviceGrant;
use acyclic_harness_core::{
    Error, Result,
    agent::{AgentLifecycle, AgentPath},
    core::AuthorityRoot,
};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

/// One agent's original device grant and its existing resource lifecycle.
/// Pass this same binding to [`acyclic_harness_core::agent::AgentComposer`]. Closing revokes
/// the parent grant of every queued or active device command before releasing
/// Machines; pausing keeps the grant and delegates only resource suspension.
pub struct DeviceLifecycle {
    grant: Arc<DeviceGrant>,
    owner: AuthorityRoot,
    resources: Option<Arc<dyn AgentLifecycle>>,
}

impl DeviceLifecycle {
    pub(crate) fn new(
        grant: Arc<DeviceGrant>,
        owner: AuthorityRoot,
        resources: Option<Arc<dyn AgentLifecycle>>,
    ) -> Result<Self> {
        if grant.scope.agent().is_none() {
            return Err(Error::Unauthorized(
                "device lifecycle has no original agent grant".into(),
            ));
        }
        owner.verifier(&mut None)?.verify(&grant.scope, &mut None)?;
        Ok(Self {
            grant,
            owner,
            resources,
        })
    }

    fn check(
        &self,
        agent: &AgentPath,
    ) -> std::result::Result<(), acyclic_harness_core::agent::Error> {
        if self.grant.scope.agent().map(|id| id.into_bytes()) != Some(agent.id_bytes()) {
            return Err(acyclic_harness_core::agent::Error::Turn(Box::new(
                Error::Unauthorized("device lifecycle belongs to another agent".into()),
            )));
        }
        Ok(())
    }
}

impl AgentLifecycle for DeviceLifecycle {
    fn pause<'a>(
        &'a self,
        agent: &'a AgentPath,
    ) -> BoxProviderFuture<'a, std::result::Result<(), acyclic_harness_core::agent::Error>> {
        Box::pin(async move {
            self.check(agent)?;
            if let Some(resources) = &self.resources {
                resources.pause(agent).await?;
            }
            Ok(())
        })
    }

    fn close<'a>(
        &'a self,
        agent: &'a AgentPath,
    ) -> BoxProviderFuture<'a, std::result::Result<(), acyclic_harness_core::agent::Error>> {
        Box::pin(async move {
            self.check(agent)?;
            self.owner
                .revoke_workload(&self.grant.scope)
                .map_err(|error| acyclic_harness_core::agent::Error::Turn(Box::new(error)))?;
            if let Some(resources) = &self.resources {
                resources.close(agent).await?;
            }
            Ok(())
        })
    }
}

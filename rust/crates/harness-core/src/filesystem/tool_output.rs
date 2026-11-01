//! Live observation of one agent: its conversation, status and history and its
//! tool output, `tools/` on its private volume.
use super::*;
use crate::core::AuthorityRoot;
use acyclic_auth::{
    trust::{ChangeSource, Follower, SourceClosed},
    verify::VerifyError,
};
use acyclic_stream::UnixMillisClock;
use acyclic_stream::session::{Authority, AuthorityState, TrustSnapshot};

/// A read-only observation of one agent: its conversation, status and history
/// and its tool output, under one credential.
pub struct AgentObservation {
    /// The `fs:/` resource of [`TOOL_OUTPUT`]; a turn's output is under
    /// `{resource}/{turn}`.
    pub resource: String,
    /// Sealed credential to follow the agent's conversation, status and history and to
    /// list and read under `resource`, bound to the agent's audience; it lives
    /// no longer than the agent's scope and is revoked with it.
    pub credential: Vec<u8>,
    /// Session authority admitting `credential` under the agent's audience.
    pub authority: Arc<Authority>,
    /// The principal `credential` authenticates: who its commands, such as
    /// approval decisions, are recorded as.
    pub principal: String,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemHost<A, O> {
    /// Native enrollment derives an [`AgentObservation`] of `agent`, whose private volume is
    /// `volume`, from `authority`'s scope at `clock`'s time. With `calls`, the
    /// credential also invokes that actor resource: the hosting server of
    /// `agent`'s tree, so one credential drives and observes it.
    ///
    /// # Errors
    /// Fails unless that scope is `agent`'s and already grants listing and
    /// reading all of [`TOOL_OUTPUT`] on `volume`, or when the volume cannot be
    /// opened.
    pub async fn agent_observation(
        &self,
        authority: &AgentAuthority<'_>,
        agent: &crate::agent::AgentPath,
        volume: &VolumeRef,
        clock: Arc<dyn UnixMillisClock>,
        calls: Option<&str>,
    ) -> Result<AgentObservation> {
        let issuer = authority.issuer.native().ok_or_else(|| {
            Error::Invalid("client credential enrollment requires native custody".into())
        })?;
        let (resource, capability) = self
            .observation_credential(
                authority,
                agent,
                volume,
                seconds(clock.as_ref()),
                calls,
                None,
            )
            .await?;
        let principal = capability.effective().principal().id.clone();
        let credential = capability.encode().to_vec();
        Ok(AgentObservation {
            resource,
            credential,
            authority: Arc::new(session_authority(issuer, clock)?),
            principal,
        })
    }
}

/// The trust every credential `issuer` signs is verified against: its
/// runtime's trust follower, read on every admission at `clock`'s time
/// protected by the follower. Each published change, such as a revocation,
/// makes the session revalidate every open follow and end those the revoked
/// credential no longer authorizes; losing the follower fences them all.
///
/// # Errors
/// When `issuer`'s runtime trust is unavailable.
pub fn session_trust(
    issuer: &AuthorityRoot,
    clock: Arc<dyn UnixMillisClock>,
) -> Result<Arc<dyn AuthorityState>> {
    Ok(Arc::new(LocalTrust {
        trust: issuer.public_workload_trust()?,
        clock,
    }))
}

/// The session authority admitting every credential `issuer` signs, under
/// its audience ([`session_trust`]): what a host holding `issuer`'s custody
/// verifies its clients' credentials against.
///
/// # Errors
/// When `issuer`'s runtime trust is unavailable or its audience is not a
/// session resource.
pub fn session_authority(
    issuer: &AuthorityRoot,
    clock: Arc<dyn UnixMillisClock>,
) -> Result<Authority> {
    Authority::new(
        session_trust(issuer, clock)?,
        issuer.audience_resource()?,
        None,
    )
    .map_err(|error| Error::Invalid(format!("session audience: {error}")))
}

/// The runtime's trust follower, both its trust and its publications, and
/// the harness's clock.
struct LocalTrust {
    trust: Arc<Follower>,
    clock: Arc<dyn UnixMillisClock>,
}

fn seconds(clock: &dyn UnixMillisClock) -> u64 {
    clock.now_unix_millis() / 1000
}

impl AuthorityState for LocalTrust {
    fn snapshot(&self) -> std::result::Result<TrustSnapshot, VerifyError> {
        Ok(TrustSnapshot {
            trust: self.trust.clone(),
            now_unix_seconds: self.trust.protected_now(seconds(self.clock.as_ref())),
        })
    }

    fn changes(&self) -> &dyn ChangeSource {
        &*self.trust
    }

    /// Completes once the harness's clock reaches `unix_seconds`. The wait is
    /// the process's one wall-clock alarm, re-armed only while the harness's
    /// clock still runs behind when it fires.
    fn expires_at(
        &self,
        unix_seconds: u64,
    ) -> acyclic_stream::BoxProviderFuture<'_, std::result::Result<(), SourceClosed>> {
        Box::pin(async move {
            let deadline = unix_seconds.saturating_mul(1000);
            loop {
                let remaining = deadline.saturating_sub(self.clock.now_unix_millis());
                if remaining == 0 {
                    return Ok(());
                }
                let wall = acyclic_stream::SystemUnixMillisClock.now_unix_millis();
                let (fired, wake) = tokio::sync::oneshot::channel();
                let fired = std::sync::Mutex::new(Some(fired));
                let alarm = acyclic_native_runtime::WallAlarm::register(
                    wall.saturating_add(remaining),
                    Arc::new(move |outcome| {
                        if let Some(fired) = fired.lock().ok().and_then(|mut fired| fired.take()) {
                            let _ = fired.send(outcome);
                        }
                    }),
                );
                let _alarm = alarm.map_err(|_| SourceClosed)?;
                wake.await
                    .map_err(|_| SourceClosed)?
                    .map_err(|_| SourceClosed)?;
            }
        })
    }
}

//! Live observation of one agent: its conversation and status streams and its
//! tool output, `tools/` on its private volume.
use super::*;
use crate::core::AuthorityRoot;
use acyclic_auth::verify::VerifyError;
use acyclic_stream::UnixMillisClock;
use acyclic_stream::session::{Authority, AuthorityState, TrustSnapshot};

/// Tool output directory on an agent's private volume.
pub const TOOL_OUTPUT: &str = "tools";

/// A read-only observation of one agent: its conversation and status streams
/// and its tool output, under one credential.
pub struct AgentObservation {
    /// The `fs:/` resource of [`TOOL_OUTPUT`]; a turn's output is under
    /// `{resource}/{turn}`.
    pub resource: String,
    /// Sealed credential to follow the agent's conversation and status and to
    /// list and read under `resource`, bound to the agent's audience; it lives
    /// no longer than the agent's scope and is revoked with it.
    pub credential: Vec<u8>,
    /// Session authority admitting `credential` under the agent's audience.
    pub authority: Arc<Authority>,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemHost<A, O> {
    /// Derives an [`AgentObservation`] of `agent`, whose private volume is
    /// `volume`, from `authority`'s scope at `clock`'s time.
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
    ) -> Result<AgentObservation> {
        if authority.scope.agent() != Some(crate::AgentId::from_bytes(agent.id().to_bytes())) {
            return Err(Error::Unauthorized(
                "an observation derives from the agent's own scope".into(),
            ));
        }
        ContentGrant::verify_directory_read(
            &authority.verifier,
            &authority.scope,
            volume,
            TOOL_OUTPUT,
            &mut None,
        )?;
        let reference = super::bounded_io::content_workspace(&self.provider, volume, None)?;
        let workspace = self.open(&reference, None).await?;
        // A structured filesystem scope is only enforced by its compiled
        // view, so prove it before minting: the derived credential may never
        // reach a path or action the parent cannot. The parent must list and
        // read all of `tools/`; the rest of the volume does not matter.
        if let Some(label) =
            crate::core::filesystem_scope_label(authority.scope.capabilities(), &mut None)?
        {
            let cancellation = acyclic_fs::CancellationToken::new();
            let io = PreparationIo::from_budget(
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )?;
            let view = super::access::access_view(
                &workspace,
                &authority.scope,
                None,
                super::access::capability_payload(Some(label), VolumeOperation::Read, &io)?,
                None,
                &io,
            )
            .await?;
            if !view.0.allows_subtree(
                TOOL_OUTPUT,
                &[
                    acyclic_fs::access::Action::List,
                    acyclic_fs::access::Action::Read,
                ],
                super::access::access_now()?,
            ) {
                return Err(Error::Unauthorized(
                    "an observation needs list and read on all of the tool output".into(),
                ));
            }
        }
        let resource = acyclic_fs::session::resource(workspace.id().volume_id(), TOOL_OUTPUT)
            .map_err(|error| Error::Invalid(format!("tool output resource: {error}")))?;
        let stream =
            |path: std::result::Result<acyclic_stream::StreamPath, crate::agent::Error>| {
                path.map(|path| acyclic_stream::wire_codec::session::stream_resource(path.as_str()))
                    .map_err(|error| Error::Invalid(format!("agent stream: {error}")))
            };
        let (conversation, status) = (stream(agent.conversation())?, stream(agent.status())?);
        let follow: &[&str] = &["streams/follow"];
        let read: &[&str] = &["fs/list", "fs/read"];
        let credential = authority.issuer.resource_credential(
            &authority.scope,
            &[
                (conversation.as_str(), false, follow),
                (status.as_str(), false, follow),
                (resource.as_str(), true, read),
            ],
            seconds(clock.as_ref()),
        )?;
        let session = Authority::new(
            Arc::new(LocalTrust {
                root: authority.issuer.clone(),
                revisions: authority.issuer.trust_revisions(),
                clock,
            }),
            authority.issuer.audience_resource()?,
            None,
        )
        .map_err(|error| Error::Invalid(format!("observation audience: {error}")))?;
        Ok(AgentObservation {
            resource,
            credential,
            authority: Arc::new(session),
        })
    }

    /// Serves change follows of this host's volumes over `streams`, the
    /// filesystem's own stream provider.
    ///
    /// # Errors
    /// Fails unless `streams` shares the filesystem's process-owned backend.
    pub fn change_provider<P: acyclic_stream::StreamProvider>(
        &self,
        streams: Arc<P>,
    ) -> Result<acyclic_fs::session::ChangeProvider<P, A, O>> {
        acyclic_fs::session::ChangeProvider::new(self.filesystem.clone(), streams)
            .map_err(|error| Error::Unauthorized(format!("tool output changes: {error}")))
    }
}

/// The local runtime's public trust, read on every admission at the harness's
/// clock. A revocation advances the trust revision, so the session
/// revalidates each open follow and ends those the revoked credential no
/// longer authorizes.
struct LocalTrust {
    root: AuthorityRoot,
    revisions: tokio::sync::watch::Receiver<u64>,
    clock: Arc<dyn UnixMillisClock>,
}

fn seconds(clock: &dyn UnixMillisClock) -> u64 {
    clock.now_unix_millis() / 1000
}

impl AuthorityState for LocalTrust {
    fn snapshot(&self) -> std::result::Result<TrustSnapshot, VerifyError> {
        let (trust, revision) = self
            .root
            .public_trust_revision()
            .map_err(|_| VerifyError::Trust)?;
        Ok(TrustSnapshot {
            trust: Arc::new(trust),
            now_unix_seconds: seconds(self.clock.as_ref()),
            revision,
        })
    }

    fn changed(&self, revision: u64) -> acyclic_stream::BoxProviderFuture<'_, ()> {
        let mut revisions = self.revisions.clone();
        Box::pin(async move {
            // A closed watch means the runtime is gone; nothing changes again.
            if revisions
                .wait_for(|current| *current != revision)
                .await
                .is_err()
            {
                futures::future::pending::<()>().await;
            }
        })
    }

    /// Completes once the harness's clock reaches `unix_seconds`. The wait is
    /// the process's one wall-clock alarm, re-armed only while the harness's
    /// clock still runs behind when it fires.
    fn expires_at(&self, unix_seconds: u64) -> acyclic_stream::BoxProviderFuture<'_, ()> {
        Box::pin(async move {
            let deadline = unix_seconds.saturating_mul(1000);
            loop {
                let remaining = deadline.saturating_sub(self.clock.now_unix_millis());
                if remaining == 0 {
                    return;
                }
                let wall = acyclic_stream::SystemUnixMillisClock.now_unix_millis();
                let (fired, wake) = tokio::sync::oneshot::channel::<()>();
                let fired = std::sync::Mutex::new(Some(fired));
                let alarm = acyclic_native_runtime::WallAlarm::register(
                    wall.saturating_add(remaining),
                    Arc::new(move || {
                        if let Some(fired) = fired.lock().ok().and_then(|mut fired| fired.take()) {
                            let _ = fired.send(());
                        }
                    }),
                );
                match alarm {
                    Ok(_alarm) => {
                        let _ = wake.await;
                    }
                    // Without the alarm thread nothing can wake this wait; the
                    // credential's own expiry still refuses each admission.
                    Err(_) => futures::future::pending::<()>().await,
                }
            }
        })
    }
}

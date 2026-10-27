//! Live follows of an agent's tool output: `tools/` on its private volume.
use super::*;
use crate::core::AuthorityRoot;
use acyclic_auth::verify::VerifyError;
use acyclic_stream::session::{Authority, AuthorityState, TrustSnapshot};

/// Tool output directory on an agent's private volume.
pub const TOOL_OUTPUT: &str = "tools";

/// A read-only follow of one agent's tool output.
pub struct ToolOutputFollow {
    /// The `fs:/` resource of [`TOOL_OUTPUT`]; a turn's output is under
    /// `{resource}/{turn}`.
    pub resource: String,
    /// Sealed credential to list and read under `resource`, bound to the
    /// agent's audience; it lives no longer than the agent's scope.
    pub credential: Vec<u8>,
    /// Session authority admitting `credential` under the agent's audience.
    pub authority: Arc<Authority>,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemHost<A, O> {
    /// Derives a [`ToolOutputFollow`] of `volume` from `authority`'s scope.
    ///
    /// # Errors
    /// Fails unless that scope already grants reading [`TOOL_OUTPUT`] on
    /// `volume`, or when the volume cannot be opened.
    pub async fn tool_output_follow(
        &self,
        authority: &AgentAuthority<'_>,
        volume: &VolumeRef,
    ) -> Result<ToolOutputFollow> {
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
        // reach a path or action the parent cannot. Conservatively, the parent
        // must list and read every path on the volume.
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
            if !view.0.allows_every_path(
                &[
                    acyclic_fs::access::Action::List,
                    acyclic_fs::access::Action::Read,
                ],
                super::access::access_now()?,
            ) {
                return Err(Error::Unauthorized(
                    "tool output follow needs list and read on the whole volume".into(),
                ));
            }
        }
        let resource = acyclic_fs::session::resource(workspace.id().volume_id(), TOOL_OUTPUT)
            .map_err(|error| Error::Invalid(format!("tool output resource: {error}")))?;
        let credential = authority
            .issuer
            .resource_read_credential(&authority.scope, &resource)?;
        let session = Authority::new(
            Arc::new(LocalTrust(authority.issuer.clone())),
            authority.issuer.audience_resource()?,
            None,
        )
        .map_err(|error| Error::Invalid(format!("tool output audience: {error}")))?;
        Ok(ToolOutputFollow {
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

/// The local runtime's public trust, read on every admission. Local trust has
/// no external keyring feed, so it never announces a change.
struct LocalTrust(AuthorityRoot);

fn now_unix_seconds() -> u64 {
    acyclic_stream::UnixMillisClock::now_unix_millis(&acyclic_stream::SystemUnixMillisClock) / 1000
}

impl AuthorityState for LocalTrust {
    fn snapshot(&self) -> std::result::Result<TrustSnapshot, VerifyError> {
        let trust = self
            .0
            .public_workload_trust()
            .map_err(|_| VerifyError::Trust)?;
        Ok(TrustSnapshot {
            trust: Arc::new(trust),
            now_unix_seconds: now_unix_seconds(),
            revision: 0,
        })
    }
    fn changed(&self, _: u64) -> acyclic_stream::BoxProviderFuture<'_, ()> {
        Box::pin(futures::future::pending())
    }
    fn expires_at(&self, unix_seconds: u64) -> acyclic_stream::BoxProviderFuture<'_, ()> {
        let wait = unix_seconds.saturating_sub(now_unix_seconds());
        Box::pin(tokio::time::sleep(std::time::Duration::from_secs(wait)))
    }
}

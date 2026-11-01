//! One own-scope proof and resource credential for live agent observation.
use super::*;
use acyclic_auth::{
    contract::Principal,
    verify::{TrustEvidence, VerifiedCapability},
};

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemHost<A, O> {
    /// Native observation and session enrollment consume the same selected proof.
    /// The caller contributes identity and dependencies, never a selected path.
    pub(crate) async fn observation_credential(
        &self,
        authority: &AgentAuthority<'_>,
        agent: &crate::agent::AgentPath,
        volume: &VolumeRef,
        now: u64,
        calls: Option<&str>,
        caller: Option<(&Principal, &TrustEvidence)>,
    ) -> Result<(String, VerifiedCapability)> {
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
        let history = stream(agent.history())?;
        let follow: &[&str] = &["streams/follow"];
        let read: &[&str] = &["fs/list", "fs/read"];
        let invoke: &[&str] = &["actors/invoke"];
        let grants = [
            (conversation.as_str(), false, follow),
            (status.as_str(), false, follow),
            (history.as_str(), false, follow),
            (resource.as_str(), true, read),
            (calls.unwrap_or(""), false, invoke),
        ];
        let grants = &grants[..if calls.is_some() { 5 } else { 4 }];
        let capability = authority
            .issuer
            .resource_credential(&crate::core::ResourceIssuance {
                parent: &authority.scope,
                grants,
                caller,
                now,
            })
            .await?;
        Ok((resource, capability))
    }
}

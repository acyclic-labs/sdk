//! Directory and range projections under the original selected agent authority.
use super::*;
use crate::{
    agent::{WorkspaceRole, WorkspaceTable},
    conversation::{ContentGrant, DirectoryQuery, DirectoryTarget, PrivateDirectoryPage},
    resources::GenerationRef,
};

struct DirectoryBinding {
    volume: crate::conversation::VolumeRef,
    path: String,
    grant: ContentGrant,
    scope: Scope,
}

impl<P, O> Bound<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn view_host(
        &self,
        agent: &AgentPath,
        volume: &crate::conversation::VolumeRef,
    ) -> Result<Arc<Host<P, O>>, Error> {
        #[cfg(not(target_arch = "wasm32"))]
        if volume.class() == crate::conversation::VolumeClass::AgentPrivate
            && volume.owner()
                == &crate::conversation::VolumeOwner::Agent(crate::AgentId::from_bytes(
                    agent.id_bytes(),
                ))
        {
            return self
                .volumes
                .for_agent_views(
                    self.agents.provider().clone(),
                    agent,
                    volume,
                    self.input_content(agent, self.maximum_bytes).await?.reader,
                )
                .await
                .map_err(turn);
        }
        #[cfg(target_arch = "wasm32")]
        let _ = (agent, volume);
        Ok(Arc::clone(&self.volumes))
    }

    async fn directory_binding(
        &self,
        agent: &AgentPath,
        target: DirectoryTarget,
        path: &str,
    ) -> Result<DirectoryBinding, Error> {
        self.admit_path(agent)?;
        if !path.is_empty() {
            crate::conversation::validate_content_path(path, &mut None).map_err(turn)?;
        }
        let state = self.agents.state(agent).await?;
        let binding = match target {
            DirectoryTarget::Private {} => state
                .volumes
                .iter()
                .find(|binding| binding.role == WorkspaceRole::Private),
            DirectoryTarget::Primary {} => WorkspaceTable::primary_in(&state.volumes),
            DirectoryTarget::Workspace { id } => {
                state.volumes.iter().find(|binding| binding.id == id)
            }
        }
        .filter(|binding| binding.role != WorkspaceRole::Internal)
        .ok_or_else(|| {
            turn(crate::Error::Unauthorized(
                "workspace is not visible".into(),
            ))
        })?;
        let volume = binding.volume().map_err(turn)?.clone();
        let authority = agent_authority(
            &self.issuer,
            &self.admission,
            &state.path,
            &state.volumes,
            &[],
        )
        .await
        .map_err(turn)?;
        let path = match (binding.root.is_empty(), path.is_empty()) {
            (true, _) => path.to_owned(),
            (false, true) => binding.root.clone(),
            (false, false) => format!("{}/{path}", binding.root),
        };
        let grant = ContentGrant::verify_directory_read(
            &authority.verifier,
            &authority.scope,
            &volume,
            &path,
            &mut None,
        )
        .map_err(turn)?;
        Ok(DirectoryBinding {
            volume,
            path,
            grant,
            scope: authority.scope,
        })
    }

    /// Read one bounded file range at an exact workspace-directory generation.
    /// The current binding, original admission and persisted policy authorize
    /// every range; no path can widen a rooted workspace or select a volume.
    pub async fn read_path(
        &self,
        agent: &AgentPath,
        target: DirectoryTarget,
        path: &str,
        generation: &GenerationRef,
        range: acyclic_fs::ByteRange,
    ) -> Result<(acyclic_stream::OwnedBytes, u64), Error> {
        let binding = self.directory_binding(agent, target, path).await?;
        let host = self.view_host(agent, &binding.volume).await?;
        let (bytes, _selected, length) = host
            .read_private_range(
                &binding.volume,
                &binding.grant,
                &binding.path,
                Some(generation),
                range,
                ContentAccess {
                    ceiling: Some(binding.scope.capabilities()),
                    io: None,
                },
            )
            .await
            .map_err(turn)?;
        Ok((bytes, length))
    }

    /// Select visible names at one immutable generation under current policy.
    /// An authorized absent directory yields an empty page at that same cut,
    /// so callers can wait for its creation using the original volume producer.
    pub async fn directory_page(
        &self,
        agent: &AgentPath,
        query: DirectoryQuery<'_>,
        credit: Option<&mut crate::filesystem::DirectoryCredit<'_>>,
    ) -> Result<PrivateDirectoryPage, Error> {
        if (query.generation.is_none() && query.after.is_some())
            || (query.after_generation.is_some()
                && (query.generation.is_some() || query.after.is_some()))
        {
            return Err(Error::Record("invalid directory selection"));
        }
        let binding = self
            .directory_binding(agent, query.target, query.path)
            .await?;
        let host = self.view_host(agent, &binding.volume).await?;
        let generation = match query.generation {
            Some(generation) => generation.clone(),
            None => {
                self.volume_generation(&host, &binding.volume, query.after_generation)
                    .await?
            }
        };
        let options = super::super::DirectoryOptions {
            generation: Some(&generation),
            after: query.after,
            maximum_entries: query.maximum_entries,
            ceiling: Some(binding.scope.capabilities()),
        };
        let page = host
            .list_private_directory_with_credit(
                &binding.volume,
                &binding.grant,
                &binding.path,
                options,
                credit,
            )
            .await
            .map_err(turn)?;
        Ok(page.unwrap_or(PrivateDirectoryPage {
            examined: 0,
            generation,
            entries: Vec::new(),
            next: None,
        }))
    }
}

//! Directory and range projections under the original selected agent authority.
use super::*;
use crate::{
    agent::{WorkspaceRole, WorkspaceTable},
    conversation::{ContentGrant, DirectoryQuery, DirectoryTarget, PrivateDirectoryPage},
    resources::GenerationRef,
};

struct DirectoryBinding<P: ?Sized + 'static, O: 'static> {
    volume: crate::conversation::VolumeRef,
    path: String,
    grant: ContentGrant,
    scope: Scope,
    host: Arc<Host<P, O>>,
}

impl<P, O> Bound<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    async fn directory_binding(
        &self,
        agent: &AgentPath,
        target: DirectoryTarget,
        path: &str,
    ) -> Result<DirectoryBinding<P, O>, Error> {
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
        #[cfg(not(target_arch = "wasm32"))]
        let host = if binding.role == WorkspaceRole::Private {
            self.volumes
                .for_agent_views(
                    self.agents.provider().clone(),
                    agent,
                    &volume,
                    &authority.verifier,
                    &authority.scope,
                )
                .await
                .map_err(turn)?
        } else {
            self.volumes.clone()
        };
        #[cfg(target_arch = "wasm32")]
        let host = self.volumes.clone();
        Ok(DirectoryBinding {
            volume,
            path,
            grant,
            scope: authority.scope,
            host,
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
        let (bytes, _selected, length) = binding
            .host
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
        let generation = match query.generation {
            Some(generation) => generation.clone(),
            None => {
                self.directory_generation(&binding, query.after_generation)
                    .await?
            }
        };
        let options = super::super::DirectoryOptions {
            generation: Some(&generation),
            after: query.after,
            maximum_entries: query.maximum_entries,
            ceiling: Some(binding.scope.capabilities()),
        };
        let page = binding
            .host
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
            generation,
            entries: Vec::new(),
            next: None,
        }))
    }

    async fn directory_generation(
        &self,
        binding: &DirectoryBinding<P, O>,
        after: Option<&GenerationRef>,
    ) -> Result<GenerationRef, Error> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .map_err(|(error, _)| turn(crate::Error::from(error)))?;
            let io = super::super::PreparationIo::from_native_work(&work).map_err(turn)?;
            let Some(mut current) = binding
                .host
                .private_current_generation(&binding.volume, after, &io)
                .await
                .map_err(turn)?
            else {
                return self.volume_generation(&binding.volume, None, after).await;
            };
            let Some(after) = after else {
                return Ok(current);
            };
            if &current != after {
                return Ok(current);
            }
            let mut source = binding.host.private_changes(&work).map_err(turn)?;
            let reference = super::super::bounded_io::content_workspace(
                &self.volumes.provider,
                &binding.volume,
                None,
            )
            .map_err(turn)?;
            let workspace = self.volumes.open(&reference, None).await.map_err(turn)?;
            let changes = acyclic_fs::session::changes_path(
                &self.volumes.filesystem,
                workspace.id().volume_id(),
            )
            .map_err(Error::Stream)?;
            loop {
                // Capture the authored cursor before sampling the complete cut.
                // The source watch emits its opening invalidation after its
                // own follows are registered, covering that opening race too.
                let cursor = self.publication_cursor(changes.clone()).await?;
                current = binding
                    .host
                    .private_current_generation(&binding.volume, None, &io)
                    .await
                    .map_err(turn)?
                    .ok_or(Error::Record("private view binding disappeared"))?;
                if &current != after {
                    return Ok(current);
                }
                let authored = async {
                    use futures::TryStreamExt as _;
                    let updates = self
                        .agents
                        .provider()
                        .follow(cursor)
                        .await
                        .map_err(Error::Stream)?;
                    acyclic_stream::records_only(updates)
                        .try_next()
                        .await
                        .map_err(Error::Stream)?
                        .ok_or(Error::Stream(acyclic_stream::StreamError::Unavailable))?;
                    Ok::<_, Error>(())
                };
                if let Some(source) = &mut source {
                    tokio::select! {
                        changed = authored => changed?,
                        changed = source.changed() => changed.map_err(turn)?,
                    }
                } else {
                    authored.await?;
                }
            }
        }
        #[cfg(target_arch = "wasm32")]
        self.volume_generation(&binding.volume, None, after).await
    }
}

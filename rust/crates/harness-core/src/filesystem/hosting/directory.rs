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
        #[cfg(not(target_arch = "wasm32"))]
        let host = if binding.volume.class() == crate::conversation::VolumeClass::AgentPrivate {
            self.volumes
                .for_evaluations(self.agents.provider().clone(), agent, &binding.volume)
                .await
                .map_err(turn)?
        } else {
            self.volumes.clone()
        };
        #[cfg(target_arch = "wasm32")]
        let host = self.volumes.clone();
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
    /// An authorized absent directory yields an empty page at that same cut.
    /// Continuations are opaque and bound to the volume, path and selected cut.
    /// Resume with `next`, including after an empty page whose `has_more` is true;
    /// every resume applies current read/list authority before disclosing names.
    /// A smaller subsequent row credit retains the undisclosed suffix.
    pub async fn directory_page(
        &self,
        agent: &AgentPath,
        query: DirectoryQuery<'_>,
    ) -> Result<PrivateDirectoryPage, Error> {
        self.directory_page_select(agent, query, |page, _| Ok(page.entries.len()))
            .await
    }

    /// Apply row and byte credit before publishing a confidential continuation.
    /// The selector's estimate takes a zero-based row index and bounds the
    /// returned token's bytes. A missing estimate means this producer has no
    /// interior position; the selector may still accept the complete page.
    /// Returned rows respect this request's `maximum_entries`.
    pub async fn directory_page_select<F>(
        &self,
        agent: &AgentPath,
        query: DirectoryQuery<'_>,
        select: F,
    ) -> Result<PrivateDirectoryPage, Error>
    where
        F: FnOnce(&PrivateDirectoryPage, &dyn Fn(usize) -> Option<usize>) -> Result<usize, Error>
            + Send,
    {
        if query.after_generation.is_some()
            && (query.generation.is_some() || query.after_name.is_some())
        {
            return Err(Error::Record("invalid directory selection"));
        }
        let binding = self
            .directory_binding(agent, query.target, query.path)
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        let host = if binding.volume.class() == crate::conversation::VolumeClass::AgentPrivate {
            self.volumes
                .for_evaluations(self.agents.provider().clone(), agent, &binding.volume)
                .await
                .map_err(turn)?
        } else {
            self.volumes.clone()
        };
        #[cfg(target_arch = "wasm32")]
        let host = self.volumes.clone();
        let generation = if query.after_name.is_some() {
            query.generation.cloned()
        } else if binding.volume.class() == crate::conversation::VolumeClass::AgentPrivate
            && query.generation.is_none()
            && query.after_generation.is_none()
        {
            None
        } else {
            Some(match query.generation {
                Some(generation) => generation.clone(),
                None => {
                    self.volume_generation(&binding.volume, None, query.after_generation)
                        .await?
                }
            })
        };
        let options = super::super::DirectoryOptions {
            generation: generation.as_ref(),
            after: query.after_name,
            maximum_entries: query.maximum_entries,
            ceiling: Some(binding.scope.capabilities()),
        };
        let ledger =
            crate::filesystem::PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())
                .map_err(turn)?;
        let (mut page, _custody, positions) = {
            let page = host
                .list_private_directory(&binding.volume, &binding.grant, &binding.path, options)
                .await
                .map_err(turn)?;
            let Some((generation, mut page)) = page else {
                return Ok(PrivateDirectoryPage {
                    generation: generation
                        .ok_or(Error::Record("directory cursor lost its selection"))?,
                    entries: Vec::new(),
                    has_more: false,
                    next: None,
                    examined: 0,
                });
            };
            let positions = std::mem::take(&mut page.positions);
            let source = ledger
                .reserve_decode(
                    generation.logical_owned_bytes().map_err(turn)?
                        + page
                            .next
                            .as_ref()
                            .map_or(0, |token| token.capacity() as u64),
                )
                .map_err(turn)?;
            let (page, custody) =
                PrivateDirectoryPage::from_filesystem(generation, page, source, &ledger)
                    .map_err(turn)?;
            (page, custody, positions)
        };
        let generation = acyclic_fs::GenerationId::from_bytes(page.generation.as_resource().key());
        let generation_bytes = generation.as_ref().map_or(0, |id| id.to_bytes().len());
        #[cfg(not(target_arch = "wasm32"))]
        let prefixes = if generation.is_none() {
            Some(
                host.directory_prefixes(&page.generation, positions, &ledger)
                    .map_err(turn)?,
            )
        } else {
            drop(positions);
            None
        };
        #[cfg(target_arch = "wasm32")]
        drop(positions);
        let selected = select(&page, &|index| {
            let name = &page.entries.get(index)?.name;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(prefixes) = &prefixes {
                return prefixes.encoded_len(index, name, &binding.path);
            }
            super::super::host::directory::cursor::encoded_len(generation_bytes, name.len())
        })?;
        if selected > page.entries.len() || (selected == 0 && !page.entries.is_empty()) {
            return Err(Error::Record(
                "directory byte credit cannot admit its first entry",
            ));
        }
        // Row and byte credit select a prefix of the one physical page.
        let kept = selected.min(query.maximum_entries as usize);
        if kept == 0 && !page.entries.is_empty() {
            return Err(Error::Record(
                "directory row credit cannot admit its first entry",
            ));
        }
        if kept < page.entries.len() {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(prefixes) = &prefixes {
                let (token, _hold) = host
                    .seal_directory_prefix(
                        prefixes,
                        kept - 1,
                        &page.entries[kept - 1].name,
                        &binding.volume,
                        &binding.path,
                        &ledger,
                    )
                    .map_err(turn)?;
                page.entries.truncate(kept);
                page.has_more = true;
                page.next = Some(token);
                return Ok(page);
            }
            let generation = generation.ok_or(Error::Record("directory generation is absent"))?;
            let name = &page.entries[kept - 1].name;
            let after = acyclic_fs::kernel::LogicalName::new(
                acyclic_fs::kernel::NameEncoding::Utf8,
                name.as_bytes().to_vec(),
            )
            .map_err(|_| Error::Record("invalid directory name"))?;
            let (token, _hold) = super::super::host::directory::cursor::seal(
                &binding.volume,
                &binding.path,
                generation,
                &after,
                &ledger,
            )
            .map_err(turn)?;
            page.entries.truncate(kept);
            page.has_more = true;
            page.next = Some(token);
        }
        Ok(page)
    }
}

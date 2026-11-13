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
    pub async fn directory_page(
        &self,
        agent: &AgentPath,
        query: DirectoryQuery<'_>,
    ) -> Result<PrivateDirectoryPage, Error> {
        self.directory_page_select(agent, query, |page, _| Ok(page.entries.len()))
            .await
    }

    /// Apply row and byte credit before publishing a confidential continuation.
    pub async fn directory_page_select<F>(
        &self,
        agent: &AgentPath,
        query: DirectoryQuery<'_>,
        select: F,
    ) -> Result<PrivateDirectoryPage, Error>
    where
        F: FnOnce(&PrivateDirectoryPage, &dyn Fn(&str) -> Option<usize>) -> Result<usize, Error>
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
        let (mut page, _custody) = if let Some(token) = query
            .after_name
            .and_then(|token| token.strip_prefix("page-v1:"))
        {
            let plain = super::super::host::directory::cursor::open_payload(
                &binding.volume,
                &binding.path,
                token,
                &ledger,
            )
            .map_err(turn)?;
            let (mut page, hold) = ledger
                .decode_seed(
                    std::marker::PhantomData::<PrivateDirectoryPage>,
                    &plain.0,
                    &crate::executor::json::JsonValueSchema::Root,
                    0,
                    crate::wire::ErrorCode::Invalid,
                )
                .map_err(turn)?;
            if page.entries.len() > query.maximum_entries as usize {
                return Err(Error::Record(
                    "directory remainder exceeds requested page bound",
                ));
            }
            host.authorize_directory_remainder(
                super::super::host::DirectoryListing {
                    volume: &binding.volume,
                    grant: &binding.grant,
                    ceiling: Some(binding.scope.capabilities()),
                    path: &binding.path,
                    generation: query.generation,
                    after: None,
                    maximum_entries: query.maximum_entries,
                },
                &mut page,
                &ledger,
            )
            .await
            .map_err(turn)?;
            (page, hold)
        } else {
            let page = host
                .list_private_directory(&binding.volume, &binding.grant, &binding.path, options)
                .await
                .map_err(turn)?;
            let Some((generation, page)) = page else {
                return Ok(PrivateDirectoryPage {
                    generation: generation
                        .ok_or(Error::Record("directory cursor lost its selection"))?,
                    entries: Vec::new(),
                    has_more: false,
                    next: None,
                    examined: 0,
                });
            };
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
            (page, custody)
        };
        let generation = acyclic_fs::GenerationId::from_bytes(page.generation.as_resource().key());
        let generation_bytes = generation.as_ref().map_or(0, |id| id.to_bytes().len());
        let lazy_remainder_bound = if generation.is_none() {
            let encoded = ledger
                .encode(
                    &page,
                    ledger.remaining().map_err(turn)?.bytes_encoded,
                    false,
                )
                .map_err(turn)?;
            let hold = ledger
                .guard_owned(encoded.capacity() as u64)
                .map_err(turn)?;
            let length = super::super::host::directory::cursor::payload_encoded_len(encoded.len())
                .and_then(|length| length.checked_add(8));
            drop(encoded);
            drop(hold);
            length
        } else {
            None
        };
        let kept = select(&page, &|name| {
            if generation.is_none() {
                return lazy_remainder_bound;
            }
            super::super::host::directory::cursor::encoded_len(generation_bytes, name.len())
        })?;
        if kept > page.entries.len() || (kept == 0 && !page.entries.is_empty()) {
            return Err(Error::Record(
                "directory byte credit cannot admit its first entry",
            ));
        }
        if kept < page.entries.len() {
            if generation.is_none() {
                let (token, _hold) =
                    seal_remainder(&binding, &page, kept, &ledger).map_err(turn)?;
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

fn seal_remainder<'io>(
    binding: &DirectoryBinding,
    page: &PrivateDirectoryPage,
    kept: usize,
    io: &'io crate::filesystem::PreparationIo,
) -> crate::Result<(String, crate::filesystem::DecodeReservation<'io>)> {
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Remainder<'a> {
        generation: &'a GenerationRef,
        entries: &'a [crate::conversation::PrivateDirectoryEntry],
        has_more: bool,
        next: &'a Option<String>,
        examined: u32,
    }
    let remainder = Remainder {
        generation: &page.generation,
        entries: &page.entries[kept..],
        has_more: page.has_more,
        next: &page.next,
        examined: (page.entries.len() - kept) as u32,
    };
    let encoded = io.encode(&remainder, io.remaining()?.bytes_encoded, false)?;
    let capacity = encoded.capacity() as u64;
    let encoded = (encoded, io.guard_owned(capacity)?);
    let sealed = super::super::host::directory::cursor::seal_payload(
        &binding.volume,
        &binding.path,
        &encoded.0,
        io,
    )?;
    let token = crate::executor::with_history_io(Some(io), |work| {
        crate::executor::preparation_format(format_args!("page-v1:{}", sealed.0), work)
    })?;
    let hold = io.guard_owned(token.capacity() as u64)?;
    Ok((token, hold))
}

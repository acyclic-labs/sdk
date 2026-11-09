//! Private directory projection under the original selected agent authority.
use super::*;
use crate::{
    conversation::{ContentGrant, PrivateDirectoryPage},
    resources::GenerationRef,
};

impl<P, O> Bound<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Read one bounded range at the selected directory's immutable generation.
    /// The original private grant and current persisted policy authorize each
    /// range; the path never selects another agent or volume.
    pub async fn read_private_file(
        &self,
        agent: &AgentPath,
        path: &str,
        generation: &GenerationRef,
        offset: u64,
        maximum: u64,
    ) -> Result<(acyclic_stream::OwnedBytes, u64), Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let authority = agent_authority(
            &self.issuer,
            &self.admission,
            &state.path,
            &state.volumes,
            &[],
        )
        .await
        .map_err(turn)?;
        let grant = ContentGrant::verify_directory_read(
            &authority.verifier,
            &authority.scope,
            &authority.private.volume,
            path,
            &mut None,
        )
        .map_err(turn)?;
        let (bytes, _selected, length) = self
            .volumes
            .read_private_range(
                &authority.private.volume,
                &grant,
                path,
                Some(generation),
                acyclic_fs::ByteRange {
                    offset,
                    length: maximum,
                },
                ContentAccess::default(),
            )
            .await
            .map_err(turn)?;
        Ok((bytes, length))
    }

    /// Select visible names at one immutable generation under current policy.
    pub async fn directory_page(
        &self,
        agent: &AgentPath,
        path: &str,
        generation: Option<&GenerationRef>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<PrivateDirectoryPage, Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let authority = agent_authority(
            &self.issuer,
            &self.admission,
            &state.path,
            &state.volumes,
            &[],
        )
        .await
        .map_err(turn)?;
        let grant = ContentGrant::verify_directory_read(
            &authority.verifier,
            &authority.scope,
            &authority.private.volume,
            path,
            &mut None,
        )
        .map_err(turn)?;
        let after = after
            .map(|name| {
                acyclic_fs::kernel::LogicalName::new(
                    acyclic_fs::kernel::NameEncoding::Utf8,
                    name.as_bytes().to_vec(),
                    u32::MAX,
                )
            })
            .transpose()
            .map_err(|_| Error::Record("invalid directory continuation name"))?;
        let options = super::super::DirectoryOptions {
            generation,
            after: after.as_ref(),
            maximum_entries: limit,
            ceiling: None,
        };
        let (generation, page) = self
            .volumes
            .list_private_directory(&authority.private.volume, &grant, path, options)
            .await
            .map_err(turn)?
            .ok_or(Error::Record("selected directory does not exist"))?;
        PrivateDirectoryPage::from_filesystem(generation, page).map_err(turn)
    }
}

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
            authority.private.volume()?,
            path,
            &mut None,
        )
        .map_err(turn)?;
        let (bytes, _selected, length) = self
            .volumes
            .read_private_range(
                authority.private.volume()?,
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
        after: Option<&[u8]>,
        limit: u32,
        credit: Option<&mut crate::filesystem::DirectoryCredit<'_>>,
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
            authority.private.volume()?,
            path,
            &mut None,
        )
        .map_err(turn)?;
        let options = super::super::DirectoryOptions {
            generation,
            after,
            maximum_entries: limit,
            ceiling: None,
        };
        let page = self
            .volumes
            .list_private_directory_with_credit(
                authority.private.volume()?,
                &grant,
                path,
                options,
                credit,
            )
            .await
            .map_err(turn)?
            .ok_or(Error::Record("selected directory does not exist"))?;
        Ok(page)
    }
}

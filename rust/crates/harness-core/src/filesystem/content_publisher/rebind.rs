//! Preserve local verification only across a checked immutable-payload match.
use super::*;

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FilesystemContentPublisher<A, O>
{
    /// Rebinds local verification evidence to an equal immutable payload.
    ///
    /// Evidence proves integrity, never disclosure authority. Final admission
    /// still checks the current policy, caller ceiling and exact file binding.
    /// References without evidence remain subject to ordinary full verification.
    pub async fn rebind_verified_publication(
        &self,
        source: &FileRef,
        candidate: FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        self.verify_creation(io).await?;
        self.grant
            .require(&self.volume, VolumeOperation::Write, io)?;
        crate::executor::with_history_io(io, |work| {
            source.validate(work)?;
            candidate.validate(work)
        })?;
        if source.volume() != &self.volume
            || candidate.volume() != source.volume()
            || candidate.path() != source.path()
            || candidate.descriptor() != source.descriptor()
            || candidate.display_name() != source.display_name()
            || source.descriptor().byte_length() > self.maximum_bytes
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "publication rebind changes the verified file identity",
            ));
        }
        if !self.host.published_here(source, io)? {
            return Ok(candidate);
        }
        let temporary = PreparationIo::temporary(io)?;
        let owned;
        let io = if let Some(io) = temporary.as_ref().or(io) {
            io
        } else {
            owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &owned
        };
        let workspace_ref = super::super::bounded_io::content_workspace(
            &self.host.provider,
            &self.volume,
            Some(io),
        )?;
        let _workspace_hold = io.guard_owned(workspace_ref.logical_owned_bytes()?)?;
        let workspace = self.host.open(&workspace_ref, Some(io)).await?;
        let source_generation = self.host.file_generation(source, Some(io))?;
        let _source_hold = io.guard_owned(source_generation.logical_owned_bytes()?)?;
        let source_generation = self
            .host
            .generation(&workspace, &source_generation, Some(io))
            .await?;
        let candidate_generation = self.host.file_generation(&candidate, Some(io))?;
        let _candidate_hold = io.guard_owned(candidate_generation.logical_owned_bytes()?)?;
        let candidate_generation = self
            .host
            .generation(&workspace, &candidate_generation, Some(io))
            .await?;
        let path = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{}", source.path()), work)
        })?;
        let _path_hold = io.guard_owned(path.capacity() as u64)?;
        // A zero-length range still checks kind and the complete immutable
        // payload identity. It cannot load or hash any content body.
        drop(
            io.sdk(
                candidate_generation
                    .read_range_if_unchanged(
                        &source_generation,
                        &path,
                        acyclic_fs::ByteRange {
                            offset: 0,
                            length: 0,
                        },
                        io,
                        io.cancellation(),
                    )
                    .await,
            )?,
        );
        super::super::bounded_io::retain(&candidate_generation, Some(io)).await?;
        self.grant.recheck(Some(io))?;
        self.host.seal_publication(candidate, Some(io))
    }
}

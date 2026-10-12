use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    /// Creates one directory with canonical metadata.
    pub async fn create_directory<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::CreateDirectory {
                    path,
                    metadata: FileMetadata::default(),
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Creates one regular file with exact bytes and metadata.
    pub async fn create_file<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
        metadata: FileMetadata,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::CreateFile {
                    path,
                    bytes: bytes.into(),
                    metadata,
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Replaces one regular-file range.
    pub async fn write_range<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        offset: u64,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::Write {
                    path,
                    offset,
                    bytes: bytes.into(),
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Creates one symbolic link with an opaque target.
    pub async fn create_symbolic_link<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        target: impl Into<acyclic_stream::OwnedBytes>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::CreateSymbolicLink {
                    path,
                    target: target.into(),
                    metadata: FileMetadata::default(),
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Replaces canonical metadata.
    pub async fn set_metadata<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        metadata: FileMetadata,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| Ok(AuthoredMutation::SetMetadata { path, metadata }),
            work,
            cancellation,
        )
        .await
    }

    /// Changes one regular file logical length.
    pub async fn resize<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        logical_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::Resize {
                    path,
                    logical_bytes,
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Punches a hole or installs allocated zeros.
    pub async fn zero_range<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        range: crate::ByteRange,
        allocated: bool,
        extend: bool,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::ZeroRange {
                    path,
                    range,
                    allocated,
                    extend,
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Allocates one sparse range.
    pub async fn preallocate<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        range: crate::ByteRange,
        keep_size: bool,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::Preallocate {
                    path,
                    range,
                    keep_size,
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Creates one FIFO, socket, or device node.
    pub async fn create_special<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        kind: FileKind,
        device: Option<(u32, u32)>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| match (kind, device) {
                (FileKind::Fifo | FileKind::Socket, None) => {
                    Ok(AuthoredMutation::CreateEmptySpecial {
                        path,
                        kind,
                        metadata: FileMetadata::default(),
                    })
                }
                (FileKind::CharacterDevice | FileKind::BlockDevice, Some((major, minor))) => {
                    Ok(AuthoredMutation::CreateDevice {
                        path,
                        kind,
                        major,
                        minor,
                        metadata: FileMetadata::default(),
                    })
                }
                _ => Err(WorkspaceError::IncompatibleWorkspace),
            },
            work,
            cancellation,
        )
        .await
    }
}

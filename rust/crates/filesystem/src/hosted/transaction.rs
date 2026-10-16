/// Sparse hosted transaction. Mutations remain private until one atomic commit.
pub struct HostedTransaction {
    workspace: HostedWorkspace,
    base: wire::GenerationRef,
    idempotency_key: IdempotencyKey,
    mutations: Vec<wire::Mutation>,
}

impl HostedTransaction {
    /// Adds one canonical protocol mutation. Generated mutation variants are
    /// intentionally reused so Rust and TypeScript cannot drift from the wire.
    pub fn push(&mut self, mutation: wire::mutation::Mutation) {
        self.mutations.push(wire::Mutation {
            mutation: Some(mutation),
        });
    }

    /// Creates parent directories as one transaction mutation.
    pub fn create_directories(&mut self, path: impl Into<String>) {
        self.push(wire::mutation::Mutation::CreateDirectories(
            wire::CreateDirectories { path: path.into() },
        ));
    }

    /// Creates one regular file with exact metadata.
    pub fn create_file(
        &mut self,
        path: impl Into<String>,
        contents: impl Into<Vec<u8>>,
        metadata: Option<wire::Metadata>,
    ) {
        self.push(wire::mutation::Mutation::CreateFile(wire::CreateFile {
            path: path.into(),
            contents: contents.into(),
            metadata,
        }));
    }

    /// Creates one directory with exact metadata.
    pub fn create_directory(&mut self, path: impl Into<String>, metadata: Option<wire::Metadata>) {
        self.push(wire::mutation::Mutation::CreateDirectory(
            wire::CreateDirectory {
                path: path.into(),
                metadata,
            },
        ));
    }

    /// Creates one symbolic link without interpreting its target bytes.
    pub fn create_symbolic_link(
        &mut self,
        path: impl Into<String>,
        target: impl Into<Vec<u8>>,
        metadata: Option<wire::Metadata>,
    ) {
        self.push(wire::mutation::Mutation::CreateSymbolicLink(
            wire::CreateSymbolicLink {
                path: path.into(),
                target: target.into(),
                metadata,
            },
        ));
    }

    /// Creates or replaces one complete regular file.
    pub fn put_file(&mut self, path: impl Into<String>, contents: impl Into<Vec<u8>>) {
        self.push(wire::mutation::Mutation::PutFile(wire::PutFile {
            path: path.into(),
            contents: contents.into(),
        }));
    }

    /// Removes one path.
    pub fn remove(&mut self, path: impl Into<String>) {
        self.push(wire::mutation::Mutation::Remove(wire::Remove {
            path: path.into(),
        }));
    }

    /// Atomically renames one path.
    pub fn rename(
        &mut self,
        source: impl Into<String>,
        destination: impl Into<String>,
        replace: bool,
    ) {
        self.push(wire::mutation::Mutation::Rename(wire::Rename {
            source: source.into(),
            destination: destination.into(),
            replace,
        }));
    }

    /// Creates a hard link without copying file contents.
    pub fn hard_link(&mut self, source: impl Into<String>, destination: impl Into<String>) {
        self.push(wire::mutation::Mutation::HardLink(wire::HardLink {
            source: source.into(),
            destination: destination.into(),
        }));
    }

    /// Copies one complete file through shared immutable extent references.
    pub fn copy_file(&mut self, source: impl Into<String>, destination: impl Into<String>) {
        self.push(wire::mutation::Mutation::CopyFile(wire::CopyFile {
            source: source.into(),
            destination: destination.into(),
        }));
    }

    /// Writes one sparse range.
    pub fn write(&mut self, path: impl Into<String>, offset: u64, contents: impl Into<Vec<u8>>) {
        self.push(wire::mutation::Mutation::Write(wire::Write {
            path: path.into(),
            offset,
            contents: contents.into(),
        }));
    }

    /// Changes logical file length without materializing holes.
    pub fn resize(&mut self, path: impl Into<String>, logical_bytes: u64) {
        self.push(wire::mutation::Mutation::Resize(wire::Resize {
            path: path.into(),
            logical_bytes,
        }));
    }

    /// Converts one range to a hole or allocated zeros.
    pub fn zero_range(
        &mut self,
        path: impl Into<String>,
        offset: u64,
        length: u64,
        allocated: bool,
        extend: bool,
    ) {
        self.push(wire::mutation::Mutation::ZeroRange(wire::ZeroRange {
            path: path.into(),
            range: Some(wire::ByteRange { offset, length }),
            allocated,
            extend,
        }));
    }

    /// Preallocates one sparse file range.
    pub fn preallocate(
        &mut self,
        path: impl Into<String>,
        offset: u64,
        length: u64,
        keep_size: bool,
    ) {
        self.push(wire::mutation::Mutation::Preallocate(wire::Preallocate {
            path: path.into(),
            range: Some(wire::ByteRange { offset, length }),
            keep_size,
        }));
    }

    /// Clones one content range without copying unchanged object bodies.
    pub fn clone_range(
        &mut self,
        source: impl Into<String>,
        source_offset: u64,
        destination: impl Into<String>,
        destination_offset: u64,
        length: u64,
    ) {
        self.push(wire::mutation::Mutation::CloneRange(wire::CloneRange {
            source: source.into(),
            source_offset,
            destination: destination.into(),
            destination_offset,
            length,
        }));
    }

    /// Replaces explicitly represented metadata fields.
    pub fn set_metadata(&mut self, path: impl Into<String>, metadata: wire::Metadata) {
        self.push(wire::mutation::Mutation::SetMetadata(wire::SetMetadata {
            path: path.into(),
            metadata: Some(metadata),
        }));
    }

    /// Publishes every accumulated mutation atomically.
    pub async fn commit(
        self,
        maximum_conflicts: u32,
    ) -> Result<wire::MutationResponse, HostedFsError> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.hosted.commit",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty
        );
        crate::obs::outcome_on(
            &span,
            crate::obs::in_span(&span, async move {
                self.workspace
                    .filesystem
                    .require_transaction_bound(self.mutations.len())?;
                self.workspace
                    .filesystem
                    .require_page_bound(maximum_conflicts, "transaction conflicts")?;
                let mut client = self.workspace.filesystem.client.clone();
                Ok(client
                    .apply_transaction(
                        self.workspace
                            .filesystem
                            .request(wire::ApplyTransactionRequest {
                                base: Some(self.base),
                                mutations: self.mutations,
                                operation: Some(operation(self.idempotency_key)),
                                maximum_conflicts,
                            })
                            .await?,
                    )
                    .await?
                    .into_inner())
            })
            .await,
        )
    }

    /// Validates replay against the current head and returns exact conflicts
    /// without publishing the transaction.
    pub async fn rebase(
        &self,
        maximum_conflicts: u32,
    ) -> Result<wire::RebaseTransactionResponse, HostedFsError> {
        self.workspace
            .filesystem
            .require_transaction_bound(self.mutations.len())?;
        self.workspace
            .filesystem
            .require_page_bound(maximum_conflicts, "transaction conflicts")?;
        let mut client = self.workspace.filesystem.client.clone();
        Ok(client
            .rebase_transaction(
                self.workspace
                    .filesystem
                    .request(wire::RebaseTransactionRequest {
                        base: Some(self.base.clone()),
                        mutations: self.mutations.clone(),
                        maximum_conflicts,
                        operation: Some(operation(self.idempotency_key)),
                    })
                    .await?,
            )
            .await?
            .into_inner())
    }
}

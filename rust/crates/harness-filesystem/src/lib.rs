#![deny(unsafe_code)]
//! Explicit integration between harness references and versioned Filesystem workspaces.

use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, Digest, Fs, Generation, GenerationId,
    IdempotencyKey as FilesystemKey, TransactionCommit, Workspace, WorkspaceDirectoryPage,
    WorkspaceError, WorkspaceStat,
};
use acyclic_harness::{
    Error, IdempotencyKey, Result,
    resources::{GenerationRef, ProviderRef, WorkspaceRef},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Resolved mutable workspace head and immutable generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceObservation {
    /// Provider-owned mutable workspace identity.
    pub workspace: WorkspaceRef,
    /// Exact immutable head observed by this operation.
    pub generation: GenerationRef,
}

/// One code-defined member of an atomic workspace mutation batch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceMutation {
    /// Creates every missing directory on an absolute path.
    CreateDirectory {
        /// Canonical absolute path.
        path: String,
    },
    /// Creates or replaces one complete file.
    PutFile {
        /// Canonical absolute path.
        path: String,
        /// Complete file content.
        bytes: Vec<u8>,
    },
    /// Removes one file or empty directory.
    Remove {
        /// Canonical absolute path.
        path: String,
    },
}

/// Adapter over any embedded, local, or distributed Filesystem provider pair.
#[derive(Clone)]
pub struct FilesystemHost<A, O> {
    filesystem: Fs<A, O>,
    provider: ProviderRef,
}

impl<A, O> FilesystemHost<A, O> {
    /// Binds one Filesystem deployment and immutable provider identity.
    pub fn new(filesystem: Fs<A, O>, provider: ProviderRef) -> Result<Self> {
        provider.validate()?;
        if provider.family() != "filesystem" {
            return Err(Error::Invalid(
                "Filesystem adapter requires the filesystem provider family".into(),
            ));
        }
        Ok(Self {
            filesystem,
            provider,
        })
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Resolves the current immutable head of a named workspace.
    pub async fn resolve(&self, reference: &WorkspaceRef) -> Result<WorkspaceObservation> {
        let workspace = self.open(reference).await?;
        let head = workspace.head().await.map_err(map_error)?;
        Ok(WorkspaceObservation {
            workspace: reference.clone(),
            generation: self.generation_ref(&head)?,
        })
    }

    /// Reads one bounded file from the current head or an exact generation.
    pub async fn read(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
        maximum_bytes: u64,
    ) -> Result<Bytes> {
        let workspace = self.open(workspace).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference)
                .await?
                .read(path, maximum_bytes)
                .await
                .map_err(map_error),
            None => workspace.read(path, maximum_bytes).await.map_err(map_error),
        }
    }

    /// Stats one path at the current head or an exact generation.
    pub async fn stat(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
    ) -> Result<WorkspaceStat> {
        let workspace = self.open(workspace).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference)
                .await?
                .stat(path)
                .await
                .map_err(map_error),
            None => workspace.stat(path).await.map_err(map_error),
        }
    }

    /// Lists one bounded directory page at the current head or an exact generation.
    pub async fn list(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
        maximum_entries: u32,
    ) -> Result<WorkspaceDirectoryPage> {
        let workspace = self.open(workspace).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference)
                .await?
                .list_directory(path, None, maximum_entries)
                .await
                .map_err(map_error),
            None => workspace
                .list_directory(path, None, maximum_entries)
                .await
                .map_err(map_error),
        }
    }

    /// Applies one atomic, idempotent mutation batch against an optional exact generation.
    pub async fn apply(
        &self,
        workspace: &WorkspaceRef,
        expected_generation: Option<&GenerationRef>,
        mutations: &[WorkspaceMutation],
        idempotency_key: &IdempotencyKey,
    ) -> Result<GenerationRef> {
        let workspace = self.open(workspace).await?;
        let key = filesystem_key(idempotency_key);
        let mut transaction = match expected_generation {
            Some(reference) => {
                let generation = self.generation(&workspace, reference).await?;
                workspace
                    .begin_transaction_at(&generation, key)
                    .await
                    .map_err(map_error)?
            }
            None => workspace.begin_transaction(key).await.map_err(map_error)?,
        };
        for mutation in mutations {
            match mutation {
                WorkspaceMutation::CreateDirectory { path } => {
                    transaction.create_dir_all(path).await.map_err(map_error)?;
                }
                WorkspaceMutation::PutFile { path, bytes } => {
                    transaction
                        .write(path, Bytes::copy_from_slice(bytes))
                        .await
                        .map_err(map_error)?;
                }
                WorkspaceMutation::Remove { path } => {
                    transaction.remove(path).await.map_err(map_error)?;
                }
            }
        }
        match transaction.commit().await.map_err(map_error)? {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => self.generation_ref(&generation),
            TransactionCommit::Conflict { .. } => {
                Err(Error::Conflict("workspace generation changed".into()))
            }
            TransactionCommit::Fenced => Err(Error::Conflict("workspace writer was fenced".into())),
            TransactionCommit::IdempotencyConflict => Err(Error::Conflict(
                "filesystem idempotency key belongs to another mutation".into(),
            )),
        }
    }

    async fn open(&self, reference: &WorkspaceRef) -> Result<Workspace<A, O>> {
        reference.validate()?;
        self.validate_provider(reference.as_resource().provider())?;
        let name = std::str::from_utf8(reference.as_resource().key())
            .map_err(|_| Error::Invalid("workspace reference key must be UTF-8".into()))?;
        self.filesystem
            .open_workspace(name)
            .await
            .map_err(map_error)
    }

    async fn generation(
        &self,
        workspace: &Workspace<A, O>,
        reference: &GenerationRef,
    ) -> Result<Generation<A, O>> {
        reference.validate()?;
        self.validate_provider(reference.as_resource().provider())?;
        let bytes: [u8; 32] = reference
            .as_resource()
            .key()
            .try_into()
            .map_err(|_| Error::Invalid("generation reference key must be 32 bytes".into()))?;
        workspace
            .generation(GenerationId::new(Digest::from_bytes(bytes)))
            .await
            .map_err(map_error)
    }

    fn generation_ref(&self, generation: &Generation<A, O>) -> Result<GenerationRef> {
        GenerationRef::new(
            self.provider.clone(),
            generation.id().digest().into_bytes(),
            None,
        )
    }

    fn validate_provider(&self, provider: &ProviderRef) -> Result<()> {
        provider.validate()?;
        if provider != &self.provider {
            return Err(Error::Invalid(
                "resource belongs to another Filesystem provider".into(),
            ));
        }
        Ok(())
    }
}

/// Creates the canonical provider-owned reference for a named workspace.
pub fn workspace_ref(provider: ProviderRef, name: &str) -> Result<WorkspaceRef> {
    acyclic_fs::WorkspaceName::new(name).map_err(|error| Error::Invalid(error.to_string()))?;
    WorkspaceRef::new(provider, name.as_bytes(), None)
}

fn filesystem_key(key: &IdempotencyKey) -> FilesystemKey {
    let digest = blake3::hash(key.as_str().as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    if bytes == [0; 16] {
        bytes[0] = 1;
    }
    FilesystemKey::from_bytes(bytes)
}

fn map_error(error: WorkspaceError) -> Error {
    match error {
        WorkspaceError::NotFound => Error::NotFound("workspace path".into()),
        WorkspaceError::RetentionConflict => Error::Conflict(error.to_string()),
        WorkspaceError::Name(_) | WorkspaceError::Path(_) => Error::Invalid(error.to_string()),
        WorkspaceError::ReadLimitExceeded
        | WorkspaceError::NotRegularFile
        | WorkspaceError::NotDirectory
        | WorkspaceError::ForeignGeneration
        | WorkspaceError::IncompatibleWorkspace
        | WorkspaceError::NoCommonAncestor
        | WorkspaceError::LineageLimit
        | WorkspaceError::JoinLimit
        | WorkspaceError::ChangeSetContinuity
        | WorkspaceError::EmptyContentSet
        | WorkspaceError::NotFork
        | WorkspaceError::ContentLengthOverflow => Error::Invalid(error.to_string()),
        WorkspaceError::Engine(value) => Error::Storage(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolves_exact_memory_generation_and_rejects_foreign_provider() -> Result<()> {
        let filesystem = Fs::memory();
        let workspace = filesystem
            .create_workspace("adapter-test")
            .await
            .map_err(map_error)?;
        let expected = workspace.head().await.map_err(map_error)?;
        let provider = ProviderRef::new("example", "filesystem", "1")?;
        let host = FilesystemHost::new(filesystem, provider.clone())?;
        let reference = workspace_ref(provider, "adapter-test")?;
        let observed = host.resolve(&reference).await?;
        assert_eq!(
            observed.generation.as_resource().key(),
            expected.id().digest().into_bytes()
        );

        let foreign = workspace_ref(
            ProviderRef::new("other", "filesystem", "1")?,
            "adapter-test",
        )?;
        assert!(matches!(
            host.resolve(&foreign).await,
            Err(Error::Invalid(_))
        ));

        let updated = host
            .apply(
                &reference,
                Some(&observed.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/hello.txt".into(),
                    bytes: b"hello".to_vec(),
                }],
                &IdempotencyKey::new("write-hello")?,
            )
            .await?;
        assert_eq!(
            host.read(&reference, Some(&updated), "/hello.txt", 16)
                .await?,
            Bytes::from_static(b"hello")
        );
        assert!(matches!(
            host.apply(
                &reference,
                Some(&observed.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/hello.txt".into(),
                    bytes: b"changed".to_vec(),
                }],
                &IdempotencyKey::new("stale-write")?,
            )
            .await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }
}

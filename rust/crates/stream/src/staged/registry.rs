//! Immutable stage bindings; installation belongs to the authority root owner.
use super::*;
use crate::content::index::Index;

const MAGIC: &[u8] = b"acyclic-stream-stage-binding-v1\0";
const IDENTITY: &[u8] = b"acyclic-stream-stage-identity-v1\0";
const MAXIMUM: usize =
    MAGIC.len() + 64 + 8 + crate::MAX_IDEMPOTENCY_KEY_BYTES + 1 + 1 + 1 + 8 + 9 + 82;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Sealed {
    staged: StagedWrite,
    directory: Root,
}

/// Immutable identity data, never an authorization or final-apply capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Binding {
    commit: CommitId,
    owner: [u8; 32],
    key: IdempotencyKey,
    sealed: Option<Sealed>,
}

impl Binding {
    /// Content-bound stage id, independent of its open/sealed state.
    pub fn id(&self) -> [u8; 32] {
        let mut bytes = Vec::with_capacity(IDENTITY.len() + 72 + self.key.as_bytes().len());
        bytes.extend_from_slice(IDENTITY);
        bytes.extend_from_slice(self.commit.as_bytes());
        bytes.extend_from_slice(&self.owner);
        bytes.extend_from_slice(&(self.key.as_bytes().len() as u64).to_le_bytes());
        bytes.extend_from_slice(self.key.as_bytes());
        crate::content_digest(&bytes)
    }
    fn same_identity(&self, other: &Self) -> bool {
        self.commit == other.commit && self.owner == other.owner && self.key == other.key
    }
    /// The commit this stage may submit; it has no assigned Stream position.
    pub const fn commit_id(&self) -> CommitId {
        self.commit
    }
    /// Original owner-run identity; the enclosing core verifies its capability.
    pub const fn owner(&self) -> &[u8; 32] {
        &self.owner
    }
    /// Sealed immutable request and directory, if preparation completed.
    pub fn sealed(&self) -> Option<(&StagedWrite, Root)> {
        self.sealed
            .as_ref()
            .map(|sealed| (&sealed.staged, sealed.directory))
    }
    async fn write(&self, storage: &impl Storage) -> Result<Root, StreamError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(MAXIMUM)
            .map_err(|_| StreamError::Capacity)?;
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(self.commit.as_bytes());
        bytes.extend_from_slice(&self.owner);
        bytes.extend_from_slice(&(self.key.as_bytes().len() as u64).to_le_bytes());
        bytes.extend_from_slice(self.key.as_bytes());
        bytes.push(u8::from(self.sealed.is_some()));
        let mut writer = tree::Writer::new(storage)?;
        if let Some(sealed) = &self.sealed {
            bytes.push(sealed.staged.family.byte());
            bytes.push(u8::from(sealed.staged.accepted));
            bytes.extend_from_slice(&sealed.staged.effects.to_le_bytes());
            bytes.push(u8::from(sealed.staged.deadline.is_some()));
            if let Some(deadline) = sealed.staged.deadline {
                bytes.extend_from_slice(&deadline.to_le_bytes());
            }
            super::directory::encode_root(&mut bytes, sealed.staged.body);
            super::directory::encode_root(&mut bytes, sealed.directory);
            // The validated directory retains this exact token body already.
            writer.reference(sealed.directory).await?;
        }
        writer.write(&bytes).await?;
        writer.finish().await
    }
    async fn read(storage: &impl Storage, root: Root) -> Result<Self, StreamError> {
        if root.part.length > MAXIMUM as u64 {
            return Err(StreamError::InvalidResponse);
        }
        let mut cursor = tree::Cursor::new(storage, root)?;
        let magic = cursor.exact::<{ MAGIC.len() }>().await?;
        crate::persistence::header(&magic, b"acyclic-stream-stage-binding-v", b"1\0")?;
        let commit = CommitId::from_bytes(cursor.exact().await?);
        let owner = cursor.exact().await?;
        let length =
            usize::try_from(cursor.number().await?).map_err(|_| StreamError::InvalidResponse)?;
        if length > crate::MAX_IDEMPOTENCY_KEY_BYTES {
            return Err(StreamError::InvalidResponse);
        }
        let key = cursor.small(length).await?;
        let key = IdempotencyKey::new(key.as_slice().to_vec())
            .map_err(|_| StreamError::InvalidResponse)?;
        let sealed = if cursor.boolean().await? {
            let family = Family::decode(cursor.byte().await?)?;
            let accepted = cursor.boolean().await?;
            let effects = cursor.number().await?;
            let deadline = cursor.optional().await?;
            let body = super::directory::decode_root(&cursor.exact::<41>().await?)?;
            let directory = super::directory::decode_root(&cursor.exact::<41>().await?)?;
            let staged = StagedWrite {
                commit_id: commit,
                key: key.clone(),
                family,
                body,
                accepted,
                effects,
                deadline,
            };
            Directory::open(storage, &staged, directory).await?;
            Some(Sealed { staged, directory })
        } else {
            None
        };
        cursor.end().await?;
        Ok(Self {
            commit,
            owner,
            key,
            sealed,
        })
    }
}

/// Paged stage registry. Every update is private until its owner installs it.
/// This contains no namespace state, receipt or sequence/revision allocator.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Registry {
    index: Index,
}

fn key(id: [u8; 32]) -> Field {
    Field::Inline(OwnedBytes::copy_from_slice(&id))
}

impl Registry {
    /// Opens a retained versioned registry checkpoint without scanning stages.
    pub async fn open(storage: &impl Storage, root: Root) -> Result<Self, StreamError> {
        Ok(Self {
            index: Index::open(storage, root).await?,
        })
    }
    /// Retains this registry's entire graph in an unpublished checkpoint.
    pub async fn checkpoint(&self, storage: &impl Storage) -> Result<Root, StreamError> {
        self.index.checkpoint(storage).await
    }
    /// Reads one content-bound binding; the enclosing namespace supplies auth.
    pub async fn get(
        &self,
        storage: &impl Storage,
        id: [u8; 32],
    ) -> Result<Option<Binding>, StreamError> {
        let Some(value) = self.index.get(storage, key(id)).await? else {
            return Ok(None);
        };
        let Field::Content(root) = value else {
            return Err(StreamError::InvalidResponse);
        };
        let binding = Binding::read(storage, root).await?;
        if binding.id() != id {
            return Err(StreamError::InvalidResponse);
        }
        Ok(Some(binding))
    }
    /// Prepares one permanent commit/key/owner binding, assigning no position.
    pub async fn begin(
        &self,
        storage: &impl Storage,
        commit: CommitId,
        retry: IdempotencyKey,
        owner: [u8; 32],
    ) -> Result<(Self, Binding), StreamError> {
        let binding = Binding {
            commit,
            owner,
            key: retry,
            sealed: None,
        };
        let id = binding.id();
        if let Some(previous) = self.get(storage, id).await? {
            if !previous.same_identity(&binding) {
                return Err(StreamError::InvalidResponse);
            }
            return Ok((self.clone(), previous));
        }
        let root = binding.write(storage).await?;
        let index = self
            .index
            .set(storage, key(id), Field::Content(root))
            .await?;
        Ok((Self { index }, binding))
    }
    /// Prepares an immutable seal. A stage can never change commit or payload.
    pub async fn seal(
        &self,
        storage: &impl Storage,
        binding: &Binding,
        staged: &StagedWrite,
        directory: Root,
    ) -> Result<Self, StreamError> {
        let id = binding.id();
        let mut current = self
            .get(storage, id)
            .await?
            .ok_or(StreamError::InvalidArgument)?;
        if !current.same_identity(binding) {
            return Err(StreamError::InvalidArgument);
        }
        if current.commit != staged.commit_id || current.key != staged.key {
            return Err(StreamError::InvalidArgument);
        }
        let sealed = Sealed {
            staged: staged.clone(),
            directory,
        };
        if let Some(previous) = &current.sealed {
            return if previous.directory == sealed.directory && previous.staged.same_input(staged) {
                Ok(self.clone())
            } else {
                Err(StreamError::InvalidArgument)
            };
        }
        Directory::open(storage, staged, directory).await?;
        current.sealed = Some(sealed);
        let root = current.write(storage).await?;
        Ok(Self {
            index: self
                .index
                .set(storage, key(id), Field::Content(root))
                .await?,
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

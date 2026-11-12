//! The one immutable control-root descriptor, never a publication capability.
use super::*;
use crate::content::index::Index;

const MAGIC: &[u8] = b"acyclic-stream-control-root-v1\0";
const BASE_BYTES: usize = MAGIC.len() + 3 * directory::ROOT_BYTES + 1;

/// References installed together by the shared publication owner.
/// Preparing this descriptor makes no component visible to Stream queries.
/// The owner supplies account authorization, page custody and durable install.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlRoot {
    /// Immutable namespace-index checkpoint.
    pub namespace: Root,
    /// Immutable stage-registry checkpoint.
    pub stages: Root,
    /// Immutable terminal-decision-index checkpoint.
    pub decisions: Root,
    /// One pinned in-progress authority decision; absent between decisions.
    /// Its decision codec must validate identity/cut before recovery uses it.
    pub pending: Option<Root>,
}

impl ControlRoot {
    /// Prepares one complete descriptor retaining all component graphs.
    /// Callers validate component checkpoints before constructing successors;
    /// this writes no mutable authority and performs no condition settlement.
    pub async fn write(&self, storage: &impl Storage) -> Result<Root, StreamError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(BASE_BYTES + directory::ROOT_BYTES)
            .map_err(|_| StreamError::Capacity)?;
        bytes.extend_from_slice(MAGIC);
        let mut writer = tree::Writer::new(storage)?;
        for root in [self.namespace, self.stages, self.decisions] {
            directory::encode_root(&mut bytes, root);
            writer.reference(root).await?;
        }
        bytes.push(u8::from(self.pending.is_some()));
        if let Some(root) = self.pending {
            directory::encode_root(&mut bytes, root);
            writer.reference(root).await?;
        }
        writer.write(&bytes).await?;
        writer.finish().await
    }

    /// Opens one complete root descriptor and bounded component checkpoints.
    /// A torn frame yields no control root, never a partially decoded tuple.
    pub async fn open(storage: &impl Storage, root: Root) -> Result<Self, StreamError> {
        if root.part.length != BASE_BYTES as u64
            && root.part.length != (BASE_BYTES + directory::ROOT_BYTES) as u64
        {
            return Err(StreamError::InvalidResponse);
        }
        let mut cursor = tree::Cursor::new(storage, root)?;
        let magic = cursor.exact::<{ MAGIC.len() }>().await?;
        crate::persistence::header(&magic, b"acyclic-stream-control-root-v", b"1\0")?;
        let namespace = directory::decode_root(&cursor.exact::<41>().await?)?;
        let stages = directory::decode_root(&cursor.exact::<41>().await?)?;
        let decisions = directory::decode_root(&cursor.exact::<41>().await?)?;
        let pending = if cursor.boolean().await? {
            Some(directory::decode_root(&cursor.exact::<41>().await?)?)
        } else {
            None
        };
        cursor.end().await?;
        Index::open(storage, namespace).await?;
        Registry::open(storage, stages).await?;
        Index::open(storage, decisions).await?;
        Ok(Self {
            namespace,
            stages,
            decisions,
            pending,
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

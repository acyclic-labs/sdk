use super::*;

pub(super) const MAGIC: &[u8] = b"acyclic-ordered-index-root-v1\0";
const MAXIMUM_BYTES: usize = MAGIC.len() + 1 + 41 + 1 + 8;

impl Index {
    /// Stores a versioned descriptor retaining this index's complete page graph.
    /// The returned reference remains private until its enclosing root publishes.
    pub async fn checkpoint(&self, storage: &impl Storage) -> Result<Root, StreamError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(MAXIMUM_BYTES)
            .map_err(|_| StreamError::Capacity)?;
        bytes.extend_from_slice(MAGIC);
        let mut writer = tree::Writer::new(storage)?;
        if let Some(root) = self.root {
            writer.reference(root).await?;
            bytes.push(1);
            codec::root_bytes(&mut bytes, root);
            bytes.push(self.height);
            bytes.extend_from_slice(&self.count.to_le_bytes());
        } else {
            bytes.push(0);
        }
        writer.write(&bytes).await?;
        writer.finish().await
    }

    /// Opens an authenticated checkpoint without scanning its descendants.
    /// The enclosing owner supplies authorization and retains referenced pages;
    /// this decoder verifies shape and integrity, not authority to publish.
    pub async fn open(storage: &impl Storage, checkpoint: Root) -> Result<Self, StreamError> {
        let bytes = codec::body(storage, checkpoint, MAGIC.len() + 1, MAXIMUM_BYTES).await?;
        let mut decode = codec::Decode {
            bytes: crate::persistence::header(&bytes, b"acyclic-ordered-index-root-v", b"1\0")?,
        };
        let index = match decode.array::<1>()?[0] {
            0 => Self::default(),
            1 => {
                let root = decode.root()?;
                let height = decode.array::<1>()?[0];
                let count = u64::from_le_bytes(decode.array()?);
                if height > MAX_HEIGHT || count == 0 {
                    return Err(StreamError::InvalidResponse);
                }
                Self {
                    root: Some(root),
                    height,
                    count,
                }
            }
            _ => return Err(StreamError::InvalidResponse),
        };
        if !decode.bytes.is_empty() {
            return Err(StreamError::InvalidResponse);
        }
        if let Some(root) = index.root {
            let valid = match codec::read(storage, root, index.height).await? {
                Page::Leaf(entries) => index.count == entries.len() as u64,
                Page::Internal(children) => index.count >= children.len() as u64,
            };
            if !valid {
                return Err(StreamError::InvalidResponse);
            }
        }
        Ok(index)
    }
}

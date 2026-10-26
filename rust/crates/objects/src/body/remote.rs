//! Immutable body transport; logical range selection stays in the shared core.
use super::*;

pub(crate) trait ImmutableBodies: Send + Sync {
    fn put(
        &self,
        id: [u8; 32],
        bytes: acyclic_stream::OwnedBytes,
    ) -> BoxFuture<'_, Result<(), Error>>;
    fn get(
        &self,
        id: [u8; 32],
        length: usize,
        start: usize,
        end: usize,
    ) -> BoxFuture<'_, Result<acyclic_stream::OwnedBytes, Error>>;
}

#[derive(Clone)]
pub(crate) struct RemoteBody {
    pub(crate) source: Arc<dyn ImmutableBodies>,
    pub(crate) id: [u8; 32],
    pub(crate) length: usize,
}

impl PartialEq for RemoteBody {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.source, &other.source)
            && self.id == other.id
            && self.length == other.length
    }
}

impl RemoteBody {
    pub(super) async fn read(
        &self,
        start: usize,
        end: usize,
        mode: BodyReadMode,
    ) -> Result<acyclic_stream::OwnedBytes, BodyReadError> {
        if let BodyReadMode::Native(_) = mode {
            return Err(Error::from(crate::v1::wire::ErrorCode::Unsupported).into());
        }
        if start == end {
            return Ok(acyclic_stream::OwnedBytes::new());
        }
        let bytes = self.source.get(self.id, self.length, start, end).await?;
        if bytes.len() != end - start
            || start == 0 && end == self.length && blake3::hash(&bytes).as_bytes() != &self.id
        {
            return Err(Error::from(Unavailable).into());
        }
        Ok(bytes)
    }
}

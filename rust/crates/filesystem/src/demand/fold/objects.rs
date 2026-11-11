//! Immutable fold cache payloads retain the original read and decode custody.
use super::*;
use crate::{
    AsyncObjectStore, AuthorityWork, HashedObject, ObjectId, ObjectKind, ObjectStoreError,
    kernel::{MetadataDecodeError, NativeAccounting, OwnedSourceChunk},
};
use serde::de::DeserializeOwned;

/// A cache failure never substitutes a different stream cut or authored state.
#[derive(Debug, thiserror::Error)]
pub enum FoldObjectError {
    /// A stream lookup, finite read or conditional publication failed.
    #[error(transparent)]
    Stream(#[from] acyclic_stream::StreamError),
    /// The fold refused an input record.
    #[error(transparent)]
    Fold(#[from] DemandError),
    /// The cache descriptor and decoded checkpoint do not name the same prefix.
    #[error("fold checkpoint does not match its cache descriptor")]
    Mismatch,
    /// Original accounting, cancellation or allocation failed.
    #[error(transparent)]
    Accounting(#[from] crate::FsError),
    /// The immutable object provider failed.
    #[error(transparent)]
    Object(#[from] ObjectStoreError),
    /// The checkpoint's versioned payload could not be decoded on the original ledger.
    #[error(transparent)]
    Decode(#[from] MetadataDecodeError),
    /// Checkpoint serialization failed.
    #[error(transparent)]
    Encode(#[from] serde_json::Error),
}

/// Exact Objects identity and length for a disposable checkpoint. `key` is the
/// fold index key; `object` is the separately computed content digest. A cache
/// catalog persists this descriptor, never the decoded state or a process handle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FoldCheckpointObject {
    /// Stable (view, sequence, generation) index key.
    pub key: [u8; 32],
    /// Authenticated immutable object identity.
    pub object: ObjectId,
    /// Exact canonical encoded length, not a preset size limit.
    pub bytes: u64,
}

impl FoldCheckpointObject {
    /// Stores a checkpoint through the existing original-ledger Objects API.
    /// This does not publish an authored volume root or start background work.
    pub async fn save<S: AsyncObjectStore, T: Serialize>(
        store: &S,
        checkpoint: &FoldCheckpoint<T>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Self, FoldObjectError> {
        if checkpoint.version != 1 {
            return Err(FoldObjectError::Mismatch);
        }
        let key = checkpoint.accounted_key(ctx)?;
        let bytes = OwnedSourceChunk::produce(ctx, |sink| {
            serde_json::to_writer(sink, checkpoint).map_err(FoldObjectError::from)
        })?;
        let cancellation = ctx.cancellation();
        let native = NativeAccounting::take(ctx)?;
        let result = async {
            let object = HashedObject::new_with_native_work(
                ObjectKind::BlobChunk,
                bytes.bytes,
                &native.work,
                cancellation,
            )?;
            let mut work = AuthorityWork::native(&native.work, cancellation);
            let object = store.prepare_hashed(object, &mut work).await?;
            let descriptor = Self {
                key,
                object: object.object_id(),
                bytes: object.length(),
            };
            store.put_hashed(object, &mut work).await?;
            Ok(descriptor)
        }
        .await;
        native.restore(ctx)?;
        result
    }

    /// Loads only the selected object. Collection may make it absent; callers
    /// can then recompute from an earlier checkpoint or the explicit seed.
    /// Wrong envelopes and lengths are rejected before exposing folded state.
    pub async fn load<S: AsyncObjectStore, T: DeserializeOwned>(
        &self,
        store: &S,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Retained<FoldCheckpoint<T>>, FoldObjectError> {
        if self.object.kind != ObjectKind::BlobChunk {
            return Err(FoldObjectError::Mismatch);
        }
        let cancellation = ctx.cancellation();
        let native = NativeAccounting::take(ctx)?;
        let result = async {
            let mut work = AuthorityWork::native(&native.work, cancellation);
            let bytes = store.read(self.object, self.bytes, &mut work).await?;
            if bytes.len() as u64 != self.bytes {
                return Err(FoldObjectError::Mismatch);
            }
            let mut decode = KernelAccounting::NativeOriginal {
                work: &native.work,
                cancellation,
            };
            let checkpoint: Retained<FoldCheckpoint<T>> = decode.decode_json(&bytes)?;
            if checkpoint.version != 1 || checkpoint.accounted_key(&mut decode)? != self.key {
                return Err(FoldObjectError::Mismatch);
            }
            Ok(checkpoint)
        }
        .await;
        native.restore(ctx)?;
        result
    }
}

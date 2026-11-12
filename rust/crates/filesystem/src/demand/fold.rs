//! Append-only folds. Checkpoints are disposable immutable cache objects, not
//! authored state. The consumer stores their encoding in Objects and indexes
//! their `key()` by cut; no process-global cache or invalidation task exists.
use super::*;
use crate::kernel::{KernelAccounting, Retained};
use std::num::NonZeroU64;

/// A deterministic append-only projection. Its identity includes both the
/// fold algorithm and the encoded state format version.
pub trait Fold {
    /// State serialized into an immutable checkpoint object by the consumer.
    type State;
    /// One committed stream record.
    type Record;
    /// Algorithm and state-format identity.
    fn identity(&self) -> [u8; 32];
    /// Applies exactly the record at `sequence`, on the caller's ledger.
    fn apply(
        &self,
        state: &mut Self::State,
        sequence: u64,
        record: &Self::Record,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), DemandError>;
}

/// Versioned cache payload for one exact (view, stream cut, volume generation).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FoldCheckpoint<S> {
    /// Checkpoint envelope format, currently v1.
    pub version: u32,
    /// Fold implementation and state-format identity.
    pub view: [u8; 32],
    /// First sequence not yet included.
    pub sequence: u64,
    /// Exact authored input generation.
    pub generation: [u8; 32],
    /// Folded prefix, containing no records from the tail.
    pub state: S,
}

impl<S> FoldCheckpoint<S> {
    /// Stable cache index key. The serialized payload is separately addressed
    /// by Objects' content digest; the key is never substituted for that digest.
    pub fn key(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic:fold-checkpoint:v1");
        hash.update(&self.view);
        hash.update(&self.sequence.to_le_bytes());
        hash.update(&self.generation);
        *hash.finalize().as_bytes()
    }
    fn accounted_key(&self, ctx: &mut KernelAccounting<'_>) -> Result<[u8; 32], crate::FsError> {
        ctx.charge(WorkCounters {
            bytes_hashed: (b"acyclic:fold-checkpoint:v1".len() + 32 + 8 + 32) as u64,
            ..WorkCounters::UNCHARGED
        })?;
        Ok(self.key())
    }
}

/// An owned fold over a checkpoint plus a contiguous stream tail. Chunk size
/// only controls checkpoint frequency; it never truncates the input or state.
pub struct FoldCursor<F: Fold> {
    fold: F,
    checkpoint: Retained<FoldCheckpoint<F::State>>,
    chunk: NonZeroU64,
}

impl<F: Fold> FoldCursor<F> {
    /// Resumes an already decoded cache object. A foreign algorithm or format
    /// refuses before any record is consumed. A missing checkpoint uses an
    /// explicit v1 checkpoint at sequence zero with the fold's initial state.
    pub fn resume(
        fold: F,
        checkpoint: Retained<FoldCheckpoint<F::State>>,
        chunk: NonZeroU64,
    ) -> Result<Self, DemandError> {
        if checkpoint.version != 1 || checkpoint.view != fold.identity() {
            return Err(DemandError::StaleVersion);
        }
        Ok(Self {
            fold,
            checkpoint,
            chunk,
        })
    }

    /// Consumes exactly the next record. Taking ownership ensures an errored
    /// partial fold can never be reused as a valid checkpoint.
    pub fn advance(
        mut self,
        sequence: u64,
        record: &F::Record,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Self, DemandError> {
        ctx.check()?;
        if sequence != self.checkpoint.sequence {
            return Err(DemandError::InvalidRequest);
        }
        let next = sequence.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        self.fold
            .apply(&mut self.checkpoint.value.state, sequence, record, ctx)?;
        self.checkpoint.value.sequence = next;
        Ok(self)
    }

    /// Borrow the current prefix for reading or cache-object encoding.
    pub fn checkpoint(&self) -> &FoldCheckpoint<F::State> {
        &self.checkpoint
    }
    /// A chunk boundary at which the consumer may persist a checkpoint.
    pub fn checkpoint_due(&self) -> bool {
        self.checkpoint.sequence.is_multiple_of(self.chunk.get())
    }
    /// Transfers the final prefix without copying its state.
    pub fn into_checkpoint(self) -> Retained<FoldCheckpoint<F::State>> {
        self.checkpoint
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod objects;
#[cfg(not(target_arch = "wasm32"))]
pub use objects::*;
#[cfg(not(target_arch = "wasm32"))]
mod stream;
#[cfg(not(target_arch = "wasm32"))]
pub use stream::*;

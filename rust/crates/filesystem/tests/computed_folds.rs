//! Checkpoint-plus-tail reads use real immutable Objects and original credit.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_fs::{
    CancellationToken, MemoryObjectStore, NativeMaterializationWork, WorkBudget,
    demand::{
        DemandError, Fold, FoldCheckpoint, FoldCheckpointObject, FoldCursor, FoldObjectError,
    },
    kernel::{KernelAccounting, Retained},
};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Total {
    label: String,
    value: u64,
}
struct Sum;
impl Fold for Sum {
    type State = Total;
    type Record = u64;
    fn identity(&self) -> [u8; 32] {
        [7; 32]
    }
    fn apply(
        &self,
        state: &mut Total,
        _: u64,
        record: &u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        ctx.check()?;
        state.value = state
            .value
            .checked_add(*record)
            .ok_or(acyclic_fs::WorkError::Overflow)?;
        Ok(())
    }
}

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn objects_checkpoint_reopens_and_resumes_on_original_credit() -> Result {
    let cancellation = CancellationToken::new();
    let mut ctx = KernelAccounting::Standalone {
        allocations: Default::default(),
        work: Default::default(),
        budget: WorkBudget::UNBOUNDED,
        cancellation: &cancellation,
    };
    let store = MemoryObjectStore::default();
    let seed: Retained<FoldCheckpoint<Total>> =
        ctx.decode_json(&serde_json::to_vec(&FoldCheckpoint {
            version: 1,
            view: Sum.identity(),
            sequence: 0,
            generation: [9; 32],
            state: Total {
                label: "the original retained state".into(),
                value: 0,
            },
        })?)?;
    let chunk = NonZeroU64::new(2).unwrap();
    let folded = FoldCursor::resume(Sum, seed, chunk)?
        .advance(0, &11, &mut ctx)?
        .advance(1, &23, &mut ctx)?;
    assert!(folded.checkpoint_due());
    let descriptor = FoldCheckpointObject::save(&store, folded.checkpoint(), &mut ctx).await?;
    // The cache descriptor is persistent data, independent of either cursor.
    let encoded = serde_json::to_vec(&descriptor)?;
    drop(folded);
    ctx.check()?;
    let reopened: FoldCheckpointObject = serde_json::from_slice(&encoded)?;
    let loaded = reopened.load::<_, Total>(&store, &mut ctx).await?;
    assert_eq!(loaded.sequence, 2);
    assert_eq!(loaded.state.value, 34);
    let tail = FoldCursor::resume(Sum, loaded, chunk)?.advance(2, &47, &mut ctx)?;
    assert_eq!(tail.checkpoint().state.value, 81);
    assert_eq!(tail.checkpoint().state.label, "the original retained state");
    drop(store);
    ctx.check()?;
    assert_eq!(tail.checkpoint().state.value, 81);
    drop(tail);
    ctx.check()?;
    Ok(())
}

#[tokio::test]
async fn checkpoint_refuses_wrong_prefix_length_and_cancelled_original() -> Result {
    let cancellation = CancellationToken::new();
    let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut ctx = KernelAccounting::NativeOriginal {
        work: &original,
        cancellation: &cancellation,
    };
    let store = MemoryObjectStore::default();
    let checkpoint = FoldCheckpoint {
        version: 1,
        view: Sum.identity(),
        sequence: 12,
        generation: [8; 32],
        state: Total {
            label: "cached".into(),
            value: 100,
        },
    };
    let descriptor = FoldCheckpointObject::save(&store, &checkpoint, &mut ctx).await?;
    let mut wrong = descriptor;
    wrong.key[0] ^= 1;
    assert!(matches!(
        wrong.load::<_, Total>(&store, &mut ctx).await,
        Err(FoldObjectError::Mismatch)
    ));
    wrong = descriptor;
    wrong.bytes += 1;
    assert!(matches!(
        wrong.load::<_, Total>(&store, &mut ctx).await,
        Err(FoldObjectError::Mismatch)
    ));
    wrong = descriptor;
    wrong.bytes -= 1;
    assert!(wrong.load::<_, Total>(&store, &mut ctx).await.is_err());
    cancellation.cancel();
    assert!(descriptor.load::<_, Total>(&store, &mut ctx).await.is_err());
    assert!(
        FoldCheckpointObject::save(&store, &checkpoint, &mut ctx)
            .await
            .is_err()
    );
    Ok(())
}

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
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

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

struct CountedSum(Arc<AtomicUsize>);
impl Fold for CountedSum {
    type State = u64;
    type Record = acyclic_stream::Record;
    fn identity(&self) -> [u8; 32] {
        [17; 32]
    }
    fn apply(
        &self,
        state: &mut u64,
        _: u64,
        record: &Self::Record,
        ctx: &mut KernelAccounting<'_>,
    ) -> std::result::Result<(), DemandError> {
        ctx.check()?;
        let value = u64::from_le_bytes(
            record
                .value
                .as_slice()
                .try_into()
                .map_err(|_| DemandError::InvalidRequest)?,
        );
        *state = state
            .checked_add(value)
            .ok_or(acyclic_fs::WorkError::Overflow)?;
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn checkpoint_catalog_reopens_nearest_prefix_and_reads_only_new_events() -> Result {
    use acyclic_fs::demand::{FoldCache, FoldCacheIdentity};
    use acyclic_stream::{
        AppendOutcome, AppendRequest, IdempotencyKey, MemoryStream, StreamPath,
        StreamProviderExt as _,
    };
    let cancellation = CancellationToken::new();
    let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut ctx = KernelAccounting::NativeOriginal {
        work: &original,
        cancellation: &cancellation,
    };
    let streams = Arc::new(MemoryStream::default());
    let objects = Arc::new(MemoryObjectStore::default());
    let path = StreamPath::new("usage/first")?;
    let mut cuts = Vec::new();
    for batch in 0..3u64 {
        let outcome = streams
            .append(AppendRequest {
                path: path.clone(),
                records: (batch * 4 + 1..=batch * 4 + 4)
                    .map(|value| value.to_le_bytes().to_vec().into())
                    .collect(),
                if_tail: Some(batch * 4),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        let AppendOutcome::Committed(receipt) = outcome else {
            return Err("fixture append refused".into());
        };
        cuts.push(receipt.revision);
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let identity = |generation, chunk| FoldCacheIdentity {
        input: path.clone(),
        authority: cuts[0].clone(),
        view: [17; 32],
        generation,
        chunk: NonZeroU64::new(chunk).unwrap(),
    };
    let seed = |generation| {
        serde_json::to_vec(&FoldCheckpoint {
            version: 1,
            view: [17; 32],
            sequence: 0,
            generation,
            state: 0u64,
        })
    };
    let encoded_seed = seed([4; 32])?;
    // Reconstruct the cache adapter for every read: no process-local index or
    // folded state survives. Historical selection must use an indexed ordinal.
    for (cut, expected, applied) in [(1, 36, 8), (2, 78, 4), (0, 10, 0), (2, 78, 0)] {
        calls.store(0, Ordering::Relaxed);
        let cache = FoldCache::new(
            Arc::clone(&streams),
            Arc::clone(&objects),
            identity([4; 32], 2),
            &mut ctx,
        )?;
        let state = cache
            .fold_at(
                CountedSum(Arc::clone(&calls)),
                cuts[cut].clone(),
                |ctx| {
                    ctx.decode_json(&encoded_seed)
                        .map_err(FoldObjectError::from)
                },
                &mut ctx,
            )
            .await?;
        assert_eq!(state.state, expected);
        assert_eq!(calls.load(Ordering::Relaxed), applied);
    }
    // Both authored generation and checkpoint frequency are namespace inputs.
    // Their deterministic publication keys must not collide in one authority.
    for (generation, chunk) in [([5; 32], 2), ([4; 32], 3)] {
        calls.store(0, Ordering::Relaxed);
        let encoded_seed = seed(generation)?;
        let cache = FoldCache::new(
            Arc::clone(&streams),
            Arc::clone(&objects),
            identity(generation, chunk),
            &mut ctx,
        )?;
        let state = cache
            .fold_at(
                CountedSum(Arc::clone(&calls)),
                cuts[2].clone(),
                |ctx| {
                    ctx.decode_json(&encoded_seed)
                        .map_err(FoldObjectError::from)
                },
                &mut ctx,
            )
            .await?;
        assert_eq!(state.state, 78);
        assert_eq!(calls.load(Ordering::Relaxed), 12);
    }
    ctx.check()?;
    Ok(())
}

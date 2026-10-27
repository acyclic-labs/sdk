//! Deterministic interleaving of two fork operations racing to create one volume.
//!
//! The child volume's first creation commit is the only meeting point between
//! two fork operations naming the same destination: it is keyed by the
//! creating operation, so a second operation loses there. Each seed yields a
//! different, reproducible number of times before every provider call while
//! both forks run on one task, which exhaustively perturbs where the two
//! operations interleave.
use super::*;
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};

thread_local! {
    static RUNNING: Cell<u8> = const { Cell::new(0) };
}

/// Marks which racing operation is being polled, so provider calls are attributed.
struct Attributed<F> {
    operation: u8,
    inner: Pin<Box<F>>,
}

impl<F: Future> Future for Attributed<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        let previous = RUNNING.with(|running| running.replace(self.operation));
        let polled = self.inner.as_mut().poll(context);
        RUNNING.with(|running| running.set(previous));
        polled
    }
}

struct YieldStream {
    inner: Arc<acyclic_stream::MemoryStream>,
    seed: u64,
    calls: Mutex<Vec<u8>>,
}

impl YieldStream {
    async fn pause(&self) {
        let call = self.calls.lock().map_or(0, |mut calls| {
            calls.push(RUNNING.with(Cell::get));
            calls.len() as u64
        });
        // splitmix64 over (seed, call): reproducible and independent per call.
        let mut mixed = self
            .seed
            .wrapping_add(call.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        for _ in 0..(mixed ^ (mixed >> 31)) % 4 {
            tokio::task::yield_now().await;
        }
    }

    /// Whether provider calls of the two operations interleaved.
    fn interleaved(&self) -> bool {
        self.calls.lock().is_ok_and(|calls| {
            let attributed: Vec<u8> = calls.iter().copied().filter(|id| *id != 0).collect();
            attributed
                .windows(2)
                .filter(|pair| pair[0] != pair[1])
                .count()
                > 1
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl acyclic_stream::StreamProvider for YieldStream {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.inner.operation_claims()
    }

    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> Result<Option<acyclic_stream::IdempotencyObservation>, acyclic_stream::StreamError> {
        self.pause().await;
        self.inner.inspect_idempotency(key).await
    }

    async fn head(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> Result<acyclic_stream::PathHead, acyclic_stream::StreamError> {
        self.pause().await;
        self.inner.head(path).await
    }
    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> Result<u64, acyclic_stream::StreamError> {
        self.pause().await;
        self.inner.tail(path).await
    }

    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> Result<acyclic_stream::StreamBounds, acyclic_stream::StreamError> {
        self.pause().await;
        self.inner.bounds(path).await
    }

    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> Result<acyclic_stream::AppendOutcome, acyclic_stream::StreamError> {
        self.pause().await;
        self.inner.append(request).await
    }

    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> Result<acyclic_stream::ForkReceipt, acyclic_stream::StreamError> {
        self.pause().await;
        self.inner.fork(request).await
    }

    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        self.pause().await;
        self.inner.read(request).await
    }

    async fn follow(&self, request: acyclic_stream::FollowRequest) -> Result<acyclic_stream::FollowStream, acyclic_stream::StreamError> {
        self.pause().await;
        self.inner.follow(request).await
    }

    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError> {
        self.pause().await;
        self.inner.commit(request).await
    }

    async fn read_commit(
        &self,
        commit_id: acyclic_stream::CommitId,
    ) -> Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        self.pause().await;
        self.inner.read_commit(commit_id).await
    }
}

type RaceFs = Fs<
    crate::distributed::StreamAuthorityStore<YieldStream>,
    crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
>;

/// Races two forks of one pinned generation into the same destination.
/// Returns how many operations won and whether their provider calls interleaved.
async fn race(seed: u64, keys: [IdempotencyKey; 2]) -> Result<(usize, bool), Box<dyn Error>> {
    let stream = Arc::new(YieldStream {
        inner: Arc::new(acyclic_stream::MemoryStream::default()),
        seed,
        calls: Mutex::new(Vec::new()),
    });
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let fs: RaceFs = Fs::new(
        crate::distributed::StreamAuthorityStore::new(Arc::clone(&stream)),
        crate::LogicalObjectStore::new(Arc::new(objects), bucket),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let parent = unbounded!(fs, create_workspace("race-parent"))?;
    parent.write_text("/base.txt", "base").await?;
    let pinned = unbounded!(parent, head())?;
    if let Ok(mut calls) = stream.calls.lock() {
        calls.clear();
    }
    let fork = |operation: u8, key: IdempotencyKey| Attributed {
        operation,
        inner: Box::pin(fork_workspace(
            &parent,
            "race-child",
            ForkOptions::from_generation(pinned.clone(), key),
        )),
    };
    let (first, second) = futures::join!(fork(1, keys[0]), fork(2, keys[1]));
    let interleaved = stream.interleaved();
    let mut winners = Vec::new();
    for outcome in [first, second] {
        match outcome {
            Ok(child) => winners.push(child),
            Err(
                WorkspaceError::Filesystem(FsError::CreationRejected)
                | WorkspaceError::RetentionConflict,
            ) => {}
            Err(other) => return Err(format!("seed {seed}: unexpected loss {other:?}").into()),
        }
    }
    // Every winner observes the one durable creation and its inherited content.
    let durable = unbounded!(fs, open_workspace("race-child"))?;
    for child in &winners {
        assert_eq!(child.id(), durable.id(), "seed {seed}");
        assert_eq!(
            unbounded!(child, read("/base.txt", 16))?,
            OwnedBytes::from_static(b"base")
        );
    }
    Ok((winners.len(), interleaved))
}

const SEEDS: u64 = 48;

#[tokio::test]
async fn two_operations_creating_one_volume_have_exactly_one_winner() -> Result<(), Box<dyn Error>>
{
    let mut interleavings = 0;
    for seed in 0..SEEDS {
        let keys = [
            IdempotencyKey::from_bytes([0xa1; 16]),
            IdempotencyKey::from_bytes([0xb2; 16]),
        ];
        let (winners, interleaved) = race(seed, keys).await?;
        assert_eq!(
            winners, 1,
            "seed {seed}: creation must admit exactly one operation"
        );
        interleavings += u64::from(interleaved);
    }
    // The schedule is not vacuous: most seeds genuinely interleave both operations.
    assert!(
        interleavings * 2 > SEEDS,
        "only {interleavings} seeds interleaved"
    );
    Ok(())
}

/// Negative control: a creation that does not distinguish the claiming
/// operation (both present the same creation identity) lets both win.
#[tokio::test]
async fn negative_control_unclaimed_creation_lets_both_operations_win() -> Result<(), Box<dyn Error>>
{
    for seed in 0..SEEDS {
        let shared = IdempotencyKey::from_bytes([0xc3; 16]);
        let (winners, _) = race(seed, [shared, shared]).await?;
        assert_eq!(
            winners, 2,
            "seed {seed}: unclaimed creation must admit both"
        );
    }
    Ok(())
}

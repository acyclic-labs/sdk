//! Crash-cut enumeration over a real on-disk `LocalStream`.
//!
//! Cut points are the provider's journal appends, observed at the append
//! itself: every persisted frame, including a conflicting commit retained
//! with its idempotency receipt, and nothing else; an elided replay persists
//! nothing. Snapshot compaction rewrites the same state and is not a cut. A
//! clean run counts the frames, then every cut replays the operation in a
//! fresh directory and crashes at one boundary:
//! - `BeforeWrite(k)`: frames `1..=k` landed, along with every non-durable
//!   effect performed before frame `k + 1`, which is never written;
//! - `LostAck(k)`: frame `k` was flushed but the process died before seeing it.
//!
//! The provider flushes each frame before acknowledging it, and a crash models
//! power loss: a written but unflushed frame does not survive. A crash
//! poisons the provider, so every later call fails. All handles are
//! dropped, an independent provider reopens the directory, and `recover`
//! observes it.
use acyclic_stream::{LocalStream, PersistenceCut};
use std::{
    future::Future,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Cut {
    BeforeWrite(usize),
    LostAck(usize),
}

#[cfg(test)]
thread_local! {
    /// Fault switch: a lost acknowledgement fires after its frame is written
    /// but before it is flushed, so the frame does not survive the crash.
    static LOSE_UNFLUSHED_ACK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

struct Cutter {
    cut: Option<Cut>,
    landed: AtomicUsize,
    crashed: AtomicBool,
}

impl Cutter {
    fn fires(&self, at: Cut) -> bool {
        if self.cut == Some(at) {
            self.crashed.store(true, Ordering::SeqCst);
        }
        self.crashed.load(Ordering::SeqCst)
    }
}

impl PersistenceCut for Cutter {
    fn admit(&self) -> bool {
        !self.fires(Cut::BeforeWrite(self.landed.load(Ordering::SeqCst)))
    }

    #[cfg(test)]
    fn written(&self) -> bool {
        let next = Cut::LostAck(self.landed.load(Ordering::SeqCst) + 1);
        if LOSE_UNFLUSHED_ACK.with(std::cell::Cell::get) && self.cut == Some(next) {
            self.landed.fetch_add(1, Ordering::SeqCst);
            return !self.fires(next);
        }
        true
    }

    fn acknowledge(&self) -> bool {
        let landed = self.landed.fetch_add(1, Ordering::SeqCst) + 1;
        !self.fires(Cut::LostAck(landed))
    }
}

pub(crate) struct CrashCuts<O> {
    /// Journal frames persisted by the uncrashed operation.
    pub(crate) writes: usize,
    /// Recovery observation after the uncrashed operation.
    pub(crate) clean: O,
    /// Every cut, in boundary order, with its recovery observation.
    pub(crate) cuts: Vec<(Cut, O)>,
}

async fn open(root: &Path) -> Arc<LocalStream> {
    Arc::new(
        LocalStream::open(root, Default::default())
            .await
            .expect("crash-cut provider opens; every prior handle must be dropped"),
    )
}

async fn once<O, S, SF, R, RF, V, VF>(
    cut: Option<Cut>,
    setup: &S,
    run: &R,
    recover: &V,
) -> (usize, O)
where
    S: Fn(Arc<LocalStream>) -> SF,
    SF: Future<Output = ()>,
    R: Fn(Arc<LocalStream>) -> RF,
    RF: Future<Output = ()>,
    V: Fn(Arc<LocalStream>) -> VF,
    VF: Future<Output = O>,
{
    let directory = tempfile::tempdir().expect("crash-cut directory");
    let stream = open(directory.path()).await;
    setup(stream.clone()).await;
    let cutter = Arc::new(Cutter {
        cut,
        landed: AtomicUsize::new(0),
        crashed: AtomicBool::new(false),
    });
    stream.cut_persistence(cutter.clone());
    run(stream.clone()).await;
    let writes = cutter.landed.load(Ordering::SeqCst);
    let Ok(stream) = Arc::try_unwrap(stream) else {
        panic!("operation retained the crashed provider");
    };
    drop(stream);
    let observed = recover(open(directory.path()).await).await;
    (writes, observed)
}

/// Runs `setup` uncounted, then `run` once cleanly and once per durable cut.
/// `run` must ignore its own result; `recover` reopens and observes.
pub(crate) async fn enumerate<O, S, SF, R, RF, V, VF>(
    setup: S,
    run: R,
    recover: V,
) -> CrashCuts<O>
where
    S: Fn(Arc<LocalStream>) -> SF,
    SF: Future<Output = ()>,
    R: Fn(Arc<LocalStream>) -> RF,
    RF: Future<Output = ()>,
    V: Fn(Arc<LocalStream>) -> VF,
    VF: Future<Output = O>,
{
    let (writes, clean) = once(None, &setup, &run, &recover).await;
    assert!(writes > 0, "operation made no durable write to cut");
    let mut cuts = Vec::with_capacity(2 * writes + 1);
    for landed in 0..=writes {
        let mut boundary = vec![Cut::BeforeWrite(landed)];
        if landed > 0 {
            boundary.insert(0, Cut::LostAck(landed));
        }
        for cut in boundary {
            let (reached, observed) = once(Some(cut), &setup, &run, &recover).await;
            // Every cut lands exactly its own writes. BeforeWrite(writes)
            // never fires, so it replays the clean run's durable state.
            let expected = match cut {
                Cut::BeforeWrite(k) | Cut::LostAck(k) => k,
            };
            assert_eq!(reached, expected, "{cut:?} did not reach its boundary");
            cuts.push((cut, observed));
        }
    }
    CrashCuts {
        writes,
        clean,
        cuts,
    }
}

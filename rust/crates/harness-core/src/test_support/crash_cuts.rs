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
//! observes it. A crash the provider could not simulate faithfully fails
//! the enumeration.
use acyclic_stream::{CutSimulationError, LocalStream, LocalStreamLimits, PersistenceCut};
use std::{
    future::Future,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum Cut {
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
    unfaithful: Mutex<Option<CutSimulationError>>,
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

    fn crashed(&self, simulation: Result<(), CutSimulationError>) {
        if let Err(error) = simulation {
            self.unfaithful
                .lock()
                .expect("simulation failure slot")
                .get_or_insert(error);
        }
    }
}

pub struct CrashCuts<O, T = ()> {
    /// Journal frames persisted by the uncrashed operation.
    #[doc(hidden)]
    pub writes: usize,
    /// Recovery observation after the uncrashed operation.
    #[doc(hidden)]
    pub clean: O,
    /// Original outcome returned before reopening the clean operation.
    pub clean_result: T,
    /// Every cut, in boundary order, with its recovery observation.
    #[doc(hidden)]
    pub cuts: Vec<(Cut, O)>,
    /// Original outcomes, in the same boundary order as `cuts`.
    pub cut_results: Vec<(Cut, T)>,
}

async fn open(root: &Path, limits: LocalStreamLimits) -> Arc<LocalStream> {
    Arc::new(
        LocalStream::open(root, limits)
            .await
            .expect("crash-cut provider opens; every prior handle must be dropped"),
    )
}

async fn once<O, T, S, SF, R, RF, V, VF>(
    limits: LocalStreamLimits,
    cut: Option<Cut>,
    setup: &S,
    run: &R,
    recover: &V,
) -> Result<(usize, T, O), CutSimulationError>
where
    S: Fn(Arc<LocalStream>) -> SF,
    SF: Future<Output = ()>,
    R: Fn(Arc<LocalStream>) -> RF,
    RF: Future<Output = T>,
    V: Fn(Arc<LocalStream>) -> VF,
    VF: Future<Output = O>,
{
    let directory = tempfile::tempdir().expect("crash-cut directory");
    let stream = open(directory.path(), limits).await;
    setup(stream.clone()).await;
    let cutter = Arc::new(Cutter {
        cut,
        landed: AtomicUsize::new(0),
        crashed: AtomicBool::new(false),
        unfaithful: Mutex::new(None),
    });
    stream.cut_persistence(cutter.clone());
    let result = run(stream.clone()).await;
    let unfaithful = cutter.unfaithful.lock().expect("simulation slot").take();
    if let Some(error) = unfaithful {
        return Err(error);
    }
    let writes = cutter.landed.load(Ordering::SeqCst);
    let Ok(stream) = Arc::try_unwrap(stream) else {
        panic!("operation retained the crashed provider");
    };
    drop(stream);
    let observed = recover(open(directory.path(), limits).await).await;
    Ok((writes, result, observed))
}

/// [`enumerate_with`] under the default limits, which never compact.
#[doc(hidden)]
pub async fn enumerate<O, T, S, SF, R, RF, V, VF>(
    setup: S,
    run: R,
    recover: V,
) -> Result<CrashCuts<O, T>, CutSimulationError>
where
    S: Fn(Arc<LocalStream>) -> SF,
    SF: Future<Output = ()>,
    R: Fn(Arc<LocalStream>) -> RF,
    RF: Future<Output = T>,
    V: Fn(Arc<LocalStream>) -> VF,
    VF: Future<Output = O>,
{
    enumerate_with(LocalStreamLimits::default(), setup, run, recover).await
}

/// Runs `setup` uncounted, then `run` once cleanly and once per durable cut,
/// every provider opened with `limits`. The original return value is retained
/// alongside each reopened observation, so the oracle can check acknowledgements.
pub async fn enumerate_with<O, T, S, SF, R, RF, V, VF>(
    limits: LocalStreamLimits,
    setup: S,
    run: R,
    recover: V,
) -> Result<CrashCuts<O, T>, CutSimulationError>
where
    S: Fn(Arc<LocalStream>) -> SF,
    SF: Future<Output = ()>,
    R: Fn(Arc<LocalStream>) -> RF,
    RF: Future<Output = T>,
    V: Fn(Arc<LocalStream>) -> VF,
    VF: Future<Output = O>,
{
    let (writes, clean_result, clean) = once(limits, None, &setup, &run, &recover).await?;
    assert!(writes > 0, "operation made no durable write to cut");
    let mut cuts = Vec::with_capacity(2 * writes + 1);
    let mut cut_results = Vec::with_capacity(2 * writes + 1);
    for landed in 0..=writes {
        let mut boundary = vec![Cut::BeforeWrite(landed)];
        if landed > 0 {
            boundary.insert(0, Cut::LostAck(landed));
        }
        for cut in boundary {
            let (reached, result, observed) =
                once(limits, Some(cut), &setup, &run, &recover).await?;
            // Every cut lands exactly its own writes. BeforeWrite(writes)
            // never fires, so it replays the clean run's durable state.
            let expected = match cut {
                Cut::BeforeWrite(k) | Cut::LostAck(k) => k,
            };
            assert_eq!(reached, expected, "{cut:?} did not reach its boundary");
            cuts.push((cut, observed));
            cut_results.push((cut, result));
        }
    }
    Ok(CrashCuts {
        writes,
        clean,
        clean_result,
        cuts,
        cut_results,
    })
}

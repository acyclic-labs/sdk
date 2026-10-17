//! A retired request must not visit or requeue any other pending request.
use super::*;
use proptest::prelude::*;
use std::{cell::Cell, rc::Rc, time::Instant};

struct Pending(Rc<Cell<usize>>);
impl Future for Pending {
    type Output = crate::task_volume::Result<HeadSnapshot>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}
fn path(n: usize) -> StreamPath {
    StreamPath::new(format!("swarm/a{n:05}")).unwrap()
}
fn check(count: usize, rebuild: bool) -> bool {
    let dropped = Rc::new(Cell::new(0));
    let mut snapshots = Snapshots::new(count);
    for id in 0..count {
        snapshots.start(path(id), Pending(dropped.clone())).unwrap();
    }
    assert!(snapshots.next().now_or_never().is_none());
    let retiring = path(count / 2);
    let started = Instant::now();
    let allocations = acyclic_test_support::measure(|| {
        if rebuild {
            // Exact former algorithm: rebuild FuturesUnordered to remove one
            // source. Requeueing visits and reallocates every surviving node.
            snapshots.futures = std::mem::take(&mut snapshots.futures).into_iter().collect();
        }
        snapshots.retire(&retiring).unwrap();
    });
    let elapsed = started.elapsed();
    let reclaimed = dropped.get() == 1 && snapshots.active.len() == count - 1;
    println!(
        "snapshot retirement n={count} rebuild={rebuild} elapsed={elapsed:?} allocations={allocations:?}"
    );
    // Indexed removal allocates nothing. The source's owner and future are
    // released immediately; draining its wake-queued shell cannot poll or
    // replace unrelated sources.
    let fixed = allocations.count_total == 0;
    assert!(snapshots.next().now_or_never().is_none());
    assert_eq!(snapshots.futures.len(), count - 1);
    reclaimed && fixed
}
#[test]
fn retirement_work_is_independent_of_the_pending_population() {
    for count in [2_048, 4_096, 8_192] {
        assert!(check(count, false));
        assert!(!check(count, true));
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn indexed_retirement_releases_only_its_owner(count in 4usize..256) { prop_assert!(check(count, false)); }
}
#[test]
fn negative_control_rebuilding_pending_futures_is_rejected() {
    assert!(!check(8, true));
}

use super::*;
use std::{cell::Cell, cmp::Ordering, collections::BTreeMap};

thread_local! {
    static COMPARISONS: Cell<usize> = const { Cell::new(0) };
}

#[derive(Eq, PartialEq)]
struct Compared(u64);
impl Ord for Compared {
    fn cmp(&self, other: &Self) -> Ordering {
        COMPARISONS.with(|count| count.set(count.get() + 1));
        self.0.cmp(&other.0)
    }
}
impl PartialOrd for Compared {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Count actual ready ingress, running notification, park, selection, unlink,
/// slot reuse and stale callbacks; population construction is outside the cut.
fn ready_work(population: usize) -> Result<(usize, usize), super::super::super::Error> {
    let set = ReadySet::default();
    let mut residents = Vec::new();
    for actor in 0..population {
        residents.push(set.register(actor)?);
    }
    assert_eq!(set.counts(), (population, 0));
    let before = registry::work();
    let last = residents
        .last()
        .ok_or(super::super::super::Error::Invalid("missing resident"))?;
    let stale = last.waker();
    stale.wake_by_ref();
    let first = set
        .take()
        .ok_or(super::super::super::Error::Invalid("lost wake"))?;
    assert_eq!(*first.key(), population - 1);
    stale.wake_by_ref();
    drop(first);
    let retained = set.take().ok_or(super::super::super::Error::Invalid(
        "lost running notification",
    ))?;
    drop(retained);
    // Remove a queued actor, then force reuse of its physical slot.
    stale.wake_by_ref();
    drop(residents.pop());
    let replacement = set.register(population)?;
    stale.wake_by_ref();
    assert!(
        set.take().is_none(),
        "stale generation revived a reused slot"
    );
    replacement.waker().wake();
    let next = set
        .take()
        .ok_or(super::super::super::Error::Invalid("lost replacement wake"))?;
    assert_eq!(*next.key(), population);
    drop(next);
    drop(replacement);
    let after = registry::work();
    assert_eq!(set.counts(), (population - 1, 0));
    Ok((after.0 - before.0, after.1 - before.1))
}

/// The former BTreeMap boundary is a positive comparison counter control,
/// not an alternative production implementation.
fn former_lookup_comparisons(population: usize) -> usize {
    let former: BTreeMap<_, _> = (0..population as u64)
        .map(|id| (Compared(id), id))
        .collect();
    let before = COMPARISONS.with(Cell::get);
    for id in [
        population as u64 - 1,
        population as u64 / 2,
        population as u64 - 2,
    ] {
        assert_eq!(former.get(&Compared(id)), Some(&id));
    }
    COMPARISONS.with(Cell::get) - before
}

#[test]
fn resident_growth_does_not_increase_original_queue_work() -> Result<(), super::super::super::Error>
{
    let small = ready_work(1_024)?;
    let large = ready_work(16_384)?;
    assert!(
        small.0 > 0 && small.1 > 0,
        "original operation counters must be live"
    );
    assert_eq!(
        small, large,
        "wake/finish/unlink/reuse work grew with residents"
    );
    assert!(
        former_lookup_comparisons(16_384) > former_lookup_comparisons(1_024),
        "former resident lookup must expose growing comparison work"
    );
    Ok(())
}

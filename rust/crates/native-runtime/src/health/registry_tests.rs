use super::*;
use crate::{NativeOperationKind, NativeReadMeter};
use acyclic_test_support::{AllocationInfo, differential, measure};
use proptest::prelude::*;
use std::sync::atomic::AtomicBool;

const ROW: u64 = std::mem::size_of::<(FileIdentity, Entry)>() as u64;
const HEALTH: u64 = std::mem::size_of::<FileHealth>() as u64;

fn id(n: u64) -> FileIdentity {
    (n, Default::default())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Call {
    Examine(u64, u64),
    AdmitExamination(u64, u64),
    RecordExamination(u64, u64),
    Allocate(u64),
    Adopt(u64),
    Release(u64),
}

/// Records every registry charge into storage reserved before measurement.
struct TraceMeter {
    calls: Mutex<Vec<Call>>,
    deny_allocations: AtomicBool,
    cancelled: AtomicBool,
}
impl TraceMeter {
    fn new() -> Self {
        let meter = Self {
            calls: Mutex::new(Vec::with_capacity(1 << 16)),
            deny_allocations: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
        };
        // Apple's pthread-backed mutex allocates on first lock; take it before measurement.
        drop(meter.calls.lock());
        meter
    }
    fn push(&self, call: Call) -> io::Result<()> {
        let mut calls = self.calls.lock().unwrap();
        assert!(
            calls.len() < calls.capacity(),
            "trace storage must not grow"
        );
        calls.push(call);
        self.check()
    }
    fn take(&self) -> Vec<Call> {
        std::mem::take(&mut *self.calls.lock().unwrap())
    }
}
impl NativeReadMeter for TraceMeter {
    fn check(&self) -> io::Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            Err(io::ErrorKind::Interrupted.into())
        } else {
            Ok(())
        }
    }
    fn examine(&self, items: u64, bytes: u64) -> io::Result<()> {
        self.push(Call::Examine(items, bytes))
    }
    fn admit_examination(&self, items: u64, bytes: u64) -> io::Result<()> {
        self.push(Call::AdmitExamination(items, bytes))
    }
    fn record_examination(&self, items: u64, bytes: u64) -> io::Result<()> {
        self.push(Call::RecordExamination(items, bytes))
    }
    fn allocate(&self, bytes: u64) -> io::Result<()> {
        self.push(Call::Allocate(bytes))?;
        if self.deny_allocations.load(Ordering::Acquire) {
            return Err(io::ErrorKind::QuotaExceeded.into());
        }
        Ok(())
    }
    fn adopt(&self, bytes: u64) -> io::Result<()> {
        self.push(Call::Adopt(bytes))
    }
    fn release(&self, bytes: u64) {
        let mut calls = self.calls.lock().unwrap();
        assert!(
            calls.len() < calls.capacity(),
            "trace storage must not grow"
        );
        calls.push(Call::Release(bytes));
    }
    fn reserve_operations(&self, _: NativeOperationKind, _: u64) -> io::Result<()> {
        unreachable!()
    }
    fn cancel_reserved_operations(&self, _: NativeOperationKind, _: u64) {
        unreachable!()
    }
    fn record_reserved_operation(&self, _: NativeOperationKind) -> io::Result<()> {
        unreachable!()
    }
    fn admit_write_bytes(&self, _: usize) -> io::Result<()> {
        unreachable!()
    }
    fn reserve_bytes(&self, _: NativeOperationKind, _: u64) -> io::Result<()> {
        unreachable!()
    }
    fn cancel_reserved_bytes(&self, _: NativeOperationKind, _: u64) {
        unreachable!()
    }
    fn record_reserved_bytes(&self, _: NativeOperationKind, _: u64, _: u64) -> io::Result<()> {
        unreachable!()
    }
    fn reserve_allocation(&self, _: u64) -> io::Result<()> {
        unreachable!()
    }
    fn cancel_reserved_allocation(&self, _: u64) {
        unreachable!()
    }
    fn record_reserved_allocation(&self, _: u64) -> io::Result<()> {
        unreachable!()
    }
    fn allocation_failed(&self, _: std::collections::TryReserveError) -> io::Error {
        unreachable!()
    }
    fn allocate_refcount_header(&self) -> io::Result<()> {
        unreachable!()
    }
    fn record_completed_adoption(&self, _: u64) -> (u64, io::Result<()>) {
        unreachable!()
    }
    fn admit_bytes(&self, _: usize) -> io::Result<()> {
        unreachable!()
    }
    fn admit_read(&self, _: usize) -> io::Result<()> {
        unreachable!()
    }
    fn record_read(&self, _: usize) -> io::Result<()> {
        unreachable!()
    }
    fn admit_write(&self, _: usize) -> io::Result<()> {
        unreachable!()
    }
    fn record_write(&self, _: usize) -> io::Result<()> {
        unreachable!()
    }
    fn admit_durability(&self) -> io::Result<()> {
        unreachable!()
    }
}

/// The admission before slot reuse, frozen verbatim: an expired row is removed, searched again and reinserted.
fn previous_admit(
    registry: &mut FileHealthRegistry,
    identity: FileIdentity,
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<Arc<FileHealth>> {
    let mut charged = 0;
    let result = (|| {
        let row = std::mem::size_of::<(FileIdentity, Entry)>();
        if let Ok(index) = registry.find(identity, meter)? {
            if let Some(owner) = registry
                .entries
                .get(index)
                .ok_or(io::ErrorKind::InvalidData)?
                .1
                .owner()
            {
                return Ok(owner);
            }
            if let Some(meter) = meter {
                meter.examine(0, ((registry.entries.len() - index - 1) * row) as u64)?;
            }
            registry.entries.remove(index);
        }
        if registry.entries.len() >= registry.capacity_limit {
            registry.sweep_idle(meter)?;
        }
        if registry.entries.len() >= registry.capacity_limit {
            return Err(io::Error::other("native file admission capacity exhausted"));
        }
        if registry.entries.len() == registry.entries.capacity() {
            let old = registry.entries.capacity() * row;
            let next = registry
                .entries
                .capacity()
                .saturating_mul(2)
                .max(1)
                .min(registry.capacity_limit);
            let bytes = next * row;
            if let Some(meter) = meter {
                meter.admit_examination(0, (registry.entries.len() * row) as u64)?;
                meter.adopt(old as u64)?;
                charged += old;
                meter.allocate(bytes as u64)?;
                charged += bytes;
            }
            let mut replacement = Vec::new();
            replacement
                .try_reserve_exact(next)
                .map_err(io::Error::other)?;
            replacement.append(&mut registry.entries);
            registry.entries = replacement;
            if let Some(meter) = meter {
                meter.release(old as u64);
                charged -= old;
                meter.record_examination(0, (registry.entries.len() * row) as u64)?;
                let actual = registry.entries.capacity() * row;
                if actual > bytes {
                    meter.adopt((actual - bytes) as u64)?;
                    charged += actual - bytes;
                }
            }
        }
        let index = registry
            .find(identity, meter)?
            .unwrap_or_else(|index| index);
        if let Some(meter) = meter {
            meter.allocate(std::mem::size_of::<FileHealth>() as u64)?;
            charged += std::mem::size_of::<FileHealth>();
        }
        let health = Arc::new(FileHealth::default());
        if let Some(meter) = meter {
            meter.examine(0, ((registry.entries.len() - index) * row) as u64)?;
        }
        registry
            .entries
            .insert(index, (identity, Entry::Healthy(Arc::downgrade(&health))));
        Ok(health)
    })();
    if let Some(meter) = meter {
        meter.release(charged as u64);
    }
    result
}

#[derive(Clone, Copy, Debug)]
enum Step {
    Admit(u64),
    Drop(usize),
    Poison(usize),
}
#[derive(Clone, Copy, Debug)]
enum Fault {
    DenyAllocation(u64),
    Cancel(u64),
}

#[derive(Debug, Eq, PartialEq)]
enum Outcome {
    /// The owner already held at this position of the run's handle list.
    Existing(usize),
    Fresh,
    Failed(io::ErrorKind, String),
}
#[derive(Debug, Eq, PartialEq)]
enum Row {
    Live(Option<usize>),
    Poisoned(Option<usize>),
}

struct Run {
    registry: FileHealthRegistry,
    meter: TraceMeter,
    owners: Vec<Option<Arc<FileHealth>>>,
    outcomes: Vec<Outcome>,
}
#[derive(Debug, Eq, PartialEq)]
struct Observed {
    outcomes: Vec<Outcome>,
    /// Observable rows; an expired row is equivalent to an absent one.
    rows: Vec<(FileIdentity, Row)>,
    allocations: AllocationInfo,
}

fn run(
    limit: usize,
    steps: &[Step],
    fault: Option<Fault>,
    previous: bool,
) -> (Observed, Vec<Call>) {
    let initialize = || Run {
        registry: FileHealthRegistry {
            entries: Vec::new(),
            capacity_limit: limit,
        },
        meter: TraceMeter::new(),
        owners: Vec::with_capacity(steps.len() + 1),
        outcomes: Vec::with_capacity(steps.len() + 1),
    };
    let admit = |state: &mut Run, identity| {
        let result = if previous {
            previous_admit(&mut state.registry, identity, Some(&state.meter))
        } else {
            state.registry.admit(identity, Some(&state.meter))
        };
        let outcome = match result {
            Ok(owner) => match state
                .owners
                .iter()
                .position(|held| held.as_ref().is_some_and(|held| Arc::ptr_eq(held, &owner)))
            {
                Some(position) => Outcome::Existing(position),
                None => {
                    state.owners.push(Some(owner));
                    Outcome::Fresh
                }
            },
            Err(error) => Outcome::Failed(error.kind(), error.to_string()),
        };
        state.outcomes.push(outcome);
    };
    let (observed, mut traces) = differential::observe(initialize, |mut state: Run| {
        let allocations = measure(|| {
            for step in steps {
                match *step {
                    Step::Admit(n) => admit(&mut state, id(n)),
                    Step::Drop(slot) => {
                        if let Some(held) = state.owners.get_mut(slot) {
                            *held = None;
                        }
                    }
                    Step::Poison(slot) => {
                        if let Some(Some(held)) = state.owners.get(slot)
                            && let Some(entry) = state.registry.entries.iter_mut().find(|entry| {
                                entry
                                    .1
                                    .owner()
                                    .is_some_and(|owner| Arc::ptr_eq(&owner, held))
                            })
                        {
                            entry.1 = Entry::Poisoned(Arc::clone(held));
                        }
                    }
                }
            }
            match fault {
                Some(Fault::DenyAllocation(n)) => {
                    state.meter.deny_allocations.store(true, Ordering::Release);
                    admit(&mut state, id(n));
                }
                Some(Fault::Cancel(n)) => {
                    state.meter.cancelled.store(true, Ordering::Release);
                    admit(&mut state, id(n));
                }
                None => {}
            }
        });
        let slot = |owner: &Arc<FileHealth>| {
            state
                .owners
                .iter()
                .position(|held| held.as_ref().is_some_and(|held| Arc::ptr_eq(held, owner)))
        };
        let rows = state
            .registry
            .entries
            .iter()
            .filter_map(|(identity, entry)| match entry {
                Entry::Healthy(health) => health
                    .upgrade()
                    .map(|owner| (*identity, Row::Live(slot(&owner)))),
                Entry::Poisoned(owner) => Some((*identity, Row::Poisoned(slot(owner)))),
            })
            .collect();
        differential::record(state.meter.take());
        Observed {
            outcomes: std::mem::take(&mut state.outcomes),
            rows,
            allocations,
        }
    });
    (observed, traces.remove(0))
}

fn extra_calls(previous: &[Call], current: &[Call]) -> Option<Vec<Call>> {
    let mut extra = Vec::new();
    let mut current = current.iter().peekable();
    for call in previous {
        if current.peek() == Some(&call) {
            current.next();
        } else {
            extra.push(*call);
        }
    }
    current.next().is_none().then_some(extra)
}

fn rescan_steps(files: u64, rounds: usize) -> Vec<Step> {
    let mut steps = Vec::new();
    for round in 0..=rounds {
        let base = round * files as usize;
        steps.extend((0..files).map(Step::Admit));
        steps.extend((0..files as usize).map(|slot| Step::Drop(base + slot)));
    }
    steps
}

/// Exact work of one rescan's readmissions: one search, one owner allocation, no row movement.
fn rescan_oracle(trace: &[Call], allocations: AllocationInfo) -> Result<(), String> {
    let files = 128;
    // Initial admissions of ascending identities, then three rescans of expired rows.
    let initial_probes = 649;
    let rescan_probes = 777;
    let probes = trace
        .iter()
        .filter(|call| **call == Call::Examine(1, 0))
        .count();
    let moved: u64 = trace
        .iter()
        .map(|call| match call {
            Call::Examine(0, bytes) => *bytes,
            _ => 0,
        })
        .sum();
    // A vector growth adopts its old storage first; an owner allocation is never preceded by adoption.
    let owners = trace
        .windows(2)
        .filter(|pair| match pair {
            [previous, Call::Allocate(HEALTH)] => !matches!(previous, Call::Adopt(_)),
            _ => false,
        })
        .count();
    let observed = (probes, moved, owners, allocations.count_total);
    // Eight vector growths reach capacity 128; each owner is one heap request.
    let expected = (
        initial_probes + 3 * rescan_probes,
        0,
        4 * files,
        4 * files as u64 + 8,
    );
    (observed == expected)
        .then_some(())
        .ok_or_else(|| format!("observed {observed:?}, expected {expected:?}"))
}

#[test]
fn expired_owner_reuses_its_slot_with_exact_rescan_work() {
    let steps = rescan_steps(128, 3);
    let [(previous, previous_trace), (current, current_trace)] =
        [true, false].map(|previous| run(4096, &steps, None, previous));
    assert_eq!(previous.outcomes, current.outcomes);
    assert_eq!(previous.rows, current.rows);
    assert_eq!(previous.allocations, current.allocations);
    rescan_oracle(&current_trace, current.allocations).unwrap();
    // Negative control: the restored remove/search/reinsert work fails the same oracle.
    let error = rescan_oracle(&previous_trace, previous.allocations).unwrap_err();
    let removed = extra_calls(&previous_trace, &current_trace).expect("candidate is a subsequence");
    let moved: u64 = removed
        .iter()
        .map(|call| match call {
            Call::Examine(0, bytes) => *bytes,
            _ => 0,
        })
        .sum();
    let probes = removed
        .iter()
        .filter(|call| **call == Call::Examine(1, 0))
        .count();
    assert!(
        removed
            .iter()
            .all(|call| matches!(call, Call::Examine(1, 0) | Call::Examine(0, _)))
    );
    // Two suffix shifts of 128*127/2 rows per rescan; the initial ascending inserts append.
    assert_eq!(moved, 3 * ROW * 128 * 127, "{error}");
    // A second search per admission: 649 initial plus 896 per rescan.
    assert_eq!(probes, 649 + 3 * 896);
    eprintln!(
        "HEALTH_REUSE previous_probes={} current_probes={} previous_moved={} current_moved=0 heap={}",
        649 * 2 + 3 * (777 + 896),
        649 + 3 * 777,
        moved,
        current.allocations.count_total
    );
}

fn steps() -> impl Strategy<Value = Vec<Step>> {
    prop::collection::vec(
        prop_oneof![
            4 => (0_u64..6).prop_map(Step::Admit),
            2 => (0_usize..12).prop_map(Step::Drop),
            1 => (0_usize..12).prop_map(Step::Poison),
        ],
        0..24,
    )
}
fn fault() -> impl Strategy<Value = Option<Fault>> {
    prop_oneof![
        Just(None),
        (0_u64..6).prop_map(|n| Some(Fault::DenyAllocation(n))),
        (0_u64..6).prop_map(|n| Some(Fault::Cancel(n))),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]
    /// Results, exact errors, observable rows and heap requests match the frozen admission;
    /// the candidate's charges are the previous ones minus searches and row movement.
    #[test]
    fn slot_reuse_matches_previous_admission(limit in 1_usize..6, steps in steps(), fault in fault()) {
        let (previous, previous_trace) = run(limit, &steps, fault, true);
        let (current, current_trace) = run(limit, &steps, fault, false);
        prop_assert_eq!(&previous.outcomes, &current.outcomes);
        prop_assert_eq!(&previous.rows, &current.rows);
        // A refused reuse leaves the expired row the previous admission had already freed.
        let requests = |info: AllocationInfo| (info.count_total, info.bytes_total);
        if fault.is_none() {
            prop_assert_eq!(previous.allocations, current.allocations);
        } else {
            prop_assert_eq!(requests(previous.allocations), requests(current.allocations));
        }
        let removed = extra_calls(&previous_trace, &current_trace);
        prop_assert!(removed.as_ref().is_some_and(|removed| removed
            .iter()
            .all(|call| matches!(call, Call::Examine(1, 0) | Call::Examine(0, _)))),
            "{:?} vs {:?}", previous_trace, current_trace);
    }
}

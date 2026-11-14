//! The deterministic cost ledger: exact hot-path counts per operation.
//!
//! [`operation`] opens a named operation for a future and every task it
//! spawns through the simulation scope. Each [`crate::cost::charge`] made by
//! it or by work it spawned counts against it and every enclosing operation,
//! even after the operation's own future returns. Once the call and all that
//! work have finished, its counts and its simulated-time latency join the
//! scope's [`Ledger`], which keeps per-operation totals and the worst single
//! call. Counts are exact and time is the paused Tokio clock, so the same seed
//! yields the same ledger. Flushes are charged at [`super::durable_flush`].
use super::*;
use crate::cost::Cost;
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

/// One operation call. Charges walk its chain of enclosing calls. Spawned work
/// holds the call, so it records only when its last holder drops: a charge
/// made after the operation's future returns still counts.
pub(crate) struct Call {
    name: &'static str,
    parent: Option<Arc<Call>>,
    counts: Mutex<Counts>,
    started: tokio::time::Instant,
    // The operation's own latency, set when its future completes.
    latency: Mutex<Option<Duration>>,
    ledger: Arc<Mutex<Ledger>>,
}

impl Drop for Call {
    fn drop(&mut self) {
        let counts = std::mem::take(&mut *lock(&self.counts));
        let latency = (*lock(&self.latency)).unwrap_or_else(|| self.started.elapsed());
        let mut ledger = lock(&self.ledger);
        let entry = ledger.operations.entry(self.name).or_default();
        entry.calls += 1;
        entry.total.sum(&counts);
        entry.worst.max(&counts);
        entry.latency = entry.latency.max(latency);
    }
}

/// Exact counts of each cost, and the bytes those costs moved.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Counts {
    /// Occurrences of each cost.
    pub costs: BTreeMap<Cost, u64>,
    /// Bytes moved, per cost (copies).
    pub bytes: BTreeMap<Cost, u64>,
}

impl Counts {
    /// Occurrences of `cost`.
    #[must_use]
    pub fn count(&self, cost: Cost) -> u64 {
        self.costs.get(&cost).copied().unwrap_or(0)
    }

    fn add(&mut self, cost: Cost, bytes: u64) {
        *self.costs.entry(cost).or_default() += 1;
        if bytes > 0 {
            *self.bytes.entry(cost).or_default() += bytes;
        }
    }

    fn max(&mut self, other: &Self) {
        for (cost, count) in &other.costs {
            let slot = self.costs.entry(*cost).or_default();
            *slot = (*slot).max(*count);
        }
        for (cost, bytes) in &other.bytes {
            let slot = self.bytes.entry(*cost).or_default();
            *slot = (*slot).max(*bytes);
        }
    }

    fn sum(&mut self, other: &Self) {
        for (cost, count) in &other.costs {
            *self.costs.entry(*cost).or_default() += count;
        }
        for (cost, bytes) in &other.bytes {
            *self.bytes.entry(*cost).or_default() += bytes;
        }
    }
}

/// One operation's calls in a scope.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Operation {
    /// Completed calls.
    pub calls: u64,
    /// Counts summed over all calls.
    pub total: Counts,
    /// The worst single call, per cost (each cost's maximum independently).
    pub worst: Counts,
    /// Longest simulated-time latency of one call.
    pub latency: Duration,
}

/// Per-operation exact costs of one simulation scope.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Ledger {
    /// Completed operations by name.
    pub operations: BTreeMap<&'static str, Operation>,
    /// Costs charged in the scope outside every operation.
    pub unattributed: Counts,
}

/// The most one call of an operation may spend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Budget {
    /// At most this many occurrences of a cost per call.
    Count(&'static str, Cost, u64),
    /// At most this simulated-time latency per call.
    Latency(&'static str, Duration),
}

impl Ledger {
    /// The named operation, if any call of it completed.
    #[must_use]
    pub fn operation(&self, name: &str) -> Option<&Operation> {
        self.operations.get(name)
    }

    /// Checks each budget against the worst call of its operation. An
    /// operation that never completed fails its budget: an unmeasured path
    /// must not pass silently.
    ///
    /// # Errors
    ///
    /// Every exceeded or unmeasured budget, followed by the whole ledger.
    pub fn check(&self, budgets: &[Budget]) -> Result<(), String> {
        let mut report = Vec::new();
        for budget in budgets {
            // The exceeded observation, compared exactly; `None` when unmeasured.
            let (name, exceeded) = match *budget {
                Budget::Count(name, cost, limit) => (
                    name,
                    self.operation(name).map(|op| {
                        let spent = op.worst.count(cost);
                        (spent > limit).then(|| spent.to_string())
                    }),
                ),
                Budget::Latency(name, limit) => (
                    name,
                    self.operation(name)
                        .map(|op| (op.latency > limit).then(|| format!("{:?}", op.latency))),
                ),
            };
            match exceeded {
                None => report.push(format!("{budget:?}: operation {name} never completed")),
                Some(Some(spent)) => report.push(format!("{budget:?}: worst call spent {spent}")),
                Some(None) => {}
            }
        }
        if report.is_empty() {
            return Ok(());
        }
        Err(format!("{}\nledger:\n{self}", report.join("\n")))
    }

    /// Every difference between two ledgers' named operation, worst call
    /// (counts and bytes) and latency, as `cost: self -> other` lines; empty
    /// when equal. Compares one
    /// operation across two worlds, for example idle against busy neighbours.
    #[must_use]
    pub fn diff(&self, other: &Self, name: &str) -> String {
        let empty = Operation::default();
        let (left, right) = (
            self.operation(name).unwrap_or(&empty),
            other.operation(name).unwrap_or(&empty),
        );
        let changed = |unit: &str, a: &BTreeMap<Cost, u64>, b: &BTreeMap<Cost, u64>| {
            let costs: std::collections::BTreeSet<Cost> =
                a.keys().chain(b.keys()).copied().collect();
            costs
                .into_iter()
                .map(|cost| {
                    (
                        cost,
                        a.get(&cost).copied().unwrap_or(0),
                        b.get(&cost).copied().unwrap_or(0),
                    )
                })
                .filter(|(_, a, b)| a != b)
                .map(|(cost, a, b)| format!("{name} {cost:?}{unit}: {a} -> {b}\n"))
                .collect::<Vec<_>>()
        };
        let mut diff = changed("", &left.worst.costs, &right.worst.costs);
        diff.extend(changed(" bytes", &left.worst.bytes, &right.worst.bytes));
        if left.latency != right.latency {
            diff.push(format!(
                "{name} latency: {:?} -> {:?}\n",
                left.latency, right.latency
            ));
        }
        diff.concat()
    }
}

impl fmt::Display for Ledger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (name, operation) in &self.operations {
            write!(
                f,
                "  {name}: calls={} worst_latency={:?} worst",
                operation.calls, operation.latency
            )?;
            for (cost, count) in &operation.worst.costs {
                write!(f, " {cost:?}={count}")?;
            }
            writeln!(f)?;
        }
        if !self.unattributed.costs.is_empty() {
            write!(f, "  (unattributed)")?;
            for (cost, count) in &self.unattributed.costs {
                write!(f, " {cost:?}={count}")?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

/// Runs `future` as one call of the named operation in the current simulation
/// scope, including the tasks it spawns there. Outside a scope it only runs.
pub async fn operation<F: Future>(name: &'static str, future: F) -> F::Output {
    let Some(mut context) = Context::current() else {
        return future.await;
    };
    let call = Arc::new(Call {
        name,
        parent: context.operation.take(),
        counts: Mutex::new(Counts::default()),
        started: tokio::time::Instant::now(),
        latency: Mutex::new(None),
        ledger: context.schedule.0.ledger.clone(),
    });
    context.operation = Some(call.clone());
    let output = CURRENT.scope(Some(context), future).await;
    *lock(&call.latency) = Some(call.started.elapsed());
    // Records now, or when the last spawned holder drops.
    drop(call);
    output
}

pub(crate) fn charge(cost: Cost, bytes: u64) {
    let Some(context) = Context::simulation() else {
        return;
    };
    let mut call = context.operation.as_deref();
    if call.is_none() {
        lock(&context.schedule.0.ledger)
            .unattributed
            .add(cost, bytes);
    }
    while let Some(open) = call {
        lock(&open.counts).add(cost, bytes);
        call = open.parent.as_deref();
    }
}

impl Schedule {
    /// Exact per-operation costs of every call in this scope that has
    /// finished, together with all the work it spawned.
    pub fn ledger(&self) -> Ledger {
        lock(&self.0.ledger).clone()
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::cost::charge_bytes;

    // Releases every boundary at once: the ledger counts work, not orderings.
    fn world<T>(future: impl Future<Output = T>) -> (T, Ledger) {
        let (schedule, mut boundaries) = Schedule::seeded(7);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("runtime");
        let output = runtime.block_on(async {
            let driver = async {
                while let Some(boundary) = boundaries.recv().await {
                    boundary.release();
                }
            };
            tokio::select! {
                output = schedule.scope(future) => output,
                () = driver => unreachable!("the schedule outlives its driver"),
            }
        });
        (output, schedule.ledger())
    }

    #[test]
    fn charges_count_against_every_open_operation_and_its_spawned_tasks() {
        let ((), ledger) = world(async {
            crate::cost::charge(Cost::Lock);
            for hops in [1, 3] {
                operation("step", async move {
                    for _ in 0..hops {
                        crate::cost::charge(Cost::JournalHop);
                    }
                    operation("write", async {
                        charge_bytes(Cost::Copy, 10);
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    })
                    .await;
                    // Spawned work inherits the open operation.
                    spawn(async { crate::cost::charge(Cost::Fsync) }).await.ok();
                })
                .await;
            }
        });
        let step = ledger.operation("step").expect("step");
        assert_eq!(
            (
                step.calls,
                step.worst.count(Cost::JournalHop),
                step.total.count(Cost::JournalHop)
            ),
            (2, 3, 4)
        );
        assert_eq!(step.worst.count(Cost::Fsync), 1);
        assert_eq!(step.total.bytes.get(&Cost::Copy), Some(&20));
        assert_eq!(step.latency, Duration::from_millis(5));
        let write = ledger.operation("write").expect("write");
        assert_eq!((write.calls, write.worst.count(Cost::JournalHop)), (2, 0));
        assert_eq!(ledger.unattributed.count(Cost::Lock), 1);
        // Outside a simulation scope a charge is not observable anywhere.
        crate::cost::charge(Cost::Lock);
    }

    #[test]
    fn budgets_report_the_exceeded_cost_and_the_ledger() {
        let ((), ledger) = world(operation("step", async {
            for _ in 0..5 {
                crate::cost::charge(Cost::JournalHop);
            }
        }));
        assert!(
            ledger
                .check(&[Budget::Count("step", Cost::JournalHop, 5)])
                .is_ok()
        );
        let report = ledger
            .check(&[
                Budget::Count("step", Cost::JournalHop, 2),
                Budget::Latency("spawn", Duration::ZERO),
            ])
            .expect_err("over budget");
        assert!(report.contains("worst call spent 5"), "{report}");
        assert!(
            report.contains("operation spawn never completed"),
            "{report}"
        );
        assert!(report.contains("step: calls=1"), "{report}");
        let ((), cheaper) = world(operation("step", async {
            crate::cost::charge(Cost::JournalHop);
            charge_bytes(Cost::Copy, 10);
        }));
        assert_eq!(
            ledger.diff(&cheaper, "step"),
            "step JournalHop: 5 -> 1\nstep Copy: 0 -> 1\nstep Copy bytes: 0 -> 10\n"
        );
        // Latency budgets compare exact durations, not whole milliseconds.
        let ((), slow) = world(operation("step", async {
            tokio::time::sleep(Duration::from_micros(1900)).await;
        }));
        assert!(
            slow.check(&[Budget::Latency("step", Duration::from_millis(1))])
                .is_err()
        );
    }

    #[test]
    fn work_spawned_by_an_operation_counts_after_the_operation_returns() {
        let ((), ledger) = world(async {
            let task = operation("detached", async {
                spawn(async {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    crate::cost::charge(Cost::Fsync);
                })
            })
            .await;
            task.await.ok();
        });
        let detached = ledger
            .operation("detached")
            .expect("recorded once its task finished");
        assert_eq!((detached.calls, detached.worst.count(Cost::Fsync)), (1, 1));
        // The operation's own future returned at once.
        assert_eq!(detached.latency, Duration::ZERO);
    }

    #[test]
    fn durable_flushes_are_charged_at_the_core_fault_site() {
        let file = tempfile::tempfile().expect("file");
        let (result, ledger) = world(operation("flush", async { crate::flush_file_all(&file) }));
        result.expect("flush");
        assert_eq!(
            ledger
                .operation("flush")
                .map(|op| op.worst.count(Cost::Fsync)),
            Some(1)
        );
    }
}

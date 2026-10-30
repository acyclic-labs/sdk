//! One counting workload for primitives and composed agent hosts.
//!
//! Adapters execute real operations and supply observations from their original
//! work owners. Unknown counters remain unknown; a partial report cannot qualify.
//! The observer must include ingress, completion and cleanup, including workers.
//! Counts are not elapsed times, provider-call estimates or sampled CPU usage.

use std::{error::Error, fmt, future::Future};

/// The complete qualification population vector, in fresh storage domains.
pub const POPULATIONS: [u64; 3] = [1, 1_000, 100_000];
/// Composed operations applied in lifecycle order by deterministic simulations.
pub const LIFECYCLE_OPERATIONS: [Operation; 6] = [
    Operation::Create,
    Operation::Turn,
    Operation::Tool,
    Operation::Fork,
    Operation::ChildPage,
    Operation::Archive,
];
/// Primitive and session operations measured by the shared native benchmark.
pub const CORE_OPERATIONS: [Operation; 17] = [
    Operation::ActorCreate,
    Operation::ActorWake,
    Operation::ActorDurableWrite,
    Operation::ActorWarmRead,
    Operation::ObjectPut,
    Operation::ObjectGet,
    Operation::ObjectHead,
    Operation::ObjectList,
    Operation::StreamAppend,
    Operation::StreamRead,
    Operation::StreamFollow,
    Operation::WorkerCold,
    Operation::WorkerWarm,
    Operation::PinnedRead,
    Operation::StagedWrite,
    Operation::Fsync,
    Operation::SessionStart,
];
/// Independently observed work dimensions. Visits expose allocation-free scans.
pub const METRICS: [&str; 7] = [
    "syscalls",
    "allocations",
    "locks",
    "io_bytes",
    "control_requests",
    "visits",
    "allocated_bytes",
];

/// One completed operation, including its completion and cleanup.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Operation {
    /// Publish one actor's initial durable state.
    ActorCreate,
    /// Deliver one wake to its actor handler.
    ActorWake,
    /// Commit one actor metadata update.
    ActorDurableWrite,
    /// Read one resident actor metadata value.
    ActorWarmRead,
    /// Store one content-addressed blob.
    ObjectPut,
    /// Read one blob body.
    ObjectGet,
    /// Read one blob's metadata.
    ObjectHead,
    /// Read one bounded blob page.
    ObjectList,
    /// Commit one stream append.
    StreamAppend,
    /// Read one bounded committed record range.
    StreamRead,
    /// Deliver one committed record to an already parked follower.
    StreamFollow,
    /// Start a native worker and complete one call.
    WorkerCold,
    /// Complete one call on an already resident worker.
    WorkerWarm,
    /// Read one file at a pinned generation.
    PinnedRead,
    /// Stage one file update without publication.
    StagedWrite,
    /// Durably publish one staged filesystem transaction.
    Fsync,
    /// Create one session through the shared host binding.
    SessionStart,
    /// Create and settle one root agent with its private volume.
    Create,
    /// Consume one input and commit exactly one reply.
    Turn,
    /// Execute and commit exactly one tool call through the stock executor.
    Tool,
    /// Fork one child at the parent's exact committed cut.
    Fork,
    /// Read one bounded child page with the expected child and continuation.
    ChildPage,
    /// Archive the parent and child and prove subsequent delivery is closed.
    Archive,
}

impl Operation {
    /// Stable report identity, shared by time and work observations.
    pub const fn name(self) -> &'static str {
        match self {
            Self::ActorCreate => "actors.create",
            Self::ActorWake => "actors.wake",
            Self::ActorDurableWrite => "actors.durable_write",
            Self::ActorWarmRead => "actors.warm_read",
            Self::ObjectPut => "objects.put",
            Self::ObjectGet => "objects.get",
            Self::ObjectHead => "objects.head",
            Self::ObjectList => "objects.list",
            Self::StreamAppend => "streams.append",
            Self::StreamRead => "streams.read",
            Self::StreamFollow => "streams.follow",
            Self::WorkerCold => "workers.cold",
            Self::WorkerWarm => "workers.warm",
            Self::PinnedRead => "fs.pinned_read",
            Self::StagedWrite => "fs.staged_write",
            Self::Fsync => "fs.fsync",
            Self::SessionStart => "swarm.session_start",
            Self::Create => "agent.create",
            Self::Turn => "agent.turn",
            Self::Tool => "agent.tool",
            Self::Fork => "agent.fork",
            Self::ChildPage => "agent.children_page",
            Self::Archive => "agent.archive",
        }
    }
}

/// Original-operation counters. `None` means unobserved, never zero work.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Work(pub [Option<u64>; METRICS.len()]);

/// Live resources after all create completions and their cleanup events.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Idle {
    /// Net retained allocator bytes since the warmed, empty host baseline.
    /// Include the volume and workers; exclude observer-owned report storage.
    pub bytes: Option<u64>,
    /// Additional live native threads; shared pools belong in the baseline.
    pub threads: Option<u64>,
    /// Additional armed timers, including dormant registrations, not firings.
    pub timers: Option<u64>,
}

/// Real lifecycle and its original work observer, with no readiness probes.
pub trait Population {
    /// Complete one operation and its safety oracle before returning work.
    /// Await only that operation's commit, delivery, native completion or channel
    /// event. Producer loss must return an error; deadlines do not prove progress.
    fn operation(
        &mut self,
        operation: Operation,
        agent: u64,
    ) -> impl Future<Output = Result<Work, Failure>>;
    /// Sample after the create phase's completions, without a timed idle window.
    fn idle(&mut self) -> Result<Idle, Failure>;
    /// Release all original owners and await their explicit terminal events.
    /// Include this final cleanup in the archive accounting where appropriate.
    fn close(self) -> impl Future<Output = Result<(), Failure>>;
}

/// A metric's exact sum and worst completed operation, with missing coverage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Counter {
    /// Sum without division or rounding of completed-operation work.
    pub total: u128,
    /// Maximum over all agents; an isolated expensive operation cannot hide.
    pub maximum: u64,
    /// Completed operations for which this metric was unobserved.
    pub missing: u64,
}

/// One operation at one population; provisioning is measured too.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Sample {
    /// Actual completed operations; independent of the resident population.
    pub observations: u64,
    /// Operation identity.
    pub operation: Operation,
    /// Metrics in [`METRICS`] order.
    pub work: [Counter; METRICS.len()],
}

impl Sample {
    /// Prepare observer-owned storage outside the operation interval.
    pub fn new(operation: Operation) -> Self {
        Self {
            observations: 0,
            operation,
            work: [Counter::default(); METRICS.len()],
        }
    }

    /// Retain exact work and missing coverage for one completed operation.
    pub fn record(&mut self, work: Work, recovery: &str) -> Result<(), Failure> {
        self.observations = self
            .observations
            .checked_add(1)
            .ok_or_else(|| failure("observation count", "u64 exhausted".into(), recovery))?;
        for (counter, value) in self.work.iter_mut().zip(work.0) {
            match value {
                Some(value) => {
                    counter.total += u128::from(value);
                    counter.maximum = counter.maximum.max(value);
                }
                None => counter.missing += 1,
            }
        }
        Ok(())
    }
}

/// One independently provisioned population and its idle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Point {
    /// Population driven through every operation.
    pub agents: u64,
    /// Exactly one sample for every requested operation, in request order.
    pub samples: Vec<Sample>,
    /// Retained memory and prohibited per-agent resources.
    pub idle: Idle,
}

/// Every measured point, retained even when work grows or coverage is incomplete.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Report {
    /// Fresh populations in strictly increasing order.
    pub points: Vec<Point>,
}

/// A typed failure with the original step and an exact caller-supplied recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Failure {
    /// Failing operation, observer or oracle.
    pub step: String,
    /// Evidence; never substitute an inferred zero for a missing counter.
    pub evidence: String,
    /// Exact command which reruns the caller's selected check.
    pub recovery: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "STEP {} failed: {}; recovery: {}",
            self.step, self.evidence, self.recovery
        )
    }
}
impl Error for Failure {}

/// Drive every parent, including provisioning, through the requested operations.
/// Callers can use smaller vectors for fast proofs; only [`POPULATIONS`] qualifies
/// the full target. Storage is fresh at each point and released before the next.
pub async fn run<P, F, Fut>(
    sizes: &[u64],
    operations: &[Operation],
    mut fresh: F,
    recovery: &str,
) -> Result<Report, Failure>
where
    P: Population,
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<P, Failure>>,
{
    if sizes.is_empty() || sizes[0] == 0 || sizes.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(failure(
            "population vector",
            format!("require increasing positive sizes: {sizes:?}"),
            recovery,
        ));
    }
    let mut report = Report {
        points: Vec::with_capacity(sizes.len()),
    };
    for &agents in sizes {
        // Observer allocations precede the empty-host baseline taken by fresh.
        let mut point = Point {
            agents,
            samples: Vec::with_capacity(operations.len()),
            idle: Idle::default(),
        };
        let mut population = fresh(agents).await?;
        for &operation in operations {
            let mut sample = Sample::new(operation);
            for agent in 0..agents {
                let work = population.operation(operation, agent).await?;
                sample.record(work, recovery)?;
            }
            point.samples.push(sample);
            if operation == Operation::Create {
                point.idle = population.idle()?;
            }
        }
        population.close().await?;
        report.points.push(point);
    }
    Ok(report)
}

fn failure(step: &str, evidence: String, recovery: &str) -> Failure {
    Failure {
        step: step.into(),
        evidence,
        recovery: recovery.into(),
    }
}

impl Report {
    /// Reject missing coverage, per-agent threads/timers, growing mean work,
    /// growing worst-operation work, and growing net retained bytes per agent.
    /// Exact integer products avoid truncating a positive slope to zero.
    pub fn findings(&self, operations: &[Operation], recovery: &str) -> Vec<Failure> {
        let mut failures = Vec::new();
        if operations.is_empty()
            || operations
                .iter()
                .enumerate()
                .any(|(index, operation)| operations[..index].contains(operation))
            || self.points.is_empty()
            || self.points.iter().any(|point| point.agents == 0)
            || self
                .points
                .windows(2)
                .any(|pair| pair[0].agents >= pair[1].agents)
        {
            return vec![failure(
                "report shape",
                "require increasing positive populations".into(),
                recovery,
            )];
        }
        for point in &self.points {
            if point
                .samples
                .iter()
                .map(|sample| sample.operation)
                .ne(operations.iter().copied())
                || point.samples.iter().any(|sample| sample.observations == 0)
            {
                failures.push(failure(
                    "report shape",
                    format!("N={}: incomplete or mismatched operations", point.agents),
                    recovery,
                ));
            }
            for sample in &point.samples {
                for (name, counter) in METRICS.iter().zip(&sample.work) {
                    let observed = sample.observations.saturating_sub(counter.missing);
                    if counter.missing > sample.observations
                        || counter.total > u128::from(counter.maximum) * u128::from(observed)
                        || u128::from(counter.maximum) > counter.total
                    {
                        failures.push(failure(
                            "report shape",
                            format!(
                                "N={} {:?} {name}: inconsistent total/maximum/coverage",
                                point.agents, sample.operation
                            ),
                            recovery,
                        ));
                    }
                    if counter.missing != 0 {
                        failures.push(failure(
                            "counter coverage",
                            format!(
                                "N={} {:?} {name}: missing {}/{}",
                                point.agents,
                                sample.operation,
                                counter.missing,
                                sample.observations
                            ),
                            recovery,
                        ));
                    }
                }
            }
            for (name, value) in [
                ("idle bytes", point.idle.bytes),
                ("idle threads", point.idle.threads),
                ("idle timers", point.idle.timers),
            ] {
                match value {
                    None => failures.push(failure(
                        "idle coverage",
                        format!("N={} {name}: unobserved", point.agents),
                        recovery,
                    )),
                    Some(value) if name != "idle bytes" && value != 0 => failures.push(failure(
                        "idle resources",
                        format!("N={} {name}={value}; required=0", point.agents),
                        recovery,
                    )),
                    Some(_) => {}
                }
            }
        }
        for pair in self.points.windows(2) {
            compare(&pair[0], &pair[1], recovery, &mut failures);
        }
        failures
    }

    /// Qualify only the full population vector with complete flat observations.
    /// Exploratory smaller runs can call [`Self::findings`] directly.
    pub fn qualify(&self, operations: &[Operation], recovery: &str) -> Result<(), Failure> {
        if self.points.iter().map(|point| point.agents).ne(POPULATIONS) {
            return Err(failure(
                "population coverage",
                format!("required={POPULATIONS:?}"),
                recovery,
            ));
        }
        if let Some(failure) = self.findings(operations, recovery).into_iter().next() {
            return Err(failure);
        }
        Ok(())
    }
}

fn compare(small: &Point, large: &Point, recovery: &str, failures: &mut Vec<Failure>) {
    for (before, after) in small.samples.iter().zip(&large.samples) {
        let operation = before.operation;
        if before.operation != after.operation
            || before.observations == 0
            || after.observations == 0
        {
            continue;
        }
        let before_count = before.observations;
        let after_count = after.observations;
        for ((name, before), after) in METRICS.iter().zip(&before.work).zip(&after.work) {
            if before.missing != 0 || after.missing != 0 {
                continue;
            }
            if mean_grows(before.total, before_count, after.total, after_count)
                || after.maximum > before.maximum
            {
                failures.push(failure(
                    "constant work",
                    format!(
                        "{operation:?} {name}: N={} total={} max={}; N={} total={} max={}",
                        small.agents,
                        before.total,
                        before.maximum,
                        large.agents,
                        after.total,
                        after.maximum
                    ),
                    recovery,
                ));
            }
        }
    }
    if let (Some(before), Some(after)) = (small.idle.bytes, large.idle.bytes)
        && u128::from(after) * u128::from(small.agents)
            > u128::from(before) * u128::from(large.agents)
    {
        failures.push(failure(
            "constant idle memory",
            format!(
                "N={} bytes={before}; N={} bytes={after}",
                small.agents, large.agents
            ),
            recovery,
        ));
    }
}

// Quotients first; the remainder products fit u128 even for u64 populations.
fn mean_grows(before: u128, small: u64, after: u128, large: u64) -> bool {
    let small = u128::from(small);
    let large = u128::from(large);
    (after / large)
        .cmp(&(before / small))
        .then_with(|| ((after % large) * small).cmp(&((before % small) * large)))
        .is_gt()
}

#[cfg(test)]
mod tests;

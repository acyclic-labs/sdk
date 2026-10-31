use acyclic_stream::testing::{RequestClass, RequestLedger, Requests};
use acyclic_test_support::{
    AllocationInfo,
    scale::{Idle, Operation, Point, Sample, Work},
};
use serde::Serialize;
use std::{collections::BTreeMap, io::Write, time::Duration};

pub(super) type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Serialize)]
pub(super) struct Quantiles {
    pub(super) samples: usize,
    pub(super) p50_ns: f64,
    pub(super) p99_ns: f64,
}

impl Quantiles {
    pub(super) fn new(values: &mut [f64]) -> Result<Self> {
        if values.is_empty()
            || values
                .iter()
                .any(|value| !value.is_finite() || *value < 0.0)
        {
            return Err("quantiles require nonnegative finite samples".into());
        }
        values.sort_unstable_by(f64::total_cmp);
        let rank = |percent: usize| -> Result<f64> {
            let index = values
                .len()
                .checked_mul(percent)
                .ok_or("sample rank overflow")?
                .div_ceil(100)
                .saturating_sub(1);
            values
                .get(index)
                .copied()
                .ok_or_else(|| "sample rank missing".into())
        };
        Ok(Self {
            samples: values.len(),
            p50_ns: rank(50)?,
            p99_ns: rank(99)?,
        })
    }
}

pub(super) fn check_quantiles() -> Result {
    let mut values: Vec<_> = (1..=100).map(f64::from).rev().collect();
    let quantiles = Quantiles::new(&mut values)?;
    if quantiles.p50_ns != 50.0 || quantiles.p99_ns != 99.0 {
        return Err("nearest-rank quantiles changed".into());
    }
    // A timestamp pair can occupy one clock tick. Preserve that observation
    // rather than fabricating a positive elapsed duration.
    let zero = Quantiles::new(&mut [0.0; 100])?;
    if zero.p50_ns != 0.0 || zero.p99_ns != 0.0 {
        return Err("zero-duration observations were changed".into());
    }
    for mut invalid in [Vec::new(), vec![-1.0], vec![f64::NAN], vec![f64::INFINITY]] {
        if Quantiles::new(&mut invalid).is_ok() {
            return Err("invalid observations admitted by quantile negative control".into());
        }
    }
    emit(
        serde_json::json!({"schema": 1, "kind": "negative_control", "control": "invalid quantile observations", "result": "rejected"}),
    )
}

pub(super) fn ns(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e9
}

pub(super) fn emit(value: impl Serialize) -> Result {
    let mut output = std::io::stdout().lock();
    serde_json::to_writer(&mut output, &value)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

/// Coefficients of the semantic floor, independent of implementation overhead.
#[derive(Clone, Copy, Default, Serialize)]
pub(super) struct Terms {
    pub(super) flushes: u32,
    pub(super) wakes: u32,
    pub(super) calls: u32,
    pub(super) read_syscalls: u32,
    pub(super) encode_bytes: u32,
    pub(super) copy_bytes: u32,
    pub(super) hash_bytes: u32,
}

pub(super) struct Measurements {
    agents: usize,
    requests: RequestLedger,
    recovery: String,
    rows: BTreeMap<Operation, Row>,
    operations: Vec<Operation>,
}

struct Row {
    terms: Terms,
    raw_ns: Vec<f64>,
    work: Sample,
    coordinator_allocations: Vec<Option<[u64; 2]>>,
    provider_requests: Vec<Option<[u64; 7]>>,
}

pub(super) struct Before {
    requests: Requests,
    allocations: Option<AllocationInfo>,
    requested_query: Option<RequestClass>,
}

#[derive(Default)]
pub(super) struct Observation {
    work: Work,
    coordinator_allocations: Option<[u64; 2]>,
    provider_requests: Option<[u64; 7]>,
}

impl Measurements {
    pub(super) fn new(
        agents: usize,
        requests: RequestLedger,
        recovery: &str,
        operations: &[Operation],
    ) -> Self {
        Self {
            agents,
            requests,
            recovery: recovery.into(),
            rows: BTreeMap::new(),
            operations: operations.to_vec(),
        }
    }

    pub(super) fn includes(&self, operation: Operation) -> bool {
        self.operations.contains(&operation)
    }

    pub(super) fn start(&self, requested_query: Option<RequestClass>) -> Before {
        Before {
            requests: self.requests.requests(),
            allocations: acyclic_test_support::current_allocations(),
            requested_query,
        }
    }

    pub(super) fn stop(&self, before: Before) -> Result<Observation> {
        let requests = self.requests.requests().since(before.requests);
        // A requested query supplies the operation's data. Exclude exactly
        // that original dispatch, preserving every additional probe, even
        // another call in the same class.
        let direct = if let Some(class) = before.requested_query {
            if !matches!(class, RequestClass::Head | RequestClass::List)
                || requests.class(class) == 0
            {
                return Err("requested query has no original provider dispatch".into());
            }
            1
        } else {
            0
        };
        let control = requests
            .control()
            .checked_sub(direct)
            .ok_or("requested query exceeds observed provider calls")?;
        let allocations = before
            .allocations
            .zip(acyclic_test_support::current_allocations());
        Ok(Observation {
            // Request accounting observes the public provider boundary. It
            // does not infer physical syscalls, locks or transport traffic.
            work: Work([None, None, None, None, Some(control), None, None]),
            // This allocator scope cannot observe native worker threads; keep
            // it diagnostic instead of qualifying the allocation dimension.
            coordinator_allocations: allocations.map(|(before, after)| {
                [
                    after.count_total - before.count_total,
                    after.bytes_total - before.bytes_total,
                ]
            }),
            provider_requests: Some([
                requests.data(),
                requests.class(RequestClass::Metadata),
                requests.class(RequestClass::List),
                requests.class(RequestClass::Head),
                requests.class(RequestClass::Lease),
                requests.class(RequestClass::Token),
                direct,
            ]),
        })
    }

    pub(super) fn require_one_control(observation: &Observation) -> Result {
        if observation.work.0[4] != Some(1) {
            return Err("redundant query was hidden by requested-data classification".into());
        }
        Ok(())
    }

    pub(super) fn record(
        &mut self,
        operation: Operation,
        floor: Terms,
        elapsed: Duration,
        observation: Observation,
    ) -> Result {
        if !self.includes(operation) {
            return Ok(());
        }
        let row = self.rows.entry(operation).or_insert_with(|| Row {
            terms: floor,
            raw_ns: Vec::new(),
            work: Sample::new(operation),
            coordinator_allocations: Vec::new(),
            provider_requests: Vec::new(),
        });
        row.raw_ns.push(ns(elapsed));
        row.work.record(observation.work, &self.recovery)?;
        row.coordinator_allocations
            .push(observation.coordinator_allocations);
        row.provider_requests.push(observation.provider_requests);
        Ok(())
    }

    pub(super) fn finish(mut self, floors: &super::calibrate::Floors) -> Result<(bool, Point)> {
        let mut complete = true;
        let mut point = Point {
            agents: u64::try_from(self.agents)?,
            samples: Vec::with_capacity(self.operations.len()),
            // Original physical retained-resource observers are required.
            idle: Idle::default(),
        };
        for operation in self.operations {
            let Some(row) = self.rows.remove(&operation) else {
                complete = false;
                let reason = match operation {
                    Operation::WorkerCold | Operation::WorkerWarm => {
                        "native Worker execution producer is absent from this composition"
                    }
                    _ => "the operation adapter did not supply observations",
                };
                emit(serde_json::json!({
                    "schema": 1, "kind": "refused", "operation": operation.name(), "agents": self.agents,
                    "reason": reason,
                    "recovery": self.recovery,
                }))?;
                continue;
            };
            let Row {
                terms,
                raw_ns,
                work,
                coordinator_allocations,
                provider_requests,
            } = row;
            let quantiles = Quantiles::new(&mut raw_ns.clone())?;
            let floor_ns = floors.compose(terms);
            let timer = floors.timer();
            emit(serde_json::json!({
                "schema": 1, "kind": "operation", "operation": operation.name(),
                "measurement_label": match operation {
                    Operation::ActorWake => "event-drain-to-handler-entry",
                    Operation::ActorWarmRead => "borrowed-resident-read",
                    _ => operation.name(),
                },
                "agents": self.agents, "active": 1, "quantiles": quantiles,
                "floor": terms, "floor_ns": floor_ns,
                "p50_ratio": quantiles.p50_ns / floor_ns,
                "p99_ratio": quantiles.p99_ns / floor_ns,
                "p50_pass": quantiles.p50_ns <= floor_ns * 1.5,
                "p99_pass": quantiles.p99_ns <= floor_ns * 3.0,
                "timer_only_quantiles": timer,
                "minimum_positive_timer_ns": floors.minimum_positive_timer_ns(),
                "p50_resolution_limited": floor_ns * 1.5 < timer.p50_ns.max(floors.minimum_positive_timer_ns()),
                "p99_resolution_limited": floor_ns * 3.0 < timer.p99_ns.max(floors.minimum_positive_timer_ns()),
                "resolution_scope": "timestamp pair only; async polling and other instrumentation add cost; no subtraction",
                "raw_ns": raw_ns,
                "work_observations": work.observations,
                "work": acyclic_test_support::scale::METRICS.iter().zip(&work.work).map(|(metric, counter)| {
                    (*metric, serde_json::json!({"total": counter.total.to_string(), "maximum": counter.maximum, "missing": counter.missing}))
                }).collect::<BTreeMap<_, _>>(),
                "coordinator_allocations_scope": "current thread only; excluded from qualification",
                "coordinator_allocations_count_and_bytes": coordinator_allocations,
                "provider_request_columns": ["data", "metadata", "list", "head", "lease", "token", "requested_query_dispatches"],
                "provider_request_counts": provider_requests,
                "control_request_scope": "public metadata/list/head/lease/token calls excluding exactly the original requested query; additional probes remain controls",
            }))?;
            point.samples.push(work);
        }
        if !self.rows.is_empty() {
            return Err("unknown core operation in measurement report".into());
        }
        Ok((complete, point))
    }
}

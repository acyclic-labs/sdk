use acyclic_stream::testing::{RequestLedger, Requests};
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
                .any(|value| !value.is_finite() || *value <= 0.0)
        {
            return Err("quantiles require positive finite samples".into());
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
    for mut invalid in [Vec::new(), vec![0.0], vec![f64::NAN], vec![f64::INFINITY]] {
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
}

pub(super) struct Before {
    requests: Requests,
    allocations: Option<AllocationInfo>,
}

#[derive(Default)]
pub(super) struct Observation {
    work: Work,
    coordinator_allocations: Option<[u64; 2]>,
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

    pub(super) fn start(&self) -> Before {
        Before {
            requests: self.requests.requests(),
            allocations: acyclic_test_support::current_allocations(),
        }
    }

    pub(super) fn stop(&self, before: Before) -> Observation {
        let requests = self.requests.requests().since(before.requests);
        let allocations = before
            .allocations
            .zip(acyclic_test_support::current_allocations());
        Observation {
            // Request accounting observes the public provider boundary. It
            // does not infer physical syscalls, locks or transport traffic.
            work: Work([None, None, None, None, Some(requests.control()), None, None]),
            // This allocator scope cannot observe native worker threads; keep
            // it diagnostic instead of qualifying the allocation dimension.
            coordinator_allocations: allocations.map(|(before, after)| {
                [
                    after.count_total - before.count_total,
                    after.bytes_total - before.bytes_total,
                ]
            }),
        }
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
        });
        row.raw_ns.push(ns(elapsed));
        row.work.record(observation.work, &self.recovery)?;
        row.coordinator_allocations
            .push(observation.coordinator_allocations);
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
            } = row;
            let quantiles = Quantiles::new(&mut raw_ns.clone())?;
            let floor_ns = floors.compose(terms);
            emit(serde_json::json!({
                "schema": 1, "kind": "operation", "operation": operation.name(),
                "measurement_label": if operation == Operation::ActorWake { "event-drain-to-handler-entry" } else { operation.name() },
                "agents": self.agents, "active": 1, "quantiles": quantiles,
                "floor": terms, "floor_ns": floor_ns,
                "p50_ratio": quantiles.p50_ns / floor_ns,
                "p99_ratio": quantiles.p99_ns / floor_ns,
                "p50_pass": quantiles.p50_ns <= floor_ns * 1.5,
                "p99_pass": quantiles.p99_ns <= floor_ns * 3.0,
                "raw_ns": raw_ns,
                "work_observations": work.observations,
                "work": acyclic_test_support::scale::METRICS.iter().zip(&work.work).map(|(metric, counter)| {
                    (*metric, serde_json::json!({"total": counter.total.to_string(), "maximum": counter.maximum, "missing": counter.missing}))
                }).collect::<BTreeMap<_, _>>(),
                "coordinator_allocations_scope": "current thread only; excluded from qualification",
                "coordinator_allocations_count_and_bytes": coordinator_allocations,
            }))?;
            point.samples.push(work);
        }
        if !self.rows.is_empty() {
            return Err("unknown core operation in measurement report".into());
        }
        Ok((complete, point))
    }
}

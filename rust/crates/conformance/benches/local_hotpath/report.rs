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
    rows: BTreeMap<&'static str, (Terms, Vec<f64>)>,
}

const CORE_OPERATIONS: [&str; 17] = [
    "actors.create",
    "actors.wake",
    "actors.durable_write",
    "actors.warm_read",
    "objects.put",
    "objects.get",
    "objects.head",
    "objects.list",
    "streams.append",
    "streams.read",
    "streams.follow",
    "workers.cold",
    "workers.warm",
    "fs.pinned_read",
    "fs.staged_write",
    "fs.fsync",
    "swarm.session_start",
];

impl Measurements {
    pub(super) fn new(agents: usize) -> Self {
        Self {
            agents,
            rows: BTreeMap::new(),
        }
    }

    pub(super) fn record(&mut self, operation: &'static str, floor: Terms, elapsed: Duration) {
        self.rows
            .entry(operation)
            .or_insert_with(|| (floor, Vec::new()))
            .1
            .push(ns(elapsed));
    }

    pub(super) fn finish(mut self, floors: &super::calibrate::Floors) -> Result<bool> {
        let mut complete = true;
        for operation in CORE_OPERATIONS {
            let Some((terms, raw_ns)) = self.rows.remove(operation) else {
                complete = false;
                let reason = match operation {
                    "actors.wake" => {
                        "Runtime::wake reports queue admission; completed delivery needs its original producer event"
                    }
                    "workers.cold" | "workers.warm" => {
                        "native Worker execution producer is absent from this composition"
                    }
                    _ => "the operation adapter did not supply observations",
                };
                emit(serde_json::json!({
                    "schema": 1, "kind": "refused", "operation": operation, "agents": self.agents,
                    "reason": reason,
                    "recovery": "supply the original execution/completion producer and rerun local_hotpath",
                }))?;
                continue;
            };
            let quantiles = Quantiles::new(&mut raw_ns.clone())?;
            let floor_ns = floors.compose(terms);
            emit(serde_json::json!({
                "schema": 1, "kind": "operation", "operation": operation,
                "agents": self.agents, "active": 1, "quantiles": quantiles,
                "floor": terms, "floor_ns": floor_ns,
                "p50_ratio": quantiles.p50_ns / floor_ns,
                "p99_ratio": quantiles.p99_ns / floor_ns,
                "p50_pass": quantiles.p50_ns <= floor_ns * 1.5,
                "p99_pass": quantiles.p99_ns <= floor_ns * 3.0,
                "raw_ns": raw_ns,
            }))?;
        }
        if !self.rows.is_empty() {
            return Err("unknown core operation in measurement report".into());
        }
        Ok(complete)
    }
}

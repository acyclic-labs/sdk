use acyclic_stream::testing::{RequestClass, RequestLedger, Requests};
use acyclic_test_support::{
    AllocationInfo,
    scale::{Counter, Idle, METRICS, Operation, Point, Sample, Work},
};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    sync::Arc,
    time::Duration,
};

pub(super) type DurabilityQuery = Option<Arc<acyclic_native_runtime::obs::Query>>;

pub(super) type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Stream the original observations without a second JSON array allocation.
#[derive(Serialize)]
pub(super) struct RawSamples<'a, T = serde_json::Value> {
    #[serde(flatten)]
    pub(super) fields: T,
    pub(super) raw_ns: &'a [f64],
}

#[derive(Serialize)]
struct OperationEvidence<'a> {
    #[serde(flatten)]
    fields: serde_json::Value,
    #[serde(serialize_with = "serialize_work_receipts")]
    original_work_receipts: &'a [RetainedObservation],
    #[serde(serialize_with = "serialize_allocations")]
    coordinator_allocations_count_and_bytes: &'a [RetainedObservation],
    #[serde(serialize_with = "serialize_requests")]
    provider_request_counts: &'a [RetainedObservation],
    #[serde(serialize_with = "serialize_attempts")]
    native_attempt_counts: &'a [RetainedObservation],
    #[serde(serialize_with = "serialize_work_summary")]
    work: &'a [Counter; METRICS.len()],
}

fn serialize_work_summary<S: serde::Serializer>(
    counters: &[Counter; METRICS.len()],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    #[derive(Serialize)]
    struct Summary<'a> {
        #[serde(serialize_with = "serialize_total")]
        total: &'a u128,
        maximum: u64,
        missing: u64,
    }

    fn serialize_total<S: serde::Serializer>(
        total: &u128,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(total)
    }

    serializer.collect_map(METRICS.iter().zip(counters).map(|(metric, counter)| {
        (
            metric,
            Summary {
                total: &counter.total,
                maximum: counter.maximum,
                missing: counter.missing,
            },
        )
    }))
}

fn serialize_work_receipts<S: serde::Serializer>(
    receipts: &[RetainedObservation],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    #[derive(Serialize)]
    struct Receipt<'a> {
        sample: usize,
        identity: &'a serde_json::Value,
        work: &'a [Option<u64>],
        physical_admission_binding: Option<()>,
        progress: &'static str,
    }

    serializer.collect_seq(receipts.iter().enumerate().map(|(sample, retained)| {
        Receipt {
            sample,
            identity: &retained.identity,
            work: &retained.observation.work.0,
            physical_admission_binding: None,
            progress: "original operation returned successfully; result oracles are complete only when cohort_completed is true",
        }
    }))
}

fn serialize_allocations<S: serde::Serializer>(
    observations: &[RetainedObservation],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.collect_seq(
        observations
            .iter()
            .map(|sample| &sample.observation.coordinator_allocations),
    )
}

fn serialize_requests<S: serde::Serializer>(
    observations: &[RetainedObservation],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.collect_seq(
        observations
            .iter()
            .map(|sample| &sample.observation.provider_requests),
    )
}

fn serialize_attempts<S: serde::Serializer>(
    observations: &[RetainedObservation],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.collect_seq(
        observations
            .iter()
            .map(|sample| &sample.observation.native_attempts),
    )
}

#[derive(Clone, Copy, Serialize)]
pub(super) struct Quantiles {
    pub(super) samples: usize,
    pub(super) p50_ns: f64,
    pub(super) p99_ns: f64,
}

impl Quantiles {
    pub(super) fn new(values: &mut [f64]) -> Result<Self> {
        Self::with_order(values, f64::total_cmp)
    }

    fn with_order(
        values: &mut [f64],
        mut compare: impl FnMut(&f64, &f64) -> std::cmp::Ordering,
    ) -> Result<Self> {
        if values.is_empty()
            || values
                .iter()
                .any(|value| !value.is_finite() || *value < 0.0)
        {
            return Err("quantiles require nonnegative finite samples".into());
        }
        let rank = |percent: usize| -> Result<usize> {
            Ok(values
                .len()
                .checked_mul(percent)
                .ok_or("sample rank overflow")?
                .div_ceil(100)
                .saturating_sub(1))
        };
        let samples = values.len();
        let (median, tail) = (rank(50)?, rank(99)?);
        // Partition only the requested ranks. The lower partition still
        // contains the median; no complete sample ordering is required.
        let (lower, tail_value, _) = values.select_nth_unstable_by(tail, &mut compare);
        let p99_ns = *tail_value;
        let p50_ns = if median == tail {
            p99_ns
        } else {
            *lower.select_nth_unstable_by(median, compare).1
        };
        Ok(Self {
            samples,
            p50_ns,
            p99_ns,
        })
    }
}

pub(super) fn check_observations() -> Result {
    let mut values: Vec<_> = (1..=100).map(f64::from).rev().collect();
    let quantiles = Quantiles::new(&mut values)?;
    if quantiles.p50_ns != 50.0 || quantiles.p99_ns != 99.0 {
        return Err("nearest-rank quantiles changed".into());
    }
    let small = Quantiles::new(&mut [5.0, 1.0, 4.0, 2.0, 3.0])?;
    if small.samples != 5 || small.p50_ns != 3.0 || small.p99_ns != 5.0 {
        return Err("small-cohort quantiles changed".into());
    }
    let singleton = Quantiles::new(&mut [7.0])?;
    if singleton.samples != 1 || singleton.p50_ns != 7.0 || singleton.p99_ns != 7.0 {
        return Err("coincident quantile ranks changed".into());
    }
    // Deterministic shuffled observations, independent of clock resolution.
    let original: Vec<_> = (0..4096)
        .map(|index| f64::from((index * 251) % 4096))
        .collect();
    let mut selected = original.clone();
    let mut selection_comparisons = 0;
    let mut result = None;
    let allocations = acyclic_test_support::measure(|| {
        result = Some(Quantiles::with_order(&mut selected, |left, right| {
            selection_comparisons += 1;
            left.total_cmp(right)
        }));
    });
    let selected = result.ok_or("quantile selection did not complete")??;
    let mut sorted = original;
    let mut sort_comparisons = 0;
    // Restore the former full sort as an independent work negative.
    sorted.sort_unstable_by(|left, right| {
        sort_comparisons += 1;
        left.total_cmp(right)
    });
    if selected.samples != sorted.len()
        || selected.p50_ns != sorted[2047]
        || selected.p99_ns != sorted[4055]
        || allocations.count_total != 0
        || allocations.bytes_total != 0
        || selection_comparisons >= sort_comparisons
    {
        return Err("quantile selection changed ranks or retained full-sort work".into());
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
    let mut cohort = Measurements::new(
        1,
        RequestLedger::default(),
        None,
        "use samples with identical floor terms",
        &[Operation::ObjectGet],
    );
    let mut record = |bytes, observation| {
        let identity = serde_json::json!({
            "request": {"operation": "object.get", "requested_copy_bytes": bytes},
            "resource": {"path": "original/requested/object"},
        });
        let mut result = Ok(());
        let allocations = acyclic_test_support::measure(|| {
            result = cohort.record(
                Operation::ObjectGet,
                Terms {
                    copy_bytes: bytes,
                    ..Terms::default()
                },
                Duration::ZERO,
                observation,
                identity,
            );
        });
        result.map(|()| allocations)
    };
    let first = record(
        1024,
        Observation {
            work: Work([Some(u64::MAX), Some(0), None, None, None, None, None, None]),
            coordinator_allocations: Some([2, 1024]),
            provider_requests: Some([1, 0, 0, 0, 0, 0, 0]),
            native_attempts: Some([1, 0, 0]),
        },
    )?;
    // One row node, its timestamp buffer and its original observation buffer.
    // Reintroducing separate diagnostic buffers adds three allocations here.
    if first.count_total != 3 {
        return Err("original sample retention added independent backing allocations".into());
    }
    if record(2048, Observation::default()).is_ok() {
        return Err("different required work was admitted into one latency cohort".into());
    }
    record(
        1024,
        Observation {
            work: Work([Some(u64::MAX), None, None, None, None, None, None, None]),
            ..Observation::default()
        },
    )?;
    if cohort.rows[&Operation::ObjectGet].raw_ns.len() != 2 {
        return Err("refused floor terms changed the original latency cohort".into());
    }
    let row = cohort
        .rows
        .get_mut(&Operation::ObjectGet)
        .ok_or("original observation row missing")?;
    row.work.observations = u64::MAX;
    let full = row.work.clone();
    if cohort
        .record(
            Operation::ObjectGet,
            Terms {
                copy_bytes: 1024,
                ..Terms::default()
            },
            Duration::ZERO,
            Observation::default(),
            serde_json::Value::Null,
        )
        .is_ok()
    {
        return Err("exhausted observation count admitted another sample".into());
    }
    let row = &cohort.rows[&Operation::ObjectGet];
    if row.work != full || [row.raw_ns.len(), row.observations.len()] != [2; 2] {
        return Err("refused observation changed the retained evidence".into());
    }
    // Restore the three former diagnostic buffers from the same retained
    // observations, independently of the serializer under test.
    let copied = acyclic_test_support::measure(|| {
        std::hint::black_box((
            row.observations
                .iter()
                .map(|sample| sample.observation.coordinator_allocations)
                .collect::<Vec<_>>(),
            row.observations
                .iter()
                .map(|sample| sample.observation.provider_requests)
                .collect::<Vec<_>>(),
            row.observations
                .iter()
                .map(|sample| sample.observation.native_attempts)
                .collect::<Vec<_>>(),
        ));
    });
    if copied.count_total != 3 || copied.bytes_total == 0 {
        return Err("separate diagnostic buffer negative did not retain three allocations".into());
    }
    let evidence = row.evidence(serde_json::json!({"schema": 1, "kind": "negative_control"}));
    check_borrowed_serialization(&evidence, "operation evidence")?;
    let encoded = serde_json::to_value(&evidence)?;
    // Compare against the former independent map builder, outside measurement.
    let original_summary: BTreeMap<_, _> = METRICS
        .iter()
        .zip(&row.work.work)
        .map(|(metric, counter)| {
            (
                *metric,
                serde_json::json!({
                    "total": counter.total.to_string(),
                    "maximum": counter.maximum,
                    "missing": counter.missing,
                }),
            )
        })
        .collect();
    if encoded["work"] != serde_json::to_value(original_summary)?
        || encoded["work"]["syscalls"]["total"] != "36893488147419103230"
        || encoded["work"]["allocations"]["maximum"] != 0
        || encoded["work"]["allocations"]["missing"] != 1
        || encoded["work"]["locks"]["missing"] != 2
    {
        return Err("operation encoding changed exact work totals or missing coverage".into());
    }
    if encoded["coordinator_allocations_count_and_bytes"] != serde_json::json!([[2, 1024], null])
        || encoded["provider_request_counts"] != serde_json::json!([[1, 0, 0, 0, 0, 0, 0], null])
        || encoded["native_attempt_counts"] != serde_json::json!([[1, 0, 0], null])
        || encoded["original_work_receipts"][0]["identity"] != row.observations[0].identity
        || encoded["original_work_receipts"][1]["identity"] != row.observations[1].identity
    {
        return Err("operation encoding changed original diagnostic evidence".into());
    }
    check_borrowed_serialization(
        RawSamples {
            fields: serde_json::json!({"schema": 1, "kind": "negative_control"}),
            raw_ns: &[5.0, 1.0, 4.0, 2.0, 3.0],
        },
        "raw samples",
    )?;
    // Capture a real operation failure. Neither a failed terminal nor a
    // sampler refusal may enter the successful cohort or replace its cause.
    let directory = tempfile::tempdir()?;
    let missing_path = directory.path().join("missing");
    let failed_identity = serde_json::json!({"path": missing_path});
    let missing = std::fs::read(&missing_path).unwrap_err();
    let original_code = missing.raw_os_error();
    let before = cohort.start(None);
    let returned = cohort
        .record_terminal(
            Operation::ObjectGet,
            Terms::default(),
            (Duration::ZERO, before),
            failed_identity.clone(),
            Err::<(), _>(missing),
        )?
        .unwrap_err();
    if returned.raw_os_error() != original_code {
        return Err("failed observation replaced the original OS cause".into());
    }
    let before = cohort.start(Some(RequestClass::Head));
    let returned = cohort
        .record_terminal(
            Operation::ObjectGet,
            Terms::default(),
            (Duration::ZERO, before),
            failed_identity.clone(),
            Err::<(), _>(returned),
        )?
        .unwrap_err();
    if returned.raw_os_error() != original_code {
        return Err("sampler refusal replaced the original operation cause".into());
    }
    let before = cohort.start(Some(RequestClass::Head));
    if cohort
        .record_terminal(
            Operation::ObjectGet,
            Terms::default(),
            (Duration::ZERO, before),
            failed_identity,
            Ok::<_, std::io::Error>(()),
        )
        .is_ok()
    {
        return Err("successful operation hid its sampler refusal".into());
    }
    let row = &cohort.rows[&Operation::ObjectGet];
    if row.work != full || [row.raw_ns.len(), row.observations.len()] != [2; 2] {
        return Err("failed terminal changed the successful observation inventory".into());
    }
    let mut partial = Vec::new();
    let original = super::durability::retain_diagnostics(
        Err::<(), _>(returned),
        || cohort.publish_partial(&mut partial),
        &cohort.recovery,
    )?
    .unwrap_err();
    let partial: serde_json::Value = serde_json::from_slice(&partial)?;
    if original.raw_os_error() != original_code
        || partial["kind"] != "partial_operation"
        || partial["cohort_completed"] != false
        || partial["ratio_qualified"] != false
        || partial.get("quantiles").is_some()
        || partial.get("floor_ns").is_some()
        || partial["raw_ns"].as_array().map(Vec::len) != Some(2)
        || partial["original_work_receipts"] != encoded["original_work_receipts"]
    {
        return Err("family failure lost or qualified its original partial receipts".into());
    }
    emit(serde_json::json!({
        "schema": 1, "kind": "negative_control",
        "control": "invalid quantile observations and mismatched floor terms", "result": "rejected",
        "rank_comparisons": {"selection": selection_comparisons, "full_sort_negative": sort_comparisons},
    }))
}

fn check_borrowed_serialization(evidence: impl Serialize, kind: &str) -> Result {
    let mut encoded = Ok(());
    let borrowed = acyclic_test_support::measure(|| {
        encoded = serde_json::to_writer(std::io::sink(), &evidence);
    });
    encoded?;
    if borrowed.count_total != 0 || borrowed.bytes_total != 0 {
        return Err(format!("{kind} direct encoding allocated").into());
    }
    // Restore the former full JSON materialization around the same evidence.
    let mut encoded = Ok(());
    let copied = acyclic_test_support::measure(|| {
        encoded = serde_json::to_value(&evidence)
            .and_then(|value| serde_json::to_writer(std::io::sink(), &value));
    });
    encoded?;
    if copied.count_total == 0 || copied.bytes_total == 0 {
        return Err(format!("{kind} materialization negative did not allocate").into());
    }
    Ok(())
}

pub(super) fn ns(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e9
}

pub(super) fn emit(value: impl Serialize) -> Result {
    let mut output = std::io::stdout().lock();
    emit_to(&mut output, value)
}

fn emit_to(output: &mut impl Write, value: impl Serialize) -> Result {
    serde_json::to_writer(&mut *output, &value)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

pub(super) fn executable_sha256() -> Result<String> {
    use sha2::{Digest as _, Sha256};
    let mut executable = std::fs::File::open(std::env::current_exe()?)?;
    let mut digest = Sha256::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let count = match executable.read(&mut chunk) {
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        };
        if count == 0 {
            break;
        }
        digest.update(&chunk[..count]);
    }
    Ok(hex::encode(digest.finalize()))
}

/// Coefficients of the semantic floor, independent of implementation overhead.
#[derive(Clone, Copy, Default, Eq, PartialEq, Serialize)]
pub(super) struct Terms {
    pub(super) flushes: u32,
    pub(super) wakes: u32,
    pub(super) calls: u32,
    pub(super) read_syscalls: u32,
    pub(super) encode_bytes: u32,
    pub(super) copy_bytes: u32,
    pub(super) hash_bytes: u32,
}

fn guarantee(operation: Operation) -> &'static str {
    match operation {
        Operation::ActorCreate => "publish the requested actor's initial state durably",
        Operation::ActorWake => {
            "enter the original actor handler for its admitted epoch and exact input cursor"
        }
        Operation::ActorDurableWrite => {
            "publish metadata, checkpoint and consumed input cursor in one conditional commit"
        }
        Operation::ActorWarmRead => {
            "borrow the value from the same admitted resident state, retaining its fence checks"
        }
        Operation::ObjectPut => {
            "store bytes under their verified digest; crash survival belongs to the separate flush"
        }
        Operation::ObjectGet => "return the requested digest's bounded body range",
        Operation::ObjectHead => "return metadata for the requested digest",
        Operation::ObjectList => "return the requested bounded digest page",
        Operation::StreamAppend => {
            "durably publish the original keyed append and its exact record range"
        }
        Operation::StreamRead => "return the original bounded committed record range",
        Operation::StreamFollow => {
            "deliver the original committed record to its already parked follower"
        }
        Operation::WorkerCold => "start the actual native worker and complete its original call",
        Operation::WorkerWarm => {
            "complete the original call on the admitted resident native worker"
        }
        Operation::PinnedRead => "return the file body from the original immutable generation",
        Operation::StagedWrite => {
            "stage the original transaction's file update without publishing it"
        }
        Operation::Fsync => "durably publish the original transaction under its publication policy",
        Operation::SessionStart => {
            "create the requested root through its original host intent and volume binding"
        }
        _ => "composed lifecycle operation outside the primitive floor model",
    }
}

fn floor_units(terms: Terms) -> serde_json::Value {
    serde_json::json!({
        "scope": "semantic model coefficients, not observed work counts; creation and in-place flush floors are reported separately",
        "F_flush": {"units": terms.flushes, "guarantee": "crash survival at the original commit frontier; physical group and directory coverage must be established separately"},
        "W_wake": {"units": terms.wakes, "guarantee": "deliver the original producer's event to its waiting consumer"},
        "C_call": {"units": terms.calls, "guarantee": "enter the original operation"},
        "R_sys": {"units": terms.read_syscalls, "guarantee": "retrieve bytes from the original descriptor"},
        "B_copy": {"units": terms.copy_bytes, "guarantee": "transfer the original returned or published bytes"},
        "B_encode": {"units": terms.encode_bytes, "guarantee": "encode the original publication bytes; modeled with calibrated copy throughput"},
        "B_hash": {"units": terms.hash_bytes, "guarantee": "bind the original bytes to their content identity"},
    })
}

pub(super) struct Measurements {
    agents: usize,
    requests: RequestLedger,
    durability: DurabilityQuery,
    recovery: String,
    rows: BTreeMap<Operation, Row>,
    operations: Vec<Operation>,
}

struct Row {
    terms: Terms,
    raw_ns: Vec<f64>,
    work: Sample,
    observations: Vec<RetainedObservation>,
}

struct RetainedObservation {
    identity: serde_json::Value,
    observation: Observation,
}

impl Row {
    fn evidence(&self, fields: serde_json::Value) -> OperationEvidence<'_> {
        OperationEvidence {
            fields,
            original_work_receipts: &self.observations,
            coordinator_allocations_count_and_bytes: &self.observations,
            provider_request_counts: &self.observations,
            native_attempt_counts: &self.observations,
            work: &self.work.work,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Counters {
    requests: Requests,
    allocations: Option<[AllocationInfo; 2]>,
    native_attempts: Option<[u64; 3]>,
}

impl Counters {
    pub(super) fn capture(requests: &RequestLedger, durability: &DurabilityQuery) -> Self {
        // Both allocation cuts exclude this observer boundary, including the
        // handler-entry prefix. Outcome totals borrow the original cohort.
        let allocations_before = acyclic_test_support::current_allocations();
        let native_attempts = durability.as_ref().and_then(|query| query.outcome_counts());
        Self {
            requests: requests.requests(),
            allocations: allocations_before
                .zip(acyclic_test_support::current_allocations())
                .map(|(before, after)| [before, after]),
            native_attempts,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct Before {
    counters: Counters,
    requested_query: Option<RequestClass>,
}

#[derive(Clone, Copy, Default)]
pub(super) struct Observation {
    work: Work,
    coordinator_allocations: Option<[u64; 2]>,
    provider_requests: Option<[u64; 7]>,
    native_attempts: Option<[u64; 3]>,
}

impl Measurements {
    pub(super) fn new(
        agents: usize,
        requests: RequestLedger,
        durability: DurabilityQuery,
        recovery: &str,
        operations: &[Operation],
    ) -> Self {
        Self {
            agents,
            requests,
            durability,
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
            counters: Counters::capture(&self.requests, &self.durability),
            requested_query,
        }
    }

    pub(super) fn stop(&self, before: Before) -> Result<Observation> {
        Self::stop_at(before, Counters::capture(&self.requests, &self.durability))
    }

    pub(super) fn stop_at(before: Before, after: Counters) -> Result<Observation> {
        let requests = after.requests.since(before.counters.requests);
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
            .auxiliary(Some(RequestClass::Data))
            .and_then(|count| count.checked_sub(direct));
        let allocations = before.counters.allocations.zip(after.allocations);
        let native_attempts = before
            .counters
            .native_attempts
            .zip(after.native_attempts)
            .map(|(before, after)| -> Result<[u64; 3]> {
                let mut counts = [0; 3];
                for index in 0..counts.len() {
                    counts[index] = after[index]
                        .checked_sub(before[index])
                        .ok_or("native durability observer changed cohorts within an interval")?;
                }
                Ok(counts)
            })
            .transpose()?;
        Ok(Observation {
            // Request accounting observes the public provider boundary. It
            // does not infer physical syscalls, locks or transport traffic.
            work: Work([None, None, None, None, control, None, None, None]),
            // This allocator scope cannot observe native worker threads; keep
            // it diagnostic instead of qualifying the allocation dimension.
            coordinator_allocations: allocations.map(|(before, after)| {
                let (before, after) = (before[1], after[0]);
                [
                    after.count_total - before.count_total,
                    after.bytes_total - before.bytes_total,
                ]
            }),
            provider_requests: requests.auxiliary(Some(RequestClass::Data)).map(|_| {
                [
                    requests.data(),
                    requests.class(RequestClass::Metadata),
                    requests.class(RequestClass::List),
                    requests.class(RequestClass::Head),
                    requests.class(RequestClass::Lease),
                    requests.class(RequestClass::Token),
                    direct,
                ]
            }),
            native_attempts,
        })
    }

    pub(super) fn require_one_control(observation: &Observation) -> Result {
        if observation.work.0[4] != Some(1) {
            return Err("redundant query was hidden by requested-data classification".into());
        }
        Ok(())
    }

    pub(super) fn require_entry_prefix(prefix: &Observation, completion: &Observation) -> Result {
        if !prefix
            .provider_requests
            .zip(completion.provider_requests)
            .is_some_and(|(prefix, completion)| prefix[0] < completion[0])
        {
            return Err("handler-entry accounting included its later data publication".into());
        }
        Ok(())
    }

    pub(super) fn record(
        &mut self,
        operation: Operation,
        floor: Terms,
        elapsed: Duration,
        observation: Observation,
        identity: serde_json::Value,
    ) -> Result {
        if !self.includes(operation) {
            return Ok(());
        }
        let row = self.rows.entry(operation).or_insert_with(|| Row {
            terms: floor,
            raw_ns: Vec::new(),
            work: Sample::new(operation),
            observations: Vec::new(),
        });
        if row.terms != floor {
            return Err(format!("{} samples have different floor terms", operation.name()).into());
        }
        row.work.record(observation.work, &self.recovery)?;
        row.raw_ns.push(ns(elapsed));
        row.observations.push(RetainedObservation {
            identity,
            observation,
        });
        Ok(())
    }

    /// Keep failed terminals outside successful latency cohorts while retaining
    /// their original identity, observed work and error. Sampler failure must
    /// not replace a different error type returned by the operation.
    pub(super) fn record_terminal<T, E: std::fmt::Display>(
        &mut self,
        operation: Operation,
        floor: Terms,
        interval: (Duration, Before),
        identity: serde_json::Value,
        result: std::result::Result<T, E>,
    ) -> Result<std::result::Result<T, E>> {
        let (elapsed, before) = interval;
        let observed = self.stop(before);
        let diagnostic = if let Err(error) = &result {
            let observation = observed.as_ref().ok();
            emit(serde_json::json!({
                "schema": 1, "kind": "failed_operation", "operation": operation.name(),
                "agents": self.agents, "identity": identity, "raw_ns": ns(elapsed),
                "error": error.to_string(),
                "observation_error": observed.as_ref().err().map(ToString::to_string),
                "work_columns": METRICS, "work": observation.map(|sample| sample.work.0),
                "coordinator_allocations_count_and_bytes": observation.and_then(|sample| sample.coordinator_allocations),
                "provider_request_counts": observation.and_then(|sample| sample.provider_requests),
                "native_attempt_counts": observation.and_then(|sample| sample.native_attempts),
                "physical_admission_binding": null, "ratio_qualified": false,
                "recovery": self.recovery,
            }))
        } else {
            observed.and_then(|observation| {
                self.record(operation, floor, elapsed, observation, identity)
            })
        };
        super::durability::retain_diagnostics(result, || diagnostic, &self.recovery)
    }

    pub(super) fn publish_partial(&self, output: &mut impl Write) -> Result {
        for (operation, row) in &self.rows {
            emit_to(output, RawSamples {
                fields: row.evidence(serde_json::json!({
                    "schema": 1, "kind": "partial_operation", "operation": operation.name(),
                    "agents": self.agents, "cohort_completed": false, "ratio_qualified": false,
                    "work_columns": METRICS, "work_observations": row.work.observations,
                    "reason": "operation family or result oracle failed; retained successful returns are not a qualified cohort",
                    "recovery": self.recovery,
                })),
                raw_ns: &row.raw_ns,
            })?;
        }
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
            let terms = row.terms;
            let quantiles = Quantiles::new(&mut row.raw_ns.clone())?;
            let floor_ns = floors.compose(terms);
            let timer = floors.timer();
            emit(RawSamples {
                fields: row.evidence(serde_json::json!({
                    "schema": 1, "kind": "operation", "operation": operation.name(),
                    "measurement_label": match operation {
                        Operation::ActorWake => "event-drain-to-handler-entry",
                        Operation::ActorWarmRead => "borrowed-resident-read",
                        _ => operation.name(),
                    },
                    "agents": self.agents, "active": 1, "quantiles": quantiles,
                    "cohort_completed": true,
                    "floor": terms, "floor_ns": floor_ns,
                    "guarantee": guarantee(operation),
                    "floor_units": floor_units(terms),
                    "ratio_qualified": false,
                    "ratio_qualification": "raw semantic-model comparison; physical work, provider group coverage and matched device guarantee remain unqualified",
                    "p50_ratio": quantiles.p50_ns / floor_ns,
                    "p99_ratio": quantiles.p99_ns / floor_ns,
                    "timer_only_quantiles": timer,
                    "minimum_positive_timer_ns": floors.minimum_positive_timer_ns(),
                    "floor_resolution_limited": floor_ns < timer.p50_ns.max(floors.minimum_positive_timer_ns()),
                    "resolution_scope": "timestamp pair only; async polling and other instrumentation add cost; no subtraction",
                    "work_columns": METRICS,
                    "identity_scope": "original API request/resource/cursor supplied by the operation driver in raw sample order; not an authenticated admission or physical coverage token",
                    "work_observations": row.work.observations,
                    "coordinator_allocations_scope": "current thread only; excluded from qualification",
                    "provider_request_columns": ["data", "metadata", "list", "head", "lease", "token", "requested_query_dispatches"],
                    "control_request_scope": "public metadata/list/head/lease/token calls excluding exactly the original requested query; additional probes remain controls",
                    "native_attempt_columns": ["ok", "err", "incomplete"],
                    "native_attempt_scope": "diagnostic completed observed native calls between the original counter boundaries; includes concurrent work; unobserved leaves and provider/group attribution remain unknown; excluded from durability_calls qualification",
                    })),
                raw_ns: &row.raw_ns,
            })?;
            point.samples.push(row.work);
        }
        if !self.rows.is_empty() {
            return Err("unknown core operation in measurement report".into());
        }
        Ok((complete, point))
    }
}

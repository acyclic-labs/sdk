//! Serial measurements of native providers against same-device primitive floors.
#[path = "local_hotpath/actor_delivery.rs"]
mod actor_delivery;
#[path = "local_hotpath/calibrate.rs"]
mod calibrate;
#[path = "local_hotpath/durability.rs"]
mod durability;
#[path = "local_hotpath/options.rs"]
mod options;
#[path = "local_hotpath/population.rs"]
mod population;
#[cfg(feature = "local-hotpath-proofs")]
#[path = "local_hotpath/proofs.rs"]
mod proofs;
#[cfg(feature = "local-hotpath-proofs")]
#[path = "local_hotpath/proofs_resources.rs"]
mod proofs_resources;
mod quantiles;
#[path = "local_hotpath/report.rs"]
mod report;
#[path = "local_hotpath/storage.rs"]
mod storage;

use acyclic_test_support::scale::{CORE_OPERATIONS, POPULATIONS, Report};
use options::Options;
use report::{Result, emit};

acyclic_test_support::counting_allocator!();

fn main() {
    if std::env::args().any(|argument| argument == "--help") {
        println!(
            "cargo bench --locked -p acyclic-conformance --features local-hotpath --bench local_hotpath -- --root <fresh-directory> --source <revision> [--samples 200] [--setup-credit 1] [--operations comma-separated-core-operation-names] [--populations comma-separated-qualification-populations] [--observe-durability]\nUse --check-observations to run only the quantile and cohort admission controls, without calibration or population setup.\nFor native root crash controls, use --features local-hotpath-proofs and --proofs --operations 'swarm.session_start'."
        );
        return;
    }
    let observations_only = std::env::args().any(|argument| argument == "--check-observations");
    let result = if observations_only {
        report::check_observations()
    } else {
        run()
    };
    if let Err(error) = result {
        let recovery = if observations_only {
            "cargo test --locked -p acyclic-conformance --features local-hotpath --bench local_hotpath -- --check-observations".into()
        } else {
            match Options::parse_from(std::env::args().skip(1)) {
                Ok(options) => options.recovery(),
                Err(_) => "cargo bench --locked -p acyclic-conformance --features local-hotpath --bench local_hotpath -- --help".into(),
            }
        };
        eprintln!("STEP_FAILED local-hotpath: {error}; recovery: {recovery}");
        std::process::exit(1);
    }
}

fn run() -> Result {
    let options = Options::parse_from(std::env::args().skip(1))?;
    let recovery = options.recovery();
    if options.proofs {
        #[cfg(feature = "local-hotpath-proofs")]
        return proofs::run(&options);
        #[cfg(not(feature = "local-hotpath-proofs"))]
        return Err("--proofs requires --features local-hotpath-proofs".into());
    }
    report::check_observations()?;
    std::fs::create_dir(&options.root)?;
    emit(serde_json::json!({
        "schema": 1, "kind": "identity", "source": options.source,
        "executable_sha256": report::executable_sha256()?,
        "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
        "root": options.root, "samples": options.samples,
        "runtime_threads": 1, "payload_bytes": 1024, "actor_payload_bytes": 128,
        "setup_credit": options.setup_credit,
        "actor_checkpoint_bytes": 128,
        "durability": "full", "batch": 1,
        "instrumentation": "original Handler entry timestamp; follow notification waker timestamp; public provider request ledger; current-thread allocator",
        "durability_observer": options.observe_durability,
        "durability_observer_scope": "optional shared operation collector; completed native attempts only; incomplete leaf and group coverage; observer overhead retained in operation latency; calibration excludes observer",
        "populations": options.populations,
        "population_scope": "initial settled dormant roots; creation samples retain new durable identities; session samples add resident roots",
        "operations": options.operations.iter().map(|operation| operation.name()).collect::<Vec<_>>(),
    }))?;
    calibrate::run(&options.root, options.samples)?;
    use acyclic_native_runtime::obs::{Metrics, Operations};
    use std::sync::Arc;
    use tracing_subscriber::layer::SubscriberExt as _;
    // Calibrate without the observer: instrumentation is implementation cost,
    // not a reason to raise the semantic floor.
    let metrics = Metrics::default();
    let durability = options
        .observe_durability
        .then(|| Arc::new(metrics.query("acyclic.runtime.durability")));
    let typed_attempts = options
        .observe_durability
        .then(durability::Projection::default);
    let dispatch = typed_attempts.as_ref().map(|projection| {
        tracing::Dispatch::new(
            tracing_subscriber::registry().with(Operations((metrics, projection.clone()))),
        )
    });
    let _subscriber = dispatch.as_ref().map(tracing::dispatcher::set_default);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut measured = None;
    let _allocations = acyclic_test_support::measure(|| {
        measured = Some(runtime.block_on(async {
            let mut complete = true;
            let mut counts = Report::default();
            for agents in &options.populations {
                let agents = usize::try_from(*agents)?;
                let root = options.root.join(agents.to_string());
                std::fs::create_dir(&root)?;
                let fixture = population::Fixture::open(
                    &root,
                    agents,
                    options.setup_credit,
                    durability.clone(),
                )
                .await?;
                let (observed, point) = storage::run(
                    &fixture,
                    agents,
                    options.samples,
                    &recovery,
                    &options.operations,
                )
                .await?;
                complete &= observed;
                counts.points.push(point);
            }
            Result::Ok((complete, counts))
        }));
    });
    let (latency_complete, counts) = durability::retain_diagnostics(
        measured.unwrap_or_else(|| Err("measurement scope did not execute".into())),
        || {
            if let Some(query) = durability {
                emit(serde_json::json!({
                    "schema": 1, "kind": "native_durability_projection", "snapshot": query.snapshot(),
                    "scope": "completed observed native attempts during setup and selected operation families; base-two latency buckets are not operation quantiles; unobserved leaves and original provider/group coverage remain unknown",
                    "complete_coverage": false,
                    "typed_attempts": typed_attempts.as_ref().map(durability::Projection::snapshot),
                    "syscall_rows": ["unknown", "fsync", "fdatasync", "F_FULLFSYNC", "F_BARRIERFSYNC", "FlushFileBuffers", "syncfs"],
                    "outcome_columns": ["ok", "err", "incomplete"],
                    "missing_field_columns": ["requested", "syscall", "descriptor", "parent", "os_error_on_failure"],
                    "task_rows": ["persistence", "blocking_io"],
                    "task_boolean_columns": ["unknown", "false", "true"],
                    "task_terminal_columns": ["unknown", "ok", "io_error", "panic", "missing"],
                    "first_os_error_scope": "first observed original OS error per syscall/task row; diagnostic only, not authenticated operation identity or error order; absent remains unknown",
                    "task_scope": "terminal samples only; queued=false means inline execution; outcome and panic are independent; absent admission, queue delay, physical owner/group, active tasks and final backing retirement remain unknown; not Worker execution coverage",
                }))?;
            }
            Result::Ok(())
        },
        &recovery,
    )??;
    let findings = counts.qualification_findings(&options.operations, &recovery);
    let counts_complete = findings.is_empty();
    for finding in findings {
        emit(serde_json::json!({
            "schema": 1, "kind": "counting_refused", "step": finding.step,
            "evidence": finding.evidence, "recovery": finding.recovery,
        }))?;
    }
    let complete = latency_complete && counts_complete;
    emit(serde_json::json!({
        "schema": 1, "kind": "run_end", "complete": complete,
        "latency_complete": latency_complete, "counts_complete": counts_complete,
        "full_core_coverage": options.operations.len() == CORE_OPERATIONS.len(),
        "full_population_coverage": options.populations.len() == POPULATIONS.len(),
    }))?;
    if complete {
        Ok(())
    } else {
        Err("incomplete operation or counter coverage: supply the original producers and observers named in refusal rows".into())
    }
}

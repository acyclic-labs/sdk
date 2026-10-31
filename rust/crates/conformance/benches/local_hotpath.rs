//! Serial measurements of native providers against same-device primitive floors.
#[path = "local_hotpath/actor_delivery.rs"]
mod actor_delivery;
#[path = "local_hotpath/calibrate.rs"]
mod calibrate;
#[path = "local_hotpath/population.rs"]
mod population;
#[path = "local_hotpath/report.rs"]
mod report;
#[path = "local_hotpath/storage.rs"]
mod storage;

use acyclic_test_support::scale::{CORE_OPERATIONS, Operation, POPULATIONS, Report};
use report::{Result, emit};
use std::path::PathBuf;

acyclic_test_support::counting_allocator!();

struct Options {
    root: PathBuf,
    samples: usize,
    source: String,
    operations: Vec<Operation>,
    populations: Vec<u64>,
}

impl Options {
    fn recovery(&self) -> String {
        fn quoted(value: &str) -> String {
            if cfg!(windows) {
                format!("'{}'", value.replace('\'', "''"))
            } else {
                format!("'{}'", value.replace('\'', "'\\''"))
            }
        }
        let mut root = self.root.as_os_str().to_owned();
        root.push(".retry");
        format!(
            "cargo bench --locked -p acyclic-conformance --features local-hotpath --bench local_hotpath -- --root {} --source {} --samples {} --operations {} --populations {}",
            quoted(&root.to_string_lossy()),
            quoted(&self.source),
            self.samples,
            quoted(
                &self
                    .operations
                    .iter()
                    .map(|operation| operation.name())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            quoted(
                &self
                    .populations
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        )
    }

    fn parse() -> Result<Self> {
        let mut arguments = std::env::args().skip(1);
        let mut root = None;
        let mut samples = 200;
        let mut source = None;
        let mut operations = CORE_OPERATIONS.to_vec();
        let mut populations = POPULATIONS.to_vec();
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--root" => root = arguments.next().map(PathBuf::from),
                "--samples" => samples = arguments.next().ok_or("missing sample count")?.parse()?,
                "--source" => source = arguments.next(),
                "--operations" => {
                    let names = arguments
                        .next()
                        .ok_or("missing comma-separated operation names")?;
                    operations.clear();
                    for name in names.split(',') {
                        let operation = CORE_OPERATIONS
                            .iter()
                            .copied()
                            .find(|operation| operation.name() == name)
                            .ok_or_else(|| format!("unknown core operation {name}"))?;
                        if operations.contains(&operation) {
                            return Err(format!("duplicate operation {name}").into());
                        }
                        operations.push(operation);
                    }
                }
                "--populations" => {
                    let sizes = arguments
                        .next()
                        .ok_or("missing comma-separated populations")?;
                    populations.clear();
                    for size in sizes.split(',') {
                        let agents = size.parse()?;
                        if !POPULATIONS.contains(&agents) {
                            return Err(format!("unknown qualification population {agents}").into());
                        }
                        if populations.contains(&agents) {
                            return Err(format!("duplicate population {agents}").into());
                        }
                        populations.push(agents);
                    }
                }
                // Cargo passes this flag to harness=false benchmarks.
                "--bench" => {}
                _ => return Err(format!("unknown option {argument}").into()),
            }
        }
        if samples < 100 {
            return Err("p99 requires at least 100 observations".into());
        }
        Ok(Self {
            root: root.ok_or("--root <fresh directory on the measured device> is required")?,
            samples,
            source: source.ok_or("--source <source revision> is required")?,
            operations,
            populations,
        })
    }
}

fn main() {
    if std::env::args().any(|argument| argument == "--help") {
        println!(
            "cargo bench --locked -p acyclic-conformance --features local-hotpath --bench local_hotpath -- --root <fresh-directory> --source <revision> [--samples 200] [--operations comma-separated-core-operation-names] [--populations comma-separated-qualification-populations]"
        );
        return;
    }
    if let Err(error) = run() {
        let recovery = Options::parse().map_or_else(
            |_| "cargo bench --locked -p acyclic-conformance --features local-hotpath --bench local_hotpath -- --help".into(),
            |options| options.recovery(),
        );
        eprintln!("STEP_FAILED local-hotpath: {error}; recovery: {recovery}");
        std::process::exit(1);
    }
}

fn run() -> Result {
    use sha2::{Digest as _, Sha256};
    let options = Options::parse()?;
    let recovery = options.recovery();
    report::check_quantiles()?;
    std::fs::create_dir(&options.root)?;
    let executable = std::fs::read(std::env::current_exe()?)?;
    emit(serde_json::json!({
        "schema": 1, "kind": "identity", "source": options.source,
        "executable_sha256": hex::encode(Sha256::digest(&executable)),
        "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
        "root": options.root, "samples": options.samples,
        "runtime_threads": 1, "payload_bytes": 1024, "actor_payload_bytes": 128,
        "actor_checkpoint_bytes": 128,
        "durability": "full", "batch": 1,
        "instrumentation": "original Handler entry timestamp; follow notification waker timestamp; public provider request ledger; current-thread allocator; no tracing subscriber",
        "populations": options.populations,
        "operations": options.operations.iter().map(|operation| operation.name()).collect::<Vec<_>>(),
    }))?;
    let floors = calibrate::run(&options.root, options.samples)?;
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
                let fixture = population::Fixture::open(&root, agents).await?;
                let (observed, point) = storage::run(
                    &fixture,
                    agents,
                    options.samples,
                    &floors,
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
    let (latency_complete, counts) = measured.ok_or("measurement scope did not execute")??;
    for finding in counts.findings(&options.operations, &recovery) {
        emit(serde_json::json!({
            "schema": 1, "kind": "counting_refused", "step": finding.step,
            "evidence": finding.evidence, "recovery": finding.recovery,
        }))?;
    }
    let counts_complete = counts.qualify(&options.operations, &recovery).is_ok();
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

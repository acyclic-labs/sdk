//! Serial measurements of native providers against same-device primitive floors.
#[path = "local_hotpath/calibrate.rs"]
mod calibrate;
#[path = "local_hotpath/population.rs"]
mod population;
#[path = "local_hotpath/report.rs"]
mod report;
#[path = "local_hotpath/storage.rs"]
mod storage;

use report::{Result, emit};
use std::path::PathBuf;

struct Options {
    root: PathBuf,
    samples: usize,
    source: String,
}

impl Options {
    fn parse() -> Result<Self> {
        let mut arguments = std::env::args().skip(1);
        let mut root = None;
        let mut samples = 200;
        let mut source = None;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--root" => root = arguments.next().map(PathBuf::from),
                "--samples" => samples = arguments.next().ok_or("missing sample count")?.parse()?,
                "--source" => source = arguments.next(),
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
        })
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!(
            "STEP_FAILED local-hotpath: {error}; recovery: cargo bench --locked -p acyclic-conformance --features local-hotpath --bench local_hotpath -- --root <new-directory-on-the-same-device> --source <revision>"
        );
        std::process::exit(1);
    }
}

fn run() -> Result {
    use sha2::{Digest as _, Sha256};
    let options = Options::parse()?;
    report::check_quantiles()?;
    std::fs::create_dir(&options.root)?;
    let executable = std::fs::read(std::env::current_exe()?)?;
    emit(serde_json::json!({
        "schema": 1, "kind": "identity", "source": options.source,
        "executable_sha256": hex::encode(Sha256::digest(&executable)),
        "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
        "root": options.root, "samples": options.samples,
        "runtime_threads": 1, "payload_bytes": 1024, "actor_payload_bytes": 128,
        "durability": "full", "batch": 1,
        "instrumentation": "follow notification waker timestamp; no tracing subscriber",
    }))?;
    let floors = calibrate::run(&options.root, options.samples)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let complete = runtime.block_on(async {
        let mut complete = true;
        for agents in [1, 100_000] {
            let root = options.root.join(agents.to_string());
            std::fs::create_dir(&root)?;
            let fixture = population::Fixture::open(&root, agents).await?;
            complete &= storage::run(&fixture, agents, options.samples, &floors).await?;
        }
        Result::Ok(complete)
    })?;
    emit(serde_json::json!({
        "schema": 1, "kind": "run_end", "complete": complete,
    }))?;
    if complete {
        Ok(())
    } else {
        Err("incomplete core-operation coverage: supply the execution/completion producers named in refusal rows".into())
    }
}

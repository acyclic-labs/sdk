//! Run the unchanged JSON scenario corpus through a selected hosting substrate.
use acyclic_harness_conformance::{
    runner,
    substrate::{self, Substrate},
};
use std::path::PathBuf;

fn main() {
    if let Err(error) = run() {
        eprintln!("scenarios: {error}");
        std::process::exit(1);
    }
}
fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut arguments = std::env::args().skip(1);
    let mut selected = "in-process".to_string();
    let mut filter = None;
    let mut report = None;
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../harness/tests/scenarios");
    while let Some(flag) = arguments.next() {
        let value = arguments
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--substrate" => selected = value,
            "--filter" => filter = Some(value),
            "--report" => report = Some(PathBuf::from(value)),
            "--corpus" => root = PathBuf::from(value),
            _ => return Err(format!("unknown option {flag}").into()),
        }
    }
    let report = report.ok_or(
        "usage: scenarios --substrate <in-process|local|URL> [--filter TEXT] --report FILE",
    )?;
    let substrate: Box<dyn Substrate> = match selected.as_str() {
        "in-process" => Box::new(substrate::InProcess),
        "local" => Box::new(substrate::Local),
        _ => {
            let http = substrate::remote::Http::new(selected)?;
            Box::new(substrate::Actors {
                runtime: http.clone(),
                volume: http,
                bindings: substrate::fixture_bindings(),
            })
        }
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let concurrency = std::thread::available_parallelism()?.get();
    let result = runtime.block_on(runner::run(
        &root,
        substrate.as_ref(),
        filter.as_deref(),
        concurrency,
    ))?;
    std::fs::write(report, serde_json::to_vec_pretty(&result)?)?;
    println!(
        "{} passed, {} failed, {} skipped",
        result.passed, result.failed, result.skipped
    );
    if !result.success() {
        return Err("scenario corpus did not pass".into());
    }
    Ok(())
}

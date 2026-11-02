//! Analyze admitted observation objects without retaining their input history.
use acyclic_harness_core::experiments::{Error, Observation, analysis::Contrast};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Write},
    process::ExitCode,
};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "analyze experiment file: {error}; recovery: run cargo run --locked -p acyclic-harness-core --example experiment-analysis -- --help, then correct the failed input or IO step and repeat the invocation"
            );
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let first = args.next().ok_or("missing baseline arm")?;
    if first == "--help" && args.next().is_none() {
        println!(
            "Usage: experiment-analysis BASELINE_ARM TREATMENT_ARM ASSIGNED_PAIRS OBSERVATIONS.jsonl\nWrite each version-2 baseline row followed by its treatment row, in strictly increasing (population, replicate) order. ASSIGNED_PAIRS is the predeclared count, including failed trials with unknown values. Output is one version-2 JSON report on stdout, including per-arm assigned cost coverage."
        );
        return Ok(());
    }
    let baseline = first
        .into_string()
        .map_err(|_| "baseline arm is not a decimal integer")?
        .parse::<u64>()?;
    let treatment = args
        .next()
        .ok_or("missing treatment arm")?
        .into_string()
        .map_err(|_| "treatment arm is not a decimal integer")?
        .parse::<u64>()?;
    let assigned = args
        .next()
        .ok_or("missing assigned-pair count")?
        .into_string()
        .map_err(|_| "assigned-pair count is not a decimal integer")?
        .parse::<u64>()?;
    let path = args.next().ok_or("missing observations path")?;
    if args.next().is_some() {
        return Err("expected two arm numbers, assigned-pair count and observations path".into());
    }
    let contrast = Contrast::new(baseline, treatment)?;
    let input = File::open(path).map_err(|error| format!("open observations: {error}"))?;
    let rows = serde_json::Deserializer::from_reader(BufReader::new(input))
        .into_iter::<Observation>()
        .map(|row| row.map_err(Error::Json));
    let report = contrast.analyze(rows, assigned)?;
    let mut output = BufWriter::new(std::io::stdout().lock());
    serde_json::to_writer(&mut output, &report)
        .map_err(|error| format!("encode report: {error}"))?;
    output
        .write_all(b"\n")
        .map_err(|error| format!("write report: {error}"))?;
    output
        .flush()
        .map_err(|error| format!("publish report: {error}"))?;
    Ok(())
}

//! Replay selected real-storage inputs and report their actual touched units.
use acyclic_codec_fuzz::object_schedule::{Terminal, check};
use acyclic_native_runtime::testing::DurableFault;
use acyclic_objects::MediumError;
use std::{collections::BTreeMap, error::Error};

fn replay() -> Result<(), Box<dyn Error>> {
    let inputs: Vec<_> = std::env::args_os().skip(1).collect();
    if inputs.is_empty() {
        return Err("missing input files; recovery: cargo run --features storage-schedules --example object_schedules -- corpus/object_schedule/medium-false-0-0-0".into());
    }
    let mut coverage = BTreeMap::new();
    let (mut cases, mut max_writes, mut max_boundaries, mut max_requested) = (0, 0, 0, 0);
    for input in inputs {
        let bytes = std::fs::read(&input)?;
        let report = check(&bytes)?.ok_or("input has no admitted full generation identity")?;
        let fault = match report.fault.fault {
            DurableFault::Before => "before",
            DurableFault::After => "after",
            DurableFault::Torn(_) => "torn",
            DurableFault::Failed => "failed",
            DurableFault::DiskFull => "disk-full",
        };
        let (stage, outcome) = match &report.terminal {
            Terminal::Mutation(Ok(true)) => ("mutation", "applied"),
            Terminal::Mutation(Ok(false)) => ("mutation", "no-effect"),
            Terminal::Mutation(Err(MediumError::Failed(_))) => ("mutation", "failed"),
            Terminal::Mutation(Err(MediumError::Ambiguous(_))) => ("mutation", "ambiguous"),
            Terminal::Flush(Ok(())) => ("flush", "durable"),
            Terminal::Flush(Err(MediumError::Failed(_))) => ("flush", "failed"),
            Terminal::Flush(Err(MediumError::Ambiguous(_))) => ("flush", "ambiguous"),
        };
        *coverage
            .entry((
                report.operation,
                report.fault.operation,
                report.fault.ordinal,
                fault,
                stage,
                outcome,
            ))
            .or_insert(0) += 1;
        cases += 1;
        max_writes = max_writes.max(report.writes.len());
        max_boundaries = max_boundaries.max(report.boundaries);
        let requested: u128 = report
            .writes
            .iter()
            .map(|write| write.requested as u128)
            .sum();
        max_requested = max_requested.max(requested);
    }
    println!(
        "cases={cases} covered-fault-outcomes={} max-physical-calls={max_writes} max-owned-boundaries={max_boundaries} max-requested-bytes={max_requested}",
        coverage.len()
    );
    for ((operation, physical, ordinal, fault, stage, outcome), count) in coverage {
        println!("{operation}\t{physical}\t{ordinal}\t{fault}\t{stage}\t{outcome}\t{count}");
    }
    Ok(())
}

fn main() {
    if let Err(error) = replay() {
        eprintln!(
            "real Medium replay failed: {error}; recovery: cargo run --features storage-schedules --example object_schedules -- corpus/object_schedule/medium-false-0-0-0"
        );
        std::process::exit(1);
    }
}

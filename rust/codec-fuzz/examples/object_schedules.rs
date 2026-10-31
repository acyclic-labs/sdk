//! Replay selected real-storage inputs and report their actual touched units.
use acyclic_codec_fuzz::object_schedule::{
    Report, Terminal, check_each_site, check_selected_site, seeds,
};
use acyclic_native_runtime::testing::DurableFault;
use acyclic_objects::MediumError;
use std::{collections::BTreeMap, error::Error};

fn replay() -> Result<(), Box<dyn Error>> {
    let inputs: Vec<_> = std::env::args_os().skip(1).collect();
    let all_sites = inputs.first().is_some_and(|input| input == "--all-sites");
    let inputs = if all_sites { &inputs[1..] } else { &inputs[..] };
    if inputs.first().is_some_and(|input| input == "--seed") {
        let names = &inputs[1..];
        if names.is_empty() {
            return Err("seed selection needs names; recovery: cargo run --features storage-schedules --example object_schedules -- --seed medium-false-0-0-0".into());
        }
        let producers = seeds();
        return replay_inputs(names.iter().map(|name| {
            producers
                .iter()
                .find(|(candidate, _)| name == candidate.as_str())
                .map(|(_, bytes)| bytes.clone())
                .ok_or_else(|| "unknown writer seed; recovery: cargo run --features storage-schedules --example object_schedules -- --seed medium-false-0-0-0".into())
        }), all_sites);
    }
    if inputs.is_empty() {
        return Err("missing input files; recovery: cargo run --features storage-schedules --example object_schedules -- --seed medium-false-0-0-0".into());
    }
    replay_inputs(inputs.iter().map(|input| {
        std::fs::read(input).map_err(|error| format!("read replay input failed: {error}; recovery: cargo run --features storage-schedules --example object_schedules -- --seed medium-false-0-0-0").into())
    }), all_sites)
}

fn replay_inputs(
    inputs: impl Iterator<Item = Result<Vec<u8>, Box<dyn Error>>>,
    all_sites: bool,
) -> Result<(), Box<dyn Error>> {
    let mut coverage = BTreeMap::new();
    let mut no_fault_inputs = 0;
    let (mut cases, mut max_writes, mut max_boundaries, mut max_requested) = (0, 0, 0, 0);
    let mut record = |report: Report| {
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
        let stage = if report.maintenance {
            "maintenance"
        } else {
            stage
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
    };
    for input in inputs {
        let bytes = input?;
        let mut reported = false;
        let mut observe = |report| {
            reported = true;
            record(report);
        };
        let admitted = if all_sites {
            check_each_site(&bytes, &mut observe)?
        } else {
            check_selected_site(&bytes, &mut observe)?
        };
        if !admitted {
            return Err("admit replay input failed: no admitted full generation identity; recovery: cargo run --features storage-schedules --example object_schedules -- --seed medium-false-0-0-0".into());
        }
        if !reported {
            no_fault_inputs += 1;
        }
    }
    println!(
        "cases={cases} inputs-without-physical-cuts={no_fault_inputs} covered-fault-outcomes={} max-physical-calls={max_writes} max-owned-boundaries={max_boundaries} max-requested-bytes={max_requested}",
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
            "real Medium replay failed: {error}; recovery: cargo run --features storage-schedules --example object_schedules -- --seed medium-false-0-0-0"
        );
        std::process::exit(1);
    }
}

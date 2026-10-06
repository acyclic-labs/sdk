use std::env;
use std::process::ExitCode;

use acyclic_sdk_contract_validation::compare_files;

fn main() -> ExitCode {
    let mut baseline = None;
    let mut candidate = None;
    let mut args = env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--baseline" => baseline = args.next(),
            "--candidate" => candidate = args.next(),
            "--help" | "-h" => {
                print_usage();
                return ExitCode::SUCCESS;
            }
            value => {
                eprintln!("unknown argument: {value}");
                print_usage();
                return ExitCode::from(2);
            }
        }
    }

    let (Some(baseline), Some(candidate)) = (baseline, candidate) else {
        print_usage();
        return ExitCode::from(2);
    };

    let report = match compare_files(baseline, candidate) {
        Ok(report) => report,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };

    println!(
        "exact bytes: {} (baseline {} bytes, candidate {} bytes)",
        if report.exact_bytes_equal {
            "equal"
        } else {
            "different"
        },
        report.baseline_len,
        report.candidate_len
    );
    println!("baseline blake3: {}", report.baseline_digest);
    println!("candidate blake3: {}", report.candidate_digest);
    println!(
        "semantic compatibility: {}",
        if report.semantic_compatible {
            "compatible"
        } else {
            "incompatible"
        }
    );
    for difference in report.differences {
        println!(
            "- {} [{}]: {}",
            difference.path, difference.kind, difference.detail
        );
    }

    if report.semantic_compatible {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn print_usage() {
    eprintln!(
        "usage: sdk-contract-validation --baseline BASELINE.binpb --candidate CANDIDATE.binpb"
    );
}

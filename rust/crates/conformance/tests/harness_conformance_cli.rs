//! End-to-end checks for the machine-readable Harness qualification command.

use acyclic_conformance::{
    HARNESS_SUITE,
    runner::{
        CaseResult, CaseStatus, RUNNER_PROTOCOL, RunnerIdentity, RunnerReport, Subject,
        harness_protocol_identity, harness_suite_digest,
    },
};
use serde::Deserialize;
use std::{
    fs,
    io::Write,
    process::{Command, Stdio},
};

#[derive(Deserialize)]
struct Suite {
    cases: Vec<SuiteCase>,
}

#[derive(Deserialize)]
struct SuiteCase {
    name: String,
    family: String,
}

fn report() -> Result<RunnerReport, Box<dyn std::error::Error>> {
    let suite: Suite = serde_json::from_slice(HARNESS_SUITE)?;
    Ok(RunnerReport {
        protocol: RUNNER_PROTOCOL.into(),
        family: "harness".into(),
        suite_version: 1,
        suite_digest: harness_suite_digest(),
        subject: Subject {
            name: "acyclic-harness".into(),
            version: "0.1.0-rc.1".into(),
            source_revision: "0123456789abcdef0123456789abcdef01234567".into(),
            artifact_digest: format!("blake3:{}", blake3::hash(b"artifact").to_hex()),
        },
        runner: RunnerIdentity {
            language: "rust".into(),
            name: "acyclic-conformance".into(),
            version: "0.1.0-rc.1".into(),
        },
        protocol_identity: harness_protocol_identity(),
        capability_profile: vec!["host".into()],
        cases: suite
            .cases
            .into_iter()
            .filter(|case| case.family == "harness")
            .map(|case| CaseResult {
                evidence_digest: format!("blake3:{}", blake3::hash(case.name.as_bytes()).to_hex()),
                name: case.name,
                status: CaseStatus::Passed,
            })
            .collect(),
    })
}

#[test]
fn cli_accepts_stdin_and_rejects_a_failed_report_from_a_path()
-> Result<(), Box<dyn std::error::Error>> {
    let bytes = serde_json::to_vec(&report()?)?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_harness-conformance"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or("conformance child has no stdin")?
        .write_all(&bytes)?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err("valid stdin report did not qualify".into());
    }
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    if receipt["qualified"] != true || receipt["total"] != 23 {
        return Err("successful CLI receipt is incomplete".into());
    }

    let mut failed = report()?;
    failed.cases[0].status = CaseStatus::Failed;
    let path = std::env::temp_dir().join(format!(
        "acyclic-harness-conformance-{}.json",
        std::process::id()
    ));
    fs::write(&path, serde_json::to_vec(&failed)?)?;
    let failed_output = Command::new(env!("CARGO_BIN_EXE_harness-conformance"))
        .arg(&path)
        .output();
    let remove = fs::remove_file(&path);
    let failed_output = failed_output?;
    remove?;
    if failed_output.status.success() {
        return Err("failed case returned a successful process status".into());
    }
    let receipt: serde_json::Value = serde_json::from_slice(&failed_output.stdout)?;
    if receipt["qualified"] != false || receipt["passed"] != 22 {
        return Err("failed CLI receipt did not preserve the result".into());
    }
    Ok(())
}

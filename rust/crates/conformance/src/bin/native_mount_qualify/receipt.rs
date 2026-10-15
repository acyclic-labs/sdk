use super::*;

pub(super) fn verify_receipt(args: &[std::ffi::OsString]) -> Result<(), Failure> {
    let receipt_path = option(args, "--verify-receipt")
        .map(PathBuf::from)
        .ok_or("--verify-receipt requires a path")?;
    let executable = option(args, "--release-executable")
        .map(PathBuf::from)
        .ok_or("--verify-receipt requires --release-executable")?;
    let required_kind =
        option(args, "--require-kind").ok_or("--verify-receipt requires --require-kind")?;
    let release_version =
        option(args, "--release-version").ok_or("--verify-receipt requires --release-version")?;
    let expected_arch =
        option(args, "--release-arch").ok_or("--verify-receipt requires --release-arch")?;
    let (expected_os, provider_process_io_observable) = match required_kind.as_str() {
        "linux-fuse" => ("linux", true),
        "macos-nfs" => ("macos", true),
        "windows-projfs" => ("windows", false),
        _ => return Err(format!("unsupported receipt backend: {required_kind}").into()),
    };
    let report: ReceiptReport = serde_json::from_slice(&fs::read(receipt_path)?)?;
    let digest = file_blake3(&executable)?;
    if report.arch != expected_arch {
        return Err(format!(
            "native mount receipt architecture {} does not match release target {expected_arch}",
            report.arch
        )
        .into());
    }
    if report.schema != "acyclic-native-mount-qualification-v2"
        || report.os != expected_os
        || !receipt_coverage_is_complete(&report.coverage)
        || report.required_kind.as_deref() != Some(required_kind.as_str())
        || report.release_version.as_deref() != Some(release_version.as_str())
        || report.executable_blake3.as_deref() != Some(digest.as_str())
        || !report.passed
        || report.capability.kind.as_deref() != Some(required_kind.as_str())
        || !report.capability.available
        || !report.capability.writable
        || report.capability.provider_process_io_observable != provider_process_io_observable
        || report.capability.session_isolation != "SharedProcess"
        || report.capability.unavailable_reason.is_some()
        || !receipt_cases_are_complete(&report.cases)
    {
        return Err("native mount receipt does not match the release artifact".into());
    }
    Ok(())
}

fn receipt_coverage_is_complete(coverage: &[String]) -> bool {
    let observed = coverage.iter().map(String::as_str).collect::<BTreeSet<_>>();
    observed.len() == coverage.len() && COVERAGE.iter().all(|required| observed.contains(required))
}

fn receipt_cases_are_complete(cases: &[ReceiptCase]) -> bool {
    const REQUIRED: &[&str] = &[
        "real-mount-mutation-matrix",
        "crash-detach-recovery",
        "checkout-and-git-untouched",
    ];
    let observed = cases
        .iter()
        .map(|case| case.name.as_str())
        .collect::<BTreeSet<_>>();
    observed.len() == cases.len()
        && REQUIRED.iter().all(|required| observed.contains(required))
        && cases
            .iter()
            .all(|case| case.status == "passed" && case.reason.is_null())
}

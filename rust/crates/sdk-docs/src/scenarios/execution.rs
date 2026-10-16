use super::*;
use crate::CommandOutput;
/// Compile every registered scenario with Cargo without treating compiler output as
/// documentation data. This keeps Rust examples as the source of truth for the
/// registry, including examples that require an external endpoint to execute.
pub fn compile_all(
    root: &Path,
    sources: &[ScenarioSource],
    cargo_path: Option<&Path>,
) -> Result<(), Error> {
    let cargo = cargo_path.unwrap_or_else(|| Path::new("cargo"));
    for source in sources {
        // Cargo resolves this beneath current_dir; do not pass Windows verbatim paths
        // through build scripts to native tools such as protoc.
        let manifest = Path::new("rust/crates")
            .join(package_directory(&source.scenario))
            .join("Cargo.toml");
        let mut command = crate::command(cargo);
        command
            .current_dir(root)
            .args(["check", "--quiet", "--locked", "--manifest-path"])
            .arg(&manifest);
        if source.scenario.family == "plugin" {
            command.args(["--bin", source.scenario.example]);
        } else {
            command.args(["--example", source.scenario.example]);
        }
        if !source.scenario.features.is_empty() {
            command
                .arg("--features")
                .arg(source.scenario.features.join(","));
        }
        sanitize_compiler_environment(&mut command);
        let output = command
            .run_output()
            .map_err(|error| Error::Io(format!("{}: {error}", source.scenario.id)))?;
        if !output.status.success() {
            return Err(Error::Invalid(format!(
                "scenario {} failed to compile: {}",
                source.scenario.id,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
    }
    Ok(())
}
/// Execute local examples with a structured Cargo invocation. Endpoint-backed
/// examples deliberately remain receipt-only until their declared service is
/// available.
pub fn execute_local(
    root: &Path,
    sources: &[ScenarioSource],
    cargo_path: Option<&Path>,
) -> Result<Vec<ScenarioExecution>, Error> {
    let mut executions = Vec::new();
    for source in sources {
        if source.scenario.mode != ScenarioMode::ExecuteLocal {
            continue;
        }
        // Cargo resolves this beneath current_dir; do not pass Windows verbatim paths
        // through build scripts to native tools such as protoc.
        let manifest = Path::new("rust/crates")
            .join(package_directory(&source.scenario))
            .join("Cargo.toml");
        let cargo = cargo_path.unwrap_or_else(|| Path::new("cargo"));
        let mut command = crate::command(cargo);
        command
            .current_dir(root)
            .args(["run", "--quiet", "--locked", "--manifest-path"])
            .arg(&manifest);
        if source.scenario.family == "plugin" {
            command.args(["--bin", source.scenario.example, "--", "--help"]);
        } else {
            command.args(["--example", source.scenario.example]);
        }
        if !source.scenario.features.is_empty() {
            command
                .arg("--features")
                .arg(source.scenario.features.join(","));
        }
        sanitize_compiler_environment(&mut command);
        let output = command
            .run_output()
            .map_err(|error| Error::Io(format!("{}: {error}", source.scenario.id)))?;
        if !output.status.success() {
            return Err(Error::Invalid(format!(
                "scenario {} failed: {}",
                source.scenario.id,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let stdout = String::from_utf8(output.stdout).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} stdout is not UTF-8: {error}",
                source.scenario.id
            ))
        })?;
        executions.push(ScenarioExecution {
            scenario: source.scenario,
            source_sha256: source.source_sha256.clone(),
            stdout_sha256: digest_bytes(stdout.as_bytes()),
            // Successful Cargo runs may emit compiler diagnostics only on the
            // first build. They are toolchain/cache noise rather than
            // scenario output, so keep the execution receipt deterministic.
            stderr_sha256: digest_bytes(&[]),
            stdout,
        });
    }
    executions.sort_by_key(|execution| {
        sources
            .iter()
            .position(|source| source.scenario.id == execution.scenario.id)
            .unwrap_or(usize::MAX)
    });
    Ok(executions)
}

/// Execute every registered scenario that has a release-gate invocation.
///
/// Local Cargo examples and endpoint-backed fixture scripts are both selected
/// from the Rust-owned registry. The latter are deliberately invoked through
/// the checked-in fixture path recorded on [`Scenario::fixture_script`], so a
/// generation run cannot silently report the 17 local examples as the full
/// 17-scenario release result.
pub fn execute_all(
    root: &Path,
    sources: &[ScenarioSource],
    cargo_path: Option<&Path>,
    bun_path: Option<&Path>,
) -> Result<Vec<ScenarioExecution>, Error> {
    let mut executions = execute_local(root, sources, cargo_path)?;
    let bun = bun_path.unwrap_or_else(|| Path::new("bun"));
    for source in sources {
        if source.scenario.mode != ScenarioMode::ExecuteWithEndpoint {
            continue;
        }
        let fixture_script = source.scenario.fixture_script.ok_or_else(|| {
            Error::Invalid(format!(
                "endpoint scenario {} has no fixture script",
                source.scenario.id
            ))
        })?;
        let fixture = root.join(fixture_script);
        if !fixture.is_file() {
            return Err(Error::Invalid(format!(
                "scenario {} fixture script is missing {}",
                source.scenario.id,
                fixture.display()
            )));
        }
        let mut command = crate::command(bun);
        command.current_dir(root).arg(&fixture);
        let output = command
            .run_output()
            .map_err(|error| Error::Io(format!("{}: {error}", source.scenario.id)))?;
        if !output.status.success() {
            return Err(Error::Invalid(format!(
                "scenario {} fixture failed: {}",
                source.scenario.id,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let stdout = String::from_utf8(output.stdout).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} fixture stdout is not UTF-8: {error}",
                source.scenario.id
            ))
        })?;
        executions.push(ScenarioExecution {
            scenario: source.scenario,
            source_sha256: source.source_sha256.clone(),
            stdout_sha256: digest_bytes(stdout.as_bytes()),
            stderr_sha256: digest_bytes(&[]),
            stdout,
        });
    }
    executions.sort_by_key(|execution| {
        sources
            .iter()
            .position(|source| source.scenario.id == execution.scenario.id)
            .unwrap_or(usize::MAX)
    });
    Ok(executions)
}

fn sanitize_compiler_environment(command: &mut Command) {
    command.env_remove("CARGO_RESOLVER_LOCKFILE_PATH");
    for (key, _) in crate::command_environment() {
        let uppercase = key.to_string_lossy().to_ascii_uppercase();
        let remove = matches!(
            uppercase.as_str(),
            "RUSTC"
                | "RUSTDOC"
                | "RUSTFLAGS"
                | "RUSTDOCFLAGS"
                | "CARGO_ENCODED_RUSTFLAGS"
                | "CARGO_ENCODED_RUSTDOCFLAGS"
                | "RUSTC_WRAPPER"
                | "RUSTC_WORKSPACE_WRAPPER"
                | "CARGO_BUILD_TARGET"
                | "RUSTUP_TOOLCHAIN"
        ) || uppercase.starts_with("CARGO_CFG_")
            || (uppercase.starts_with("CARGO_BUILD_") && uppercase != "CARGO_BUILD_JOBS")
            || (uppercase.starts_with("CARGO_TARGET_") && uppercase != "CARGO_TARGET_DIR")
            || uppercase.starts_with("RUSTC_")
            || uppercase.starts_with("RUSTDOC_");
        if remove {
            command.env_remove(&key);
        }
    }
}

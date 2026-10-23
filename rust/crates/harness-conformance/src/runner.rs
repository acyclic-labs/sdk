use crate::{TestResult, observation, scenario::Scenario, scenario_data, substrate::Substrate};
use futures::{FutureExt, StreamExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Terminal result of one discovered case.
pub enum Status {
    /// All applicable oracles passed.
    Pass,
    /// Execution, protocol validation or an oracle failed.
    Fail,
    /// Explicit filter exclusion or missing declared capability.
    Skip,
}
#[derive(Debug, Serialize, Deserialize)]
/// Exactly one record per case, ordered by corpus-relative path.
pub struct Record {
    /// Stable corpus-relative path with forward slashes.
    pub case: String,
    /// Terminal case result.
    pub status: Status,
    /// Human-readable diagnostic, including every skip reason.
    pub reason: String,
    /// Capabilities required by the resolved driver.
    pub requires: Vec<String>,
}
#[derive(Debug, Serialize, Deserialize)]
/// Deterministic report without run identifiers or timings.
pub struct Report {
    /// JSON report schema version.
    pub version: u32,
    /// All discovered cases, including filtered cases.
    pub records: Vec<Record>,
    /// Number of successful cases.
    pub passed: usize,
    /// Number of failing cases.
    pub failed: usize,
    /// Number of explicitly skipped cases.
    pub skipped: usize,
}
impl Report {
    /// Qualification requires at least one pass and no failures.
    pub fn success(&self) -> bool {
        self.failed == 0 && self.passed > 0
    }
}

/// Capability tagging is based on the resolved driver, never a case name.
/// Concrete fixture drivers have capabilities distinct from hosted execution.
pub fn requirements(value: &Value) -> TestResult<Vec<String>> {
    match value.get("driver") {
        None => Ok(vec!["executor".into()]),
        Some(Value::String(driver)) if driver == "executor" => Ok(vec!["executor".into()]),
        Some(Value::String(driver))
            if [
                "goal-local",
                "agents",
                "composer",
                "composer_entry",
                "notifications",
                "task_query",
                "subagents",
                "local",
                "swarm",
                "plans",
                "plan-query",
            ]
            .contains(&driver.as_str()) =>
        {
            Ok(vec![format!("local-driver:{driver}")])
        }
        _ => Err("unknown scenario driver".into()),
    }
}

/// Runs every discovered case with bounded admission and client-side oracles.
pub async fn run(
    root: &Path,
    substrate: &dyn Substrate,
    filter: Option<&str>,
    concurrency: usize,
) -> TestResult<Report> {
    if concurrency == 0 {
        return Err("scenario concurrency must be positive".into());
    }
    let mut paths = Vec::new();
    scenario_data::discover(root, &mut paths)?;
    paths.sort();
    let capabilities = substrate.capabilities();
    // One corpus deadline, including queued cases. A stalled host still produces
    // a failing record for every case instead of hanging or truncating JSON.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(55);
    let mut records = stream::iter(
        paths
            .into_iter()
            .map(|path| run_case(root, path, substrate, filter, &capabilities, deadline)),
    )
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;
    records.sort_by(|a, b| a.case.cmp(&b.case));
    if records.is_empty() {
        return Err("no scenarios discovered".into());
    }
    Ok(Report {
        version: 1,
        passed: records.iter().filter(|r| r.status == Status::Pass).count(),
        failed: records.iter().filter(|r| r.status == Status::Fail).count(),
        skipped: records.iter().filter(|r| r.status == Status::Skip).count(),
        records,
    })
}

async fn run_case(
    root: &Path,
    path: PathBuf,
    substrate: &dyn Substrate,
    filter: Option<&str>,
    capabilities: &std::collections::BTreeSet<String>,
    deadline: tokio::time::Instant,
) -> Record {
    let case = path
        .strip_prefix(root)
        .unwrap_or(&path)
        .to_string_lossy()
        .replace('\\', "/");
    let mut requires = Vec::new();
    let result = AssertUnwindSafe(async {
        let mut value = scenario_data::load_value(root, &path)?;
        requires = requirements(&value)?;
        if filter.is_some_and(|filter| !case.contains(filter)) {
            return Ok((Status::Skip, "excluded by --filter".into()));
        }
        let missing: Vec<_> = requires
            .iter()
            .filter(|capability| !capabilities.contains(*capability))
            .cloned()
            .collect();
        if !missing.is_empty() {
            return Ok((
                Status::Skip,
                format!(
                    "requires {}: {}",
                    missing.join(", "),
                    fixture_reason(&value)
                ),
            ));
        }
        let driver = value
            .as_object_mut()
            .ok_or("scenario must be an object")?
            .remove("driver");
        let parsed = if driver.is_none() {
            Scenario::load(root, &path)?
        } else {
            Scenario::from_value(value.clone())?
        };
        let paths = parsed.expect.files.keys().cloned().collect();
        value
            .as_object_mut()
            .ok_or("scenario must be an object")?
            .remove("expect");
        // Resume entries are error oracles. The host needs only their count.
        value["resume"] = serde_json::to_value(vec!["fixture interruption"; parsed.resume.len()])?;
        let observed = tokio::time::timeout_at(deadline, substrate.execute(value, paths))
            .await
            .map_err(|_| "corpus execution budget exhausted")??;
        observation::check(&parsed, &observed)?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
            Status::Pass,
            "all scenario oracles passed".into(),
        ))
    })
    .catch_unwind()
    .await;
    let (status, reason) = match result {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => (Status::Fail, error.to_string()),
        Err(_) => (Status::Fail, "substrate or case runner panicked".into()),
    };
    Record {
        case,
        status,
        reason,
        requires,
    }
}

fn fixture_reason(value: &Value) -> &'static str {
    match value["driver"].as_str() {
        Some("local" | "goal-local" | "agents" | "composer_entry") => {
            "driver inspects native LocalHarness state or native client entrypoints"
        }
        Some("swarm") => {
            "driver requires an instrumented in-memory provider and synchronous wire accounting"
        }
        Some("subagents") => "driver creates its own in-process Streams session/server fixture",
        Some("task_query") => {
            "driver asserts synchronous delivery from its concrete MemoryStream fixture"
        }
        Some("notifications") => "driver inspects its fixture-owned notification projection",
        Some("composer" | "plans" | "plan-query") => {
            "driver binds a concrete MemoryHarnessStorage fixture rather than the package fs binding"
        }
        _ => "host did not declare the required capability",
    }
}

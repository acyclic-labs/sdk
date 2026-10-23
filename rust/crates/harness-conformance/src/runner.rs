use crate::{
    CaseObservation, TestResult, composer::Composer, observation, scenario::Scenario,
    scenario_data, substrate::Substrate,
};
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
        Some(Value::String(driver)) if driver == "composer" => Ok(vec!["composer".into()]),
        Some(Value::String(driver))
            if [
                "goal-local",
                "agents",
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
        // Capability and filter exclusions do not excuse an invalid recipe.
        // Native drivers share these exact schemas with their original runner.
        let parsed = if requires
            .iter()
            .any(|capability| capability.starts_with("local-driver:"))
        {
            crate::native_schema::validate(value.clone())?;
            None
        } else {
            Some(Case::parse(root, &path, &mut value)?)
        };
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
        let parsed = parsed.ok_or("no portable adapter for declared native capability")?;
        let paths = parsed.paths();
        parsed.prepare(&mut value)?;
        let observed = tokio::time::timeout_at(deadline, substrate.execute(value, paths))
            .await
            .map_err(|_| "corpus execution budget exhausted")??;
        parsed.check(&observed)?;
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
        Some("plans" | "plan-query") => {
            "driver uses native storage.builder/run_prompt and fixture-owned file-query projections"
        }
        _ => "host did not declare the required capability",
    }
}

enum Case {
    Executor(Box<Scenario>),
    Composer(Box<Composer>),
}
impl Case {
    fn parse(root: &Path, path: &Path, value: &mut Value) -> TestResult<Self> {
        let driver = value
            .as_object_mut()
            .ok_or("scenario must be an object")?
            .remove("driver");
        match driver.as_ref().and_then(Value::as_str) {
            Some("composer") => Ok(Self::Composer(Box::new(Composer::parse(value.clone())?))),
            Some("executor") => Ok(Self::Executor(Box::new(Scenario::from_value(
                value.clone(),
            )?))),
            None if driver.is_none() => Ok(Self::Executor(Box::new(Scenario::load(root, path)?))),
            _ => Err("no portable adapter for declared driver".into()),
        }
    }
    fn paths(&self) -> Vec<String> {
        match self {
            Self::Executor(case) => case.expect.files.keys().cloned().collect(),
            Self::Composer(case) => case.paths(),
        }
    }
    fn prepare(&self, value: &mut Value) -> TestResult {
        let object = value.as_object_mut().ok_or("scenario must be an object")?;
        object.remove("expect");
        match self {
            Self::Composer(_) => {
                object.insert("driver".into(), "composer".into());
            }
            Self::Executor(case) => {
                // Resume strings are error oracles; the host needs only their count.
                object.insert(
                    "resume".into(),
                    serde_json::to_value(vec!["fixture interruption"; case.resume.len()])?,
                );
            }
        }
        Ok(())
    }
    fn check(&self, actual: &CaseObservation) -> TestResult {
        match (self, actual) {
            (Self::Executor(case), CaseObservation::Executor(actual)) => {
                observation::check(case, actual)
            }
            (Self::Composer(case), CaseObservation::Composer(actual)) => case.check(actual),
            _ => Err("host returned observations for a different driver".into()),
        }
    }
}

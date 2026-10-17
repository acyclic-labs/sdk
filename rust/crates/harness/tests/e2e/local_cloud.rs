//! The same scenario data on in-process Streams and on a local cloud's Streams
//! service. `scripts/local-cloud.mjs` starts the cloud and names its endpoints
//! in `ACYCLIC_LOCAL_CLOUD`; the default suite runs only the local controls.
use super::{TestResult, execution, scenario::Scenario, scenario_data};
use acyclic_stream::{MemoryStream, StreamProvider, grpc::Client};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Executor families compared: the core loop and resume after interruption.
const FAMILIES: [&str; 2] = ["core", "recovery"];

#[derive(serde::Deserialize)]
struct Endpoints {
    streams: Endpoint,
}

#[derive(serde::Deserialize)]
struct Endpoint {
    endpoint: String,
    bearer: String,
    ca_file: PathBuf,
}

/// The scenario data. Cargo names the crate at run time too; a prebuilt test
/// binary run by the fixture, away from its build tree, is given it the same way.
fn root() -> PathBuf {
    std::env::var_os("CARGO_MANIFEST_DIR")
        .map_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")), PathBuf::from)
        .join("tests/scenarios")
}

/// Every executor scenario of `FAMILIES`, in path order.
fn scenarios() -> TestResult<Vec<(PathBuf, Value)>> {
    let root = root();
    let mut paths = Vec::new();
    for family in FAMILIES {
        scenario_data::discover(&root.join(family), &mut paths)?;
    }
    paths.sort();
    let mut cases = Vec::new();
    for path in paths {
        let mut value = scenario_data::load_value(&root, &path)?;
        let driver = value
            .as_object_mut()
            .ok_or("scenario must be an object")?
            .remove("driver");
        if driver.is_none() || driver.as_ref().and_then(Value::as_str) == Some("executor") {
            cases.push((path, value));
        }
    }
    Ok(cases)
}

/// Runs `case` on both providers; both must pass its oracle and replay the
/// same trace.
async fn compare<A: StreamProvider, B: StreamProvider>(
    path: &Path,
    case: &Value,
    left: Arc<A>,
    right: Arc<B>,
) -> TestResult {
    let named = |side: &str, error: Box<dyn std::error::Error + Send + Sync>| {
        format!("{} on {side}: {error}", path.display())
    };
    let left = execution::run_on(Scenario::from_value(case.clone())?, left)
        .await
        .map_err(|error| named("left", error))?
        .trace()
        .await?;
    let right = execution::run_on(Scenario::from_value(case.clone())?, right)
        .await
        .map_err(|error| named("right", error))?
        .trace()
        .await?;
    if left != right {
        return Err(format!(
            "{}: traces differ\nleft: {left:?}\nright: {right:?}",
            path.display()
        )
        .into());
    }
    Ok(())
}

#[tokio::test]
async fn traces_are_provider_independent_and_detect_divergence() -> TestResult {
    let cases = scenarios()?;
    let case = |name: &str| {
        cases
            .iter()
            .find(|(path, _)| path.ends_with(name))
            .map(|(_, value)| value.clone())
            .ok_or(format!("missing scenario {name}"))
    };
    let text = case("core/text.json")?;
    let path = Path::new("core/text.json");
    compare(
        path,
        &text,
        Arc::new(MemoryStream::default()),
        Arc::new(MemoryStream::default()),
    )
    .await?;
    // Negative control: a different model reply must change the trace.
    let mut changed = text.clone();
    changed["turns"] = serde_json::json!(["changed"]);
    changed["expect"] = serde_json::json!({"text": "changed"});
    let left = execution::run_on(
        Scenario::from_value(text)?,
        Arc::new(MemoryStream::default()),
    );
    let right = execution::run_on(
        Scenario::from_value(changed)?,
        Arc::new(MemoryStream::default()),
    );
    let (left, right) = (left.await?.trace().await?, right.await?.trace().await?);
    if left == right {
        return Err("the trace ignores model output".into());
    }
    Ok(())
}

#[tokio::test]
#[ignore = "needs a local cloud: node scripts/local-cloud.mjs"]
async fn scenario_families_match_between_memory_and_local_cloud_streams() -> TestResult {
    let file = std::env::var_os("ACYCLIC_LOCAL_CLOUD")
        .ok_or("ACYCLIC_LOCAL_CLOUD must name the local cloud endpoints file")?;
    let endpoints: Endpoints = serde_json::from_slice(&std::fs::read(file)?)?;
    let streams = endpoints.streams;
    let remote = Arc::new(
        Client::connect_eager_with_ca_certificate(
            &streams.endpoint,
            &streams.bearer,
            std::fs::read(&streams.ca_file)?,
        )
        .await?,
    );
    // The provider contract first: a divergence there names the operation.
    acyclic_stream::conformance::verify(remote.as_ref()).await?;
    let cases = scenarios()?;
    if cases.is_empty() {
        return Err("no executor scenarios discovered".into());
    }
    for (path, case) in &cases {
        let relative = path.strip_prefix(root()).unwrap_or(path);
        compare(
            relative,
            case,
            Arc::new(MemoryStream::default()),
            remote.clone(),
        )
        .await?;
    }
    println!(
        "local cloud: Stream conformance passed; {} executor scenarios matched",
        cases.len()
    );
    Ok(())
}

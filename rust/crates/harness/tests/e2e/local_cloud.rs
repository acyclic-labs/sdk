//! Shared positive contracts over in-process Streams and SDK TLS localhost.
//! Both paths require the same executor outcomes.
#[cfg(feature = "objects")]
#[path = "substrate_matrix.rs"]
mod substrate_matrix;
use super::{
    TestResult, execution,
    resume::{self, Remote},
    scenario::Scenario,
    scenario_data,
};
use acyclic_stream::{MemoryStream, StreamProvider, grpc::Client};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Families compared: the executor's core loop and resume after interruption.
const FAMILIES: [&str; 2] = ["core", "recovery"];

/// The scenario data. Cargo names the crate at run time too; a prebuilt test
/// binary run by the fixture, away from its build tree, is given it the same way.
fn root() -> PathBuf {
    acyclic_harness::test_support::environment("CARGO_MANIFEST_DIR")
        .map_or_else(
            || acyclic_test_support::source_path("rust/crates/harness"),
            PathBuf::from,
        )
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
        if matches!(
            driver.as_ref().map(Value::as_str),
            None | Some(Some("executor"))
        ) {
            cases.push((path, value));
        }
    }
    Ok(cases)
}

/// The trace of `case` on `stream`, after its own oracle passed.
async fn trace<P: StreamProvider>(case: &Value, stream: Arc<P>) -> TestResult<Value> {
    Ok(Value::Array(
        execution::run_on(Scenario::from_value(case.clone())?, stream)
            .await?
            .trace()
            .await?,
    ))
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
    let left = trace(case, left)
        .await
        .map_err(|error| named("left", error))?;
    let right = trace(case, right)
        .await
        .map_err(|error| named("right", error))?;
    if left != right {
        return Err(format!(
            "{}: traces differ\nleft: {left:?}\nright: {right:?}",
            path.display()
        )
        .into());
    }
    Ok(())
}

/// A Streams client over TLS on loopback, served by the SDK's own unary and
/// Session services: the transport a cloud Edge speaks, in process.
struct Loopback {
    client: Arc<Client>,
    /// How another process reaches this server.
    remote: Remote,
    _ca: tempfile::TempDir,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Drop for Loopback {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn loopback<P: StreamProvider + 'static>(provider: Arc<P>) -> TestResult<Loopback> {
    use acyclic_stream::{grpc, session, wire};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let incoming = futures::stream::try_unfold(listener, |listener| async move {
        let (socket, _) = listener.accept().await?;
        Ok::<_, std::io::Error>(Some((socket, listener)))
    });
    let identity = tonic::transport::Identity::from_pem(
        certificate.cert.pem(),
        certificate.signing_key.serialize_pem(),
    );
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?
            .add_service(wire::stream_service_server::StreamServiceServer::new(
                grpc::Service::new(provider.clone()),
            ))
            .add_service(
                wire::stream_session_service_server::StreamSessionServiceServer::new(
                    session::Service::new(
                        provider,
                        acyclic_stream::session::testing::authority(),
                        "loopback".into(),
                        session::Limits::default(),
                    ),
                ),
            )
            .serve_with_incoming(incoming),
    );
    let ca = tempfile::tempdir()?;
    let remote = Remote {
        endpoint,
        bearer: "loopback".into(),
        workload: acyclic_stream::session::testing::credential().to_vec(),
        ca_file: ca.path().join("ca.pem"),
    };
    std::fs::write(&remote.ca_file, certificate.cert.pem())?;
    Ok(Loopback {
        client: remote.connect().await?,
        remote,
        _ca: ca,
        server,
    })
}

/// The one scenario at `name` (a path suffix).
fn case(cases: &[(PathBuf, Value)], name: &str) -> TestResult<Value> {
    cases
        .iter()
        .find(|(path, _)| path.ends_with(name))
        .map(|(_, case)| case.clone())
        .ok_or_else(|| format!("missing scenario {name}").into())
}

#[tokio::test]
async fn traces_are_provider_independent_and_detect_divergence() -> TestResult {
    let cases = scenarios()?;
    let text = case(&cases, "core/text.json")?;
    compare(
        Path::new("core/text.json"),
        &text,
        Arc::new(MemoryStream::default()),
        Arc::new(MemoryStream::default()),
    )
    .await?;
    // Negative control: a different model reply must change the trace.
    let mut changed = text.clone();
    changed["turns"] = serde_json::json!(["changed"]);
    changed["expect"] = serde_json::json!({"text": "changed"});
    let memory = || Arc::new(MemoryStream::default());
    let left = trace(&text, memory()).await?;
    let right = trace(&changed, memory()).await?;
    if left == right {
        return Err("the trace ignores model output".into());
    }
    Ok(())
}

#[tokio::test]
async fn killed_turn_resumes_from_a_loopback_journal() -> TestResult {
    let loopback = loopback(Arc::new(MemoryStream::default())).await?;
    resume::kill_and_resume(&loopback.remote, loopback.client.clone()).await?;
    // Negative control: without a journaled prefix there is nothing to resume.
    resume::resume_unjournaled(loopback.client.clone()).await
}

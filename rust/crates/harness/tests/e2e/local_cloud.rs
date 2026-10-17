//! The same scenario data on in-process Streams and on a local cloud's Streams
//! service. `scripts/local-cloud.mjs` starts the cloud and names its endpoints
//! in `ACYCLIC_LOCAL_CLOUD`; the default suite runs only the local controls,
//! including the subagent family over a loopback Session. On the cloud, the
//! executor families are compared and the subagent family's refusals pinned.
use super::{
    TestResult, execution,
    resume::{self, Remote},
    scenario::Scenario,
    scenario_data,
    subagents::{self, Refusal, SubagentCase},
};
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenPage, ChildrenPageRequest, ChildrenRequest,
    CommitCondition, CommitId, CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt,
    ForkRequest, IdempotencyKey, IdempotencyObservation, MemoryStream, ReadRequest, RecordStream,
    StreamBounds, StreamError, StreamPath, StreamProvider, StreamWatermarks, grpc::Client,
};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Families compared: the executor's core loop and resume after interruption,
/// and subagent fan-out followed over the Session.
const FAMILIES: [&str; 3] = ["core", "recovery", "subagents"];

/// One comparable scenario.
#[derive(Clone)]
enum Case {
    Executor(Value),
    Subagents(SubagentCase),
}

#[derive(serde::Deserialize)]
struct Endpoints {
    streams: Remote,
}

/// The scenario data. Cargo names the crate at run time too; a prebuilt test
/// binary run by the fixture, away from its build tree, is given it the same way.
fn root() -> PathBuf {
    std::env::var_os("CARGO_MANIFEST_DIR")
        .map_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")), PathBuf::from)
        .join("tests/scenarios")
}

/// Every executor and subagent scenario of `FAMILIES`, in path order.
fn scenarios() -> TestResult<Vec<(PathBuf, Case)>> {
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
        match driver.as_ref().map(Value::as_str) {
            None | Some(Some("executor")) => cases.push((path, Case::Executor(value))),
            Some(Some("subagents")) => {
                cases.push((path, Case::Subagents(serde_json::from_value(value)?)));
            }
            _ => {}
        }
    }
    Ok(cases)
}

/// The trace of `case` on `stream`, after its own oracle passed. Subagent
/// cases fan out under `root`.
async fn trace<P: StreamProvider>(case: &Case, stream: Arc<P>, root: &str) -> TestResult<Value> {
    match case {
        Case::Executor(value) => Ok(Value::Array(
            execution::run_on(Scenario::from_value(value.clone())?, stream)
                .await?
                .trace()
                .await?,
        )),
        Case::Subagents(case) => subagents::run_on(case, stream, root).await,
    }
}

/// Runs `case` on both providers; both must pass its oracle and replay the
/// same trace.
async fn compare<A: StreamProvider, B: StreamProvider>(
    path: &Path,
    case: &Case,
    left: Arc<A>,
    right: Arc<B>,
    root: &str,
) -> TestResult {
    let named = |side: &str, error: Box<dyn std::error::Error + Send + Sync>| {
        format!("{} on {side}: {error}", path.display())
    };
    let left = trace(case, left, root)
        .await
        .map_err(|error| named("left", error))?;
    let right = trace(case, right, root)
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

async fn loopback(provider: Arc<MemoryStream>) -> TestResult<Loopback> {
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
                    session::Service::new(provider, "loopback".into(), session::Limits::default()),
                ),
            )
            .serve_with_incoming(incoming),
    );
    let ca = tempfile::tempdir()?;
    let remote = Remote {
        endpoint,
        bearer: "loopback".into(),
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
fn case(cases: &[(PathBuf, Case)], name: &str) -> TestResult<Case> {
    cases
        .iter()
        .find(|(path, _)| path.ends_with(name))
        .map(|(_, case)| case.clone())
        .ok_or_else(|| format!("missing scenario {name}").into())
}

/// In-process Streams that refuse, with the given cause, what a cloud's
/// Streams refuse today (cloud-local finding 15): commits fenced on a head,
/// checked follows and live children.
struct Refusing(MemoryStream, StreamError);

#[async_trait::async_trait]
impl StreamProvider for Refusing {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.0.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.0.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.0.bounds(path).await
    }
    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        self.0.watermarks(path).await
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.0.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.0.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.0.read(request).await
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.0.follow(path, from).await
    }
    async fn follow_checked(
        &self,
        _: StreamPath,
        _: u64,
        _: CommitId,
    ) -> Result<RecordStream, StreamError> {
        Err(self.1.clone())
    }
    async fn children(&self, _: ChildrenRequest) -> Result<ChildStream, StreamError> {
        Err(self.1.clone())
    }
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.0.children_page(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        let head = |condition: &CommitCondition| matches!(condition, CommitCondition::Head { .. });
        if request.conditions.iter().any(head) {
            return Err(self.1.clone());
        }
        self.0.commit(request).await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.0.read_commit(id).await
    }
}

/// What a cloud's Streams refuse today, each as an unsupported capability
/// (cloud-local finding 15). Pinned, so the live test fails once the cloud
/// serves any of them, or refuses one for another reason.
fn cloud_refusals() -> Vec<Refusal> {
    subagents::KINDS
        .into_iter()
        .chain([subagents::LATEST])
        .map(|kind| (kind, StreamError::Unsupported))
        .collect()
}

/// Every refusal of `stream`, including LATEST delivery on `client`'s Session.
async fn refusals<P: StreamProvider>(
    stream: Arc<P>,
    client: &Client,
    root: &str,
) -> TestResult<Vec<Refusal>> {
    let mut refused = subagents::refused(stream, root).await?;
    refused.extend(subagents::latest(client, root).await?);
    Ok(refused)
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
        "subagents",
    )
    .await?;
    // Negative control: a different model reply must change the trace.
    let Case::Executor(value) = text else {
        return Err("core/text.json is not an executor scenario".into());
    };
    let mut changed = value.clone();
    changed["turns"] = serde_json::json!(["changed"]);
    changed["expect"] = serde_json::json!({"text": "changed"});
    let memory = || Arc::new(MemoryStream::default());
    let left = trace(&Case::Executor(value), memory(), "subagents").await?;
    let right = trace(&Case::Executor(changed), memory(), "subagents").await?;
    if left == right {
        return Err("the trace ignores model output".into());
    }
    Ok(())
}

#[tokio::test]
async fn subagent_follows_match_over_a_loopback_session_and_detect_divergence() -> TestResult {
    let cases = scenarios()?;
    let subagent_cases: Vec<_> = cases
        .iter()
        .filter(|(_, case)| matches!(case, Case::Subagents(_)))
        .collect();
    if subagent_cases.len() < 3 {
        return Err(format!(
            "only {} subagent scenarios discovered",
            subagent_cases.len()
        )
        .into());
    }
    for (path, case) in subagent_cases {
        let loopback = loopback(Arc::new(MemoryStream::default())).await?;
        let memory = Arc::new(MemoryStream::default());
        compare(path, case, memory, loopback.client.clone(), "subagents").await?;
    }
    // Negative control: a different final status must change the trace.
    let Case::Subagents(fan_out) = case(&cases, "subagents/fan-out.json")? else {
        return Err("subagents/fan-out.json is not a subagent scenario".into());
    };
    let mut changed = fan_out.clone();
    changed.status = "cancelled".into();
    changed.expect.status = "cancelled".into();
    let loopback = loopback(Arc::new(MemoryStream::default())).await?;
    let left = trace(&Case::Subagents(fan_out), loopback.client.clone(), "left").await?;
    let right = trace(&Case::Subagents(changed), loopback.client.clone(), "right").await?;
    if left == right {
        return Err("the trace ignores the final status".into());
    }
    // The refusal probe: the SDK's TLS Session serves every kind, LATEST included.
    let probe = refusals(loopback.client.clone(), &loopback.client, "probe").await?;
    if !probe.is_empty() {
        return Err(format!("the loopback Session refused {probe:?}").into());
    }
    // A provider refusing as the cloud does matches the pin, and one refusing
    // the same kinds for an unrelated cause does not.
    let pinned = cloud_refusals();
    for (cause, matches) in [
        (StreamError::Unsupported, true),
        (StreamError::AccessDenied, false),
        (StreamError::InvalidArgument, false),
    ] {
        let refusing = Arc::new(Refusing(MemoryStream::default(), cause.clone()));
        let mut probe = subagents::refused(refusing, "probe").await?;
        probe.push((subagents::LATEST, StreamError::Unsupported));
        if (probe == pinned) != matches {
            return Err(format!("refusing with {cause:?} probed {probe:?}").into());
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "needs a local cloud: node scripts/local-cloud.mjs"]
async fn scenario_families_match_between_memory_and_local_cloud_streams() -> TestResult {
    let file = std::env::var_os("ACYCLIC_LOCAL_CLOUD")
        .ok_or("ACYCLIC_LOCAL_CLOUD must name the local cloud endpoints file")?;
    let endpoints: Endpoints = serde_json::from_slice(&std::fs::read(file)?)?;
    let remote = endpoints.streams.connect().await?;
    let cases = scenarios()?;
    let executor_cases: Vec<_> = cases
        .iter()
        .filter(|(_, case)| matches!(case, Case::Executor(_)))
        .collect();
    if executor_cases.is_empty() {
        return Err("no executor scenarios discovered".into());
    }
    // Catalog pagination fences the whole account's hierarchy generation, so
    // conformance needs a stable catalog before the other phases create streams.
    timed("Streams conformance", async {
        acyclic_stream::conformance::verify(remote.as_ref()).await?;
        TestResult::Ok(())
    })
    .await?;
    // The remaining phases use disjoint paths and journal identities. Join all
    // results before returning so a failure cannot cancel a child mid-cleanup.
    let probes = timed(
        "Session refusals",
        refusals(remote.clone(), &remote, "probe"),
    );
    let compared = timed("executor scenarios", async {
        futures::future::try_join_all(executor_cases.iter().map(|(path, case)| {
            let relative = path.strip_prefix(root()).unwrap_or(path).to_owned();
            let remote = remote.clone();
            async move {
                let memory = Arc::new(MemoryStream::default());
                compare(&relative, case, memory, remote, "subagents").await
            }
        }))
        .await?;
        TestResult::Ok(())
    });
    let resumed = timed("kill, resume and completed reopen", async {
        let loopback = loopback(Arc::new(MemoryStream::default())).await?;
        let (cloud, local, unjournaled) = futures::join!(
            resume::kill_and_resume(&endpoints.streams, remote.clone()),
            resume::kill_and_resume(&loopback.remote, loopback.client.clone()),
            resume::resume_unjournaled(remote.clone()),
        );
        let (cloud, local) = (cloud?, local?);
        unjournaled?;
        if cloud != local {
            return Err(format!(
                "resumed journals differ: {cloud:?} on the cloud, {local:?} on loopback"
            )
            .into());
        }
        TestResult::Ok(())
    });
    let (refused, compared, resumed) = futures::join!(probes, compared, resumed);
    let refused = refused?;
    compared?;
    resumed?;
    if refused != cloud_refusals() {
        return Err(format!("the cloud's Streams now refuse {refused:?}").into());
    }
    println!(
        "local cloud: Stream conformance passed; {} executor scenarios matched; a killed turn resumed from the cloud journal; the subagent family is refused: {refused:?}",
        executor_cases.len()
    );
    Ok(())
}

async fn timed<T>(
    phase: &str,
    run: impl std::future::Future<Output = TestResult<T>>,
) -> TestResult<T> {
    let started = std::time::Instant::now();
    let result = run.await;
    eprintln!(
        "local cloud phase {phase}: {:.3}s",
        started.elapsed().as_secs_f64()
    );
    result.map_err(|error| format!("{phase}: {error}").into())
}

#[tokio::test]
async fn killed_turn_resumes_from_a_loopback_journal() -> TestResult {
    let loopback = loopback(Arc::new(MemoryStream::default())).await?;
    resume::kill_and_resume(&loopback.remote, loopback.client.clone()).await?;
    // Negative control: without a journaled prefix there is nothing to resume.
    resume::resume_unjournaled(loopback.client.clone()).await
}

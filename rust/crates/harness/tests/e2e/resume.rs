//! A harness process killed mid-turn resumes from its remote journal. A child
//! test process runs a stock turn whose journal is on remote Streams and whose
//! journal files are on local disk. Its model streams a prefix and then never
//! finishes. Once the journal holds that prefix, the parent kills the child and
//! reopens the same journal in a fresh executor. The fresh provider only
//! reconciles: the resumed turn must continue the journaled prefix without a
//! new model request.
use super::TestResult;
use acyclic_harness::{
    AgentId, Capabilities, ConversationId, IdempotencyKey, OperationId,
    context::{ContextPipeline, ModelContextCapacity, ModelTokenCount},
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry},
    executor::{ExecutionEvent, ExecutionJournal, Executor, StockExecutor, TurnInput},
    filesystem::{
        FilesystemContentVerifier, FilesystemExecutionJournal, FilesystemHost,
        FilesystemJournalBinding,
    },
    model::{Model, ModelContent, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest},
    resources::ProviderRef,
    store::StreamAggregate,
    test_support::{ScriptedModelProvider, ScriptedReconciliation},
    tool::ToolRegistry,
};
use acyclic_stream::{StreamProvider, grpc::Client};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// The child's configuration, passed in its environment.
const CHILD: &str = "ACYCLIC_RESUME_CHILD";
/// The line the child prints once its journal holds the streamed prefix.
const JOURNALED: &str = "acyclic-resume: journaled";
const INSTRUCTION: &str = "Resume after a kill.";
const PREFIX: &str = "partial-";
const REST: &str = "restored";
const MAXIMUM_FILE_BYTES: u64 = 1_048_576;

/// Where a process finds the remote Streams and the journal's files.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct Remote {
    pub endpoint: String,
    pub bearer: String,
    pub ca_file: PathBuf,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Child {
    remote: Remote,
    files: PathBuf,
    seed: u128,
}

impl Remote {
    pub(super) async fn connect(&self) -> TestResult<Arc<Client>> {
        let ca = std::fs::read(&self.ca_file)?;
        Ok(Arc::new(
            Client::connect_eager_with_ca_certificate(&self.endpoint, &self.bearer, ca).await?,
        ))
    }
}

/// Every identity of one durable turn, derived from `seed`, so that a second
/// process reopens exactly what the first one wrote.
struct Identity {
    agent: AgentId,
    conversation: String,
    operation: OperationId,
    key: [u8; 32],
}

impl Identity {
    fn new(seed: u128) -> Self {
        let bytes = seed.to_be_bytes();
        Self {
            agent: AgentId::from_bytes(bytes),
            conversation: ConversationId::from_bytes(bytes).to_string(),
            operation: OperationId::from_bytes(bytes),
            key: *blake3::hash(&bytes).as_bytes(),
        }
    }

    fn input(&self) -> TurnInput {
        TurnInput {
            operation_id: self.operation,
            input: ModelContent::Text(INSTRUCTION.into()),
            selected_context: None,
            max_steps: 4,
        }
    }
}

/// Opens the journal of `identity` on `stream`, with its files under `files`.
/// Only the first opening binds the conversation.
async fn journal<P: StreamProvider + 'static>(
    stream: Arc<P>,
    files: &Path,
    identity: &Identity,
    first: bool,
) -> TestResult<Arc<dyn ExecutionJournal>> {
    let filesystem = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(files)).await?;
    let provider = ProviderRef::new("resume".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    let volume = VolumeRef::new(
        provider,
        format!("journal-{}", identity.agent),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(identity.agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let conversation = Authority {
        kind: AggregateKind::Conversation,
        id: identity.conversation.clone(),
    };
    let issuer = AuthorityIssuer::new("resume", identity.key, conversation.clone());
    let scope = issuer.issue(
        "owner",
        &Capabilities::new([
            "conversation:bind".to_owned(),
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(identity.agent),
        None,
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    if first {
        StreamAggregate::open(
            &stream,
            (
                conversation,
                issuer.verifier(&mut None)?,
                SchemaRegistry::new(),
            ),
            None,
            acyclic_harness::store::default_projection_read_limits(),
            acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None,
        )
        .await?
        .execute(
            Command {
                operation_id: OperationId::new(),
                idempotency_key: IdempotencyKey::new("resume-conversation-bind")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation {
                    agent: identity.agent,
                },
            },
            None,
            &mut None,
        )
        .await?;
    }
    let input =
        FilesystemContentVerifier::new(host.clone(), &verifier, &scope, MAXIMUM_FILE_BYTES, None)?;
    let journal = FilesystemExecutionJournal::new(
        stream,
        host,
        &SchemaRegistry::new(),
        FilesystemJournalBinding {
            volume: &volume,
            verifier: &verifier,
            scope: &scope,
            maximum_bytes: MAXIMUM_FILE_BYTES,
            io: None,
        },
    )?
    .with_input_verifier(input);
    Ok(Arc::new(journal))
}

fn executor(provider: Arc<dyn ModelProvider>) -> TestResult<StockExecutor> {
    Ok(StockExecutor::new(
        Model::new("scripted".into(), "resume".into(), "1".into(), Value::Null)?,
        provider,
        ContextPipeline::default(),
        ToolRegistry::default(),
    ))
}

fn content(delta: &str) -> TestResult<ModelEvent> {
    Ok(serde_json::from_value(
        json!({"kind": "content", "delta": delta}),
    )?)
}

/// Every journaled observation of `operation`: event kinds, with model events loaded.
async fn observed(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
) -> TestResult<Vec<Value>> {
    let mut events = Vec::new();
    let mut after = 0;
    loop {
        let records = journal.replay(operation, after, 64, None)?.await?;
        if records.is_empty() {
            return Ok(events);
        }
        for record in records {
            after = record.sequence;
            events.push(match &record.event {
                ExecutionEvent::Model {
                    step,
                    purpose,
                    event,
                } => json!({
                    "kind": "model",
                    "step": step,
                    "purpose": purpose,
                    "event": serde_json::from_slice::<Value>(&journal.load(event, None)?.await?)?,
                }),
                event => json!({"kind": serde_json::to_value(event)?["kind"]}),
            });
        }
    }
}

fn expected() -> Vec<Value> {
    vec![
        json!({"kind": "started"}),
        json!({"kind": "context_prepared"}),
        json!({"kind": "model_started"}),
        json!({"kind": "model", "step": 0, "purpose": "response",
            "event": {"kind": "content", "delta": PREFIX}}),
        json!({"kind": "model", "step": 0, "purpose": "response",
            "event": {"kind": "content", "delta": REST}}),
        json!({"kind": "model", "step": 0, "purpose": "response",
            "event": {"kind": "completed", "metadata": {"finish": "stop"}}}),
    ]
}

fn check_resumed(events: &[Value]) -> TestResult {
    if events != expected() {
        return Err(format!("the resumed journal holds {events:?}").into());
    }
    Ok(())
}

#[test]
fn full_journal_oracle_rejects_missing_completion_metadata_changes_and_reordering() {
    let exact = expected();
    assert!(check_resumed(&exact).is_ok());
    let mut dropped = exact.clone();
    dropped.pop();
    assert!(check_resumed(&dropped).is_err());
    let mut changed = exact.clone();
    changed[5]["event"]["metadata"]["finish"] = json!("length");
    assert!(check_resumed(&changed).is_err());
    let mut reordered = exact;
    reordered.swap(3, 4);
    assert!(check_resumed(&reordered).is_err());
}

struct PausedPrefix {
    journal: Arc<dyn ExecutionJournal>,
    operation: OperationId,
    accounting: ScriptedModelProvider,
}

impl ModelProvider for PausedPrefix {
    fn context_capacity(&self, model: &Model) -> acyclic_harness::Result<ModelContextCapacity> {
        self.accounting.context_capacity(model)
    }

    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> acyclic_stream::BoxProviderFuture<'a, acyclic_harness::Result<ModelTokenCount>> {
        self.accounting.count_tokens(request)
    }

    fn generate<'a>(
        &'a self,
        _: PreparedModelRequest,
        _: ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, acyclic_harness::Result<ModelEvent>> {
        Box::pin(futures::stream::unfold(false, move |sent| async move {
            if !sent {
                return Some((
                    Ok(ModelEvent::Content {
                        delta: PREFIX.into(),
                    }),
                    true,
                ));
            }
            // StockExecutor awaits the preceding event's successful journal
            // append before requesting this next item. Verify that prefix once;
            // a producer prefetched before append must fail, never signal ready.
            match observed(self.journal.as_ref(), self.operation).await {
                Ok(events) if events.as_slice() == &expected()[..4] => {}
                result => {
                    return Some((
                        Err(acyclic_harness::Error::Storage(format!(
                            "the prefix was not journaled before the next item: {result:?}"
                        ))),
                        true,
                    ));
                }
            }
            // libtest may have written a test-name prefix without a newline.
            println!("\n{JOURNALED}");
            std::future::pending().await
        }))
    }
}

/// The child process: run the turn until the parent kills it.
#[tokio::test]
#[ignore = "the child of resume::killed_turn_resumes_from_a_loopback_journal"]
async fn child() -> TestResult {
    let Some(config) = acyclic_harness::test_support::environment(CHILD) else {
        return Ok(());
    };
    let config: Child = serde_json::from_str(config.to_str().ok_or("child config is not UTF-8")?)?;
    child_turn(async move {
        let identity = Identity::new(config.seed);
        let stream = config.remote.connect().await?;
        let journal = journal(stream, &config.files, &identity, true).await?;
        executor(Arc::new(PausedPrefix {
            journal: journal.clone(),
            operation: identity.operation,
            accounting: ScriptedModelProvider::new([]),
        }))?
        .execute(identity.input(), journal.as_ref())
        .await?;
        Err("the pending turn finished".into())
    })
    .await
}

/// The child's liveness is judged where its work runs: its own progress guard
/// sees its connection, journal and stream wakes. A stalled child returns, and
/// its exit closes the stdout the parent follows.
async fn child_turn(turn: impl std::future::Future<Output = TestResult>) -> TestResult {
    super::stall::guarded(Path::new("resume child"), turn).await?
}

/// Runs a child turn on `remote`, kills it once its prefix is journaled, and
/// resumes it in this process from `stream`. Returns the resumed journal's
/// observations, after the resumed text and provider counts passed.
pub(super) async fn kill_and_resume<P: StreamProvider + 'static>(
    remote: &Remote,
    stream: Arc<P>,
) -> TestResult<Vec<Value>> {
    let files = tempfile::tempdir()?;
    let seed = u128::from_be_bytes(*uuid::Uuid::new_v4().as_bytes());
    let config = serde_json::to_string(&Child {
        remote: remote.clone(),
        files: files.path().to_owned(),
        seed,
    })?;
    let mut child = tokio::process::Command::new(std::env::current_exe()?)
        .args(["--exact", "resume::child", "--ignored", "--nocapture"])
        .env(CHILD, config)
        .env("RUST_TEST_THREADS", "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = child.stdout.take().ok_or("child has no stdout")?;
    // Readiness is the child's own event: its line, or end of file once it
    // exits, including after its guard rejects a stall. No parent-side idle
    // budget applies to work this process cannot observe.
    let journaled = journaled(stdout).await;
    // Killed, not asked: nothing in the child runs after this point.
    child.kill().await?;
    let status = child.wait().await?;
    if !journaled? {
        return Err(format!("the child exited ({status}) without journaling its prefix").into());
    }
    let identity = Identity::new(seed);
    let resumed = journal(stream.clone(), files.path(), &identity, false).await?;
    let provider =
        Arc::new(
            ScriptedModelProvider::new([]).with_reconciliations([ScriptedReconciliation {
                orphan: true,
                observed: vec![content(PREFIX)?],
                continuation: Some(vec![
                    content(REST)?,
                    serde_json::from_value(
                        json!({"kind": "completed", "metadata": {"finish": "stop"}}),
                    )?,
                ]),
            }]),
        );
    let output = executor(provider.clone())?
        .execute(identity.input(), resumed.as_ref())
        .await?;
    let (requests, reconciliations) = (provider.requests().len(), provider.reconciliations().len());
    if output.text != format!("{PREFIX}{REST}") || (requests, reconciliations) != (0, 1) {
        return Err(format!(
            "resumed {:?} with {requests} requests and {reconciliations} reconciliations",
            output.text
        )
        .into());
    }
    let events = observed(resumed.as_ref(), identity.operation).await?;
    check_resumed(&events)?;
    drop(resumed);
    let reopened = journal(stream, files.path(), &identity, false).await?;
    let provider = Arc::new(ScriptedModelProvider::new([]));
    let replayed = executor(provider.clone())?
        .execute(identity.input(), reopened.as_ref())
        .await?;
    if replayed != output
        || !provider.requests().is_empty()
        || !provider.reconciliations().is_empty()
    {
        return Err(format!(
            "a completed reopen called the provider or changed output: {replayed:?}"
        )
        .into());
    }
    let replayed = observed(reopened.as_ref(), identity.operation).await?;
    check_resumed(&replayed)?;
    if replayed != events {
        return Err("a completed reopen changed the journal".into());
    }
    Ok(events)
}

/// Negative control: an operation that no killed process journaled cannot be
/// resumed by a provider that only reconciles.
pub(super) async fn resume_unjournaled<P: StreamProvider + 'static>(stream: Arc<P>) -> TestResult {
    let files = tempfile::tempdir()?;
    let identity = Identity::new(u128::from_be_bytes(*uuid::Uuid::new_v4().as_bytes()));
    let journal = journal(stream, files.path(), &identity, true).await?;
    let provider =
        Arc::new(
            ScriptedModelProvider::new([]).with_reconciliations([ScriptedReconciliation {
                orphan: true,
                observed: vec![],
                continuation: Some(vec![
                    content(REST)?,
                    serde_json::from_value(
                        json!({"kind": "completed", "metadata": {"finish": "stop"}}),
                    )?,
                ]),
            }]),
        );
    let result = executor(provider.clone())?
        .execute(identity.input(), journal.as_ref())
        .await;
    if !matches!(&result, Err(acyclic_harness::Error::Invalid(message)) if message == "scripted model exhausted")
        || (provider.requests().len(), provider.reconciliations().len()) != (1, 0)
    {
        return Err(format!(
            "unjournaled turn: {result:?}, {} requests, {} reconciliations",
            provider.requests().len(),
            provider.reconciliations().len(),
        )
        .into());
    }
    Ok(())
}

async fn journaled(reader: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<bool> {
    use tokio::io::AsyncBufReadExt as _;
    let mut lines = tokio::io::BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        if line == JOURNALED {
            return Ok(true);
        }
    }
    Ok(false)
}

// Virtual-clock controls live with the other guard controls, outside the
// deadline scan of scenario sources.
#[path = "../support/resume_readiness_tests.rs"]
mod readiness_tests;

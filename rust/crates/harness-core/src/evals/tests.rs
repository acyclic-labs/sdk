use super::*;
use crate::{
    OperationId,
    agent::{AgentId, AgentPath, AgentRecord, Sender},
    filesystem::MemoryHarnessStorage,
};
use acyclic_fs::{CancellationToken, NativeMaterializationWork, VolumeLimits, WorkBudget};
use acyclic_stream::{AppendRequest, IdempotencyKey, MemoryStream, StreamProviderExt as _};
use std::sync::Arc;

mod export;

type TestResult = Result<(), Box<dyn std::error::Error>>;
fn path(text: &str) -> NamespacePath {
    NamespacePath::from_text_in_profile(
        text,
        acyclic_fs::FilesystemProfile::Portable,
        VolumeLimits::default(),
    )
    .unwrap()
}
fn accounting(work: &NativeMaterializationWork) -> KernelAccounting<'_> {
    KernelAccounting::NativeOriginal {
        work,
        cancellation: work.cancellation(),
    }
}
struct Changes(tokio::sync::mpsc::UnboundedSender<bool>);
impl SourceChangeSink for Changes {
    fn source_changed(
        &self,
        changes: &[SourceChange],
        _: &mut KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        self.0
            .send(matches!(changes, [SourceChange::Name(root)] if root.is_root()))
            .map_err(|_| DemandError::SourceUnavailable)
    }
    fn delivery_lost(&self) {}
}

// Supply authentic retained records with the adjacent boundaries reversed.
struct ReversedBoundaries(Snapshot);
impl Inputs for ReversedBoundaries {
    fn stream(&self) -> &StreamPath {
        self.0.stream()
    }
    fn tail(&self) -> u64 {
        self.0.tail()
    }
    fn entry(&self, sequence: u64) -> Result<(&Record, &ConversationEntry), DemandError> {
        self.0.entry(sequence)
    }
    fn checkpoint(&self, sequence: u64) -> Result<&HistoryCheckpoint, DemandError> {
        self.0.checkpoint(1 - sequence)
    }
}
#[tokio::test]
async fn computed_cases_keep_original_decisions_cut_ranges_and_event_delivery() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let agent = AgentPath::root(&AgentId::from_bytes([73; 16]));
    let stream = agent.conversation()?;
    let storage =
        MemoryHarnessStorage::new(crate::AgentId::from_bytes(agent.id().to_bytes()), 1_000_000)
            .await?;
    let streams = Arc::new(MemoryStream::default());
    let mut entries = Vec::new();
    let baseline = HistoryCheckpoint {
        version: 3,
        live_children: false,
        conversation: 0,
        checkpoint: None,
        context: crate::agent::ContextCut {
            path: acyclic_stream::StreamPath::new(format!("{agent}/context/0"))?,
            tail: 0,
            checkpoint: None,
        },
        volumes: Vec::new(),
    };
    streams
        .append(AppendRequest {
            path: agent.history()?,
            records: vec![serde_json::to_vec(&baseline)?.into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let mut checkpoints = vec![baseline];
    for (inbox, decision) in [
        "Reject the proposed diff.",
        "Override the earlier decision.",
    ]
    .into_iter()
    .enumerate()
    {
        let body = storage
            .stage(
                OperationId::new(),
                &format!("decisions/{inbox}.txt"),
                decision.as_bytes(),
                "text/plain",
                "decision",
            )
            .await?;
        let output = storage
            .stage(
                OperationId::new(),
                &format!("outcomes/{inbox}.json"),
                br#"{"text":"settled"}"#,
                "application/json",
                "outcome",
            )
            .await?;
        let entry = ConversationEntry {
            observed_through: inbox as u64 + 1,
            steering: None,
            inbox: inbox as u64,
            input: TurnInput::Message {
                from: Sender::User {
                    principal: "reviewer".into(),
                },
                id: format!("decision-{inbox}").try_into()?,
                re: None,
                body: Box::new(body),
                attachments: None,
            },
            output,
            outcome: TurnOutcome::Completed,
            goal: None,
            announcements: Vec::new(),
            tool_results: None,
            notices: None,
        };
        let checkpoint = HistoryCheckpoint {
            version: 3,
            live_children: false,
            conversation: inbox as u64 + 1,
            checkpoint: None,
            context: crate::agent::ContextCut {
                path: acyclic_stream::StreamPath::new(format!("{agent}/context/0"))?,
                tail: if inbox == 0 { 1 } else { 0 },
                checkpoint: None,
            },
            volumes: Vec::new(),
        };
        streams
            .append(AppendRequest {
                path: agent.history()?,
                records: vec![serde_json::to_vec(&checkpoint)?.into()],
                if_tail: None,
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        checkpoints.push(checkpoint);
        streams
            .append(AppendRequest {
                path: stream.clone(),
                records: vec![entry.encode()?.into()],
                if_tail: None,
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        entries.push(entry);
    }
    let source = Arc::new(StreamProvider::new(Arc::clone(&streams), &agent)?.into_source());
    let cancel = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
        .map_err(|(error, _)| error)?;
    let reference = source.reference(&mut accounting(&work)).await?;
    streams
        .append(AppendRequest {
            path: acyclic_stream::StreamPath::new("unrelated/companion")?,
            records: vec![b"unrelated publication".to_vec().into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    assert_eq!(
        reference,
        source.reference(&mut accounting(&work)).await?,
        "unrelated state must not advance an evaluation cut"
    );
    let provider = StreamProvider::new(Arc::clone(&streams), &agent)?;
    let snapshot = provider
        .snapshot(
            provider.reference(&mut accounting(&work)).await?,
            ViewQuery::Open(&path("/00000000000000000000.json")),
            &mut accounting(&work),
        )
        .await?;
    let mut encoded = Vec::new();
    write_case(&snapshot, 0, &mut encoded)?;
    assert!(!encoded.is_empty());
    encoded.clear();
    assert!(matches!(
        write_case(&ReversedBoundaries(snapshot), 0, &mut encoded),
        Err(DemandError::StaleSource)
    ));
    assert!(
        encoded.is_empty(),
        "invalid boundaries must fail before writing case bytes"
    );
    let first = source
        .list_page(reference.clone(), &path("/"), None, 1, &work, &cancel)
        .await?;
    assert_eq!(first.entries.len(), 1);
    assert_eq!(
        first.entries[0].name.as_bytes(),
        b"00000000000000000000.json"
    );
    let next = first.next.clone().ok_or("missing prefix continuation")?;
    drop(first);
    assert!(
        source
            .list_page(
                reference.clone(),
                &path("/other"),
                Some(next.clone()),
                1,
                &work,
                &cancel,
            )
            .await
            .is_err(),
        "an interior cursor retains the original directory binding"
    );
    let last = source
        .list_page(reference.clone(), &path("/"), Some(next), 1, &work, &cancel)
        .await?;
    assert_eq!(
        last.entries[0].name.as_bytes(),
        b"00000000000000000001.json"
    );
    assert!(last.next.is_none());
    drop(last);
    for (sequence, entry) in entries.iter().enumerate() {
        let file = path(&format!("/{sequence:020}.json"));
        let node = source
            .lookup(reference.clone(), &file, &work, &cancel)
            .await?
            .ok_or("missing case")?;
        let output = source
            .read_range(
                reference.clone(),
                &file,
                node.version,
                0,
                u64::MAX,
                &work,
                &cancel,
            )
            .await?;
        let case: serde_json::Value = serde_json::from_slice(&output.bytes)?;
        assert_eq!(case["cut"]["through"], sequence as u64);
        assert_eq!(case["cut"]["stream"], stream.as_str());
        assert_eq!(
            case["outcome"]["checkpoint"],
            serde_json::to_value(&checkpoints[sequence + 1])?
        );
        assert_eq!(
            case["cut"]["checkpoint"],
            serde_json::to_value(&checkpoints[sequence])?
        );
        assert_eq!(
            case["decision"]["input"],
            serde_json::to_value(&entry.input)?
        );
        assert_eq!(
            case["outcome"]["output"],
            serde_json::to_value(&entry.output)?
        );
        let range = source
            .read_range(
                reference.clone(),
                &file,
                node.version,
                7,
                19,
                &work,
                &cancel,
            )
            .await?;
        assert_eq!(range.bytes.as_slice(), &output.bytes.as_slice()[7..26]);
    }
    export::selected_case_export(Arc::clone(&source), &work).await?;
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let watch = source
        .watch(Arc::new(Changes(send)), &work, &cancel)?
        .ok_or("missing subscription")?;
    assert!(
        receive.recv().await.ok_or("opening delivery ended")?,
        "opening invalidation must cover the directory namespace"
    );
    streams
        .append(AppendRequest {
            path: stream,
            records: vec![entries[0].encode()?.into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    assert!(
        receive.recv().await.ok_or("append delivery ended")?,
        "an appended case must invalidate cached directory names"
    );
    watch.stop(&mut accounting(&work))?;
    assert!(watch.is_stopped());
    assert!(
        source
            .lookup(
                reference,
                &path("/00000000000000000002.json"),
                &work,
                &cancel
            )
            .await?
            .is_none()
    );
    let current = source.reference(&mut accounting(&work)).await?;
    assert!(
        matches!(
            source
                .lookup(current, &path("/00000000000000000002.json"), &work, &cancel)
                .await,
            Err(DemandError::StaleSource)
        ),
        "a missing original descriptor must not fall back to current context"
    );
    streams
        .append(AppendRequest {
            path: agent.history()?,
            records: vec![b"{\"version\":99}".to_vec().into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let current = source.reference(&mut accounting(&work)).await?;
    assert!(
        matches!(
            source
                .lookup(current, &path("/00000000000000000002.json"), &work, &cancel)
                .await,
            Err(DemandError::InvalidRequest)
        ),
        "a malformed original descriptor must not produce a case"
    );
    Ok(())
}

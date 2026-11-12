use super::*;
use crate::{
    AgentId, OperationId,
    agent::{AgentPath, AgentRecord, Sender},
    filesystem::MemoryHarnessStorage,
};
use acyclic_fs::{CancellationToken, NativeMaterializationWork, VolumeLimits, WorkBudget};
use acyclic_stream::{AppendRequest, IdempotencyKey, MemoryStream, StreamProviderExt as _};
use std::sync::Arc;

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
struct Changes(tokio::sync::mpsc::UnboundedSender<()>);
impl ViewChangeSink<()> for Changes {
    fn changed(&self, _: &(), _: &mut KernelAccounting<'_>) -> Result<(), DemandError> {
        self.0.send(()).map_err(|_| DemandError::SourceUnavailable)
    }
    fn delivery_lost(&self) {}
}

#[tokio::test]
async fn computed_cases_keep_original_decisions_cut_ranges_and_event_delivery() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let agent = AgentPath::root(&AgentId::from_bytes([73; 16]));
    let stream = agent.conversation()?;
    let storage = MemoryHarnessStorage::new(agent.id(), 1_000_000).await?;
    let streams = Arc::new(MemoryStream::default());
    let mut entries = Vec::new();
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
    let provider = StreamProvider::new(Arc::clone(&streams), &agent)?;
    let source = StreamProvider::new(Arc::clone(&streams), &agent)?.into_source();
    let cancel = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
        .map_err(|(error, _)| error)?;
    let reference = source.reference(&mut accounting(&work)).await?;
    let mut first = source
        .list_page(reference.clone(), &path("/"), None, 1, &work, &cancel)
        .await?;
    assert_eq!(first.entries.len(), 1);
    assert_eq!(
        first.entries[0].name.as_bytes(),
        b"00000000000000000000.json"
    );
    let next = first.next.take().ok_or("missing next")?;
    drop(first);
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
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let watch = provider
        .watch(Arc::new(Changes(send)), &mut accounting(&work))?
        .ok_or("missing subscription")?;
    receive.recv().await.ok_or("opening delivery ended")?;
    streams
        .append(AppendRequest {
            path: stream,
            records: vec![entries[0].encode()?.into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    receive.recv().await.ok_or("append delivery ended")?;
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
    Ok(())
}

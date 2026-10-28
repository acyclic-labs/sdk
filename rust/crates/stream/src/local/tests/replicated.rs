use super::super::*;
use crate::replicated::{
    AppendIf, CommandJournal, JournalFrame, JournalPage, ReplicaClock, Replicated,
};

struct Advancing {
    port: port::LocalPort,
    advance: Option<AppendRequest>,
    pages: Vec<usize>,
    proposals: Vec<crate::OwnedBytes>,
    lose_reply: bool,
}

#[async_trait]
impl CommandJournal for Advancing {
    async fn read(&mut self, from: u64, page: usize) -> Result<JournalPage, StreamError> {
        let received = self.port.read(from, page).await?;
        self.pages.push(received.entries.len());
        if let Some(advance) = self.advance.take() {
            self.port.stream.append(advance).await?;
        }
        Ok(received)
    }
    async fn append_if(
        &mut self,
        expected: u64,
        frames: Vec<JournalFrame>,
    ) -> Result<AppendIf, StreamError> {
        self.proposals
            .extend(frames.iter().map(|frame| frame.command.clone()));
        let outcome = self.port.append_if(expected, frames).await?;
        if self.lose_reply && matches!(outcome, AppendIf::Accepted { .. }) {
            return Err(StreamError::Unavailable);
        }
        Ok(outcome)
    }
    fn changed(
        &mut self,
        after: u64,
    ) -> crate::BoxProviderFuture<'static, Result<(), StreamError>> {
        self.port.changed(after)
    }
}

fn append(path: &StreamPath, sequence: u64) -> AppendRequest {
    AppendRequest {
        path: path.clone(),
        records: vec![sequence.to_le_bytes().to_vec().into()],
        if_tail: Some(sequence),
        idempotency_key: IdempotencyKey::unique(),
    }
}

#[tokio::test]
async fn real_journal_pages_pin_a_head_while_the_source_advances()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let source = LocalStream::open(root.path(), LocalStreamLimits::default()).await?;
    let path = StreamPath::new("paged/events")?;
    for sequence in 0..65 {
        source.append(append(&path, sequence)).await?;
    }
    for (index, budget) in [1, 8, 64].into_iter().enumerate() {
        let head = 65 + index as u64 * 2;
        let clock = Arc::new(ReplicaClock::default());
        let provider = MemoryStream::new_with_commit_clock(
            MemoryLimits::default(),
            Arc::new(SystemUnixMillisClock),
            clock.clone(),
        );
        let mut replica = Replicated::new(
            Advancing {
                port: port::LocalPort {
                    stream: source.clone(),
                },
                advance: Some(append(&path, head)),
                pages: Vec::new(),
                proposals: Vec::new(),
                lose_reply: false,
            },
            provider,
            clock,
            HEADER_BYTES as u64,
        );
        replica.refresh(budget).await?;
        assert_eq!(replica.provider.tail(path.clone()).await?, head);
        assert_eq!(source.tail(path.clone()).await?, head + 1);
        assert!(replica.journal.pages.iter().all(|count| *count <= budget));
        replica.refresh(budget).await?;
        assert_eq!(replica.provider.tail(path.clone()).await?, head + 1);
        let wait = replica.changed()?;
        let mut wait = std::pin::pin!(wait);
        assert!(futures::poll!(wait.as_mut()).is_pending());
        source.append(append(&path, head + 1)).await?;
        wait.await?;
        replica.refresh(budget).await?;
        assert_eq!(replica.provider.tail(path.clone()).await?, head + 2);
    }
    Ok(())
}

#[tokio::test]
async fn a_lost_conditional_proposal_keeps_its_exact_command()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let source = LocalStream::open(root.path(), LocalStreamLimits::default()).await?;
    let path = StreamPath::new("conditional/events")?;
    source.append(append(&path, 0)).await?;
    let clock = Arc::new(ReplicaClock::default());
    let mut replica = Replicated::new(
        Advancing {
            port: port::LocalPort {
                stream: source.clone(),
            },
            advance: Some(append(&path, 1)),
            pages: Vec::new(),
            proposals: Vec::new(),
            lose_reply: false,
        },
        MemoryStream::new_with_commit_clock(MemoryLimits::default(), clock.clone(), clock.clone()),
        clock,
        HEADER_BYTES as u64,
    );
    let mut request = append(&path, 2);
    request.if_tail = None;
    let crate::journal::Outcome::Append(AppendOutcome::Committed(receipt)) = replica
        .execute(Command::Append(request.clone()), None, 1)
        .await?
    else {
        panic!("conditional proposal did not commit")
    };
    assert_eq!(receipt.start, 2);
    assert_eq!(receipt.tail, 3);
    assert_eq!(replica.journal.proposals.len(), 2);
    assert_eq!(replica.journal.proposals[0], replica.journal.proposals[1]);
    assert_eq!(replica.provider.tail(path.clone()).await?, 3);
    drop(replica);
    drop(source);
    let reopened = LocalStream::open(root.path(), LocalStreamLimits::default()).await?;
    let bytes = std::fs::metadata(root.path().join(JOURNAL_FILE))?.len();
    let AppendOutcome::Committed(replayed) = reopened.append(request).await? else {
        panic!("accepted proposal was not retained")
    };
    assert_eq!(replayed.start, 2);
    assert_eq!(reopened.tail(path).await?, 3);
    assert_eq!(
        std::fs::metadata(root.path().join(JOURNAL_FILE))?.len(),
        bytes
    );
    Ok(())
}

#[tokio::test]
async fn a_lost_durable_reply_closes_the_replica_and_recovers_the_original_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let source = LocalStream::open(root.path(), LocalStreamLimits::default()).await?;
    let path = StreamPath::new("lost-reply/events")?;
    let request = append(&path, 0);
    let clock = Arc::new(ReplicaClock::default());
    let mut replica = Replicated::new(
        Advancing {
            port: port::LocalPort {
                stream: source.clone(),
            },
            advance: None,
            pages: Vec::new(),
            proposals: Vec::new(),
            lose_reply: true,
        },
        MemoryStream::new_with_commit_clock(MemoryLimits::default(), clock.clone(), clock.clone()),
        clock,
        HEADER_BYTES as u64,
    );
    assert!(matches!(
        replica
            .execute(Command::Append(request.clone()), None, 1)
            .await,
        Err(StreamError::Unavailable)
    ));
    assert!(matches!(
        replica.refresh(1).await,
        Err(StreamError::Unavailable)
    ));
    assert!(matches!(replica.changed(), Err(StreamError::Unavailable)));
    assert_eq!(replica.journal.proposals.len(), 1);
    drop(replica);
    drop(source);
    let reopened = LocalStream::open(root.path(), LocalStreamLimits::default()).await?;
    let bytes = std::fs::metadata(root.path().join(JOURNAL_FILE))?.len();
    let observed = reopened
        .inspect_idempotency(request.idempotency_key.clone())
        .await?
        .ok_or("accepted identity missing after reopen")?;
    let AppendOutcome::Committed(receipt) = reopened.append(request).await? else {
        panic!("accepted proposal was not retained")
    };
    assert_eq!(
        observed.outcome,
        crate::IdempotencyOutcome::Append(AppendOutcome::Committed(receipt.clone()))
    );
    assert_eq!(receipt.tail, 1);
    assert_eq!(reopened.tail(path).await?, 1);
    assert_eq!(
        std::fs::metadata(root.path().join(JOURNAL_FILE))?.len(),
        bytes
    );
    Ok(())
}

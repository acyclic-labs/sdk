use super::super::*;
use crate::replicated::{
    AppendIf, CommandJournal, JournalFrame, JournalPage, ReplicaClock, Replicated,
};

struct Advancing {
    port: port::LocalPort,
    advance: Option<AppendRequest>,
    pages: Vec<usize>,
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
        self.port.append_if(expected, frames).await
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

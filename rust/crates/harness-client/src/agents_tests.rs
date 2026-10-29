use super::*;
use crate::{AgentId, Phase};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{
    AppendRecord, AppendRequest, BoxProviderFuture, MemoryStream, StreamError, StreamProvider,
    runtime::PlatformExecutor,
    session::{self, transport},
};
use futures::{StreamExt as _, future::poll_fn};
use std::sync::Arc;

struct Endpoint(Arc<MemoryStream>);
impl transport::Transport for Endpoint {
    fn session(
        &self,
        source: transport::Requests,
    ) -> BoxProviderFuture<'_, std::result::Result<transport::Responses, StreamError>> {
        Box::pin(async move {
            session::serve(
                self.0.clone(),
                session::testing::authority(),
                "node".into(),
                session::Limits::default(),
                source.map(Ok).boxed(),
            )
        })
    }
}

async fn append(
    memory: &MemoryStream,
    path: StreamPath,
    records: Vec<AppendRecord>,
) -> Result<(), StreamError> {
    memory
        .append(AppendRequest {
            path,
            records,
            if_tail: None,
            idempotency_key: acyclic_stream::IdempotencyKey::unique(),
        })
        .await?;
    Ok(())
}

async fn status(client: &mut AgentClient) -> Result<StatusRecord, AgentClientError> {
    loop {
        if let AgentEvent::Status { status, .. } = poll_fn(|cx| client.poll_next(cx)).await? {
            return Ok(status);
        }
    }
}

#[tokio::test]
async fn real_json_status_bootstraps_head_filters_keys_and_rejects_invalid_payload()
-> Result<(), Box<dyn std::error::Error>> {
    // The deadline bounds a broken event path; readiness always comes from the
    // production session, never from a sleep or a periodic check.
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let memory = Arc::new(MemoryStream::default());
        let agent = AgentPath::root(&AgentId::from_bytes([8; 16]));
        let path = agent.status()?;
        append(&memory, agent.conversation()?, vec!["past".into()]).await?;
        let history = (0..64)
            .map(|cursor| {
                StatusRecord {
                    phase: Phase::Running,
                    cursor,
                    conversation: 1,
                }
                .append_record()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let original = history[0].value.clone();
        append(&memory, path.clone(), history).await?;
        let pool = Client::with_transport(
            Arc::new(Endpoint(memory.clone())),
            Arc::new(|_| Box::pin(async { Err(StreamError::Unavailable) })),
            Arc::new(PlatformExecutor),
        );
        pool.register_credential(0, session::testing::credential())?;
        let mut observer = AgentClient::new(pool.clone(), agent, 1, 0, 0)?;
        assert_eq!(status(&mut observer).await?.cursor, 63);
        assert_eq!(observer.current_status()?.unwrap().cursor, 63);
        // ALL retains the exact JSON bytes; LATEST never requires a nested
        // protobuf value in the stored record.
        let mut all = pool
            .follow_records(acyclic_stream::FollowRequest::new(path.clone(), 0))
            .await?;
        assert_eq!(all.next().await.ok_or("ALL ended")??.value, original);
        append(
            &memory,
            path.clone(),
            vec![AppendRecord::latest(
                "not status JSON",
                "another-key",
                true,
            )?],
        )
        .await?;
        let attention = StatusRecord {
            phase: Phase::WaitingHost {
                host: "codex".into(),
            },
            cursor: 64,
            conversation: 1,
        };
        append(&memory, path.clone(), vec![attention.append_record()?]).await?;
        assert_eq!(status(&mut observer).await?, attention);
        append(
            &memory,
            path,
            vec![AppendRecord::latest("invalid JSON", "status", true)?],
        )
        .await?;
        assert!(matches!(
            status(&mut observer).await,
            Err(AgentClientError::Record(_))
        ));
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await?
}

use super::*;
use crate::replicated::{CommandJournal as _, ReplayObserver};
use futures::FutureExt as _;

async fn append(provider: &PostgresStream, path: &str) -> Result<(), StreamError> {
    provider
        .append(AppendRequest {
            path: StreamPath::new(path)?,
            records: vec!["value".into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    Ok(())
}

struct AppendDuringReplay<'a> {
    writer: &'a PostgresStream,
    appended: bool,
}

impl ReplayObserver for AppendDuringReplay<'_> {
    async fn observe<'a>(
        &'a mut self,
        _: &'a MemoryStream,
        _: &'a WriteRequest,
        after: bool,
    ) -> Result<(), StreamError> {
        if after && !self.appended {
            self.appended = true;
            append(self.writer, "paged/extra").await?;
        }
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database through STORAGE_POSTGRES_URL"]
async fn paged_refresh_retains_observed_head_after_consuming_its_wake()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database = std::env::var("STORAGE_POSTGRES_URL")?;
    let namespace = format!("paged-refresh-{}", uuid::Uuid::new_v4());
    let follower = PostgresStream::connect(&database, &namespace).await?;
    let writer = PostgresStream::connect(&database, &namespace).await?;
    // Hold the real follower's replica lock while another connection publishes
    // enough commands to require a second page. Its LISTEN driver remains live.
    let mut session = follower.inner.session.lock().await;
    for _ in 0..=PAGE {
        append(&writer, "paged/history").await?;
    }
    let target = writer.inner.session.lock().await.cursor;
    let Session {
        client,
        cursor,
        observed_head,
        publish,
        read,
        wakes,
    } = &mut *session;
    let mut replica = Replicated::new(
        PgJournal {
            client,
            publish,
            read,
            namespace: &namespace,
            wakes,
            head: observed_head,
        },
        follower.inner.core.clone(),
        Arc::clone(&follower.inner.clock),
        *cursor,
    );
    replica
        .refresh_with(
            PAGE,
            &mut AppendDuringReplay {
                writer: &writer,
                appended: false,
            },
        )
        .await?;
    // The refresh keeps its first snapshot's cut, while its second real SQL
    // page sees the append made during replay of the first page.
    assert_eq!(replica.cursor, target);
    *cursor = replica.cursor;
    drop(replica);
    assert!(*observed_head > *cursor);
    // Expose exactly the consumed-notification case, without another write,
    // query, readiness loop or background task repairing the pending work.
    wakes.borrow_and_update();
    let mut forgotten = *cursor;
    let mut journal = PgJournal {
        client,
        publish,
        read,
        namespace: &namespace,
        wakes,
        head: &mut forgotten,
    };
    // Negative control: reconstructing from only the applied cursor parks.
    assert!(journal.changed(*cursor).now_or_never().is_none());
    journal.head = observed_head;
    assert_eq!(journal.changed(*cursor).now_or_never(), Some(Ok(())));
    let mut replica = Replicated::new(
        journal,
        follower.inner.core.clone(),
        Arc::clone(&follower.inner.clock),
        *cursor,
    );
    replica.refresh(PAGE).await?;
    *cursor = replica.cursor;
    assert_eq!(
        replica
            .provider
            .tail(StreamPath::new("paged/extra")?)
            .await?,
        1
    );
    Ok(())
}

struct CompetingPublication<'a> {
    writer: &'a PostgresStream,
    request: AppendRequest,
    published: bool,
}

impl ReplayObserver for CompetingPublication<'_> {
    async fn observe<'a>(
        &'a mut self,
        _: &'a MemoryStream,
        _: &'a WriteRequest,
        after: bool,
    ) -> Result<(), StreamError> {
        if !after && !self.published {
            let writer = self.writer.inner.session.lock().await;
            let free = writer
                .client
                .query_one(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended($1,1937006962))",
                    &[&self.writer.inner.namespace],
                )
                .await
                .map_err(unavailable)?;
            assert!(
                free.get::<_, bool>(0),
                "planning retained the publication fence"
            );
            drop(writer);
            self.published = true;
            self.writer.append(self.request.clone()).await?;
        }
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database through STORAGE_POSTGRES_URL"]
async fn stale_publication_replans_without_retaining_the_losing_identity()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database = std::env::var("STORAGE_POSTGRES_URL")?;
    let namespace = format!("stale-publication-{}", uuid::Uuid::new_v4());
    let provider = PostgresStream::connect(&database, &namespace).await?;
    let writer = PostgresStream::connect(&database, &namespace).await?;
    let request = AppendRequest {
        path: StreamPath::new("contended")?,
        records: vec!["original".into()],
        if_tail: Some(0),
        idempotency_key: IdempotencyKey::unique(),
    };
    // The other real session commits after our read, before our decision.
    // Retaining a read-time database fence would deadlock this exact control.
    let mut session = provider.inner.session.lock().await;
    let Session {
        client,
        cursor,
        observed_head,
        publish,
        read,
        wakes,
    } = &mut *session;
    let mut replica = Replicated::new(
        PgJournal {
            client,
            publish,
            read,
            namespace: &namespace,
            wakes,
            head: observed_head,
        },
        provider.inner.core.clone(),
        Arc::clone(&provider.inner.clock),
        *cursor,
    );
    let result = replica
        .execute_with(
            WriteRequest::Append(request.clone()),
            None,
            PAGE,
            &mut CompetingPublication {
                writer: &writer,
                request: AppendRequest {
                    idempotency_key: IdempotencyKey::unique(),
                    ..request.clone()
                },
                published: false,
            },
        )
        .await?;
    assert!(matches!(
        result.outcome,
        WriteOutcome::Append(AppendOutcome::TailConflict { actual_tail: 1 })
    ));
    *cursor = replica.cursor;
    drop(replica);
    let count = client
        .query_one(
            "SELECT count(*) FROM acyclic_stream_journal WHERE namespace=$1",
            &[&namespace],
        )
        .await?;
    assert_eq!(count.get::<_, i64>(0), 1);
    drop(session);
    assert!(
        provider
            .inspect_idempotency(request.idempotency_key.clone())
            .await?
            .is_none()
    );
    let corrected = AppendRequest {
        if_tail: Some(1),
        ..request
    };
    let accepted = provider.append(corrected.clone()).await?;
    assert_eq!(writer.append(corrected).await?, accepted);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database through STORAGE_POSTGRES_URL"]
async fn cancelled_acknowledgement_replays_the_committed_identity()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database = std::env::var("STORAGE_POSTGRES_URL")?;
    let namespace = format!("cancelled-ack-{}", uuid::Uuid::new_v4());
    let provider = PostgresStream::connect(&database, &namespace).await?;
    let peer = PostgresStream::connect(&database, &namespace).await?;
    let (listener, mut notifications) = tokio_postgres::connect(&database, NoTls).await?;
    let (sent, received) = tokio::sync::oneshot::channel();
    let observed = namespace.clone();
    let notification = tokio::spawn(async move {
        while let Some(message) =
            futures::future::poll_fn(|cx| notifications.poll_message(cx)).await
        {
            match message {
                Ok(AsyncMessage::Notification(value)) if value.payload() == observed => {
                    let _ = sent.send(());
                    return;
                }
                Ok(_) => {}
                Err(_) => return,
            }
        }
    });
    listener
        .batch_execute("LISTEN acyclic_stream_commits")
        .await?;
    let request = AppendRequest {
        path: StreamPath::new("accepted")?,
        records: vec!["original".into()],
        if_tail: Some(0),
        idempotency_key: IdempotencyKey::unique(),
    };
    {
        let pending = provider.append(request.clone());
        tokio::pin!(pending);
        assert!(futures::poll!(&mut pending).is_pending());
        // PostgreSQL delivers NOTIFY only after COMMIT. Abandon the caller's
        // acknowledgement after this real event, without polling it to success.
        received.await?;
    }
    notification.await?;
    let receipt = peer.append(request.clone()).await?;
    assert_eq!(provider.append(request).await?, receipt);
    let rows = peer
        .inner
        .session
        .lock()
        .await
        .client
        .query_one(
            "SELECT count(*) FROM acyclic_stream_journal WHERE namespace=$1",
            &[&namespace],
        )
        .await?;
    assert_eq!(rows.get::<_, i64>(0), 1);
    Ok(())
}

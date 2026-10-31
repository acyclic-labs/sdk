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

async fn sql_case(replica: &Replicated<PgJournal>, case: &str) -> Result<(), StreamError> {
    replica
        .journal
        .client
        .query_one(
            "SELECT set_config('application_name',$1,false)",
            &[&format!("publication-count-{case}")],
        )
        .await
        .map_err(unavailable)?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database through STORAGE_POSTGRES_URL"]
async fn publication_uses_only_required_sql_observations()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database = std::env::var("STORAGE_POSTGRES_URL")?;
    let namespace = format!("publication-count-{}", uuid::Uuid::new_v4());
    let provider = PostgresStream::connect(&database, &namespace).await?;
    let writer = PostgresStream::connect(&database, &namespace).await?;
    // The held session excludes its background refresh. Server statement logs
    // distinguish each real operation without a proxy port or timing inference.
    let mut replica = provider.inner.session.lock().await;
    let original = AppendRequest {
        path: StreamPath::new("accepted")?,
        records: vec!["original".into()],
        if_tail: Some(0),
        idempotency_key: IdempotencyKey::unique(),
    };
    sql_case(&replica, "accepted").await?;
    let accepted = replica
        .execute(WriteRequest::Append(original.clone()), None, PAGE)
        .await?
        .outcome;
    let WriteOutcome::Append(AppendOutcome::Committed(receipt)) = &accepted else {
        return Err(StreamError::InvalidResponse.into());
    };
    let envelope = provider.read_commit(receipt.commit_id).await?;
    assert!(envelope.committed_at_micros > 0);
    sql_case(&replica, "inspect-retained").await?;
    let observed = provider
        .inspect_idempotency(original.idempotency_key.clone())
        .now_or_never()
        .ok_or(StreamError::Unavailable)??
        .ok_or(StreamError::NotFound)?;
    assert_eq!(
        observed.outcome,
        IdempotencyOutcome::Append(AppendOutcome::Committed(receipt.clone()))
    );
    assert_eq!(
        observed.revision,
        replica
            .provider
            .inspect_idempotency(original.idempotency_key.clone())
            .await?
            .ok_or(StreamError::NotFound)?
            .revision
    );
    sql_case(&replica, "replay").await?;
    assert_eq!(
        replica
            .execute(WriteRequest::Append(original.clone()), None, PAGE)
            .await?
            .outcome,
        accepted
    );
    sql_case(&replica, "mismatch").await?;
    let changed = AppendRequest {
        records: vec!["changed".into()],
        ..original.clone()
    };
    assert!(matches!(
        replica
            .execute(WriteRequest::Append(changed), None, PAGE)
            .await,
        Err(StreamError::IdempotencyMismatch)
    ));
    sql_case(&replica, "refusal").await?;
    let refused = AppendRequest {
        if_tail: Some(99),
        idempotency_key: IdempotencyKey::unique(),
        ..original.clone()
    };
    assert!(matches!(
        replica
            .execute(WriteRequest::Append(refused.clone()), None, PAGE)
            .await?
            .outcome,
        WriteOutcome::Append(AppendOutcome::TailConflict { actual_tail: 1 })
    ));
    assert!(
        replica
            .provider
            .inspect_idempotency(refused.idempotency_key)
            .await?
            .is_none()
    );

    // Negative control: the cached refusal is false at the server. No LISTEN
    // delivery or background task is allowed to repair the cache for this call.
    append(&writer, "remote").await?;
    let remote = AppendRequest {
        path: StreamPath::new("remote")?,
        if_tail: Some(1),
        idempotency_key: IdempotencyKey::unique(),
        ..original.clone()
    };
    sql_case(&replica, "stale-refusal").await?;
    assert_eq!(
        replica.provider.tail(remote.path.clone()).await,
        Err(StreamError::NotFound)
    );
    assert!(matches!(
        replica
            .execute(WriteRequest::Append(remote), None, PAGE)
            .await?
            .outcome,
        WriteOutcome::Append(AppendOutcome::Committed(AppendReceipt {
            start: 1,
            tail: 2,
            ..
        }))
    ));
    append(&writer, "other").await?;
    sql_case(&replica, "stale-cas").await?;
    let competing = AppendRequest {
        if_tail: None,
        idempotency_key: IdempotencyKey::unique(),
        ..original.clone()
    };
    assert!(matches!(
        replica
            .execute(WriteRequest::Append(competing), None, PAGE)
            .await?
            .outcome,
        WriteOutcome::Append(AppendOutcome::Committed(AppendReceipt {
            start: 1,
            tail: 2,
            ..
        }))
    ));
    // The retained cursor is current, but its proposal clock predates this
    // already elapsed deadline. A condition conflict must not mask expiry.
    let expired_conflict = CommitRequest {
        deadline_unix_millis: Some(1),
        conditions: vec![CommitCondition::Tail {
            path: original.path.clone(),
            expected: 0,
        }],
        mutations: vec![CommitMutation::Append {
            path: StreamPath::new("deadline/control")?,
            records: vec!["deadline".into()],
        }],
        idempotency_key: IdempotencyKey::unique(),
    };
    replica.set_proposal_clock(0);
    let before = replica.cursor;
    sql_case(&replica, "expired-conflict").await?;
    assert!(matches!(
        replica
            .execute(WriteRequest::Commit(expired_conflict.clone()), None, PAGE)
            .await,
        Err(StreamError::DeadlineElapsed)
    ));
    assert_eq!(replica.cursor, before);
    assert!(
        replica
            .provider
            .inspect_idempotency(expired_conflict.idempotency_key.clone())
            .await?
            .is_none()
    );
    sql_case(&replica, "retry-expired-conflict").await?;
    assert!(matches!(
        replica
            .execute(
                WriteRequest::Commit(CommitRequest {
                    deadline_unix_millis: None,
                    conditions: vec![CommitCondition::Tail {
                        path: original.path.clone(),
                        expected: 2,
                    }],
                    ..expired_conflict
                }),
                None,
                PAGE,
            )
            .await?
            .outcome,
        WriteOutcome::Commit(CommitOutcome::Committed(_))
    ));
    sql_case(&replica, "late-deadline").await?;
    let expired = AppendRequest {
        if_tail: None,
        idempotency_key: IdempotencyKey::unique(),
        ..original.clone()
    };
    assert!(matches!(
        replica
            .execute(WriteRequest::Append(expired.clone()), Some(1), PAGE)
            .await,
        Err(StreamError::DeadlineElapsed)
    ));
    assert!(
        replica
            .provider
            .inspect_idempotency(expired.idempotency_key.clone())
            .await?
            .is_none()
    );
    sql_case(&replica, "retry-refused").await?;
    assert!(matches!(
        replica
            .execute(WriteRequest::Append(expired), None, PAGE)
            .await?
            .outcome,
        WriteOutcome::Append(AppendOutcome::Committed(AppendReceipt {
            start: 2,
            tail: 3,
            ..
        }))
    ));
    sql_case(&replica, "done").await?;
    let rows = replica
        .journal
        .client
        .query_one(
            "SELECT count(*) FROM acyclic_stream_journal WHERE namespace=$1",
            &[&namespace],
        )
        .await?;
    assert_eq!(rows.get::<_, i64>(0), 7);
    let unseen = AppendRequest {
        path: StreamPath::new("unobserved")?,
        if_tail: Some(0),
        idempotency_key: IdempotencyKey::unique(),
        ..original.clone()
    };
    let remote = writer.append(unseen.clone()).await?;
    assert!(
        replica
            .provider
            .inspect_idempotency(unseen.idempotency_key.clone())
            .await?
            .is_none()
    );
    sql_case(&replica, "inspect-unobserved").await?;
    {
        let inspect = provider.inspect_idempotency(unseen.idempotency_key);
        let mut inspect = std::pin::pin!(inspect);
        assert!(futures::poll!(inspect.as_mut()).is_pending());
        drop(replica);
        assert_eq!(
            inspect.await?.ok_or(StreamError::NotFound)?.outcome,
            IdempotencyOutcome::Append(remote)
        );
    }
    drop(provider);
    drop(writer);
    let reopened = PostgresStream::connect(&database, &namespace).await?;
    assert_eq!(reopened.read_commit(receipt.commit_id).await?, envelope);
    assert_eq!(
        reopened.append(original).await?,
        match accepted {
            WriteOutcome::Append(outcome) => outcome,
            _ => return Err(StreamError::InvalidResponse.into()),
        }
    );
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
    let mut replica = follower.inner.session.lock().await;
    for _ in 0..=PAGE {
        append(&writer, "paged/history").await?;
    }
    let target = writer.inner.session.lock().await.cursor;
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
    assert!(replica.journal.head > replica.cursor);
    // Expose exactly the consumed-notification case, without another write,
    // query, readiness loop or background task repairing the pending work.
    replica.journal.wakes.borrow_and_update();
    let observed_head = replica.journal.head;
    let cursor = replica.cursor;
    replica.journal.head = cursor;
    // Negative control: reconstructing from only the applied cursor parks.
    assert!(replica.journal.changed(cursor).now_or_never().is_none());
    replica.journal.head = observed_head;
    assert_eq!(replica.journal.changed(cursor).now_or_never(), Some(Ok(())));
    replica.refresh(PAGE).await?;
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
                .journal
                .client
                .query_one(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended($1,1937006962))",
                    &[&writer.journal.namespace],
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
    let mut replica = provider.inner.session.lock().await;
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
    let count = replica
        .journal
        .client
        .query_one(
            "SELECT count(*) FROM acyclic_stream_journal WHERE namespace=$1",
            &[&namespace],
        )
        .await?;
    assert_eq!(count.get::<_, i64>(0), 1);
    drop(replica);
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
        .journal
        .client
        .query_one(
            "SELECT count(*) FROM acyclic_stream_journal WHERE namespace=$1",
            &[&namespace],
        )
        .await?;
    assert_eq!(rows.get::<_, i64>(0), 1);
    Ok(())
}

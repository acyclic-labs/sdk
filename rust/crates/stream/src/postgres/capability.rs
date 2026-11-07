//! Requires a disposable PostgreSQL database through `STORAGE_POSTGRES_URL`.
//! Select this entire capability with the `postgres::capability::` test filter.
//! SQL observation counts additionally use server statement logs tagged with
//! the `publication-count-` application name; enable `log_statement = 'all'`
//! and include `%a` in `log_line_prefix` when collecting those observations.

use super::*;
use crate::replicated::ReplayObserver;
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
async fn publication_uses_only_required_sql_observations()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database = std::env::var("STORAGE_POSTGRES_URL")?;
    let namespace = format!("publication-count-{}", uuid::Uuid::new_v4());
    let provider = PostgresStream::connect(&database, &namespace).await?;
    let writer = PostgresStream::connect(&database, &namespace).await?;
    // The held session excludes its background refresh. Server statement logs
    // distinguish each real operation without a proxy port or timing inference.
    let mut replica = provider.inner.session.lock().await;
    // Invalid reads must finish even while the real SQL session is unavailable.
    // Moving validation behind refresh parks here instead of returning a refusal.
    let mut future = provider.inner.core.revisions.current();
    future.index += 1;
    for (limit, key) in [
        (0, None),
        (u32::MAX, None),
        (
            1,
            Some(crate::ReadKeySeek {
                key: vec![0; crate::MAX_RECORD_KEY_BYTES + 1].into(),
                before: 1,
            }),
        ),
    ] {
        assert!(matches!(
            provider
                .read(ReadRequest {
                    path: StreamPath::new("invalid")?,
                    from: 0,
                    limit,
                    key,
                    at_least: crate::RevisionCut::of(future.clone()),
                })
                .now_or_never(),
            Some(Err(StreamError::LimitExceeded))
        ));
    }
    // Foreign cuts cannot be satisfied by SQL or a later local publication.
    // Keep the real session held: a refresh or lock wait fails these controls.
    let mut foreign = future.clone();
    foreign.incarnation ^= 1;
    let foreign = crate::RevisionCut::of(foreign);
    assert!(matches!(
        provider
            .read(ReadRequest {
                path: StreamPath::new("foreign")?,
                from: 0,
                limit: 1,
                key: None,
                at_least: foreign.clone(),
            })
            .now_or_never(),
        Some(Err(StreamError::RevisionForeign))
    ));
    assert!(matches!(
        provider
            .follow(FollowRequest {
                at_least: foreign.clone(),
                ..FollowRequest::new(StreamPath::new("foreign")?, 0)
            })
            .now_or_never(),
        Some(Err(StreamError::RevisionForeign))
    ));
    assert!(matches!(
        provider
            .heads(HeadsRequest {
                retained: Default::default(),
                durable_through: None,
                projection: HeadProjection::History,
                paths: [StreamPath::new("foreign")?].into(),
                children: None,
                limit: 1,
                at_least: foreign,
            })
            .now_or_never(),
        Some(Err(StreamError::RevisionForeign))
    ));
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
    let retained = HeadsRequest {
        retained: [crate::RetainedRecord {
            commit_id: receipt.commit_id,
            path: original.path.clone(),
            sequence: receipt.start,
        }]
        .into_iter()
        .collect(),
        durable_through: None,
        projection: HeadProjection::History,
        paths: Default::default(),
        children: None,
        limit: 1,
        at_least: crate::RevisionCut::of(receipt.revision.clone()),
    };
    sql_case(&replica, "retained-record-hit").await?;
    let view = provider
        .heads(retained.clone())
        .now_or_never()
        .ok_or(StreamError::Unavailable)??;
    view.validate_for(&retained)?;
    let CommittedMutation::Append(original_append) = &envelope.mutations[0] else {
        return Err(StreamError::InvalidResponse.into());
    };
    let observed = view.retained[0]
        .record
        .as_ref()
        .ok_or(StreamError::NotFound)?;
    assert_eq!(observed, &original_append.records[0]);
    assert_eq!(
        observed.value.as_ptr(),
        original_append.records[0].value.as_ptr()
    );
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
    writer
        .wait_applied(&RevisionCut::of(replica.provider.revisions.current()))
        .await?;
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
            .execute(WriteRequest::Append(remote.clone()), None, PAGE)
            .await,
        Err(StreamError::PrefixNotRetained)
    ));
    assert!(
        replica
            .provider
            .inspect_idempotency(remote.idempotency_key.clone())
            .await?
            .is_none()
    );
    sql_case(&replica, "explicit-stale-refusal-resend").await?;
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
    writer
        .wait_applied(&RevisionCut::of(replica.provider.revisions.current()))
        .await?;
    append(&writer, "other").await?;
    sql_case(&replica, "stale-cas").await?;
    let competing = AppendRequest {
        if_tail: None,
        idempotency_key: IdempotencyKey::unique(),
        ..original.clone()
    };
    assert!(matches!(
        replica
            .execute(WriteRequest::Append(competing.clone()), None, PAGE)
            .await,
        Err(StreamError::PrefixNotRetained)
    ));
    assert!(!replica.is_failed());
    assert!(
        replica
            .provider
            .inspect_idempotency(competing.idempotency_key.clone())
            .await?
            .is_none()
    );
    sql_case(&replica, "explicit-stale-cas-resend").await?;
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
    writer
        .wait_applied(&RevisionCut::of(replica.provider.revisions.current()))
        .await?;
    let remote = writer.append(unseen.clone()).await?;
    assert!(
        replica
            .provider
            .inspect_idempotency(unseen.idempotency_key.clone())
            .await?
            .is_none()
    );
    sql_case(&replica, "inspect-unobserved").await?;
    let AppendOutcome::Committed(remote_receipt) = &remote else {
        return Err(StreamError::InvalidResponse.into());
    };
    let remote_receipt = remote_receipt.clone();
    let remote_retained = HeadsRequest {
        retained: [crate::RetainedRecord {
            commit_id: remote_receipt.commit_id,
            path: unseen.path.clone(),
            sequence: remote_receipt.start,
        }]
        .into_iter()
        .collect(),
        // Keep the original covered cut: a future minimum must not be what
        // forces this stale replica to observe the remote publication.
        ..retained
    };
    {
        let mut heads = std::pin::pin!(provider.heads(remote_retained.clone()));
        assert!(
            futures::poll!(heads.as_mut()).is_pending(),
            "a stale retained miss escaped without the current journal observation"
        );
        let inspect = provider.inspect_idempotency(unseen.idempotency_key);
        let mut inspect = std::pin::pin!(inspect);
        assert!(futures::poll!(inspect.as_mut()).is_pending());
        drop(replica);
        // Both futures are queued on the same real session. Poll them together
        // so its first granted owner can release it to the next observation.
        let (view, inspected) = futures::try_join!(heads, inspect)?;
        assert_eq!(
            inspected.ok_or(StreamError::NotFound)?.outcome,
            IdempotencyOutcome::Append(remote)
        );
        view.validate_for(&remote_retained)?;
        let observed = view.retained[0]
            .record
            .as_ref()
            .ok_or(StreamError::NotFound)?;
        assert_eq!(observed.commit_id, remote_receipt.commit_id);
        assert_eq!(observed.value.as_ref(), b"original");
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
async fn stale_publication_returns_without_retaining_the_losing_identity()
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
        .await;
    assert!(matches!(result, Err(StreamError::PrefixNotRetained)));
    assert!(!replica.is_failed());
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
    let AppendOutcome::Committed(receipt) = &accepted else {
        return Err(StreamError::InvalidResponse.into());
    };
    writer
        .wait_applied(&RevisionCut::of(receipt.revision.clone()))
        .await?;
    assert_eq!(writer.append(corrected).await?, accepted);
    Ok(())
}

#[tokio::test]
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

#[test]
fn refresh_producer_retirement_ends_public_waits()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    struct Observer(std::sync::atomic::AtomicBool);
    impl futures::task::ArcWake for Observer {
        fn wake_by_ref(observer: &Arc<Self>) {
            observer.0.store(true, Ordering::Release);
        }
    }
    let database = std::env::var("STORAGE_POSTGRES_URL")?;
    for producer in 0..4 {
        for park in [true, false] {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let remote = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let namespace = format!("refresh-retirement-{}", uuid::Uuid::new_v4());
            let provider = runtime.block_on(PostgresStream::connect(&database, &namespace))?;
            let retire = |runtime: &tokio::runtime::Runtime| {
                let task = provider
                    .inner
                    .tasks
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(producer);
                task.abort();
                assert!(
                    runtime
                        .block_on(task)
                        .is_err_and(|error| error.is_cancelled())
                );
            };
            let writer = remote.block_on(PostgresStream::connect(&database, &namespace))?;
            remote.block_on(append(&writer, "retirement"))?;
            let path = StreamPath::new("retirement")?;
            if park {
                let mut follow = runtime.block_on(provider.follow(FollowRequest {
                    progress: true,
                    ..FollowRequest::new(path.clone(), 1)
                }))?;
                assert!(matches!(
                    runtime.block_on(follow.next()),
                    Some(Ok(FollowUpdate::Opening { tail: 1 }))
                ));
                let mut next = follow.next();
                assert!(
                    runtime
                        .block_on(async { (&mut next).now_or_never() })
                        .is_none()
                );

                let mut future = provider.inner.core.revisions.current();
                future.index += 1;
                let cut = RevisionCut::of(future);
                let mut waits: [BoxProviderFuture<'_, Result<(), StreamError>>; 3] = [
                    Box::pin(provider.wait_applied(&cut).map(|result| result.map(|_| ()))),
                    Box::pin(
                        provider
                            .heads(HeadsRequest {
                                retained: Default::default(),
                                durable_through: None,
                                projection: HeadProjection::Tail,
                                paths: [path.clone()].into(),
                                children: None,
                                limit: 1,
                                at_least: cut.clone(),
                            })
                            .map(|result| result.map(|_| ())),
                    ),
                    Box::pin(
                        provider
                            .read(ReadRequest {
                                path: path.clone(),
                                from: 0,
                                limit: 1,
                                key: None,
                                at_least: cut.clone(),
                            })
                            .map(|result| result.map(|_| ())),
                    ),
                ];
                for waiting in &mut waits {
                    runtime.block_on(async {
                    // Poll the public operation first. Its refresh takes the
                    // FIFO session mutex; reacquiring that mutex joins the real
                    // SQL response before we inspect the revision park.
                    tokio::select! {
                        biased;
                        result = waiting.as_mut() => panic!("future cut completed before application: {result:?}"),
                        session = provider.inner.session.lock() => drop(session),
                    }
                    assert!(waiting.as_mut().now_or_never().is_none());
                });
                }
                // Retain the real session owner so no SQL completion or mutex
                // release can conceal a missing retirement observation.
                let session = runtime.block_on(provider.inner.session.lock());
                let mut tail = Box::pin(provider.tail(path.clone()));
                let mut publication = Box::pin(provider.commit(CommitRequest {
                    conditions: vec![CommitCondition::Tail {
                        path: path.clone(),
                        expected: 1,
                    }],
                    mutations: vec![CommitMutation::Append {
                        path: path.clone(),
                        records: vec!["retained-session".into()],
                    }],
                    deadline_unix_millis: None,
                    idempotency_key: IdempotencyKey::unique(),
                }));
                let observers = [
                    Arc::new(Observer(AtomicBool::new(false))),
                    Arc::new(Observer(AtomicBool::new(false))),
                ];
                let wakers = observers
                    .each_ref()
                    .map(|observer| futures::task::waker(observer.clone()));
                runtime.block_on(async {
                    assert!(
                        tail.as_mut()
                            .poll(&mut std::task::Context::from_waker(&wakers[0]))
                            .is_pending()
                    );
                    assert!(
                        publication
                            .as_mut()
                            .poll(&mut std::task::Context::from_waker(&wakers[1]))
                            .is_pending()
                    );
                });
                // Retire each original producer independently, then the executor.
                // The provider, cursor and remote writer remain alive throughout.
                if producer == 3 {
                    drop(runtime);
                } else {
                    retire(&runtime);
                }
                for waiting in &mut waits {
                    assert_eq!(
                        waiting.as_mut().now_or_never(),
                        Some(Err(StreamError::Unavailable)),
                        "retired refresh producer left a public revision wait parked"
                    );
                }
                assert_eq!(
                    tail.as_mut().now_or_never(),
                    Some(Err(StreamError::Unavailable)),
                    "retired producer left a public session refresh parked"
                );
                assert_eq!(
                    publication.as_mut().now_or_never(),
                    Some(Err(StreamError::Unavailable)),
                    "retired producer left a conditional publication parked"
                );
                for observer in observers {
                    assert!(
                        observer.0.load(Ordering::Acquire),
                        "retirement lost a public session waiter wake"
                    );
                }
                drop(session);
                assert!(matches!(
                    next.now_or_never(),
                    Some(Some(Err(StreamError::Unavailable)))
                ));
            } else {
                // connect returns immediately after spawning refresh on this
                // current-thread executor; the producer has never been polled.
                if producer == 3 {
                    drop(runtime);
                } else {
                    retire(&runtime);
                }
            }
            assert_eq!(
                provider
                    .wait_applied(&RevisionCut::default())
                    .now_or_never(),
                Some(Err(StreamError::Unavailable)),
                "retired refresh producer served a later observer"
            );
            assert!(matches!(
                provider.tail(path.clone()).now_or_never(),
                Some(Err(StreamError::Unavailable))
            ));
            // The authority still makes progress through another member. Its
            // successful conditional publication cannot resurrect the lost replica.
            assert!(matches!(
                remote.block_on(writer.append(AppendRequest {
                    path,
                    records: vec!["later".into()],
                    if_tail: Some(1),
                    idempotency_key: IdempotencyKey::unique(),
                }))?,
                AppendOutcome::Committed(_)
            ));
        }
    }
    Ok(())
}

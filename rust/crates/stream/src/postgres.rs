//! PostgreSQL durability and notification transport for the canonical Stream core.
//!
//! A database owns one ordered command journal. Each process keeps a shared core
//! replica, refreshed in bounded pages. Transactions serialize publication across
//! processes; LISTEN/NOTIFY drives live replicas without timers or polling.

use crate::{
    journal::{Command, Outcome, decode_command, replay},
    *,
};
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{Mutex, watch};
use tokio_postgres::{AsyncMessage, Client, GenericClient, NoTls, Statement};

const PAGE: i64 = 64;
const SCHEMA: &str = include_str!("postgres/schema.sql");

#[derive(Default)]
struct Clock(AtomicU64);
impl UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

struct Session {
    client: Client,
    cursor: i64,
    publish: Statement,
}

struct Inner {
    core: MemoryStream,
    clock: Arc<Clock>,
    session: Mutex<Session>,
    namespace: String,
    failed: AtomicBool,
    failure: watch::Sender<bool>,
    // Dropping the owner closes both connections and their drivers. No task
    // retains the owner while idle, so this does not form a reference cycle.
    tasks: std::sync::Mutex<Vec<tokio::task::AbortHandle>>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        for task in self
            .tasks
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            task.abort();
        }
    }
}

/// Cloneable PostgreSQL-backed Stream provider. Clones share two connections,
/// independent of the number of streams and subscriptions.
#[derive(Clone)]
pub struct PostgresStream {
    inner: Arc<Inner>,
}

fn unavailable(error: impl std::fmt::Display) -> StreamError {
    tracing::error!(%error, "Postgres Stream operation failed; reopen PostgresStream::connect with the same database and namespace");
    StreamError::Unavailable
}

impl PostgresStream {
    /// Opens a namespace in a caller-owned database and installs its journal.
    /// The connection string is never included in diagnostics. Use a TLS proxy
    /// or Unix socket when the database is outside the local trust boundary.
    pub async fn connect(database: &str, namespace: &str) -> Result<Self, StreamError> {
        if namespace.is_empty() || namespace.len() > 256 || namespace.contains('\0') {
            return Err(StreamError::InvalidArgument);
        }
        let (client, connection) = tokio_postgres::connect(database, NoTls)
            .await
            .map_err(unavailable)?;
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .query_one(
                "SELECT set_config('application_name', $1, false)",
                &[&format!("acyclic-stream-query-{namespace}")],
            )
            .await
            .map_err(unavailable)?;
        client.batch_execute(SCHEMA).await.map_err(unavailable)?;
        let publish = client
            .prepare(
                "SELECT revision, at_millis, accepted FROM acyclic_stream_publish($1,$2,$3,$4)",
            )
            .await
            .map_err(unavailable)?;
        let (listener, mut notifications) = tokio_postgres::connect(database, NoTls)
            .await
            .map_err(unavailable)?;
        let (wake, mut wakes) = tokio::sync::mpsc::channel(1);
        let notified_namespace = namespace.to_owned();
        let notification_driver = tokio::spawn(async move {
            while let Some(message) =
                futures::future::poll_fn(|cx| notifications.poll_message(cx)).await
            {
                match message {
                    Ok(AsyncMessage::Notification(notification))
                        if notification.payload() == notified_namespace =>
                    {
                        let _ = wake.try_send(());
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });
        listener
            .batch_execute("LISTEN acyclic_stream_commits")
            .await
            .map_err(unavailable)?;
        listener
            .query_one(
                "SELECT set_config('application_name', $1, false)",
                &[&format!("acyclic-stream-listener-{namespace}")],
            )
            .await
            .map_err(unavailable)?;
        let clock = Arc::new(Clock::default());
        // Both planning and recorded admission use the transaction's database
        // clock. A process wall-clock skew cannot override SQL admission.
        let core = MemoryStream::new_with_commit_clock(
            MemoryLimits::default(),
            clock.clone(),
            clock.clone(),
        );
        let (failure, _) = watch::channel(false);
        let inner = Arc::new(Inner {
            core,
            clock,
            namespace: namespace.to_owned(),
            session: Mutex::new(Session {
                client,
                cursor: 0,
                publish,
            }),
            failed: AtomicBool::new(false),
            failure,
            tasks: std::sync::Mutex::new(vec![
                driver.abort_handle(),
                notification_driver.abort_handle(),
            ]),
        });
        let provider = Self { inner };
        // LISTEN is active before the initial catch-up, closing the startup gap.
        provider.refresh().await?;
        let weak = Arc::downgrade(&provider.inner);
        let refresh = tokio::spawn(async move {
            // Keep the LISTEN client alive, but never the provider owner.
            let _listener = listener;
            while wakes.recv().await.is_some() {
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let provider = Self { inner };
                if provider.refresh().await.is_err() {
                    provider.fail();
                    return;
                }
            }
            if let Some(inner) = weak.upgrade() {
                Self { inner }.fail();
            }
        });
        provider
            .inner
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(refresh.abort_handle());
        Ok(provider)
    }

    fn fail(&self) {
        self.inner.failed.store(true, Ordering::Release);
        self.inner.failure.send_replace(true);
    }

    fn check(&self) -> Result<(), StreamError> {
        if self.inner.failed.load(Ordering::Acquire) {
            Err(StreamError::Unavailable)
        } else {
            Ok(())
        }
    }

    async fn catch_up(
        &self,
        client: &impl GenericClient,
        cursor: &mut i64,
    ) -> Result<(), StreamError> {
        loop {
            let rows = client.query(
                "SELECT revision, at_millis, command FROM acyclic_stream_journal WHERE namespace=$1 AND revision>$2 ORDER BY revision LIMIT $3",
                &[&self.inner.namespace, &*cursor, &PAGE],
            ).await.map_err(unavailable)?;
            if rows.is_empty() {
                return Ok(());
            }
            for row in rows {
                let revision: i64 = row.get(0);
                let millis: i64 = row.get(1);
                let bytes: Vec<u8> = row.get(2);
                self.inner.clock.0.store(
                    u64::try_from(millis).map_err(unavailable)?,
                    Ordering::Release,
                );
                let command = decode_command(&bytes).map_err(unavailable)?;
                // Refusals are durable decisions too. Replaying them preserves
                // the core's exact retry and conflict semantics without copying
                // any admission or mutation mechanism into the adapter.
                if matches!(
                    replay(&self.inner.core, command).await,
                    Err(StreamError::Unavailable | StreamError::InvalidResponse)
                ) {
                    self.fail();
                    return Err(unavailable("committed command could not be replayed"));
                }
                *cursor = revision;
            }
        }
    }

    async fn refresh(&self) -> Result<(), StreamError> {
        self.check()?;
        let mut session = self.inner.session.lock().await;
        let Session { client, cursor, .. } = &mut *session;
        self.catch_up(client, cursor).await
    }

    async fn apply(&self, command: Command) -> Result<Outcome, StreamError> {
        match command {
            Command::Append(request) => self.inner.core.append(request).await.map(Outcome::Append),
            Command::Fork(request) => self.inner.core.fork(request).await.map(Outcome::Fork),
            Command::Commit(request) => self.inner.core.commit(request).await.map(Outcome::Commit),
        }
    }

    async fn publish(
        &self,
        mut command: Command,
        deadline: Option<u64>,
    ) -> Result<Outcome, StreamError> {
        // Validate before submitting any database work or retaining a command.
        match &mut command {
            Command::Append(request) => {
                crate::request::append_digest(request)?;
            }
            Command::Fork(request) => {
                crate::request::fork_digest(request)?;
            }
            Command::Commit(request) => {
                crate::request::commit_digest(request)?;
            }
        }
        let provider = self.clone();
        // Publication owns its completion across caller cancellation. Only
        // committed journal entries can ever reach the visible core.
        tokio::spawn(async move { provider.publish_owned(command, deadline).await })
            .await
            .map_err(unavailable)?
    }

    async fn publish_owned(
        &self,
        command: Command,
        deadline: Option<u64>,
    ) -> Result<Outcome, StreamError> {
        self.check()?;
        let mut session = self.inner.session.lock().await;
        let Session {
            client,
            cursor,
            publish,
        } = &mut *session;
        // Acquire the namespace lock before taking the catch-up snapshot. A
        // caller-configured repeatable-read default could otherwise preserve a
        // snapshot from before a competing publisher released the lock.
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
            .start()
            .await
            .map_err(unavailable)?;
        // Hash collisions only serialize unrelated namespaces; they cannot
        // weaken isolation. The lock is held through the durable SQL commit.
        tx.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 1937006962))",
            &[&self.inner.namespace],
        )
        .await
        .map_err(unavailable)?;
        self.catch_up(&tx, cursor).await?;
        // The shared core gives an existing retry receipt precedence over an
        // elapsed deadline, including cross-operation identity mismatches.
        if command.has_replay(&self.inner.core).await? {
            return self.apply(command).await;
        }
        let bytes = command.encode_to_vec();
        // The journal's protobuf oneof adds a field tag and length delimiter;
        // the public request ceiling remains MAX_COMMAND_BYTES.
        if bytes.len() > crate::wire_codec::requests::block_len(MAX_COMMAND_BYTES) {
            return Err(StreamError::LimitExceeded);
        }
        // Clock, physical cursor CAS, insertion, notification and the final
        // acceptance deadline share one server-side statement. The transaction
        // fence still serializes catch-up/publication; Stream conditions remain
        // in the canonical core.
        let deadline = deadline.map(|value| i64::try_from(value).unwrap_or(i64::MAX));
        let row = tx
            .query_one(
                &*publish,
                &[&self.inner.namespace, &*cursor, &bytes, &deadline],
            )
            .await
            .map_err(|error| {
                if error
                    .as_db_error()
                    .is_some_and(|error| error.code().code() == "PST01")
                {
                    StreamError::DeadlineElapsed
                } else {
                    unavailable(error)
                }
            })?;
        if !row.get::<_, bool>(2) {
            // A cursor mismatch is impossible under this publication fence.
            // The future shared replica owns refresh/replan, not the SQL adapter.
            self.fail();
            return Err(unavailable(
                "journal changed while holding its namespace fence",
            ));
        }
        let revision: i64 = row.get(0);
        let at = u64::try_from(row.get::<_, i64>(1)).map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        self.inner.clock.0.store(at, Ordering::Release);
        let result = self.apply(command).await;
        if matches!(
            result,
            Err(StreamError::Unavailable | StreamError::InvalidResponse)
        ) {
            self.fail();
            return result;
        }
        *cursor = revision;
        result
    }

    fn guarded<T: Send + 'static>(
        &self,
        records: BoxProviderStream<'static, Result<T, StreamError>>,
    ) -> BoxProviderStream<'static, Result<T, StreamError>> {
        let failure = self.inner.failure.subscribe();
        // The returned subscription owns the shared connections until dropped.
        Box::pin(futures::stream::unfold(
            (records, failure, false, self.clone()),
            |(mut records, mut failure, ended, owner)| async move {
                if ended {
                    return None;
                }
                if *failure.borrow() {
                    return Some((
                        Err(StreamError::Unavailable),
                        (records, failure, true, owner),
                    ));
                }
                tokio::select! {
                    value = records.next() => value.map(|value| (value, (records, failure, false, owner))),
                    _ = failure.changed() => Some((Err(StreamError::Unavailable), (records, failure, true, owner))),
                }
            },
        ))
    }
}

#[async_trait]
impl StreamProvider for PostgresStream {
    fn operation_claims(&self) -> &OperationClaims {
        self.inner.core.operation_claims()
    }
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.refresh().await?;
        self.inner.core.inspect_idempotency(key).await
    }
    async fn head(&self, path: StreamPath) -> Result<crate::PathHead, StreamError> {
        self.refresh().await?;
        let mut head = self.inner.core.head(path).await?;
        head.durable = head.tail;
        Ok(head)
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.refresh().await?;
        self.inner.core.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.refresh().await?;
        self.inner.core.bounds(path).await
    }
    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        let tail = self.tail(path).await?;
        Ok(StreamWatermarks {
            accepted: tail,
            durable: tail,
        })
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        match self.publish(Command::Append(request), None).await? {
            Outcome::Append(value) => Ok(value),
            _ => Err(StreamError::InvalidResponse),
        }
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        match self.publish(Command::Fork(request), None).await? {
            Outcome::Fork(value) => Ok(value),
            _ => Err(StreamError::InvalidResponse),
        }
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        match self.publish(Command::Commit(request), None).await? {
            Outcome::Commit(value) => Ok(value),
            _ => Err(StreamError::InvalidResponse),
        }
    }
    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline: u64,
    ) -> Result<CommitOutcome, StreamError> {
        match self
            .publish(Command::Commit(request), Some(deadline))
            .await?
        {
            Outcome::Commit(value) => Ok(value),
            _ => Err(StreamError::InvalidResponse),
        }
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.refresh().await?;
        self.inner.core.read(request).await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.refresh().await?;
        self.inner.core.read_commit(id).await
    }
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.refresh().await?;
        self.inner.core.children_page(request).await
    }
    async fn follow(&self, request: crate::FollowRequest) -> Result<crate::FollowStream, StreamError> {
        self.refresh().await?;
        Ok(self.guarded(self.inner.core.follow(request).await?))
    }
}

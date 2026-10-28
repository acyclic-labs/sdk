//! PostgreSQL durability and notification transport for the canonical Stream core.
//!
//! A database owns one ordered command journal. Each process keeps a shared core
//! replica, refreshed in bounded pages. Transactions serialize publication across
//! processes; LISTEN/NOTIFY drives live replicas without timers or polling.

use crate::{
    journal::{Command, Outcome},
    replicated::{ReplicaClock, Replicated},
    *,
};
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, watch};
use tokio_postgres::{AsyncMessage, Client, NoTls, Statement};

const PAGE: usize = 64;
mod port;
use port::{PgJournal, Storage};
const SCHEMA: &str = include_str!("postgres/schema.sql");

struct Session {
    client: Client,
    cursor: u64,
    publish: Statement,
    read_fenced: Statement,
    wakes: watch::Receiver<()>,
}

struct Inner {
    core: MemoryStream,
    session: Mutex<Session>,
    clock: Arc<ReplicaClock>,
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
        // The first opener installs the generation; every later replica reads it.
        let generation: Vec<u8> = client
            .query_one(
                "INSERT INTO acyclic_stream_generation(namespace, generation) VALUES ($1, $2)                  ON CONFLICT (namespace) DO UPDATE SET generation = acyclic_stream_generation.generation                  RETURNING generation",
                &[&namespace, &uuid::Uuid::new_v4().as_bytes().as_slice()],
            )
            .await
            .map_err(unavailable)?
            .get(0);
        let generation = <[u8; 16]>::try_from(generation.as_slice()).map_err(unavailable)?;
        let publish = client
            .prepare(
                "SELECT revision, at_millis, accepted FROM acyclic_stream_publish(\
                 $1,$2,$3::bytea[],$4::bigint[],$5::bigint[])",
            )
            .await
            .map_err(unavailable)?;
        let read_fenced = client
            .prepare(
                "SELECT revision, at_millis, command FROM acyclic_stream_read_fenced($1,$2,$3)",
            )
            .await
            .map_err(unavailable)?;
        let (listener, mut notifications) = tokio_postgres::connect(database, NoTls)
            .await
            .map_err(unavailable)?;
        let (wake, wakes) = watch::channel(());
        let notified_namespace = namespace.to_owned();
        let notification_driver = tokio::spawn(async move {
            while let Some(message) =
                futures::future::poll_fn(|cx| notifications.poll_message(cx)).await
            {
                match message {
                    Ok(AsyncMessage::Notification(notification))
                        if notification.payload() == notified_namespace =>
                    {
                        wake.send_replace(());
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
        let clock = Arc::new(ReplicaClock::default());
        // Both planning and recorded admission use the transaction's database
        // clock. A process wall-clock skew cannot override SQL admission.
        // Every process replays the namespace journal in one order, so all
        // replicas share its scope, its generation and revision indexes. The
        // scope comes from the database's own generation row, never from
        // connection settings: replicas that reach one database through
        // different credentials, hosts or parameters share one scope.
        let mut location = generation.to_vec();
        location.extend_from_slice(namespace.as_bytes());
        let core = MemoryStream::new_with_commit_clock(
            MemoryLimits::default(),
            clock.clone(),
            clock.clone(),
        )
        .with_authority(
            crate::AppliedRevisions::storage_scope("postgres", &location),
            u128::from_be_bytes(generation),
        );
        let (failure, _) = watch::channel(false);
        let inner = Arc::new(Inner {
            session: Mutex::new(Session {
                client,
                cursor: 0,
                publish,
                read_fenced,
                wakes,
            }),
            clock,
            namespace: namespace.to_owned(),
            core,
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
            loop {
                // The registered event owns no provider or connection. Release
                // the replica lock and its strong owner before parking.
                let changed = {
                    let Some(inner) = weak.upgrade() else {
                        return;
                    };
                    let mut session = inner.session.lock().await;
                    let Session {
                        client,
                        cursor,
                        publish,
                        read_fenced,
                        wakes,
                    } = &mut *session;
                    let mut replica = Replicated::new(
                        PgJournal {
                            storage: Storage::Connection(client),
                            publish,
                            read_fenced,
                            namespace: &inner.namespace,
                            wakes,
                            at_millis: 0,
                            head: *cursor,
                        },
                        inner.core.clone(),
                        Arc::clone(&inner.clock),
                        *cursor,
                    );
                    replica.changed()
                };
                let result = match changed {
                    Ok(changed) => changed.await,
                    Err(error) => Err(error),
                };
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let provider = Self { inner };
                if result.is_err() || provider.refresh().await.is_err() {
                    provider.fail();
                    return;
                }
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
        // Parked revision waits end too: this replica serves nothing more.
        self.inner.core.revisions.close();
    }

    fn check(&self) -> Result<(), StreamError> {
        if self.inner.failed.load(Ordering::Acquire) {
            Err(StreamError::Unavailable)
        } else {
            Ok(())
        }
    }

    async fn refresh(&self) -> Result<(), StreamError> {
        self.check()?;
        let mut session = self.inner.session.lock().await;
        let Session {
            client,
            cursor,
            publish,
            read_fenced,
            wakes,
        } = &mut *session;
        let mut replica = Replicated::new(
            PgJournal {
                storage: Storage::Connection(client),
                publish,
                read_fenced,
                namespace: &self.inner.namespace,
                wakes,
                at_millis: 0,
                head: *cursor,
            },
            self.inner.core.clone(),
            Arc::clone(&self.inner.clock),
            *cursor,
        );
        let result = replica.refresh(PAGE).await;
        *cursor = replica.cursor;
        drop(replica);
        if result.is_err() {
            self.fail();
        }
        result
    }

    async fn publish(
        &self,
        command: Command,
        deadline: Option<u64>,
    ) -> Result<Outcome, StreamError> {
        let command = command.validated()?;
        let provider = self.clone();
        // This one exact publication retains custody through caller cancellation.
        tokio::spawn(async move {
            provider.check()?;
            let mut session = provider.inner.session.lock().await;
            let Session {
                client,
                cursor,
                publish,
                read_fenced,
                wakes,
            } = &mut *session;
            let transaction = client
                .build_transaction()
                .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
                .start()
                .await
                .map_err(unavailable)?;
            let mut replica = Replicated::new(
                PgJournal {
                    storage: Storage::Fenced(Some(transaction)),
                    publish,
                    read_fenced,
                    namespace: &provider.inner.namespace,
                    wakes,
                    at_millis: 0,
                    head: *cursor,
                },
                provider.inner.core.clone(),
                Arc::clone(&provider.inner.clock),
                *cursor,
            );
            let result = replica.execute(command, deadline, PAGE).await;
            *cursor = replica.cursor;
            drop(replica);
            if matches!(
                result,
                Err(StreamError::Unavailable | StreamError::InvalidResponse)
            ) {
                provider.fail();
            }
            result.map(|execution| execution.outcome)
        })
        .await
        .map_err(unavailable)?
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
    fn context(&self) -> crate::ProviderContext<'_> {
        crate::ProviderContext::new(self.inner.core.context().operation_claims())
    }
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.refresh().await?;
        self.inner.core.inspect_idempotency(key).await
    }
    async fn heads(&self, request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
        request.validate()?;
        self.check()?;
        let revisions = &self.inner.core.revisions;
        if !request.paths.is_empty()
            || request.children.is_some()
            || revisions
                .floor(&request.at_least)?
                .is_some_and(|floor| floor > revisions.current().index)
        {
            // Named observations refresh their snapshot. A covered revision
            // wait needs no unrelated journal read or server clock request.
            self.refresh().await?;
        }
        let projection = request.projection;
        self.inner.core.revisions.wait(&request.at_least).await?;
        let mut view = self.inner.core.capture_heads(&request).await?;
        for head in view.heads.iter_mut().flatten() {
            head.durable = if projection == crate::HeadProjection::Tail {
                0
            } else {
                head.tail
            };
        }
        Ok(view)
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
        let deadline = request.deadline_unix_millis;
        match self.publish(Command::Commit(request), deadline).await? {
            Outcome::Commit(value) => Ok(value),
            _ => Err(StreamError::InvalidResponse),
        }
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.refresh().await?;
        self.inner.core.read(request).await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.check()?;
        match self.inner.core.read_commit(id).await {
            Err(StreamError::NotFound) => {}
            result => return result,
        }
        // Success is immutable; only a missing envelope needs a fresh journal
        // observation. A miss is never cached as an authoritative absence.
        self.refresh().await?;
        self.inner.core.read_commit(id).await
    }
    async fn follow(
        &self,
        request: crate::FollowRequest,
    ) -> Result<crate::FollowStream, StreamError> {
        self.refresh().await?;
        Ok(self.guarded(self.inner.core.follow(request).await?))
    }
}

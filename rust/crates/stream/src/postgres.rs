//! PostgreSQL durability and notification transport for the canonical Stream core.
//!
//! A database owns one ordered command journal. Each process keeps a shared core
//! replica, refreshed in bounded pages. Server-side conditional publication orders
//! processes; LISTEN/NOTIFY drives live replicas without timers or polling.

use crate::{
    replicated::{ReplicaClock, Replicated},
    *,
};
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, watch};
use tokio_postgres::{AsyncMessage, Client, NoTls, Statement};

const PAGE: usize = 64;
#[cfg(test)]
mod capability;
mod port;
use port::PgJournal;
const SCHEMA: &str = include_str!("postgres/schema.sql");

struct Inner {
    core: MemoryStream,
    session: Mutex<Replicated<PgJournal>>,
    failed: AtomicBool,
    failure: watch::Sender<Failure>,
    // Dropping the owner closes both connections and their drivers. No task
    // retains the owner while idle, so this does not form a reference cycle.
    tasks: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
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

struct Failure {
    retired: bool,
    owner: std::sync::Weak<Inner>,
}

// Connection drivers must run during initialization, before the provider
// exists. The original failure cell joins their retirement with owner binding.
struct ConnectionOwner(watch::Sender<Failure>);

impl ConnectionOwner {
    fn retire(failure: &watch::Sender<Failure>) {
        let mut owner = None;
        failure.send_modify(|failure| {
            failure.retired = true;
            owner = failure.owner.upgrade();
            if let Some(inner) = &owner {
                inner.failed.store(true, Ordering::Release);
            }
        });
        if let Some(inner) = owner {
            inner.core.revisions.close();
        }
    }
}

impl Drop for ConnectionOwner {
    fn drop(&mut self) {
        Self::retire(&self.0);
    }
}

// The replica's only refresh producer must publish retirement even if its
// executor drops the task before its first poll or while it is parked.
struct RefreshOwner(std::sync::Weak<Inner>);

impl RefreshOwner {
    fn provider(&self) -> Option<PostgresStream> {
        self.0.upgrade().map(|inner| PostgresStream { inner })
    }
}

impl Drop for RefreshOwner {
    fn drop(&mut self) {
        if let Some(provider) = self.provider() {
            provider.fail();
        }
    }
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
        let (failure, _) = watch::channel(Failure {
            retired: false,
            owner: std::sync::Weak::new(),
        });
        let query_owner = ConnectionOwner(failure.clone());
        let driver = tokio::spawn(async move {
            let _owner = query_owner;
            let _ = connection.await;
        });
        client
            .query_one(
                "SELECT set_config('application_name', $1, false), \
                 set_config('default_transaction_isolation', 'read committed', false)",
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
                 $1,$2,$3::bytea[],$4::bigint[])",
            )
            .await
            .map_err(unavailable)?;
        let read = client
            .prepare(
                "SELECT j.revision,j.at_millis,j.command,h.head,h.at_millis FROM \
                 (SELECT COALESCE(max(revision),0) AS head, \
                 floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS at_millis \
                 FROM acyclic_stream_journal WHERE namespace=$1) h \
                 LEFT JOIN LATERAL (SELECT revision,at_millis,command FROM acyclic_stream_journal \
                 WHERE namespace=$1 AND revision>$2 ORDER BY revision LIMIT $3) j ON true \
                 ORDER BY j.revision",
            )
            .await
            .map_err(unavailable)?;
        let (listener, mut notifications) = tokio_postgres::connect(database, NoTls)
            .await
            .map_err(unavailable)?;
        let (wake, wakes) = watch::channel(());
        let notified_namespace = namespace.to_owned();
        let notification_owner = ConnectionOwner(failure.clone());
        let notification_driver = tokio::spawn(async move {
            let _owner = notification_owner;
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
        // Both planning and recorded admission use the journal's database
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
        let inner = Arc::new(Inner {
            session: Mutex::new(
                Replicated::new(
                    PgJournal {
                        client,
                        head: 0,
                        publish,
                        read,
                        wakes,
                        namespace: namespace.to_owned(),
                    },
                    core.clone(),
                    clock,
                    0,
                )
                .with_acceptance_clock(),
            ),
            core,
            failed: AtomicBool::new(false),
            failure,
            tasks: std::sync::Mutex::new(vec![driver, notification_driver]),
        });
        let provider = Self { inner };
        let mut retired = false;
        provider.inner.failure.send_if_modified(|failure| {
            failure.owner = Arc::downgrade(&provider.inner);
            retired = failure.retired;
            false
        });
        if retired {
            provider.fail();
        }
        // LISTEN is active before the initial catch-up, closing the startup gap.
        provider.refresh().await?;
        let owner = RefreshOwner(Arc::downgrade(&provider.inner));
        let refresh = tokio::spawn(async move {
            // Keep the LISTEN client alive, but never the provider owner.
            let _listener = listener;
            loop {
                // The registered event owns no provider or connection. Release
                // the replica lock and its strong owner before parking.
                let changed = {
                    let Some(provider) = owner.provider() else {
                        return;
                    };
                    let mut replica = provider.inner.session.lock().await;
                    if replica.is_failed() {
                        Err(StreamError::Unavailable)
                    } else {
                        let cursor = replica.cursor;
                        Ok(replica.journal.changed(cursor))
                    }
                };
                let result = match changed {
                    Ok(changed) => changed.await,
                    Err(error) => Err(error),
                };
                let Some(provider) = owner.provider() else {
                    return;
                };
                if result.is_err() || provider.refresh().await.is_err() {
                    return;
                }
            }
        });
        provider
            .inner
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(refresh);
        Ok(provider)
    }

    fn fail(&self) {
        ConnectionOwner::retire(&self.inner.failure);
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
        let mut failure = self.inner.failure.subscribe();
        let result = tokio::select! {
            biased;
            _ = failure.wait_for(|failure| failure.retired) => return Err(StreamError::Unavailable),
            result = async {
                let mut replica = self.inner.session.lock().await;
                replica.refresh(PAGE).await
            } => result,
        };
        if result.is_err() {
            self.fail();
        }
        result
    }

    async fn publish(
        &self,
        command: WriteRequest,
        deadline: Option<u64>,
    ) -> Result<WriteOutcome, StreamError> {
        let span = match &command {
            WriteRequest::Append(_) => crate::native::span!("acyclic.stream.append"),
            WriteRequest::Fork(_) => crate::native::span!("acyclic.stream.fork"),
            WriteRequest::Commit(_) | WriteRequest::CommitAccepted(_) => {
                crate::native::span!("acyclic.stream.commit")
            }
        };
        let completion = acyclic_observability::Completion::new(&span);
        let command = match command.validated() {
            Ok(command) => command,
            Err(error) => return completion.finish(Err(error), StreamError::code),
        };
        self.check()?;
        let mut failure = self.inner.failure.subscribe();
        let provider = self.clone();
        // This one exact publication retains custody through caller cancellation.
        let publication = tokio::spawn(span.scope(completion.observe_output(
            async move {
                let mut replica = provider.inner.session.lock().await;
                provider.check()?;
                let result = replica.execute(command, deadline, PAGE).await;
                if matches!(
                    result,
                    Err(StreamError::Unavailable
                        | StreamError::Refused(_)
                        | StreamError::InvalidResponse)
                ) {
                    provider.fail();
                }
                result.map(|execution| execution.outcome)
            },
            |result| result.as_ref().err().map(StreamError::code),
        )));
        // Losing observation does not cancel the submitted identity. The task
        // retains custody; a retirement result never asserts non-publication.
        tokio::select! {
            biased;
            result = publication => result.map_err(unavailable)?,
            _ = failure.wait_for(|failure| failure.retired) => Err(StreamError::Unavailable),
        }
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
                if failure.borrow().retired {
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
        acyclic_observability::async_operation!(
            "acyclic.stream.inspect_idempotency",
            StreamError::code,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.check()?;
                // A retained terminal binding is immutable; an unobserved key still
                // needs an authoritative cut before absence can be returned.
                if !self.inner.core.has_replay(&key).await? {
                    self.refresh().await?;
                }
                self.inner.core.inspect_idempotency_inner(key).await
            }
        )
    }
    async fn heads(&self, request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
        acyclic_observability::async_operation!(
            "acyclic.stream.heads",
            StreamError::code,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                request.validate()?;
                self.check()?;
                let revisions = &self.inner.core.revisions;
                let floor = revisions.floor(&request.at_least)?;
                let current = !request.paths.is_empty()
                    || request.children.is_some()
                    || floor.is_some_and(|floor| floor > revisions.current().index);
                if current {
                    // Named observations refresh their snapshot. A covered revision
                    // wait needs no unrelated journal read or server clock request.
                    self.refresh().await?;
                }
                self.inner.core.revisions.wait(&request.at_least).await?;
                let view = self.inner.core.capture_heads(&request, true).await?;
                if !current
                    && view
                        .retained
                        .iter()
                        .any(|selected| selected.record.is_none())
                {
                    // Immutable hits need no current observation. A missing
                    // publication or position still needs the journal's cut.
                    self.refresh().await?;
                    return self.inner.core.capture_heads(&request, true).await;
                }
                Ok(view)
            }
        )
    }

    fn write(
        &self,
        request: crate::WriteRequest,
    ) -> crate::BoxProviderFuture<'_, std::result::Result<crate::WriteOutcome, crate::StreamError>>
    {
        Box::pin(async move {
            let deadline = match &request {
                crate::WriteRequest::Commit(request)
                | crate::WriteRequest::CommitAccepted(request) => request.deadline_unix_millis,
                _ => None,
            };
            self.publish(request, deadline).await
        })
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        acyclic_observability::async_operation!(
            "acyclic.stream.read",
            StreamError::code,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let request = request.validate()?;
                let _ = self
                    .inner
                    .core
                    .revisions
                    .floor(&request.request().at_least)?;
                self.refresh().await?;
                self.inner.core.read_inner(request).await
            }
        )
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        acyclic_observability::async_operation!(
            "acyclic.stream.read_commit",
            StreamError::code,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.check()?;
                match self.inner.core.read_commit_inner(id).await {
                    Err(StreamError::NotFound) => {}
                    result => return result,
                }
                // Success is immutable; only a missing envelope needs a fresh journal
                // observation. A miss is never cached as an authoritative absence.
                self.refresh().await?;
                self.inner.core.read_commit_inner(id).await
            }
        )
    }
    async fn follow(
        &self,
        request: crate::FollowRequest,
    ) -> Result<crate::FollowStream, StreamError> {
        acyclic_observability::async_operation!(
            "acyclic.stream.follow",
            StreamError::code,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let _ = self.inner.core.revisions.floor(&request.at_least)?;
                self.refresh().await?;
                Ok(self
                    .inner
                    .core
                    .follow_inner(request)
                    .await?
                    .map_cursor(|cursor| self.guarded(cursor)))
            }
        )
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::NativeStreamProvider for PostgresStream {}

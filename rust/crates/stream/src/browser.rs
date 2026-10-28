//! Strict `IndexedDB` command publication around the existing Rust state machine.

use crate::{
    journal::{Command, Outcome},
    replicated::{ReplicaClock, Replicated},
    *,
};
use async_trait::async_trait;
use futures::{StreamExt, stream};
use indexed_db_futures::{
    database::Database,
    prelude::*,
    transaction::{Transaction, TransactionDurability, TransactionMode, TransactionOptions},
    typed_array::Uint8Array,
};
use std::{rc::Rc, sync::Arc};
use wasm_bindgen::{JsCast, JsValue};

mod follow;
mod notifications;
mod port;
mod publication;
#[cfg(test)]
mod tests;

const JOURNAL: &str = "journal";
const METADATA: &str = "metadata";
/// The database's revision incarnation: 16 big-endian bytes minted with the
/// database itself, so every opener shares one identity and a deleted or
/// cleared database mints a new one.
const INCARNATION: &str = "incarnation";
const METADATA_WORDS: usize = 10;

/// Finite durable recovery/retention and canonical in-memory admission limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrowserStreamLimits {
    /// Maximum retained successful journal commands.
    pub commands: u64,
    /// Maximum retained journal bytes, including sampled times.
    pub journal_bytes: u64,
    /// Canonical Rust state capacity; must match on every opener.
    pub memory: MemoryLimits,
}

impl Default for BrowserStreamLimits {
    fn default() -> Self {
        Self {
            commands: 65_536,
            journal_bytes: 256 * 1024 * 1024,
            memory: MemoryLimits::default(),
        }
    }
}

struct BrowserClock;
impl UnixMillisClock for BrowserClock {
    fn now_unix_millis(&self) -> u64 {
        // Invalid/unavailable clocks fail deadline admission closed.
        js_sys::Date::now().to_string().parse().unwrap_or(u64::MAX)
    }
}

struct Cache {
    provider: MemoryStream,
    cursor: u64,
    valid: bool,
    announced: u64,
}
struct Inner {
    operation_claims: crate::OperationClaims,
    database: Database,
    // Lock order: cache -> memory state or refresh request. The refresh lock is
    // released before acquiring cache; interest/Change dispatch follows release.
    cache: crate::query_sync::Mutex<Cache>,
    clock: Arc<ReplicaClock>,
    limits: BrowserStreamLimits,
    publications: notifications::Publications,
    // Durable authority: advanced only through journal frames IndexedDB has
    // committed. The cache provider stages under the same identity, so a write
    // whose transaction fails never publishes an index a rebuild cannot cover.
    revisions: Arc<crate::AppliedRevisions>,
    #[cfg(test)]
    refreshes: std::cell::Cell<usize>,
    #[cfg(test)]
    follow_reads: std::cell::Cell<usize>,
    #[cfg(test)]
    transactions: std::cell::Cell<usize>,
    #[cfg(test)]
    messages: std::cell::Cell<usize>,
}

/// One account-local durable provider, shared safely across tabs and workers.
#[derive(Clone)]
pub struct BrowserStream {
    inner: Rc<Inner>,
}

fn bounded_buffer(value: JsValue, minimum: usize, maximum: usize) -> Result<Vec<u8>, StreamError> {
    let array = value.dyn_into::<js_sys::Uint8Array>().map_err(backend)?;
    let length = usize::try_from(array.length()).map_err(backend)?;
    if length < minimum || length > maximum {
        return Err(StreamError::Unavailable);
    }
    Ok(array.to_vec())
}

fn backend<T>(_: T) -> StreamError {
    StreamError::Unavailable
}

impl BrowserStream {
    /// Opens one browser-local database. Construction acquires no runtime lease.
    pub async fn open(name: &str, limits: BrowserStreamLimits) -> Result<Self, StreamError> {
        if name.is_empty() || limits.words().contains(&0) || limits.commands > u64::from(u32::MAX) {
            return Err(StreamError::InvalidArgument);
        }
        let minted = u128::from_be_bytes(uuid::Uuid::new_v4().into_bytes());
        let database = Database::open(name)
            .with_version(1_u32)
            .with_on_upgrade_needed(move |_, database| {
                // Runs once, in the transaction that creates the database.
                let store = database.create_object_store(JOURNAL).build()?;
                store
                    .put(Uint8Array::from(minted.to_be_bytes().to_vec()))
                    .with_key(INCARNATION)
                    .primitive()?;
                Ok(())
            })
            .await
            .map_err(backend)?;
        let incarnation = {
            let transaction = database.transaction([JOURNAL]).build().map_err(backend)?;
            let recorded = transaction
                .object_store(JOURNAL)
                .map_err(backend)?
                .get::<JsValue, _, _>(INCARNATION)
                .primitive()
                .map_err(backend)?
                .await
                .map_err(backend)?
                .ok_or(StreamError::Unavailable)?;
            u128::from_be_bytes(
                bounded_buffer(recorded, 16, 16)?
                    .try_into()
                    .map_err(backend)?,
            )
        };
        let clock = Arc::new(ReplicaClock::default());
        let revisions = Arc::new(crate::AppliedRevisions::new(
            crate::AppliedRevisions::storage_scope("browser", name.as_bytes()),
            incarnation,
            0,
        ));
        let provider = staged(limits.memory, Arc::clone(&clock), &revisions);
        let inner = Rc::new(Inner {
            revisions,
            operation_claims: crate::OperationClaims::default(),
            publications: notifications::Publications::new(name)?,
            database,
            cache: crate::query_sync::Mutex::new(Cache {
                provider,
                cursor: 0,
                valid: true,
                announced: 0,
            }),
            clock,
            limits,
            #[cfg(test)]
            refreshes: std::cell::Cell::new(0),
            #[cfg(test)]
            follow_reads: std::cell::Cell::new(0),
            #[cfg(test)]
            transactions: std::cell::Cell::new(0),
            #[cfg(test)]
            messages: std::cell::Cell::new(0),
        });
        inner.publications.bind(&inner);
        Ok(Self { inner })
    }

    fn transaction(&self) -> Result<Transaction<'_>, StreamError> {
        #[cfg(test)]
        self.inner
            .transactions
            .set(self.inner.transactions.get() + 1);
        let mut options = TransactionOptions::new();
        options.set_durability(TransactionDurability::Strict);
        self.inner
            .database
            .transaction([JOURNAL])
            .with_mode(TransactionMode::Readwrite)
            .with_options(options)
            .build()
            .map_err(backend)
    }

    async fn refresh(
        &self,
        cache: &mut Cache,
        changed: &mut publication::Changes,
    ) -> Result<(), StreamError> {
        self.run(cache, changed, None).await.map(|_| ())
    }

    async fn run(
        &self,
        cache: &mut Cache,
        changed: &mut publication::Changes,
        command: Option<(Command, Option<u64>)>,
    ) -> Result<Option<Outcome>, StreamError> {
        if !cache.valid {
            cache.provider = staged(
                self.inner.limits.memory,
                Arc::clone(&self.inner.clock),
                &self.inner.revisions,
            );
            cache.cursor = 0;
        }
        // Cancellation leaves a replaceable cache; only durable replay can
        // restore validity and advance the public revision authority.
        cache.valid = false;
        let port = port::BrowserPort::new(self, self.transaction()?)?;
        let mut replica = Replicated::new(
            port,
            cache.provider.clone(),
            Arc::clone(&self.inner.clock),
            cache.cursor,
        );
        let mut observe = publication::Observer {
            stream: self,
            changed,
            cursor: cache.cursor,
            announced: cache.announced,
            before: None,
        };
        let result = match command {
            Some((command, deadline)) => replica
                .execute_with(command, deadline, 64, &mut observe)
                .await
                .map(|execution| Some(execution.outcome)),
            None => replica.refresh_with(64, &mut observe).await.map(|()| None),
        };
        let result = result?;
        cache.cursor = replica.cursor;
        cache.announced = observe.announced;
        cache.valid = true;
        self.inner
            .revisions
            .advance(cache.provider.revisions.current().index);
        Ok(result)
    }

    async fn with_provider<T>(
        &self,
        operation: impl AsyncFnOnce(&MemoryStream) -> Result<T, StreamError>,
    ) -> Result<T, StreamError> {
        let mut dispatch = publication::Dispatch::new(self);
        let mut cache = self.inner.cache.lock().await;
        dispatch.repair = true;
        let result = self.refresh(&mut cache, &mut dispatch.keys).await;
        dispatch.repair = false;
        dispatch.failed = result.is_err();
        result?;
        operation(&cache.provider).await
    }

    async fn execute(
        &self,
        command: Command,
        deadline: Option<u64>,
    ) -> Result<Outcome, StreamError> {
        let mut dispatch = publication::Dispatch::new(self);
        let mut cache = self.inner.cache.lock().await;
        dispatch.repair = true;
        let result = self
            .run(&mut cache, &mut dispatch.keys, Some((command, deadline)))
            .await;
        dispatch.repair = false;
        dispatch.failed = result.is_err();
        result?.ok_or(StreamError::InvalidResponse)
    }
}

fn staged(
    limits: MemoryLimits,
    clock: Arc<ReplicaClock>,
    durable: &crate::AppliedRevisions,
) -> MemoryStream {
    let mut provider = MemoryStream::new_with_commit_clock(limits, Arc::new(BrowserClock), clock);
    provider.revisions = Arc::new(durable.staging());
    provider
}
impl BrowserStreamLimits {
    fn words(self) -> [u64; 8] {
        [
            self.commands,
            self.journal_bytes,
            self.memory.paths as u64,
            self.memory.path_bytes as u64,
            self.memory.records as u64,
            self.memory.payload_bytes as u64,
            self.memory.commits as u64,
            self.memory.idempotency_results as u64,
        ]
    }
}

#[async_trait(?Send)]
impl StreamProvider for BrowserStream {
    fn context(&self) -> crate::ProviderContext<'_> {
        crate::ProviderContext::new(&self.inner.operation_claims)
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.with_provider(async move |provider| provider.inspect_idempotency(key).await)
            .await
    }
    async fn heads(&self, request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
        request.validate()?;
        if !request.at_least.is_empty() {
            self.wait_revision(&request.at_least).await?;
        }
        self.with_provider(async move |provider| {
            let projection = request.projection;
            let mut view = provider.capture_heads(&request).await?;
            for head in view.heads.iter_mut().flatten() {
                head.durable = if projection == crate::HeadProjection::Tail {
                    0
                } else {
                    head.tail
                };
            }
            view.check_size()?;
            Ok(view)
        })
        .await
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        match self.execute(Command::Append(request), None).await? {
            Outcome::Append(value) => Ok(value),
            _ => Err(StreamError::Unavailable),
        }
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        match self.execute(Command::Fork(request), None).await? {
            Outcome::Fork(value) => Ok(value),
            _ => Err(StreamError::Unavailable),
        }
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        let deadline = request.deadline_unix_millis;
        match self.execute(Command::Commit(request), deadline).await? {
            Outcome::Commit(value) => Ok(value),
            _ => Err(StreamError::Unavailable),
        }
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        if !request.at_least.is_empty() {
            self.wait_revision(&request.at_least).await?;
        }
        self.with_provider(async move |provider| provider.read(request).await)
            .await
    }

    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.with_provider(async move |provider| provider.read_commit(id).await)
            .await
    }
    async fn follow(
        &self,
        request: crate::FollowRequest,
    ) -> Result<crate::FollowStream, StreamError> {
        let crate::FollowRequest {
            path,
            from,
            preceding,
            filter,
            progress,
            when_absent,
            at_least,
        } = request;
        if !at_least.is_empty() {
            self.wait_revision(&at_least).await?;
        }
        if progress {
            self.progress_from(path, from, preceding, when_absent, filter)
                .await
        } else {
            self.follow_from(path, from, preceding, when_absent, filter)
                .await
        }
    }
}

impl BrowserStream {
    async fn wait_revision(
        &self,
        at_least: &crate::RevisionCut,
    ) -> Result<crate::Revision, StreamError> {
        // Park on the durable authority outside the cache lock, so writes can
        // advance it. Refresh first: another tab's commits reach it by replay.
        self.with_provider(async |_| Ok(())).await?;
        self.inner.revisions.wait(at_least).await
    }
}

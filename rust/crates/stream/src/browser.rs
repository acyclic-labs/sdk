//! Strict `IndexedDB` command publication around the existing Rust state machine.

use crate::{
    journal::{Command, decode_command, replay},
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
use std::{
    rc::Rc,
    sync::{Arc, Mutex},
};
use wasm_bindgen::{JsCast, JsValue};

mod follow;
mod notifications;
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
#[derive(Default)]
struct ReplayClock(Mutex<u64>);
impl UnixMillisClock for ReplayClock {
    fn now_unix_millis(&self) -> u64 {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
impl ReplayClock {
    fn set(&self, value: u64) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = value;
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
    clock: Arc<ReplayClock>,
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
        let clock = Arc::new(ReplayClock::default());
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
        transaction: &Transaction<'_>,
        cache: &mut Cache,
        changed: &mut publication::Changes,
    ) -> Result<(u64, u64), StreamError> {
        if !cache.valid {
            // The rebuilt cache replays the committed journal into the same
            // indexes under the same identity, so issued tokens stay valid;
            // a staged index the journal never committed is discarded.
            cache.provider = staged(
                self.inner.limits.memory,
                Arc::clone(&self.inner.clock),
                &self.inner.revisions,
            );
            cache.cursor = 0;
        }
        // Keep invalid throughout every await: cancellation cannot retain a
        cache.valid = false;
        let store = transaction.object_store(JOURNAL).map_err(backend)?;
        let metadata = store
            .get::<JsValue, _, _>(METADATA)
            .primitive()
            .map_err(backend)?
            .await
            .map_err(backend)?;
        let (count, bytes) = match metadata {
            None => (0, 0),
            Some(value) => {
                let words = bounded_buffer(value, METADATA_WORDS * 8, METADATA_WORDS * 8)?
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .map(|word| u64::from_le_bytes(*word))
                    .collect::<Vec<_>>();
                let mut words = words.into_iter();
                let count = words.next().ok_or(StreamError::Unavailable)?;
                let bytes = words.next().ok_or(StreamError::Unavailable)?;
                if !words.eq(self.inner.limits.words()) {
                    return Err(StreamError::InvalidArgument);
                }
                (count, bytes)
            }
        };
        if count > self.inner.limits.commands
            || bytes > self.inner.limits.journal_bytes
            || cache.cursor > count
            || cache.announced > count
        {
            return Err(StreamError::Unavailable);
        }
        while cache.cursor < count {
            let key = format!("{:016x}", cache.cursor);
            let value = store
                .get::<JsValue, _, _>(key.as_str())
                .primitive()
                .map_err(backend)?
                .await
                .map_err(backend)?
                .ok_or(StreamError::Unavailable)?;
            let frame = bounded_buffer(value, 8, MAX_COMMAND_BYTES + 8)?;
            let (time, encoded) = frame.split_at(8);
            let millis = u64::from_le_bytes(time.try_into().map_err(backend)?);
            self.inner.clock.set(millis);
            let command = decode_command(encoded)?;
            let before = if cache.cursor >= cache.announced {
                Some(publication::Before::capture(&cache.provider, &command).await?)
            } else {
                None
            };
            replay(&cache.provider, command).await?;
            if let Some(before) = before {
                before
                    .collect(&cache.provider, changed, &self.inner.publications)
                    .await?;
                cache.announced = cache.cursor + 1;
            }
            cache.cursor += 1;
        }
        cache.valid = true;
        self.inner
            .revisions
            .advance(cache.provider.revisions.current().index);
        Ok((count, bytes))
    }

    async fn with_provider<T>(
        &self,
        operation: impl AsyncFnOnce(&MemoryStream) -> Result<T, StreamError>,
    ) -> Result<T, StreamError> {
        let mut dispatch = publication::Dispatch::new(self);
        let mut cache = self.inner.cache.lock().await;
        let transaction = self.transaction()?;
        dispatch.repair = true;
        let result = self
            .refresh(&transaction, &mut cache, &mut dispatch.keys)
            .await;
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
        let command = command.validated()?;
        let journal = &command;
        let encoded = journal.encode_to_vec();
        let mut dispatch = publication::Dispatch::new(self);
        let mut cache = self.inner.cache.lock().await;
        let transaction = self.transaction()?;
        dispatch.repair = true;
        let result = self
            .refresh(&transaction, &mut cache, &mut dispatch.keys)
            .await;
        dispatch.repair = false;
        dispatch.failed = result.is_err();
        let (count, bytes) = result?;
        let before = publication::Before::capture(&cache.provider, &command).await?;
        let existing = command.has_replay(&cache.provider).await?;
        let millis = BrowserClock.now_unix_millis();
        self.inner.clock.set(millis);
        cache.valid = false;
        dispatch.repair = true;
        let outcome = match command {
            Command::Append(request) => Outcome::Append(cache.provider.append(request).await?),
            Command::Fork(request) => Outcome::Fork(cache.provider.fork(request).await?),
            Command::Commit(mut request) => {
                request.deadline_unix_millis = deadline
                    .into_iter()
                    .chain(request.deadline_unix_millis)
                    .min();
                Outcome::Commit(cache.provider.commit(request).await?)
            }
        };
        if existing
            || matches!(
                outcome,
                Outcome::Append(AppendOutcome::TailConflict { .. })
                    | Outcome::Commit(CommitOutcome::Conflict(_))
            )
        {
            cache.valid = true;
            return Ok(outcome);
        }
        let frame_bytes = u64::try_from(encoded.len() + 8).map_err(backend)?;
        let next_bytes = bytes
            .checked_add(frame_bytes)
            .ok_or(StreamError::Capacity)?;
        if !existing
            && (count >= self.inner.limits.commands || next_bytes > self.inner.limits.journal_bytes)
        {
            return Err(StreamError::Capacity);
        }
        let store = transaction.object_store(JOURNAL).map_err(backend)?;
        let mut frame = millis.to_le_bytes().to_vec();
        frame.extend_from_slice(&encoded);
        let frame = Uint8Array::from(frame);
        let index = format!("{count:016x}");
        store
            .add(frame)
            .with_key(index.as_str())
            .primitive()
            .map_err(backend)?
            .await
            .map_err(backend)?;
        let metadata = [count + 1, next_bytes]
            .into_iter()
            .chain(self.inner.limits.words())
            .flat_map(u64::to_le_bytes)
            .collect::<Vec<_>>();
        store
            .put(Uint8Array::from(metadata))
            .with_key(METADATA)
            .primitive()
            .map_err(backend)?
            .await
            .map_err(backend)?;
        // Wake before the commit await so cancelling that await cannot strand
        // subscribers. Their refresh transaction waits for this one to commit
        // or abort, and same-handle readers also share the cache lock.
        self.inner.publications.publish()?;
        transaction.commit().await.map_err(backend)?;
        before
            .collect(
                &cache.provider,
                &mut dispatch.keys,
                &self.inner.publications,
            )
            .await?;
        cache.cursor = count + 1;
        cache.announced = cache.cursor;
        cache.valid = true;
        self.inner
            .revisions
            .advance(cache.provider.revisions.current().index);
        dispatch.repair = false;
        Ok(outcome)
    }
}

fn staged(
    limits: MemoryLimits,
    clock: Arc<ReplayClock>,
    durable: &crate::AppliedRevisions,
) -> MemoryStream {
    let mut provider = MemoryStream::new_with_commit_clock(limits, Arc::new(BrowserClock), clock);
    provider.revisions = Arc::new(durable.staging());
    provider
}
enum Outcome {
    Append(AppendOutcome),
    Fork(ForkReceipt),
    Commit(CommitOutcome),
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
    fn operation_claims(&self) -> &crate::OperationClaims {
        &self.inner.operation_claims
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.with_provider(async move |provider| provider.inspect_idempotency(key).await)
            .await
    }
    async fn head(&self, path: StreamPath) -> Result<crate::PathHead, StreamError> {
        self.with_provider(async move |provider| {
            let mut head = provider.head(path).await?;
            head.durable = head.tail;
            Ok(head)
        })
        .await
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.with_provider(async move |provider| provider.tail(path).await)
            .await
    }
    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        // tail refreshes from completed strict IndexedDB transactions; an
        // uncommitted cache mutation is never exposed outside its writer.
        let accepted = self.tail(path).await?;
        Ok(StreamWatermarks {
            accepted,
            durable: accepted,
        })
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
            self.wait_applied(&request.at_least).await?;
        }
        self.with_provider(async move |provider| provider.read(request).await)
            .await
    }
    async fn wait_applied(
        &self,
        at_least: &crate::RevisionCut,
    ) -> Result<crate::Revision, StreamError> {
        // Park on the durable authority outside the cache lock, so writes can
        // advance it. Refresh first: another tab's commits reach it by replay.
        self.with_provider(async |_| Ok(())).await?;
        self.inner.revisions.wait(at_least).await
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.with_provider(async move |provider| provider.children_page(request).await)
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
            at_least,
        } = request;
        if !at_least.is_empty() {
            self.wait_applied(&at_least).await?;
        }
        if progress {
            self.progress_from(path, from, preceding, filter).await
        } else {
            self.follow_from(path, from, preceding, filter).await
        }
    }
}

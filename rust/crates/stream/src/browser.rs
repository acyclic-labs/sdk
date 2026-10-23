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

mod discovery;
mod follow;
mod publication;
#[cfg(test)]
mod tests;

const JOURNAL: &str = "journal";
const METADATA: &str = "metadata";
const METADATA_WORDS: usize = 10;

/// Finite durable recovery/retention and canonical in-memory admission limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrowserStreamLimits {
    /// Maximum retained journal commands, including terminal conflicts.
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
    publications: discovery::Publications,
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
        let database = Database::open(name)
            .with_version(1_u32)
            .with_on_upgrade_needed(|_, database| {
                database.create_object_store(JOURNAL).build()?;
                Ok(())
            })
            .await
            .map_err(backend)?;
        let clock = Arc::new(ReplayClock::default());
        let provider = memory(limits.memory, Arc::clone(&clock));
        let inner = Rc::new(Inner {
            operation_claims: crate::OperationClaims::default(),
            publications: discovery::Publications::new(name)?,
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
        changed: &mut std::collections::BTreeSet<publication::Key>,
    ) -> Result<(u64, u64), StreamError> {
        if !cache.valid {
            cache.provider = memory(self.inner.limits.memory, Arc::clone(&self.inner.clock));
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
                before.collect(&cache.provider, changed).await?;
                cache.announced = cache.cursor + 1;
            }
            cache.cursor += 1;
        }
        cache.valid = true;
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
        mut command: Command,
        deadline: Option<u64>,
    ) -> Result<Outcome, StreamError> {
        match &mut command {
            Command::Append(request) => {
                crate::request::validate_records(&request.records)?;
                crate::request::validate_append_size(request)?;
            }
            Command::Fork(request) => crate::request::validate_fork_size(request)?,
            Command::Commit(request) => {
                crate::request::normalize_commit(request)?;
                crate::request::validate_commit_shape(request)?;
            }
        }
        let journal = &command;
        if journal.encoded_len() > MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
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
        let key = match &command {
            Command::Append(request) => request.idempotency_key.clone(),
            Command::Fork(request) => request.idempotency_key.clone(),
            Command::Commit(request) => Some(request.idempotency_key.clone()),
        };
        let has_key = key.is_some();
        let existing = match key {
            Some(key) => cache.provider.inspect_idempotency(key).await?.is_some(),
            None => false,
        };
        let millis = BrowserClock.now_unix_millis();
        self.inner.clock.set(millis);
        cache.valid = false;
        dispatch.repair = true;
        let outcome = match command {
            Command::Append(request) => Outcome::Append(cache.provider.append(request).await?),
            Command::Fork(request) => Outcome::Fork(cache.provider.fork(request).await?),
            Command::Commit(request) => Outcome::Commit(match deadline {
                Some(deadline) => cache.provider.commit_before(request, deadline).await?,
                None => cache.provider.commit(request).await?,
            }),
        };
        if existing
            || matches!(outcome, Outcome::Append(AppendOutcome::TailConflict { .. })) && !has_key
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
        before.collect(&cache.provider, &mut dispatch.keys).await?;
        cache.cursor = count + 1;
        cache.announced = cache.cursor;
        cache.valid = true;
        dispatch.repair = false;
        Ok(outcome)
    }
}

fn memory(limits: MemoryLimits, clock: Arc<ReplayClock>) -> MemoryStream {
    MemoryStream::new_with_commit_clock(limits, Arc::new(BrowserClock), clock)
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
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.with_provider(async move |provider| provider.bounds(path).await)
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
        match self.execute(Command::Commit(request), None).await? {
            Outcome::Commit(value) => Ok(value),
            _ => Err(StreamError::Unavailable),
        }
    }
    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline: u64,
    ) -> Result<CommitOutcome, StreamError> {
        match self
            .execute(Command::Commit(request), Some(deadline))
            .await?
        {
            Outcome::Commit(value) => Ok(value),
            _ => Err(StreamError::Unavailable),
        }
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.with_provider(async move |provider| provider.read(request).await)
            .await
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.discover(request).await
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
        path: StreamPath,
        from: u64,
        filter: crate::FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        self.follow_from(path, from, None, filter).await
    }
    async fn follow_updates(
        &self,
        path: StreamPath,
        from: u64,
        preceding: Option<crate::CommitId>,
        filter: crate::FollowFilter,
    ) -> Result<crate::FollowStream, StreamError> {
        self.progress_from(path, from, preceding, filter).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
        filter: crate::FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        self.follow_from(path, from, Some(preceding), filter).await
    }
}

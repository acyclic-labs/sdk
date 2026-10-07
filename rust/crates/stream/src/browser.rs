//! Strict `IndexedDB` command publication around the existing Rust state machine.
//!
//! Each handle catches up only the commands since its durable cursor. Initial
//! recovery and total retention have explicit finite ceilings. No snapshot is
//! serialized per request, and the browser supplies no semantic transitions.

use crate::{
    journal::{Command, decode_command, journal_command, replay},
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
use prost::Message;
use std::{
    rc::Rc,
    sync::{Arc, Mutex},
};
use wasm_bindgen::{JsCast, JsValue};

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
}
struct Inner {
    database: Database,
    cache: tokio::sync::Mutex<Cache>,
    clock: Arc<ReplayClock>,
    limits: BrowserStreamLimits,
}

/// One account-local durable provider, shared safely across tabs and workers.
///
/// `IndexedDB`'s strict read/write transaction serializes refresh, Rust admission,
/// and journal publication across handles. A cancelled/failed publication
/// invalidates its disposable cache; the next request recovers durable facts.
/// Browser quotas or failed persistence return `Unavailable`, never success.
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
    /// Recovery runs on the first operation under the same publication lock.
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
        Ok(Self {
            inner: Rc::new(Inner {
                database,
                cache: tokio::sync::Mutex::new(Cache {
                    provider,
                    cursor: 0,
                    valid: true,
                }),
                clock,
                limits,
            }),
        })
    }

    fn transaction(&self) -> Result<Transaction<'_>, StreamError> {
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
    ) -> Result<(u64, u64), StreamError> {
        if !cache.valid {
            cache.provider = memory(self.inner.limits.memory, Arc::clone(&self.inner.clock));
            cache.cursor = 0;
        }
        // Keep invalid throughout every await: cancellation cannot retain a
        // partially recovered or unpublished state as an authoritative cache.
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
            replay(&cache.provider, decode_command(encoded)?).await?;
            cache.cursor += 1;
        }
        cache.valid = true;
        Ok((count, bytes))
    }

    async fn with_provider<T>(
        &self,
        operation: impl AsyncFnOnce(&MemoryStream) -> Result<T, StreamError>,
    ) -> Result<T, StreamError> {
        let mut cache = self.inner.cache.lock().await;
        let transaction = self.transaction()?;
        self.refresh(&transaction, &mut cache).await?;
        operation(&cache.provider).await
    }

    async fn execute(
        &self,
        mut command: Command,
        deadline: Option<u64>,
    ) -> Result<Outcome, StreamError> {
        match &mut command {
            Command::Append(request) => {
                memory::validate_records(&request.records)?;
                memory::validate_append_size(request)?;
            }
            Command::Fork(request) => memory::validate_fork_size(request)?,
            Command::Commit(request) => {
                memory::normalize_commit(request)?;
                memory::validate_commit_shape(request)?;
            }
        }
        let journal = journal_command(&command);
        if journal.encoded_len() > MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        let encoded = journal.encode_to_vec();
        let mut cache = self.inner.cache.lock().await;
        let transaction = self.transaction()?;
        let (count, bytes) = self.refresh(&transaction, &mut cache).await?;
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
        transaction.commit().await.map_err(backend)?;
        cache.cursor = count + 1;
        cache.valid = true;
        Ok(outcome)
    }
}

fn memory(limits: MemoryLimits, clock: Arc<ReplayClock>) -> MemoryStream {
    MemoryStream::new_with_commit_clock(limits, Arc::new(BrowserClock), clock.clone(), clock)
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
        self.with_provider(async move |provider| provider.children(request).await)
            .await
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
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        if from > self.tail(path.clone()).await? {
            return Err(StreamError::OutOfRange);
        }
        // Durable polling is the wakeup transport. It observes publication even
        // when the publishing context dies before sending any notification.
        // A dropped cursor stops polling; no detached task owns its lifetime.
        Ok(stream::try_unfold(
            (self.clone(), path, from),
            |(provider, path, mut cursor)| async move {
                loop {
                    let mut page = provider
                        .read(ReadRequest {
                            path: path.clone(),
                            from: cursor,
                            limit: 1,
                        })
                        .await?;
                    if let Some(record) = page.next().await {
                        let record = record?;
                        cursor += 1;
                        return Ok(Some((record, (provider, path, cursor))));
                    }
                    poll_delay().await?;
                }
            },
        )
        .boxed_local())
    }
}

async fn poll_delay() -> Result<(), StreamError> {
    let timer = js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("setTimeout"))
        .map_err(backend)?
        .dyn_into::<js_sys::Function>()
        .map_err(backend)?;
    let promise = js_sys::Promise::new(&mut |resolve, reject| {
        if let Err(error) = timer.call2(&JsValue::UNDEFINED, &resolve, &JsValue::from_f64(100.0)) {
            let _ = reject.call1(&JsValue::UNDEFINED, &error);
        }
    });
    wasm_bindgen_futures::JsFuture::from(promise)
        .await
        .map_err(backend)?;
    Ok(())
}

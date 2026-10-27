//! Shared command publication over an injected append-if-sequence journal.
//!
//! The injected journal owns only journal I/O and durability. Every Stream decision is
//! made by the same [`MemoryStream`] as the other persistent adapters. Commands
//! become visible only after the journal's durability promise resolves.

use crate::{
    journal::{Command, Outcome, decode_command},
    *,
};
use async_trait::async_trait;
use futures::StreamExt as _;
use js_sys::Uint8Array;
use std::{
    cell::{Cell, RefCell},
    rc::{Rc, Weak},
    sync::{Arc, Mutex},
};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen]
extern "C" {
    type HostJournal;
    #[wasm_bindgen(method, catch)]
    fn configure(
        this: &HostJournal,
        configuration: &[u8],
        maximum_frame_bytes: u32,
    ) -> Result<js_sys::Promise, JsValue>;
    #[wasm_bindgen(method, catch)]
    fn flush(this: &HostJournal) -> Result<js_sys::Promise, JsValue>;
    #[wasm_bindgen(method, catch)]
    fn read(this: &HostJournal, index: u32) -> Result<JsValue, JsValue>;
    #[wasm_bindgen(method, catch)]
    fn append(
        this: &HostJournal,
        index: u32,
        frame: &js_sys::Function,
        changes: &js_sys::Array,
    ) -> Result<js_sys::Promise, JsValue>;
}

/// Retained command journal and shared core capacities for one consistency domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalStreamLimits {
    /// Maximum retained accepted commands. Refusals consume no journal entry.
    pub commands: u32,
    /// Maximum retained bytes, including command clocks and deadlines.
    pub journal_bytes: u64,
    /// Capacity of the canonical Stream state machine.
    pub memory: MemoryLimits,
}

impl Default for JournalStreamLimits {
    fn default() -> Self {
        Self {
            commands: 65_536,
            journal_bytes: 256 * 1024 * 1024,
            memory: MemoryLimits::default(),
        }
    }
}

impl JournalStreamLimits {
    fn configuration(self) -> Vec<u8> {
        [
            u64::from(self.commands),
            self.journal_bytes,
            self.memory.paths as u64,
            self.memory.path_bytes as u64,
            self.memory.records as u64,
            self.memory.payload_bytes as u64,
            self.memory.commits as u64,
            self.memory.idempotency_results as u64,
        ]
        .into_iter()
        .flat_map(u64::to_le_bytes)
        .collect()
    }
}

#[derive(Default)]
struct PinnedClock(Mutex<u64>);
impl UnixMillisClock for PinnedClock {
    fn now_unix_millis(&self) -> u64 {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
impl PinnedClock {
    fn set(&self, value: u64) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = value;
    }
}

struct Cache {
    configured: bool,
    failed: bool,
    provider: MemoryStream,
    cursor: u32,
    bytes: u64,
}
struct Inner {
    journal: HostJournal,
    limits: JournalStreamLimits,
    clock: Arc<PinnedClock>,
    cache: crate::query_sync::Mutex<Cache>,
    claims: OperationClaims,
}

thread_local! {
    // Weak custody is essential: closing every session really discards replay
    // state. Retaining the host's journal never retains a provider.
    static OPEN: RefCell<Vec<Weak<Inner>>> = const { RefCell::new(Vec::new()) };
}

/// An exclusively owned journal domain inside one event host. Clone a handle
/// for concurrent users; all openers over the same journal share live follows.
#[derive(Clone)]
pub struct JournalStream(Rc<Inner>);

fn unavailable<T>(_: T) -> StreamError {
    StreamError::Unavailable
}

impl JournalStream {
    /// Opens a host journal with configure, flush, read and append primitives. This
    /// constructor performs no network I/O and starts no detached work.
    pub async fn open(journal: JsValue, limits: JournalStreamLimits) -> Result<Self, StreamError> {
        if !journal.is_object() || limits.commands == 0 || limits.journal_bytes == 0 {
            return Err(StreamError::InvalidArgument);
        }
        let existing = OPEN.with(|open| {
            let mut open = open.borrow_mut();
            open.retain(|weak| weak.strong_count() != 0);
            open.iter()
                .filter_map(Weak::upgrade)
                .find(|inner| js_sys::Object::is(inner.journal.as_ref(), &journal))
        });
        if let Some(inner) = existing {
            if inner.limits != limits {
                return Err(StreamError::InvalidArgument);
            }
            let stream = Self(inner);
            stream.query(async |_| Ok(())).await?;
            return Ok(stream);
        }
        let journal: HostJournal = journal.unchecked_into();
        let clock = Arc::new(PinnedClock::default());
        let provider =
            MemoryStream::new_with_commit_clock(limits.memory, clock.clone(), clock.clone());
        let inner = Rc::new(Inner {
            journal,
            limits,
            clock,
            cache: crate::query_sync::Mutex::new(Cache {
                configured: false,
                failed: false,
                provider,
                cursor: 0,
                bytes: 0,
            }),
            claims: OperationClaims::default(),
        });
        OPEN.with(|open| open.borrow_mut().push(Rc::downgrade(&inner)));
        let stream = Self(inner);
        stream.query(async |_| Ok(())).await?;
        Ok(stream)
    }

    async fn refresh(&self, cache: &mut Cache) -> Result<(), StreamError> {
        if cache.failed {
            return Err(StreamError::Unavailable);
        }
        if !cache.configured {
            let maximum = u32::try_from(MAX_COMMAND_BYTES + 17).map_err(unavailable)?;
            JsFuture::from(
                self.0
                    .journal
                    .configure(&self.0.limits.configuration(), maximum)
                    .map_err(unavailable)?,
            )
            .await
            .map_err(unavailable)?;
            cache.configured = true;
        }
        JsFuture::from(self.0.journal.flush().map_err(unavailable)?)
            .await
            .map_err(unavailable)?;
        loop {
            let value = self.0.journal.read(cache.cursor).map_err(unavailable)?;
            if value.is_null() {
                return Ok(());
            }
            let frame = value.dyn_into::<Uint8Array>().map_err(unavailable)?;
            let length = usize::try_from(frame.length()).map_err(unavailable)?;
            if !(18..=MAX_COMMAND_BYTES + 17).contains(&length)
                || cache.cursor >= self.0.limits.commands
            {
                return Err(StreamError::Unavailable);
            }
            let frame = frame.to_vec();
            let time = u64::from_le_bytes(
                frame
                    .get(..8)
                    .ok_or(StreamError::Unavailable)?
                    .try_into()
                    .map_err(unavailable)?,
            );
            let deadline = u64::from_le_bytes(
                frame
                    .get(9..17)
                    .ok_or(StreamError::Unavailable)?
                    .try_into()
                    .map_err(unavailable)?,
            );
            let deadline = match frame.get(8) {
                Some(0) if deadline == 0 => None,
                Some(1) => Some(deadline),
                _ => return Err(StreamError::Unavailable),
            };
            let encoded = frame.get(17..).ok_or(StreamError::Unavailable)?;
            let command = decode_command(encoded)?;
            let bytes = cache
                .bytes
                .checked_add(u64::try_from(length).map_err(unavailable)?)
                .filter(|bytes| *bytes <= self.0.limits.journal_bytes)
                .ok_or(StreamError::Capacity)?;
            self.0.clock.set(time);
            // Recovery uses the canonical evaluator. New publication only
            // journals accepted mutations, never conditional refusals.
            let result = apply(&cache.provider, command, deadline).await;
            replayable(&result)?;
            cache.cursor = cache.cursor.checked_add(1).ok_or(StreamError::Capacity)?;
            cache.bytes = bytes;
        }
    }

    async fn query<T>(
        &self,
        operation: impl AsyncFnOnce(&MemoryStream) -> Result<T, StreamError>,
    ) -> Result<T, StreamError> {
        let mut cache = self.0.cache.lock().await;
        self.refresh(&mut cache).await?;
        operation(&cache.provider).await
    }

    async fn execute(
        &self,
        command: Command,
        deadline: Option<u64>,
    ) -> Result<Outcome, StreamError> {
        let command = command.validated()?;
        let encoded = command.encode_to_vec();
        let mut cache = self.0.cache.lock().await;
        self.refresh(&mut cache).await?;
        if command.has_replay(&cache.provider).await? {
            return apply(&cache.provider, command, deadline).await;
        }
        let length = encoded.len() + 17;
        let base_bytes = cache.bytes;
        let cursor = cache.cursor;
        // Journal capacity is checked only after the canonical core accepts a
        // new mutation; a conditional refusal does not need a durable slot.
        let bytes = base_bytes
            .checked_add(u64::try_from(length).map_err(unavailable)?)
            .filter(|bytes| *bytes <= self.0.limits.journal_bytes);
        // The host owns durable wake routing, but never decodes command semantics.
        // Only accepted commands reach it. Forks affect a destination subtree;
        // ordinary appends affect exactly their path.
        let changes = js_sys::Array::new();
        let changed = |path: &StreamPath, subtree: bool| {
            let change = js_sys::Array::new();
            change.push(&JsValue::from_str(path.as_str()));
            change.push(&JsValue::from_bool(subtree));
            changes.push(&change);
        };
        match &command {
            Command::Append(request) => changed(&request.path, false),
            Command::Fork(request) => changed(&request.destination, true),
            Command::Commit(request) => {
                for mutation in &request.mutations {
                    changed(
                        crate::request::mutation_path(mutation),
                        matches!(mutation, CommitMutation::Fork { .. }),
                    );
                }
            }
        }
        // The journal calls this once, inside its acceptance transaction after
        // any waiting and before publishing bytes. Sampling here pins deadline
        // refusal and commit time to acceptance, rather than enqueue time. The
        // storage adapter never interprets a deadline or a Stream command.
        let accepted = Rc::new(Cell::new(None));
        let sampled = accepted.clone();
        let clock = self.0.clock.clone();
        let frame = Closure::<dyn FnMut() -> Result<Uint8Array, JsValue>>::new(move || {
            if sampled.get().is_some() {
                return Err(JsValue::from_str(
                    "STEP journal-accept failed: repeated frame admission; recovery: reopen and replay the original Stream request",
                ));
            }
            let time = SystemUnixMillisClock.now_unix_millis();
            sampled.set(Some(time));
            if deadline.is_some_and(|deadline| time >= deadline) {
                return Err(JsValue::from_str(
                    "STEP journal-accept failed: deadline elapsed; recovery: inspect the original request identity before submitting another operation",
                ));
            }
            clock.set(time);
            let mut frame = time.to_le_bytes().to_vec();
            frame.push(u8::from(deadline.is_some()));
            frame.extend_from_slice(&deadline.unwrap_or(0).to_le_bytes());
            frame.extend_from_slice(&encoded);
            Ok(Uint8Array::from(frame.as_slice()))
        }).into_js_value();
        // The host's promise owns this JavaScript function across caller
        // cancellation. Dropping this Rust future must not invalidate a frame
        // callback that the host may still call in its acceptance transaction.
        // Cancellation after acceptance leaves the cursor unchanged. The next
        // query replays that accepted row before exposing any new state.
        self.0.clock.set(SystemUnixMillisClock.now_unix_millis());
        let mut published = false;
        let result = cache
            .provider
            .apply_durable(command, deadline, async {
                if cursor >= self.0.limits.commands {
                    return Err(StreamError::Capacity);
                }
                bytes.ok_or(StreamError::Capacity)?;
                let appended = JsFuture::from(
                    self.0
                        .journal
                        .append(cursor, frame.unchecked_ref(), &changes)
                        .map_err(unavailable)?,
                )
                .await
                .map_err(unavailable);
                if appended.is_err()
                    && accepted
                        .get()
                        .is_some_and(|at| deadline.is_some_and(|deadline| at >= deadline))
                {
                    return Err(StreamError::DeadlineElapsed);
                }
                appended?;
                accepted.get().ok_or(StreamError::Unavailable)?;
                published = true;
                Ok(())
            })
            .await;
        cache.failed = (published && result.is_err())
            || matches!(
                &result,
                Err(StreamError::Unavailable
                    | StreamError::InvalidResponse
                    | StreamError::Unsupported)
            );
        if published {
            cache.cursor += 1;
            cache.bytes = bytes.ok_or(StreamError::Capacity)?;
        }
        result
    }

    fn retain<T: 'static>(
        &self,
        stream: BoxProviderStream<'static, T>,
    ) -> BoxProviderStream<'static, T> {
        let owner = self.clone();
        stream
            .map(move |item| {
                let _owner = &owner;
                item
            })
            .boxed_local()
    }
}

async fn apply(
    provider: &MemoryStream,
    command: Command,
    deadline: Option<u64>,
) -> Result<Outcome, StreamError> {
    match command {
        Command::Append(request) => provider.append(request).await.map(Outcome::Append),
        Command::Fork(request) => provider.fork(request).await.map(Outcome::Fork),
        Command::Commit(request) => match deadline {
            Some(deadline) => provider.commit_before(request, deadline).await,
            None => provider.commit(request).await,
        }
        .map(Outcome::Commit),
    }
}
fn replayable(result: &Result<Outcome, StreamError>) -> Result<(), StreamError> {
    match result {
        Err(StreamError::Unavailable | StreamError::InvalidResponse | StreamError::Unsupported) => {
            Err(StreamError::Unavailable)
        }
        _ => Ok(()),
    }
}

#[async_trait(?Send)]
impl StreamProvider for JournalStream {
    fn operation_claims(&self) -> &OperationClaims {
        &self.0.claims
    }
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.query(async move |p| p.inspect_idempotency(key).await)
            .await
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.query(async move |p| p.tail(path).await).await
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.query(async move |p| p.bounds(path).await).await
    }
    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        let tail = self.tail(path).await?;
        Ok(StreamWatermarks {
            accepted: tail,
            durable: tail,
        })
    }
    async fn durable_receipt(
        &self,
        path: StreamPath,
        through: u64,
    ) -> Result<Option<DurableReceipt>, StreamError> {
        self.query(async move |p| p.durable_receipt(path, through).await)
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
        self.query(async move |p| p.read(request).await).await
    }
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.query(async move |p| p.children_page(request).await)
            .await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.query(async move |p| p.read_commit(id).await).await
    }
    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
        filter: FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        Ok(self.retain(
            self.query(async move |p| p.follow(path, from, filter).await)
                .await?,
        ))
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
        filter: FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        Ok(self.retain(
            self.query(async move |p| p.follow_checked(path, from, preceding, filter).await)
                .await?,
        ))
    }
    async fn follow_updates(
        &self,
        path: StreamPath,
        from: u64,
        preceding: Option<CommitId>,
        filter: FollowFilter,
    ) -> Result<FollowStream, StreamError> {
        Ok(self.retain(
            self.query(async move |p| p.follow_updates(path, from, preceding, filter).await)
                .await?,
        ))
    }
}

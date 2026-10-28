//! Shared command publication over an injected append-if-sequence journal.
//!
//! The injected journal owns only journal I/O and durability. Every Stream decision is
//! made by the same [`MemoryStream`] as the other persistent adapters. Commands
//! become visible only after the journal's durability promise resolves.

use crate::{
    journal::{Command, Outcome},
    replicated::{ReplicaClock, Replicated},
    *,
};
use async_trait::async_trait;
use futures::StreamExt as _;
use js_sys::Uint8Array;
use std::{
    cell::{Cell, RefCell},
    rc::{Rc, Weak},
    sync::Arc,
};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen]
extern "C" {
    #[derive(Clone)]
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
    fn read(this: &HostJournal, index: u32, page: u32) -> Result<JsValue, JsValue>;
    #[wasm_bindgen(method, catch)]
    fn changed(
        this: &HostJournal,
        after: u32,
        callback: &js_sys::Function,
    ) -> Result<js_sys::Function, JsValue>;
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

mod port;
use port::HostPort;
const PAGE: usize = 64;

struct Inner {
    journal: HostJournal,
    limits: JournalStreamLimits,
    cache: crate::query_sync::Mutex<Replicated<HostPort>>,
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
        let clock = Arc::new(ReplicaClock::default());
        let provider =
            MemoryStream::new_with_commit_clock(limits.memory, clock.clone(), clock.clone());
        let inner = Rc::new(Inner {
            cache: crate::query_sync::Mutex::new(Replicated::new(
                HostPort {
                    journal: journal.clone(),
                    limits,
                    configured: false,
                    bytes: 0,
                },
                provider,
                clock,
                0,
            )),
            journal,
            limits,
            claims: OperationClaims::default(),
        });
        OPEN.with(|open| open.borrow_mut().push(Rc::downgrade(&inner)));
        let stream = Self(inner);
        stream.query(async |_| Ok(())).await?;
        Ok(stream)
    }

    async fn query<T>(
        &self,
        operation: impl AsyncFnOnce(&MemoryStream) -> Result<T, StreamError>,
    ) -> Result<T, StreamError> {
        let mut cache = self.0.cache.lock().await;
        cache.refresh(PAGE).await?;
        operation(&cache.provider).await
    }

    async fn execute(
        &self,
        command: Command,
        deadline: Option<u64>,
    ) -> Result<Outcome, StreamError> {
        self.0
            .cache
            .lock()
            .await
            .execute(command, deadline, PAGE)
            .await
            .map(|execution| execution.outcome)
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

#[async_trait(?Send)]
impl StreamProvider for JournalStream {
    fn context(&self) -> crate::ProviderContext<'_> {
        crate::ProviderContext::new(&self.0.claims)
    }
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.query(async move |p| p.inspect_idempotency(key).await)
            .await
    }
    async fn heads(&self, request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
        request.validate()?;
        if !request.at_least.is_empty() {
            self.wait_revision(&request.at_least).await?;
        }
        self.query(async move |provider| {
            let projection = request.projection;
            let mut view = provider.capture_heads(&request).await?;
            for head in view.heads.iter_mut().flatten() {
                head.durable = if projection == crate::HeadProjection::Tail {
                    0
                } else {
                    head.tail
                };
            }
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
        self.query(async move |p| p.read(request).await).await
    }

    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.query(async move |p| p.read_commit(id).await).await
    }
    async fn follow(
        &self,
        request: crate::FollowRequest,
    ) -> Result<crate::FollowStream, StreamError> {
        if !request.at_least.is_empty() {
            self.wait_revision(&request.at_least).await?;
        }
        Ok(self.retain(self.query(async move |p| p.follow(request).await).await?))
    }
}

impl JournalStream {
    async fn wait_revision(
        &self,
        at_least: &crate::RevisionCut,
    ) -> Result<crate::Revision, StreamError> {
        // Park outside the cache lock so writes and refreshes can advance it.
        let revisions = self
            .query(async |p| Ok(std::sync::Arc::clone(&p.revisions)))
            .await?;
        revisions.wait(at_least).await
    }
}

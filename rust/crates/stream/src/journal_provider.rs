//! Shared command publication over an injected append-if-sequence journal.
//!
//! The injected journal owns only journal I/O and durability. Every Stream decision is
//! made by the same [`MemoryStream`] as the other persistent adapters. Commands
//! become visible only after the journal's durability promise resolves.

use crate::{
    replicated::{ReplicaClock, Replicated},
    *,
};
use async_trait::async_trait;
use futures::StreamExt as _;
use js_sys::Uint8Array;
use prost::Message;
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
        configuration: &Uint8Array,
        maximum_frame_bytes: u32,
        authority: &Uint8Array,
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
    pub commands: Option<u32>,
    /// Maximum retained bytes, including command clocks and deadlines.
    pub journal_bytes: Option<u64>,
    /// Capacity of the canonical Stream state machine.
    pub memory: MemoryLimits,
}

impl Default for JournalStreamLimits {
    fn default() -> Self {
        Self {
            commands: None,
            journal_bytes: None,
            memory: MemoryLimits::default(),
        }
    }
}

impl JournalStreamLimits {
    fn configuration(self) -> Vec<u8> {
        [
            self.commands.map(u64::from),
            self.journal_bytes,
            self.memory.paths.map(|value| value as u64),
            self.memory.path_bytes.map(|value| value as u64),
            self.memory.records.map(|value| value as u64),
            self.memory.payload_bytes.map(|value| value as u64),
            self.memory.commits.map(|value| value as u64),
            self.memory.idempotency_results.map(|value| value as u64),
        ]
        .into_iter()
        .flat_map(|value| [u64::from(value.is_some()), value.unwrap_or_default()])
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
    cache: tokio::sync::OnceCell<crate::query_sync::Mutex<Replicated<HostPort>>>,
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
        if !journal.is_object() || limits.commands == Some(0) || limits.journal_bytes == Some(0) {
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
        let inner = Rc::new(Inner {
            cache: tokio::sync::OnceCell::new(),
            journal,
            limits,
            claims: OperationClaims::default(),
        });
        OPEN.with(|open| open.borrow_mut().push(Rc::downgrade(&inner)));
        let stream = Self(inner);
        stream.query(async |_| Ok(())).await?;
        Ok(stream)
    }

    async fn replica(
        &self,
    ) -> Result<&crate::query_sync::Mutex<Replicated<HostPort>>, StreamError> {
        self.0
            .cache
            .get_or_try_init(|| async {
                let clock = Arc::new(ReplicaClock::default());
                let provider = MemoryStream::new_with_commit_clock(
                    self.0.limits.memory,
                    clock.clone(),
                    clock.clone(),
                );
                let maximum = u32::try_from(MAX_COMMAND_BYTES + 17).map_err(unavailable)?;
                let proposed =
                    crate::wire_codec::revision_to_wire(&provider.revisions.at(0)).encode_to_vec();
                let retained = JsFuture::from(
                    self.0
                        .journal
                        .configure(
                            &Uint8Array::from(&self.0.limits.configuration()[..]),
                            maximum,
                            &Uint8Array::from(&proposed[..]),
                        )
                        .map_err(unavailable)?,
                )
                .await
                .map_err(unavailable)?;
                let retained = retained.dyn_into::<Uint8Array>().map_err(unavailable)?;
                if retained.length() as usize > MAX_COMMAND_BYTES {
                    return Err(StreamError::LimitExceeded);
                }
                let authority = crate::wire::Revision::decode(retained.to_vec().as_slice())
                    .map_err(unavailable)?;
                let authority = crate::wire_codec::revision_from_wire(authority)?;
                if authority.scope.as_bytes().is_empty() || authority.index != 0 {
                    return Err(StreamError::InvalidResponse);
                }
                // All concurrent openers share this initialization. No replay,
                // provider or revision token is exposed before durable authority.
                let provider = provider.with_authority(authority.scope, authority.incarnation);
                Ok(crate::query_sync::Mutex::new(Replicated::new(
                    HostPort {
                        journal: self.0.journal.clone(),
                        limits: self.0.limits,
                        bytes: 0,
                    },
                    provider,
                    clock,
                    0,
                )))
            })
            .await
    }
    async fn query<T>(
        &self,
        operation: impl AsyncFnOnce(&MemoryStream) -> Result<T, StreamError>,
    ) -> Result<T, StreamError> {
        let mut cache = self.replica().await?.lock().await;
        cache.refresh(PAGE).await?;
        operation(&cache.provider).await
    }

    async fn execute(
        &self,
        command: WriteRequest,
        deadline: Option<u64>,
    ) -> Result<WriteOutcome, StreamError> {
        self.replica()
            .await?
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
        self.query(async move |provider| provider.capture_heads(&request, true).await)
            .await
    }

    fn write(
        &self,
        request: crate::WriteRequest,
    ) -> crate::BoxProviderFuture<'_, std::result::Result<crate::WriteOutcome, crate::StreamError>>
    {
        Box::pin(async move {
            let deadline = match &request {
                crate::WriteRequest::Commit(request) => request.deadline_unix_millis,
                _ => None,
            };
            self.execute(request, deadline).await
        })
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

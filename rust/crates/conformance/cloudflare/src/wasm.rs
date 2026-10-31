use acyclic_actors::guest::Handler;
use acyclic_agent_host_conformance::{Backend, Result, Storage};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{BoxProviderFuture, JournalStream, JournalStreamLimits, StreamProvider};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

/// The original journal decides deadlines at acceptance, including after reopen.
#[wasm_bindgen(js_name = qualifyJournalDeadline)]
pub async fn qualify_journal_deadline(
    journal: js_sys::Function,
    retry: bool,
) -> std::result::Result<(), JsValue> {
    use acyclic_stream::{
        AppendRecord, CommitCondition, CommitMutation, CommitOutcome, CommitRequest,
        IdempotencyKey, StreamError, StreamPath, UnixMillisClock,
    };
    use std::{
        cell::Cell,
        rc::Rc,
        sync::atomic::{AtomicU64, Ordering},
    };
    struct Clock(AtomicU64);
    impl UnixMillisClock for Clock {
        fn now_unix_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }
    const BEFORE: u64 = 1_000;
    const DEADLINE: u64 = BEFORE + 1;
    let clock = Arc::new(Clock(AtomicU64::new(BEFORE)));
    let expire_next = Rc::new(Cell::new(false));
    let admissions = Rc::new(Cell::new(0_usize));
    let acceptance = {
        let clock = clock.clone();
        let expire_next = expire_next.clone();
        let admissions = admissions.clone();
        Closure::<dyn FnMut()>::new(move || {
            admissions.set(admissions.get() + 1);
            if expire_next.replace(false) {
                clock.0.store(DEADLINE, Ordering::SeqCst);
            }
        })
        .into_js_value()
    };
    let result = async {
        let journal = journal
            .call1(&JsValue::NULL, &acceptance)
            .map_err(|_| StreamError::Unavailable)?;
        let path = StreamPath::new("deadline/acceptance")?;
        let key = IdempotencyKey::new(b"deadline-acceptance".to_vec())?;
        let request = CommitRequest {
            conditions: vec![CommitCondition::Absent { path: path.clone() }],
            mutations: vec![CommitMutation::Append {
                path: path.clone(),
                records: vec![AppendRecord::from(b"accepted".as_slice())],
            }],
            idempotency_key: key.clone(),
            deadline_unix_millis: None,
        };
        let (provider, peer) = futures::try_join!(
            JournalStream::open_with_clock(
                journal.clone(),
                JournalStreamLimits::default(),
                clock.clone()
            ),
            JournalStream::open_with_clock(
                journal.clone(),
                JournalStreamLimits::default(),
                clock.clone()
            ),
        )?;
        // A second owner with the same value cannot change this live domain's
        // authority clock, and the system-clock opener cannot replace it either.
        if !matches!(
            JournalStream::open_with_clock(
                journal.clone(),
                JournalStreamLimits::default(),
                Arc::new(Clock(AtomicU64::new(BEFORE))),
            )
            .await,
            Err(StreamError::InvalidArgument)
        ) || !matches!(
            JournalStream::open(journal.clone(), JournalStreamLimits::default()).await,
            Err(StreamError::InvalidArgument)
        ) {
            return Err(StreamError::InvalidResponse);
        }
        if peer.tail(path.clone()).await != Err(StreamError::NotFound) {
            return Err(StreamError::InvalidResponse);
        }
        drop(peer);
        if provider.tail(path.clone()).await != Err(StreamError::NotFound)
            || provider.inspect_idempotency(key.clone()).await?.is_some()
        {
            return Err(StreamError::InvalidResponse);
        }
        if !retry {
            // The same physical acceptance succeeds before expiry. Its distinct
            // identity leaves the original expired request unspent for restart.
            let control = StreamPath::new("deadline/control")?;
            let accepted = provider
                .commit(CommitRequest {
                    conditions: vec![CommitCondition::Absent {
                        path: control.clone(),
                    }],
                    mutations: vec![CommitMutation::Append {
                        path: control.clone(),
                        records: vec![AppendRecord::from(b"before-expiry".as_slice())],
                    }],
                    idempotency_key: IdempotencyKey::new(b"deadline-control".to_vec())?,
                    deadline_unix_millis: Some(DEADLINE),
                })
                .await?;
            if !matches!(accepted, CommitOutcome::Committed(_))
                || admissions.get() != 1
                || clock.now_unix_millis() != BEFORE
                || provider.tail(control.clone()).await? != 1
            {
                return Err(StreamError::InvalidResponse);
            }
            expire_next.set(true);
            let result = provider
                .commit(CommitRequest {
                    deadline_unix_millis: Some(DEADLINE),
                    ..request
                })
                .await;
            if result != Err(StreamError::DeadlineElapsed)
                || admissions.get() != 2
                || expire_next.get()
                || clock.now_unix_millis() != DEADLINE
            {
                return Err(StreamError::InvalidResponse);
            }
            drop(provider);
            // Last-provider retirement releases the registry entry: a fresh
            // owner may now open the same retained host journal.
            let reopened = JournalStream::open_with_clock(
                journal,
                JournalStreamLimits::default(),
                Arc::new(Clock(AtomicU64::new(DEADLINE))),
            )
            .await?;
            if reopened.tail(path).await != Err(StreamError::NotFound)
                || reopened.inspect_idempotency(key).await?.is_some()
                || reopened.tail(control).await? != 1
            {
                return Err(StreamError::InvalidResponse);
            }
        } else {
            // The same key was not spent by the expired acceptance before restart.
            let outcome = provider
                .commit(CommitRequest {
                    deadline_unix_millis: Some(DEADLINE),
                    ..request.clone()
                })
                .await?;
            if !matches!(outcome, CommitOutcome::Committed(_)) || admissions.get() != 1 {
                return Err(StreamError::InvalidResponse);
            }
            let CommitOutcome::Committed(receipt) = &outcome else {
                return Err(StreamError::InvalidResponse);
            };
            let issued = receipt.revision.clone();
            drop(provider);
            let reopened = JournalStream::open_with_clock(
                journal,
                JournalStreamLimits::default(),
                clock.clone(),
            )
            .await?;
            let applied = reopened
                .wait_applied(&acyclic_stream::RevisionCut::of(issued.clone()))
                .await?;
            if !applied.same_authority(&issued) || applied.index < issued.index {
                return Err(StreamError::InvalidResponse);
            }
            let foreign = acyclic_stream::AppliedRevisions::in_process().at(0);
            if reopened
                .wait_applied(&acyclic_stream::RevisionCut::of(foreign))
                .await
                != Err(StreamError::RevisionForeign)
            {
                return Err(StreamError::InvalidResponse);
            }
            if reopened
                .commit(CommitRequest {
                    deadline_unix_millis: Some(0),
                    ..request
                })
                .await?
                != outcome
                || reopened.tail(path).await? != 1
                || admissions.get() != 1
            {
                return Err(StreamError::InvalidResponse);
            }
        }
        Ok::<_, StreamError>(())
    }
    .await;
    result.map_err(|error| {
        JsValue::from_str(&format!(
            "STEP journal-deadline failed: {error}; recovery: bash scripts/qualify-cloudflare.sh"
        ))
    })
}

/// Produce a real canonical append through the supplied SQLite ledger port.
#[wasm_bindgen(js_name = appendWakeRecord)]
pub async fn append_wake_record(port: JsValue, path: String) -> std::result::Result<(), JsValue> {
    let provider = acyclic_stream::WasmStream::connect_provider(port)?.provider();
    let result = async {
        let key = acyclic_stream::IdempotencyKey::new(format!("wake-record/{path}").into_bytes())?;
        provider
            .append(acyclic_stream::AppendRequest {
                path: acyclic_stream::StreamPath::new(path)?,
                records: vec![acyclic_stream::AppendRecord::from(b"wake".as_slice())],
                if_tail: None,
                idempotency_key: key,
            })
            .await?;
        Ok::<_, acyclic_stream::StreamError>(())
    }
    .await;
    result.map_err(|error| {
        JsValue::from_str(&format!(
            "STEP wake-record append failed: {error}; recovery: bash scripts/qualify-cloudflare.sh"
        ))
    })
}

struct Sqlite(js_sys::Function, js_sys::Function);
struct Domain(JsValue);
struct Rpc(js_sys::Function, js_sys::Function);
struct Port(JsValue);

// Keep each concrete production provider available until Counted has installed
// its ledger. An already erased Arc cannot transparently forward native work.
trait StreamFixture {
    fn stream(
        &self,
    ) -> BoxProviderFuture<'_, Result<Box<dyn acyclic_stream::conformance::Backend>>>;
}

impl StreamFixture for Sqlite {
    fn stream(
        &self,
    ) -> BoxProviderFuture<'_, Result<Box<dyn acyclic_stream::conformance::Backend>>> {
        Box::pin(async move {
            let journal = self
                .0
                .call0(&JsValue::UNDEFINED)
                .map_err(|error| format!("SQLite domain creation failed: {error:?}"))?;
            let provider = JournalStream::open(journal, JournalStreamLimits::default()).await?;
            Ok(
                Box::new(acyclic_stream::conformance::Fixture::new(provider))
                    as Box<dyn acyclic_stream::conformance::Backend>,
            )
        })
    }
}

impl StreamFixture for Rpc {
    fn stream(
        &self,
    ) -> BoxProviderFuture<'_, Result<Box<dyn acyclic_stream::conformance::Backend>>> {
        Box::pin(async move {
            let port = self
                .0
                .call0(&JsValue::UNDEFINED)
                .map_err(|error| format!("RPC domain creation failed: {error:?}"))?;
            Ok(Box::new(acyclic_stream::conformance::injected(port)?)
                as Box<dyn acyclic_stream::conformance::Backend>)
        })
    }
}

impl Storage for Port {
    fn objects(&self) -> BoxProviderFuture<'_, Result<acyclic_agent_host_conformance::Objects>> {
        Box::pin(async { Ok(acyclic_agent_host_conformance::Objects::memory()) })
    }

    fn open(&self) -> BoxProviderFuture<'_, Result<Arc<dyn StreamProvider>>> {
        Box::pin(async move {
            Ok(acyclic_stream::WasmStream::connect_provider(self.0.clone())
                .map_err(|error| format!("provider port failed: {error:?}"))?
                .provider())
        })
    }
}

impl Backend for Rpc {
    fn serving(
        &self,
        policy: acyclic_harness_core::approvals::ApprovalPolicy,
    ) -> BoxProviderFuture<'_, Result<Box<dyn acyclic_agent_host_conformance::protocol::Endpoint>>>
    {
        Box::pin(crate::hosting::open(&self.1, policy))
    }
    fn fresh(&self) -> BoxProviderFuture<'_, Result<Box<dyn Storage>>> {
        Box::pin(async move {
            let port = self
                .0
                .call0(&JsValue::UNDEFINED)
                .map_err(|error| format!("RPC domain creation failed: {error:?}"))?;
            Ok(Box::new(Port(port)) as Box<dyn Storage>)
        })
    }
    fn handler(&self, handler: Arc<dyn Handler>) -> Result<Arc<dyn Handler>> {
        acyclic_agent_host_conformance::remote(handler)
    }
}

impl Storage for Domain {
    fn objects(&self) -> BoxProviderFuture<'_, Result<acyclic_agent_host_conformance::Objects>> {
        Box::pin(async { Ok(acyclic_agent_host_conformance::Objects::memory()) })
    }

    fn open(&self) -> BoxProviderFuture<'_, Result<Arc<dyn StreamProvider>>> {
        Box::pin(async move {
            Ok(
                Arc::new(JournalStream::open(self.0.clone(), JournalStreamLimits::default()).await?)
                    as Arc<dyn StreamProvider>,
            )
        })
    }
}

impl Backend for Sqlite {
    fn serving(
        &self,
        policy: acyclic_harness_core::approvals::ApprovalPolicy,
    ) -> BoxProviderFuture<'_, Result<Box<dyn acyclic_agent_host_conformance::protocol::Endpoint>>>
    {
        Box::pin(crate::hosting::open(&self.1, policy))
    }
    fn fresh(&self) -> BoxProviderFuture<'_, Result<Box<dyn Storage>>> {
        Box::pin(async move {
            let journal = self
                .0
                .call0(&JsValue::UNDEFINED)
                .map_err(|error| format!("SQLite domain creation failed: {error:?}"))?;
            Ok(Box::new(Domain(journal)) as Box<dyn Storage>)
        })
    }

    fn handler(&self, handler: Arc<dyn Handler>) -> Result<Arc<dyn Handler>> {
        acyclic_agent_host_conformance::remote(handler)
    }
}

/// Runs both suites without selecting or replacing vectors. `fresh` returns a
/// new SDK SqliteJournal on the calling Durable Object's real SQLite storage.
#[wasm_bindgen(js_name = qualifyCloudflare)]
pub async fn qualify(
    fresh: js_sys::Function,
    hosted: js_sys::Function,
) -> std::result::Result<(), JsValue> {
    verify(&Sqlite(fresh, hosted), "sqlite").await
}

/// The same suites over actual cross-DO canonical RPC, including pull cursors.
#[wasm_bindgen(js_name = qualifyCloudflareRpc)]
pub async fn qualify_rpc(
    fresh: js_sys::Function,
    hosted: js_sys::Function,
) -> std::result::Result<(), JsValue> {
    verify(&Rpc(fresh, hosted), "rpc").await
}

async fn verify(
    backend: &(impl Backend + StreamFixture),
    name: &str,
) -> std::result::Result<(), JsValue> {
    let stream = backend
        .stream()
        .await
        .map_err(|error| failure(name, "stream-open", error))?;
    acyclic_stream::conformance::verify(stream.as_ref())
        .await
        .map_err(|error| failure(name, "stream-suite", error))?;
    drop(stream);
    acyclic_agent_host_conformance::verify(backend)
        .await
        .map_err(|error| failure(name, "agent-host-suite", error))
}

fn failure(backend: &str, step: &str, error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&format!(
        "STEP cloudflare-{backend}-{step} failed: {error}; recovery: bash scripts/qualify-cloudflare.sh"
    ))
}

//! A shared QuickJS interpreter; effects remain ordinary journaled tool calls.
//!
//! An execution owns a transient realm while holding the interpreter's credit.
//! No realm, process, task or runtime is retained for a dormant agent. The
//! interpreter has no module loader, filesystem, network, timer or process API.

mod declarations;
mod typescript;

pub use declarations::declarations;
pub use typescript::strip_types;

use crate::{
    Error, Result,
    tool::{ToolDefinition, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;
use futures::{StreamExt, stream::FuturesOrdered};
use rquickjs::{Context, Function, Runtime};
use serde_json::Value;
use std::{
    collections::{HashSet, VecDeque},
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

/// The original invocation's admission and interruption producer.
///
/// Implementations retain their original reservation and cancellation owner.
/// Exhausted work is a typed refusal; cancellation/producer loss completes
/// `interrupted`. There is no interpreter-specific timer or step allowance.
pub trait CodeControl: Send + Sync {
    /// Charges/checks the original work credit at an interpreter interruption.
    fn check(&self) -> Result<()>;
    /// Waits on the original cancellation/owner-loss event, never a probe.
    fn interrupted(&self) -> BoxProviderFuture<'_, Error>;
}

/// The original harness-wide memory reservation funding the shared interpreter.
/// Its owner remains retained while the VM or any transient realm is live.
pub trait CodeMemory: Send + Sync {
    /// Already admitted shared memory credit; `None` means truly unbounded.
    fn credit_bytes(&self) -> Option<NonZeroUsize>;
    /// Acquires the original pool's transient execution/preparation credit.
    /// Waits on release by that pool's original owner; producer loss must refuse.
    /// The permit funds parsing as well as the realm and all pending host calls.
    fn acquire(&self, source_bytes: usize) -> BoxProviderFuture<'_, Result<Box<dyn CodeCredit>>>;
}

/// Original reservation custody, retained until all code and issued calls end.
/// Dropping the concrete reservation returns credit to its existing pool.
pub trait CodeCredit: Send + Sync {}
impl<T: Send + Sync> CodeCredit for T {}

/// Canonical host result and the original reservation funding its live bytes.
/// The reservation retires only after those bytes have been delivered to JS.
pub struct CodeHostResult {
    /// Original tool result, without a second projection or response ledger.
    pub result: ToolResult,
    /// Original allocation custody, when the dispatch uses accounted memory.
    pub credit: Option<Box<dyn CodeCredit>>,
}

impl From<ToolResult> for CodeHostResult {
    fn from(result: ToolResult) -> Self {
        Self {
            result,
            credit: None,
        }
    }
}

/// The normal approval, validation, execution and journal boundary.
///
/// Implementations dispatch the complete invocation through that boundary,
/// including exact replay/reconciliation. Calling `ToolExecutor` directly here
/// would bypass approvals and durable request custody and is not supported.
/// An uncertain error must remain the original error, with its request retained
/// by the ordinary journal; code mode never retries or acknowledges it.
pub trait CodeHost: Send + Sync {
    /// Derives the nested identity on the same original accounting path. Native
    /// dispatchers override this with their retained hash/work authority.
    fn invocation(
        &self,
        outer: &ToolInvocation,
        ordinal: u64,
        name: String,
        arguments: Value,
    ) -> Result<ToolInvocation> {
        ToolInvocation::for_model_call(
            outer.operation_id,
            0,
            format!("code-{ordinal}"),
            name,
            arguments,
            None,
        )
    }
    /// Executes or replays one stable nested call through the ordinary dispatcher.
    fn dispatch(&self, invocation: ToolInvocation)
    -> BoxProviderFuture<'_, Result<CodeHostResult>>;
}

/// One interpreter shared by the harness, with event-driven execution credit.
pub struct CodeRuntime {
    runtime: Runtime,
    phase: tokio::sync::Mutex<()>,
    _memory: Arc<dyn CodeMemory>,
}

struct QueuedCall {
    ordinal: u64,
    name: String,
    arguments: String,
}

impl CodeRuntime {
    /// Creates the harness's shared interpreter, without any ambient host APIs.
    pub fn new(memory: Arc<dyn CodeMemory>) -> Result<Self> {
        let runtime = Runtime::new().map_err(js_error)?;
        runtime.set_memory_limit(memory.credit_bytes().map_or(0, NonZeroUsize::get));
        Ok(Self {
            runtime,
            phase: tokio::sync::Mutex::new(()),
            _memory: memory,
        })
    }

    /// Runs TypeScript with only the admitted tools available as host functions.
    ///
    /// `outer` is the exact journal-authenticated code invocation. It supplies
    /// deterministic randomness/time and nested call identities. Host results
    /// remain in the original journal, so replay executes pure code again and
    /// receives those same results without repeating effects. All issued calls
    /// settle before success, including calls whose promises were not awaited.
    /// Effects run concurrently; promise results are delivered in issue order
    /// so replay has the same continuations without a separate ordering ledger.
    /// The outer code tool cannot recursively invoke itself.
    pub async fn execute(
        &self,
        outer: &ToolInvocation,
        source: &str,
        tools: &[ToolDefinition],
        host: Arc<dyn CodeHost + '_>,
        control: Arc<dyn CodeControl>,
    ) -> Result<Value> {
        control.check()?;
        let _credit = tokio::select! {
            error = control.interrupted() => return Err(error),
            credit = self._memory.acquire(source.len()) => credit?,
        };
        let javascript = strip_types(source)?;
        run(self, outer, &javascript, tools, host, control).await
    }

    async fn phase<T>(
        &self,
        control: &Arc<dyn CodeControl>,
        action: impl FnOnce(&Runtime) -> Result<T>,
    ) -> Result<T> {
        let _phase = tokio::select! {
            error = control.interrupted() => return Err(error),
            phase = self.phase.lock() => phase,
        };
        control.check()?;
        let failure = Arc::new(Mutex::new(None));
        let interrupt_failure = Arc::clone(&failure);
        let interrupt_control = Arc::clone(&control);
        self.runtime.set_interrupt_handler(Some(Box::new(move || {
            match interrupt_control.check() {
                Ok(()) => false,
                Err(error) => {
                    let mut failure = lock(&interrupt_failure);
                    if failure.is_none() {
                        *failure = Some(error);
                    }
                    true
                }
            }
        })));
        let _lease = PhaseLease(&self.runtime);
        let result = action(&self.runtime).and_then(|value| {
            while self
                .runtime
                .execute_pending_job()
                .map_err(|_| rejected("code microtask failed"))?
            {
                control.check()?;
            }
            Ok(value)
        });
        let interruption = lock(&failure).take();
        interruption.map_or(result, Err)
    }
}

// Every exit, including future drop, retires only this transient realm's jobs.
// Native callbacks merely enqueue descriptors; cleanup cannot dispatch effects.
struct PhaseLease<'a>(&'a Runtime);
impl Drop for PhaseLease<'_> {
    fn drop(&mut self) {
        self.0.set_interrupt_handler(Some(Box::new(|| true)));
        while self.0.is_job_pending() {
            let _ = self.0.execute_pending_job();
        }
        self.0.set_interrupt_handler(None);
    }
}

async fn run(
    engine: &CodeRuntime,
    outer: &ToolInvocation,
    javascript: &str,
    tools: &[ToolDefinition],
    host: Arc<dyn CodeHost + '_>,
    control: Arc<dyn CodeControl>,
) -> Result<Value> {
    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let context = engine
        .phase(&control, |runtime| {
            let context = Context::full(runtime).map_err(js_error)?;
            install(&context, outer, tools, Arc::clone(&queue))?;
            context
                .with(|ctx| {
                    ctx.eval::<(), _>(format!("__run(async function () {{\n{javascript}\n}});"))
                })
                .map_err(js_error)?;
            Ok(context)
        })
        .await?;
    let mut calls = FuturesOrdered::new();
    loop {
        control.check()?;
        let queued = std::mem::take(&mut *lock(&queue));
        for call in queued {
            let invocation = host.invocation(
                outer,
                call.ordinal,
                call.name,
                serde_json::from_str(&call.arguments)
                    .map_err(|_| rejected("code host arguments are not JSON"))?,
            )?;
            let host = Arc::clone(&host);
            calls.push_back(async move { (call.ordinal, host.dispatch(invocation).await) });
        }
        let state: String = engine
            .phase(&control, |_| {
                context.with(|ctx| ctx.eval("__state()")).map_err(js_error)
            })
            .await?;
        if calls.is_empty() {
            return match state.as_str() {
                "pending" => Err(rejected("code promise has no live host-call producer")),
                "rejected" => Err(rejected("code execution failed")),
                _ => serde_json::from_str(&state).map_err(|_| rejected("code result is not JSON")),
            };
        }
        let (ordinal, result) = tokio::select! {
            error = control.interrupted() => return Err(error),
            completed = calls.next() => completed.ok_or_else(|| rejected("code host-call producer was dropped"))?,
        };
        // Preserve the original storage/uncertain/refusal error. JavaScript
        // cannot catch it and turn an unacknowledged effect into success.
        let result = result?;
        let json = serde_json::to_string(&result.result.value)
            .map_err(|_| rejected("host result is not JSON"))?;
        engine
            .phase(&control, |_| {
                context
                    .with(|ctx| {
                        let complete: Function = ctx.globals().get("__complete")?;
                        complete.call::<_, ()>((ordinal.to_string(), json))
                    })
                    .map_err(js_error)
            })
            .await?;
    }
}

fn install(
    context: &Context,
    outer: &ToolInvocation,
    tools: &[ToolDefinition],
    queue: Arc<Mutex<VecDeque<QueuedCall>>>,
) -> Result<()> {
    let names: Vec<_> = tools.iter().map(|tool| tool.name.as_str()).collect();
    let allowed: HashSet<_> = names.iter().map(|name| (*name).to_owned()).collect();
    if allowed.len() != names.len() {
        return Err(rejected("code tool names are duplicated"));
    }
    let seed = outer.operation_id.into_bytes();
    let seed = u32::from_le_bytes(
        seed[..4]
            .try_into()
            .map_err(|_| rejected("code seed is malformed"))?,
    );
    let names =
        serde_json::to_string(&names).map_err(|_| rejected("code tool names are not JSON"))?;
    let recursive_name = outer.name.clone();
    context
        .with(|ctx| {
            let mut ordinal = 0_u64;
            let enqueue = Function::new(
                ctx.clone(),
                rquickjs::function::MutFn::new(move |name: String, arguments: String| {
                    if !allowed.contains(&name) || name == recursive_name {
                        return Err(rquickjs::Error::new_from_js_message(
                            "name",
                            "tool",
                            "tool is unavailable",
                        ));
                    }
                    let current = ordinal;
                    ordinal = ordinal.checked_add(1).ok_or_else(|| {
                        rquickjs::Error::new_from_js_message(
                            "ordinal",
                            "tool",
                            "host-call ordinal overflow",
                        )
                    })?;
                    lock(&queue).push_back(QueuedCall {
                        ordinal: current,
                        name,
                        arguments,
                    });
                    Ok(current.to_string())
                }),
            )?;
            ctx.globals().set("__enqueue", enqueue)?;
            ctx.eval::<(), _>(format!("({BOOTSTRAP})({seed}, {names});"))
        })
        .map_err(js_error)
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn rejected(reason: &'static str) -> Error {
    Error::ToolRejected {
        code: crate::wire::ErrorCode::Invalid,
        reason,
    }
}

fn js_error(_: rquickjs::Error) -> Error {
    rejected("code interpreter rejected the program")
}

const BOOTSTRAP: &str = include_str!("sandbox.js");

#[cfg(test)]
mod tests;

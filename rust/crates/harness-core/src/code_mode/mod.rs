//! A shared QuickJS interpreter; effects remain ordinary journaled tool calls.
//!
//! An execution owns a transient realm while holding the interpreter's credit.
//! No realm, process, task or runtime is retained for a dormant agent. The
//! interpreter has no module loader, filesystem, network, timer or process API.

mod buffers;
mod custody;
mod declarations;
#[cfg(feature = "filesystem")]
mod native_allocator;
mod typescript;

pub use declarations::declarations;

use crate::{
    Error, Result,
    tool::{ToolDefinition, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;
use buffers::{Buffers, Owned, PendingQueue};
use custody::Retained;
use futures::{StreamExt, stream::FuturesOrdered};
use rquickjs::{Context, Function, MultiWith, Runtime};
use serde_json::Value;
use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
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
    fn acquire(&self) -> BoxProviderFuture<'_, Result<Box<dyn CodeCredit>>>;
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
    pub credit: crate::NativeCustody,
}

/// The result and its exact original allocation custody. The caller retains
/// both until the ordinary tool result publication has consumed the value.
pub struct CodeOutput {
    /// Schema-checked by the ordinary tool settlement path.
    pub value: Value,
    /// Same original owner that admitted the result graph, without a re-claim.
    pub credit: crate::NativeCustody,
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
    compiler: typescript::Compiler,
    runtime: Runtime,
    phase: tokio::sync::Mutex<()>,
    _memory: Arc<dyn CodeMemory>,
    #[cfg(feature = "filesystem")]
    native_allocator: Option<Arc<native_allocator::AllocatorState>>,
}

struct QueuedCall {
    ordinal: u64,
    name: Owned<String>,
    arguments: Owned<String>,
}

// Protocol functions live in the realm, but permission to enter them lives in
// Rust. Admission is consumed before any JavaScript can run, including a
// thenable getter during promise resolution. A caught JS exception cannot
// acknowledge a forged protocol entry.
#[derive(Default)]
struct Protocol {
    admitted: AtomicBool,
    forged: AtomicBool,
}

impl Protocol {
    fn enter(&self) -> ProtocolLease<'_> {
        self.admitted.store(true, Ordering::Release);
        ProtocolLease(self)
    }

    fn consume(&self) -> rquickjs::Result<()> {
        if self.admitted.swap(false, Ordering::AcqRel) {
            Ok(())
        } else {
            self.forged.store(true, Ordering::Release);
            Err(rquickjs::Error::new_from_js_message(
                "code",
                "protocol",
                "interpreter protocol entry is not admitted",
            ))
        }
    }

    fn check(&self) -> Result<()> {
        if self.forged.load(Ordering::Acquire) {
            Err(rejected("code forged an interpreter protocol entry"))
        } else {
            Ok(())
        }
    }
}

struct ProtocolLease<'a>(&'a Protocol);
impl Drop for ProtocolLease<'_> {
    fn drop(&mut self) {
        self.0.admitted.store(false, Ordering::Release);
    }
}

impl CodeRuntime {
    /// Creates the harness's shared interpreter, without any ambient host APIs.
    pub fn new(memory: Arc<dyn CodeMemory>) -> Result<Self> {
        let runtime = Runtime::new().map_err(js_error)?;
        if let Some(credit) = memory.credit_bytes() {
            runtime.set_memory_limit(credit.get());
        }
        let compiler = typescript::Compiler::new(&runtime)?;
        Ok(Self {
            compiler,
            runtime,
            phase: tokio::sync::Mutex::new(()),
            _memory: memory,
            #[cfg(feature = "filesystem")]
            native_allocator: None,
        })
    }

    /// Creates the shared interpreter with its C heap funded by the original
    /// harness-wide native reservation. Each actual alloc/realloc/free retains
    /// or returns that reservation's exact capacity, with no allocation index.
    ///
    /// The TypeScript parser and output share this same credited C heap. The
    /// supplied execution permit also retains original Rust dispatch custody
    /// and waits on its original release event before profile activation.
    #[cfg(feature = "filesystem")]
    pub fn new_with_native_work(
        memory: Arc<dyn CodeMemory>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let (runtime, allocator) = native_allocator::runtime(work)?;
        let compiler = typescript::Compiler::new(&runtime);
        if let Some(error) = allocator.take_failure() {
            return Err(error);
        }
        let compiler = compiler?;
        Ok(Self {
            compiler,
            runtime,
            phase: tokio::sync::Mutex::new(()),
            _memory: memory,
            native_allocator: Some(allocator),
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
    ) -> Result<CodeOutput> {
        self.execute_with_input(outer, source, &Value::Null, tools, host, control)
            .await
    }

    /// Executes a pinned skill with its schema-checked input bound as JSON data,
    /// never as interpolated JavaScript or shell text.
    pub async fn execute_with_input(
        &self,
        outer: &ToolInvocation,
        source: &str,
        input: &Value,
        tools: &[ToolDefinition],
        host: Arc<dyn CodeHost + '_>,
        control: Arc<dyn CodeControl>,
    ) -> Result<CodeOutput> {
        control.check()?;
        let _credit = tokio::select! {
            error = control.interrupted() => return Err(error),
            credit = self._memory.acquire() => credit?,
        };
        run(self, outer, source, input, tools, host, control).await
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
        let failure = self.retained_arc(Mutex::new(None))?;
        let interrupt_failure = Arc::clone(&failure);
        let interrupt_control = Arc::clone(&control);
        let lease = self.interrupt_handler(move || match interrupt_control.check() {
            Ok(()) => false,
            Err(error) => {
                let mut failure = lock(&interrupt_failure);
                if failure.is_none() {
                    *failure = Some(error);
                }
                true
            }
        })?;
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
        drop(lease);
        #[cfg(feature = "filesystem")]
        if let Some(error) = self
            .native_allocator
            .as_ref()
            .and_then(|state| state.take_failure())
        {
            return Err(error);
        }
        let interruption = lock(&failure).take();
        interruption.map_or(result, Err)
    }
}

// Every exit, including future drop, retires only this transient realm's jobs.
// Native callbacks merely enqueue descriptors; cleanup cannot dispatch effects.
struct PhaseLease<'a> {
    runtime: &'a Runtime,
    _custody: crate::NativeCustody,
}
impl Drop for PhaseLease<'_> {
    fn drop(&mut self) {
        self.runtime.set_interrupt_handler(Some(Box::new(|| true)));
        while self.runtime.is_job_pending() {
            let _ = self.runtime.execute_pending_job();
        }
        self.runtime.set_interrupt_handler(None);
    }
}

async fn run(
    engine: &CodeRuntime,
    outer: &ToolInvocation,
    source: &str,
    input: &Value,
    tools: &[ToolDefinition],
    host: Arc<dyn CodeHost + '_>,
    control: Arc<dyn CodeControl>,
) -> Result<CodeOutput> {
    let buffers = Buffers::new(engine);
    let queue = engine.retained_arc(Mutex::new(PendingQueue::new(&buffers)?))?;
    let protocol = engine.retained_arc(Protocol::default())?;
    let context = engine
        .phase(&control, |runtime| {
            let context = Context::full(runtime).map_err(js_error)?;
            install(
                &context,
                outer,
                tools,
                input,
                Arc::clone(&queue),
                Arc::clone(&protocol),
                buffers.clone(),
            )?;
            // Both contexts belong to this one runtime. MultiWith holds its
            // original lock once, so transformation never leaks a Rust-owned
            // output buffer or exposes the compiler realm to model code.
            (&engine.compiler.context, &context)
                .with(|(compiler, ctx)| {
                    let javascript = typescript::Compiler::prepare(&compiler, source)?;
                    let constructor: Function = ctx.eval("(async function(){}).constructor")?;
                    let body: Function = constructor.call((javascript,))?;
                    let run: Function = ctx.globals().get("__run")?;
                    let _entry = protocol.enter();
                    run.call::<_, ()>((body,))
                })
                .map_err(js_error)?;
            Ok(context)
        })
        .await?;
    protocol.check()?;
    let mut calls = FuturesOrdered::new();
    loop {
        control.check()?;
        let queued = lock(&queue).take()?;
        let _queue_credit = queued.credit;
        for call in queued.value {
            let arguments = buffers.decode(&call.arguments.value)?;
            drop(call.arguments);
            let invocation =
                host.invocation(outer, call.ordinal, call.name.value, arguments.value)?;
            let host = Arc::clone(&host);
            calls.push_back(async move {
                let _name_credit = call.name.credit;
                let _argument_credit = arguments.credit;
                (call.ordinal, host.dispatch(invocation).await)
            });
        }
        drop(_queue_credit);
        let state = engine
            .phase(&control, |_| {
                context.with(|ctx| {
                    let state: rquickjs::CString = ctx.eval("__state()").map_err(js_error)?;
                    buffers.text(state.as_str())
                })
            })
            .await?;
        protocol.check()?;
        lock(&queue).check()?;
        if calls.is_empty() {
            return match state.value.as_str() {
                "pending" => Err(rejected("code promise has no live host-call producer")),
                "rejected" => Err(rejected("code execution failed")),
                _ => buffers.decode(&state.value),
            };
        }
        let (ordinal, result) = tokio::select! {
            error = control.interrupted() => return Err(error),
            completed = calls.next() => completed.ok_or_else(|| rejected("code host-call producer was dropped"))?,
        };
        // Preserve the original storage/uncertain/refusal error. JavaScript
        // cannot catch it and turn an unacknowledged effect into success.
        let result = result?;
        let json = buffers.json(&result.result.value)?;
        let ordinal = buffers.format(format_args!("{ordinal}"))?;
        engine
            .phase(&control, |_| {
                let _entry = protocol.enter();
                context
                    .with(|ctx| {
                        let complete: Function = ctx.globals().get("__complete")?;
                        complete.call::<_, ()>((ordinal.value.as_str(), json.value.as_str()))
                    })
                    .map_err(js_error)
            })
            .await?;
        protocol.check()?;
        lock(&queue).check()?;
    }
}

fn install(
    context: &Context,
    outer: &ToolInvocation,
    tools: &[ToolDefinition],
    input: &Value,
    queue: Arc<Retained<Mutex<PendingQueue>>>,
    protocol: Arc<Retained<Protocol>>,
    buffers: Buffers,
) -> Result<()> {
    let seed = outer.operation_id.into_bytes();
    let seed = u32::from_le_bytes(
        seed[..4]
            .try_into()
            .map_err(|_| rejected("code seed is malformed"))?,
    );
    let recursive_name = buffers.text(&outer.name)?;
    let input = buffers.json(input)?;
    context
        .with(|ctx| {
            let names = rquickjs::Array::new(ctx.clone())?;
            for (index, tool) in tools.iter().enumerate() {
                names.set(index, tool.name.as_str())?;
            }
            ctx.globals()
                .set("__input", ctx.json_parse(input.value.as_str())?)?;
            let mut ordinal = 0_u64;
            let enqueue = Function::new(
                ctx.clone(),
                rquickjs::function::MutFn::new(
                    move |callback_ctx: rquickjs::Ctx,
                          name: rquickjs::CString,
                          arguments: rquickjs::CString| {
                        if name.as_str() == recursive_name.value().as_str() {
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
                        let result = (|| {
                            let name = buffers.text(name.as_str())?;
                            let arguments = buffers.text(arguments.as_str())?;
                            let ordinal_text = buffers.format(format_args!("{current}"))?;
                            lock(&queue).push(QueuedCall {
                                ordinal: current,
                                name,
                                arguments,
                            })?;
                            rquickjs::String::from_str(callback_ctx.clone(), &ordinal_text.value)
                                .map_err(js_error)
                        })();
                        result.map_err(|error| {
                            lock(&queue).fail(error);
                            rquickjs::Error::new_from_js("code", "original work")
                        })
                    },
                ),
            )?;
            ctx.globals().set("__enqueue", enqueue)?;
            ctx.globals().set(
                "__admit",
                Function::new(ctx.clone(), move || protocol.consume())?,
            )?;
            let bootstrap: Function = ctx.eval(BOOTSTRAP)?;
            bootstrap.call::<_, ()>((seed, names))
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

const BOOTSTRAP: &str = concat!("(", include_str!("sandbox.js"), ")");

#[cfg(test)]
mod tests;

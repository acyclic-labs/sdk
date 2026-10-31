//! A shared QuickJS interpreter; effects remain ordinary journaled tool calls.
//!
//! An execution owns a transient realm while holding the interpreter's credit.
//! No realm, process, task or runtime is retained for a dormant agent. The
//! interpreter has no module loader, filesystem, network, timer or process API.

mod buffers;
mod custody;
mod declarations;
mod engine;
#[cfg(feature = "filesystem")]
mod native_allocator;
mod typescript;

pub use declarations::declarations;

use crate::{
    Error, NativeFutureBundle, NativeOrdered, Result,
    tool::{ToolDefinition, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;
use buffers::{Buffers, Owned, PendingQueue};
use custody::Retained;
use engine::{Context, Runtime};
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
/// `poll_interrupted`. There is no interpreter-specific timer or step allowance.
pub trait CodeControl: Send + Sync {
    /// Charges/checks the original work credit at an interpreter interruption.
    fn check(&self) -> Result<()>;
    /// Waits on the original cancellation/owner-loss event, never a probe.
    fn poll_interrupted(&self, context: &mut std::task::Context<'_>) -> std::task::Poll<Error>;
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
    /// Admits the concrete future before executing or replaying one stable
    /// nested call through the ordinary dispatcher. Admission executes no effect.
    fn dispatch(
        &self,
        invocation: ToolInvocation,
    ) -> Result<NativeFutureBundle<'_, CodeHostResult>>;
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

struct CallFuture<'a> {
    ordinal: u64,
    future: NativeFutureBundle<'a, CodeHostResult>,
    // The dispatched future is destroyed before its borrowed payload funding.
    _name_credit: crate::NativeCustody,
    _argument_credit: crate::NativeCustody,
}

impl std::future::Future for CallFuture<'_> {
    type Output = Result<(u64, CodeHostResult)>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        std::pin::Pin::new(&mut this.future)
            .poll(context)
            .map(|result| result.map(|value| (this.ordinal, value)))
    }
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

    fn consume(&self) -> Result<()> {
        if self.admitted.swap(false, Ordering::AcqRel) {
            Ok(())
        } else {
            self.forged.store(true, Ordering::Release);
            Err(rejected("interpreter protocol entry is not admitted"))
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
        let runtime = Runtime::new()?;
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
            error = futures::future::poll_fn(|context| control.poll_interrupted(context)) => return Err(error),
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
            error = futures::future::poll_fn(|context| control.poll_interrupted(context)) => return Err(error),
            phase = self.phase.lock() => phase,
        };
        control.check()?;
        let failure = Mutex::new(None);
        let interruption = engine::Interrupt {
            control,
            failure: &failure,
        };
        let lease = self.runtime.interrupt(&interruption);
        let result = action(&self.runtime).and_then(|value| {
            while self.runtime.execute_pending_job()? {
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
            let context = Context::new(
                runtime,
                Some(engine::Callbacks {
                    queue: Arc::clone(&queue),
                    protocol: Arc::clone(&protocol),
                    buffers: buffers.clone(),
                    recursive: buffers.text(&outer.name)?,
                    ordinal: 0,
                }),
            )?;
            engine::at_stage(
                install(&context, outer, tools, input, &buffers),
                "code sandbox initialization failed",
            )?;
            engine
                .compiler
                .context
                .with_pair(&context, |compiler, ctx| {
                    let javascript = typescript::Compiler::prepare(compiler, source)?;
                    let constructor = ctx.eval(c"(async function(){}).constructor")?;
                    let body = engine::at_stage(
                        constructor.call([javascript]),
                        "code program compilation failed",
                    )?;
                    let run = ctx.global()?.get(c"__run")?;
                    let _entry = protocol.enter();
                    engine::at_stage(run.call([body]), "code program startup failed").map(drop)
                })?;
            Ok(context)
        })
        .await?;
    protocol.check()?;
    let work = engine.native_allocator.as_ref().map(|state| state.work());
    let mut calls = NativeOrdered::new(work)?;
    loop {
        control.check()?;
        let queued = lock(&queue).take()?;
        let _queue_credit = queued.credit;
        for call in queued.value {
            let arguments = buffers.decode(&call.arguments.value)?;
            drop(call.arguments);
            let invocation =
                host.invocation(outer, call.ordinal, call.name.value, arguments.value)?;
            let dispatched = host.dispatch(invocation)?;
            calls.push_back(CallFuture {
                ordinal: call.ordinal,
                future: dispatched,
                _name_credit: call.name.credit,
                _argument_credit: arguments.credit,
            })?;
        }
        drop(_queue_credit);
        let state = engine
            .phase(&control, |_| {
                context.with(|ctx| {
                    let state = engine::at_stage(
                        ctx.eval(c"__state()"),
                        "code execution state projection failed",
                    )?;
                    let text = state.text()?;
                    buffers.text(text.as_str()?)
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
            error = futures::future::poll_fn(|context| control.poll_interrupted(context)) => return Err(error),
            completed = calls.next() => completed?.ok_or_else(|| rejected("code host-call producer was dropped"))??,
        };
        // Preserve the original storage/uncertain/refusal error. JavaScript
        // cannot catch it and turn an unacknowledged effect into success.
        let json = buffers.json(&result.result.value)?;
        let ordinal = buffers.format(format_args!("{ordinal}"))?;
        engine
            .phase(&control, |_| {
                let _entry = protocol.enter();
                context.with(|ctx| {
                    let complete = ctx.global()?.get(c"__complete")?;
                    engine::at_stage(
                        complete.call([ctx.string(&ordinal.value)?, ctx.string(&json.value)?]),
                        "code host result delivery failed",
                    )
                    .map(drop)
                })
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
    buffers: &Buffers,
) -> Result<()> {
    let seed = outer.operation_id.into_bytes();
    let seed = u32::from_le_bytes(
        seed[..4]
            .try_into()
            .map_err(|_| rejected("code seed is malformed"))?,
    );
    let input = buffers.json(input)?;
    context.with(|ctx| {
        let names = ctx.array()?;
        for (index, tool) in tools.iter().enumerate() {
            names.set_index(
                u32::try_from(index).map_err(|_| rejected("code tool index overflow"))?,
                ctx.string(&tool.name)?,
            )?;
        }
        let global = ctx.global()?;
        let parse = global.get(c"JSON")?.get(c"parse")?;
        global.set(c"__input", parse.call([ctx.string(&input.value)?])?)?;
        global.set(c"__enqueue", ctx.callback(c"enqueue", true)?)?;
        global.set(c"__admit", ctx.callback(c"admit", false)?)?;
        ctx.eval(BOOTSTRAP)?
            .call([ctx.number(seed), names])
            .map(drop)
    })
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

const BOOTSTRAP: &std::ffi::CStr = match std::ffi::CStr::from_bytes_with_nul(
    concat!("(", include_str!("sandbox.js"), ")\0").as_bytes(),
) {
    Ok(source) => source,
    Err(_) => panic!("code bootstrap must have exactly one trailing zero"),
};

#[cfg(test)]
mod tests;

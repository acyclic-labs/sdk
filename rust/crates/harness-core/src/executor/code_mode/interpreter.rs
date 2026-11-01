use super::{Call, CancellationToken, Request};
use rquickjs::{AsyncContext, AsyncRuntime, CaughtError, Function, Promise, prelude::Async};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

// Protective engine ceilings, independent of product capacity and tool counts.
const MEMORY_BYTES: usize = 1024 * 1024 * 1024;
pub(super) const INTERRUPT_BUDGET: u64 = 1_000_000_000;

struct Reply(acyclic_stream::OwnedBytes);
impl<'js> rquickjs::IntoJs<'js> for Reply {
    fn into_js(self, ctx: &rquickjs::Ctx<'js>) -> rquickjs::Result<rquickjs::Value<'js>> {
        let text = std::str::from_utf8(&self.0).map_err(|_| rquickjs::Error::Unknown)?;
        // The host credit remains live until the engine owns its copy.
        rquickjs::String::from_str(ctx.clone(), text).map(rquickjs::String::into_value)
    }
}

pub(super) async fn run(
    code: String,
    definitions: acyclic_stream::OwnedBytes,
    send: mpsc::Sender<Request>,
    cancellation: CancellationToken,
    maximum: usize,
    allocator: super::allocator::EngineAllocator,
    interrupt_budget: u64,
) -> Result<Value, String> {
    let bridge = allocator.clone();
    let runtime = AsyncRuntime::new_with_alloc(allocator).map_err(|error| error.to_string())?;
    runtime.set_memory_limit(MEMORY_BYTES).await;
    let interrupt = cancellation.clone();
    let mut remaining = interrupt_budget;
    runtime
        .set_interrupt_handler(Some(Box::new(move || {
            remaining = remaining.saturating_sub(1);
            interrupt.is_cancelled() || remaining == 0
        })))
        .await;
    let context = AsyncContext::full(&runtime)
        .await
        .map_err(|error| error.to_string())?;
    let execution = context.async_with(async move |ctx| {
        // Bound diagnostics inside JS before converting them to a host string.
        let diagnostic = ctx.eval::<Function, _>(r#"(() => {
            const text = String, slice = Function.prototype.call.bind(String.prototype.slice);
            return value => slice(text(value), 0, 2048);
        })()"#).ok();
        let program = async {
            // No libc, loader, OS modules, environment, timers or I/O are
            // installed. Remove ambient time/randomness to make receipt replay
            // deterministic; the only external data comes from admitted tools.
            ctx.eval::<(), _>("delete globalThis.Date; delete Math.random;")?;
            let rpc = Function::new(ctx.clone(), Async(move |name: Option<rquickjs::String<'_>>, arguments: rquickjs::String<'_>| {
                let send = send.clone();
                let input = name.as_ref().map(|name| bridge.copy_text(name)).transpose().and_then(|name| bridge.copy_text(&arguments).map(|arguments| (name, arguments)));
                async move {
                    let (name, arguments) = input?;
                    let (reply, receive) = oneshot::channel();
                    let request = match name {
                        Some(name) => Request::Tool(Call { name, arguments, reply }),
                        None => Request::Log { arguments, reply },
                    };
                    send.send(request).await.map_err(|_| rquickjs::Error::Unknown)?;
                    receive.await.map(Reply).map_err(|_| rquickjs::Error::Unknown)
                }
            }))?;
            let definitions = rquickjs::String::from_str(ctx.clone(), std::str::from_utf8(&definitions).map_err(|_| rquickjs::Error::Unknown)?)?;
            let setup: Function = ctx.eval(r#"
                (rpc, definitions) => {
                    const parse = JSON.parse, stringify = JSON.stringify;
                    const ErrorType = Error, PromiseType = Promise;
                    const define = Object.defineProperty, tools = Object.create(null);
                    const then = Promise.prototype.then;
                    const constructor = Object.freeze({[Symbol.species]: PromiseType});
                    // Await only private promises with a stable constructor. User
                    // mutations of Promise.prototype cannot replace their await.
                    const stable = promise => define(promise, 'constructor', {value: PromiseType});
                    let pending = 0, wake;
                    let closed = false;
                    const invoke = async (name, args) => {
                        pending++;
                        try {
                            const reply = parse(await stable(rpc(name, stringify(args))));
                            if (name === null) return;
                            if (!reply.ok) throw new ErrorType(reply.value);
                            return reply.value;
                        } finally {
                            if (--pending === 0 && wake) wake();
                        }
                    };
                    for (const definition of parse(definitions)) {
                        const tool = args => {
                            if (closed) throw new ErrorType('JavaScript calls are closed');
                            return invoke(definition.name, args);
                        };
                        Object.defineProperty(tool, 'input_schema', {value: definition.input_schema});
                        tools[definition.name] = Object.freeze(tool);
                    }
                    Object.defineProperty(globalThis, 'tools', {value: Object.freeze(tools)});
                    Object.defineProperty(globalThis, 'console', {value: Object.freeze({
                        log: (...values) => {
                            if (closed) throw new ErrorType('JavaScript calls are closed');
                            return invoke(null, values);
                        }
                    })});
                    return {
                        // The Rust promise future attaches its waker through then.
                        protect: promise => define(define(promise, 'constructor', {value: constructor}), 'then', {value: then}),
                        drain: async () => {
                            while (pending) await stable(new PromiseType(resolve => { wake = resolve; }));
                            closed = true;
                        }
                    };
                }
            "#)?;
            let bridge: rquickjs::Object = setup.call((rpc, definitions))?;
            let drain: Function = bridge.get("drain")?;
            let protect: Function = bridge.get("protect")?;
            let program: Promise = protect.call((ctx.eval_promise(code)?,))?;
            // Async eval wraps its completion in { value } to avoid assimilating
            // a user-returned promise. Encode the completion's value once.
            let result = program.into_future::<rquickjs::Object>().await
                .and_then(|completion| completion.get::<_, rquickjs::Value>("value"));
            // Encoding can execute user getters/toJSON, including more tool calls.
            let json = result.and_then(|result| ctx.json_stringify(result));
            // Format exceptions before draining: their toString may also call tools.
            let json = json.map_err(|error| {
                let value = match CaughtError::from_error(&ctx, error) {
                    CaughtError::Error(error) => return error.to_string().chars().take(2048).collect(),
                    CaughtError::Exception(error) => error.into_value(),
                    CaughtError::Value(value) => value,
                };
                diagnostic.as_ref().and_then(|format| format.call::<_, String>((value,)).ok())
                    .unwrap_or_else(|| "JavaScript exception or interrupt".into())
            });
            // Fire-and-forget calls, including encoding calls, retire first.
            let drained: Promise = protect.call((drain.call::<_, Promise>(())?,))?;
            drained.into_future::<()>().await?;
            let json = match json {
                Ok(json) => json,
                Err(error) => return Ok(Err(error)),
            };
            let Some(json) = json else { return Ok(Ok(Value::Null)); };
            let json = json.to_cstring()?;
            if json.len() > maximum {
                return Ok(Ok(serde_json::json!({"truncated": true, "bytes": json.len()})));
            }
            serde_json::from_slice(json.as_str().as_bytes()).map(Ok).map_err(|_| rquickjs::Error::Unknown)
        }.await;
        program.unwrap_or_else(|error| Err({
            let value = match CaughtError::from_error(&ctx, error) {
                CaughtError::Error(error) => return Err(error.to_string().chars().take(2048).collect()),
                CaughtError::Exception(error) => error.into_value(),
                CaughtError::Value(value) => value,
            };
            diagnostic.as_ref().and_then(|format| format.call::<_, String>((value,)).ok())
                .unwrap_or_else(|| "JavaScript exception or interrupt".into())
        }))
    });
    let execution = async {
        futures::pin_mut!(execution);
        loop {
            // The runtime owns every possible producer: JavaScript jobs and
            // bound-tool futures. Its idle event retires both before reporting
            // that a pending program has nobody left to resolve its promise.
            let idle = runtime.idle();
            futures::pin_mut!(idle);
            if let futures::future::Either::Left((result, _)) =
                futures::future::select(execution.as_mut(), idle).await
            {
                return result;
            }
            // The idle driver may have resolved the program, or this poll may
            // start encoding/draining calls. Check for newly created producers
            // only after giving that continuation its turn.
            if let std::task::Poll::Ready(result) = futures::poll!(execution.as_mut()) {
                return result;
            }
            if !runtime.is_job_pending().await {
                return Err(
                    "JavaScript promise cannot settle: no pending jobs or tool calls".into(),
                );
            }
        }
    };
    let cancelled = cancellation.cancelled();
    futures::pin_mut!(execution, cancelled);
    match futures::future::select(cancelled, execution).await {
        futures::future::Either::Left(_) => Err("JavaScript interrupted".into()),
        futures::future::Either::Right((result, _)) => result,
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn producerless_promise_returns_without_waiting_for_an_external_wake() {
        use futures::FutureExt;

        let (send, _receive) = tokio::sync::mpsc::channel(1);
        let result = super::run(
            "await new Promise(() => {})".into(),
            acyclic_stream::OwnedBytes::from_static(b"[]"),
            send,
            tokio_util::sync::CancellationToken::new(),
            4096,
            super::super::allocator::EngineAllocator::new(None),
            super::INTERRUPT_BUDGET,
        )
        .now_or_never()
        .expect("no producer remains to wake this program");
        assert_eq!(
            result.unwrap_err(),
            "JavaScript promise cannot settle: no pending jobs or tool calls"
        );
    }

    #[tokio::test]
    async fn dropping_the_dispatcher_wakes_the_pending_rpc_with_an_error() {
        for code in [
            "await tools.code({code:'1'})",
            "await console.log('message')",
        ] {
            let tool = crate::tools::catalog()
                .find(|tool| tool.name == "code")
                .unwrap()
                .bind()
                .unwrap();
            let definitions = serde_json::to_vec(&vec![tool.definition]).unwrap().into();
            let (send, mut receive) = tokio::sync::mpsc::channel(1);
            let (result, ()) = tokio::join!(
                acyclic_native_runtime::blocking_future(super::run(
                    code.into(),
                    definitions,
                    send,
                    tokio_util::sync::CancellationToken::new(),
                    4096,
                    super::super::allocator::EngineAllocator::new(None),
                    super::INTERRUPT_BUDGET,
                )),
                async move {
                    let call = receive.recv().await.expect("engine submitted its call");
                    drop(receive);
                    drop(call);
                }
            );
            assert!(result.unwrap().is_err());
        }
    }

    #[tokio::test]
    async fn protective_interrupt_stops_cpu_bytecode_without_a_host_event() {
        let (send, _receive) = tokio::sync::mpsc::channel(1);
        let result = acyclic_native_runtime::blocking_future(super::run(
            "while (true) {}".into(),
            acyclic_stream::OwnedBytes::from_static(b"[]"),
            send,
            tokio_util::sync::CancellationToken::new(),
            4096,
            super::super::allocator::EngineAllocator::new(None),
            16,
        ))
        .await
        .unwrap();
        assert!(result.unwrap_err().contains("interrupt"));
    }
}

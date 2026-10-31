use super::{Call, CancellationToken};
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
    send: mpsc::Sender<Call>,
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
            let rpc = Function::new(ctx.clone(), Async(move |name: rquickjs::String<'_>, arguments: rquickjs::String<'_>| {
                let send = send.clone();
                let input = bridge.copy_text(&name).and_then(|name| bridge.copy_text(&arguments).map(|arguments| (name, arguments)));
                async move {
                    let (name, arguments) = input?;
                    let (reply, receive) = oneshot::channel();
                    send.send(Call { name, arguments, reply }).await.map_err(|_| rquickjs::Error::Unknown)?;
                    receive.await.map(Reply).map_err(|_| rquickjs::Error::Unknown)
                }
            }))?;
            let definitions = rquickjs::String::from_str(ctx.clone(), std::str::from_utf8(&definitions).map_err(|_| rquickjs::Error::Unknown)?)?;
            let setup: Function = ctx.eval(r#"
                (rpc, definitions) => {
                    const parse = JSON.parse, stringify = JSON.stringify;
                    const ErrorType = Error, allSettled = Promise.allSettled.bind(Promise);
                    const pending = new Set(), tools = Object.create(null);
                    let closed = false;
                    for (const definition of parse(definitions)) {
                        const invoke = async args => {
                            const reply = parse(await rpc(definition.name, stringify(args)));
                            if (!reply.ok) throw new ErrorType(reply.value);
                            return reply.value;
                        };
                        const tool = args => {
                            if (closed) throw new ErrorType('JavaScript calls are closed');
                            const promise = invoke(args);
                            pending.add(promise);
                            promise.then(() => pending.delete(promise), () => pending.delete(promise));
                            return promise;
                        };
                        Object.defineProperty(tool, 'input_schema', {value: definition.input_schema});
                        tools[definition.name] = Object.freeze(tool);
                    }
                    Object.defineProperty(globalThis, 'tools', {value: Object.freeze(tools)});
                    return async () => {
                        while (pending.size) await allSettled(Array.from(pending));
                        closed = true;
                    };
                }
            "#)?;
            let drain: Function = setup.call((rpc, definitions))?;
            let result = ctx.eval_promise(code)?.into_future::<rquickjs::Value>().await;
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
            drain.call::<_, Promise>(())?.into_future::<()>().await?;
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
    async fn dropping_the_dispatcher_wakes_the_pending_rpc_with_an_error() {
        let tool = crate::tools::catalog()
            .find(|tool| tool.name == "run_js")
            .unwrap()
            .bind()
            .unwrap();
        let definitions = serde_json::to_vec(&vec![tool.definition]).unwrap().into();
        let (send, mut receive) = tokio::sync::mpsc::channel(1);
        let (result, ()) = tokio::join!(
            acyclic_native_runtime::blocking_future(super::run(
                "await tools.run_js({code:'1'})".into(),
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

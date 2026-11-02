use super::{Call, CancellationToken, Request};
use rquickjs::{
    AsyncContext, AsyncRuntime, CaughtError, Function, Module, Promise, prelude::Async,
};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

// Protective engine ceilings, independent of product capacity and tool
// counts. Settings evaluation (`crate::config`) shares the memory ceiling.
pub(crate) const MEMORY_BYTES: usize = 1024 * 1024 * 1024;
pub(super) const INTERRUPT_BUDGET: u64 = 1_000_000_000;

struct Reply(Option<acyclic_stream::OwnedBytes>);
impl<'js> rquickjs::IntoJs<'js> for Reply {
    fn into_js(self, ctx: &rquickjs::Ctx<'js>) -> rquickjs::Result<rquickjs::Value<'js>> {
        let Some(bytes) = self.0 else {
            return Ok(rquickjs::Value::new_undefined(ctx.clone()));
        };
        let text = std::str::from_utf8(&bytes).map_err(|_| rquickjs::Error::Unknown)?;
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
    let compiled = crate::config::compile_module("code.ts", code.as_bytes())?;
    if let Some(import) = compiled
        .imports
        .iter()
        .find(|name| name.as_str() != "acyclic")
    {
        return Err(format!(
            "code.ts imports {import}: effective file imports are not bound"
        ));
    }
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
        let diagnostic = ctx
            .eval::<Function, _>(
                r#"(() => {
            const text = String, slice = Function.prototype.call.bind(String.prototype.slice);
            return value => slice(text(value), 0, 2048);
        })()"#,
            )
            .ok();
        let describe_error = |error| {
            let value = match CaughtError::from_error(&ctx, error) {
                CaughtError::Error(error) => return error.to_string().chars().take(2048).collect(),
                CaughtError::Exception(error) => error.into_value(),
                CaughtError::Value(value) => value,
            };
            diagnostic
                .as_ref()
                .and_then(|format| format.call::<_, String>((value,)).ok())
                .unwrap_or_else(|| "JavaScript exception or interrupt".into())
        };
        let program = async {
            // No libc, loader, OS modules, environment, timers or I/O are
            // installed. Remove ambient time/randomness to make receipt replay
            // deterministic; the only external data comes from admitted tools.
            ctx.eval::<(), _>("delete globalThis.Date; delete Math.random;")?;
            let rpc = Function::new(
                ctx.clone(),
                Async(
                    move |name: Option<rquickjs::String<'_>>, arguments: rquickjs::String<'_>| {
                        let send = send.clone();
                        let input = name
                            .as_ref()
                            .map(|name| bridge.copy_text(name))
                            .transpose()
                            .and_then(|name| {
                                bridge
                                    .copy_text(&arguments)
                                    .map(|arguments| (name, arguments))
                            });
                        async move {
                            let (name, arguments) = input?;
                            let (reply, receive) = oneshot::channel();
                            let request = match name {
                                Some(name) => Request::Tool(Call {
                                    name,
                                    arguments,
                                    reply,
                                }),
                                None => Request::Log { arguments, reply },
                            };
                            send.send(request)
                                .await
                                .map_err(|_| rquickjs::Error::Unknown)?;
                            receive
                                .await
                                .map(Reply)
                                .map_err(|_| rquickjs::Error::Unknown)
                        }
                    },
                ),
            )?;
            let definitions = rquickjs::String::from_str(
                ctx.clone(),
                std::str::from_utf8(&definitions).map_err(|_| rquickjs::Error::Unknown)?,
            )?;
            let setup: Function = ctx.eval(include_str!("bridge.js"))?;
            let bridge: rquickjs::Object = setup.call((rpc, definitions))?;
            let drain: Function = bridge.get("drain")?;
            let protect: Function = bridge.get("protect")?;
            Module::declare(ctx.clone(), "acyclic", include_str!("acyclic.mjs"))?;
            let module = Module::declare(ctx.clone(), "code.ts", compiled.script)?;
            let (module, evaluated) = module.eval()?;
            let evaluated: Promise = protect.call((evaluated,))?;
            let result = async {
                evaluated.into_future::<()>().await?;
                let value: rquickjs::Value = module.get("default")?;
                let complete: Function = bridge.get("complete")?;
                let completion: Promise =
                    protect.call((complete.call::<_, Promise>((value,))?,))?;
                completion
                    .into_future::<rquickjs::Object>()
                    .await?
                    .get::<_, rquickjs::Value>("value")
            }
            .await;
            // Encoding can execute user getters/toJSON, including more tool calls.
            let json = result.and_then(|result| ctx.json_stringify(result));
            // Format exceptions before draining: their toString may also call tools.
            let json = json.map_err(&describe_error);
            // Fire-and-forget calls, including encoding calls, retire first.
            let drained: Promise = protect.call((drain.call::<_, Promise>(())?,))?;
            drained.into_future::<()>().await?;
            let json = match json {
                Ok(json) => json,
                Err(error) => return Ok(Err(error)),
            };
            let Some(json) = json else {
                return Ok(Ok(Value::Null));
            };
            let json = json.to_cstring()?;
            if json.len() > maximum {
                return Ok(Err("JavaScript result exceeds admitted output".into()));
            }
            serde_json::from_slice(json.as_str().as_bytes())
                .map(Ok)
                .map_err(|_| rquickjs::Error::Unknown)
        }
        .await;
        program
            .map_err(describe_error)
            .and_then(std::convert::identity)
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
    #[cfg(feature = "filesystem")]
    #[tokio::test]
    async fn acyclic_files_use_the_real_conditional_publisher_and_pinned_reader()
    -> crate::Result<()> {
        use crate::{
            Capabilities, OperationId,
            conversation::{Limits, VolumeOperation},
            runtime::{Bindings, RuntimeScope, ToolContext},
            tool::ToolInvocation,
        };
        use std::sync::Arc;
        let storage =
            crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65536).await?;
        let content = storage.content_bindings();
        let grants = Capabilities::new([
            content
                .volume
                .capability(VolumeOperation::Read, &mut None)?,
            content
                .volume
                .capability(VolumeOperation::Write, &mut None)?,
            "tool:call:read".into(),
            "tool:call:write".into(),
        ]);
        let task = Arc::new(
            Bindings {
                scope: RuntimeScope::new(
                    grants,
                    Limits {
                        file_bytes: 65536,
                        render_bytes: 4096,
                        ..Limits::default()
                    },
                )?,
                content: Some(content.clone()),
                ..Bindings::local()
            }
            .build()?
            .agent_context(&Capabilities::default(), content)?,
        );
        let code = r#"
            import {read, write, spawn, fork, DeniedError, ConflictError} from 'acyclic';
            const first = await write('file.txt', 'one\r\nβ\n', 'absent');
            const file = await read('file.txt');
            const lines = file.lines(1, 1);
            await write('file.txt', 'changed', first);
            let conflict;
            try { await write('file.txt', 'wrong', file.version); }
            catch (error) { if (!(error instanceof ConflictError)) throw error; conflict = error.current.text(); }
            try { await write('file.txt', 'wrong', 'absent'); throw new Error('create replaced a file'); }
            catch (error) { if (!(error instanceof ConflictError)) throw error; }
            const absent = await read('missing.txt');
            const stubs = [];
            for (const create of [spawn, fork]) {
                try { await create(); throw new Error('creation succeeded'); }
                catch (error) { if (!(error instanceof DeniedError)) throw error; stubs.push(error.reason); }
            }
            await write('agent.ts', {model: 'example/model'}, 'any');
            const moduleText = (await read('agent.ts')).text();
            const notebook = '{"cells":[],"nbformat":4}';
            await write('empty.ipynb', notebook, 'any');
            if ((await read('empty.ipynb')).text() !== notebook) throw new Error('source was projected');
            const large = 'β\r\n'.repeat(2000);
            await write('large.txt', large, 'any');
            const paged = await read('large.txt');
            if (paged.text() !== large || paged.lines(1999, 1)[0] !== 'β') throw new Error('page lost bytes');
            export default {lines, conflict, absent, stubs, moduleText};
        "#;
        let (send, mut receive) = tokio::sync::mpsc::channel(1);
        let (result, served) = tokio::join!(
            acyclic_native_runtime::blocking_future(super::run(
                code.into(),
                acyclic_stream::OwnedBytes::from_static(b"[]"),
                send,
                tokio_util::sync::CancellationToken::new(),
                4096,
                super::super::allocator::EngineAllocator::new(None),
                super::INTERRUPT_BUDGET,
            )),
            async move {
                let mut count = 0;
                let mut large_pages = 0;
                while let Some(request) = receive.recv().await {
                    let super::Request::Tool(call) = request else {
                        panic!("unexpected log");
                    };
                    let name = call
                        .name
                        .value
                        .strip_prefix("acyclic.")
                        .expect("canonical action");
                    let tool = match name {
                        "read" => crate::tool::files::read()?,
                        "write" => crate::tool::files::write()?,
                        _ => panic!("unexpected action"),
                    };
                    count += 1;
                    let invocation = ToolInvocation {
                        operation_id: OperationId::new(),
                        call_id: count.to_string(),
                        name: name.into(),
                        arguments: serde_json::from_str(&call.arguments.value).unwrap(),
                    };
                    if name == "read" && invocation.arguments["path"] == "large.txt" {
                        large_pages += 1;
                    }
                    let context = ToolContext::new(
                        task.clone(),
                        invocation.operation_id,
                        &invocation.call_id,
                    )?;
                    let reply = match tool.executor.execute(invocation, Some(context)).await {
                        Ok(result) => {
                            let version =
                                crate::tool::files::result_version(name, &result.value, None)?;
                            serde_json::json!({"ok":true,"value":result.value,"version":version.map(|v| v.0)})
                        }
                        Err(error) => serde_json::json!({"ok":false,"value":{
                            "class":crate::executor::ToolErrorClass::of(&error),"message":error.to_string(),
                        }}),
                    };
                    let _ = call
                        .reply
                        .send(Some(serde_json::to_vec(&reply).unwrap().into()));
                }
                Ok::<_, crate::Error>((count, large_pages))
            }
        );
        assert_eq!(
            result.unwrap().unwrap(),
            serde_json::json!({
                "lines":["β"],"conflict":"changed","absent":null,
                "stubs":["needs P2","needs P2"],"moduleText":"export default {\"model\":\"example/model\"}",
            })
        );
        let (calls, large_pages) = served?;
        // Ten original operations, two notebook operations and one large write.
        // Only the source pages add calls; lines() has no host round trip.
        assert!(large_pages > 1);
        assert_eq!(calls, 13 + large_pages);
        Ok(())
    }

    #[tokio::test]
    async fn module_default_is_awaited_and_script_completion_is_not_a_result() {
        for (code, expected) in [
            (
                "export default Promise.resolve(42)",
                Ok(serde_json::json!(42)),
            ),
            ("42", Ok(serde_json::Value::Null)),
            (
                "export default Promise.reject(new Error('failed'))",
                Err("Error: failed".into()),
            ),
        ] {
            let (send, _receive) = tokio::sync::mpsc::channel(1);
            let result = acyclic_native_runtime::blocking_future(super::run(
                code.into(),
                acyclic_stream::OwnedBytes::from_static(b"[]"),
                send,
                tokio_util::sync::CancellationToken::new(),
                4096,
                super::super::allocator::EngineAllocator::new(None),
                super::INTERRUPT_BUDGET,
            ))
            .await
            .unwrap();
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn producerless_promise_returns_without_waiting_for_an_external_wake() {
        use futures::FutureExt;

        for code in [
            "await new Promise(() => {})",
            "export default new Promise(() => {})",
        ] {
            let (send, _receive) = tokio::sync::mpsc::channel(1);
            let result = super::run(
                code.into(),
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
    }

    #[tokio::test]
    async fn dropping_the_dispatcher_wakes_the_pending_rpc_with_an_error() {
        for code in [
            "await tools.code({code:'1'})",
            "export default tools.code({code:'1'})",
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

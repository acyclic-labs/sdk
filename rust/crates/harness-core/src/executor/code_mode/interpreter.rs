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

enum Reply {
    Data(Option<acyclic_stream::OwnedBytes>),
    DispatcherClosed,
    ProducerDropped,
}
impl<'js> rquickjs::IntoJs<'js> for Reply {
    fn into_js(self, ctx: &rquickjs::Ctx<'js>) -> rquickjs::Result<rquickjs::Value<'js>> {
        let bytes = match self {
            Self::Data(bytes) => bytes,
            Self::DispatcherClosed => {
                return Err(rquickjs::Exception::throw_message(
                    ctx,
                    "JavaScript dispatcher closed before submission",
                ));
            }
            Self::ProducerDropped => {
                return Err(rquickjs::Exception::throw_message(
                    ctx,
                    "JavaScript reply producer dropped before settlement",
                ));
            }
        };
        let Some(bytes) = bytes else {
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
                            if send.send(request).await.is_err() {
                                return Ok(Reply::DispatcherClosed);
                            }
                            Ok::<_, rquickjs::Error>(match receive.await {
                                Ok(bytes) => Reply::Data(bytes),
                                Err(_) => Reply::ProducerDropped,
                            })
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
        use crate::conversation::{ContentPublisher, VolumeClass, VolumeOwner, VolumeRef};
        use crate::core::{AggregateKind, Authority, AuthorityRoot};
        use crate::filesystem::{
            FilesystemContentPublisher, FilesystemContentVerifier, FilesystemHost,
        };
        use crate::{
            Capabilities, OperationId,
            conversation::{Limits, VolumeOperation},
            runtime::{Bindings, ContentBindings, RuntimeScope, ToolContext},
            tool::ToolInvocation,
        };
        use acyclic_fs::access::{Action, Capability};
        use std::sync::Arc;
        let agent = crate::AgentId::new();
        let provider = crate::resources::ProviderRef::new(
            "a1".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?;
        let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
        let volume = VolumeRef::new(
            provider,
            "files".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        host.create_volume(&volume).await?;
        // Deletion is explicitly admitted. A volume write grant alone retains
        // its original Create/Write ceiling and cannot authorize deletion.
        let filesystem = Capability::whole_volume(
            None,
            &[
                Action::List,
                Action::Read,
                Action::ReadHistory,
                Action::Create,
                Action::Write,
                Action::Delete,
            ],
        );
        let write_only = Capability::whole_volume(None, &[Action::Create, Action::Write]);
        let write_only = filesystem
            .clone()
            .attenuate(write_only.layers.into_iter().flatten().collect(), None)
            .to_scope_label()
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let filesystem = filesystem
            .to_scope_label()
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let grants = Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
            filesystem,
            "tool:call:read".into(),
            "tool:call:write".into(),
            "tool:call:remove".into(),
        ]);
        let issuer = AuthorityRoot::new(
            "a1",
            [43; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "a1".into(),
            },
        );
        let scope = issuer.bootstrap_scope("owner", &grants, Some(agent), &mut None)?;
        let verifier = issuer.verifier(&mut None)?;
        let publisher =
            FilesystemContentPublisher::new(host.clone(), &volume, &verifier, &scope, 65536, None)?;
        let content = ContentBindings {
            volume,
            reader: FilesystemContentVerifier::new(host, &verifier, &scope, 65536, None)?,
            writer: Some(publisher.clone()),
        };
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
            import {read, write, remove, list, spawn, fork, DeniedError, ConflictError} from 'acyclic';
            const first = await write('file.txt', 'one\r\nβ\n', 'absent');
            const file = await read('file.txt');
            if (file.lines(0, 0).length !== 0) throw new Error('empty line selection');
            const lines = file.lines(1, 1);
            if (file.lines(0, 1)[0] !== 'one' || file.lines(2, 1).length !== 0)
                throw new Error('line selection changed the file');
            try { file.lines(-1, 1); throw new Error('invalid line selection succeeded'); }
            catch (error) { if (!(error instanceof RangeError)) throw error; }
            await write('file.txt', 'changed', first);
            let conflict;
            try { await write('file.txt', 'wrong', file.version); }
            catch (error) { if (!(error instanceof ConflictError)) throw error; conflict = error.current.text(); }
            try { await write('file.txt', 'wrong', 'absent'); throw new Error('create replaced a file'); }
            catch (error) { if (!(error instanceof ConflictError)) throw error; }
            const absent = await read('missing.txt');
            await write('denied.txt', 'preserved', 'any');
            try { await remove('denied.txt', 'any'); throw new Error('write grant authorized deletion'); }
            catch (error) { if (!(error instanceof DeniedError)) throw error; }
            if ((await read('denied.txt')).text() !== 'preserved') throw new Error('denied removal changed content');
            const removeVersion = await write('remove.txt', 'old', 'any');
            const retained = await read('remove.txt');
            await write('remove.txt', 'new', 'any');
            try { await remove('remove.txt', removeVersion); throw new Error('stale removal succeeded'); }
            catch (error) {
                if (!(error instanceof ConflictError) || error.current.text() !== 'new') throw error;
            }
            await remove('remove.txt', 'any');
            if (await read('remove.txt') !== null) throw new Error('unconditional removal did not remove current binding');
            await write('remove.txt', 'again'.repeat(400), 'any');
            const latest = await read('remove.txt');
            if (!latest.version.startsWith('authored-path-v1:')) throw new Error('removal control needs original namespace version');
            await remove('remove.txt', latest.version);
            if (await read('remove.txt') !== null || retained.text() !== 'old') throw new Error('versioned removal changed pinned content');
            const contentVersion = await write('content-remove.txt', 'again'.repeat(400), 'any');
            if (contentVersion.startsWith('authored-path-v1:')) throw new Error('content removal control needs original immutable reference');
            await remove('content-remove.txt', contentVersion);
            if (await read('content-remove.txt') !== null) throw new Error('content version removal kept current binding');
            await write('recovery.txt', 'original', 'any');
            await remove('recovery.txt', 'any');
            if ((await read('recovery.txt')).text() !== 'later') throw new Error('removal recovery deleted a later publication');
            try { await remove('.system/private', 'any'); throw new Error('private removal succeeded'); }
            catch (error) { if (!(error instanceof DeniedError)) throw error; }
            try { await remove('remove.txt', 'absent'); throw new Error('invalid condition succeeded'); }
            catch (error) { if (!(error instanceof TypeError)) throw error; }
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
            for (let i = 0; i < 18; i++) await write(`dir/a${String(i).padStart(2, '0')}`, 'old', 'absent');
            const listing = list('dir/');
            if (listing.cursor !== '') throw new Error('unread cursor advanced');
            let firstEntry;
            for await (const entry of listing) { firstEntry = entry; break; }
            if (firstEntry.name !== 'a00' || firstEntry.path !== 'dir/a00' || firstEntry.kind !== 'file'
                || listing.cursor !== firstEntry.version) throw new Error('delivered cursor differs');
            // Resumption retains the original cut despite an intervening publication.
            await write('dir/a00a', 'new', 'absent');
            const rest = [];
            for await (const entry of list('dir', listing.cursor)) rest.push(entry.name);
            if (rest.length !== 17 || rest[0] !== 'a01' || rest[16] !== 'a17' || rest.includes('a00a'))
                throw new Error('directory continuation changed generation');
            try {
                for await (const entry of list('.', listing.cursor)) throw new Error('wrong parent yielded');
                throw new Error('wrong parent succeeded');
            } catch (error) { if (!error.message.includes('cursor belongs')) throw error; }
            let directory;
            for await (const entry of list('.')) if (entry.name === 'dir') directory = entry;
            if (directory?.kind !== 'dir' || directory.path !== 'dir') throw new Error('root directory missing');
            try {
                for await (const entry of list('.system')) throw new Error('private storage yielded');
                throw new Error('private storage succeeded');
            } catch (error) { if (!(error instanceof DeniedError)) throw error; }
            let missingError;
            try {
                for await (const entry of list('missing-directory')) throw new Error('missing directory yielded');
            } catch (error) { missingError = error; }
            if (missingError?.class !== 'not_found') throw new Error('missing directory was not refused');
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
                let mut directory_pages = 0;
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
                        "remove" => crate::tool::files::remove()?,
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
                    if invocation.arguments["selection"]["kind"] == "directory" {
                        directory_pages += 1;
                    }
                    let namespace_removal = name == "remove"
                        && invocation.arguments["ifVersion"]
                            .as_str()
                            .is_some_and(|version| version.starts_with("authored-path-v1:"));
                    let bounded_removal = namespace_removal
                        || (name == "remove"
                            && invocation.arguments["path"] == "content-remove.txt");
                    let acting = if name == "remove" && invocation.arguments["path"] == "denied.txt"
                    {
                        Arc::new(
                            task.scoped(
                                Capabilities::new([
                                    publisher
                                        .volume()
                                        .capability(VolumeOperation::Read, &mut None)?,
                                    publisher
                                        .volume()
                                        .capability(VolumeOperation::Write, &mut None)?,
                                    write_only.clone(),
                                    "tool:call:remove".into(),
                                ]),
                                task.scope().limits(),
                            )?,
                        )
                    } else if bounded_removal {
                        // The original version is admitted, but the 2000-byte body
                        // cannot be read on this narrower operation's allowance.
                        Arc::new(task.scoped(
                            task.scope().grants().clone(),
                            Limits {
                                file_bytes: 1536,
                                ..task.scope().limits()
                            },
                        )?)
                    } else {
                        task.clone()
                    };
                    let context =
                        ToolContext::new(acting, invocation.operation_id, &invocation.call_id)?;
                    if bounded_removal {
                        let refusal = context
                            .read_tool_path(
                                invocation.arguments["path"].as_str().unwrap(),
                                None,
                                None,
                            )
                            .await
                            .unwrap_err();
                        assert!(refusal.to_string().contains("read bound"), "{refusal}");
                    }
                    let recovering =
                        name == "remove" && invocation.arguments["path"] == "recovery.txt";
                    let reply = match tool
                        .executor
                        .execute(invocation.clone(), Some(context))
                        .await
                    {
                        Ok(result) => {
                            if recovering {
                                publisher
                                    .stage(
                                        OperationId::new(),
                                        "recovery.txt",
                                        b"later",
                                        "text/plain",
                                        "recovery.txt",
                                        crate::conversation::ContentAccess::default(),
                                    )?
                                    .await?;
                                let context = ToolContext::new(
                                    task.clone(),
                                    invocation.operation_id,
                                    &invocation.call_id,
                                )?;
                                let before_file =
                                    context.read_tool_path("recovery.txt", None, None).await?.0;
                                let work = acyclic_fs::NativeMaterializationWork::new(
                                    acyclic_fs::performance::work_budget(100_000, 16_000_000),
                                    &acyclic_fs::CancellationToken::new(),
                                )
                                .map_err(|(cause, work)| crate::Error::NativeConstruction {
                                    cause,
                                    work,
                                })?;
                                let before = work.receipt();
                                let (recovered, _custody) = tool
                                    .executor
                                    .reconcile_with_native_context(&context, &invocation, &work)?
                                    .await?;
                                assert_eq!(recovered.unwrap(), result);
                                assert_eq!(
                                    tool.executor
                                        .reconcile(invocation.clone(), Some(context.clone()))
                                        .await?,
                                    Some(result.clone())
                                );
                                let after = work.receipt();
                                assert_eq!(
                                    after.backend_write_operations,
                                    before.backend_write_operations
                                );
                                assert_eq!(
                                    after.authority_records_appended,
                                    before.authority_records_appended
                                );
                                assert_eq!(
                                    after.durability_operations,
                                    before.durability_operations
                                );
                                assert_eq!(
                                    context.read_tool_path("recovery.txt", None, None).await?.0,
                                    before_file
                                );
                            }
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
                Ok::<_, crate::Error>((count, large_pages, directory_pages))
            }
        );
        // A serving failure drops its reply producer. Preserve that original
        // error before inspecting the guest's resulting channel-closure error.
        let (calls, large_pages, directory_pages) = served?;
        assert_eq!(
            result.unwrap().unwrap(),
            serde_json::json!({
                "lines":["β"],"conflict":"changed","absent":null,
                "stubs":["needs P2","needs P2"],"moduleText":"export default {\"model\":\"example/model\"}",
            })
        );
        // Ten original operations, two notebook operations and one large write.
        // Only the source pages add calls; lines() has no host round trip.
        assert!(large_pages > 1);
        // One initial page, two resumed pages, one root page and three refusals.
        // Breaking after the first entry does not prefetch the next page.
        assert_eq!(directory_pages, 7);
        assert_eq!(calls, 13 + 22 + 18 + large_pages + directory_pages);
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
        let tool = crate::tools::catalog()
            .find(|tool| tool.name == "code")
            .unwrap()
            .bind()
            .unwrap();
        let definitions: acyclic_stream::OwnedBytes =
            serde_json::to_vec(&vec![tool.definition]).unwrap().into();
        for code in [
            "await tools.code({code:'1'})",
            "export default tools.code({code:'1'})",
            "await console.log('message')",
        ] {
            for submitted in [false, true] {
                let (send, receive) = tokio::sync::mpsc::channel(1);
                let mut receive = Some(receive);
                if !submitted {
                    drop(receive.take());
                }
                let (result, ()) = tokio::join!(
                    acyclic_native_runtime::blocking_future(super::run(
                        code.into(),
                        definitions.clone(),
                        send,
                        tokio_util::sync::CancellationToken::new(),
                        4096,
                        super::super::allocator::EngineAllocator::new(None),
                        super::INTERRUPT_BUDGET,
                    )),
                    async move {
                        if let Some(mut receive) = receive {
                            let call = receive.recv().await.expect("engine submitted its call");
                            drop(receive);
                            drop(call);
                        }
                    }
                );
                let error = result.unwrap().unwrap_err();
                let expected = if submitted {
                    "JavaScript reply producer dropped before settlement"
                } else {
                    "JavaScript dispatcher closed before submission"
                };
                assert!(error.contains(expected), "{error}");
            }
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

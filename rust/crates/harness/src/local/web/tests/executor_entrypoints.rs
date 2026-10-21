use super::*;
use crate::durable_host::tests::fixture::{Fixture, FixtureMachine, test_registries};
use crate::{
    runtime::{TaskSpawner as _, ToolContext},
    tool::ToolInvocation,
    wire::ErrorCode,
};
use acyclic_fs::NativeMaterializationWork;

fn fixture_error(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

#[tokio::test]
async fn ordinary_and_native_executor_entrypoints_return_refusals_and_never_reconcile_by_redispatch()
-> Result<()> {
    let fixture = Fixture::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        acyclic_fs::Fs::memory(),
        json!({"type":"object"}),
        None,
        &["tool:call:url_fetch"],
    )
    .await?;
    let admission = fixture.admission(82, json!({}));
    let scope = crate::runtime::RuntimeScope::new(admission.grants.clone(), admission.limits)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let metadata =
        NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let metadata_io = crate::filesystem::PreparationIo::from_native_work(&metadata)?;
    let (_, _, tasks, machines) = test_registries(
        Arc::new(FixtureMachine {
            identity: fixture.machine.clone(),
            schema: fixture.schema.clone(),
        }),
        fixture.schema.clone(),
        fixture.schema.clone(),
        Some(&metadata_io),
    )?;
    let signed = fixture.issuer.issue(
        "web-entrypoints",
        scope.grants(),
        Some(crate::AgentId::from_bytes([1; 16])),
        None,
        &mut None,
    )?;
    let runtime = crate::filesystem::FilesystemTaskRuntime::open(
        fixture.stream.clone(),
        fixture.files.clone(),
        &fixture.storage,
        None,
        &fixture.issuer.verifier(&mut None)?,
        &signed,
        &scope,
        &tasks,
        &machines,
        &crate::tool::ToolRegistry::default(),
        crate::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 1,
            model_steps: 1,
        },
        1,
        scope.limits().file_bytes,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        None,
        &[],
        &crate::durable_tool::ResumableToolRegistry::default(),
        Some(&metadata_io),
    )
    .await?;
    let crate::Admission::Accepted(task) = runtime.task_host().admit(admission).await? else {
        return Err(Error::Conflict("fixture admission rejected".into()));
    };
    let crate::distributed::WorkPull::Claimed(lease) = runtime
        .task_host()
        .pull_work(
            &crate::distributed::Worker {
                id: "web-entrypoints".into(),
                available: Default::default(),
                labels: Default::default(),
            },
            None,
            &mut None,
        )
        .await?
    else {
        return Err(Error::NotFound("fixture lease".into()));
    };
    runtime.task_host().start_task(&lease, &mut None).await?;
    let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
    let mut tools = crate::tool::ToolRegistry::default();
    crate::search::web::WebExecutor::bind(&mut tools, None)?;
    let executor = tools
        .get("url_fetch", None, None)?
        .ok_or_else(|| Error::NotFound("fixture fetch executor".into()))?
        .executor
        .clone();
    for native in [false, true] {
        for case in 0..3 {
            let (url, server) = match case {
                0 => ("not-a-url".into(), None),
                1 => {
                    let (url, server) = serve_response(503, "", "private-body".into()).await?;
                    (url, Some(server))
                }
                _ => {
                    let (url, server) = serve_response(
                        302,
                        "Location: http://127.0.0.1:9/private\r\n",
                        String::new(),
                    )
                    .await?;
                    (url, Some(server))
                }
            };
            let invocation = ToolInvocation {
                operation_id: crate::OperationId::new(),
                call_id: "entrypoint-fetch".into(),
                name: "url_fetch".into(),
                arguments: json!({"url":url}),
            };
            let value = if native {
                let cancellation = acyclic_fs::CancellationToken::new();
                let work = NativeMaterializationWork::new(
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                let task_context = runtime
                    .task_context_with_native_work(task, &fence, &work)
                    .await?;
                let io = crate::executor::PreparationIo::from_native_work(&work)?;
                let context = ToolContext::new(
                    task_context,
                    invocation.operation_id,
                    &invocation.call_id,
                    Some(&io),
                )?;
                let (result, held) = executor
                    .execute_with_native_context(&context, &invocation, &work)?
                    .await?;
                assert!(held.work().shares_allocation_context(&work));
                let (reconciled, _) = executor
                    .reconcile_with_native_context(&context, &invocation, &work)?
                    .await?;
                assert!(
                    reconciled.is_none(),
                    "uncertain execution was redispatched during reconciliation"
                );
                let foreign_cancel = acyclic_fs::CancellationToken::new();
                let foreign = NativeMaterializationWork::new(
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &foreign_cancel,
                )
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                assert!(matches!(
                    executor.execute_with_native_context(&context, &invocation, &foreign),
                    Err(Error::NativeBoundary {
                        code: ErrorCode::Unauthorized,
                        ..
                    })
                ));
                result.value
            } else {
                let context = ToolContext::new(
                    Arc::new(runtime.task_context(task, fence.clone()).await?),
                    invocation.operation_id,
                    &invocation.call_id,
                    None,
                )?;
                let result = executor
                    .execute(invocation.clone(), Some(context.clone()))
                    .await?;
                assert!(
                    executor
                        .reconcile(invocation.clone(), Some(context))
                        .await?
                        .is_none()
                );
                result.value
            };
            assert_eq!(value["status"], "rejected");
            assert_eq!(value["code"], "invalid");
            assert_published_web_contract("url_fetch", &value)?;
            if let Some(server) = server {
                server.await.map_err(fixture_error)??;
            }
        }
    }
    Ok(())
}

async fn serve_response(
    status: u16,
    headers: &str,
    content: String,
) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    serve_web_response("GET", status, headers, content).await
}

async fn serve_web_response(
    method: &'static str,
    status: u16,
    headers: &str,
    content: String,
) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|_| Error::Storage("fixture bind failed".into()))?;
    let address = listener
        .local_addr()
        .map_err(|_| Error::Storage("fixture address failed".into()))?;
    let response = format!(
        "HTTP/1.1 {status} Test\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{content}",
        content.len()
    );
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .map_err(|_| Error::Storage("fixture accept failed".into()))?;
        let mut request = Vec::new();
        loop {
            let mut buffer = [0; 1024];
            let count = socket
                .read(&mut buffer)
                .await
                .map_err(|_| Error::Storage("fixture read failed".into()))?;
            assert!(count > 0);
            request.extend_from_slice(&buffer[..count]);
            assert!(request.len() <= 8192);
            if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                break;
            }
        }
        assert!(request.starts_with(format!("{method} /source HTTP/1.1\r\n").as_bytes()));
        socket
            .write_all(response.as_bytes())
            .await
            .map_err(|_| Error::Storage("fixture write failed".into()))?;
        Ok(())
    });
    Ok((format!("http://{address}/source"), server))
}

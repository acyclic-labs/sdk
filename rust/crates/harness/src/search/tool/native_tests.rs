use super::*;
use crate::{
    OperationId,
    durable_host::tests::created_fixture::CreationFixture,
    runtime::TaskContext,
    search::{SearchResult, native::response_bytes},
    test_support::{ScriptedSearch, ScriptedSearchProvider},
};
use acyclic_fs::{CancellationToken, NativeMaterializationWork};

struct NativeOnly(Arc<ScriptedSearchProvider>);

impl SearchProvider for NativeOnly {
    fn search<'a>(&'a self, _: &'a SearchRequest) -> BoxProviderFuture<'a, Result<SearchResponse>> {
        Box::pin(async { Err(Error::Unsupported("ordinary search must not run".into())) })
    }
    fn supports_native_search(&self, work: &NativeMaterializationWork) -> Result<bool> {
        self.0.supports_native_search(work)
    }
    fn search_with_native_work<'a>(
        &'a self,
        request: &'a SearchRequest,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (SearchResponse, acyclic_fs::NativeAllocation)>> {
        self.0.search_with_native_work(request, work)
    }
}

async fn context() -> Result<(CreationFixture, NativeMaterializationWork, Arc<TaskContext>)> {
    let fixture = CreationFixture::new_with_grants(&["tool:call:web_search"]).await?;
    let work = NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            items_examined: 1_048_576,
            allocation_operations: 131_072,
            ..acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024)
        },
        &CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let task = fixture
        .runtime
        .task_context_with_native_work(
            fixture.root,
            &crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation),
            &work,
        )
        .await?;
    Ok((fixture, work, task))
}

fn provider() -> Result<Arc<ScriptedSearchProvider>> {
    Ok(Arc::new(ScriptedSearchProvider::new([ScriptedSearch {
        request: SearchRequest {
            query: "native query".into(),
            max_results: 5,
        },
        response: Some(SearchResponse {
            results: vec![SearchResult {
                url: "https://example.com/native".into(),
                title: "Native".into(),
                snippet: "Funded result".into(),
            }],
        }),
        error: None,
    }])?))
}

fn invocation() -> ToolInvocation {
    ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "native-search".into(),
        name: "web_search".into(),
        arguments: serde_json::json!({"query":"native query"}),
    }
}

#[tokio::test]
async fn search_native_dispatch_projects_on_the_original_ledger_and_never_reissues() -> Result<()> {
    let (_fixture, work, task) = context().await?;
    let backend = provider()?;
    let tool = web_search(Arc::new(NativeOnly(backend.clone())))?;
    let call = invocation();
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    let context = ToolContext::new(task, call.operation_id, &call.call_id, Some(&io))?;
    tool.executor.supports_native_execution(&work)?;
    tool.executor
        .authorize(None, Some(&context), &call, Some(&work))?;
    let baseline = work.live_allocation_bytes();
    let reconciled = tool
        .executor
        .reconcile_with_native_context(&context, &call, &work)?
        .await?;
    assert!(reconciled.0.is_none());
    assert!(reconciled.1.work().shares_ledger(&work));
    drop(reconciled);
    assert_eq!(backend.remaining(), 1);
    let result = tool
        .executor
        .execute_with_native_context(&context, &call, &work)?
        .await?;
    assert_eq!(result.0.value["results"][0]["title"], "Native");
    assert!(result.1.work().shares_ledger(&work));
    assert!(work.live_allocation_bytes() > baseline);
    let projection = tool.projection.project(&call, &result.0, Some(&work))?;
    assert_eq!(projection.0["kind"], "json");
    assert_eq!(projection.0["value"], result.0.value);
    drop(projection);
    drop(result);
    assert_eq!(backend.remaining(), 0);
    assert_eq!(work.live_allocation_bytes(), baseline);
    let reconciled = tool
        .executor
        .reconcile_with_native_context(&context, &call, &work)?
        .await?;
    assert!(reconciled.0.is_none());
    assert_eq!(backend.remaining(), 0);
    Ok(())
}

#[tokio::test]
async fn search_native_rejects_foreign_context_and_exhaustion_before_dispatch() -> Result<()> {
    let (_fixture, work, task) = context().await?;
    let backend = provider()?;
    let tool = web_search(backend.clone())?;
    let call = invocation();
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    let context = ToolContext::new(task, call.operation_id, &call.call_id, Some(&io))?;
    let foreign = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(4096, 1_048_576),
        &CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    assert!(matches!(
        tool.executor
            .execute_with_native_context(&context, &call, &foreign),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    let mut wrong = call.clone();
    wrong.call_id = "retargeted".into();
    assert!(
        tool.executor
            .execute_with_native_context(&context, &wrong, &work)
            .is_err()
    );
    let available = work.budget().peak_allocation_bytes - work.live_allocation_bytes();
    let held = work.claim(available, u64::from(available != 0), Default::default())?;
    let mut storage = Vec::<u8>::new();
    storage
        .try_reserve_exact(available as usize)
        .map_err(Error::Allocation)?;
    assert!(
        tool.executor
            .execute_with_native_context(&context, &call, &work)
            .is_err()
    );
    assert_eq!(backend.remaining(), 1);
    drop(storage);
    drop(held);
    work.cancellation().cancel();
    assert!(matches!(
        tool.executor
            .execute_with_native_context(&context, &call, &work),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert_eq!(backend.remaining(), 1);
    Ok(())
}

#[tokio::test]
async fn search_native_script_results_keep_their_full_original_custody() -> Result<()> {
    let (_fixture, work, _task) = context().await?;
    let provider = provider()?;
    let request = SearchRequest {
        query: "native query".into(),
        max_results: 5,
    };
    let result = provider.search_with_native_work(&request, &work)?.await?;
    assert_eq!(result.1.bytes(), response_bytes(&result.0)?);
    assert!(result.1.work().shares_ledger(&work));
    Ok(())
}

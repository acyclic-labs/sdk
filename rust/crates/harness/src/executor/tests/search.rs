use super::*;
use crate::{
    durable_host::tests::created_fixture::CreationFixture,
    search::{SearchProvider, SearchRequest, SearchResponse, SearchResult, web_search},
    test_support::{ScriptedSearch, ScriptedSearchProvider},
};
use acyclic_fs::{CancellationToken, NativeMaterializationWork};

struct NativeSearch {
    backend: Arc<ScriptedSearchProvider>,
    lose_response: bool,
}

impl SearchProvider for NativeSearch {
    fn search<'a>(&'a self, _: &'a SearchRequest) -> BoxFuture<'a, Result<SearchResponse>> {
        Box::pin(async { Err(Error::Unsupported("ordinary search must not run".into())) })
    }
    fn supports_native_search(&self, work: &NativeMaterializationWork) -> Result<bool> {
        self.backend.supports_native_search(work)
    }
    fn search_with_native_work<'a>(
        &'a self,
        request: &'a SearchRequest,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (SearchResponse, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let result = self.backend.search_with_native_work(request, work)?.await?;
            if self.lose_response {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Storage,
                    reason: "scripted response lost after dispatch",
                });
            }
            Ok(result)
        })
    }
}

#[tokio::test]
async fn native_search_dispatch_scenario_executes_projects_and_replays_once() -> Result<()> {
    for (lose_response, rejected) in [(false, false), (true, false), (false, true)] {
        dispatch_scenario(lose_response, rejected).await?;
    }
    Ok(())
}

async fn dispatch_scenario(lose_response: bool, rejected: bool) -> Result<()> {
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
    let fence = crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation);
    let task = fixture
        .runtime
        .task_context_with_native_work(fixture.root, &fence, &work)
        .await?;
    let (journal, operation) = fixture
        .runtime
        .execution_journal_with_native_work(fixture.root, &fence, OperationId::new(), &work)
        .await?;
    let response = SearchResponse {
        results: vec![SearchResult {
            url: "https://example.com/native".into(),
            title: "Native source".into(),
            snippet: "Original-ledger search".into(),
        }],
    };
    let backend = Arc::new(ScriptedSearchProvider::new([ScriptedSearch {
        request: SearchRequest {
            query: "native query".into(),
            max_results: 5,
        },
        response: (!rejected).then(|| response.clone()),
        error: rejected.then(|| "scripted search unavailable".into()),
    }])?);
    let mut source = ToolRegistry::default();
    source.register(
        (
            web_search(Arc::new(NativeSearch {
                backend: backend.clone(),
                lose_response,
            }))?,
            None,
        ),
        &mut None,
        None,
    )?;
    let (tools, _tools_custody) = source.clone_with_native_work(&work)?;
    let (scope, _scope_custody) = task.scope().clone_with_native_work(&work)?;
    let stock = StockExecutor::new(
        Model::new("fixture".into(), "search".into(), "1".into(), Value::Null)?,
        Arc::new(ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(scope, None)?
    .with_durable_task(fixture.host.clone(), fixture.root, fence);
    let io = PreparationIo::from_native_work(&work)?;
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            Some(&io),
        )?
        .await?;
    let session = StockExecution {
        context: Some((&task, operation)),
        io: Some(&io),
        ..StockExecution::ordinary(&stock)
    };
    let call = ToolInvocation {
        operation_id: operation,
        call_id: "native-search".into(),
        name: "web_search".into(),
        arguments: json!({"query":"native query"}),
    };
    let mut first = Vec::new();
    let dispatched = session
        .settle_tool_call(journal.as_ref(), operation, 0, call.clone(), &mut first)
        .await;
    assert_eq!(backend.remaining(), 0);
    if rejected {
        let reason = ToolFailureKind::ExecutorRejected.message();
        assert!(matches!(dispatched, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid, reason: actual,
        }) if actual == reason));
        assert!(
            matches!(session.settle_tool_call(journal.as_ref(), operation, 0, call, &mut first).await,
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: actual }) if actual == reason)
        );
        assert!(first.is_empty());
        assert_eq!(backend.remaining(), 0);
        let records = journal.replay(operation, 0, 64, Some(&io))?.await?;
        assert_eq!(
            records
                .iter()
                .filter(|r| matches!(r.event, ExecutionEvent::ToolStarted { .. }))
                .count(),
            1
        );
        assert_eq!(
            records
                .iter()
                .filter(|r| matches!(
                    r.event,
                    ExecutionEvent::ToolFailed {
                        reason: ToolFailureKind::ExecutorRejected,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert!(
            !records
                .iter()
                .any(|r| matches!(r.event, ExecutionEvent::ToolCompleted { .. }))
        );
        return Ok(());
    }
    if lose_response {
        assert!(matches!(dispatched, Err(Error::Indeterminate(id)) if id == operation));
        assert!(
            matches!(session.settle_tool_call(journal.as_ref(), operation, 0, call, &mut first).await,
            Err(Error::Indeterminate(id)) if id == operation)
        );
        assert!(first.is_empty());
        assert_eq!(backend.remaining(), 0);
        return Ok(());
    }
    assert!(matches!(dispatched?, ExecutionProgress::Ready(())));
    let expected = serde_json::to_value(response).map_err(|e| Error::Invalid(e.to_string()))?;
    assert!(matches!(&first[0].content,
        ModelContent::Part(ModelContentPart::ToolResult { content: crate::model::ToolResultContent::Json { value }, .. })
            if value == &expected
    ));
    let mut replayed = Vec::new();
    assert!(matches!(
        session
            .settle_tool_call(journal.as_ref(), operation, 0, call, &mut replayed)
            .await?,
        ExecutionProgress::Ready(())
    ));
    assert_eq!(replayed, first);
    assert_eq!(backend.remaining(), 0);
    assert!(work.receipt().backend_read_operations > 0);
    Ok(())
}

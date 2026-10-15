use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_harness::{
    AgentId, Capabilities, Error, OperationId, Result,
    context::ContextPipeline,
    executor::{
        ExecutionEvent, ExecutionJournal, ExecutionRecord, Executor, StockExecutor,
        ToolFailureKind, TurnInput,
    },
    filesystem::MemoryHarnessStorage,
    model::{Model, ModelContent},
    runtime::RuntimeScope,
    search::{
        SearchProvider, SearchRequest, SearchResponse, decode_search_http_response, web_search,
    },
    test_support::ScriptedModelProvider,
    tool::ToolRegistry,
};
use acyclic_stream::BoxProviderFuture;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Response {
    bytes: Vec<u8>,
    exa: bool,
    calls: AtomicUsize,
}

impl SearchProvider for Response {
    fn search<'a>(&'a self, _: &'a SearchRequest) -> BoxProviderFuture<'a, Result<SearchResponse>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let work = NativeMaterializationWork::new(
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &CancellationToken::new(),
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = work.live_allocation_bytes();
            let result = decode_search_http_response(&self.bytes, 5, self.exa, Some(&work));
            match result {
                Ok((response, custody)) => {
                    assert!(custody.as_ref().unwrap().work().shares_ledger(&work));
                    let encoded = serde_json::to_vec(&response).unwrap();
                    let ordinary = serde_json::from_slice::<SearchResponse>(&encoded).unwrap();
                    assert_eq!(ordinary, response);
                    drop((response, custody));
                    assert_eq!(work.live_allocation_bytes(), baseline);
                    Ok(ordinary)
                }
                Err(error) => {
                    assert!(matches!(error, Error::ToolRejected(_)), "{error:?}");
                    assert_eq!(work.live_allocation_bytes(), baseline);
                    Err(error)
                }
            }
        })
    }
}

// Complete responses must settle once. Replay must preserve the terminal
// result and must not decode or contact the provider again.
pub async fn check(bytes: Vec<u8>, exa: bool) {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 1_048_576)
        .await
        .unwrap();
    let backend = Arc::new(Response {
        bytes,
        exa,
        calls: AtomicUsize::new(0),
    });
    let mut tools = ToolRegistry::default();
    tools
        .register(
            (web_search(backend.clone()).unwrap(), None),
            &mut None,
            None,
        )
        .unwrap();
    let turns: Vec<acyclic_harness::test_support::ScriptedTurn> = serde_json::from_value(json!([
        [{"type":"event","kind":"tool_call","call_id":"search","name":"web_search","arguments":{"query":"codec"}},
         {"type":"event","kind":"completed","metadata":null}],
        "done"
    ])).unwrap();
    let provider = Arc::new(ScriptedModelProvider::new(turns));
    let executor = StockExecutor::new(
        Model::new(
            "scripted".into(),
            "codec".into(),
            "1".into(),
            serde_json::Value::Null,
        )
        .unwrap(),
        provider,
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(["tool:call:web_search"]),
            acyclic_harness::conversation::Limits::default(),
        )
        .unwrap(),
        None,
    )
    .unwrap();
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("codec".into()),
        selected_context: None,
        max_steps: 4,
    };
    let journal = storage.journal();
    let first = executor.execute(input.clone(), journal.as_ref()).await;
    let records = records(journal.as_ref(), input.operation_id).await;
    assert_eq!(
        records
            .iter()
            .filter(|r| matches!(r.event, ExecutionEvent::ToolStarted { .. }))
            .count(),
        1
    );
    let failed = records
        .iter()
        .filter(|r| {
            matches!(
                r.event,
                ExecutionEvent::ToolFailed {
                    reason: ToolFailureKind::ExecutorRejected
                        | ToolFailureKind::PublicationRejected,
                    ..
                }
            )
        })
        .count();
    let succeeded = records
        .iter()
        .filter(|r| matches!(r.event, ExecutionEvent::ToolCompleted { .. }))
        .count();
    assert_eq!(failed + succeeded, 1, "{first:?}; {records:?}");
    assert_eq!(first.is_ok(), succeeded == 1);
    let replay = executor.execute(input.clone(), journal.as_ref()).await;
    assert_eq!(replay.is_ok(), first.is_ok());
    assert_eq!(
        replay.as_ref().err().map(Error::code),
        first.as_ref().err().map(Error::code)
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        serde_json::to_value(&records).unwrap(),
        serde_json::to_value(self::records(journal.as_ref(), input.operation_id).await).unwrap()
    );
}

async fn records(journal: &dyn ExecutionJournal, operation: OperationId) -> Vec<ExecutionRecord> {
    let mut records = Vec::new();
    loop {
        let batch = journal
            .replay(operation, records.len() as u64, 64, None)
            .unwrap()
            .await
            .unwrap();
        if batch.is_empty() {
            return records;
        }
        records.extend(batch);
    }
}

pub fn fuzz_response(data: &[u8]) -> Vec<u8> {
    let Some(data) = data.strip_prefix(&[0xff]) else {
        return data.to_vec();
    };
    let depth = u16::from_le_bytes([
        data.first().copied().unwrap_or(0),
        data.get(1).copied().unwrap_or(0),
    ]) as usize;
    let size = u32::from_le_bytes([
        data.get(2).copied().unwrap_or(0),
        data.get(3).copied().unwrap_or(0),
        data.get(4).copied().unwrap_or(0),
        data.get(5).copied().unwrap_or(0),
    ]) as usize
        % 1_048_578;
    response(
        depth,
        size,
        data.get(6).copied().unwrap_or(0),
        &serde_json::Value::String(
            String::from_utf8_lossy(data.get(7..).unwrap_or_default()).into_owned(),
        ),
    )
}

// Iterative construction explores depth independently of allocation/stack
// limits in serde_json::Value. Scalar size includes the HTTP byte boundary.
pub fn response(depth: usize, size: usize, shape: u8, leaf: &serde_json::Value) -> Vec<u8> {
    let leaf = serde_json::to_string(leaf).unwrap();
    let text = serde_json::to_string(&leaf).unwrap();
    let mut body = match shape % 6 {
        0 => r#"{"results":[],"metadata":"#.to_owned(),
        1 | 4 | 5 => {
            let highlights = match shape % 6 {
                1 => format!("[{text}]"),
                4 => format!("[{text},{text}]"),
                _ => "[]".into(),
            };
            format!(
                "{{\"results\":[{{\"url\":\"https://example.com\",\"title\":\"source\",\"snippet\":{text},\"highlights\":{highlights}}}],\"metadata\":"
            )
        }
        2 => r#"{"results":false,"metadata":"#.to_owned(),
        _ => r#"{"results":[{"url":null}],"metadata":"#.to_owned(),
    };
    for level in 0..depth {
        body.push_str(if level % 2 == 0 { "[" } else { "{\"x\":" });
    }
    body.push_str("[\"");
    body.extend(std::iter::repeat_n('x', size));
    body.push_str("\",");
    body.push_str(&leaf);
    body.push(']');
    for level in (0..depth).rev() {
        body.push(if level % 2 == 0 { ']' } else { '}' });
    }
    body.push('}');
    body.into_bytes()
}

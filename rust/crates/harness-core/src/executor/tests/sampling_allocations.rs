use super::*;
use crate::mcp::callbacks::{McpCallbackPolicy, McpCallbackRequest, McpRuntimeCallbacks};

fn catalog(count: usize) -> Result<ToolRegistry> {
    let template = crate::tool::files::read()?;
    let mut tools = ToolRegistry::default();
    for index in 0..count {
        let name = format!("tool_{index:05}");
        for revision in ["1", "2"] {
            let mut tool = template.clone();
            tool.definition.name = name.clone();
            tool.definition.revision = revision.into();
            tools.register((tool, None), &mut None, None)?;
        }
        if index % 2 == 0 {
            tools.remove_from_model(&name)?;
        } else {
            tools.select_model_version(&name, "1")?;
        }
    }
    Ok(tools)
}

fn request(parent: OperationId, id: &str) -> McpCallbackRequest {
    McpCallbackRequest {
        parent,
        policy: McpCallbackPolicy {
            server: "sample".into(),
            revision: "1".into(),
            sampling: true,
            elicitation: false,
            maximum_requests: 4,
        },
        message: json!({"jsonrpc":"2.0","id":id,"method":"sampling/createMessage",
            "params":{"messages":[{"role":"user","content":{"type":"text","text":"sample"}}],"maxTokens":128}}),
    }
}

#[tokio::test]
async fn tool_free_sampling_allocations_are_independent_of_catalog_size() -> Result<()> {
    let mut observations = [Vec::new(), Vec::new()];
    // Both catalogs retain two revisions and alternate hidden/selected names.
    // Exact allocation equality detects copying even one extra schema; 64
    // selected names already make the deliberate full-copy control distinct.
    for count in [10, 128] {
        let model = Arc::new(ScriptedModelProvider::new([
            ScriptedTurn::Text("answer".into()),
            ScriptedTurn::Text("answer".into()),
            ScriptedTurn::Text("answer".into()),
        ]));
        let scope = RuntimeScope::new(Capabilities::new(["model:generate"]), Limits::default())?;
        let context = Arc::new(capture_admitted_context(scope.clone()).await?);
        let parent = OperationId::from_bytes([1; 16]);
        let tool = crate::runtime::ToolContext::new(context, parent, "remote", None)?;
        let catalog = catalog(count)?;
        let callbacks = McpRuntimeCallbacks::new(
            uncompacted_executor(
                Model::new("test".into(), "sampling".into(), "1".into(), Value::Null)?,
                model.clone(),
                ContextPipeline::default(),
                catalog.clone(),
            )
            .with_tool_authority(scope, None)?,
            Arc::new(Journal::default()),
            "test".into(),
            ModelContent::Text("continue".into()),
            None,
            None,
        )?;
        callbacks
            .admit(tool.clone(), &request(parent, "warmup"))
            .await?;
        // Warm the synchronous executor's thread-local parking state outside measurement.
        futures::executor::block_on(async {});
        for (index, id) in ["sample", "broken"].into_iter().enumerate() {
            let request = request(parent, id);
            let mut result = None;
            let allocations = acyclic_test_support::measure(|| {
                result = Some(futures::executor::block_on(async {
                    if index == 1 {
                        // Deliberately restore the former request-assembly bug:
                        // deep-copy the catalog before discarding all tools.
                        std::hint::black_box(catalog.definitions(None)?);
                    }
                    callbacks.admit(tool.clone(), &request).await
                }));
            });
            let (_, reply) = result.expect("sample ran")?;
            assert_eq!(
                reply.pointer("/result/content/text"),
                Some(&json!("answer"))
            );
            observations[index].push((allocations.count_total, allocations.bytes_total));
        }
        assert!(
            model
                .requests()
                .iter()
                .all(|request| request.tools.is_empty())
        );
        assert_eq!(model.remaining(), 0);
    }
    let independent = |measurements: &[(u64, u64)]| measurements[0] == measurements[1];
    assert!(
        independent(&observations[0]),
        "sampling copied catalog-sized data: {observations:?}"
    );
    assert!(
        !independent(&observations[1]),
        "detector missed deliberately copied schemas: {observations:?}"
    );
    eprintln!("tool-free sampling (allocations, bytes), 10 versus 128 tools: {observations:?}");
    Ok(())
}

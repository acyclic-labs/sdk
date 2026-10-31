use super::*;
use crate::{
    model::NativeToolProfile,
    tool::{Tool, ToolExecutor, ToolResult},
    tools::native::NativeDeclarations,
};

async fn call(
    task: &crate::runtime::TaskContext,
    tool: &Tool,
    arguments: Value,
) -> Result<(ToolInvocation, ToolResult)> {
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "native-file".into(),
        name: tool.definition.name.clone(),
        arguments,
    };
    let context = crate::runtime::ToolContext::new(
        Arc::new(task.clone()),
        invocation.operation_id,
        &invocation.call_id,
    )?;
    let result = tool
        .executor
        .execute(invocation.clone(), Some(context))
        .await?;
    Ok((invocation, result))
}

// A negative control for the execution boundary, beside the real filesystem
// publication below. A schema adapter must never replace a refusing producer.
struct Refuses;
impl ToolExecutor for Refuses {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async { Err(Error::Conflict("original producer refused".into())) })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Err(Error::Conflict("original recovery refused".into())) })
    }
}

#[tokio::test]
async fn native_file_variants_use_original_publication_authority_and_recovery() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 4_096).await?;
    let limits = Limits {
        file_bytes: 4_096,
        render_bytes: 4_096,
        ..Limits::default()
    };
    run_admitted_file_case(&storage, limits, |task| async move {
        let originals = crate::tools::defaults()?;
        for profile in [NativeToolProfile::Anthropic, NativeToolProfile::Google] {
            let revisions = profile.tools()?;
            let mut contracts = NativeDeclarations::new(profile,None)?;
            let mut tools = Vec::new();
            for revision in revisions.iter().take(3) {
                let original = originals.get_shared(&revision.capability,None,None)?.expect("stock capability").clone();
                tools.push(contracts.bind_variant(revision,original)?.0);
            }
            let cc = profile == NativeToolProfile::Anthropic;
            let path = if cc {"claude.txt"} else {"gemini.txt"};
            call(&task,&tools[1],json!({"file_path":path,"content":"before before\n"})).await?;
            let read_args = if cc {
                json!({"file_path":path,"offset":1,"limit":1})
            } else {json!({"file_path":path,"start_line":1,"end_line":1})};
            assert_eq!(call(&task,&tools[0],read_args.clone()).await?.1.value["text"],"before before\n");
            let edit_args = if cc {
                json!({"file_path":path,"old_string":"before","new_string":"after","replace_all":true})
            } else {json!({"file_path":path,"instruction":"Replace both occurrences","old_string":"before","new_string":"after","allow_multiple":true})};
            let (edited,result) = call(&task,&tools[2],edit_args).await?;
            assert_eq!(call(&task,&tools[0],read_args.clone()).await?.1.value["text"],"after after\n");
            let ambiguous = if cc {
                json!({"file_path":path,"old_string":"after","new_string":"wrong"})
            } else {json!({"file_path":path,"instruction":"Ambiguous replacement","old_string":"after","new_string":"wrong"})};
            assert!(call(&task,&tools[2],ambiguous).await.is_err());
            assert_eq!(call(&task,&tools[0],read_args.clone()).await?.1.value["text"],"after after\n");
            call(&task,&tools[1],json!({"file_path":path,"content":"later\n"})).await?;
            let context = crate::runtime::ToolContext::new(Arc::new(task.clone()),edited.operation_id,&edited.call_id)?;
            assert_eq!(tools[2].executor.reconcile(edited,Some(context)).await?,Some(result));
            assert_eq!(call(&task,&tools[0],read_args.clone()).await?.1.value["text"],"later\n");

            let original = originals.get("read",None,None)?.expect("stock read");
            let mut refusing = original.clone();
            refusing.executor = Arc::new(Refuses);
            let mut control = NativeDeclarations::new(profile,None)?;
            let control = control.bind_variant(&revisions[0],Arc::new(refusing))?.0;
            assert!(matches!(call(&task,&control,read_args).await,Err(Error::Conflict(reason)) if reason == "original producer refused"));
            assert_eq!(call(&task,&tools[0],json!({"file_path":path})).await?.1.value["text"],"later\n");
        }
        Ok(())
    }).await
}

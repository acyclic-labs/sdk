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
    fn authorize_replay<'a>(
        &'a self,
        _: Option<&'a crate::runtime::ToolContext>,
        invocation: &'a ToolInvocation,
        work: crate::NativeWork<'a>,
    ) -> Result<Option<crate::NativeFutureBundle<'a, ()>>> {
        Ok(Some(crate::NativeFutureBundle::new_optional(
            work,
            || async move {
                assert_eq!(invocation.name, "read");
                assert_eq!(invocation.arguments["path"], "replay-path");
                Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "original replay authority refused",
                })
            },
        )?))
    }
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
    run_admitted_file_case(&storage, limits, move |task| async move {
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
            let error = call(&task,&tools[2],ambiguous).await.unwrap_err();
            assert_eq!(error.code(), crate::wire::ErrorCode::Conflict);
            if !cc {
                assert_eq!(error.to_string(), format!("Failed to edit, Expected 1 occurrence but found 2 for old_string in file: {path}. If you intended to replace multiple occurrences, set 'allow_multiple' to true."));
                let missing = json!({"file_path":path,"instruction":"Missing text","old_string":"absent","new_string":"wrong"});
                let error = call(&task,&tools[2],missing).await.unwrap_err();
                assert_eq!(error.to_string(), format!("Could not find an exact match for 'old_string' in '{path}'. If previous edits modified the file or you are modifying lines outside your recent read window, please use ReadFile to inspect the target lines before retrying with an exact 'old_string'."));
            }
            assert_eq!(call(&task,&tools[0],read_args.clone()).await?.1.value["text"],"after after\n");
            if !cc {
                let before = call(&task,&tools[0],read_args.clone()).await?.1.value["file"].clone();
                let identical = json!({"file_path":path,"old_string":"after","new_string":"after","allow_multiple":true});
                let error = call(&task,&tools[2],identical).await.unwrap_err();
                assert_eq!(error.to_string(), format!("No changes to apply. The old_string and new_string are identical in file: {path}"));
                assert_eq!(call(&task,&tools[0],read_args.clone()).await?.1.value["file"],before);
            }
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
            let stop = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(64 * 1024, 64 * 1024), &stop,
            ).map_err(|(cause, work)| Error::NativeConstruction {cause,work})?;
            let baseline = work.live_allocation_bytes();
            let invocation = ToolInvocation {
                operation_id: OperationId::new(), call_id:"replay-authority".into(),
                name:control.definition.name.clone(), arguments:json!({"file_path":"replay-path"}),
            };
            let authorization = control.executor.authorize_replay(None,&invocation,Some(&work))?
                .expect("retained original replay producer");
            assert!(work.live_allocation_bytes() > baseline);
            assert!(matches!(authorization.await, Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "original replay authority refused",
            })));
            assert_eq!(work.live_allocation_bytes(),baseline);

            assert_eq!(call(&task,&tools[0],json!({"file_path":path})).await?.1.value["text"],"later\n");
            let lines_path = format!("{path}.lines");
            call(&task,&tools[1],json!({"file_path":lines_path,"content":"first\nβeta\nlast"})).await?;
            let lines_args = if cc {
                json!({"file_path":lines_path,"offset":2,"limit":1})
            } else {json!({"file_path":lines_path,"start_line":2,"end_line":2})};
            let (line_call,line_result) = call(&task,&tools[0],lines_args).await?;
            assert_eq!(line_result.value["text"],"βeta\n");
            assert_eq!(line_result.value["page"]["offset"],1);
            assert_eq!(line_result.value["page"]["start"],6);
            assert_eq!(line_result.value["page"]["end"],12);
            let projected = tools[0].projection.project(&line_call,&line_result,None)?.0;
            tools[0].definition.validate_projection(&projected,limits,None)?;
            let image_path = format!("{path}.png");
            task.stage_file(None,OperationId::new(),&image_path,
                include_bytes!("../../../tests/fixtures/notebook-output.png"),"image/png",&image_path).await?;
            let (image_call,image_result) = call(&task,&tools[0],json!({"file_path":image_path})).await?;
            assert_eq!(image_result.value["media"]["kind"],"image");
            let projected = tools[0].projection.project(&image_call,&image_result,None)?.0;
            tools[0].definition.validate_projection(&projected,limits,None)?;
            assert!(matches!(call(&task,&tools[0],json!({"file_path":"absent.pdf","pages":"1-2"})).await,
                Err(Error::Unsupported(_))));
            if cc {
                let revision = revisions.iter().find(|revision| revision.variant == "NotebookEdit").expect("native notebook");
                let original = originals.get_shared("edit",None,None)?.expect("stock edit").clone();
                let notebook = contracts.bind_variant(revision,original)?.0;
                assert_eq!(notebook.definition.capability(), "edit");
                let source = r#"{"nbformat":4,"cells":[{"id":"a","cell_type":"code","source":"old","outputs":[],"execution_count":1}],"metadata":{"keep":true}}"#;
                call(&task,&tools[1],json!({"file_path":"cells.ipynb","content":source})).await?;
                let (inserted,result) = call(&task,&notebook,json!({"notebook_path":"cells.ipynb","cell_id":"a","edit_mode":"insert","cell_type":"markdown","new_source":"inserted"})).await?;
                let read = call(&task,&tools[0],json!({"file_path":"cells.ipynb"})).await?.1;
                let inserted_text = read.value["text"].as_str().expect("cell text");
                assert!(inserted_text.contains("old"));
                assert!(inserted_text.contains("inserted"));
                let inserted_id = inserted.operation_id.to_string();
                assert!(inserted_text.contains(&inserted_id));
                call(&task,&notebook,json!({"notebook_path":"cells.ipynb","cell_id":"a","new_source":"changed","cell_type":"markdown"})).await?;
                let context = crate::runtime::ToolContext::new(Arc::new(task.clone()),inserted.operation_id,&inserted.call_id)?;
                assert_eq!(notebook.executor.reconcile(inserted,Some(context)).await?,Some(result));
                call(&task,&notebook,json!({"notebook_path":"cells.ipynb","cell_id":inserted_id,"edit_mode":"delete","new_source":""})).await?;
                call(&task,&notebook,json!({"notebook_path":"cells.ipynb","edit_mode":"insert","cell_type":"code","new_source":"first"})).await?;
                let read = call(&task,&tools[0],json!({"file_path":"cells.ipynb"})).await?.1;
                let text = read.value["text"].as_str().expect("cell text");
                assert!(text.find("first").unwrap() < text.find("changed").unwrap());
                assert!(!text.contains("inserted"));
                let before = read.value["file"].clone();
                for arguments in [
                    json!({"notebook_path":"cells.ipynb","new_source":"wrong"}),
                    json!({"notebook_path":"cells.ipynb","edit_mode":"insert","new_source":"wrong"}),
                    json!({"notebook_path":"cells.ipynb","cell_id":"absent","new_source":"wrong"}),
                ] {
                    assert!(call(&task,&notebook,arguments).await.is_err());
                }
                assert_eq!(call(&task,&tools[0],json!({"file_path":"cells.ipynb"})).await?.1.value["file"],before);
                let mut refusing = originals.get("edit",None,None)?.expect("stock edit").clone();
                refusing.executor = Arc::new(Refuses);
                let mut contracts = NativeDeclarations::new(profile,None)?;
                let refusing = contracts.bind_variant(revision,Arc::new(refusing))?.0;
                assert!(matches!(call(&task,&refusing,json!({"notebook_path":"cells.ipynb","cell_id":"a","new_source":"wrong"})).await,
                    Err(Error::Conflict(reason)) if reason == "original producer refused"));
            }
        }
        Ok(())
    }).await
}

#[tokio::test]
async fn native_patch_publishes_all_members_once_or_leaves_every_file_unchanged() -> Result<()> {
    let agent = AgentId::new();
    let storage = MemoryHarnessStorage::new(agent, 4096).await?;
    let limits = Limits {
        file_bytes: 4096,
        ..Limits::default()
    };
    // Destination publication alone intentionally grants no deletion. The
    // positive move/delete case uses an explicit signed file-operation grant
    // on the same publisher; no tool name can manufacture that authority.
    let label = acyclic_fs::access::Capability::whole_volume(
        None,
        &[
            acyclic_fs::access::Action::Create,
            acyclic_fs::access::Action::Write,
            acyclic_fs::access::Action::Delete,
        ],
    )
    .to_scope_label()
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let scope = storage.issuer.bootstrap_scope(
        "patch-owner",
        &Capabilities::new([storage.write_capability.clone(), label]),
        Some(agent),
        &mut None,
    )?;
    let verifier = storage.issuer.verifier(&mut None)?;
    let mut authorized = storage.content_bindings();
    authorized.writer = Some(FilesystemContentPublisher::new(
        Arc::clone(&storage.host),
        &storage.volume,
        &verifier,
        &scope,
        4096,
        None,
    )?);
    run_admitted_file_case(&storage, limits, move |task| {
        let authorized = authorized.clone();
        async move {
        let profile = NativeToolProfile::Openai;
        let revision = profile.tools()?.into_iter().find(|tool| tool.capability == "edit").unwrap();
        let original = Arc::new(crate::tools::variants::apply_patch()?);
        let mut contracts = NativeDeclarations::new(profile, None)?;
        let patch = contracts.bind_variant(&revision, original.clone())?.0;
        let read = crate::tool::files::read()?;
        let initial = "*** Begin Patch\n*** Add File: first\n+old\n*** Add File: deleted\n+remove\n*** End Patch\n";
        call(&task, &patch, json!({"input":initial})).await?;
        // A valid early member cannot publish when a later exact edit fails.
        let invalid = "*** Begin Patch\n*** Add File: must-stay-absent\n+partial\n*** Update File: first\n-wrong\n+bad\n*** End Patch\n";
        assert!(call(&task, &patch, json!({"input":invalid})).await.is_err());
        assert!(call(&task, &read, json!({"path":"must-stay-absent"})).await.is_err());
        assert_eq!(call(&task, &read, json!({"path":"first"})).await?.1.value["text"], "old\n");

        let complete = "*** Begin Patch\n*** Update File: first\n*** Move to: moved\n-old\n+new\n*** Delete File: deleted\n*** Add File: added\n+created\n*** End Patch\n";
        assert!(matches!(call(&task, &patch, json!({"input":complete})).await,
            Err(Error::Unauthorized(_))));
        assert_eq!(call(&task, &read, json!({"path":"first"})).await?.1.value["text"], "old\n");
        assert_eq!(call(&task, &read, json!({"path":"deleted"})).await?.1.value["text"], "remove\n");
        assert!(call(&task, &read, json!({"path":"moved"})).await.is_err());
        assert!(call(&task, &read, json!({"path":"added"})).await.is_err());
        let mut bindings = crate::runtime::Bindings::local();
        bindings.scope = RuntimeScope::new(task.scope().grants().clone(), limits)?;
        bindings.content = Some(authorized.clone());
        let task = bindings.build()?.agent_context(
            &Capabilities::new(Vec::<String>::new()), authorized)?;
        let (invocation, result) = call(&task, &patch, json!({"input":complete})).await?;
        assert!(call(&task, &read, json!({"path":"first"})).await.is_err());
        assert!(call(&task, &read, json!({"path":"deleted"})).await.is_err());
        assert_eq!(call(&task, &read, json!({"path":"moved"})).await?.1.value["text"], "new\n");
        assert_eq!(call(&task, &read, json!({"path":"added"})).await?.1.value["text"], "created\n");
        let files = result.value["files"].as_array().unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0]["version"], files[1]["version"]);
        let projected = patch.projection.project(&invocation, &result, None)?.0;
        assert_eq!(projected, json!({"kind":"parts","parts":[{"kind":"text","text":"Success. Updated the following files:\nA added\nM moved\nD deleted\n"}]}));
        let write = crate::tool::files::write()?;
        call(&task, &write, json!({"path":"moved","content":"later\n"})).await?;
        let context = crate::runtime::ToolContext::new(Arc::new(task.clone()), invocation.operation_id, &invocation.call_id)?;
        assert_eq!(patch.executor.reconcile(invocation, Some(context)).await?, Some(result));
        assert_eq!(call(&task, &read, json!({"path":"moved"})).await?.1.value["text"], "later\n");

        // Refusing the retained original producer must refuse the native call;
        // there is no schema-specific publisher to fall back to.
        let mut refusing = (*original).clone();
        refusing.executor = Arc::new(Refuses);
        let mut contracts = NativeDeclarations::new(profile, None)?;
        let refusing = contracts.bind_variant(&revision, Arc::new(refusing))?.0;
        assert!(matches!(call(&task, &refusing, json!({"input":"*** Begin Patch\n*** Add File: forbidden\n+wrong\n*** End Patch\n"})).await,
            Err(Error::Conflict(reason)) if reason == "original producer refused"));
        assert!(call(&task, &read, json!({"path":"forbidden"})).await.is_err());
        Ok(())
        }
    }).await
}

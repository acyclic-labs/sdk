//! The production reader and volume receipts qualify immutable ordinal seeks.
use super::*;
use crate::runtime::{Bindings, TaskContext, ToolContext};

#[test]
fn notebook_cells_use_bounded_original_ranges_and_reject_invalid_sources() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let storage = runtime.block_on(MemoryHarnessStorage::new(AgentId::new(), 131_072))?;
    let selected = json!({"id":"selected", "cell_type":"code", "source":["λ\n"],
        "outputs":[{"output_type":"stream","name":"stdout","text":["kept\n"]}],
        "execution_count":4,"metadata":{"retain":true}});
    let notebook = json!({"cells":[
        {"id":"large","cell_type":"code","source":[],"outputs":[{"data":{"text/plain":"αβ".repeat(4000)}}]},
        selected,
        {"id":"last","cell_type":"markdown","source":["after"]}
    ], "metadata":{"nested":{"cells":[]}}, "nbformat":4, "nbformat_minor":5});
    let source = serde_json::to_string_pretty(&notebook)
        .unwrap()
        .replace("\"cells\"", "\"ce\\u006cls\"");
    let file = runtime.block_on(storage.stage(
        OperationId::new(),
        "book.ipynb",
        source.as_bytes(),
        "application/x-ipynb+json",
        "book.ipynb",
    ))?;
    let limits = Limits {
        file_bytes: 4096,
        render_bytes: 4096,
        ..Limits::default()
    };
    let task = Bindings {
        scope: RuntimeScope::new(
            Capabilities::new([storage.read_capability.clone(), "tool:call:read".into()]),
            limits,
        )?,
        content: Some(storage.content_bindings()),
        ..Bindings::local()
    }
    .build()?;
    let tool = crate::tool::files::read()?;
    let mut invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "selected-cell".into(),
        name: tool.definition.name.clone(),
        arguments: json!({"path":"book.ipynb",
            "selection":{"kind":"cells","offset":1,"limit":1}}),
    };
    let context = ToolContext::new(
        Arc::new(task.clone()),
        invocation.operation_id,
        &invocation.call_id,
    )?;
    let (result, trace) =
        acyclic_test_support::differential::observe::<_, _, (&'static str, u64, u64)>(
            || context.clone(),
            |context| runtime.block_on(tool.executor.execute(invocation.clone(), Some(context))),
        );
    let result = result?;
    assert_eq!(
        serde_json::from_str::<Value>(result.value["text"].as_str().unwrap()).unwrap(),
        selected
    );
    let start = result.value["page"]["start"].as_u64().unwrap() as usize;
    let end = result.value["page"]["end"].as_u64().unwrap() as usize;
    assert_eq!(&source[start..end], result.value["text"]);
    assert!(
        trace
            .iter()
            .any(|(kind, offset, length)| *kind == "generation-bound range"
                && *offset > 4096
                && *length != 0)
    );
    assert!(
        trace
            .iter()
            .all(|(kind, _, length)| *kind != "generation-bound range"
                || *length <= limits.file_bytes)
    );
    let projection = tool.projection.project(&invocation, &result, None)?.0;
    tool.definition
        .validate_projection(&projection, limits, None)?;
    assert!(serde_json::to_vec(&projection).unwrap().len() as u64 <= limits.render_bytes);

    // The original complete descriptor selects the same cell even when its
    // source body exceeds file credit. The ordinary complete read still fails.
    invocation.arguments["version"] = json!(serde_json::to_string(&file).unwrap());
    let pinned = runtime.block_on(
        tool.executor
            .execute(invocation.clone(), Some(context.clone())),
    )?;
    assert_eq!(pinned.value["text"], result.value["text"]);
    assert_eq!(pinned.value["page"], result.value["page"]);
    assert_eq!(
        runtime
            .block_on(task.read_file(&file, None))
            .unwrap_err()
            .code(),
        crate::wire::ErrorCode::Invalid
    );
    invocation.arguments["selection"]["offset"] = json!(99);
    assert!(
        runtime
            .block_on(
                tool.executor
                    .execute(invocation.clone(), Some(context.clone()))
            )
            .is_err()
    );

    #[cfg(not(target_arch = "wasm32"))]
    {
        invocation.arguments["selection"]["offset"] = json!(1);
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(FINITE_CONTENT_BUDGET, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let custody;
        let (native, retained) = runtime.block_on(tool.executor.execute_with_native_context(
            &context,
            &invocation,
            &work,
        )?)?;
        custody = retained;
        assert_eq!(native.value["text"], result.value["text"]);
        drop(native);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), baseline);
        cancellation.cancel();
        let cancelled =
            match tool
                .executor
                .execute_with_native_context(&context, &invocation, &work)
            {
                Ok(future) => runtime.block_on(future).map(|_| ()),
                Err(error) => Err(error),
            };
        assert!(cancelled.is_err());
    }
    let workspace = super::super::super::bounded_io::content_workspace(
        &storage.host.provider,
        &storage.volume,
        None,
    )?;
    let mut wrong_format = notebook;
    wrong_format["nbformat"] = json!(3);
    let wrong_format = serde_json::to_vec(&wrong_format).unwrap();
    for (key, bytes) in [
        ("wrong-notebook-format", wrong_format),
        (
            "incomplete-notebook",
            source.as_bytes()[..source.len() - 1].to_vec(),
        ),
    ] {
        runtime.block_on(storage.host.apply(
            &workspace,
            None,
            &[crate::filesystem::WorkspaceMutation::PutFile {
                path: "/book.ipynb".into(),
                bytes: bytes::Bytes::from(bytes).into(),
            }],
            &crate::IdempotencyKey::new(key)?,
        ))?;
        invocation
            .arguments
            .as_object_mut()
            .unwrap()
            .remove("version");
        invocation.arguments["selection"]["offset"] = json!(1);
        assert!(
            runtime
                .block_on(
                    tool.executor
                        .execute(invocation.clone(), Some(context.clone()))
                )
                .is_err()
        );
    }
    Ok(())
}

async fn read(task: &TaskContext, version: Option<&str>) -> Result<(Value, String)> {
    let tool = crate::tool::files::read()?;
    let mut arguments =
        json!({"path":"lines.txt","selection":{"kind":"lines","offset":1025,"limit":2}});
    if let Some(version) = version {
        arguments["version"] = json!(version);
    }
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "indexed-read".into(),
        name: tool.definition.name.clone(),
        arguments,
    };
    let context = ToolContext::new(
        Arc::new(task.clone()),
        invocation.operation_id,
        &invocation.call_id,
    )?;
    let result = tool
        .executor
        .execute(invocation.clone(), Some(context))
        .await?;
    let projection = tool.projection.project(&invocation, &result, None)?.0;
    tool.definition
        .validate_projection(&projection, task.scope().limits(), None)?;
    Ok((
        result.value,
        projection["value"]["version"].as_str().unwrap().to_owned(),
    ))
}

#[test]
fn cached_line_reads_keep_original_receipts_outside_the_source_volume() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let source = runtime.block_on(MemoryHarnessStorage::new(AgentId::new(), 16_384))?;
    let private = runtime.block_on(MemoryHarnessStorage::new(AgentId::new(), 4096))?;
    let workspace = super::super::super::bounded_io::content_workspace(
        &source.host.provider,
        &source.volume,
        None,
    )?;
    let file = runtime.block_on(source.stage(
        OperationId::new(),
        "lines.txt",
        ("αβ\n".repeat(2000) + "tail").as_bytes(),
        "text/plain",
        "lines.txt",
    ))?;
    let limits = Limits {
        file_bytes: 4096,
        render_bytes: 4096,
        ..Limits::default()
    };
    let source_grants = [source.read_capability.clone(), "tool:call:read".into()];
    let task = Bindings {
        scope: RuntimeScope::new(
            Capabilities::new(source_grants.iter().cloned().chain([
                private.read_capability.clone(),
                private.write_capability.clone(),
            ])),
            limits,
        )?,
        content: Some(source.content_bindings()),
        artifacts: Some(private.content_bindings()),
        ..Bindings::local()
    }
    .build()?
    .agent_context(
        &Capabilities::new(Vec::<String>::new()),
        private.content_bindings(),
    )?;
    let (expected, version) = runtime.block_on(read(&task, None))?;
    assert_eq!(expected["text"], "αβ\nαβ\n");
    assert_eq!(expected["page"]["start"], 5125);
    // The cache has committed four complete pages. Its independent commits
    // leave the original source head, and therefore its edit version, intact.
    let context = ToolContext::new(Arc::new(task.clone()), OperationId::new(), "source-head")?;
    let (selected, _, _) = runtime.block_on(context.read_tool_path_range(
        "lines.txt",
        None,
        acyclic_fs::ByteRange {
            offset: 0,
            length: 0,
        },
        None,
    ))?;
    assert_eq!(
        serde_json::to_value(selected).unwrap(),
        expected["file"]["authored"]
    );
    let pages = runtime.block_on(task.list_private_directory(
        &private.volume,
        "",
        "projections/v1/lines",
        None,
        None,
        8,
    ))?;
    assert_eq!(pages.entries.len(), 4);
    assert!(!pages.has_more);

    let (warm, warm_ranges) =
        acyclic_test_support::differential::observe::<_, _, (&'static str, u64, u64)>(
            || task.clone(),
            |task| runtime.block_on(read(&task, Some(&version))),
        );
    assert_eq!(warm?.0, expected);
    let warm_ranges: Vec<_> = warm_ranges
        .into_iter()
        .filter(|(kind, _, length)| *kind == "generation-bound range" && *length != 0)
        .collect();
    assert_eq!(warm_ranges, [("generation-bound range", 5115, 4096)]);
    // Removing cache publication authority is a negative control: the same
    // source must still succeed, but the original prefix ranges become visible.
    let cold = task.scoped(Capabilities::new(source_grants), limits)?;
    let (uncached, cold_ranges) =
        acyclic_test_support::differential::observe::<_, _, (&'static str, u64, u64)>(
            || cold,
            |task| runtime.block_on(read(&task, Some(&version))),
        );
    assert_eq!(uncached?.0, expected);
    let cold_ranges: Vec<_> = cold_ranges
        .into_iter()
        .filter(|(kind, _, length)| *kind == "generation-bound range" && *length != 0)
        .collect();
    assert_eq!(
        cold_ranges,
        [
            ("generation-bound range", 0, 4096),
            ("generation-bound range", 4096, 4096),
        ]
    );

    // A complete descriptor and an authored token share the same bounded
    // selection. The full-body read remains an independent negative control.
    let content_version = serde_json::to_string(&file).unwrap();
    let complete = runtime.block_on(read(&task, Some(&content_version)))?.0;
    assert_eq!(complete["text"], expected["text"]);
    assert_eq!(complete["page"], expected["page"]);
    assert_eq!(
        runtime
            .block_on(task.read_file(&file, None))
            .unwrap_err()
            .code(),
        crate::wire::ErrorCode::Invalid
    );
    let interval = acyclic_fs::ByteRange {
        offset: 5125,
        length: 10,
    };
    assert_eq!(
        runtime
            .block_on(context.read_file_range(&file, interval, None))?
            .as_ref(),
        "αβ\nαβ\n".as_bytes()
    );
    for range in [
        acyclic_fs::ByteRange {
            offset: 0,
            length: 4097,
        },
        acyclic_fs::ByteRange {
            offset: u64::MAX,
            length: 1,
        },
    ] {
        assert_eq!(
            runtime
                .block_on(context.read_file_range(&file, range, None))
                .unwrap_err()
                .code(),
            crate::wire::ErrorCode::Invalid
        );
    }
    let substituted = crate::conversation::FileRef::new(
        file.volume().clone(),
        file.path().to_owned(),
        file.version().to_owned(),
        crate::conversation::FileDescriptor::new(
            [9; 32],
            file.descriptor().byte_length(),
            "text/plain".into(),
        )?,
        file.display_name().to_owned(),
        &mut None,
    )?;
    assert_eq!(
        runtime
            .block_on(context.read_file_range(&substituted, interval, None))
            .unwrap_err()
            .code(),
        crate::wire::ErrorCode::Conflict
    );

    #[cfg(not(target_arch = "wasm32"))]
    {
        let tool = crate::tool::files::read()?;
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "native-descriptor-window".into(),
            name: tool.definition.name.clone(),
            arguments: json!({"path":"lines.txt","version":content_version,
                "range":{"start":0,"end":file.descriptor().byte_length()}}),
        };
        let context = ToolContext::new(
            Arc::new(task.clone()),
            invocation.operation_id,
            &invocation.call_id,
        )?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(FINITE_CONTENT_BUDGET, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let _custody;
        let (result, custody) = runtime.block_on(tool.executor.execute_with_native_context(
            &context,
            &invocation,
            &work,
        )?)?;
        _custody = custody;
        assert!(
            result.value["page"]["next"]
                .as_u64()
                .is_some_and(|next| next > 0 && next < file.descriptor().byte_length())
        );
        let projection = tool.projection.project(&invocation, &result, Some(&work))?;
        tool.definition
            .validate_projection(&projection.0, limits, None)?;
        cancellation.cancel();
        let cancelled =
            match tool
                .executor
                .execute_with_native_context(&context, &invocation, &work)
            {
                Ok(future) => runtime.block_on(future).map(|_| ()),
                Err(error) => Err(error),
            };
        assert!(cancelled.is_err());
    }

    // Replacing the files' mutable heads cannot poison the original immutable
    // receipts used by a later seek.
    let private_workspace = super::super::super::bounded_io::content_workspace(
        &private.host.provider,
        &private.volume,
        None,
    )?;
    let mutations: Vec<_> = pages
        .entries
        .into_iter()
        .map(|entry| crate::filesystem::WorkspaceMutation::PutFile {
            path: format!("/projections/v1/lines/{}", entry.name),
            bytes: bytes::Bytes::from_static(b"not an index").into(),
        })
        .collect();
    runtime.block_on(private.host.apply(
        &private_workspace,
        None,
        &mutations,
        &crate::IdempotencyKey::new("changed-index-heads")?,
    ))?;
    assert_eq!(runtime.block_on(read(&task, Some(&version)))?.0, expected);
    runtime.block_on(source.host.apply(
        &workspace,
        None,
        &[crate::filesystem::WorkspaceMutation::PutFile {
            path: "/lines.txt".into(),
            bytes: bytes::Bytes::from("z\n".repeat(2000) + "tail").into(),
        }],
        &crate::IdempotencyKey::new("changed-index-source")?,
    ))?;
    let (changed, changed_version) = runtime.block_on(read(&task, None))?;
    assert_eq!(changed["text"], "z\nz\n");
    assert_eq!(changed["page"]["start"], 2050);
    assert_ne!(changed_version, version);
    Ok(())
}

//! The production reader and volume receipts qualify immutable ordinal seeks.
use super::*;
use crate::runtime::{Bindings, TaskContext, ToolContext};

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
    let source = runtime.block_on(MemoryHarnessStorage::new(AgentId::new(), 4096))?;
    let private = runtime.block_on(MemoryHarnessStorage::new(AgentId::new(), 4096))?;
    let workspace = super::super::super::bounded_io::content_workspace(
        &source.host.provider,
        &source.volume,
        None,
    )?;
    runtime.block_on(source.host.apply(
        &workspace,
        None,
        &[crate::filesystem::WorkspaceMutation::PutFile {
            path: "/lines.txt".into(),
            bytes: bytes::Bytes::from("αβ\n".repeat(2000) + "tail").into(),
        }],
        &crate::IdempotencyKey::new("indexed-source")?,
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

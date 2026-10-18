use super::*;

async fn read_lines(
    task: &runtime::TaskContext,
    arguments: Value,
) -> Result<(ToolResult, Option<ToolResult>)> {
    let tool = crate::tool::files::read()?;
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "lines".into(),
        name: "read".into(),
        arguments,
    };
    let context = runtime::ToolContext::new(
        Arc::new(task.clone()),
        invocation.operation_id,
        &invocation.call_id,
        None,
    )?;
    let result = tool
        .executor
        .execute(invocation.clone(), Some(context.clone()))
        .await?;
    let replayed = tool.executor.reconcile(invocation, Some(context)).await?;
    Ok((result, replayed))
}

#[tokio::test]
async fn default_read_selects_exact_line_windows_by_path_and_replays_them() -> Result<()> {
    let limits = Limits {
        file_bytes: 65536,
        render_bytes: 4096,
        ..Limits::default()
    };
    let storage = MemoryHarnessStorage::new(AgentId::new(), limits.file_bytes).await?;
    storage
        .stage(
            OperationId::new(),
            "lines.txt",
            "a\r\n€\nz".as_bytes(),
            "text/plain",
            "lines",
        )
        .await?;
    storage
        .stage(
            OperationId::new(),
            "binary.bin",
            &[0xff, 0xfe, b'\n'],
            "application/octet-stream",
            "binary",
        )
        .await?;
    run_admitted_file_case(&storage, limits, move |task| async move {
        for (window, expected) in [
            (json!({}), Some((1, "a\r\n€\nz"))),
            (json!({"offset":2,"limit":1}), Some((2, "€\n"))),
            (json!({"offset":1,"limit":2}), Some((1, "a\r\n€\n"))),
            (json!({"offset":3}), Some((3, "z"))),
            (json!({"offset":3,"limit":9}), Some((3, "z"))),
            // Negative controls: past the end, zero-based and empty windows.
            (json!({"offset":4}), None),
            (json!({"offset":0}), None),
            (json!({"limit":0}), None),
        ] {
            let mut arguments = window.clone();
            arguments["path"] = json!("lines.txt");
            let result = read_lines(&task, arguments).await;
            match expected {
                Some((offset, text)) => {
                    let (result, replayed) = result?;
                    assert_eq!(result.value["offset"], json!(offset), "{window}");
                    assert_eq!(result.value["text"], json!(text), "{window}");
                    assert_eq!(result.value["file"]["path"], json!("lines.txt"));
                    assert_eq!(replayed, Some(result));
                }
                None => assert!(
                    matches!(result, Err(Error::Invalid(_))),
                    "{window}: {result:?}"
                ),
            }
        }
        for path in ["binary.bin", "missing.txt"] {
            assert!(read_lines(&task, json!({"path":path})).await.is_err());
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn default_read_bounds_the_numbered_rendering_not_the_raw_bytes() -> Result<()> {
    let line_two = "     2\t€".len() as u64;
    let storage = MemoryHarnessStorage::new(AgentId::new(), 65536).await?;
    storage
        .stage(
            OperationId::new(),
            "lines.txt",
            "a\n€\nz\n".as_bytes(),
            "text/plain",
            "lines",
        )
        .await?;
    for (render_bytes, admitted) in [(line_two, true), (line_two - 1, false)] {
        let limits = Limits {
            file_bytes: 65536,
            render_bytes,
            ..Limits::default()
        };
        run_admitted_file_case(&storage, limits, move |task| async move {
            let window = read_lines(&task, json!({"path":"lines.txt","offset":2,"limit":1})).await;
            assert_eq!(window.is_ok(), admitted, "{window:?}");
            // The raw selection "€\n" fits either bound; the number column does not.
            assert!(matches!(
                read_lines(&task, json!({"path":"lines.txt"})).await,
                Err(Error::Invalid(_))
            ));
            Ok(())
        })
        .await?;
    }
    Ok(())
}

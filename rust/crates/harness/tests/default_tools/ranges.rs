use super::*;

#[tokio::test]
async fn default_read_ranges_are_exact_bounded_utf8_and_replayable() -> Result<()> {
    let limits = Limits {
        file_bytes: 65536,
        render_bytes: 4,
        ..Limits::default()
    };
    let storage = MemoryHarnessStorage::new(AgentId::new(), limits.file_bytes).await?;
    let file = storage
        .stage(
            OperationId::new(),
            "unicode.txt",
            "a€z".as_bytes(),
            "text/plain",
            "unicode",
        )
        .await?;
    run_admitted_file_case(&storage, limits, move |task| {
        let file = file.clone();
        async move {
            let tool = acyclic_harness::tool::files::read()?;
            for (range, expected) in [
                (json!({"start":1,"end":4}), Some("€")),
                (json!({"start":0,"end":1}), Some("a")),
                (json!({"start":4,"end":5}), Some("z")),
                (json!({"start":0,"end":0}), Some("")),
                (json!({"start":1,"end":1}), Some("")),
                (json!({"start":2,"end":2}), None),
                (json!({"start":0,"end":2}), None),
                (json!({"start":2,"end":4}), None),
                (json!({"start":4,"end":1}), None),
                (json!({"start":0,"end":6}), None),
                (json!({"start":0,"end":5}), None),
                (Value::Null, None),
            ] {
                let invocation = ToolInvocation {
                    operation_id: OperationId::new(),
                    call_id: "range".into(),
                    name: "read".into(),
                    arguments: json!({"file":file,"range":range}),
                };
                let context = acyclic_harness::runtime::ToolContext::new(
                    Arc::new(task.clone()),
                    invocation.operation_id,
                    &invocation.call_id,
                    None,
                )?;
                let result = tool
                    .executor
                    .execute(invocation.clone(), Some(context.clone()))
                    .await;
                match expected {
                    Some(expected) => {
                        let result = result?;
                        assert_eq!(result.value, json!(expected));
                        assert_eq!(
                            tool.executor.reconcile(invocation, Some(context)).await?,
                            Some(result)
                        );
                    }
                    None => assert!(
                        matches!(result, Err(Error::Invalid(_))),
                        "{range}: {result:?}"
                    ),
                }
            }
            Ok(())
        }
    })
    .await
}

#[tokio::test]
async fn ranged_io_preserves_crlf_and_rejects_non_boundary_intervals() -> Result<()> {
    let limits = Limits::default();
    let storage = MemoryHarnessStorage::new(AgentId::new(), limits.file_bytes).await?;
    let file = storage
        .stage(
            OperationId::new(),
            "crlf.txt",
            "a\r\n🦀\r\nb".as_bytes(),
            "text/plain",
            "crlf",
        )
        .await?;
    run_admitted_file_case(&storage, limits, move |task| {
        let file = file.clone();
        async move {
            let tool = acyclic_harness::tool::files::read()?;
            for (start, end, expected) in [
                (3, 9, Some("🦀\r\n")),
                (4, 9, None),
                (3, 6, None),
                (9, 8, None),
                (0, 11, None),
                (3, 3, Some("")),
            ] {
                let invocation = ToolInvocation {
                    operation_id: OperationId::new(),
                    call_id: "crlf".into(),
                    name: "read".into(),
                    arguments: json!({"file":file,"range":{"start":start,"end":end}}),
                };
                let context = runtime::ToolContext::new(
                    Arc::new(task.clone()),
                    invocation.operation_id,
                    &invocation.call_id,
                    None,
                )?;
                let result = tool.executor.execute(invocation, Some(context)).await;
                if let Some(expected) = expected {
                    assert_eq!(result?.value, json!(expected));
                } else {
                    assert!(matches!(result, Err(Error::Invalid(_))));
                }
            }
            Ok(())
        }
    })
    .await
}

use super::*;

#[tokio::test]
async fn process_result_does_not_repeat_stream_bytes() -> Result<()> {
    let (harness, context, _directory, _state) = fixture(None).await?;
    let output = Output::open(&context, Arc::clone(harness.task_files()), b"bash".to_vec()).await?;
    let mut output = tokio::task::spawn_blocking(move || {
        output_chunks(output, &vec![b'o'; 2048], &vec![b'e'; 1024])
    })
    .await
    .map_err(storage)?
    .map_err(storage)?;
    let result = ToolResult {
        value: serde_json::json!({
            "success": true, "exit_code": 0, "stdout": "o".repeat(2048), "stderr": "e".repeat(1024), "stop": null,
        }),
    };
    output.finish(&result).await?;
    let files = snapshot(&harness, &context).await?;
    let terminal = files
        .files
        .iter()
        .find(|file| {
            matches!(
                &file.key,
                FileKey::Task {
                    file: TaskFile::Tool {
                        file: ToolFile::Result,
                        ..
                    },
                    ..
                }
            )
        })
        .ok_or_else(|| Error::NotFound("terminal result".into()))?;
    let value: serde_json::Value = serde_json::from_slice(&terminal.bytes).map_err(storage)?;
    let stream_bytes = stream(&files, false).len() + stream(&files, true).len();
    eprintln!(
        "OUTPUT_STORAGE stream_bytes={stream_bytes} result_bytes={} total_bytes={}",
        terminal.bytes.len(),
        stream_bytes + terminal.bytes.len()
    );
    assert_eq!(stream_bytes, 3072);
    assert!(
        value.get("stdout").is_none() && value.get("stderr").is_none(),
        "durable terminal metadata must not repeat captured stream bytes"
    );
    assert_eq!(
        value,
        serde_json::json!({"success": true, "exit_code": 0, "stop": null})
    );
    assert_eq!(
        result.value["stdout"],
        "o".repeat(2048),
        "the caller retains the complete result"
    );
    harness.disconnect_unfinished().await?;
    Ok(())
}

#[tokio::test]
async fn summaries_read_streams_at_the_selected_cut_and_join_split_utf8() -> Result<()> {
    let (harness, context, _directory, _state) = fixture(None).await?;
    let task = context
        .task()
        .durable_task_id()
        .ok_or_else(|| Error::Invalid("fixture task is not durable".into()))?;
    let query = Query::Turns(Component::new(task.to_string())?);
    let from = harness
        .task_files()
        .inner
        .snapshot(&query, 512)
        .await?
        .cursor;
    let mut output =
        Output::open(&context, Arc::clone(harness.task_files()), b"bash".to_vec()).await?;
    // Persist the first two bytes independently, matching a pipe/frame split
    // inside a four-byte code point. Decoding each file would corrupt it.
    output = tokio::task::spawn_blocking(move || {
        output.observe(ProcessOutput::Stdout(&[0xf0, 0x9f]))?;
        Ok::<_, std::io::Error>(output)
    })
    .await
    .map_err(storage)?
    .map_err(storage)?;
    output.writer.lock().await.flush(false, false).await?;
    output = tokio::task::spawn_blocking(move || {
        output.observe(ProcessOutput::Stderr(b"selected error"))?;
        output.observe(ProcessOutput::Stdout(&[0x98, 0x80]))?;
        Ok::<_, std::io::Error>(output)
    })
    .await
    .map_err(storage)?
    .map_err(storage)?;
    output.writer.lock().await.flush(false, false).await?;
    output.writer.lock().await.flush(true, false).await?;
    let through = harness
        .task_files()
        .inner
        .snapshot(&query, 512)
        .await?
        .cursor;
    output = tokio::task::spawn_blocking(move || {
        output.observe(ProcessOutput::Stdout(b"UNSELECTED_LATER_OUTPUT"))?;
        Ok::<_, std::io::Error>(output)
    })
    .await
    .map_err(storage)?
    .map_err(storage)?;
    output.finish(&ToolResult {
        value: serde_json::json!({"success":true, "stdout":"😀UNSELECTED_LATER_OUTPUT", "stderr":"selected error"}),
    }).await?;
    for requests in [false, true] {
        let mut text = String::new();
        harness
            .append_turns(&mut text, task, &from, &through, requests)
            .await?;
        assert!(
            text.contains("😀"),
            "summary lost split UTF-8 output: {text}"
        );
        assert!(text.contains("selected error"));
        assert!(
            !text.contains("UNSELECTED_LATER_OUTPUT"),
            "summary crossed its selected cut"
        );
        assert!(!text.contains('�'));
    }
    harness.checkpoint_history(OperationId::new(), task).await?;
    let (_, checkpoint) = harness
        .selected_checkpoint(task)
        .await?
        .ok_or_else(|| Error::NotFound("output history checkpoint".into()))?;
    let text = harness.summary_text(&checkpoint).await?;
    assert!(
        text.contains("😀UNSELECTED_LATER_OUTPUT"),
        "head summary lost canonical stdout"
    );
    assert!(text.contains("selected error"));
    harness.disconnect_unfinished().await?;
    Ok(())
}

#[tokio::test]
async fn large_process_result_uses_bounded_stream_frames() -> Result<()> {
    let (harness, context, _directory, _state) = fixture(None).await?;
    let output = Output::open(&context, Arc::clone(harness.task_files()), b"bash".to_vec()).await?;
    let stdout = "complete output\n".repeat(8192);
    let expected = stdout.clone();
    let mut output = tokio::task::spawn_blocking(move || {
        output_chunks(output, stdout.as_bytes(), b"error tail")
    })
    .await
    .map_err(storage)?
    .map_err(storage)?;
    output
        .finish(&ToolResult {
            value: serde_json::json!({"success":true, "stdout":expected, "stderr":"error tail"}),
        })
        .await?;
    let files = snapshot(&harness, &context).await?;
    assert_eq!(stream(&files, false), expected.as_bytes());
    assert_eq!(stream(&files, true), b"error tail");
    for file in files.files {
        assert!(crate::task_volume::record_bytes(&file)? <= acyclic_stream::MAX_RECORD_BYTES);
    }
    harness.disconnect_unfinished().await?;
    Ok(())
}

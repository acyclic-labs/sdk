use super::*;

#[tokio::test]
async fn cancelled_finish_settles_both_captured_tails() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let (harness, context, _directory, _state) = fixture(Some(&cancellation)).await?;
    let output = Output::open(&context, Arc::clone(harness.task_files()), b"bash".to_vec()).await?;
    let mut observation = harness
        .task_files()
        .observe(query(&harness, &context).await?, None, 16)
        .await?;
    let mut output =
        tokio::task::spawn_blocking(move || output_chunks(output, b"captured", b"stopped"))
            .await
            .map_err(storage)?
            .map_err(storage)?;
    // Hold publication while finish starts. Its first poll sends the timer's
    // stop signal, but cannot complete the tail writes or publish a result.
    let writer = Arc::clone(&output.writer);
    let held = writer.lock().await;
    let result = ToolResult {
        value: serde_json::json!({"success": true}),
    };
    let mut finishing = Box::pin(output.finish(&result));
    std::future::poll_fn(|context| {
        assert!(finishing.as_mut().poll(context).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(finishing);
    cancellation.cancel();
    drop(output);
    drop(held);
    tokio::time::timeout(Duration::from_secs(1), async {
        let mut seen = (false, false);
        while !seen.0 || !seen.1 {
            let change = observation
                .changes
                .next()
                .await
                .expect("cleanup follow remains open")?;
            for file in change.file.iter().chain(&change.files) {
                match &file.key {
                    FileKey::Task {
                        file:
                            TaskFile::Tool {
                                file: ToolFile::Stdout(_),
                                ..
                            },
                        ..
                    } => seen.0 = true,
                    FileKey::Task {
                        file:
                            TaskFile::Tool {
                                file: ToolFile::Stderr(_),
                                ..
                            },
                        ..
                    } => seen.1 = true,
                    _ => {}
                }
            }
        }
        Ok::<_, crate::Error>(())
    })
    .await
    .expect("cancelled finish must settle both captured streams")?;
    let files = snapshot(&harness, &context).await?;
    assert_eq!(stream(&files, false), b"captured");
    assert_eq!(stream(&files, true), b"stopped");
    harness.disconnect_unfinished().await?;
    Ok(())
}

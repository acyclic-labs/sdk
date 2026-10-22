use super::*;
use acyclic_native_runtime::testing::descriptors_naming;

#[tokio::test]
async fn an_open_stream_holds_one_journal_descriptor() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let journal = directory.path().canonicalize()?.join(JOURNAL_FILE);
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    provider
        .append(AppendRequest {
            path: StreamPath::new("agents")?,
            records: vec![Bytes::from_static(b"one")],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    // Appends, syncs and replay share the journal's single descriptor,
    // which the process may already have closed while the stream idles.
    assert!(descriptors_naming(&journal)? <= 1);
    drop(provider);
    Ok(())
}

fn append(path: &str, tail: u64) -> Result<AppendRequest, Box<dyn std::error::Error>> {
    Ok(AppendRequest {
        path: StreamPath::new(path)?,
        records: vec![Bytes::from_static(b"record")],
        if_tail: Some(tail),
        idempotency_key: None,
    })
}

#[tokio::test]
async fn an_idle_journal_closes_its_descriptor_and_appends_after_reopening()
-> Result<(), Box<dyn std::error::Error>> {
    // The bound is the process's, so the test runs in its own process,
    // where its evictions cannot change other tests' work.
    if acyclic_native_runtime::cached_descriptor_capacity() != 1 {
        let module = module_path!();
        let test = format!(
            "{}::an_idle_journal_closes_its_descriptor_and_appends_after_reopening",
            module.split_once("::").map_or(module, |(_, path)| path)
        );
        let status = std::process::Command::new(std::env::current_exe()?)
            .args([test.as_str(), "--exact", "--nocapture"])
            .env(acyclic_native_runtime::CACHED_DESCRIPTORS_VARIABLE, "1")
            .status()?;
        assert!(status.success(), "child test failed: {status}");
        return Ok(());
    }
    let first = tempfile::tempdir()?;
    let second = tempfile::tempdir()?;
    let journal = first.path().canonicalize()?.join(JOURNAL_FILE);
    let idle = LocalStream::open(first.path(), LocalStreamLimits::default()).await?;
    idle.append(append("agents", 0)?).await?;
    // The process keeps one idle descriptor, so opening another stream
    // closes the idle journal's.
    let busy = LocalStream::open(second.path(), LocalStreamLimits::default()).await;
    // A flush that finished may still hold the descriptor for a moment.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut closed = descriptors_naming(&journal);
    while closed.as_ref().is_ok_and(|open| *open != 0) && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        closed = descriptors_naming(&journal);
    }
    let appended = idle.append(append("agents", 1)?).await;
    busy?.append(append("agents", 0)?).await?;
    assert_eq!(closed?, 0, "an idle journal kept its descriptor");
    // The append after reopening lands after the first, not over the header.
    appended?;
    drop(idle);
    let reopened = LocalStream::open(first.path(), LocalStreamLimits::default()).await?;
    reopened.append(append("agents", 2)?).await?;
    Ok(())
}

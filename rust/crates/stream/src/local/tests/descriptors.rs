use super::*;

/// Counts this process's descriptors that name `path`.
fn descriptors_naming(path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    let mut count = 0;
    for entry in std::fs::read_dir("/proc/self/fd")? {
        if std::fs::read_link(entry?.path()).is_ok_and(|target| target == path) {
            count += 1;
        }
    }
    Ok(count)
}

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
    // Appends, syncs and replay share the journal's single descriptor: a
    // host with many open streams pays one descriptor per journal.
    assert_eq!(descriptors_naming(&journal)?, 1);
    drop(provider);
    Ok(())
}

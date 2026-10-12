use super::*;

#[cfg(all(feature = "local", any(unix, windows)))]
fn cut_journal(root: &std::path::Path, journal: &[u8], length: usize) -> std::io::Result<()> {
    std::fs::write(
        root.join("stream").join("stream.journal"),
        journal.get(..length).unwrap_or(journal),
    )
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_fork_survives_power_loss_at_every_journal_cut() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let journal_path = root.join("stream").join("stream.journal");
    let key = IdempotencyKey::from_bytes([0x61; 16]);
    let source_id;
    {
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = unbounded!(fs, create_workspace("repo"))?;
        main.write_text("/base.txt", "base").await?;
        source_id = unbounded!(main, head())?.id();
        drop(main);
        close_local(fs).await?;
    }
    let before = std::fs::metadata(&journal_path)?.len();
    let forked_head;
    {
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = unbounded!(fs, open_workspace("repo"))?;
        let source = unbounded!(main, head())?;
        assert_eq!(source.id(), source_id);
        let fork =
            fork_workspace(&main, "agent", ForkOptions::from_generation(source, key)).await?;
        forked_head = unbounded!(fork, head())?.id();
        drop((main, fork));
        close_local(fs).await?;
    }
    let journal = std::fs::read(&journal_path)?;
    let before = usize::try_from(before)?;
    let mut cuts = vec![before];
    let mut frame = before;
    while frame < journal.len() {
        let length = journal
            .get(frame..frame + 4)
            .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
            .map(u32::from_le_bytes)
            .ok_or("fork journal frame is truncated")?;
        let end = frame + 4 + usize::try_from(length)? + 32;
        cuts.extend([
            frame + 2,
            frame + 4 + usize::try_from(length)? / 2,
            end - 1,
            end,
        ]);
        frame = end;
    }
    assert_eq!(frame, journal.len(), "fork frames do not end the journal");
    for length in cuts {
        cut_journal(root, &journal, length)?;
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = unbounded!(fs, open_workspace("repo"))?;
        let source = unbounded!(main, head())?;
        assert_eq!(source.id(), source_id);
        if let Ok(agent) = unbounded!(fs, open_workspace("agent")) {
            assert_eq!(unbounded!(agent, head())?.id(), forked_head);
            assert_eq!(
                unbounded!(agent, read("/base.txt", 16))?,
                OwnedBytes::from_static(b"base")
            );
        }
        let retried =
            fork_workspace(&main, "agent", ForkOptions::from_generation(source, key)).await?;
        assert_eq!(unbounded!(retried, head())?.id(), forked_head);
        assert_eq!(
            unbounded!(retried, read("/base.txt", 16))?,
            OwnedBytes::from_static(b"base")
        );
        drop((main, retried));
        close_local(fs).await?;
    }
    Ok(())
}

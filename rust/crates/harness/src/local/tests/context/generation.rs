use super::*;

// Both capture routes call this after their first real instruction read. The
// update runs inline, so the interleaving has no timers, threads or assertions
// about which reader the routing selected.
fn update_after_agents<'a>(
    harness: &'a LocalHarness,
    name: &'a str,
) -> acyclic_stream::BoxProviderFuture<'a, Result<()>> {
    Box::pin(async move {
        if name == "AGENTS.md" {
            instruction(harness, "AGENTS.md", "new agents").await?;
            instruction(harness, "CLAUDE.md", "new claude").await?;
            #[cfg(any(test, feature = "native-execution"))]
            for (name, bytes) in [("AGENTS.md", "new agents"), ("CLAUDE.md", "new claude")] {
                std::fs::write(harness.workspace().unwrap().path.join(name), bytes)
                    .map_err(super::super::super::storage)?;
            }
        }
        Ok(())
    })
}

async fn capture(checkout: bool) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>)> {
    let directory = tempfile::tempdir().map_err(super::super::super::storage)?;
    let project = volume(directory.path()).await?;
    let mut harness = LocalHarness::open(volume_config(directory.path(), project, text())?).await?;
    instruction(&harness, "AGENTS.md", "old agents").await?;
    instruction(&harness, "CLAUDE.md", "old claude").await?;
    #[cfg(any(test, feature = "native-execution"))]
    {
        for (name, bytes) in [("AGENTS.md", "old agents"), ("CLAUDE.md", "old claude")] {
            std::fs::write(harness.workspace().unwrap().path.join(name), bytes)
                .map_err(super::super::super::storage)?;
        }
        harness.checkout_context = checkout;
    }
    #[cfg(not(any(test, feature = "native-execution")))]
    assert!(!checkout);
    // The former live-read shape retains the first read, then observes the
    // second file after the inline update. Both production routes now pin it.
    let first_live = std::fs::read(harness.workspace().unwrap().path.join("AGENTS.md"))
        .map_err(super::super::super::storage)?;
    harness.context_read_observer = Some(update_after_agents);
    let operation = OperationId::new();
    harness.stage_context_manifest(operation).await?;
    let (_, captured) = manifest(&harness, operation).await?;
    assert_eq!(captured.discovery.instructions.len(), 2);
    let reader = harness.context_reader()?;
    let mut bodies = Vec::new();
    for file in &captured.discovery.instructions {
        bodies.push(reader.read(file, 1024, None, None)?.await?.to_vec());
    }
    let second_live = std::fs::read(harness.workspace().unwrap().path.join("CLAUDE.md"))
        .map_err(super::super::super::storage)?;
    Ok((bodies, vec![first_live, second_live]))
}

#[tokio::test]
async fn volume_generation_is_pinned_across_an_update_between_instruction_reads() -> Result<()> {
    let expected = vec![b"old agents".to_vec(), b"old claude".to_vec()];
    let (actual, broken) = capture(false).await?;
    assert_eq!(actual, expected, "manifest mixed project generations");
    assert_eq!(broken, [b"old agents".to_vec(), b"new claude".to_vec()]);
    assert_ne!(
        broken, expected,
        "detector missed live reads across generations"
    );
    #[cfg(any(test, feature = "native-execution"))]
    {
        let (actual, broken) = capture(true).await?;
        assert_eq!(
            actual, expected,
            "checkout manifest mixed project generations"
        );
        assert_ne!(broken, expected, "detector missed live checkout reads");
    }
    Ok(())
}

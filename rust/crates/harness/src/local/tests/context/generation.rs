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
            #[cfg(feature = "native-execution")]
            for (name, bytes) in [("AGENTS.md", "new agents"), ("CLAUDE.md", "new claude")] {
                std::fs::write(harness.workspace().unwrap().path.join(name), bytes)
                    .map_err(super::super::super::storage)?;
            }
        }
        Ok(())
    })
}

async fn capture(restore_faulty_route: bool) -> Result<Vec<Vec<u8>>> {
    let directory = tempfile::tempdir().map_err(super::super::super::storage)?;
    let project = volume(directory.path()).await?;
    let mut harness = LocalHarness::open(volume_config(directory.path(), project, text())?).await?;
    instruction(&harness, "AGENTS.md", "old agents").await?;
    instruction(&harness, "CLAUDE.md", "old claude").await?;
    #[cfg(feature = "native-execution")]
    {
        for (name, bytes) in [("AGENTS.md", "old agents"), ("CLAUDE.md", "old claude")] {
            std::fs::write(harness.workspace().unwrap().path.join(name), bytes)
                .map_err(super::super::super::storage)?;
        }
        // Negative control: restoring the former routing must make this same
        // production manifest capture fail the exact same byte-level detector.
        if restore_faulty_route {
            harness.checkout_context = true;
        }
    }
    #[cfg(not(feature = "native-execution"))]
    assert!(!restore_faulty_route);
    harness.context_read_observer = Some(update_after_agents);
    let operation = OperationId::new();
    harness.stage_context_manifest(operation).await?;
    let (_, captured) = manifest(&harness, operation).await?;
    assert_eq!(captured.discovery.instructions.len(), 2);
    let reader = harness.context_reader()?;
    let mut bodies = Vec::new();
    for file in &captured.discovery.instructions {
        bodies.push(reader.read(file, 1024, None)?.await?.to_vec());
    }
    Ok(bodies)
}

#[tokio::test]
async fn volume_generation_is_pinned_across_an_update_between_instruction_reads() -> Result<()> {
    let expected = vec![b"old agents".to_vec(), b"old claude".to_vec()];
    let actual = capture(false).await?;
    assert_eq!(actual, expected, "manifest mixed project generations");
    #[cfg(feature = "native-execution")]
    {
        let broken = capture(true).await?;
        assert_eq!(broken, [b"old agents".to_vec(), b"new claude".to_vec()]);
        assert_ne!(broken, expected, "detector missed the former checkout route");
    }
    Ok(())
}

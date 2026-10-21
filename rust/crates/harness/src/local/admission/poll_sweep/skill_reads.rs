//! Derive the cold catalog cost from its root probes, independently of discovery.
use super::*;
use crate::conversation::ContentResidencyVerifier;
use crate::resources::GenerationRef;

async fn selected(harness: &Harness) -> Result<(crate::resources::WorkspaceRef, GenerationRef)> {
    let workspace = crate::filesystem::workspace_ref(
        harness.project.provider().clone(),
        &harness.project.storage_name(&mut None)?,
    )?;
    let generation = harness.files.resolve(&workspace, None).await?.generation;
    Ok((workspace, generation))
}

async fn measured(
    fixture: &mut Fixture,
    future: impl std::future::Future<Output = Result<()>>,
) -> Result<usize> {
    let before = fixture.schedule.ready_reads();
    fixture.schedule.start();
    let (exit, _, _) = drive(
        future,
        None,
        |_| true,
        &fixture.schedule,
        fixture.events.get_mut(),
    )
    .await;
    match exit {
        Exit::Completed(result) => result?,
        Exit::Cancelled => return Err(Error::Storage("uncut catalog probe cancelled".into())),
    }
    assert!(
        fixture.schedule.points().is_empty(),
        "root probes added cancellation boundaries: {:?}",
        fixture.schedule.points()
    );
    Ok(fixture.schedule.ready_reads() - before)
}

async fn root_reads(fixture: &mut Fixture, harness: &Harness) -> Result<usize> {
    // Instruction capture has already selected the generation, outside this
    // incremental cost. Do not call discovery or inspect its observed trace.
    let (workspace, generation) = selected(harness).await?;
    let reader = harness.context_reader()?;
    for name in ["AGENTS.md", "CLAUDE.md"] {
        match reader
            .read_private_path(
                &harness.project,
                "",
                name,
                Some(&generation),
                harness.limits.file_bytes,
                None,
            )?
            .await
        {
            Err(Error::NotFound(_)) => {}
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(Error::Storage(format!(
                    "expected absent fixture instruction: {name}"
                )));
            }
        }
    }
    measured(fixture, async {
        for root in ["/.agents/skills", "/.claude/skills"] {
            match harness
                .files
                .stat(&workspace, Some(&generation), root)
                .await
            {
                Err(Error::NotFound(_)) => {}
                other => {
                    return Err(Error::Storage(format!(
                        "expected absent skill root: {other:?}"
                    )));
                }
            }
        }
        Ok(())
    })
    .await
}

#[test]
fn pinned_skill_catalog_reads_roots_once_and_reuses_the_generation() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        let _inline = acyclic_native_runtime::InlineBlocking::enter();
        for scenario in [
            Scenario::Text,
            Scenario::EffectAndContinuation,
            Scenario::Compaction,
        ] {
            let seed = Seed::new(scenario).await?;
            let mut fixture = Fixture::new(scenario, Some(&seed)).await?;
            let harness = fixture.open().await?;
            let expected = root_reads(&mut fixture, &harness).await?;
            let (_, generation) = selected(&harness).await?;
            let cold = measured(&mut fixture, async {
                assert!(harness.skills(Some(&generation)).await?.is_empty());
                Ok(())
            })
            .await?;
            let warm = measured(&mut fixture, async {
                for _ in 0..32 {
                    assert!(harness.skills(Some(&generation)).await?.is_empty());
                }
                Ok(())
            })
            .await?;
            eprintln!(
                "{scenario:?} skill root probes={expected}, cold={cold}, 32 cached loads={warm}"
            );
            assert!(expected > 0);
            assert_eq!(warm, 0, "cached catalog reread the authority stream");
            assert_eq!(
                cold, expected,
                "cold discovery did more than probe its roots"
            );
        }
        Ok(())
    })
}

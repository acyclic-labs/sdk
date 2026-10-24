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
    let (_, generation) = selected(harness).await?;
    let reader = harness.context_reader()?;
    for name in ["AGENTS.md", "CLAUDE.md"] {
        match reader
            .read_private_path(
                &harness.project,
                "",
                name,
                Some(&generation),
                harness.limits.file_bytes,
                crate::conversation::ContentAccess::default(),
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
        // The independent oracle uses the authorized public reader directly.
        // Discovery uses its trait adapter, which must not reverify creation.
        for root in [".agents/skills", ".claude/skills"] {
            match reader
                .list_private_directory(
                    &harness.project,
                    root,
                    root,
                    crate::filesystem::DirectoryOptions {
                        generation: Some(&generation),
                        after: None,
                        maximum_entries: 128,
                        ceiling: None,
                    },
                )
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

#[test]
fn directory_trait_adapter_verifies_creation_exactly_once() -> Result<()> {
    use acyclic_test_support::differential;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let (mut fixture, harness) = runtime.block_on(async {
        let seed = Seed::new(Scenario::Text).await?;
        let fixture = Fixture::new(Scenario::Text, Some(&seed)).await?;
        let harness = fixture.open().await?;
        Ok::<_, Error>((fixture, harness))
    })?;
    let (_, generation) = runtime.block_on(selected(&harness))?;
    let reader = harness.context_reader()?;
    let pair = differential::pair_of(
        &["content creation verify"],
        || (),
        |(), direct| runtime.block_on(async {
            measured(&mut fixture, async {
                let root = ".agents/skills";
                let result = if direct {
                    reader.list_private_directory(
                        &harness.project, root, root,
                        crate::filesystem::DirectoryOptions {
                            generation: Some(&generation), after: None,
                            maximum_entries: 128, ceiling: None,
                        },
                    ).await.map(|_| ())
                } else {
                    ContentResidencyVerifier::list_private_directory(
                        reader.as_ref(), &harness.project, root, root,
                        Some(&generation), None, 128, None, None,
                    )?.await.map(|_| ())
                };
                match result {
                    Err(Error::NotFound(_)) => Ok(()),
                    result => result,
                }
            }).await
        }),
    );
    for (result, trace) in pair {
        result?;
        assert_eq!(trace, [("content creation verify", 0)], "directory adapter repeated creation verification");
    }
    Ok(())
}

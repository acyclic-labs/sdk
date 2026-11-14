// Inject this module into local/workspace/volumes.rs for the same workload on
// main and the PR. It uses the real native_scope and current-thread executor.
// No timing threshold is asserted: compare printed samples between exact SHAs.
#[cfg(test)]
mod command_scope_benchmark {
    use super::*;
    use crate::test_support::{ScriptedModelProvider, ScriptedTurn};
    use std::time::Instant;

    async fn open(root: &Path, state: &Path) -> Result<crate::LocalHarness> {
        let model = crate::model::Model::new("scripted".into(), "test".into(), "1".into())?;
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("done".into())]).with_context_capacity(
                crate::context::ModelContextCapacity {
                    context_tokens: 32_768,
                    output_tokens: Some(4096),
                },
            ),
        );
        crate::LocalHarness::open(
            crate::test_support::local_harness_config(root, (model, provider))?
                .state_directory(state)
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await
    }

    async fn command(mounts: &Mounts) -> Result<(f64, f64)> {
        let start = Instant::now();
        let entered = native_scope(mounts, async |claims| Ok((Instant::now(), claims))).await?;
        Ok((
            entered.duration_since(start).as_secs_f64() * 1000.0,
            entered.elapsed().as_secs_f64() * 1000.0,
        ))
    }

    #[tokio::test]
    async fn command_scope_workload_benchmark() -> Result<()> {
        for files in [0, 500] {
            let root = tempfile::tempdir().map_err(storage)?;
            let state = tempfile::tempdir().map_err(storage)?;
            let harness = open(root.path(), state.path()).await?;
            let mounts = &harness.workspace().unwrap().mounts;
            let volumes = ordered_mounts(mounts.mounts());
            let volume_count = volumes
                .iter()
                .filter(|(_, mount)| {
                    mount
                        .view
                        .source
                        .as_ref()
                        .is_some_and(|source| source.options.is_some())
                })
                .count();
            for (_, volume) in volumes {
                let Some(source) = volume
                    .view
                    .source
                    .as_ref()
                    .filter(|source| source.options.is_some())
                else {
                    continue;
                };
                for index in 0..files {
                    source
                        .workspace
                        .workspace()
                        .write_text(&format!("/bench-{index:04}"), "benchmark seed")
                        .await?;
                }
            }
            assert!(volume_count > 0, "benchmark admitted no native volumes");
            // Warm driver/session setup before measuring per-command work.
            command(mounts).await?;
            let mut single = Vec::new();
            let mut admission = Vec::new();
            let mut completion = Vec::new();
            for _ in 0..10 {
                let start = Instant::now();
                let (entry, exit) = command(mounts).await?;
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                admission.push(entry);
                completion.push(exit);
                single.push(elapsed);
            }
            let start = Instant::now();
            for result in futures::future::join_all((0..8).map(|_| command(mounts))).await {
                result?;
            }
            let concurrent8 = start.elapsed().as_secs_f64() * 1000.0;
            let (entered, inside) = tokio::sync::oneshot::channel();
            let (release, released) = tokio::sync::oneshot::channel();
            let holder = native_scope(mounts, async |claims| {
                entered
                    .send(())
                    .map_err(|_| Error::Conflict("contender gone".into()))?;
                released.await.map_err(storage)?;
                Ok(((), claims))
            });
            let contender = async {
                inside.await.map_err(storage)?;
                let start = Instant::now();
                let result = command(mounts).await;
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                // Release even on error, so the holder has a terminal event.
                let _ = release.send(());
                result?;
                Ok::<_, Error>(elapsed)
            };
            let (held, contended) = futures::join!(holder, contender);
            held?;
            let contended = contended?;
            single.sort_by(f64::total_cmp);
            admission.sort_by(f64::total_cmp);
            completion.sort_by(f64::total_cmp);
            eprintln!(
                "COMMAND_SCOPE_PHASES files={files} volumes={volume_count} admission_ms={admission:?} completion_ms={completion:?}",
            );
            eprintln!(
                "COMMAND_SCOPE_BENCH files={files} volumes={volume_count} single_ms={single:?} single_p50_ms={} concurrent8_ms={concurrent8} contended_ms={contended}",
                single[single.len() / 2],
            );
            harness.disconnect().await?;
        }
        Ok(())
    }
}

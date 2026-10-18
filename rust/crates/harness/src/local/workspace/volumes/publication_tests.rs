use super::*;
use crate::test_support::{ScriptedModelProvider, ScriptedTurn};

async fn published_while_another_command_waits(skip_publication: bool) -> Result<bool> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let model = crate::model::Model::new(
        "scripted".into(),
        "test".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text("done".into())]).with_context_capacity(
            crate::context::ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: 4096,
            },
        ),
    );
    let harness = crate::LocalHarness::open(
        crate::test_support::local_harness_config(root.path(), (model, provider))?
            .state_directory(state.path())
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    let mounts = &harness.workspace().unwrap().mounts;
    let scratch = mounts
        .named(Some("scratch"))?
        .1
        .view
        .source
        .as_ref()
        .unwrap();
    let (admitted, admission) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let b = native_scope(mounts, async |claims| {
        admitted.send(()).unwrap();
        released.await.map_err(storage)?;
        Ok(((), claims))
    });
    let a = async {
        admission.await.map_err(storage)?;
        native_scope(mounts, async |claims| {
            std::fs::write(scratch.destination.join("data"), b"published A").map_err(storage)?;
            let claims = if skip_publication {
                // Restore the former last-user-only publication. B still
                // owns the projection, so A's acknowledgement skips its write.
                drop(claims);
                Claims(Vec::new())
            } else {
                claims
            };
            Ok(((), claims))
        })
        .await?;
        assert!(
            scratch.native().await.is_some(),
            "B lost its live projection"
        );
        let cancellation = CancellationToken::new();
        // Independently reopen the volume head, not the projected filesystem
        // or the publishing mount's cached checkout.
        let head = scratch
            .workspace
            .workspace()
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value;
        let read = head
            .read("/data", 32, WorkBudget::UNBOUNDED, &cancellation)
            .await;
        let published = read.is_ok_and(|read| read.value.as_ref() == b"published A");
        release.send(()).unwrap();
        Ok::<_, Error>(published)
    };
    let (published, b) = tokio::join!(a, b);
    b?;
    assert!(scratch.native().await.is_none());
    assert!(!scratch.destination.exists());
    published
}

#[tokio::test]
async fn acknowledged_command_is_reopenable_while_another_command_owns_projection() -> Result<()> {
    assert!(published_while_another_command_waits(false).await?);
    assert!(
        !published_while_another_command_waits(true).await?,
        "omitted publication escaped the independent head oracle"
    );
    Ok(())
}

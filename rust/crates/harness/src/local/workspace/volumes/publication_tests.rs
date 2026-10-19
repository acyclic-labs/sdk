use super::*;
use crate::test_support::{ScriptedModelProvider, ScriptedTurn};

async fn open(root: &Path, state: &Path) -> Result<crate::LocalHarness> {
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

async fn published_while_another_command_waits(skip_publication: bool) -> Result<bool> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
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

// A scope whose later volume cannot be entered runs no command; the volumes it
// already projected leave through the same exit instead of staying mounted.
#[tokio::test]
async fn failed_scope_entry_retires_the_projections_it_entered() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = &harness.workspace().unwrap().mounts;
    let (_, mount) = mounts.named(Some("scratch"))?;
    let scratch = mount.view.source.as_ref().unwrap();
    // `.volumes/scratch` enters first; `.volumes/zzz` names a binding its
    // source does not own, so its entry fails after scratch is projected.
    let table = super::super::mounts::owner(
        &mounts.root,
        &mounts.named(None)?.1.view.volume,
        None,
        ["scratch", "zzz"].map(|name| {
            (
                name.to_owned(),
                mount.view.volume.clone(),
                Some(Arc::clone(scratch)),
            )
        }),
    )?;
    let ran = std::sync::atomic::AtomicBool::new(false);
    let failure = native_scope(&table, async |claims| {
        ran.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(((), claims))
    })
    .await
    .expect_err("an unowned binding entered");
    assert!(
        failure.to_string().contains("outside its binding"),
        "{failure:?}"
    );
    assert!(!ran.into_inner());
    assert!(
        scratch.native().await.is_none(),
        "failed entry stranded a projection"
    );
    assert!(!scratch.destination.exists());
    // Negative control: the former exit dropped entered claims without leaving.
    // The same oracle sees that projection remain mounted.
    let root_path = projection_root(&scratch.destination, SCRATCH_PATH)?;
    drop(scratch.enter(root_path, SCRATCH_PATH, false).await?);
    assert!(
        scratch.native().await.is_some(),
        "oracle missed a stranded projection"
    );
    retire(mounts).await?;
    assert!(scratch.native().await.is_none());
    Ok(())
}

// Disconnect retirement attempts every volume. A volume a cancelled worker still
// owns reports its conflict, but cannot strand a later volume's projection
// until the harness drops and abandons it unpublished.
#[tokio::test]
async fn retirement_continues_past_a_volume_a_worker_still_owns() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = &harness.workspace().unwrap().mounts;
    let (_, mount) = mounts.named(Some("scratch"))?;
    let scratch = mount.view.source.as_ref().unwrap();
    // `.volumes/aaa` precedes scratch and still has one worker claim.
    let busy = Arc::new(MountedVolume {
        workspace: crate::tools::git::OwnedView::Volume(scratch.workspace.workspace().clone()),
        destination: mounts.root.join(".volumes/aaa"),
        native: StateArc::new(AsyncMutex::new(None)),
        users: StateArc::new(crate::sync::SyncMutex::new(1)),
    });
    let table = super::super::mounts::owner(
        &mounts.root,
        &mounts.named(None)?.1.view.volume,
        None,
        [("aaa", busy), ("scratch", Arc::clone(scratch))].map(|(name, source)| {
            (name.to_owned(), mount.view.volume.clone(), Some(source))
        }),
    )?;
    let project = async || {
        let root = projection_root(&scratch.destination, SCRATCH_PATH)?;
        drop(scratch.enter(root, SCRATCH_PATH, false).await?);
        Ok::<_, Error>(())
    };
    project().await?;
    assert!(matches!(
        retire(&table).await,
        Err(Error::Conflict(message)) if message.contains("native workers")
    ));
    assert!(
        scratch.native().await.is_none(),
        "a conflict stranded a later projection"
    );
    assert!(!scratch.destination.exists());
    // Negative control: the former loop stopped at the first conflict. The
    // same oracle sees the later projection remain mounted.
    project().await?;
    let former = async {
        for (_, mount) in table.mounts() {
            if let Some(source) = &mount.view.source {
                Box::pin(source.retire()).await?;
            }
        }
        Ok::<_, Error>(())
    };
    assert!(former.await.is_err());
    assert!(
        scratch.native().await.is_some(),
        "oracle missed a stranded projection"
    );
    retire(mounts).await?;
    assert!(scratch.native().await.is_none());
    Ok(())
}

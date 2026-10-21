//! The held FSEvents source fences with a cookie in an excluded subtree.
use super::*;
use std::time::Duration;

const FENCE: Duration = Duration::from_secs(5);

fn fenced_batch(watch: &mut NativeWatch) -> Result<WatchBatch, Box<dyn std::error::Error>> {
    watch.fence(FENCE)?;
    Ok(watch
        .poll(64, WorkBudget::UNBOUNDED, &CancellationToken::new())?
        .value)
}

/// Rebaselines until a fence confirms a quiet tree. FSEvents may replay the
/// temporary root's own creation once after the first baseline.
fn quiet(watch: &mut NativeWatch) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..3 {
        let _ = watch.begin_rescan()?;
        let _ = watch.finish_rescan()?;
        if let WatchBatch::Changes { changes, .. } = fenced_batch(watch)?
            && changes.is_empty()
        {
            return Ok(());
        }
    }
    Err("a fence after a quiet baseline never confirmed delivery".into())
}

#[test]
fn a_quiet_fence_confirms_delivery_without_a_rescan() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let volumes = directory.path().join(".volumes");
    std::fs::create_dir(&volumes)?;
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    let mut watch = NativeWatch::open_with_profile_with_native_work(
        directory.path(),
        native_filesystem_profile(),
        NativeWatchOptions::new(VolumeLimits::default()),
        &work,
    )?;
    let excluded = relative_namespace_path(
        directory.path(),
        &volumes,
        native_filesystem_profile(),
        VolumeLimits::default(),
    )?;
    watch.exclude_subtrees(&[excluded])?;
    quiet(&mut watch)?;
    // Repeated quiet fences, and harness writes beneath the excluded subtree,
    // never require reading the captured tree again.
    for attempt in 0..3 {
        std::fs::write(volumes.join(format!("owned-{attempt}")), b"x")?;
        let batch = fenced_batch(&mut watch)?;
        assert!(
            matches!(&batch, WatchBatch::Changes { changes, .. } if changes.is_empty()),
            "quiet fence {attempt} produced {batch:?}"
        );
    }
    let leftovers = std::fs::read_dir(&volumes)?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".acyclic-fence-")
        })
        .count();
    assert_eq!(leftovers, 0, "fence cookies are removed");
    // A change to the captured tree is still reported.
    std::fs::write(directory.path().join("authored.txt"), b"x")?;
    assert!(matches!(
        fenced_batch(&mut watch)?,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::NativeRescanRequired,
            ..
        }
    ));
    Ok(())
}

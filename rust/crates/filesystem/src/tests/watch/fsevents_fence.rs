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

#[test]
fn a_watch_without_exclusions_fences_by_requiring_a_rescan()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    let mut watch = NativeWatch::open_with_profile_with_native_work(
        directory.path(),
        native_filesystem_profile(),
        NativeWatchOptions::new(VolumeLimits::default()),
        &work,
    )?;
    let _ = watch.begin_rescan()?;
    let _ = watch.finish_rescan()?;
    // There is no excluded subtree for a cookie, and the captured tree is
    // never written, so the fence cannot confirm delivery: it succeeds and
    // the consumer rescans.
    assert!(matches!(
        fenced_batch(&mut watch)?,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::NativeRescanRequired,
            ..
        }
    ));
    let entries = std::fs::read_dir(directory.path())?.count();
    assert_eq!(entries, 0, "the fence wrote nothing into the captured tree");
    Ok(())
}

/// A read-only `.git` sorts before the harness's own excluded directory, but
/// cannot hold a cookie: the fence places it in the next one and still
/// confirms a quiet tree without a rescan.
#[test]
fn a_read_only_git_directory_does_not_block_a_quiet_fence() -> Result<(), Box<dyn std::error::Error>>
{
    use std::os::unix::fs::PermissionsExt as _;
    let directory = tempfile::tempdir()?;
    let git = directory.path().join(".git");
    let volumes = directory.path().join(".volumes");
    std::fs::create_dir(&git)?;
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
    let excluded = [&git, &volumes]
        .into_iter()
        .map(|host| {
            relative_namespace_path(
                directory.path(),
                host,
                native_filesystem_profile(),
                VolumeLimits::default(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    watch.exclude_subtrees(&excluded)?;
    std::fs::set_permissions(&git, std::fs::Permissions::from_mode(0o555))?;
    let quiet = quiet(&mut watch);
    std::fs::set_permissions(&git, std::fs::Permissions::from_mode(0o755))?;
    quiet?;
    assert_eq!(
        std::fs::read_dir(&git)?.count(),
        0,
        "no cookie reached .git"
    );
    Ok(())
}

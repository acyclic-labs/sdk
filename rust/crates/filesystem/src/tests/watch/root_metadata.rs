use super::*;
use std::os::unix::fs::PermissionsExt as _;
use std::time::Duration;

#[test]
fn root_attributes_are_metadata_hints_and_replacements_still_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("watched");
    std::fs::create_dir(&root)?;
    let mut options = NativeWatchOptions::new(VolumeLimits::default());
    options.recursive = false;
    let mut watch = NativeWatch::open(&root, options)?;
    let rescan = watch.begin_rescan()?;
    assert!(matches!(
        watch.finish_rescan(rescan)?,
        WatchBatch::Changes { .. }
    ));
    let identity = watch.root_identity();
    let mode = std::fs::metadata(&root)?.permissions().mode();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(mode ^ 0o040))?;
    watch.fence(Duration::from_secs(1))?;
    let observed = watch.poll(8, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
    assert!(
        matches!(
            &observed.value,
            WatchBatch::Changes { changes, .. }
                if changes.iter().any(|change| matches!(change,
                    WatchChange::MetadataChanged(path) if path.is_root()))
        ),
        "unexpected root metadata batch: {:?}",
        observed.value
    );
    assert_eq!(watch.root_identity(), identity);
    std::fs::rename(&root, parent.path().join("displaced"))?;
    std::fs::create_dir(&root)?;
    let observed = watch.poll(8, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
    assert!(matches!(observed.value, WatchBatch::RescanRequired { .. }));
    Ok(())
}

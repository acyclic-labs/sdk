//! ProjFS cannot start a virtualization root inside another active one.
use super::*;

// A child view mounted inside a parent's active projection is rejected before
// any physical state exists, as its own typed cause; neither failure cleanup
// nor the parent's teardown waits on the rejected start.
#[tokio::test]
async fn nested_destination_is_rejected_before_marking() -> Result<(), Box<dyn std::error::Error>> {
    let _services = setup_work()?;
    let parent = windows_checkout_source().await?;
    let (_root, destination, mut session) = mount_source(&parent)?;
    let nested = destination.join("child-view");
    std::fs::create_dir(&nested)?;
    let child = windows_checkout_source().await?;
    let started = std::time::Instant::now();
    let Err(NativeMountError::StartupFailed(mut failure)) = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: child.volume_id()?,
            destination: nested.clone(),
            writable: true,
        },
        Arc::clone(&child) as Arc<dyn MountFilesystem>,
    ) else {
        return Err("a nested projection started or lost its cleanup owner".into());
    };
    assert!(
        matches!(failure.error(), NativeMountError::NestedDestination),
        "{failure:?}"
    );
    failure.owner_mut().stop()?;
    assert!(failure.owner().cleanup_state().cleanup_complete());
    assert_eq!(super::reparse_tag(&nested)?.0, None);
    assert!(sorted_names(&nested)?.is_empty());
    session.stop()?;
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    Ok(())
}

#[tokio::test]
async fn junction_cannot_hide_a_parent_projection() -> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let parent = windows_checkout_source().await?;
    let (_root, destination, mut session) = mount_source(&parent)?;
    let subdirectory = destination.join("subdirectory");
    std::fs::create_dir(&subdirectory)?;
    let aliases = tempfile::tempdir()?;
    let alias = aliases.path().join("alias");
    create_junction(&alias, &subdirectory)?;
    let child = alias.join("child");
    std::fs::create_dir(&child)?;
    // Exercise the pre-mark guard itself: the old implementation accepts this
    // layout, without starting a second driver or leaving a rejected root.
    let rejected = reject_nested_projection(&child, &work);
    std::fs::remove_dir(&child)?;
    std::fs::remove_dir(&alias)?;
    session.stop()?;
    assert!(
        matches!(rejected, Err(NativeMountError::NestedDestination)),
        "junction hid the parent projection: {rejected:?}"
    );
    Ok(())
}

#[test]
fn junction_to_an_ordinary_directory_remains_allowed() -> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let ordinary = tempfile::tempdir()?;
    let aliases = tempfile::tempdir()?;
    let alias = aliases.path().join("alias");
    create_junction(&alias, ordinary.path())?;
    let child = alias.join("child");
    std::fs::create_dir(&child)?;
    let result = reject_nested_projection(&child, &work);
    std::fs::remove_dir(&alias)?;
    result?;
    Ok(())
}

fn create_junction(alias: &std::path::Path, target: &std::path::Path) -> std::io::Result<()> {
    crate::windows_test_filesystems::junction(alias, target)
}

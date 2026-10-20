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

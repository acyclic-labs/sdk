use super::*;

#[test]
fn fresh_process_recovery_initializes_its_registry() -> Result<(), Box<dyn std::error::Error>> {
    const MODE: &str = "ACYCLIC_FS_RECOVERY_BOOTSTRAP";
    const DESTINATION: &str = "ACYCLIC_FS_RECOVERY_DESTINATION";
    if let Ok(mode) = std::env::var(MODE) {
        let destination = PathBuf::from(std::env::var_os(DESTINATION).unwrap());
        let original =
            NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(|(error, _)| error)?;
        // Negative control: the former acquisition reaches this uninitialized
        // bounded open. A separate process prevents other tests masking it.
        assert!(matches!(
            registry::open(&mut &original),
            Err(NativeMountError::ServicesNotInitialized)
        ));
        match mode.as_str() {
            "recover" => recover_native_mount_destination(&destination)?,
            "detach" => detach_native_mount_destination_after_crash(&destination)?,
            "owner" | "pinned" => {
                let mut owner = if mode == "pinned" {
                    let parent = std::fs::File::open(destination.parent().unwrap())?;
                    NativeMountRecoveryOwner::acquire_in_parent(&destination, &parent.into())?
                } else {
                    NativeMountRecoveryOwner::acquire(&destination)?
                };
                assert!(owner.recover()?);
                await_recovery_worker(owner)?;
            }
            _ => panic!("unknown recovery mode"),
        }
        assert_eq!(destination.exists(), mode == "detach");
        return Ok(());
    }
    let root = tempfile::tempdir()?;
    for mode in ["recover", "detach", "owner", "pinned"] {
        let destination = root.path().join(mode);
        std::fs::create_dir(&destination)?;
        let mut crashed = MountDestinationGuard::acquire_canonical(&destination.canonicalize()?)?;
        let fence = crashed.lock_path.clone();
        drop(crashed.file.take());
        drop(crashed);
        let output = acyclic_native_runtime::process_output(
            std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", "native_mount::tests::recovery_bootstrap::fresh_process_recovery_initializes_its_registry", "--nocapture"])
                .env(MODE, mode)
                .env(DESTINATION, &destination),
            Duration::from_secs(5),
            16 * 1024,
        )?;
        assert!(output.status.success(), "{mode}: {output:?}");
        assert!(!fence.exists());
        assert_eq!(destination.exists(), mode == "detach");
    }
    Ok(())
}

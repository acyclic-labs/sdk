use super::*;
use std::io::{BufRead, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::process::{Child, Command, Stdio};

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;
const OWNER_ROOT: &str = "ACYCLIC_SCRATCH_RECOVERY_TEST_ROOT";
const OWNER_TEST: &str = "local::workspace::volumes::recovery_tests::scratch_mount_owner";

// Own the real provider process through every assertion, including failures.
struct Owner(Child, PathBuf);
impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
        let _ = recover_after_crash(&self.1);
    }
}

#[test]
fn scratch_mount_owner() -> TestResult {
    let Some(root) = std::env::var_os(OWNER_ROOT) else {
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        use acyclic_fs::model::{
            AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
            VolumeConfig,
        };
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let cancellation = CancellationToken::new();
        let fs = acyclic_fs::Fs::memory();
        let volume = fs
            .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        let checkout = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let source = Arc::new(acyclic_fs::CheckoutMountSource::new(
            Arc::new(acyclic_fs::SharedCheckout::new(checkout)),
            config,
        )?);
        let _mount = acyclic_fs::mount_native_over_existing(
            acyclic_fs::NativeMountRequest {
                mount_id: acyclic_fs::MountId::new(),
                volume_id: source.volume_id()?,
                destination: PathBuf::from(root).join(SCRATCH_PATH),
                writable: true,
            },
            source,
        )?;
        println!("SCRATCH_READY");
        std::io::stdout().flush()?;
        // The parent kills the provider after observing the mounted namespace.
        std::io::stdin().read_exact(&mut [0])?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

fn require_recovered(root: &Path, recover: impl FnOnce(&Path) -> Result<()>) -> TestResult {
    recover(root)?;
    assert_eq!(
        std::fs::read(root.join(SCRATCH_PATH).join("authored"))?,
        b"keep me"
    );
    Ok(())
}

#[test]
fn scratch_recovery_preserves_live_mounts_and_uses_the_exclusive_fence() -> TestResult {
    let root = tempfile::tempdir()?;
    let root = root.path().canonicalize()?;
    let destination = root.join(SCRATCH_PATH);
    std::fs::create_dir_all(&destination)?;
    std::fs::write(destination.join("authored"), b"keep me")?;
    let original = std::fs::metadata(&destination)?;
    require_recovered(&root, recover_after_crash)?;
    assert_eq!(std::fs::metadata(&destination)?.ino(), original.ino());

    let mut owner = Owner(
        Command::new(std::env::current_exe()?)
            .args(["--exact", OWNER_TEST, "--nocapture"])
            .env(OWNER_ROOT, &root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?,
        root.clone(),
    );
    let mut output = std::io::BufReader::new(owner.0.stdout.take().unwrap());
    loop {
        let mut line = String::new();
        assert_ne!(
            output.read_line(&mut line)?,
            0,
            "provider exited before mounting"
        );
        if line.trim() == "SCRATCH_READY" {
            break;
        }
    }
    let live = std::fs::metadata(&destination)?;
    assert_ne!(
        live.dev(),
        original.dev(),
        "fixture must be a real FUSE mount"
    );
    recover_after_crash(&root)?;
    assert_eq!(std::fs::metadata(&destination)?.dev(), live.dev());
    assert!(matches!(
        acyclic_fs::NativeMountRecoveryOwner::acquire(&destination),
        Err(acyclic_fs::NativeMountError::DestinationBusy)
    ));
    owner.0.kill()?;
    owner.0.wait()?;

    // Negative control: cached attributes misclassify the dead provider as live.
    assert!(std::fs::metadata(&destination).is_ok());
    assert!(
        require_recovered(&root, |root| {
            if std::fs::metadata(root.join(SCRATCH_PATH)).is_ok() {
                Ok(())
            } else {
                recover_after_crash(root)
            }
        })
        .is_err()
    );
    assert_eq!(
        scratch_status(&destination).unwrap_err().kind(),
        std::io::ErrorKind::NotConnected
    );

    // Hold the actual destination flock, not a substitute synchronization model.
    // Recovery must reject it without detaching; releasing it permits recovery.
    let controls = acyclic_fs::native_mount_control_paths(&destination)?;
    let fence = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&controls[0])?;
    fence.try_lock()?;
    assert!(
        recover_after_crash(&root).is_err(),
        "recovery bypassed the fence"
    );
    assert_eq!(
        scratch_status(&destination).unwrap_err().kind(),
        std::io::ErrorKind::NotConnected
    );
    drop(fence);
    require_recovered(&root, recover_after_crash)?;
    assert_eq!(std::fs::metadata(&destination)?.ino(), original.ino());
    Ok(())
}

//! The public SDK mount is retired even when its owner cannot unwind.

#![cfg(target_os = "macos")]

use acyclic_fs::{CancellationToken, Fs, MountOptions};
use acyclic_fs_work::performance::WorkBudget;
use acyclic_native_runtime::{FileLock, FileLockMode};
use acyclic_test_support::readiness;
use std::ffi::{CStr, OsStr};
use std::io::{self, Read as _};
use std::os::unix::{ffi::OsStrExt as _, process::ExitStatusExt as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

#[path = "support/native_access.rs"]
mod native_access;

const TEST: &str = "darwin_mount_owner::sigkill_retires_the_live_owner_mount";
const DESTINATION: &str = "ACYCLIC_DARWIN_MOUNT_OWNER_DESTINATION";
const READY: &str = "MOUNT_READY";

#[allow(unsafe_code, reason = "reads the cached Darwin kernel mount table")]
fn mount_socket(destination: &Path) -> io::Result<Option<PathBuf>> {
    let mut records = std::ptr::null_mut();
    // SAFETY: the kernel supplies its cached array of count initialized records.
    let count = unsafe { libc::getmntinfo(&raw mut records, libc::MNT_NOWAIT) };
    if count <= 0 || records.is_null() {
        return Err(io::Error::last_os_error());
    }
    let count = usize::try_from(count).map_err(io::Error::other)?;
    // SAFETY: this test has one mount table reader and no competing getmntinfo call.
    let records = unsafe { std::slice::from_raw_parts(records, count) };
    Ok(records.iter().find_map(|record| {
        // SAFETY: Darwin terminates both mount table path fields with NUL.
        let (mounted, source) = unsafe {
            (
                CStr::from_ptr(record.f_mntonname.as_ptr()),
                CStr::from_ptr(record.f_mntfromname.as_ptr()),
            )
        };
        (mounted.to_bytes() == destination.as_os_str().as_bytes())
            .then(|| source.to_bytes().strip_prefix(b"<")?.strip_suffix(b">:/"))
            .flatten()
            .map(|socket| PathBuf::from(OsStr::from_bytes(socket)))
    }))
}

#[tokio::test]
async fn sigkill_retires_the_live_owner_mount() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(destination) = std::env::var_os(DESTINATION) {
        let fs = Fs::memory();
        let workspace = Box::pin(fs.create_workspace(
            "owner-death",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ))
        .await?
        .value;
        let options = native_access::options(&workspace, MountOptions::read_only()).await?;
        let _mount = Box::pin(workspace.mount(Path::new(&destination), options)).await?;
        readiness::signal(&mut io::stdout().lock(), READY)?;
        io::stdin().read_exact(&mut [0])?;
        return Err("the mount owner must exit from SIGKILL".into());
    }
    // The case's temporary root also anchors its private mount registry. Use
    // that same root in the owner so both processes observe the original fence.
    let temporary = std::env::temp_dir().canonicalize()?;
    let root = tempfile::tempdir_in(&temporary)?;
    let destination = root.path().join("owned");
    std::fs::create_dir(&destination)?;
    let [fence_path, _] = acyclic_fs::native_mount_control_paths(&destination)?;
    let mut owner = Command::new(std::env::current_exe()?)
        .args(["--exact", TEST, "--nocapture"])
        .env(DESTINATION, &destination)
        .env("TMPDIR", &temporary)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let stdout = owner.stdout.take().ok_or("missing owner stdout")?;
    let ready = readiness::wait(&mut io::BufReader::new(stdout), READY);
    let socket = ready.and_then(|()| {
        mount_socket(&destination)?
            .ok_or_else(|| io::Error::other("owner's live mount was missing"))
    });
    let killed = owner.kill();
    let status = owner.wait()?;
    let socket = socket?;
    killed?;
    assert_eq!(status.signal(), Some(libc::SIGKILL));
    let socket_directory = socket.parent().ok_or("missing NFS socket directory")?;

    // The inherited fence releases only after the custodian confirms kernel detachment.
    let fence = FileLock::acquire(std::fs::File::open(fence_path)?, FileLockMode::Exclusive)?;
    let attached = mount_socket(&destination)?.is_some();
    drop(fence);
    if attached {
        if let Err(error) = acyclic_fs::recover_native_mount_destination(&destination) {
            let _retained = root.keep();
            return Err(error.into());
        }
        return Err("the SIGKILLed owner left its NFS mount attached".into());
    }
    acyclic_fs::reclaim_native_mount_destination_fence(&destination)?;
    assert_eq!(socket_directory.parent(), Some(temporary.as_path()));
    assert!(
        !socket_directory.try_exists()?,
        "the private NFS socket directory survived"
    );
    Ok(())
}

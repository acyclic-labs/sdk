//! Blocking native access must not require the embedder's Tokio executor.

use super::*;

#[test]
fn current_thread_embedder_writes_through_its_own_view() -> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    let storage = tempfile::tempdir()?;
    let destination = tempfile::tempdir()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut session = runtime.block_on(async {
        let fs = crate::Fs::local(crate::LocalOptions::new(storage.path().join("fs"))).await?;
        let cancellation = crate::CancellationToken::new();
        let config = posix_config();
        let volume = fs
            .create_volume(config, crate::WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let checkout = volume
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let source = crate::native_mount::CheckoutMountSource::new(
            Arc::new(crate::native_mount::SharedCheckout::with_publication(
                checkout,
                crate::MountPublication::CloseAndSync,
            )),
            config,
        )?;
        let source =
            crate::native_mount::adapter::tests::access::bind_test_access(source, vec![], None)?;
        let session = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: destination.path().join("view"),
                writable: true,
                shared: true,
            },
            Arc::new(source),
        )?;
        // Deliberately stay on the only runtime thread. No spawn_blocking,
        // block_in_place, executor migration, or second task drives this call.
        let path = destination.path().join("view/file");
        std::fs::write(&path, b"current-thread owner")?;
        assert_eq!(std::fs::read(path)?, b"current-thread owner");
        Ok::<_, Box<dyn std::error::Error>>(session)
    })?;
    assert!(session.stop()?);
    Ok(())
}

const OWNER_CASE: &str = "ACYCLIC_FUSE_OWNER_DEATH_CASE";
const OWNER_ROOT: &str = "ACYCLIC_FUSE_OWNER_DEATH_ROOT";
const OWNER_TEST: &str =
    "native_mount::fuse::tests::owner::killed_owner_with_pending_request_retires_without_fusectl";

#[test]
fn killed_owner_with_pending_request_retires_without_fusectl()
-> Result<(), Box<dyn std::error::Error>> {
    use std::process::Command;
    if let Some(case) = std::env::var_os(OWNER_CASE) {
        let root =
            std::path::PathBuf::from(std::env::var_os(OWNER_ROOT).ok_or("owner root absent")?);
        if case == "controller" {
            return controller(&root);
        }
        return pending_owner(&root, case == "old");
    }
    let temporary = tempfile::tempdir()?;
    let output = Command::new("unshare")
        .args([
            "--user",
            "--map-root-user",
            "--mount",
            "--net",
            "--propagation",
            "private",
            "--",
        ])
        .arg(std::env::current_exe()?)
        .args([OWNER_TEST, "--exact", "--nocapture"])
        .env(OWNER_CASE, "controller")
        .env(OWNER_ROOT, temporary.path())
        .output()?;
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn transport(session: &crate::NativeMountSession) -> Arc<super::super::host_io::HostIo> {
    Arc::clone(
        session
            .driver
            .as_ref()
            .expect("native driver")
            .state
            .lock()
            .unwrap()
            .device
            .as_ref()
            .expect("physical host transport"),
    )
}

fn pending_owner(root: &Path, old: bool) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{Read, Write};
    use std::os::fd::BorrowedFd;
    crate::initialize_native_mount_services()?;
    // Own one duplicate, without stealing the process's original stdin slot.
    let mut control = std::fs::File::from(rustix::io::dup(unsafe { BorrowedFd::borrow_raw(0) })?);
    let (inner, _) = shared_sources(posix_config())?;
    let volume_id = inner.volume_id()?;
    let (entered, pending) = sync_channel(1);
    let (release, released) = sync_channel(1);
    let source = Arc::new(super::mount::GatedSource {
        inner: Arc::new(inner),
        gated_lookup: b"blocked".to_vec(),
        gated_directory: Vec::new(),
        entered,
        released: Mutex::new(released),
    });
    let start = || mount(source, volume_id, root);
    let mut session = if old {
        super::super::mount::in_process_negative_control(start)
    } else {
        start()
    }?;
    let devices = std::fs::read_dir("/proc/self/fd")?
        .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
        .filter(|target| target == Path::new("/dev/fuse"))
        .count();
    assert_eq!(
        devices,
        usize::from(old),
        "only the old-path control may retain the mounted FUSE device"
    );
    let device = session
        .native_root_identity()
        .ok_or("root identity absent")?
        .device;
    let connection =
        (u64::from(rustix::fs::major(device)) << 20) | u64::from(rustix::fs::minor(device));
    let action = session
        .driver
        .as_ref()
        .ok_or("driver absent")?
        .state
        .lock()
        .unwrap()
        .mount
        .exit_action_id()
        .ok_or("exit action absent")?;
    let owner_thread = acyclic_native_runtime::PacketDevice::owner_thread_for_test()?;
    let info = [
        u64::from(transport(&session).host_id().unwrap_or(0)),
        u64::from(action),
        connection,
        u64::from(owner_thread),
    ];
    control.write_all(info.map(u64::to_ne_bytes).as_flattened())?;
    let mut go = [0];
    control.read_exact(&mut go)?;
    let path = root.join("blocked");
    let request = std::thread::spawn(move || std::fs::metadata(path));
    // Actual FUSE dispatch has entered the original source. No response is
    // possible until the producer is released; no wall clock creates this cut.
    let observed = pending.recv();
    let io = observed.map_err(std::io::Error::other).and_then(|()| {
        control.write_all(&[1])?;
        control.read_exact(&mut go)
    });
    // Error cleanup must release the source before joining its packet workers.
    drop(release);
    let _ = request.join();
    let stopped = session.stop();
    io?;
    assert!(stopped?);
    Ok(())
}

fn process_event(id: u32, thread: bool) -> std::io::Result<std::os::fd::OwnedFd> {
    use std::os::fd::FromRawFd;
    // SAFETY: ids name live original participants whose handles are retained
    // before sending SIGKILL. pidfd_open returns one new owned descriptor.
    let file = unsafe {
        libc::syscall(
            libc::SYS_pidfd_open,
            id,
            if thread { libc::PIDFD_THREAD } else { 0 },
        )
    };
    if file < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(file as i32) })
}

fn wait_exit(event: &std::os::fd::OwnedFd) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let mut interest = libc::pollfd {
        fd: event.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        // One OS exit wait, not a periodic process probe.
        if unsafe { libc::poll(&raw mut interest, 1, -1) } >= 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn controller(root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{Read, Write};
    use std::os::{fd::OwnedFd, unix::net::UnixStream};
    use std::process::{Command, Stdio};
    crate::initialize_native_mount_services()?;
    for old in [false, true] {
        let survivor_root = root.join(format!("survivor-{old}"));
        let owner_root = root.join(format!("owner-{old}"));
        std::fs::create_dir(&survivor_root)?;
        std::fs::create_dir(&owner_root)?;
        let (source, _) = shared_sources(posix_config())?;
        let volume_id = source.volume_id()?;
        let mut survivor = mount(Arc::new(source), volume_id, &survivor_root)?;
        let host_id = transport(&survivor)
            .host_id()
            .ok_or("shared external host absent")?;
        let host_exit = process_event(host_id, false)?;
        let (mut control, input) = UnixStream::pair()?;
        // This deadline only fails a broken child protocol; no elapsed time
        // establishes the pending request or supplies a passing observation.
        control.set_read_timeout(Some(Duration::from_secs(5)))?;
        let child = Command::new(std::env::current_exe()?)
            .args([OWNER_TEST, "--exact", "--nocapture"])
            .env(OWNER_CASE, if old { "old" } else { "hosted" })
            .env(OWNER_ROOT, &owner_root)
            .stdin(Stdio::from(OwnedFd::from(input)))
            .spawn()?;
        let mut child = OwnerProcess {
            child,
            root: owner_root.clone(),
        };
        let mut bytes = [[0; 8]; 4];
        control.read_exact(bytes.as_flattened_mut())?;
        let info = bytes.map(u64::from_ne_bytes);
        if old {
            assert_eq!(info[0], 0);
        } else {
            assert_eq!(
                info[0],
                u64::from(host_id),
                "owners must share one host process"
            );
        }
        let cleanup_exit = process_event(u32::try_from(info[1])?, false)?;
        let thread_exit = process_event(u32::try_from(info[3])?, true)?;
        let connection = std::path::PathBuf::from(format!("/sys/fs/fuse/connections/{}", info[2]));
        control.write_all(&[1])?;
        let mut pending = [0];
        control.read_exact(&mut pending)?;
        assert_eq!(pending, [1]);
        child.kill()?;
        wait_exit(&thread_exit)?;
        if old {
            // The original device is still owned by the killed process's
            // blocked caller, even though its mount-free thread has exited.
            assert!(
                child.try_wait()?.is_none(),
                "old-path negative control exited before its pending request was answered"
            );
            let waiting: u64 = std::fs::read_to_string(connection.join("waiting"))?
                .trim()
                .parse()?;
            assert!(
                waiting > 0,
                "negative control lost the actual unanswered kernel request"
            );
            std::fs::write(connection.join("abort"), b"1")?;
        }
        let exited = acyclic_native_runtime::wait_child_until(
            &mut child,
            std::time::Instant::now() + Duration::from_secs(5),
        );
        if exited.is_err() {
            // A broken positive control still retires its real connection,
            // keeping failed test runs from pinning the validation machine.
            let _ = std::fs::write(connection.join("abort"), b"1");
            let _ = child.kill();
            let _ = child.wait();
        }
        assert!(!exited?.success(), "owner must terminate from SIGKILL");
        wait_exit(&cleanup_exit)?;
        assert!(
            !connection.try_exists()?,
            "dead owner's connection survived physical cleanup"
        );
        std::fs::write(survivor_root.join("still-live"), b"independent owner")?;
        assert_eq!(
            std::fs::read(survivor_root.join("still-live"))?,
            b"independent owner"
        );
        assert!(survivor.stop()?);
        wait_exit(&host_exit)?;
    }
    Ok(())
}

/// A failed proof must not strand the deliberately blocked old-path owner.
/// Discover its actual connection from kernel metadata even if startup failed
/// before the child could send its information record.
struct OwnerProcess {
    child: std::process::Child,
    root: std::path::PathBuf,
}

impl std::ops::Deref for OwnerProcess {
    type Target = std::process::Child;
    fn deref(&self) -> &Self::Target {
        &self.child
    }
}

impl std::ops::DerefMut for OwnerProcess {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.child
    }
}

impl Drop for OwnerProcess {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(Some(_))) {
            return;
        }
        if let Ok(table) = std::fs::read_to_string(format!("/proc/{}/mountinfo", self.child.id())) {
            for line in table.lines() {
                let Some((mount, filesystem)) = line.split_once(" - ") else {
                    continue;
                };
                if !filesystem.starts_with("fuse ") {
                    continue;
                }
                let fields = mount.split_whitespace().collect::<Vec<_>>();
                if fields
                    .get(4)
                    .is_some_and(|path| Path::new(path) == self.root)
                    && let Some((major, minor)) =
                        fields.get(2).and_then(|device| device.split_once(':'))
                    && let (Ok(major), Ok(minor)) = (major.parse::<u64>(), minor.parse::<u64>())
                {
                    let connection = (major << 20) | minor;
                    let _ = std::fs::write(
                        format!("/sys/fs/fuse/connections/{connection}/abort"),
                        b"1",
                    );
                }
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

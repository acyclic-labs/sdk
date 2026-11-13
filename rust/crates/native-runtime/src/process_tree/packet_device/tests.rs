//! Real host handoff at the bound-but-not-listening and last-owner cuts.

use super::*;
use std::sync::mpsc::sync_channel;

const CASE: &str = "ACYCLIC_MOUNT_HOST_ADMISSION_PROOF";
const TEST: &str = "process_tree::packet_device::tests::concurrent_admission_survives_startup_and_last_owner_retirement";

fn device() -> io::Result<PacketDevice> {
    // Admission precedes mounting: the server must not read this descriptor.
    let file = rustix::fs::memfd_create(c"admission-proof", rustix::fs::MemfdFlags::CLOEXEC)?;
    PacketDevice::prepare(File::from(file), 4096)
}

fn exit_event(id: u32) -> io::Result<OwnedFd> {
    // SAFETY: the host has acknowledged its live PID; this binds its lifetime.
    let descriptor = unsafe { libc::syscall(libc::SYS_pidfd_open, id, 0) };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: pidfd_open returned this fresh descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(descriptor as i32) })
}

fn exited(event: &OwnedFd, timeout: i32) -> io::Result<bool> {
    let mut interest = libc::pollfd {
        fd: event.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // One OS event wait. A deadline only fails the proof, never establishes it.
    let count = unsafe { libc::poll(&raw mut interest, 1, timeout) };
    if count < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(count > 0)
}

#[test]
fn concurrent_admission_survives_startup_and_last_owner_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os(CASE).is_some_and(|case| case == "foreign-env") {
        let refused = admissions()?.into_iter().any(|admission| {
            admission
                .lock
                .try_lock(crate::FileLockMode::Exclusive)
                .is_err_and(|error| error.kind() == io::ErrorKind::WouldBlock)
        });
        assert!(
            refused,
            "a different HOME/XDG_RUNTIME_DIR must still contend on the original authority"
        );
        return Ok(());
    }
    if std::env::var_os(CASE).is_none() {
        let output = std::process::Command::new("unshare")
            .args([
                "--user",
                "--map-root-user",
                "--mount",
                "--net",
                "--propagation",
                "private",
                "--",
            ])
            .arg("/proc/self/fd/0")
            .stdin(File::open("/proc/self/exe")?)
            .args([TEST, "--exact", "--nocapture"])
            .env(CASE, "1")
            .output()?;
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return Ok(());
    }
    crate::private_file::initialize_isolated_user_runtime()?;

    // An attacker can reserve the former public pathname without any access
    // to the user's private root. Actual admission must ignore that entry.
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let namespace =
        |name| std::fs::metadata(format!("/proc/self/ns/{name}")).map(|file| file.ino());
    let poisoned = std::path::PathBuf::from(format!(
        "/tmp/acyclic-mount-host-{}-{}-{}.lock",
        crate::private_file::effective_user(),
        namespace("mnt")?,
        namespace("net")?
    ));
    std::os::unix::fs::symlink("/dev/null", &poisoned)?;
    struct Poisoned(std::path::PathBuf);
    impl Drop for Poisoned {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _poisoned = Poisoned(poisoned);

    // The former abstract socket name was public too. A different user can
    // bind it without entering the owner's private directory. Keep it bound
    // but not listening, so any regression fails instead of awaiting a peer.
    let reserved = socket()?;
    let mut legacy: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    legacy.sun_family = libc::AF_UNIX as _;
    let name = format!(
        "acyclic-packet-device-v1-{}-{}",
        crate::private_file::effective_user(),
        namespace("mnt")?
    );
    for (slot, byte) in legacy.sun_path[1..].iter_mut().zip(name.bytes()) {
        *slot = byte as _;
    }
    let length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + 1 + name.len();
    if unsafe {
        libc::bind(
            reserved.as_raw_fd(),
            (&raw const legacy).cast(),
            length as _,
        )
    } < 0
    {
        return Err(io::Error::last_os_error().into());
    }

    let private = tempfile::tempdir()?;
    std::fs::set_permissions(private.path(), std::fs::Permissions::from_mode(0o777))?;
    assert_eq!(
        admission_directory(private.path()).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied,
        "a public parent lets another user reserve the authority name"
    );
    std::fs::set_permissions(private.path(), std::fs::Permissions::from_mode(0o700))?;
    let alias = private.path().join(".acyclic-mount-host");
    std::os::unix::fs::symlink("/tmp", &alias)?;
    assert_eq!(
        admission_directory(private.path()).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied,
        "an alias cannot supply private directory custody"
    );
    std::fs::remove_file(alias)?;
    admission_directory(private.path())?;

    {
        let admissions = admissions()?;
        let output = with_admissions(&admissions, &mut || {
            std::process::Command::new("/proc/self/exe")
                .args([TEST, "--exact", "--nocapture"])
                .env(CASE, "foreign-env")
                .env("HOME", private.path())
                .env("XDG_RUNTIME_DIR", private.path())
                .output()
        })?;
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let (first, second) = std::thread::scope(|scope| -> Result<_, Box<dyn std::error::Error>> {
        let (bound, saw_bound) = sync_channel(1);
        let (listen, may_listen) = sync_channel(1);
        let leader = scope.spawn(move || {
            BEFORE_LISTEN.with_borrow_mut(|probe| {
                *probe = Some(Box::new(move || {
                    bound.send(()).unwrap();
                    may_listen.recv().unwrap();
                }));
            });
            device()
        });
        saw_bound.recv()?;
        // The exact old lock-free path sees the real bound socket before
        // listen and refuses the concurrent mount. No scheduler race is used.
        WITHOUT_ADMISSION_LOCK.set(true);
        let old = connect();
        WITHOUT_ADMISSION_LOCK.set(false);
        assert_eq!(old.unwrap_err().raw_os_error(), Some(libc::ECONNREFUSED));

        let (blocked, saw_blocked) = sync_channel(1);
        let follower = scope.spawn(move || {
            BEFORE_ADMISSION.with_borrow_mut(|probe| {
                *probe = Some(Box::new(move |file| {
                    let refusal = crate::FileLock::try_acquire(
                        file.try_clone().unwrap(),
                        crate::FileLockMode::Exclusive,
                    )
                    .unwrap_err();
                    assert_eq!(refusal.kind(), io::ErrorKind::WouldBlock);
                    blocked.send(()).unwrap();
                }));
            });
            device()
        });
        saw_blocked.recv()?;
        listen.send(())?;
        Ok((
            leader.join().map_err(|_| "leader panicked")??,
            follower.join().map_err(|_| "follower panicked")??,
        ))
    })?;
    assert_eq!(first.retirement.host_id(), second.retirement.host_id());
    let host = first.retirement.host_id();
    let exit = exit_event(host)?;
    drop(second);

    // Hold an independently opened admission lock before connect, then retire
    // the last original session. The receipt follows the host's atomic
    // retirement decision, so this is an actual cut at the former close race.
    let next = std::thread::scope(|scope| -> Result<_, Box<dyn std::error::Error>> {
        let (locked, saw_locked) = sync_channel(1);
        let (connect, may_connect) = sync_channel(1);
        let follower = scope.spawn(move || {
            BEFORE_CONNECT.with_borrow_mut(|probe| {
                *probe = Some(Box::new(move || {
                    locked.send(()).unwrap();
                    may_connect.recv().unwrap();
                }));
            });
            device()
        });
        saw_locked.recv()?;
        drop(first);
        assert!(
            !exited(&exit, 0)?,
            "pending connector must keep the host live"
        );
        connect.send(())?;
        Ok(follower.join().map_err(|_| "connector panicked")??)
    })?;
    assert_eq!(next.retirement.host_id(), host);
    drop(next);
    assert!(
        exited(&exit, 5000)?,
        "last close must retire the actual host"
    );
    runtime_appearance_and_read_only_home()?;
    Ok(())
}

fn runtime_appearance_and_read_only_home() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::ffi::OsStrExt;
    let home = crate::private_file::user_home_directory()?;
    assert!(home.parent().is_some());
    let target = std::ffi::CString::new(home.as_os_str().as_bytes())?;
    let flags = libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC;
    // This test already owns fresh user/mount/network namespaces. Supply an
    // account home without accessing or changing the original account files.
    if unsafe {
        libc::mount(
            c"tmpfs".as_ptr(),
            target.as_ptr(),
            c"tmpfs".as_ptr(),
            flags,
            c"mode=0700".as_ptr().cast(),
        )
    } < 0
    {
        return Err(io::Error::last_os_error().into());
    }
    let runtime = std::path::PathBuf::from(format!(
        "/run/user/{}",
        crate::private_file::effective_user()
    ));
    let parked = runtime.with_extension("parked");
    std::fs::rename(&runtime, &parked)?;
    let original = device()?;
    let host = original.retirement.host_id();
    let exit = exit_event(host)?;
    std::fs::rename(parked, runtime)?;
    // A login directory appears while the original home becomes read-only.
    // Original exclusion and IN_ACCESS wake delivery must still be usable.
    if unsafe {
        libc::mount(
            std::ptr::null(),
            target.as_ptr(),
            std::ptr::null(),
            flags | libc::MS_BIND | libc::MS_REMOUNT | libc::MS_RDONLY,
            std::ptr::null(),
        )
    } < 0
    {
        return Err(format!(
            "make original home mount read-only: {}",
            io::Error::last_os_error()
        )
        .into());
    }
    assert_eq!(
        File::create(home.join("read-only-proof"))
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EROFS),
        "the original home mount must actually refuse writes"
    );
    let joined = device()?;
    assert_eq!(
        joined.retirement.host_id(),
        host,
        "runtime appearance must join the original home-backed host"
    );
    drop(original);
    drop(joined);
    assert!(exited(&exit, 5000)?);
    // Its closed read-only endpoint must not block creating the next host in
    // the writable OS runtime directory, nor replace its permanent lock.
    let next = device()?;
    let exit = exit_event(next.retirement.host_id())?;
    drop(next);
    assert!(exited(&exit, 5000)?);
    Ok(())
}

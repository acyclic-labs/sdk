//! Views of one shared FUSE instance, against the live kernel.

use super::*;
use std::process::{Command, Stdio};
use std::sync::PoisonError;

const CHILD: &str = "ACYCLIC_FS_SHARED_FUSE_CHILD";
/// Negative control for the cost test: one mount per view, as before.
const UNSHARED: &str = "ACYCLIC_FS_SHARED_FUSE_UNSHARED";

fn tracking_views(count: usize) -> Result<Vec<MemorySource>, Box<dyn std::error::Error>> {
    let config = posix_config();
    memory_checkout(
        config,
        crate::model::CheckoutMode::tracking_transaction(),
        count,
    )?
    .into_iter()
    .map(|checkout| {
        crate::native_mount::adapter::tests::access::bind_test_access(
            MemorySource::new(
                Arc::new(crate::native_mount::SharedCheckout::new(checkout)),
                config,
            )?,
            vec![],
            None,
        )
        .map_err(Into::into)
    })
    .collect()
}

fn view(
    source: MemorySource,
    destination: &Path,
) -> Result<crate::NativeMountSession, crate::NativeMountError> {
    mount_view(source, destination, true)
}

fn mount_view(
    source: MemorySource,
    destination: &Path,
    shared: bool,
) -> Result<crate::NativeMountSession, crate::NativeMountError> {
    let volume_id = source.volume_id()?;
    crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id,
            destination: destination.to_path_buf(),
            writable: true,
            shared,
        },
        Arc::new(source),
    )
}

/// Mount points of this process's namespace at exactly `root`.
fn mounts_at(root: &Path) -> std::io::Result<usize> {
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let table = std::fs::read_to_string("/proc/self/mountinfo")?;
    Ok(table
        .lines()
        .filter_map(|line| line.split(' ').nth(4))
        .filter(|point| Path::new(point) == root)
        .count())
}

fn thread_names() -> std::io::Result<std::collections::BTreeMap<String, usize>> {
    let mut names = std::collections::BTreeMap::new();
    for task in std::fs::read_dir("/proc/self/task")? {
        let name = std::fs::read_to_string(task?.path().join("comm"))?;
        *names.entry(name.trim().to_owned()).or_default() += 1;
    }
    Ok(names)
}

fn entries(path: &str) -> std::io::Result<usize> {
    Ok(std::fs::read_dir(path)?.count())
}

/// Runs one exact test of this binary in a child process, so process-wide
/// counts see only that test and a kill there is a real crash.
fn child(test: &str, root: &Path) -> Command {
    let mut command = Command::new(std::env::current_exe().expect("test binary"));
    command
        .args([test, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD, root)
        .env_remove(UNSHARED)
        .stdin(Stdio::null());
    command
}

#[test]
pub(super) fn linux_shared_views_are_independent_namespaces_of_one_mount()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("views");
    std::fs::create_dir(&root)?;
    let mut sources = tracking_views(2)?.into_iter();
    let (first, second) = (sources.next().ok_or("one")?, sources.next().ok_or("two")?);
    let mut a = view(first, &root.join("a"))?;
    let mut b = view(second, &root.join("b"))?;
    assert_eq!(mounts_at(&root)?, 1, "both views share one mount");
    assert_eq!(listing(&root)?, ["a", "b"]);

    std::fs::write(root.join("a/file"), b"only in a")?;
    assert_eq!(std::fs::read(root.join("a/file"))?, b"only in a");
    assert!(!root.join("b/file").exists(), "views are independent");
    std::fs::create_dir(root.join("b/directory"))?;
    assert_eq!(listing(&root.join("b"))?, ["directory"]);

    let crossed = std::fs::rename(root.join("a/file"), root.join("b/file"))
        .expect_err("a rename across views");
    assert_eq!(crossed.raw_os_error(), Some(libc::EXDEV));
    let crossed = std::fs::hard_link(root.join("a/file"), root.join("b/linked"))
        .expect_err("a link across views");
    assert_eq!(crossed.raw_os_error(), Some(libc::EXDEV));
    let refused = std::fs::create_dir(root.join("c")).expect_err("the root lists views only");
    assert_eq!(refused.raw_os_error(), Some(libc::EACCES));

    // A descriptor held inside a retired view never reaches another view.
    let held = std::fs::File::open(root.join("a"))?;
    assert!(a.stop()?);
    assert_eq!(listing(&root)?, ["b"]);
    assert!(!root.join("a").exists());
    let late = rustix::fs::openat(
        &held,
        "file",
        rustix::fs::OFlags::RDONLY,
        rustix::fs::Mode::empty(),
    )
    .expect_err("a lookup in a retired view");
    assert_eq!(late.raw_os_error(), libc::ESTALE);
    drop(held);
    assert_eq!(listing(&root.join("b"))?, ["directory"]);
    assert_eq!(mounts_at(&root)?, 1, "the other view keeps the mount");

    let (worker, physical, views) = {
        let state = b
            .driver
            .as_ref()
            .ok_or("shared driver")?
            .state
            .lock()
            .map_err(|_| "shared state poisoned")?;
        let instance = &state.shared.as_ref().ok_or("shared view")?._instance;
        let (worker, physical) = instance.physical_owner();
        (worker, physical, Arc::clone(&instance.views))
    };
    assert_ne!(physical, std::thread::current().id());
    assert!(worker.upgrade().is_some(), "the native worker is live");
    // Keep the original kernel connection busy through last-view retirement.
    // Lazy namespace detach must still stop and join its packet worker before
    // releasing the root registration and fence.
    let held_last = std::fs::File::open(root.join("b"))?;
    assert!(b.stop()?);
    let worker_retired = worker.upgrade().is_none();
    // Release the kernel borrower before reporting a failed control.
    drop(held_last);
    assert!(
        worker_retired,
        "shared root releases custody only after worker exit"
    );
    assert_eq!(
        views.retirement_thread.get(),
        Some(&physical),
        "the existing physical service owns root cleanup"
    );
    assert_eq!(mounts_at(&root)?, 0, "the last view's retirement unmounts");
    let [fence, ..] = crate::native_mount_control_paths(&root.canonicalize()?)?;
    assert!(!fence.exists(), "the unmounted root releases its fence");
    Ok(())
}

#[test]
pub(super) fn linux_changes_around_a_shared_view_reach_the_kernel()
-> Result<(), Box<dyn std::error::Error>> {
    let (mounted, around) = shared_sources(posix_config())?;
    let file = name("file");
    crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.create_file(&file, FileMetadata::default(), None, work)
    })?;
    around.write_range(&file, 0, Bytes::from_static(b"first"))?;
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("views");
    std::fs::create_dir(&root)?;
    let (_, idle) = shared_sources(posix_config())?;
    // A second view shares the invalidator and must not hold up the first.
    let mut other = view(idle, &root.join("other"))?;
    let mut session = view(mounted, &root.join("view"))?;
    let path = root.join("view/file");
    assert_eq!(std::fs::read(&path)?, b"first");
    around.write_range(&file, 0, Bytes::from_static(b"other"))?;
    session.revalidate(&fuse_fixture_work())?;
    assert_eq!(std::fs::read(&path)?, b"other");
    crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.create_file(&name("added"), FileMetadata::default(), None, work)
    })?;
    session.revalidate(&fuse_fixture_work())?;
    assert_eq!(listing(&root.join("view"))?, ["added", "file"]);
    assert!(session.stop()?);
    assert!(other.stop()?);
    Ok(())
}

#[test]
pub(super) fn linux_shared_views_cost_no_thread_descriptor_or_mount()
-> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os(CHILD).is_some() {
        return shared_view_costs_in_this_process();
    }
    for unshared in [false, true] {
        let temporary = tempfile::tempdir()?;
        let mut command = child(
            "native_mount::fuse::tests::shared::linux_shared_views_cost_no_thread_descriptor_or_mount",
            temporary.path(),
        );
        if unshared {
            command.env(UNSHARED, "1");
        }
        let output = command.output()?;
        let text = String::from_utf8_lossy(&output.stdout);
        let expected = if unshared {
            "unshared view costs: grow"
        } else {
            "shared view costs: ok"
        };
        assert!(
            output.status.success() && text.contains(expected),
            "child failed: {text}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

/// Child half of `linux_shared_views_cost_no_thread_descriptor_or_mount`.
fn shared_view_costs_in_this_process() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::var_os(CHILD).ok_or("shared-view child root is absent")?;
    let root = Path::new(&base).join("views");
    std::fs::create_dir(&root)?;
    let root = root.canonicalize()?;
    let shared = std::env::var_os(UNSHARED).is_none();
    let start = |source, index: usize| -> Result<_, Box<dyn std::error::Error>> {
        let destination = root.join(format!("{index}"));
        if !shared {
            std::fs::create_dir(&destination)?;
        }
        Ok(mount_view(source, &destination, shared)?)
    };
    let mut sources = tracking_views(9)?.into_iter();
    let mut views = Vec::new();
    // The first view starts the instance; measure views after it, at N and 2N.
    views.push(start(sources.next().ok_or("source")?, 0)?);
    let measure = || -> std::io::Result<(usize, usize, usize)> {
        let threads = thread_names()?;
        // The session runner starts its fixed callback pool asynchronously.
        // Account for that one instance separately from per-view resources.
        let packet_threads = threads.get("acyclic-fuse").copied().unwrap_or(0);
        if shared {
            assert!(
                packet_threads <= 1 + crate::native_mount::CALLBACK_THREADS as usize,
                "one instance owns one runner and its fixed callback pool: {threads:?}"
            );
        }
        // Only this test's mounts: other processes share the namespace.
        let table = std::fs::read_to_string("/proc/self/mountinfo")?;
        let mounts = table
            .lines()
            .filter_map(|line| line.split(' ').nth(4))
            .filter(|point| Path::new(point).starts_with(&root))
            .count();
        Ok((
            threads.values().sum::<usize>() - packet_threads,
            entries("/proc/self/fd")?,
            mounts,
        ))
    };
    let base = measure()?;
    for (index, source) in sources.by_ref().take(4).enumerate() {
        views.push(start(source, index + 1)?);
    }
    let at_n = measure()?;
    for (index, source) in sources.enumerate() {
        views.push(start(source, index + 5)?);
    }
    let at_2n = measure()?;
    assert_eq!(views.len(), 9);
    let views_listed = listing(&root)?
        .iter()
        .filter(|name| !name.as_encoded_bytes().starts_with(b"."))
        .count();
    assert_eq!(views_listed, 9);
    if !shared {
        // Each view's own mount costs threads, a descriptor and a mount.
        assert!(
            at_n.0 > base.0 && at_n.1 > base.1 && at_n.2 > base.2,
            "{base:?} {at_n:?}"
        );
        assert!(
            at_2n.0 > at_n.0 && at_2n.1 > at_n.1 && at_2n.2 > at_n.2,
            "{at_n:?} {at_2n:?}"
        );
        for mut view in views {
            assert!(view.stop()?);
        }
        println!("unshared view costs: grow");
        return Ok(());
    }
    // Further views add no resources beyond the original fixed packet pool.
    assert_eq!(at_n, base, "4 more views: {:?}", thread_names()?);
    assert_eq!(at_2n, base, "8 more views: {:?}", thread_names()?);
    assert_eq!(base.2, 1);
    for mut view in views {
        assert!(view.stop()?);
    }
    assert_eq!(mounts_at(&root)?, 0);
    println!("shared view costs: ok");
    Ok(())
}

#[test]
pub(super) fn linux_server_loss_and_owner_death_retire_the_original_connection_and_namespace()
-> Result<(), Box<dyn std::error::Error>> {
    if let Some(mode) = std::env::var_os(CHILD) {
        if mode == "server-loss" {
            return original_connection_retires_after_server_loss();
        }
        return shared_view_owner_until_killed();
    }
    let temporary = tempfile::tempdir()?;
    let status = child(
        "native_mount::fuse::tests::shared::linux_server_loss_and_owner_death_retire_the_original_connection_and_namespace",
        temporary.path(),
    )
    .env(CHILD, "server-loss")
    .status()?;
    assert!(status.success(), "isolated server-loss control: {status}");
    for killed_action in [false, true] {
        owner_exit_namespace(killed_action)?;
    }
    Ok(())
}

fn original_connection_retires_after_server_loss() -> Result<(), Box<dyn std::error::Error>> {
    // A live facade and its notification sinks must not retain a device after
    // the original packet server has stopped. Check physical ownership before
    // issuing a lookup, so the old-source negative cannot strand a kernel call.
    let device_count = || -> std::io::Result<usize> {
        let mut count = 0;
        for entry in std::fs::read_dir("/proc/self/fd")? {
            if std::fs::read_link(entry?.path())
                .is_ok_and(|target| target == Path::new("/dev/fuse"))
            {
                count += 1;
            }
        }
        Ok(count)
    };
    let before = device_count()?;
    let server_root = tempfile::tempdir()?;
    let (source, _) = shared_sources(posix_config())?;
    let mut live = mount_view(source, server_root.path(), false)?;
    let io = {
        let driver = live.driver.as_ref().ok_or("live FUSE driver")?;
        driver
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .device
            .as_ref()
            .cloned()
            .ok_or("live FUSE transport")?
    };
    assert_eq!(device_count()?, before + 1);
    io.stop();
    let remaining = device_count()?;
    // Always clean up this exact mount, including the negative control.
    if remaining != before {
        let _ = live.stop();
        assert_eq!(remaining, before, "the stopped server retained /dev/fuse");
    }
    let error = std::fs::metadata(server_root.path().join("after-server-exit"))
        .expect_err("a terminated connection refuses a fresh lookup");
    assert!(matches!(
        error.raw_os_error(),
        Some(libc::ENOTCONN | libc::ENOENT)
    ));
    assert!(live.stop()?);
    assert_eq!(mounts_at(server_root.path())?, 0);

    Ok(())
}

fn owner_exit_namespace(killed_action: bool) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{BufRead as _, BufReader};
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("views");
    let mut owner = child(
        "native_mount::fuse::tests::shared::linux_server_loss_and_owner_death_retire_the_original_connection_and_namespace",
        temporary.path(),
    )
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::inherit())
    .spawn()?;
    let mut lines = BufReader::new(owner.stdout.take().ok_or("child stdout")?).lines();
    let cleanup_pid = loop {
        let line = lines
            .next()
            .ok_or("the owner exited before its view was live")??;
        // The test host can prefix the first output line with its test name.
        if let Some((_, pid)) = line.rsplit_once("shared view owner: ready ") {
            break pid.parse::<u32>()?;
        }
    };
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
    // The original owner still holds this PID; registration precedes its
    // kill, so PID reuse cannot select another process's terminal event.
    // SAFETY: pidfd_open takes the retained original PID and no pointers.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, cleanup_pid, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: pidfd_open returned this fresh owned descriptor.
    let cleanup = unsafe { OwnedFd::from_raw_fd(i32::try_from(fd)?) };
    // Remove the actual cleanup producer through its retained kernel identity.
    // This negative control cannot accidentally signal a reused numeric PID.
    if killed_action {
        // SAFETY: the retained pidfd names the original action; SIGKILL carries
        // no siginfo pointer and flags zero selects the ordinary delivery.
        if unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                cleanup.as_raw_fd(),
                libc::SIGKILL,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        } < 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    owner.kill()?;
    owner.wait()?;
    let mut terminal = libc::pollfd {
        fd: cleanup.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        // SAFETY: one initialized record, with its original pidfd retained.
        if unsafe { libc::poll(&mut terminal, 1, -1) } >= 0 {
            break;
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error.into());
        }
    }
    let remaining = mounts_at(&root)?;
    let [fence, ..] = crate::native_mount_control_paths(&root)?;
    assert!(fence.exists(), "the original fence remains recoverable");
    println!(
        "owner-exit action control: killed_action={killed_action}, remaining_mounts={remaining}"
    );
    if remaining != 0 {
        // The planted producer-loss control must not itself abandon a mount.
        let (source, _) = shared_sources(posix_config())?;
        let mut recovered = view(source, &root.join("negative-cleanup"))?;
        assert!(recovered.stop()?);
    }
    assert_eq!(
        remaining == 0,
        !killed_action,
        "the killed owner's namespace outlived its cleanup action"
    );
    let (source, _) = shared_sources(posix_config())?;
    let mut session = view(source, &root.join("after"))?;
    assert_eq!(mounts_at(&root)?, 1, "the successor owns its new mount");
    assert_eq!(listing(&root)?, ["after"]);
    assert!(session.stop()?);
    assert_eq!(mounts_at(&root)?, 0);
    assert!(!fence.exists());
    Ok(())
}

/// Child half of the original connection and namespace retirement guard:
/// holds a view until its parent kills it.
fn shared_view_owner_until_killed() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::var_os(CHILD).ok_or("shared-view child root is absent")?;
    let root = Path::new(&base).join("views");
    std::fs::create_dir(&root)?;
    let (source, _) = shared_sources(posix_config())?;
    let session = view(source, &root.join("crashed"))?;
    let cleanup = session
        .driver
        .as_ref()
        .ok_or("live driver")?
        .state
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .shared
        .as_ref()
        .and_then(|view| view.exit_action_id())
        .ok_or("original owner-exit action")?;
    println!("shared view owner: ready {cleanup}");
    loop {
        std::thread::park();
    }
}

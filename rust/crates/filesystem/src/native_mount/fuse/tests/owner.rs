//! Blocking native access must not require the embedder's Tokio executor.

use super::super::invalidation::KernelCacheNotifier;
use super::*;

#[test]
fn current_thread_embedder_writes_through_its_own_view() -> Result<(), Box<dyn std::error::Error>> {
    if acyclic_native_runtime::private_file::with_isolated_user_runtime(
        "native_mount::fuse::tests::owner::current_thread_embedder_writes_through_its_own_view",
    )? {
        return Ok(());
    }
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

#[test]
fn missing_cached_inode_reports_errno_and_keeps_the_mount_live()
-> Result<(), Box<dyn std::error::Error>> {
    if acyclic_native_runtime::private_file::with_isolated_user_runtime(
        "native_mount::fuse::tests::owner::missing_cached_inode_reports_errno_and_keeps_the_mount_live",
    )? {
        return Ok(());
    }
    crate::initialize_native_mount_services()?;
    let (source, _) = shared_sources(posix_config())?;
    let volume = source.volume_id()?;
    let temporary = tempfile::tempdir()?;
    let mut session = mount(Arc::new(source), volume, temporary.path())?;
    let notifier = super::super::notification::CacheNotifier::new(transport(&session));
    let error = notifier
        .inval_attributes(u64::MAX)
        .expect_err("inode is not cached");
    assert_eq!(error.raw_os_error(), Some(libc::ENOENT));
    let path = temporary.path().join("after-cache-miss");
    std::fs::write(&path, b"connection remains live")?;
    assert_eq!(std::fs::read(path)?, b"connection remains live");
    assert!(session.stop()?);
    Ok(())
}

#[test]
fn entry_invalidation_cannot_block_the_lookup_that_releases_its_inode_lock()
-> Result<(), Box<dyn std::error::Error>> {
    if acyclic_native_runtime::private_file::with_isolated_user_runtime(
        "native_mount::fuse::tests::owner::entry_invalidation_cannot_block_the_lookup_that_releases_its_inode_lock",
    )? {
        return Ok(());
    }
    crate::initialize_native_mount_services()?;
    let (inner, _) = shared_sources(posix_config())?;
    let volume = inner.volume_id()?;
    let (entered, pending) = sync_channel(1);
    let (release, released) = sync_channel(1);
    let source = Arc::new(super::mount::GatedSource {
        inner: Arc::new(inner),
        gated_lookup: b"blocked".to_vec(),
        gated_directory: Vec::new(),
        entered,
        released: Mutex::new(released),
    });
    let root = tempfile::tempdir()?;
    let mut session = mount(source, volume, root.path())?;
    let io = transport(&session);
    assert!(
        io.connection.get().is_some(),
        "failed controls require original kernel abort custody"
    );
    let parent = session
        .driver
        .as_ref()
        .ok_or("native driver absent")?
        .core
        .state_to_finish()
        .root_inode;
    struct Cleanup {
        io: Arc<super::super::host_io::HostIo>,
        release: Option<SyncSender<()>>,
        armed: bool,
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            // Closing the source gate and aborting the actual connection
            // happens before scoped joins, including a failing inline control.
            drop(self.release.take());
            if self.armed {
                if let Some(connection) = self.io.connection.get() {
                    let _ = connection.abort();
                }
            }
        }
    }
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let mut cleanup = Cleanup {
            io: Arc::clone(&io),
            release: Some(release),
            armed: true,
        };
        let (lookup_done, lookup_result) = sync_channel(1);
        let path = root.path().join("blocked");
        scope.spawn(move || {
            let _ = lookup_done.send(std::fs::metadata(path));
        });
        pending.recv_timeout(Duration::from_secs(5))?;
        drop(pending);
        let (submitted, submission) = sync_channel(1);
        let (completed, completion) = sync_channel(1);
        let notifier = super::super::notification::CacheNotifier::new(
            Arc::clone(&io) as Arc<dyn super::super::notification::PacketSink>
        );
        scope.spawn(move || {
            super::super::host_io::BEFORE_RECEIPT.with_borrow_mut(|hook| {
                *hook = Some(Box::new(move || {
                    let _ = submitted.send(());
                }));
            });
            let _ = completed.send(notifier.inval_entry(parent, b"blocked"));
        });
        // This receipt means every notification byte precedes the eventual
        // lookup reply in the same ordered stream. No sleep guesses whether
        // the physical notifier has reached the kernel's parent inode lock.
        submission.recv_timeout(Duration::from_secs(5))?;
        drop(cleanup.release.take());
        assert_eq!(
            lookup_result
                .recv_timeout(Duration::from_secs(5))?
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
        if let Err(error) = completion.recv_timeout(Duration::from_secs(5))? {
            assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        }
        cleanup.armed = false;
        Ok(())
    })?;
    assert!(session.stop()?);
    Ok(())
}

#[test]
fn expired_pending_reply_keeps_unrelated_requests_live() -> Result<(), Box<dyn std::error::Error>> {
    if acyclic_native_runtime::private_file::with_isolated_user_runtime(
        "native_mount::fuse::tests::owner::expired_pending_reply_keeps_unrelated_requests_live",
    )? {
        return Ok(());
    }
    crate::initialize_native_mount_services()?;
    for old_abort in [false, true] {
        let (source, _) = shared_sources(posix_config())?;
        let volume = source.volume_id()?;
        let root = tempfile::tempdir()?;
        let mut session = mount(Arc::new(source), volume, root.path())?;
        let io = transport(&session);
        assert!(
            io.connection.get().is_some(),
            "negative control needs actual abort custody"
        );
        let outcome = super::super::host_session::publish_expired_reply_for_test(&io, old_abort);
        let path = root.path().join("after-expired-reply");
        if old_abort {
            assert_eq!(outcome.unwrap_err().raw_os_error(), Some(libc::ENOENT));
            assert_eq!(
                std::fs::write(&path, b"must be disconnected")
                    .unwrap_err()
                    .raw_os_error(),
                Some(libc::ENOTCONN)
            );
        } else {
            outcome?;
            std::fs::write(&path, b"independent request")?;
            assert_eq!(std::fs::read(path)?, b"independent request");
        }
        assert!(session.stop()?);
    }
    Ok(())
}

#[test]
fn rejected_opens_reclaim_original_handles_and_credit() -> Result<(), Box<dyn std::error::Error>> {
    use super::super::packet::{PacketInput, PacketRequest};
    use super::super::packet_router::PacketRouter;
    use super::super::packet_session::{MAXIMUM_WRITE, PacketSession};
    use super::super::{FuseProjection, NativeMountWorkScope};

    if acyclic_native_runtime::private_file::with_isolated_user_runtime(
        "native_mount::fuse::tests::owner::rejected_opens_reclaim_original_handles_and_credit",
    )? {
        return Ok(());
    }
    crate::initialize_native_mount_services()?;
    for shared in [false, true] {
        for old_delivery in [false, true] {
            let (source, _) = shared_sources(posix_config())?;
            let volume = source.volume_id()?;
            let source = Arc::new(source);
            let root = tempfile::tempdir()?;
            let mut mounted = mount(source.clone(), volume, root.path())?;
            std::fs::write(root.path().join("retained"), b"original source")?;
            let io = transport(&mounted);
            let work = fuse_fixture_work();
            let (core, router, _view) = if shared {
                let views = super::super::shared::SharedViews::new(0, 0);
                let view = views.insert(b"original", |base| {
                    projection_core_at(
                        source.clone(),
                        crate::WorkBudget::UNBOUNDED,
                        work.cancellation(),
                        base,
                    )
                })?;
                (
                    Arc::clone(view.core()),
                    PacketRouter::Shared(views),
                    Some(view),
                )
            } else {
                let core = projection_core(
                    source.clone(),
                    crate::WorkBudget::UNBOUNDED,
                    work.cancellation(),
                )?;
                (
                    Arc::clone(&core),
                    PacketRouter::Single(FuseProjection { core }),
                    None,
                )
            };
            let _active = NativeMountWorkScope::new(&core, &work)?;
            let projection = FuseProjection {
                core: Arc::clone(&core),
            };
            let parent = core.state_to_finish().root_inode;
            let entry = core
                .finish(core.request(|work| {
                    projection.lookup_entry(parent, std::ffi::OsStr::new("retained"), work)
                }))
                .map_err(std::io::Error::from_raw_os_error)?;
            let inode = entry.attr.ino.0;
            let protocol = PacketSession::backend(
                router,
                MAXIMUM_WRITE,
                std::num::NonZeroU32::new(4096).ok_or("nonzero page size")?,
            )?;
            let nodes_before = core.state_to_finish().by_inode.len();
            let mut response = [0; super::super::packet::OUTPUT_HEADER + 4096];
            for opcode in [14_u32, 35, 1, 44] {
                let mut body = Vec::new();
                match opcode {
                    14 => body.extend_from_slice(&[0; 8]),
                    35 => {
                        for field in [(libc::O_CREAT | libc::O_RDWR) as u32, 0o644, 0, 0] {
                            body.extend_from_slice(&field.to_ne_bytes());
                        }
                        body.extend_from_slice(b"rejected-created\0");
                    }
                    1 => body.extend_from_slice(b"rejected-created\0"),
                    44 => {
                        body.extend_from_slice(&[0; 16]);
                        body.extend_from_slice(&4096_u32.to_ne_bytes());
                        body.extend_from_slice(&[0; 20]);
                    }
                    _ => unreachable!(),
                }
                let mut bytes = vec![0; 40];
                bytes[4..8].copy_from_slice(&opcode.to_ne_bytes());
                bytes[8..16].copy_from_slice(&u64::MAX.to_ne_bytes());
                bytes[16..24]
                    .copy_from_slice(&(if opcode == 14 { inode } else { parent }).to_ne_bytes());
                bytes.extend_from_slice(&body);
                let length = u32::try_from(bytes.len())?;
                bytes[..4].copy_from_slice(&length.to_ne_bytes());
                let request =
                    PacketRequest::parse(&bytes).map_err(std::io::Error::from_raw_os_error)?;
                let mut reclaimed_bytes = None;
                for attempt in 0..4 {
                    super::super::host_session::publish_expired_packet_for_test(
                        &io,
                        &protocol,
                        &request,
                        &mut response,
                        old_delivery,
                    )?;
                    let mut header = PacketInput::new(&response);
                    header.u32().map_err(std::io::Error::from_raw_os_error)?;
                    assert_eq!(
                        header.u32().map_err(std::io::Error::from_raw_os_error)?,
                        0,
                        "the original source operation must succeed: shared={shared} old_delivery={old_delivery} opcode={opcode} attempt={attempt}"
                    );
                    let state = core.state_to_finish();
                    if old_delivery {
                        assert!(
                            state.by_inode.len() > nodes_before
                                || state.by_inode.values().any(|entry| {
                                    entry.lookup_references > 1 || entry.open_handles > 0
                                }),
                            "former delivery policy must leak kernel custody"
                        );
                    } else {
                        assert!(state.files.is_empty(), "rejected reply retained its handle");
                        assert_eq!(
                            state.by_inode.len(),
                            nodes_before,
                            "rejected entry retained a lookup reference"
                        );
                        assert!(
                            state
                                .by_inode
                                .values()
                                .all(|entry| entry.open_handles == 0 && entry.write_handles == 0)
                        );
                    }
                    drop(state);
                    // First access can fund a source/cache page. Subsequent
                    // rejected opens must return the original allocation credit.
                    if !old_delivery && opcode == 14 {
                        let bytes = work.live_allocation_bytes();
                        assert_eq!(
                            *reclaimed_bytes.get_or_insert(bytes),
                            bytes,
                            "rejected OPEN leaked original credit at attempt {attempt}"
                        );
                    }
                }
                if old_delivery {
                    let mut state = core.state_to_finish();
                    let handles = state
                        .files
                        .iter()
                        .map(|(&handle, file)| (file.inode, handle))
                        .collect::<Vec<_>>();
                    for (inode, handle) in handles {
                        state
                            .discard_file_handle(inode, handle)
                            .map_err(std::io::Error::from_raw_os_error)?;
                    }
                    let references = state
                        .by_inode
                        .iter()
                        .filter(|(other, _)| **other != parent)
                        .map(|(&other, entry)| {
                            (other, entry.lookup_references - u64::from(other == inode))
                        })
                        .collect::<Vec<_>>();
                    for (inode, count) in references {
                        state.release_lookup_reference(inode, count);
                    }
                }
            }
            assert!(!protocol.closed());
            assert!(
                std::fs::read(root.path().join("rejected-created"))?.is_empty(),
                "rejected delivery must retain the completed CREATE mutation"
            );
            std::fs::write(root.path().join("after-rejections"), b"mount remains live")?;
            assert_eq!(
                std::fs::read(root.path().join("after-rejections"))?,
                b"mount remains live"
            );
            assert!(mounted.stop()?);
        }
    }
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
            // SAFETY: the controller is launched below in a fresh private
            // mount namespace before constructing any native service.
            unsafe {
                acyclic_native_runtime::private_file::initialize_isolated_user_runtime()?;
            }
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
        .arg("/proc/self/fd/0")
        .stdin(std::fs::File::open("/proc/self/exe")?)
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
    // Release source work before session worker joining on all error paths.
    let release_on_exit = release;
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
        if !old {
            let transport = transport(&session);
            let parent = session
                .driver
                .as_ref()
                .expect("native driver")
                .core
                .state_to_finish()
                .root_inode;
            let (submitted, submission) = sync_channel(1);
            std::thread::spawn(move || {
                super::super::host_io::BEFORE_RECEIPT.with_borrow_mut(|hook| {
                    *hook = Some(Box::new(move || {
                        let _ = submitted.send(());
                    }));
                });
                let notifier = super::super::notification::CacheNotifier::new(transport);
                let _ = notifier.inval_entry(parent, b"blocked");
            });
            // The kernel request still owns the directory lock. Send the
            // death barrier only after the complete notification frame is on
            // the host transport, ahead of any retirement refusal reply.
            submission
                .recv_timeout(Duration::from_secs(2))
                .map_err(std::io::Error::other)?;
        }
        control.write_all(&[1])?;
        control.read_exact(&mut go)
    });
    // Error cleanup must release the source before joining its packet workers.
    drop(release_on_exit);
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
    for (case, (old, keep_survivor)) in [(false, false), (false, true), (true, true)]
        .into_iter()
        .enumerate()
    {
        let survivor_root = root.join(format!("survivor-{case}"));
        let owner_root = root.join(format!("owner-{case}"));
        std::fs::create_dir(&survivor_root)?;
        std::fs::create_dir(&owner_root)?;

        let (mut control, input) = UnixStream::pair()?;
        // This deadline only fails a broken child protocol; no elapsed time
        // establishes the pending request or supplies a passing observation.
        control.set_read_timeout(Some(Duration::from_secs(5)))?;
        let child = Command::new("/proc/self/exe")
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
        // The child creates the host. Exercise both its sole-owner death and
        // its death while a later owner still relies on that same process.
        let mut survivor = if keep_survivor {
            let (source, _) = shared_sources(posix_config())?;
            let volume_id = source.volume_id()?;
            Some(mount(Arc::new(source), volume_id, &survivor_root)?)
        } else {
            None
        };
        let host_id = if let Some(session) = survivor.as_ref() {
            transport(session)
                .host_id()
                .ok_or("shared external host absent")?
        } else {
            u32::try_from(info[0])?
        };
        let host_exit = process_event(host_id, false)?;
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
        if let Some(survivor) = survivor.as_mut() {
            std::fs::write(survivor_root.join("still-live"), b"independent owner")?;
            assert_eq!(
                std::fs::read(survivor_root.join("still-live"))?,
                b"independent owner"
            );
            assert!(survivor.stop()?);
        }
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

#[cfg(not(loom))]
#[derive(Default)]
pub(in crate::native_mount::fuse) struct PublicationTest {
    pub(in crate::native_mount::fuse) pause: Option<(SyncSender<()>, Receiver<()>)>,
    pub(in crate::native_mount::fuse) queued: Option<SyncSender<()>>,
}

#[cfg(not(loom))]
pub(in crate::native_mount::fuse) fn pause_invalidations(core: &ProjectionCore) {
    let pause = core
        .publication_test
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .pause
        .take();
    if let Some((entered, release)) = pause {
        let _ = entered.send(());
        let _ = release.recv();
    }
}

#[cfg(not(loom))]
#[test]
fn an_open_waits_for_its_cached_size_without_holding_a_callback_worker()
-> Result<(), Box<dyn std::error::Error>> {
    if acyclic_native_runtime::private_file::with_isolated_user_runtime(
        "native_mount::fuse::tests::owner::an_open_waits_for_its_cached_size_without_holding_a_callback_worker",
    )? {
        return Ok(());
    }
    crate::initialize_native_mount_services()?;
    for retire in [false, true] {
        let (source, around) = shared_sources(posix_config())?;
        let path = name("versioned");
        crate::native_mount::adapter::boundary(around.cancellation(), |work| {
            around.create_file(&path, FileMetadata::default(), None, work)
        })?;
        let volume = source.volume_id()?;
        let destination = tempfile::tempdir()?;
        let mut session = mount(Arc::new(source), volume, destination.path())?;
        let mounted = destination.path().join("versioned");
        assert!(std::fs::read(&mounted)?.is_empty());
        let core = Arc::clone(&session.driver.as_ref().ok_or("driver absent")?.core);
        let io = transport(&session);
        let (entered, paused) = sync_channel(1);
        let (release, released) = sync_channel(1);
        let (queued, observed) = sync_channel(1);
        {
            let mut hooks = core.publication_test.lock().unwrap();
            hooks.pause = Some((entered, released));
            hooks.queued = Some(queued);
        }
        struct Cleanup {
            release: Option<SyncSender<()>>,
            io: Arc<super::super::host_io::HostIo>,
            armed: bool,
        }
        impl Drop for Cleanup {
            fn drop(&mut self) {
                drop(self.release.take());
                if self.armed {
                    if let Some(connection) = self.io.connection.get() {
                        let _ = connection.abort();
                    }
                }
            }
        }
        std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let mut cleanup = Cleanup {
                release: Some(release),
                io,
                armed: true,
            };
            let work = fuse_fixture_work();
            {
                let mut state = core.state_to_finish();
                let root = state.root_inode;
                state.reserve_invalidations(1, &work)?;
                state
                    .invalidation
                    .deferred
                    .push(super::super::invalidation::KernelCacheItem::Inode(root));
            }
            paused.recv_timeout(Duration::from_secs(5))?;
            around.write_range(&path, 0, Bytes::from_static(b"changed"))?;
            let (finished, result) = sync_channel(1);
            scope.spawn(move || {
                let _ = finished.send(std::fs::read(mounted));
            });
            // A production reply transfer, not a timing guess, proves the OPEN
            // has reached its prerequisite while retaining the original view.
            observed.recv_timeout(Duration::from_secs(5))?;
            assert!(matches!(
                result.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ));
            assert_eq!(
                std::fs::metadata(destination.path().join("independent"))
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::NotFound,
            );
            if retire {
                core.stop();
                assert_eq!(
                    result
                        .recv_timeout(Duration::from_secs(5))?
                        .unwrap_err()
                        .raw_os_error(),
                    Some(libc::EIO)
                );
            }
            drop(cleanup.release.take());
            if !retire {
                assert_eq!(result.recv_timeout(Duration::from_secs(5))??, b"changed");
            }
            cleanup.armed = false;
            Ok(())
        })?;
        assert!(session.stop()?);
    }
    Ok(())
}

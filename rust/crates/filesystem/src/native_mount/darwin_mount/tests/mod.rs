#[test]
fn filtered_callback_error_does_not_overwrite_parent_errno() {
    crate::obs::assert_filtered_callback_isolation(|span| {
        let status = super::ffi(0, span, false, |_, _| Ok(0));
        assert_eq!(status, -libc::ESTALE);
        -status
    });
}
use super::*;
use crate::native_mount::MountPublication;
use bytes::Bytes;
use std::os::unix::fs::MetadataExt as _;
use std::process::Command;

unsafe extern "C" {
    fn nfs4_test_read_reply() -> c_int;
    fn nfs4_test_durable_writes() -> c_int;
    fn nfs4_test_change_attribute() -> c_int;
    fn nfs4_test_access_rights() -> c_int;
    fn nfs4_test_verify_attributes() -> c_int;
    fn nfs4_test_node_identity() -> c_int;
    fn nfs4_test_release_open_files() -> c_int;
}

type TestResult = Result<(), Box<dyn std::error::Error>>;
type MemorySource = crate::native_mount::CheckoutMountSource<
    crate::facade::MemoryAuthorityBackend,
    crate::facade::MemoryObjectBackend,
>;

fn work(
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<crate::NativeMaterializationWork, crate::NativeTreeMaterializationError> {
    crate::NativeMaterializationWork::new(budget, cancellation).map_err(|(cause, _)| cause)
}

fn checkout(
    publication: MountPublication,
) -> Result<
    (
        Arc<
            crate::native_mount::SharedCheckout<
                crate::facade::MemoryAuthorityBackend,
                crate::facade::MemoryObjectBackend,
            >,
        >,
        crate::model::VolumeConfig,
    ),
    Box<dyn std::error::Error>,
> {
    use crate::Fs;
    use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, Lifecycle};
    use crate::native_mount::SharedCheckout;

    let mut config = crate::model::VolumeConfig::portable(Lifecycle::Ephemeral);
    config.profile = FilesystemProfile::Posix;
    let fs = Fs::memory();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let checkout = runtime.block_on(async {
        let cancellation = crate::CancellationToken::new();
        let volume = fs
            .create_volume(config, crate::WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map(|receipt| receipt.value)
    })?;
    Ok((
        Arc::new(SharedCheckout::with_publication(checkout, publication)),
        config,
    ))
}

fn checkout_context(
    publication: MountPublication,
) -> Result<
    (
        Arc<MemorySource>,
        DarwinMountContext,
        crate::NativeMaterializationWork,
    ),
    Box<dyn std::error::Error>,
> {
    let (shared, config) = checkout(publication)?;
    let source = Arc::new(MemorySource::new(shared, config)?);
    let root_id = source
        .lookup(&MountPath::root())?
        .ok_or("root absent")?
        .node
        .file_id;
    let work = work(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let metadata = std::fs::metadata(".")?;
    let context = DarwinMountContext::new(
        Arc::clone(&source) as Arc<dyn MountFilesystem>,
        true,
        metadata.uid(),
        metadata.gid(),
        root_id,
        &work,
    )?;
    Ok((source, context, work))
}

fn os(error: i32) -> std::io::Error {
    std::io::Error::from_raw_os_error(error)
}

#[test]
fn close_publishes_only_what_its_handle_wrote() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::CloseAndSync)?;
    let epoch = || source.generation_id();
    let path = MountPath::root().child(b"published".to_vec());
    context.mutate(
        || source.create_file(&path, FileMetadata::default(), None, &work),
        &work,
    )?;
    let created = context.open(&path, context.ledger.latest(), &work)?;
    let before = epoch()?;
    context.flush_on_close(created, &work)?;
    assert_ne!(epoch()?, before);

    let reader = context.open(&path, 0, &work)?;
    let before = epoch()?;
    context.flush_on_close(reader, &work)?;
    context.sync(&work)?;
    assert_eq!(epoch()?, before);

    context.mutate_file(
        &path,
        reader,
        |file| file.write_range(0, Bytes::from_static(b"through the handle")),
        &work,
    )?;
    let before = epoch()?;
    context.flush_on_close(reader, &work)?;
    assert_ne!(epoch()?, before);
    let before = epoch()?;
    context.flush_on_close(reader, &work)?;
    context.sync(&work)?;
    assert_eq!(epoch()?, before);

    context.mutate_file(
        &path,
        0,
        |file| file.write_range(0, Bytes::from_static(b"without a handle")),
        &work,
    )?;
    context.flush_on_close(reader, &work)?;
    let before = epoch()?;
    context.sync(&work)?;
    assert_ne!(epoch()?, before);
    Ok(())
}

#[test]
fn manual_close_and_fsync_publish_nothing_until_sync() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let epoch = || source.generation_id();
    let path = MountPath::root().child(b"manual".to_vec());
    context.mutate(
        || source.create_file(&path, FileMetadata::default(), None, &work),
        &work,
    )?;
    let created = context.open(&path, context.ledger.latest(), &work)?;
    context.mutate_file(
        &path,
        created,
        |file| file.write_range(0, Bytes::from_static(b"deferred")),
        &work,
    )?;
    let reader = context.open(&path, 0, &work)?;
    let before = epoch()?;
    context.flush_on_close(reader, &work)?;
    context.flush_on_close(created, &work)?;
    context.sync(&work)?;
    assert_eq!(epoch()?, before);
    source.sync()?;
    assert_ne!(epoch()?, before);
    Ok(())
}

#[test]
fn change_labels_advance_exactly_for_objects_that_changed() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let label = |path: &MountPath| {
        context
            .attributes(path, 0, &work)
            .map(|attributes| attributes.change)
    };
    let root = MountPath::root();
    let first = root.child(b"first".to_vec());
    let second = root.child(b"second".to_vec());
    for path in [&first, &second] {
        context.mutate(
            || source.create_file(path, FileMetadata::default(), None, &work),
            &work,
        )?;
    }
    let (root_label, first_label, second_label) = (label(&root)?, label(&first)?, label(&second)?);
    assert_eq!(label(&first)?, first_label);
    assert_ne!(first_label, second_label);

    context.mutate_file(
        &first,
        0,
        |file| file.write_range(0, Bytes::from_static(b"written")),
        &work,
    )?;
    assert!(label(&first)? > first_label);
    assert_eq!(label(&second)?, second_label);
    assert_eq!(label(&root)?, root_label);

    source.remove(&second, None, &work)?;
    assert!(label(&root)? > root_label);

    let first_label = label(&first)?;
    context.forget_view();
    assert!(label(&first)? > first_label);

    let lookup = source.lookup(&first)?.ok_or("file absent")?;
    assert_ne!(
        context.change_label(&first, lookup, None, &work)?,
        context.change_label(&first, lookup, None, &work)?
    );
    Ok(())
}

#[test]
fn an_unconfirmed_fence_waits_only_for_what_changed() -> TestResult {
    use super::super::view_ledger::ViewChange;

    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let context = Arc::new(context);
    let observer = Arc::downgrade(&context);
    let observer: std::sync::Weak<dyn ViewObserver> = observer;
    source.observe_view(observer);
    let path = MountPath::root().child(b"kept".to_vec());
    source.create_file(&path, FileMetadata::default(), None, &work)?;
    // The creation was made around the mount; take it as waited out.
    let made = context.around.made.load(Ordering::Acquire);
    context.around.settled.store(made, Ordering::Release);
    let label = context.attributes(&path, 0, &work)?.change;

    source.record_projection_change(&ViewChange::Unconfirmed);
    assert!(
        !context
            .run(false, |work| context.revalidate(work))
            .map_err(os)?
    );
    assert_eq!(context.attributes(&path, 0, &work)?.change, label);

    // What the client holds no longer matches what the source reads.
    for issued in context
        .changes
        .labels
        .lock()
        .map_err(|_| "labels poisoned")?
        .values_mut()
    {
        issued.lookup.node.logical_bytes += 1;
    }
    source.record_projection_change(&ViewChange::Unconfirmed);
    assert!(
        context
            .run(false, |work| context.revalidate(work))
            .map_err(os)?
    );
    Ok(())
}

#[test]
fn attributes_observed_before_a_change_never_take_a_later_label() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let path = MountPath::root().child(b"raced".to_vec());
    let created = source.create_file(&path, FileMetadata::default(), None, &work)?;
    let before = context
        .cache_stamp(&work)?
        .ok_or("checkout has no view stamp")?;
    source.write_range(&path, 0, Bytes::from_static(b"changed"))?;
    let after = context
        .cache_stamp(&work)?
        .ok_or("checkout has no view stamp")?;
    let written = source.lookup(&path)?.ok_or("file absent")?;
    let current = context.change_label(&path, written, Some(after), &work)?;
    assert_eq!(
        context.change_label(&path, written, Some(after), &work)?,
        current
    );
    assert!(context.change_label(&path, created, Some(before), &work)? > current);
    Ok(())
}

#[test]
fn revalidation_waits_out_the_attribute_timeout_for_changes_around_the_mount() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let context = Arc::new(context);
    source.observe_view_with_meter(Arc::downgrade(&context), &work)?;
    // SAFETY: the bridge reports a constant.
    let timeout = u64::from(unsafe { acyclic_fs_darwin_mount_attribute_timeout() });
    let barrier = || -> Result<_, NativeMountError> {
        let began = uptime(&work)?;
        context.revalidate(&work)?;
        Ok((began, uptime(&work)?))
    };
    let (began, ended) = barrier()?;
    assert!(ended - began < REPLY_DELIVERY_SLACK);

    // SAFETY: the name is NUL-terminated and `context` outlives the call.
    let status = unsafe {
        acyclic_fs_darwin_mount_mkdir(
            Arc::as_ptr(&context) as usize,
            c"/own".as_ptr(),
            0o755,
            0,
            0,
        )
    };
    assert_eq!(status, 0);
    let (began, ended) = barrier()?;
    assert!(ended - began < REPLY_DELIVERY_SLACK);

    let waits_out_the_timeout = || -> TestResult {
        let (began, ended) = barrier()?;
        assert!(ended.as_secs() >= (began + REPLY_DELIVERY_SLACK).as_secs() + timeout);
        let (began, ended) = barrier()?;
        assert!(ended - began < REPLY_DELIVERY_SLACK);
        Ok(())
    };
    source.create_file(
        &MountPath::root().child(b"around".to_vec()),
        FileMetadata::default(),
        None,
        &work,
    )?;
    waits_out_the_timeout()?;
    context.forget_view();
    waits_out_the_timeout()?;
    Ok(())
}

#[test]
fn a_barrier_waits_only_for_callbacks_that_began_before_it() -> TestResult {
    let gate = Arc::new(CallbackGate::new());
    let earlier = gate.enter();
    let (drained, finished) = std::sync::mpsc::channel();
    let barrier = std::thread::spawn({
        let gate = Arc::clone(&gate);
        move || {
            let work = work(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )?;
            gate.drain(&work)?;
            let _ = drained.send(());
            Ok::<_, NativeMountError>(())
        }
    });
    assert!(finished.recv_timeout(Duration::from_millis(50)).is_err());
    let later = gate.enter();
    drop(earlier);
    finished.recv_timeout(Duration::from_secs(5))?;
    drop(later);
    barrier.join().map_err(|_| "barrier panicked")??;
    Ok(())
}

#[test]
fn nfs_read_answers_short_reads_as_eof_in_place() {
    // SAFETY: the test hook owns all callback state.
    assert_eq!(unsafe { nfs4_test_read_reply() }, 0);
}

#[test]
fn nfs_fileids_name_nodes_so_hard_links_share_one() {
    // SAFETY: the test hook owns all callback state.
    assert_eq!(unsafe { nfs4_test_node_identity() }, 0);
}

#[test]
fn nfs_durable_writes_are_stable_without_a_sync() {
    // SAFETY: the test hook owns all callback state.
    assert_eq!(unsafe { nfs4_test_durable_writes() }, 0);
}

#[test]
fn only_a_publishing_source_leaves_writes_unstable() -> TestResult {
    for (publication, durable) in [
        (MountPublication::Manual, 1),
        (MountPublication::CloseAndSync, 0),
    ] {
        let (_, context, _work) = checkout_context(publication)?;
        // SAFETY: `context` outlives the call it is passed to.
        let answer = unsafe {
            acyclic_fs_darwin_mount_durable_writes(std::ptr::from_ref(&context) as usize)
        };
        assert_eq!(answer, durable);
    }
    Ok(())
}

#[test]
fn handles_keep_the_node_they_opened() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let path = MountPath::root().child(b"deferred".to_vec());
    let created = source.create_file(&path, FileMetadata::default(), None, &work)?;
    source.write_range(&path, 0, Bytes::from_static(b"kept"))?;
    let closed_unused = context.open(&path, 0, &work)?;
    assert_eq!(
        context
            .lookup_handle(&path, closed_unused, &work)?
            .node
            .file_id,
        created.node.file_id
    );

    let read = context.open(&path, 0, &work)?;
    let bytes = context
        .file_target(&path, read, &work)?
        .file
        .0
        .read_up_to(0, 16)?;
    assert_eq!(bytes.as_ref(), b"kept");

    let stale = context.open(&path, 0, &work)?;
    source.remove(&path, Some(created.node.file_id), &work)?;
    source.create_file(&path, FileMetadata::default(), None, &work)?;
    assert_eq!(
        context
            .file_target(&path, stale, &work)
            .err()
            .as_ref()
            .map(callback_errno),
        Some(libc::ESTALE)
    );
    Ok(())
}

#[test]
fn renaming_over_an_open_file_keeps_it_for_its_handles() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let (replaced, replacement) = (
        MountPath::root().child(b"replaced".to_vec()),
        MountPath::root().child(b"replacement".to_vec()),
    );
    let original = source.create_file(&replaced, FileMetadata::default(), None, &work)?;
    source.write_range(&replaced, 0, Bytes::from_static(b"original"))?;
    source.create_file(&replacement, FileMetadata::default(), None, &work)?;
    let handle = context.open(&replaced, 0, &work)?;
    let rename_over = |from: &CStr| {
        // SAFETY: both names are NUL-terminated and `context` outlives the call.
        unsafe {
            acyclic_fs_darwin_mount_rename(
                std::ptr::from_ref(&context) as usize,
                from.as_ptr(),
                c"/replaced".as_ptr(),
                0,
            )
        }
    };
    assert_eq!(rename_over(c"/missing"), -libc::ENOENT);
    assert_eq!(rename_over(c"/replacement"), 0);
    let target = context.file_target(&replaced, handle, &work)?;
    let file = &target.file.0;
    assert_eq!(file.lookup()?.node.file_id, original.node.file_id);
    assert_eq!(file.read_up_to(0, 16)?.as_ref(), b"original");
    Ok(())
}

#[test]
fn named_attribute_probes_need_no_source_call_without_attributes() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let path = MountPath::root().child(b"plain".to_vec());
    source.create_file(&path, FileMetadata::default(), None, &work)?;
    assert!(!context.may_have_named_attributes(&path, &work)?);
    // SAFETY: the names are NUL-terminated and `context` outlives the call.
    let probe = || unsafe {
        acyclic_fs_darwin_mount_getxattr(
            std::ptr::from_ref(&context) as usize,
            c"/plain".as_ptr(),
            c"com.apple.provenance".as_ptr(),
            ptr::null_mut(),
            0,
        )
    };
    assert_eq!(probe(), -libc::ENOATTR);
    source.write_attribute(
        &path,
        b"com.apple.provenance",
        crate::kernel::OwnedSourceChunk::intrinsic(Bytes::from_static(b"origin")),
        MountAttributeWriteMode::Upsert,
        &work,
    )?;
    assert!(context.may_have_named_attributes(&path, &work)?);
    assert_eq!(probe(), 6);
    Ok(())
}

#[test]
fn nfs_change_attribute_travels_with_its_attributes() {
    // SAFETY: the test hook owns all callback state.
    assert_eq!(unsafe { nfs4_test_change_attribute() }, 0);
}

#[test]
fn nfs_access_applies_the_callers_identity_to_mode_bits() {
    // SAFETY: the test hook owns all callback state.
    assert_eq!(unsafe { nfs4_test_access_rights() }, 0);
}

#[test]
fn nfs_verify_compares_current_attribute_values() {
    // SAFETY: the test hook owns all callback state.
    assert_eq!(unsafe { nfs4_test_verify_attributes() }, 0);
}

#[test]
fn nfs_teardown_releases_every_open_handle_once() {
    // SAFETY: the test hook owns all callback state.
    assert_eq!(unsafe { nfs4_test_release_open_files() }, 0);
}

#[test]
fn content_changes_stamp_modification_and_status_times() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let path = MountPath::root().child(b"stamped".to_vec());
    let created = FileMetadata {
        modified_ns: MetadataField::Value(1),
        changed_ns: MetadataField::Value(1),
        ..create_metadata(0o644, mode::IFREG, 0, 0)
    };
    context.mutate(|| source.create_file(&path, created, None, &work), &work)?;
    let times = || -> Result<_, Box<dyn std::error::Error>> {
        let metadata = source.lookup(&path)?.ok_or("file absent")?.metadata;
        Ok((metadata.modified_ns, metadata.changed_ns))
    };
    context.mutate_file(
        &path,
        0,
        |file| file.write_range(0, Bytes::from_static(b"stamped")),
        &work,
    )?;
    let (MetadataField::Value(modified), MetadataField::Value(changed)) = times()? else {
        return Err("write dropped a time".into());
    };
    assert!(modified > 1 && changed > 1);
    context.mutate_file(&path, 0, |file| file.resize(1), &work)?;
    let (MetadataField::Value(resized), _) = times()? else {
        return Err("resize dropped a time".into());
    };
    assert!(resized >= modified);
    Ok(())
}

#[allow(
    clippy::type_complexity,
    reason = "the tuple keeps the temporary directory, source, and session alive together for one test"
)]
fn live_mount() -> Result<
    (
        tempfile::TempDir,
        Arc<MemorySource>,
        crate::NativeMountSession,
        crate::NativeMaterializationWork,
    ),
    Box<dyn std::error::Error>,
> {
    let (source, _, work) = checkout_context(MountPublication::Manual)?;
    let temporary = tempfile::tempdir()?;
    let mount = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: temporary.path().to_path_buf(),
            writable: true,
        },
        Arc::clone(&source) as Arc<dyn MountFilesystem>,
    )?;
    Ok((temporary, source, mount, work))
}

fn mount_socket(destination: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let destination = destination.canonicalize()?;
    let mut mounts = std::ptr::null_mut::<libc::statfs>();
    // SAFETY: `getmntinfo` points `mounts` at an OS-owned array of the returned length, valid until the next call.
    let count = unsafe { libc::getmntinfo(&raw mut mounts, libc::MNT_NOWAIT) };
    // SAFETY: as above.
    let mounts = unsafe { std::slice::from_raw_parts(mounts, usize::try_from(count)?) };
    mounts
        .iter()
        .find_map(|mount| {
            // SAFETY: both names are NUL-terminated by the OS.
            let (on, from) = unsafe {
                (
                    CStr::from_ptr(mount.f_mntonname.as_ptr()),
                    CStr::from_ptr(mount.f_mntfromname.as_ptr()),
                )
            };
            (on.to_bytes() == destination.as_os_str().as_bytes())
                .then(|| from.to_str().ok())
                .flatten()
                .and_then(|from| from.strip_prefix('<'))
                .and_then(|from| from.split_once(">:"))
                .map(|(socket, _)| PathBuf::from(socket))
        })
        .ok_or_else(|| "mount has no local socket source".into())
}

#[test]
#[ignore = "requires a live macOS NFS mount"]
fn macos_revalidation_shows_changes_made_around_the_mount() -> TestResult {
    let (temporary, source, mut mount, work) = live_mount()?;
    let (changed, created) = (
        temporary.path().join("changed"),
        temporary.path().join("created"),
    );
    let names = || -> std::io::Result<Vec<_>> {
        std::fs::read_dir(temporary.path())?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect()
    };
    // Cache the file's attributes, the listing, and the absent name.
    std::fs::write(&changed, b"old")?;
    assert_eq!(std::fs::metadata(&changed)?.len(), 3);
    assert_eq!(names()?, ["changed"]);
    assert_eq!(
        std::fs::metadata(&created).err().map(|error| error.kind()),
        Some(std::io::ErrorKind::NotFound)
    );
    let began = Instant::now();
    mount.revalidate(&work)?;
    assert!(began.elapsed() < REPLY_DELIVERY_SLACK);

    source.write_range(
        &MountPath::root().child(b"changed".to_vec()),
        0,
        Bytes::from_static(b"newer"),
    )?;
    source.create_file(
        &MountPath::root().child(b"created".to_vec()),
        FileMetadata::default(),
        None,
        &work,
    )?;
    mount.revalidate(&work)?;
    assert_eq!(std::fs::metadata(&changed)?.len(), 5);
    assert!(created.is_file());
    let mut listed = names()?;
    listed.sort();
    assert_eq!(listed, ["changed", "created"]);
    assert_eq!(std::fs::read(&changed)?, b"newer");
    assert!(mount.stop()?);
    Ok(())
}

#[test]
#[ignore = "requires a live macOS NFS mount"]
fn macos_server_answers_only_the_kernel() -> TestResult {
    use std::io::{Read as _, Write as _};
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let (temporary, _, mut mount, _work) = live_mount()?;
    let socket = mount_socket(temporary.path())?;
    let directory = std::fs::metadata(socket.parent().ok_or("socket has no directory")?)?;
    assert_eq!(directory.permissions().mode() & 0o777, 0o700);
    // SAFETY: `getuid` has no preconditions.
    assert_eq!(directory.uid(), unsafe { libc::getuid() });

    // An NFS NULL call, as any local process could send it.
    let mut stream = std::os::unix::net::UnixStream::connect(&socket)?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let call: [u32; 11] = [0x8000_0028, 1, 0, 2, 100_003, 4, 0, 0, 0, 0, 0];
    let bytes = call
        .iter()
        .flat_map(|word| word.to_be_bytes())
        .collect::<Vec<_>>();
    let _ = stream.write_all(&bytes);
    let mut reply = [0_u8; 4];
    assert_eq!(stream.read(&mut reply).unwrap_or(0), 0);
    std::fs::write(temporary.path().join("still-served"), b"kernel")?;
    assert_eq!(
        std::fs::read(temporary.path().join("still-served"))?,
        b"kernel"
    );
    assert!(mount.stop()?);
    Ok(())
}

#[test]
#[ignore = "requires a live macOS NFS mount"]
fn macos_locks_exclude_other_processes() -> TestResult {
    use std::os::fd::AsRawFd as _;

    let (temporary, _, mut mount, _work) = live_mount()?;
    let path = temporary.path().join("locked");
    std::fs::write(&path, b"")?;
    // The NFS client keys locks by process, so another process contends.
    let contender = |operation: &str| {
        acyclic_native_runtime::process_status(Command::new("/usr/bin/perl")
                .arg("-e")
                .arg(format!(
                    "use Fcntl qw(:flock :DEFAULT); open(F, '+<', $ARGV[0]) or die;                      exit({operation} ? 0 : 1)"
                ))
                .arg(&path), std::time::Duration::from_secs(120))
                .map(|status| status.success())
    };
    let flock = "flock(F, LOCK_EX | LOCK_NB)";
    let fcntl = "fcntl(F, F_SETLK, my $region = pack('q q l s s', 0, 0, 0, F_WRLCK, 0))";
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    // SAFETY: `file` owns a live descriptor for each call.
    assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) }, 0);
    assert!(!contender(flock)?);
    // SAFETY: as above.
    assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) }, 0);
    assert!(contender(flock)?);
    let region = libc::flock {
        l_start: 0,
        l_len: 0,
        l_pid: 0,
        l_type: libc::c_short::try_from(libc::F_WRLCK)?,
        l_whence: libc::c_short::try_from(libc::SEEK_SET)?,
    };
    assert_eq!(
        // SAFETY: `region` outlives the call on the live descriptor.
        unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &raw const region) },
        0
    );
    assert!(!contender(fcntl)?);
    drop(file);
    assert!(contender(fcntl)?);
    assert!(mount.stop()?);
    Ok(())
}

#[test]
#[ignore = "requires a live macOS NFS mount"]
fn macos_mode_bits_deny_access() -> TestResult {
    use std::os::unix::fs::PermissionsExt as _;

    let (temporary, _, mut mount, _work) = live_mount()?;
    let path = temporary.path().join("read-only");
    std::fs::write(&path, b"kept")?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))?;
    assert_eq!(
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .err()
            .map(|error| error.kind()),
        Some(std::io::ErrorKind::PermissionDenied)
    );
    let script = temporary.path().join("script");
    std::fs::write(&script, b"#!/bin/sh\nexit 0\n")?;
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o644))?;
    assert_eq!(
        acyclic_native_runtime::process_status(
            &mut Command::new(&script),
            std::time::Duration::from_secs(120)
        )
        .err()
        .map(|error| error.kind()),
        Some(std::io::ErrorKind::PermissionDenied)
    );
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))?;
    assert!(
        acyclic_native_runtime::process_status(
            &mut Command::new(&script),
            std::time::Duration::from_secs(120)
        )?
        .success()
    );
    assert_eq!(std::fs::read(&path)?, b"kept");
    assert!(mount.stop()?);
    Ok(())
}

#[test]
fn directory_page_rejects_a_native_mutation_between_reads() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    source.create_file(
        &MountPath::root().child(b"in-page".to_vec()),
        FileMetadata::default(),
        None,
        &work,
    )?;
    let root = MountPath::root();
    let mut directory = DirectoryHandle::new(
        clone_path(&root, &work)?,
        source.binding_epoch(),
        context.cache_epochs(&work)?,
    );
    advance_directory_to_offset(&context, &mut directory, 3, &work)?;
    source.create_file(
        &MountPath::root().child(b"after-page".to_vec()),
        FileMetadata::default(),
        None,
        &work,
    )?;
    assert_eq!(
        directory
            .keep_current(&context, &work)
            .err()
            .as_ref()
            .map(callback_errno),
        Some(libc::ESTALE)
    );
    assert_eq!(
        ensure_directory_page(&context, &mut directory, &work)
            .err()
            .as_ref()
            .map(callback_errno),
        Some(libc::ESTALE)
    );
    Ok(())
}

#[test]
fn a_listing_that_delivered_nothing_restarts_in_the_new_view() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let root = MountPath::root();
    let mut directory = DirectoryHandle::new(
        clone_path(&root, &work)?,
        source.binding_epoch(),
        context.cache_epochs(&work)?,
    );
    ensure_directory_page(&context, &mut directory, &work)?;
    source.create_file(
        &MountPath::root().child(b"after-page".to_vec()),
        FileMetadata::default(),
        None,
        &work,
    )?;
    // Only `.` and `..` were delivered: the listing starts again.
    directory.keep_current(&context, &work)?;
    ensure_directory_page(&context, &mut directory, &work)?;
    Ok(())
}

#[test]
fn directory_continuation_requires_its_listing_and_binding_unchanged() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let nested = MountPath::root().child(b"nested".to_vec());
    source.create_directory(&nested, FileMetadata::default(), &work)?;
    let epochs = context
        .cache_epochs(&work)?
        .ok_or("checkout has no view stamp")?;
    let root = DirectoryHandle::new(
        DarwinOwned::empty(MountPath::root()),
        Some(epochs.binding),
        Some(epochs),
    );
    let directory = DirectoryHandle::new(
        clone_path(&nested, &work)?,
        Some(epochs.binding),
        Some(epochs),
    );
    let rebound = DirectoryHandle::new(
        clone_path(&nested, &work)?,
        Some(epochs.binding + 1),
        Some(CacheEpochs {
            binding: epochs.binding + 1,
            ..epochs
        }),
    );
    assert!(root.is_current(&context, &work)?);
    assert!(directory.is_current(&context, &work)?);
    assert!(!rebound.is_current(&context, &work)?);
    assert!(directory.epochs.is_some());

    source.create_file(
        &MountPath::root().child(b"sibling".to_vec()),
        FileMetadata::default(),
        None,
        &work,
    )?;
    assert!(!root.is_current(&context, &work)?);
    assert!(directory.is_current(&context, &work)?);
    source.create_file(
        &nested.child(b"child".to_vec()),
        FileMetadata::default(),
        None,
        &work,
    )?;
    assert!(!directory.is_current(&context, &work)?);

    let unrecorded = DirectoryHandle::new(
        clone_path(&nested, &work)?,
        source.binding_epoch(),
        context.cache_epochs(&work)?,
    );
    assert!(unrecorded.is_current(&context, &work)?);
    context.forget_view();
    assert!(!unrecorded.is_current(&context, &work)?);

    let epochless = DirectoryHandle::new(DarwinOwned::empty(MountPath::root()), None, None);
    assert!(!epochless.is_current(&context, &work)?);
    assert!(!epochless.epochs.is_some());
    Ok(())
}

mod callbacks;

mod lifecycle;

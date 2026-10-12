// Every Unix backend must support independent sequential sessions. On
// macOS this also proves that each loopback NFS server receives its own
// kernel-assigned port; on Linux it exercises two distinct FUSE sessions.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
#[ignore = "mounts two live Unix sessions; requires the host's native mount capability"]
fn two_sequential_unix_mounts_in_one_process_both_stay_independently_writable()
-> Result<(), Box<dyn std::error::Error>> {
    let source_a = Arc::new(source(FilesystemProfile::Posix)?);
    let source_b = Arc::new(source(FilesystemProfile::Posix)?);
    let temporary_a = tempfile::tempdir()?;
    let temporary_b = tempfile::tempdir()?;

    let mut mount_a = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source_a.volume_id()?,
            destination: temporary_a.path().to_path_buf(),
            writable: true,
        },
        Arc::clone(&source_a) as Arc<dyn MountFilesystem>,
    )?;
    let mut mount_b = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source_b.volume_id()?,
            destination: temporary_b.path().to_path_buf(),
            writable: true,
        },
        Arc::clone(&source_b) as Arc<dyn MountFilesystem>,
    )?;

    std::fs::write(temporary_a.path().join("a.txt"), b"from-a")?;
    std::fs::write(temporary_b.path().join("b.txt"), b"from-b")?;
    assert_eq!(std::fs::read(temporary_a.path().join("a.txt"))?, b"from-a");
    assert_eq!(std::fs::read(temporary_b.path().join("b.txt"))?, b"from-b");

    assert!(mount_a.stop()?);
    assert!(mount_b.stop()?);
    Ok(())
}

// `O_CREAT|O_EXCL` is what mkstemp, git's index.lock and editors' atomic
// saves use. On macOS the NFS client sends it as an EXCLUSIVE4 create,
// whose verifier the server parks in the new file's timestamps until the
// client's follow-up SETATTR replaces them (RFC 7530 s16.16.5). The
// second create must see the existing file, and the timestamps a caller
// observes must be real ones, not the verifier. Removing and replacing
// the path must not let a remembered verifier reopen the replacement.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
#[ignore = "mounts a live Unix session; requires the host's native mount capability"]
fn exclusive_create_on_a_unix_mount_creates_once_with_real_timestamps()
-> Result<(), Box<dyn std::error::Error>> {
    let source = Arc::new(source(FilesystemProfile::Posix)?);
    let temporary = tempfile::tempdir()?;
    let mut mount = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: temporary.path().to_path_buf(),
            writable: true,
        },
        Arc::clone(&source) as Arc<dyn MountFilesystem>,
    )?;

    let path = temporary.path().join("index.lock");
    let before = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
    let mut first = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    std::io::Write::write_all(&mut first, b"locked")?;
    drop(first);
    let second = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path);
    assert_eq!(
        second.err().map(|error| error.kind()),
        Some(std::io::ErrorKind::AlreadyExists)
    );
    let metadata = std::fs::metadata(&path)?;
    assert!(
        metadata.modified()? >= before && metadata.accessed()? >= before,
        "timestamps still carry the create verifier: {metadata:?}"
    );
    assert_eq!(std::fs::read(&path)?, b"locked");

    std::fs::remove_file(&path)?;
    std::fs::write(&path, b"replacement")?;
    let after_replacement = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path);
    assert_eq!(
        after_replacement.err().map(|error| error.kind()),
        Some(std::io::ErrorKind::AlreadyExists)
    );
    assert_eq!(std::fs::read(&path)?, b"replacement");

    assert!(mount.stop()?);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "mounts a live FUSE session; requires the host's native mount capability"]
fn linux_negative_lookup_clears_on_create_and_invalidation()
-> Result<(), Box<dyn std::error::Error>> {
    use std::io::{Read as _, Seek as _, SeekFrom};

    let source = Arc::new(source(FilesystemProfile::Posix)?);
    let temporary = tempfile::tempdir()?;
    let mut mount = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: temporary.path().to_path_buf(),
            writable: true,
        },
        Arc::clone(&source) as Arc<dyn MountFilesystem>,
    )?;

    let local = temporary.path().join("created-locally");
    assert_eq!(
        std::fs::metadata(&local).err().map(|error| error.kind()),
        Some(std::io::ErrorKind::NotFound)
    );
    std::fs::write(&local, b"local")?;
    assert_eq!(std::fs::read(&local)?, b"local");

    let external = temporary.path().join("created-by-source");
    assert_eq!(
        std::fs::metadata(&external).err().map(|error| error.kind()),
        Some(std::io::ErrorKind::NotFound)
    );
    source.create_file(
        &MountPath::root().child(b"created-by-source".to_vec()),
        metadata(),
    )?;
    mount.invalidate(b"/created-by-source")?;
    assert!(
        external.is_file(),
        "invalidation must evict a negative dentry"
    );

    let nested = temporary.path().join("nested");
    std::fs::create_dir(&nested)?;
    let nested_external = nested.join("created-by-source");
    assert_eq!(
        std::fs::metadata(&nested_external)
            .err()
            .map(|error| error.kind()),
        Some(std::io::ErrorKind::NotFound)
    );
    source.create_file(
        &MountPath::root()
            .child(b"nested".to_vec())
            .child(b"created-by-source".to_vec()),
        metadata(),
    )?;
    mount.invalidate(b"/nested/created-by-source")?;
    assert!(
        nested_external.is_file(),
        "nested invalidation must evict a negative dentry"
    );

    let data_path = MountPath::root().child(b"changed-by-source".to_vec());
    source.create_file(&data_path, metadata())?;
    source.write_range(&data_path, 0, Bytes::from_static(b"before"))?;
    mount.invalidate(b"/changed-by-source")?;
    let mut opened = std::fs::File::open(temporary.path().join("changed-by-source"))?;
    let mut body = [0_u8; 6];
    opened.read_exact(&mut body)?;
    assert_eq!(&body, b"before");
    source.write_range(&data_path, 0, Bytes::from_static(b"after!"))?;
    mount.invalidate(b"/changed-by-source")?;
    opened.seek(SeekFrom::Start(0))?;
    opened.read_exact(&mut body)?;
    assert_eq!(&body, b"after!");
    drop(opened);

    assert!(mount.stop()?);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "opens a live FUSE file with O_TRUNC; requires the host's native mount capability"]
fn linux_truncate_open_keeps_its_own_handle() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write as _;

    let source = Arc::new(source(FilesystemProfile::Posix)?);
    let temporary = tempfile::tempdir()?;
    let mut mount = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: temporary.path().to_path_buf(),
            writable: true,
        },
        Arc::clone(&source) as Arc<dyn MountFilesystem>,
    )?;
    let path = temporary.path().join("incremental-output");
    std::fs::write(&path, b"old")?;
    for iteration in 0..8 {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)?;
        write!(file, "new-{iteration}")?;
        file.sync_all()?;
        assert_eq!(std::fs::read_to_string(&path)?, format!("new-{iteration}"));
    }
    assert!(mount.stop()?);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires a live Linux FUSE mount"]
fn linux_hard_link_alias_survives_mounted_write_and_rename()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::MetadataExt as _;

    let source = Arc::new(source(FilesystemProfile::Posix)?);
    let temporary = tempfile::tempdir()?;
    let mut mount = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: temporary.path().to_path_buf(),
            writable: true,
        },
        Arc::clone(&source) as Arc<dyn MountFilesystem>,
    )?;
    let original = temporary.path().join("original");
    let alias = temporary.path().join("alias");
    let renamed = temporary.path().join("renamed");
    std::fs::write(&original, b"before")?;
    std::fs::hard_link(&original, &alias)?;
    assert_eq!(
        std::fs::metadata(&original)?.ino(),
        std::fs::metadata(&alias)?.ino()
    );
    assert_eq!(std::fs::metadata(&alias)?.nlink(), 2);

    std::fs::write(&alias, b"after")?;
    assert_eq!(std::fs::read(&original)?, b"after");
    std::fs::rename(&original, &renamed)?;
    assert_eq!(
        std::fs::metadata(&renamed)?.ino(),
        std::fs::metadata(&alias)?.ino()
    );
    assert_eq!(std::fs::read(&renamed)?, b"after");
    std::fs::remove_file(&alias)?;
    assert_eq!(std::fs::metadata(&renamed)?.nlink(), 1);
    assert_eq!(std::fs::read(&renamed)?, b"after");
    assert!(mount.stop()?);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires a live Linux FUSE mount"]
fn linux_fsync_waits_for_an_admitted_write() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write as _;

    let source = Arc::new(source(FilesystemProfile::Posix)?);
    let temporary = tempfile::tempdir()?;
    let mut mount = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: temporary.path().to_path_buf(),
            writable: true,
        },
        Arc::clone(&source) as Arc<dyn MountFilesystem>,
    )?;
    let path = temporary.path().join("ordered-write");
    std::fs::write(&path, b"old")?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    let mut writer = file.try_clone()?;
    let syncer = file.try_clone()?;
    let gate = super::super::fuse::pause_next_write_after_admission();
    let writer_thread = std::thread::spawn(move || writer.write_all(b"new"));
    assert!(
        gate.wait_until_admitted(Duration::from_secs(5)),
        "write must reach the per-handle gate"
    );

    let (started, started_rx) = std::sync::mpsc::sync_channel(1);
    let (finished, finished_rx) = std::sync::mpsc::sync_channel(1);
    let sync_thread = std::thread::spawn(move || {
        let _ = started.send(());
        let result = syncer.sync_all();
        let _ = finished.send(result);
    });
    started_rx.recv_timeout(Duration::from_secs(5))?;
    assert!(
        finished_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "fsync must not overtake a write admitted on the same handle"
    );
    assert!(gate.release(), "release paused write");
    writer_thread
        .join()
        .map_err(|_| std::io::Error::other("mounted writer panicked"))??;
    sync_thread
        .join()
        .map_err(|_| std::io::Error::other("mounted fsync panicked"))?;
    finished_rx.recv_timeout(Duration::from_secs(5))??;
    drop(file);
    assert_eq!(std::fs::read(&path)?, b"new");
    assert!(
        !checkout_state(&source).1,
        "fsync published the admitted write"
    );
    assert!(mount.stop()?);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires a live Linux FUSE mount"]
fn linux_fsync_on_clean_descriptor_publishes_other_descriptor_write()
-> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write as _;

    let source = Arc::new(source(FilesystemProfile::Posix)?);
    let temporary = tempfile::tempdir()?;
    let mut mount = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: temporary.path().to_path_buf(),
            writable: true,
        },
        Arc::clone(&source) as Arc<dyn MountFilesystem>,
    )?;
    let path = temporary.path().join("cross-descriptor-fsync");
    std::fs::write(&path, b"old")?;
    let syncer = std::fs::OpenOptions::new().read(true).open(&path)?;
    let mut writer = std::fs::OpenOptions::new().write(true).open(&path)?;
    writer.write_all(b"new")?;
    assert!(checkout_state(&source).1, "write remains pending");
    syncer.sync_all()?;
    assert!(
        !checkout_state(&source).1,
        "explicit fsync published the write"
    );
    assert_eq!(std::fs::read(&path)?, b"new");
    writer.write_all(b"2")?;
    assert!(checkout_state(&source).1, "second write remains pending");
    std::fs::File::open(temporary.path())?.sync_all()?;
    assert!(
        !checkout_state(&source).1,
        "directory fsync published the write"
    );
    drop(writer);
    drop(syncer);
    assert!(mount.stop()?);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires a live Linux FUSE mount"]
fn linux_concurrent_truncates_do_not_fence_unrelated_files()
-> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write as _;

    let source = Arc::new(source(FilesystemProfile::Posix)?);
    let temporary = tempfile::tempdir()?;
    let mut mount = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: temporary.path().to_path_buf(),
            writable: true,
        },
        Arc::clone(&source) as Arc<dyn MountFilesystem>,
    )?;

    // Unrelated mounted mutations may advance the global cache epoch
    // while another file is opening. They must not fence its attached
    // handle or make O_TRUNC report ESTALE.
    let barrier = Arc::new(std::sync::Barrier::new(9));
    let mut workers = Vec::new();
    for worker in 0..8 {
        let path = temporary.path().join(format!("concurrent-{worker}"));
        std::fs::write(&path, b"old")?;
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || -> std::io::Result<()> {
            barrier.wait();
            for iteration in 0..8 {
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(&path)
                    .map_err(|error| {
                        std::io::Error::other(format!(
                            "concurrent open {worker}/{iteration}: {error}"
                        ))
                    })?;
                write!(file, "new-{worker}-{iteration}")?;
                file.sync_all()?;
            }
            assert_eq!(std::fs::read_to_string(&path)?, format!("new-{worker}-7"));
            Ok(())
        }));
    }
    barrier.wait();
    for worker in workers {
        worker
            .join()
            .map_err(|_| std::io::Error::other("mounted writer panicked"))??;
    }
    assert!(mount.stop()?);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "mounts a live FUSE session and invokes the installed Rust compiler"]
fn rustc_compiles_inside_a_linux_mount() -> Result<(), Box<dyn std::error::Error>> {
    let source = Arc::new(source(FilesystemProfile::Posix)?);
    let temporary = tempfile::tempdir()?;
    let mut mount = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: temporary.path().to_path_buf(),
            writable: true,
        },
        Arc::clone(&source) as Arc<dyn MountFilesystem>,
    )?;

    std::fs::write(temporary.path().join("main.rs"), b"fn main() {}\n")?;
    let output = acyclic_native_runtime::process_output(
        std::process::Command::new("rustc")
            .current_dir(temporary.path())
            .args(["--edition", "2021", "main.rs", "-o", "main"]),
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )?;
    assert!(
        output.status.success(),
        "rustc failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(temporary.path().join("main").is_file());
    assert!(
        acyclic_native_runtime::process_status(
            &mut std::process::Command::new(temporary.path().join("main")),
            std::time::Duration::from_secs(120)
        )?
        .success(),
        "compiled executable did not run from the mount"
    );

    assert!(mount.stop()?);
    Ok(())
}

// Simultaneous callers must receive independent servers, callback state,
// kernel mounts, and teardown lifecycles.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
#[ignore = "mounts two live Unix sessions concurrently; requires the host's native mount capability"]
fn two_concurrent_unix_mounts_in_one_process_remain_independent()
-> Result<(), Box<dyn std::error::Error>> {
    let source_a = Arc::new(source(FilesystemProfile::Posix)?);
    let source_b = Arc::new(source(FilesystemProfile::Posix)?);
    let temporary_a = tempfile::tempdir()?;
    let temporary_b = tempfile::tempdir()?;
    let request_a = crate::NativeMountRequest {
        mount_id: crate::MountId::new(),
        volume_id: source_a.volume_id()?,
        destination: temporary_a.path().to_path_buf(),
        writable: true,
    };
    let request_b = crate::NativeMountRequest {
        mount_id: crate::MountId::new(),
        volume_id: source_b.volume_id()?,
        destination: temporary_b.path().to_path_buf(),
        writable: true,
    };
    let dyn_source_a = Arc::clone(&source_a) as Arc<dyn MountFilesystem>;
    let dyn_source_b = Arc::clone(&source_b) as Arc<dyn MountFilesystem>;

    let (result_a, result_b) = std::thread::scope(|scope| {
        let handle_a = scope.spawn(|| crate::mount_native(request_a, dyn_source_a));
        let handle_b = scope.spawn(|| crate::mount_native(request_b, dyn_source_b));
        Ok::<_, &'static str>((
            handle_a.join().map_err(|_| "mount a thread panicked")?,
            handle_b.join().map_err(|_| "mount b thread panicked")?,
        ))
    })?;

    let mut mount_a = result_a?;
    let mut mount_b = result_b?;
    std::fs::write(temporary_a.path().join("a.txt"), b"concurrent-a")?;
    std::fs::write(temporary_b.path().join("b.txt"), b"concurrent-b")?;
    assert_eq!(
        std::fs::read(temporary_a.path().join("a.txt"))?,
        b"concurrent-a"
    );
    assert_eq!(
        std::fs::read(temporary_b.path().join("b.txt"))?,
        b"concurrent-b"
    );
    assert!(!temporary_a.path().join("b.txt").exists());
    assert!(!temporary_b.path().join("a.txt").exists());
    assert!(mount_a.stop()?);
    assert!(mount_b.stop()?);
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn run_unix_mount_cross_process_worker() -> Result<(), Box<dyn std::error::Error>> {
    let destination =
        std::env::var_os("ACYCLIC_FS_UNIX_CHILD_MOUNT").ok_or("child mount path is absent")?;
    let ready =
        std::env::var_os("ACYCLIC_FS_UNIX_CHILD_READY").ok_or("child ready path is absent")?;
    let release =
        std::env::var_os("ACYCLIC_FS_UNIX_CHILD_RELEASE").ok_or("child release path is absent")?;
    let identity = std::env::var("ACYCLIC_FS_UNIX_CHILD_ID")?;
    let source = Arc::new(source(FilesystemProfile::Posix)?);
    let mut mount = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: destination.clone().into(),
            writable: true,
        },
        source as Arc<dyn MountFilesystem>,
    )?;
    let filename = format!("{identity}.txt");
    std::fs::write(
        std::path::Path::new(&destination).join(filename),
        identity.as_bytes(),
    )?;
    std::fs::write(&ready, identity.as_bytes())?;
    wait_for_path(
        std::path::Path::new(&release),
        std::time::Duration::from_secs(10),
    )?;
    assert!(mount.stop()?);
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
#[ignore = "starts two real mount-owning processes; requires the host's native mount capability"]
fn unix_mounts_in_distinct_processes_remain_independent() -> Result<(), Box<dyn std::error::Error>>
{
    if std::env::var_os("ACYCLIC_FS_UNIX_CHILD_MOUNT").is_some() {
        return run_unix_mount_cross_process_worker();
    }
    let temporary = tempfile::tempdir()?;
    let mount_a = temporary.path().join("mount-a");
    let mount_b = temporary.path().join("mount-b");
    std::fs::create_dir(&mount_a)?;
    std::fs::create_dir(&mount_b)?;
    let ready_a = temporary.path().join("ready-a");
    let ready_b = temporary.path().join("ready-b");
    let release = temporary.path().join("release");
    let executable = std::env::current_exe()?;
    let worker =
        "native_mount::adapter::tests::unix_mounts_in_distinct_processes_remain_independent";
    let mut child_a = acyclic_native_runtime::spawn_process_tree(
        std::process::Command::new(&executable)
            .args(["--ignored", "--exact", worker, "--test-threads=1"])
            .env("ACYCLIC_FS_UNIX_CHILD_MOUNT", &mount_a)
            .env("ACYCLIC_FS_UNIX_CHILD_READY", &ready_a)
            .env("ACYCLIC_FS_UNIX_CHILD_RELEASE", &release)
            .env("ACYCLIC_FS_UNIX_CHILD_ID", "a"),
    )?;
    let mut child_b = acyclic_native_runtime::spawn_process_tree(
        std::process::Command::new(&executable)
            .args(["--ignored", "--exact", worker, "--test-threads=1"])
            .env("ACYCLIC_FS_UNIX_CHILD_MOUNT", &mount_b)
            .env("ACYCLIC_FS_UNIX_CHILD_READY", &ready_b)
            .env("ACYCLIC_FS_UNIX_CHILD_RELEASE", &release)
            .env("ACYCLIC_FS_UNIX_CHILD_ID", "b"),
    )?;

    let validation = (|| -> Result<(), Box<dyn std::error::Error>> {
        wait_for_path(&ready_a, std::time::Duration::from_secs(10))?;
        wait_for_path(&ready_b, std::time::Duration::from_secs(10))?;
        assert_eq!(std::fs::read(mount_a.join("a.txt"))?, b"a");
        assert_eq!(std::fs::read(mount_b.join("b.txt"))?, b"b");
        assert!(!mount_a.join("b.txt").exists());
        assert!(!mount_b.join("a.txt").exists());
        Ok(())
    })();
    std::fs::write(&release, b"release")?;
    let status_a = wait_for_child(&mut child_a, std::time::Duration::from_secs(10))?;
    let status_b = wait_for_child(&mut child_b, std::time::Duration::from_secs(10))?;
    validation?;
    assert!(status_a.success(), "first mount process failed: {status_a}");
    assert!(
        status_b.success(),
        "second mount process failed: {status_b}"
    );
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn wait_for_path(
    path: &std::path::Path,
    timeout: std::time::Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + timeout;
    while !path.exists() {
        if std::time::Instant::now() >= deadline {
            return Err(format!("timed out waiting for {}", path.display()).into());
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn wait_for_child(
    child: &mut acyclic_native_runtime::ProcessTree,
    timeout: std::time::Duration,
) -> Result<std::process::ExitStatus, Box<dyn std::error::Error>> {
    let result = child.wait(timeout);
    child.terminate()?;
    Ok(result?)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn posix_mount_path_preserves_non_utf8_components() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Posix)?;
    let (initial, pending) = checkout_state(&source);
    assert!(!pending);
    let name = vec![b'r', 0xff, b'w'];
    let path = MountPath::root().child(name.clone());
    let created = source.create_file(&path, metadata())?;
    assert_eq!(source.lookup(&path)?, Some(created));
    source.write_attribute(
        &path,
        b"user.acyclic",
        Bytes::from_static(b"native-xattr"),
        MountAttributeWriteMode::Create,
    )?;
    assert_eq!(
        source.read_attribute(&path, b"user.acyclic")?.as_deref(),
        Some(b"native-xattr".as_slice())
    );
    let attributes = source.list_attributes(&path, None, 8)?;
    assert_eq!(attributes.names, vec![b"user.acyclic".to_vec()]);
    assert_eq!(attributes.next_cursor, None);
    source.remove_attribute(&path, b"user.acyclic")?;
    assert_eq!(source.read_attribute(&path, b"user.acyclic")?, None);
    let page = source.read_directory(&MountPath::root(), None, 8)?;
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, name);
    assert!(checkout_state(&source).1);
    source.flush()?;
    source.flush()?;
    let (published, pending) = checkout_state(&source);
    assert_ne!(published, initial);
    assert!(!pending);
    Ok(())
}

// The FUSE release callback flushes (seals) the checkout before the
// kernel's next namespace mutation arrives, so mutations must stay
// valid across a publication boundary.
#[test]
fn rename_after_flush_preserves_namespace() -> Result<(), Box<dyn std::error::Error>> {
    let profile = if cfg!(target_os = "windows") {
        FilesystemProfile::Windows
    } else {
        FilesystemProfile::Posix
    };
    let source = source(profile)?;
    let path = native_test_path("payload.bin");
    source.create_file(&path, metadata())?;
    let open = source.open_file(&path)?;
    open.write_range(0, Bytes::from_static(b"BRAVO"))?;
    drop(open);
    source.flush()?;
    source.flush()?;
    let renamed = native_test_path("renamed.bin");
    source.rename(&path, &renamed, true)?;
    assert!(source.lookup(&path)?.is_none());
    let landed = source.lookup(&renamed)?;
    assert!(landed.is_some());
    Ok(())
}

#[test]
fn preexisting_entry_survives_published_link_unlink_and_recreate_cycles()
-> Result<(), Box<dyn std::error::Error>> {
    let profile = if cfg!(target_os = "windows") {
        FilesystemProfile::Windows
    } else {
        FilesystemProfile::Posix
    };
    let source = source(profile)?;
    let seed = native_test_path("shared-seed.bin");
    source.create_file(&seed, metadata())?;
    source.write_range(&seed, 0, Bytes::from_static(b"seed"))?;
    source.flush()?;

    let payload = native_test_path("payload.bin");
    source.create_file(&payload, metadata())?;
    source.write_range(&payload, 0, Bytes::from_static(b"BRAVO"))?;
    source.flush()?;
    let renamed = native_test_path("renamed.bin");
    source.rename(&payload, &renamed, true)?;
    let link = native_test_path("link.bin");
    source.hard_link(&renamed, &link)?;
    let renamed_id = source
        .lookup(&renamed)?
        .ok_or("renamed file is absent")?
        .node
        .file_id;
    source.remove(&renamed, Some(renamed_id))?;
    let link_id = source
        .lookup(&link)?
        .ok_or("linked file is absent")?
        .node
        .file_id;
    source.remove(&link, Some(link_id))?;
    source.create_file(&link, metadata())?;
    source.write_range(&link, 0, Bytes::from_static(b"replacement"))?;
    source.flush()?;

    assert_eq!(source.read_range(&seed, 0, 4)?.as_ref(), b"seed");
    let page = source.read_directory(&MountPath::root(), None, 16)?;
    let mut names = page
        .entries
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        vec![
            native_test_path("link.bin").components()[0].clone(),
            native_test_path("shared-seed.bin").components()[0].clone(),
        ]
    );
    assert_eq!(page.next_cursor, None);
    Ok(())
}

// Host-side capture of raw-byte names requires a host filesystem that
// admits them; APFS enforces valid UTF-8 and rejects such names with
// EILSEQ before this crate is involved, so the host half runs on Linux
// only.
#[cfg(target_os = "linux")]
#[test]
fn host_capture_preserves_non_utf8_names() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Posix)?;
    let host = tempfile::tempdir()?;
    let captured_name = vec![b'c', 0xfe, b'p'];
    let host_path = host
        .path()
        .join(std::ffi::OsString::from_vec(captured_name.clone()));
    std::fs::write(&host_path, b"captured")?;
    let captured_path = MountPath::root().child(captured_name);
    source.capture_host_path(host.path(), &captured_path)?;
    assert_eq!(
        source.read_range(&captured_path, 0, 8)?.as_ref(),
        b"captured"
    );
    std::fs::remove_file(host_path)?;
    source.capture_host_path(host.path(), &captured_path)?;
    assert_eq!(source.lookup(&captured_path)?, None);
    Ok(())
}

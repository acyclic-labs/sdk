use super::native::NativeDemandSource;
use super::*;
use crate::model::{FilesystemProfile, VolumeLimits};
use std::error::Error;
use std::sync::{Arc, Mutex};

async fn lookup_node<D: DemandSource>(
    source: &D,
    reference: SourceReference,
    name: &NamespacePath,
    cancellation: &CancellationToken,
) -> Result<SourceNode, Box<dyn Error>> {
    Ok(source
        .lookup(reference, name, WorkBudget::UNBOUNDED, cancellation)
        .await?
        .value
        .ok_or("file absent")?)
}

#[derive(Default)]
struct RecordingObserver {
    directories: Mutex<Vec<NamespacePath>>,
}

impl DemandDirectoryObserver for RecordingObserver {
    fn observe_directory(&self, directory: &NamespacePath) -> Result<(), DemandError> {
        self.directories
            .lock()
            .map_err(|_| DemandError::SourceUnavailable)?
            .push(directory.clone());
        Ok(())
    }
}

fn path(value: &str) -> Result<NamespacePath, Box<dyn Error>> {
    let portable = crate::path::PortablePath::parse(value, VolumeLimits::default())?;
    Ok(NamespacePath::from_portable(
        &portable,
        VolumeLimits::default(),
    )?)
}

async fn native_source(
    path: impl AsRef<std::path::Path>,
) -> Result<NativeDemandSource, DemandError> {
    NativeDemandSource::open(
        path,
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
    )
    .await
}

#[derive(Default)]
struct RecordedChanges(Mutex<Vec<SourceChange>>);

impl SourceChangeSink for RecordedChanges {
    fn source_changed(&self, changes: &[SourceChange]) {
        if let Ok(mut recorded) = self.0.lock() {
            recorded.extend_from_slice(changes);
        }
    }
}

impl RecordedChanges {
    fn take(&self) -> Result<Vec<SourceChange>, Box<dyn Error>> {
        Ok(std::mem::take(
            &mut *self.0.lock().map_err(|_| "recording poisoned")?,
        ))
    }

    fn until(
        &self,
        watch: &dyn SourceWatch,
        reported: impl Fn(&[SourceChange]) -> bool,
    ) -> Result<Vec<SourceChange>, Box<dyn Error>> {
        watch.fence()?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut changes = self.take()?;
        while !reported(&changes) && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
            changes.extend(self.take()?);
        }
        Ok(changes)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn native_source_reports_the_names_and_nodes_it_changed() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("d"))?;
    std::fs::write(root.path().join("f"), b"one")?;
    let source = native_source(root.path()).await?;
    let recorded = Arc::new(RecordedChanges::default());
    let watch = source
        .watch(recorded.clone())?
        .ok_or("a local root is watched")?;
    let cancellation = CancellationToken::new();
    let node = lookup_node(&source, source.reference(), &path("/f")?, &cancellation).await?;
    // Reading beneath a directory watches it where the host needs that.
    assert!(
        source
            .lookup(source.reference(), &path("/d/x")?, &cancellation)
            .await?
            .value
            .is_none()
    );

    std::fs::write(root.path().join("f"), b"changed")?;
    std::fs::write(root.path().join("d").join("x"), b"x")?;
    let (f, x) = (path("/f")?, path("/d/x")?);
    let changes = recorded.until(watch.as_ref(), |changes| {
        changes.contains(&SourceChange::Node(node.file_identity))
            && changes.contains(&SourceChange::Name(x.clone()))
            && (changes.contains(&SourceChange::Entry(f.clone()))
                || changes.contains(&SourceChange::Name(f.clone())))
    })?;
    // `FSEvents` may coalesce the file's recent creation into its write,
    // which reports the name rebound rather than its entry altered.
    assert!(
        changes.contains(&SourceChange::Entry(path("/f")?))
            || cfg!(target_os = "macos") && changes.contains(&SourceChange::Name(path("/f")?)),
        "{changes:?}"
    );
    assert!(
        changes.contains(&SourceChange::Node(node.file_identity)),
        "{changes:?}"
    );
    assert!(
        changes.contains(&SourceChange::Name(path("/d/x")?)),
        "{changes:?}"
    );
    // Nothing was lost; a macOS fence reports everything by design.
    #[cfg(not(target_os = "macos"))]
    assert!(!changes.contains(&SourceChange::Everything));
    assert!(watch.is_exact());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_filtered_source_reports_nothing_beneath_its_exclusions() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join(".git"))?;
    let source = FilteredDemandSource::new(native_source(root.path()).await?, vec![path("/.git")?]);
    let recorded = Arc::new(RecordedChanges::default());
    let watch = source
        .watch(recorded.clone())?
        .ok_or("a local root is watched")?;
    std::fs::write(root.path().join(".git").join("index"), b"x")?;
    std::fs::write(root.path().join("visible"), b"x")?;
    let visible = path("/visible")?;
    let changes = recorded.until(watch.as_ref(), |changes| {
        changes.contains(&SourceChange::Name(visible.clone()))
    })?;
    let excluded = path("/.git")?;
    assert!(
        changes.contains(&SourceChange::Name(path("/visible")?)),
        "{changes:?}"
    );
    assert!(
        !changes.iter().any(|change| matches!(
            change,
            SourceChange::Name(name) | SourceChange::Entry(name) if name.is_within(&excluded)
        )),
        "{changes:?}"
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn native_source_reports_symbolic_link_target_bytes() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    std::os::unix::fs::symlink("target", root.path().join("link"))?;
    let source = NativeDemandSource::open(
        root.path(),
        FilesystemProfile::Posix,
        VolumeLimits::default(),
        None,
        None,
    )
    .await?;
    let reference = source.reference();
    let cancellation = CancellationToken::new();
    let node = lookup_node(&source, reference, &path("/link")?, &cancellation).await?;

    assert_eq!(node.kind, SourceNodeKind::SymbolicLink);
    assert_eq!(node.logical_bytes, Some(6));
    assert_eq!(
        source
            .read_link(reference, &path("/link")?, node.version, &cancellation)
            .await?
            .value
            .as_ref(),
        b"target"
    );
    Ok(())
}

#[tokio::test]
async fn native_source_observes_only_the_demanded_directory_before_reading()
-> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("nested"))?;
    std::fs::write(root.path().join("nested/file"), b"content")?;
    let observer = Arc::new(RecordingObserver::default());
    let source = NativeDemandSource::open(
        root.path(),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        Some(observer.clone()),
    )
    .await?;
    let reference = source.reference();
    source
        .lookup(reference, &path("/nested/file")?, &CancellationToken::new())
        .await?;
    assert_eq!(
        observer
            .directories
            .lock()
            .map_err(|_| "observer state poisoned")?
            .as_slice(),
        &[path("/nested")?]
    );
    Ok(())
}

#[tokio::test]
async fn native_provider_reads_do_not_visit_unrelated_content() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("forbidden"))?;
    std::fs::write(
        root.path().join("forbidden").join("unrelated"),
        vec![7; 8 * 1024 * 1024],
    )?;
    let mut body = vec![b'0'; 1024 * 1024];
    body[3..7].copy_from_slice(b"3456");
    std::fs::write(root.path().join("seen"), body)?;

    let source = native_source(root.path()).await?;
    let provider = &source;
    let reference = provider.reference();
    let cancellation = CancellationToken::new();
    let node = provider
        .lookup(reference, &path("/seen")?, &cancellation)
        .await?;
    let version = node.value.ok_or("seen path was absent")?.version;

    let bytes = provider
        .read_range(reference, &path("/seen")?, version, 3, 4, &cancellation)
        .await?;
    assert_eq!(bytes.value.as_ref(), b"3456");

    let first = provider
        .list_page(reference, &path("/")?, None, 1, &cancellation)
        .await?;
    assert_eq!(first.value.entries.len(), 1);
    let continuation = first.value.next.ok_or("second root entry missing")?;
    let second = provider
        .list_page(reference, &path("/")?, Some(continuation), 1, &cancellation)
        .await?;
    assert_eq!(second.value.entries.len(), 1);
    Ok(())
}

#[tokio::test]
async fn listed_nodes_are_exactly_what_lookups_report() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("directory"))?;
    std::fs::write(root.path().join("file"), b"body")?;
    std::fs::hard_link(root.path().join("file"), root.path().join("alias"))?;
    // A link is listed as itself, never as its target.
    #[cfg(unix)]
    std::os::unix::fs::symlink("directory", root.path().join("link"))?;
    // Growing a file through one of its links leaves the other link's
    // directory record describing the old file on NTFS.
    {
        use std::io::Write as _;
        std::fs::OpenOptions::new()
            .append(true)
            .open(root.path().join("file"))?
            .write_all(b" grown through one link")?;
    }
    // A writer still open has not updated its name's directory record.
    let mut open = std::fs::File::create(root.path().join("open"))?;
    {
        use std::io::Write as _;
        open.write_all(b"written, never closed")?;
        open.flush()?;
    }
    let source = native_source(root.path()).await?;
    let provider = &source;
    let reference = provider.reference();
    let cancellation = CancellationToken::new();
    let page = provider
        .list_page(reference, &path("/")?, None, 16, &cancellation)
        .await?;
    assert_eq!(page.value.entries.len(), if cfg!(unix) { 5 } else { 4 });
    let mut listed = Vec::new();
    for entry in page.value.entries {
        let name = entry
            .name
            .unicode_text()
            .ok_or("listed name is not Unicode")?
            .to_string();
        let looked_up = provider
            .lookup(reference, &path(&format!("/{name}"))?, &cancellation)
            .await?;
        assert_eq!(looked_up.value, Some(entry.node), "{name}");
        listed.push((name, entry.node));
    }
    // Every listed version opens: a listing never pins a version that a
    // handle on the same file disagrees with.
    let identity = |name| {
        listed
            .iter()
            .find(|(listed, _)| listed == name)
            .map(|(_, node)| node.file_identity)
    };
    assert_eq!(identity("file"), identity("alias"));
    for (name, node) in listed {
        if node.kind != SourceNodeKind::RegularFile {
            continue;
        }
        let file = provider
            .open_file(
                reference,
                &path(&format!("/{name}"))?,
                node.version,
                &cancellation,
            )
            .await
            .map_err(|failure| format!("{name}: {:?}", failure.error))?
            .value;
        let length = node.logical_bytes.ok_or("regular file has no length")?;
        let read = file
            .read_range(0, length, &cancellation)
            .map_err(|failure| format!("{name}: {:?}", failure.error))?;
        assert_eq!(read.value.len() as u64, length, "{name}");
    }
    drop(open);
    Ok(())
}

#[tokio::test]
async fn native_requests_wait_for_capacity_and_cancel_before_work() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    let source = native_source(root.path()).await?;
    let permits = source.occupy_all_requests().await?;
    let cancellation = CancellationToken::new();
    let root_path = path("/")?;
    let query = source.lookup(source.reference(), &root_path, &cancellation);
    tokio::pin!(query);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut query)
            .await
            .is_err()
    );
    cancellation.cancel();
    let Err(failure) = tokio::time::timeout(std::time::Duration::from_secs(1), &mut query).await?
    else {
        return Err("cancelled request entered the worker".into());
    };
    assert!(matches!(failure.error, DemandError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    drop(permits);
    Ok(())
}

async fn held_file(
    contents: &[u8],
) -> Result<(tempfile::TempDir, NativeDemandSource, Box<dyn DemandFile>), Box<dyn Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("source");
    std::fs::create_dir(&root)?;
    std::fs::write(root.join("file"), contents)?;
    let source = native_source(&root).await?;
    let reference = source.reference();
    let cancellation = CancellationToken::new();
    let node = lookup_node(&source, reference, &path("/file")?, &cancellation).await?;
    let file = source
        .open_file(reference, &path("/file")?, node.version, &cancellation)
        .await?
        .value;
    Ok((temporary, source, file))
}

#[cfg(windows)]
#[tokio::test]
#[allow(
    unsafe_code,
    reason = "renames over the held file with MoveFileExW, as editors save"
)]
async fn a_held_file_gives_way_to_a_save_over_it() -> Result<(), Box<dyn Error>> {
    use std::os::windows::ffi::OsStrExt as _;
    use windows::Win32::Storage::FileSystem::{MOVEFILE_REPLACE_EXISTING, MoveFileExW};
    use windows::core::PCWSTR;

    let (temporary, _source, file) = held_file(b"before").await?;
    let root = temporary.path().join("source");
    std::fs::write(root.join("saved"), b"after!")?;
    let wide = |path: &std::path::Path| {
        path.as_os_str()
            .encode_wide()
            .chain([0])
            .collect::<Vec<u16>>()
    };
    let (saved, replaced) = (wide(&root.join("saved")), wide(&root.join("file")));
    // SAFETY: both names are live, NUL-terminated wide strings.
    unsafe {
        MoveFileExW(
            PCWSTR(saved.as_ptr()),
            PCWSTR(replaced.as_ptr()),
            MOVEFILE_REPLACE_EXISTING,
        )
    }?;
    assert_eq!(std::fs::read(root.join("file"))?, b"after!");
    assert!(matches!(
        read_failure(file.as_ref()),
        Some(DemandError::StaleVersion)
    ));
    Ok(())
}

#[cfg(windows)]
#[tokio::test]
async fn a_benign_oplock_break_preserves_the_held_read() -> Result<(), Box<dyn Error>> {
    let (temporary, _source, file) = held_file(b"before").await?;
    let path = temporary.path().join("source/file");
    // A second opener with write access breaks a handle oplock even if
    // it never changes the file. The held read must reopen at the same
    // version instead of reporting a change that did not occur.
    drop(
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?,
    );
    let barrier = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        let reads = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    file.read_range(0, 6, &CancellationToken::new())
                        .is_ok_and(|read| read.value == Bytes::from_static(b"before"))
                })
            })
            .collect::<Vec<_>>();
        for read in reads {
            assert!(read.join().unwrap_or(false));
        }
    });
    assert_eq!(
        file.read_range(0, 6, &CancellationToken::new())?.value,
        Bytes::from_static(b"before")
    );
    assert_eq!(
        file.read_range(0, 6, &CancellationToken::new())?.value,
        Bytes::from_static(b"before")
    );
    Ok(())
}

fn read_failure(file: &dyn DemandFile) -> Option<DemandError> {
    file.read_range(0, 6, &CancellationToken::new())
        .err()
        .map(|failure| failure.error)
}

#[tokio::test]
async fn held_files_under_remote_roots_read_off_the_callback_thread() -> Result<(), Box<dyn Error>>
{
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("file"), b"remote")?;
    let source = native_source(temporary.path()).await?.remote();
    let reference = source.reference();
    let cancellation = CancellationToken::new();
    let node = lookup_node(&source, reference, &path("/file")?, &cancellation).await?;
    let file = source
        .open_file(reference, &path("/file")?, node.version, &cancellation)
        .await?
        .value;
    let read = file.read_range(0, 6, &CancellationToken::new())?;
    assert_eq!(read.value.as_ref(), b"remote");
    std::fs::write(temporary.path().join("file"), b"edited again")?;
    assert!(matches!(
        read_failure(file.as_ref()),
        Some(DemandError::StaleVersion)
    ));
    Ok(())
}

#[tokio::test]
async fn held_file_reads_detect_in_place_modification() -> Result<(), Box<dyn Error>> {
    use std::io::Write as _;
    let (temporary, _source, file) = held_file(b"before").await?;
    let read = file.read_range(0, 6, &CancellationToken::new())?;
    assert_eq!(read.value.as_ref(), b"before");
    std::fs::OpenOptions::new()
        .append(true)
        .open(temporary.path().join("source/file"))?
        .write_all(b" and after")?;
    assert!(matches!(
        read_failure(file.as_ref()),
        Some(DemandError::StaleVersion)
    ));
    Ok(())
}

#[tokio::test]
async fn held_file_reads_detect_path_replacement() -> Result<(), Box<dyn Error>> {
    let (temporary, _source, file) = held_file(b"before").await?;
    let root = temporary.path().join("source");
    std::fs::rename(root.join("file"), root.join("moved"))?;
    std::fs::write(root.join("file"), b"before")?;
    assert!(matches!(
        read_failure(file.as_ref()),
        Some(DemandError::StaleVersion)
    ));
    Ok(())
}

#[tokio::test]
async fn held_file_reads_detect_invalidation() -> Result<(), Box<dyn Error>> {
    let (_temporary, source, file) = held_file(b"before").await?;
    source.invalidate();
    assert!(matches!(
        read_failure(file.as_ref()),
        Some(DemandError::StaleSource)
    ));
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn held_file_reads_detect_root_replacement() -> Result<(), Box<dyn Error>> {
    let (temporary, _source, file) = held_file(b"before").await?;
    let root = temporary.path().join("source");
    std::fs::rename(&root, temporary.path().join("moved"))?;
    std::fs::create_dir(&root)?;
    std::fs::write(root.join("file"), b"before")?;
    assert!(matches!(
        read_failure(file.as_ref()),
        Some(DemandError::SourceUnavailable)
    ));
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn reads_never_reopen_the_root() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::PermissionsExt as _;
    let (temporary, source, file) = held_file(b"before").await?;
    let root = temporary.path().join("source");
    let reference = source.reference();
    let cancellation = CancellationToken::new();
    let version = lookup_node(&source, reference, &path("/file")?, &cancellation)
        .await?
        .version;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o100))?;
    let held = file.read_range(0, 6, &cancellation);
    let by_path = source
        .read_range(reference, &path("/file")?, version, 0, 6, &cancellation)
        .await;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755))?;
    assert_eq!(held?.value.as_ref(), b"before");
    assert_eq!(by_path?.value.as_ref(), b"before");
    Ok(())
}

#[tokio::test]
async fn dropping_request_cancels_admitted_worker() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    let source = native_source(root.path()).await?;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
    let request = tokio::spawn(async move {
        source
            .wait_for_cancelled_worker(started_tx, stopped_tx)
            .await;
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), started_rx).await??;
    request.abort();
    let _ = request.await;
    tokio::time::timeout(std::time::Duration::from_secs(1), stopped_rx).await??;
    Ok(())
}

#[tokio::test]
async fn version_and_cursor_evidence_fail_closed() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("directory"))?;
    std::fs::write(root.path().join("file"), b"old")?;
    let source = native_source(root.path()).await?;
    let reference = source.reference();
    let cancellation = CancellationToken::new();
    let node = lookup_node(&source, reference, &path("/file")?, &cancellation).await?;
    std::fs::write(root.path().join("file"), b"newer contents")?;
    let Err(stale) = source
        .read_range(
            reference,
            &path("/file")?,
            node.version,
            0,
            3,
            &cancellation,
        )
        .await
    else {
        return Err("stale source content was accepted".into());
    };
    assert!(matches!(stale.error, DemandError::StaleVersion));
    assert!(matches!(
        source
            .read_metadata(reference, &path("/file")?, Some(node.version), &cancellation)
            .await,
        Err(failure) if matches!(failure.error, DemandError::StaleVersion)
    ));

    let first = source
        .list_page(reference, &path("/")?, None, 1, &cancellation)
        .await?;
    let cursor = first.value.next.ok_or("second root entry missing")?;
    assert!(matches!(
        source
            .list_page(
                reference,
                &path("/directory")?,
                Some(cursor),
                1,
                &cancellation
            )
            .await,
        Err(failure) if matches!(failure.error, DemandError::StaleCursor)
    ));
    source.invalidate();
    assert!(matches!(
        source
            .lookup(reference, &path("/file")?, &cancellation)
            .await,
        Err(failure) if matches!(failure.error, DemandError::StaleSource)
    ));
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn native_lookup_preserves_non_utf8_posix_names() -> Result<(), Box<dyn Error>> {
    use crate::kernel::{LogicalName, NameEncoding};
    use std::os::unix::ffi::OsStringExt;

    let raw = vec![b'n', 0xff];
    let name = LogicalName::new(
        NameEncoding::PosixBytes,
        raw.clone(),
        VolumeLimits::default().maximum_component_bytes,
    )?;
    assert_lossless_name(
        FilesystemProfile::Posix,
        std::ffi::OsString::from_vec(raw),
        name,
    )
    .await
}

#[cfg(windows)]
#[tokio::test]
async fn native_lookup_preserves_utf16_windows_names() -> Result<(), Box<dyn Error>> {
    use crate::kernel::{LogicalName, NameEncoding};

    let raw = "snowman-☃"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect();
    let name = LogicalName::new(
        NameEncoding::WindowsUtf16Le,
        raw,
        VolumeLimits::default().maximum_component_bytes,
    )?;
    assert_lossless_name(
        FilesystemProfile::Windows,
        std::ffi::OsString::from("snowman-☃"),
        name,
    )
    .await
}

#[cfg(windows)]
#[tokio::test]
async fn native_lookup_accepts_windows_names_longer_than_127_code_units()
-> Result<(), Box<dyn Error>> {
    use crate::kernel::{LogicalName, NameEncoding};

    let host_name = "x".repeat(200);
    let limits = crate::model::VolumeConfig::native(crate::model::Lifecycle::Durable).limits;
    let raw = host_name
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect();
    let name = LogicalName::new(
        NameEncoding::WindowsUtf16Le,
        raw,
        limits.maximum_component_bytes,
    )?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join(&host_name), b"data")?;
    let source =
        NativeDemandSource::open(root.path(), FilesystemProfile::Windows, limits, None, None)
            .await?;
    let exact = NamespacePath::new(vec![name], limits)?;
    assert!(
        source
            .lookup(source.reference(), &exact, &CancellationToken::new())
            .await?
            .value
            .is_some()
    );
    Ok(())
}

#[cfg(any(target_os = "linux", windows))]
async fn assert_lossless_name(
    profile: FilesystemProfile,
    host_name: std::ffi::OsString,
    name: crate::kernel::LogicalName,
) -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join(host_name), b"data")?;
    let source =
        NativeDemandSource::open(root.path(), profile, VolumeLimits::default(), None, None).await?;
    let exact = NamespacePath::new(vec![name], VolumeLimits::default())?;
    let node = node(
        &source,
        source.reference(),
        &exact,
        &CancellationToken::new(),
    )
    .await?;
    let answer = source
        .read_range(
            source.reference(),
            &exact,
            node.version,
            0,
            4,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(answer.value.as_ref(), b"data");
    Ok(())
}

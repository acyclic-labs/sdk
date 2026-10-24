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
    fn observe_directory(
        &self,
        directory: &NamespacePath,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        accounting.check().map_err(DemandError::from)?;
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

#[tokio::test]
async fn name_search_metadata_token_detects_a_write_with_restored_mtime()
-> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    let file = root.path().join(".gitignore");
    std::fs::write(&file, b"before\n")?;
    let source = native_source(root.path()).await?;
    let stop = CancellationToken::new();
    let name = path("/.gitignore")?;
    let before = lookup_node(&source, source.reference(), &name, &stop).await?;
    let metadata = std::fs::metadata(&file)?;
    std::fs::write(&file, b"after!\n")?;
    std::fs::OpenOptions::new()
        .write(true)
        .open(&file)?
        .set_times(std::fs::FileTimes::new().set_modified(metadata.modified()?))?;
    let after_metadata = std::fs::metadata(&file)?;
    let after = lookup_node(&source, source.reference(), &name, &stop).await?;
    // Negative control: size, identity and mtime cannot distinguish this
    // offline policy edit. Windows must also include its native ChangeTime.
    assert_eq!(metadata.len(), after_metadata.len());
    assert_eq!(metadata.modified()?, after_metadata.modified()?);
    assert_eq!(before.file_identity, after.file_identity);
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        assert_eq!(metadata.created()?, after_metadata.created()?);
        assert_eq!(metadata.file_attributes(), after_metadata.file_attributes());
    }
    assert_ne!(before.version, after.version);
    Ok(())
}

#[tokio::test]
async fn name_search_mount_check_uses_the_nearest_existing_directory() -> Result<(), Box<dyn Error>>
{
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("nested-🦀"))?;
    std::fs::write(root.path().join("nested-🦀/file"), b"metadata only")?;
    let source = native_source(root.path()).await?;
    let stop = CancellationToken::new();
    for name in [
        "/",
        "/nested-🦀",
        "/absent/deep/leaf",
        "/nested-🦀/absent/leaf",
        "/nested-🦀/file/child",
    ] {
        assert!(
            source
                .same_volume(
                    source.reference(),
                    &path(name)?,
                    WorkBudget::UNBOUNDED,
                    &stop
                )
                .await?
                .value
        );
    }
    // Negative control: checking only the requested path cannot classify an
    // authored directory that does not yet exist in the native source.
    assert_eq!(
        std::fs::metadata(root.path().join("absent/deep/leaf"))
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotFound
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn name_search_native_cookie_resumes_after_source_restart_without_replaying_entries()
-> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    for ordinal in 0..47 {
        std::fs::write(
            root.path().join(format!("file-{ordinal:03}")),
            b"never read",
        )?;
    }
    let stop = CancellationToken::new();
    let directory = path("/")?;
    let mut provider = native_source(root.path()).await?;
    let reference = provider.reference();
    let mut cursor = None;
    let mut seen = std::collections::BTreeSet::new();
    let mut last_cursor = None;
    loop {
        if seen.len() == 3 {
            // Cancellation can retain a larger retry page. A smaller retry
            // must checkpoint the accepted slice, not the reader's lookahead.
            let mut abandoned = provider
                .list_page(
                    reference,
                    &directory,
                    cursor.clone(),
                    7,
                    WorkBudget::UNBOUNDED,
                    &stop,
                )
                .await?;
            abandoned
                .value
                .publication
                .as_mut()
                .ok_or("publication missing")?
                .abort(&mut crate::kernel::KernelAccounting::standalone(
                    WorkBudget::UNBOUNDED,
                    &stop,
                ))?;
        }
        let mut page = provider
            .list_page(
                reference,
                &directory,
                cursor,
                3,
                WorkBudget::UNBOUNDED,
                &stop,
            )
            .await?;
        assert!(page.work.source_entries_visited <= 4);
        assert_eq!(page.work.source_bytes_read, 0);
        for entry in &page.value.entries {
            assert!(
                seen.insert(entry.name.as_bytes().to_vec()),
                "replayed a prior entry"
            );
        }
        page.value
            .publication
            .as_mut()
            .ok_or("publication missing")?
            .accept(&mut crate::kernel::KernelAccounting::standalone(
                WorkBudget::UNBOUNDED,
                &stop,
            ))?;
        cursor = page.value.next.take();
        if cursor.is_none() {
            break;
        }
        last_cursor = cursor.clone();
        if seen.len() == 3 {
            // Keep this handle for the cancellation/smaller-retry case;
            // subsequent boundaries all reopen the source.
            continue;
        }
        drop(page);
        drop(provider);
        provider = NativeDemandSource::open(
            std::borrow::Cow::Borrowed(root.path()),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            Some(reference),
            None,
            WorkBudget::UNBOUNDED,
            &stop,
        )
        .await?
        .value;
    }
    assert_eq!(seen.len(), 47);

    // Without a durable cookie the old in-memory handle cannot be resumed.
    let mut missing = last_cursor.clone().ok_or("continuation missing")?;
    missing.resume = None;
    assert!(
        matches!(provider.list_page(reference, &directory, Some(missing), 3,
        WorkBudget::UNBOUNDED, &stop).await, Err(failure)
        if matches!(failure.error, DemandError::StaleCursor))
    );

    // Directory mutation invalidates the cookie, including an offline write.
    std::fs::write(root.path().join("offline-new"), b"new")?;
    assert!(
        matches!(provider.list_page(reference, &directory, last_cursor, 3,
        WorkBudget::UNBOUNDED, &stop).await, Err(failure)
        if matches!(failure.error, DemandError::StaleCursor))
    );
    Ok(())
}

async fn native_source(
    path: impl AsRef<std::path::Path>,
) -> Result<NativeDemandSource, DemandError> {
    NativeDemandSource::open(
        std::borrow::Cow::Borrowed(path.as_ref()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)
}

#[derive(Default)]
struct RecordedChanges(Mutex<Vec<SourceChange>>);

impl SourceChangeSink for RecordedChanges {
    fn source_changed(
        &self,
        changes: &[SourceChange],
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        accounting.check().map_err(DemandError::from)?;
        if let Ok(mut recorded) = self.0.lock() {
            recorded.extend_from_slice(changes);
        }
        Ok(())
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
        watch.fence(&mut crate::kernel::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ))?;
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
        .watch(
            recorded.clone(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )?
        .value
        .ok_or("a local root is watched")?;
    let cancellation = CancellationToken::new();
    let node = lookup_node(&source, source.reference(), &path("/f")?, &cancellation).await?;
    // Reading beneath a directory watches it where the host needs that.
    assert!(
        source
            .lookup(
                source.reference(),
                &path("/d/x")?,
                WorkBudget::UNBOUNDED,
                &cancellation
            )
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
    let source = native_source(root.path())
        .await?
        .exclude(
            vec![path("/.git")?],
            &WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )?
        .value;
    let recorded = Arc::new(RecordedChanges::default());
    let watch = source
        .watch(
            recorded.clone(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )?
        .value
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
        std::borrow::Cow::Borrowed(root.path()),
        FilesystemProfile::Posix,
        VolumeLimits::default(),
        None,
        None,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    let reference = source.reference();
    let cancellation = CancellationToken::new();
    let node = lookup_node(&source, reference, &path("/link")?, &cancellation).await?;

    assert_eq!(node.kind, SourceNodeKind::SymbolicLink);
    assert_eq!(node.logical_bytes, Some(6));
    assert_eq!(
        source
            .read_link(
                reference,
                &path("/link")?,
                node.version,
                6,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value
            .bytes
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
        std::borrow::Cow::Borrowed(root.path()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        Some(observer.clone()),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .value;
    let reference = source.reference();
    source
        .lookup(
            reference,
            &path("/nested/file")?,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
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
        .lookup(
            reference,
            &path("/seen")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let version = node.value.ok_or("seen path was absent")?.version;

    let bytes = provider
        .read_range(
            reference,
            &path("/seen")?,
            version,
            3,
            4,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(bytes.value.bytes.as_ref(), b"3456");

    let mut first = provider
        .list_page(
            reference,
            &path("/")?,
            None,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(first.value.entries.len(), 1);
    first
        .value
        .publication
        .as_mut()
        .ok_or("publication absent")?
        .accept(&mut crate::kernel::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?;
    let continuation = first.value.next.ok_or("second root entry missing")?;
    let second = provider
        .list_page(
            reference,
            &path("/")?,
            Some(continuation),
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
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
        .list_page(
            reference,
            &path("/")?,
            None,
            16,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(page.value.entries.len(), if cfg!(unix) { 5 } else { 4 });
    let mut listed = Vec::new();
    for entry in &page.value.entries {
        let name = entry
            .name
            .unicode_text(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .ok()
            .map(|receipt| receipt.value)
            .ok_or("listed name is not Unicode")?
            .to_string();
        let looked_up = provider
            .lookup(
                reference,
                &path(&format!("/{name}"))?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
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
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| format!("{name}: {failure:?}"))?
            .value;
        let length = node.logical_bytes.ok_or("regular file has no length")?;
        let read = read(file.as_ref(), 0, length, &cancellation)
            .map_err(|failure| format!("{name}: {failure:?}"))?;
        assert_eq!(read.len() as u64, length, "{name}");
    }
    drop(open);
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
        .open_file(
            reference,
            &path("/file")?,
            node.version,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
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
    // An unchanged write opener must not invalidate the pinned version.
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
                    read(file.as_ref(), 0, 6, &CancellationToken::new())
                        .is_ok_and(|read| read.as_ref() == b"before")
                })
            })
            .collect::<Vec<_>>();
        for read in reads {
            assert!(read.join().unwrap_or(false));
        }
    });
    assert_eq!(
        read(file.as_ref(), 0, 6, &CancellationToken::new())?.as_ref(),
        b"before".as_slice()
    );
    Ok(())
}

fn read(
    file: &dyn DemandFile,
    offset: u64,
    length: u64,
    stop: &CancellationToken,
) -> Result<acyclic_stream::OwnedBytes, DemandError> {
    file.read_range(
        offset,
        length,
        &mut crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, stop),
    )
    .map(|chunk| chunk.bytes)
}

fn read_failure(file: &dyn DemandFile) -> Option<DemandError> {
    read(file, 0, 6, &CancellationToken::new()).err()
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
        .open_file(
            reference,
            &path("/file")?,
            node.version,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let read = read(file.as_ref(), 0, 6, &CancellationToken::new())?;
    assert_eq!(read.as_ref(), b"remote");
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
    let read = read(file.as_ref(), 0, 6, &CancellationToken::new())?;
    assert_eq!(read.as_ref(), b"before");
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
    let held = read(file.as_ref(), 0, 6, &cancellation);
    let by_path = source
        .read_range(
            reference,
            &path("/file")?,
            version,
            0,
            6,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755))?;
    assert_eq!(held?.as_ref(), b"before");
    assert_eq!(by_path?.value.bytes.as_ref(), b"before");
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
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
    else {
        return Err("stale source content was accepted".into());
    };
    assert!(matches!(stale.error, DemandError::StaleVersion));
    assert!(matches!(
        source
            .read_metadata(reference, &path("/file")?, Some(node.version), WorkBudget::UNBOUNDED, &cancellation)
            .await,
        Err(failure) if matches!(failure.error, DemandError::StaleVersion)
    ));

    let mut first = source
        .list_page(
            reference,
            &path("/")?,
            None,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    first
        .value
        .publication
        .as_mut()
        .ok_or("publication absent")?
        .accept(&mut crate::kernel::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?;
    let cursor = first.value.next.ok_or("second root entry missing")?;
    assert!(matches!(
        source
            .list_page(reference, &path("/directory")?, Some(cursor), 1, WorkBudget::UNBOUNDED, &cancellation)
            .await,
        Err(failure) if matches!(failure.error, DemandError::StaleCursor)
    ));
    source.invalidate();
    assert!(matches!(
        source
            .lookup(reference, &path("/file")?, WorkBudget::UNBOUNDED, &cancellation)
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
    let source = NativeDemandSource::open(
        std::borrow::Cow::Borrowed(root.path()),
        FilesystemProfile::Windows,
        limits,
        None,
        None,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .value;
    let exact = NamespacePath::new(vec![name], limits)?;
    assert!(
        source
            .lookup(
                source.reference(),
                &exact,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
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
    let source = NativeDemandSource::open(
        std::borrow::Cow::Borrowed(root.path()),
        profile,
        VolumeLimits::default(),
        None,
        None,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .value;
    let exact = NamespacePath::new(vec![name], VolumeLimits::default())?;
    let node = lookup_node(
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
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(answer.value.bytes.as_ref(), b"data");
    Ok(())
}

/// Binds a source to a root held before its path was replaced, or by path
/// after the replacement when `by_path`, and reports which root it serves.
async fn bound_across_replacement(by_path: bool) -> Result<bool, Box<dyn Error>> {
    let parent = tempfile::tempdir()?;
    let checkout = parent.path().join("checkout");
    std::fs::create_dir(&checkout)?;
    let held = crate::hold_root(&checkout)?;
    let original = held.identity();
    // A replacement in the window between capture and binding; a host that
    // refuses to move a held root leaves nothing to replace.
    if std::fs::rename(&checkout, parent.path().join("moved")).is_ok() {
        std::fs::create_dir(&checkout)?;
        assert_ne!(crate::capture_root_identity(&checkout)?, original);
    }
    let source = if by_path {
        native_source(&checkout).await?
    } else {
        NativeDemandSource::open_held(
            held,
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            None,
            None,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?
    };
    Ok(source.root_identity() == original)
}

#[tokio::test]
async fn a_held_root_binds_the_root_it_captured_across_a_replacement() {
    assert!(bound_across_replacement(false).await.unwrap());
}

#[tokio::test]
async fn negative_control_binding_by_path_attaches_the_replacement() {
    let replaced = !bound_across_replacement(true).await.unwrap();
    assert!(replaced || cfg!(windows));
}

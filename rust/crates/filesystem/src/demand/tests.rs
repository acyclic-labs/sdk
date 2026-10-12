use super::native::NativeDemandSource;
use super::*;
use crate::model::{FilesystemProfile, VolumeLimits};
use std::error::Error;
use std::sync::{Arc, Mutex};
struct OrdinaryNativeSource(NativeDemandSource);

impl DemandSource for OrdinaryNativeSource {
    fn reference(&self) -> SourceReference { self.0.reference() }
    async fn lookup<W: crate::facade::FsWork + Send>(
        &self, source: SourceReference, path: &NamespacePath, work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<SourceNode>, DemandError> {
        let accounting = work.accounting(cancellation);
        if let Err(error) = ordinary_source_budget(&accounting) { return work.finish(Err(error), &accounting); }
        drop(accounting);
        self.0.lookup(source, path, work, cancellation).await
    }
    async fn list_page<W: crate::facade::FsWork + Send>(&self, source: SourceReference, directory: &NamespacePath,
        cursor: Option<SourceCursor>, maximum_entries: u32, work: W,
        cancellation: &CancellationToken) -> W::Output<SourceDirectoryPage, DemandError> {
        let accounting = work.accounting(cancellation);
        if let Err(error) = ordinary_source_budget(&accounting) { return work.finish(Err(error), &accounting); }
        drop(accounting);
        self.0.list_page(source, directory, cursor, maximum_entries, work, cancellation).await
    }
    async fn read_range<W: crate::facade::FsWork + Send>(&self, source: SourceReference, path: &NamespacePath,
        expected: SourceVersion, offset: u64, length: u64, work: W,
        cancellation: &CancellationToken) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
        let accounting = work.accounting(cancellation);
        if let Err(error) = ordinary_source_budget(&accounting) { return work.finish(Err(error), &accounting); }
        drop(accounting);
        self.0.read_range(source, path, expected, offset, length, work, cancellation).await
    }
    async fn read_link<W: crate::facade::FsWork + Send>(&self, source: SourceReference, path: &NamespacePath,
        expected: SourceVersion, maximum_bytes: u64, work: W,
        cancellation: &CancellationToken) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
        let accounting = work.accounting(cancellation);
        if let Err(error) = ordinary_source_budget(&accounting) { return work.finish(Err(error), &accounting); }
        drop(accounting);
        self.0.read_link(source, path, expected, maximum_bytes, work, cancellation).await
    }
    async fn open_file<W: crate::facade::FsWork + Send>(&self, source: SourceReference, path: &NamespacePath,
        expected: SourceVersion, work: W,
        cancellation: &CancellationToken) -> W::Output<Box<dyn DemandFile>, DemandError> {
        let accounting = work.accounting(cancellation);
        if let Err(error) = ordinary_source_budget(&accounting) { return work.finish(Err(error), &accounting); }
        drop(accounting);
        self.0.open_file(source, path, expected, work, cancellation).await
    }
}

#[tokio::test]
async fn unknown_original_source_preserves_precancellation_without_io()
-> Result<(), Box<dyn Error>> {
    use crate::materializer::NativeMaterializationWork;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("actual"), b"actual")?;
    let source = OrdinaryNativeSource(NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()), FilesystemProfile::Portable, VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?);
    let requested = path("/actual")?;
    let node = source.lookup(source.reference(), &requested, WorkBudget::UNBOUNDED,
        &CancellationToken::new()).await?.value.ok_or("actual file absent")?;
    let cancellation = CancellationToken::new();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.backend_read_operations = 0;
    budget.source_bytes_read = 0;
    budget.bytes_copied = 0;
    let work = NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)?;
    let before = work.receipt();
    for cancelled in [false, true] {
        if cancelled { cancellation.cancel(); }
        let errors = [
            source.lookup(source.reference(), &requested, &work, &cancellation).await.err(),
            source.list_page(source.reference(), &path("/")?, None, 1, &work, &cancellation).await.err(),
            source.read_range(source.reference(), &requested, node.version, 0, 6, &work, &cancellation).await.err(),
            source.read_link(source.reference(), &requested, node.version, 6, &work, &cancellation).await.err(),
            source.open_file(source.reference(), &requested, node.version, &work, &cancellation).await.err(),
            source.read_metadata(source.reference(), &requested, None, &work, &cancellation).await.err(),
        ];
        for error in errors {
            assert!(matches!((cancelled, error),
                (false, Some(DemandError::NativeWorkUnavailable)) |
                (true, Some(DemandError::Cancelled))));
        }
        assert_eq!(work.receipt(), before);
    }
    Ok(())
}

#[test]
fn unknown_original_directory_observer_fails_without_ordinary_callback()
-> Result<(), Box<dyn Error>> {
    use crate::materializer::NativeMaterializationWork;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let observer = RecordingObserver::default();
    let before = work.receipt();
    assert!(matches!(observer.observe_directory_with_native_work(
        &path("/")?, NativeSourceWork::new(work.native_read_meter()),
    ), Err(DemandError::NativeWorkUnavailable)));
    assert!(observer.directories.lock().map_err(|_| "observer lock poisoned")?.is_empty());
    assert_eq!(work.receipt(), before);
    cancellation.cancel();
    assert!(matches!(observer.observe_directory_with_native_work(
        &path("/")?, NativeSourceWork::new(work.native_read_meter()),
    ), Err(DemandError::Cancelled)));
    assert!(observer.directories.lock().map_err(|_| "observer lock poisoned")?.is_empty());
    assert_eq!(work.receipt(), before);
    Ok(())
}

#[tokio::test]
async fn real_native_lookup_miss_does_not_poison_the_original_next_read()
    -> Result<(), Box<dyn Error>>
{
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("present"), b"real source")?;
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(temporary.path()).to_path_buf()), FilesystemProfile::Portable, VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let budget = finite_watch_budget()?;
    let original = crate::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)?;
    assert!(source.lookup(
        source.reference(), &path("/absent")?, &original, &cancellation,
    ).await?.is_none());
    assert!(source.lookup(
        source.reference(), &path("/present")?, &original, &cancellation,
    ).await?.is_some());
    original.receipt().verify(budget)?;
    Ok(())
}

#[derive(Default)]
struct RecordingObserver {
    directories: Mutex<Vec<NamespacePath>>,
}

impl DemandDirectoryObserver for RecordingObserver {
    fn observe_directory(
        &self, directory: &NamespacePath, _budget: WorkBudget, cancellation: &CancellationToken,
    ) -> DemandResult<()> {
        let result = (|| {
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            self.directories.lock().map_err(|_| DemandError::SourceUnavailable)?.push(directory.clone());
            Ok(())
        })();
        result.map(|value| OperationReceipt { value, work: WorkCounters::UNCHARGED })
            .map_err(OperationFailure::before_work)
    }
}

fn finite_watch_budget() -> Result<WorkBudget, Box<dyn Error>> {
    let mut value = serde_json::to_value(WorkBudget::UNBOUNDED)?;
    if let serde_json::Value::Object(fields) = &mut value {
        for allowance in fields.values_mut() { *allowance = serde_json::json!(1_000_000_u64); }
    }
    Ok(serde_json::from_value(value)?)
}

fn path(value: &str) -> Result<NamespacePath, Box<dyn Error>> {
    let portable = crate::path::PortablePath::parse(value, VolumeLimits::default())?;
    Ok(NamespacePath::from_portable(
        &portable,
        VolumeLimits::default(),
    )?)
}

#[derive(Default)]
struct RecordedChanges(Mutex<Vec<SourceChange>>);

impl SourceChangeSink for RecordedChanges {
    fn source_changed(
        &self, changes: &[SourceChange], _budget: WorkBudget, cancellation: &CancellationToken,
    ) -> DemandResult<()> {
        cancellation.check().map_err(|_| OperationFailure::before_work(DemandError::Cancelled))?;
        if let Ok(mut recorded) = self.0.lock() { recorded.extend_from_slice(changes); }
        Ok(OperationReceipt { value: (), work: WorkCounters::UNCHARGED })
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
        fence_watch(watch, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut changes = self.take()?;
        while !reported(&changes) && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
            changes.extend(self.take()?);
        }
        Ok(changes)
    }
}

#[tokio::test]
async fn native_source_watch_fence_honors_original_pre_cancellation() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    let source = NativeDemandSource::open(std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()), FilesystemProfile::Portable, VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let subscription_cancellation = CancellationToken::new();
    let recorded = Arc::new(RecordedChanges::default());
    let subscribed = source.watch(recorded, WorkBudget::UNBOUNDED, &subscription_cancellation)?;
    let watch = subscribed.value.ok_or("native source is watched")?;
    assert!(watch.logical_owned_bytes() > 0);
    assert!(watch.logical_owned_bytes() <= subscribed.work.peak_allocation_bytes);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = fence_watch(watch.as_ref(), WorkBudget::default(), &cancellation)
        .expect_err("pre-cancelled fence must not start host work");
    assert!(matches!(failure.error, DemandError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::UNCHARGED);
    let failure = stop_watch(watch.as_ref(), WorkBudget::default(), &cancellation)
        .expect_err("pre-cancelled stop retains the original live subscription");
    assert!(matches!(failure.error, DemandError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::UNCHARGED);
    fence_watch(watch.as_ref(), WorkBudget::UNBOUNDED, &subscription_cancellation)?;
    let failure = stop_watch(watch.as_ref(), WorkBudget::default(), &subscription_cancellation)
        .expect_err("native quiescence is admitted before starting cleanup");
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(*failure.work, WorkCounters::UNCHARGED);
    fence_watch(watch.as_ref(), WorkBudget::UNBOUNDED, &subscription_cancellation)?;
    stop_watch(watch.as_ref(), WorkBudget::UNBOUNDED, &subscription_cancellation)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn native_source_reports_the_names_and_nodes_it_changed() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("d"))?;
    std::fs::write(root.path().join("f"), b"one")?;
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    )
    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let recorded = Arc::new(RecordedChanges::default());
    let cancellation = CancellationToken::new();
    let watch = source
        .watch(recorded.clone(), WorkBudget::UNBOUNDED, &cancellation)?
        .value
        .ok_or("a local root is watched")?;
    let node = source.lookup(source.reference(), &path("/f")?, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .ok_or("f is present")?;
    // Reading beneath a directory watches it where the host needs that.
    assert!(
        source
            .lookup(source.reference(), &path("/d/x")?, WorkBudget::UNBOUNDED, &cancellation)
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
async fn a_filtered_source_reports_nothing_beneath_its_exclusions() -> Result<(), Box<dyn Error>>
{
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join(".git"))?;
    let source = FilteredDemandSource::new(
        NativeDemandSource::open(
            std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()),
            FilesystemProfile::Portable,
            VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        )
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        vec![path("/.git")?],
    );
    let recorded = Arc::new(RecordedChanges::default());
    let cancellation = CancellationToken::new();
    let watch = source
        .watch(recorded.clone(), WorkBudget::UNBOUNDED, &cancellation)?
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
async fn a_reused_inode_is_a_new_identity_and_hard_links_share_one()
-> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::MetadataExt as _;

    let root = tempfile::tempdir()?;
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()),
        FilesystemProfile::Posix,
        VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    )
    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let reference = source.reference();
    let cancellation = CancellationToken::new();
    let identity = |name: &'static str| {
        let source = &source;
        let cancellation = &cancellation;
        async move {
            Ok::<_, Box<dyn Error>>(
                source.lookup(reference, &path(name)?, WorkBudget::UNBOUNDED, cancellation)
                    .await?
                    .value
                    .ok_or("file was absent")?
                    .file_identity,
            )
        }
    };
    std::fs::write(root.path().join("first"), b"first")?;
    std::fs::hard_link(root.path().join("first"), root.path().join("alias"))?;
    let first = identity("/first").await?;
    assert_eq!(identity("/alias").await?, first);

    let inode = std::fs::metadata(root.path().join("first"))?.ino();
    let born = std::fs::metadata(root.path().join("first"))?.created().ok();
    std::fs::remove_file(root.path().join("first"))?;
    std::fs::remove_file(root.path().join("alias"))?;
    // Hosts reuse a freed inode soon, but not always at once.
    for attempt in 0..64 {
        let name = format!("later-{attempt}");
        std::fs::write(root.path().join(&name), b"later")?;
        let metadata = std::fs::metadata(root.path().join(&name))?;
        if metadata.ino() == inode && born.is_some() && metadata.created().ok() != born {
            let later = source.lookup(reference, &path(&format!("/{name}"))?, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .ok_or("file was absent")?
                .file_identity;
            assert_ne!(later, first, "the reused inode names a new file");
            break;
        }
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn native_source_reports_symbolic_link_target_bytes() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    std::os::unix::fs::symlink("target", root.path().join("link"))?;
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()),
        FilesystemProfile::Posix,
        VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    )
    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let reference = source.reference();
    let cancellation = CancellationToken::new();
    let node = source.lookup(reference, &path("/link")?, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .ok_or("symbolic link was absent")?;

    assert_eq!(node.kind, SourceNodeKind::SymbolicLink);
    assert_eq!(node.logical_bytes, Some(6));
    assert_eq!(
        source
            .read_link(reference, &path("/link")?, node.version, u64::MAX, WorkBudget::UNBOUNDED, &cancellation)
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
        std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(), None, Some(observer.clone()), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    )
    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let reference = source.reference();
    source.lookup(reference, &path("/nested/file")?, WorkBudget::UNBOUNDED, &CancellationToken::new())
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

    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    )
    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let provider = &source;
    let reference = provider.reference();
    let cancellation = CancellationToken::new();
    let node = provider.lookup(reference, &path("/seen")?, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    assert_eq!(node.work.source_path_components, 1);
    assert_eq!(node.work.source_entries_visited, 0);
    assert_eq!(node.work.source_bytes_read, 0);
    let version = node.value.ok_or("seen path was absent")?.version;

    let bytes = provider.read_range(reference, &path("/seen")?, version, 3, 4, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    assert_eq!(bytes.value.bytes.as_ref(), b"3456");
    assert_eq!(bytes.work.source_bytes_read, 4);
    assert_eq!(bytes.work.source_entries_visited, 0);

    let mut first = provider.list_page(reference, &path("/")?, None, 1, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    assert_eq!(first.value.entries.len(), 1);
    assert_eq!(first.work.source_entries_visited, 1);
    accept_publication(&mut first.value.publication, WorkBudget::UNBOUNDED, &cancellation).await?;
    let continuation = first.value.next.ok_or("second root entry missing")?;
    let mut second = provider.list_page(reference, &path("/")?, Some(continuation), 1, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    assert_eq!(second.value.entries.len(), 1);
    assert_eq!(second.work.source_entries_visited, 1);
    accept_publication(&mut second.value.publication, WorkBudget::UNBOUNDED, &cancellation).await?;
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
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    )
    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let provider = &source;
    let reference = provider.reference();
    let cancellation = CancellationToken::new();
    let mut page = provider.list_page(reference, &path("/")?, None, 16, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    assert_eq!(page.value.entries.len(), if cfg!(unix) { 5 } else { 4 });
    accept_publication(&mut page.value.publication, WorkBudget::UNBOUNDED, &cancellation).await?;
    let mut listed = Vec::new();
    for entry in page.value.entries.iter() {
        let name = entry
            .name
            .unicode_text()
            .ok_or("listed name is not Unicode")?
            .to_string();
        let looked_up = provider.lookup(reference, &path(&format!("/{name}"))?, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        assert_eq!(looked_up.value, Some(entry.node), "{name}");
        listed.push((name, entry.node));
    }
    for (name, node) in listed {
        if node.kind != SourceNodeKind::RegularFile {
            continue;
        }
        let file = provider.open_file(reference, &path(&format!("/{name}"))?, node.version, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| format!("{name}: {:?}", failure.error))?
            .value;
        let length = node.logical_bytes.ok_or("regular file has no length")?;
        let read = read_file(&*file, 0, length, WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|failure| format!("{name}: {:?}", failure.error))?;
        assert_eq!(read.value.bytes.len() as u64, length, "{name}");
    }
    drop(open);
    Ok(())
}

#[tokio::test]
async fn native_requests_bound_queue_allocations_and_cancel_before_io() -> Result<(), Box<dyn Error>>
{
    let root = tempfile::tempdir()?;
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    )
    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let permits = source.occupy_all_requests().await?;
    let cancellation = CancellationToken::new();
    let root_path = path("/")?;
    let budget = finite_watch_budget()?;
    let mut no_allocations = budget;
    no_allocations.allocation_operations = 0;
    let failure = source.lookup(source.reference(), &root_path, no_allocations, &cancellation)
        .await.err().ok_or("queue admitted a waiter without its allocation allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    let query = source.lookup(source.reference(), &root_path, budget, &cancellation);
    tokio::pin!(query);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut query)
            .await
            .is_err()
    );
    cancellation.cancel();
    let Err(failure) =
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut query).await?
    else {
        return Err("cancelled request entered the worker".into());
    };
    assert!(matches!(failure.error, DemandError::Cancelled));
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.source_entries_visited, 0);
    assert_eq!(failure.work.source_bytes_read, 0);
    assert!(failure.work.allocation_operations >= 2);
    assert!(failure.work.peak_allocation_bytes > 0);
    assert!(failure.work.items_examined > 0);
    failure.work.verify(budget)?;
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
    let source =
        NativeDemandSource::open(std::borrow::Cow::Owned(std::path::Path::new(&root).to_path_buf()), FilesystemProfile::Portable, VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let reference = source.reference();
    let cancellation = CancellationToken::new();
    let node = source.lookup(reference, &path("/file")?, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .ok_or("file absent")?;
    let file = source.open_file(reference, &path("/file")?, node.version, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    Ok((temporary, source, file))
}

/// A held file gives way to a save over it: replacing it by rename, as
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
                    read_file(&*file, 0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())
                        .is_ok_and(|read| read.value.bytes.as_ref() == b"before")
                })
            })
            .collect::<Vec<_>>();
        for read in reads {
            assert!(read.join().unwrap_or(false));
        }
    });
    assert_eq!(
        read_file(&*file, 0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())?.value.bytes.as_ref(),
        b"before"
    );
    assert_eq!(
        read_file(&*file, 0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())?.value.bytes.as_ref(),
        b"before"
    );
    Ok(())
}

fn read_failure(file: &dyn DemandFile) -> Option<DemandError> {
    read_file(&*file, 0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .err()
        .map(|failure| failure.error)
}

#[tokio::test]
async fn held_files_under_remote_roots_read_off_the_callback_thread()
-> Result<(), Box<dyn Error>> {
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("file"), b"remote")?;
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(temporary.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    )
    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
    .remote();
    let reference = source.reference();
    let cancellation = CancellationToken::new();
    let node = source.lookup(reference, &path("/file")?, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .ok_or("file absent")?;
    let file = source.open_file(reference, &path("/file")?, node.version, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let read = read_file(&*file, 0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
    assert_eq!(read.value.bytes.as_ref(), b"remote");
    std::fs::write(temporary.path().join("file"), b"edited again")?;
    assert!(matches!(
        read_failure(file.as_ref()),
        Some(DemandError::StaleVersion)
    ));
    Ok(())
}

#[tokio::test]
async fn remote_deadline_quiesces_receipts_and_does_not_cancel_reopened_source() -> Result<(), Box<dyn Error>> {
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("file"), b"remote")?;
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(temporary.path()).to_path_buf()), FilesystemProfile::Portable, VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.remote();
    let reference = source.reference();
    let cancellation = CancellationToken::new();
    let budget = WorkBudget {
        backend_read_operations: 1024, backend_write_operations: 1024,
        bytes_hashed: 1024 * 1024, bytes_copied: 1024 * 1024, bytes_encoded: 1024 * 1024,
        source_bytes_read: 1024 * 1024, source_path_components: 128,
        source_entries_visited: 1024, output_bytes: 1024 * 1024,
        items_examined: 4096, items_returned: 1024, allocation_operations: 4096,
        peak_allocation_bytes: 1024 * 1024, ..WorkCounters::default()
    };
    let requested = path("/file")?;
    let node = source.lookup(reference, &requested, budget, &cancellation).await?.value.ok_or("file absent")?;
    match source.read_with_deadline(reference, &requested, node.version, budget, &cancellation, std::time::Duration::ZERO) {
        Ok(receipt) => {
            assert_eq!(receipt.value.bytes.as_ref(), b"remote");
            assert_eq!(receipt.work.source_bytes_read, 6);
            receipt.work.verify(budget)?;
        }
        Err(failure) => {
            assert!(matches!(failure.error, DemandError::SourceUnavailable));
            assert!(failure.work.source_bytes_read == 0 || failure.work.source_bytes_read == 6);
            assert!(failure.work.backend_read_operations > 0);
            assert!(failure.work.allocation_operations > 0);
            failure.work.verify(budget)?;
        }
    }
    assert!(!cancellation.is_cancelled());
    let reopened = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(temporary.path()).to_path_buf()), FilesystemProfile::Portable, VolumeLimits::default(), Some(reference), None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(source.root_identity(), reopened.root_identity());
    let read = reopened.read_range(reference, &requested, node.version, 0, 6, budget, &cancellation).await?;
    assert_eq!(read.value.bytes.as_ref(), b"remote");
    read.work.verify(budget)?;
    cancellation.cancel();
    let failure = reopened.read_range(reference, &requested, node.version, 0, 6, budget, &cancellation)
        .await.err().ok_or("cancelled reopened source read succeeded")?;
    assert!(matches!(failure.error, DemandError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn held_file_reads_detect_source_changes() -> Result<(), Box<dyn Error>> {
    use std::io::Write as _;
    for change in [0, 1, 2] {
        let (temporary, source, file) = held_file(b"before").await?;
        assert_eq!(read_file(&*file, 0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())?.value.bytes.as_ref(), b"before");
        let root = temporary.path().join("source");
        let expected = match change {
            0 => {
                std::fs::OpenOptions::new().append(true).open(root.join("file"))?
                    .write_all(b" and after")?;
                DemandError::StaleVersion
            }
            1 => {
                std::fs::rename(root.join("file"), root.join("moved"))?;
                std::fs::write(root.join("file"), b"before")?;
                DemandError::StaleVersion
            }
            _ => { source.invalidate(&mut DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()))?; DemandError::StaleSource }
        };
        assert_eq!(std::mem::discriminant(&read_failure(file.as_ref()).ok_or("stale read succeeded")?),
            std::mem::discriminant(&expected));
    }
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
    let version = source.lookup(reference, &path("/file")?, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .ok_or("file absent")?
        .version;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o100))?;
    let held = read_file(&*file, 0, 6, WorkBudget::UNBOUNDED, &cancellation);
    let by_path = source.read_range(reference, &path("/file")?, version, 0, 6, WorkBudget::UNBOUNDED, &cancellation)
        .await;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755))?;
    assert_eq!(held?.value.bytes.as_ref(), b"before");
    assert_eq!(by_path?.value.bytes.as_ref(), b"before");
    Ok(())
}

#[tokio::test]
async fn dropping_request_cancels_admitted_worker() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    )
    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
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
async fn cancelled_directory_page_retries_without_skipping_entries()
-> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    for name in ["a", "b", "c", "d", "e", "f"] {
        std::fs::write(root.path().join(name), b"x")?;
    }
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    )
    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let reference = source.reference();
    let directory = path("/")?;
    let live = CancellationToken::new();
    let mut first = source.list_page(reference, &directory, None, 1, WorkBudget::UNBOUNDED, &live)
        .await?;
    accept_publication(&mut first.value.publication, WorkBudget::UNBOUNDED, &live).await?;
    let cursor = first.value.next.ok_or("expected continuation")?;
    let interrupted = CancellationToken::new();
    source.cancel_during_page(reference, &directory, cursor.clone(), &interrupted)?;
    let mut found = first.value.entries.iter().map(|entry| entry.name.clone()).collect::<Vec<_>>();
    let mut next = Some(cursor);
    while let Some(cursor) = next {
        let mut page = source.list_page(reference, &directory, Some(cursor), 2, WorkBudget::UNBOUNDED, &live)
            .await?;
        accept_publication(&mut page.value.publication, WorkBudget::UNBOUNDED, &live).await?;
        found.extend(page.value.entries.iter().map(|entry| entry.name.clone()));
        next = page.value.next;
    }
    let mut names: Vec<_> = found
        .into_iter()
        .map(|name| name.as_bytes().to_vec())
        .collect();
    names.sort();
    assert_eq!(names.len(), 6);
    assert_eq!(names.first().map(Vec::as_slice), Some(&b"a"[..]));
    assert_eq!(names.last().map(Vec::as_slice), Some(&b"f"[..]));
    Ok(())
}

#[tokio::test]
async fn concurrent_use_reports_busy_without_losing_cursor() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    for name in ["a", "b", "c"] {
        std::fs::write(root.path().join(name), b"x")?;
    }
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    )
    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let reference = source.reference();
    let directory = path("/")?;
    let cancellation = CancellationToken::new();
    let mut first = source.list_page(reference, &directory, None, 1, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    accept_publication(&mut first.value.publication, WorkBudget::UNBOUNDED, &cancellation).await?;
    let cursor = first.value.next.ok_or("expected continuation")?;
    let mut lease_work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let lease = source.cursor_lease(
        reference,
        &directory,
        first.value.version,
        Some(cursor.clone()),
        1,
        &mut lease_work,
    )?;
    assert!(matches!(
        source.cursor_lease(
            reference,
            &directory,
            first.value.version,
            Some(cursor.clone()),
            1,
            &mut DemandWork::new(WorkBudget::UNBOUNDED, &cancellation)
        ),
        Err(DemandError::CursorBusy)
    ));
    assert!(matches!(
        source.cursor_lease(
            reference,
            &directory,
            first.value.version,
            None,
            1,
            &mut DemandWork::new(WorkBudget::UNBOUNDED, &cancellation)
        ),
        Err(DemandError::CursorBusy)
    ));
    drop(lease);
    let mut second = source.list_page(reference, &directory, Some(cursor), 1, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    assert_eq!(second.value.entries.len(), 1);
    accept_publication(&mut second.value.publication, WorkBudget::UNBOUNDED, &cancellation).await?;
    Ok(())
}

#[tokio::test]
async fn version_and_cursor_evidence_fail_closed() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("directory"))?;
    std::fs::write(root.path().join("file"), b"old")?;
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    )
    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let reference = source.reference();
    let cancellation = CancellationToken::new();
    let node = source.lookup(reference, &path("/file")?, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .ok_or("file absent")?;
    std::fs::write(root.path().join("file"), b"newer contents")?;
    let Err(stale) = source.read_range(reference, &path("/file")?, node.version, 0, 3, WorkBudget::UNBOUNDED, &cancellation)
        .await
    else {
        return Err("stale source content was accepted".into());
    };
    assert!(matches!(stale.error, DemandError::StaleVersion));
    assert_eq!(stale.work.source_path_components, 1);
    assert_eq!(stale.work.source_bytes_read, 0);
    assert!(matches!(
        source
            .read_metadata(reference, &path("/file")?, Some(node.version), WorkBudget::UNBOUNDED, &cancellation)
            .await,
        Err(failure) if matches!(failure.error, DemandError::StaleVersion)
    ));

    let mut first = source.list_page(reference, &path("/")?, None, 1, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    accept_publication(&mut first.value.publication, WorkBudget::UNBOUNDED, &cancellation).await?;
    let cursor = first.value.next.ok_or("second root entry missing")?;
    assert!(matches!(
        source
            .list_page(
                reference,
                &path("/directory")?,
                Some(cursor),
                1,
                WorkBudget::UNBOUNDED,
                &cancellation
            )
            .await,
        Err(failure) if matches!(failure.error, DemandError::StaleCursor)
    ));
    source.invalidate(&mut DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()))?;
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
    let source =
        NativeDemandSource::open(std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()), FilesystemProfile::Windows, limits, None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let exact = NamespacePath::new(vec![name], limits)?;
    assert!(
        source
            .lookup(source.reference(), &exact, WorkBudget::UNBOUNDED, &CancellationToken::new())
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
        NativeDemandSource::open(std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()), profile, VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let exact = NamespacePath::new(vec![name], VolumeLimits::default())?;
    let node = source.lookup(source.reference(), &exact, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value
        .ok_or("lossless file absent")?;
    let answer = source.read_range(source.reference(), &exact, node.version, 0, 4, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?;
    assert_eq!(answer.value.bytes.as_ref(), b"data");
    Ok(())
}

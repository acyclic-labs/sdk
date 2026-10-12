use super::*;

#[test]
#[ignore = "requires a live Linux FUSE mount"]
pub(super) fn linux_changes_around_the_mount_reach_every_kernel_cache()
-> Result<(), Box<dyn std::error::Error>> {
    let (mounted_source, around) = shared_sources(posix_config())?;
    let mounted_source = Arc::new(mounted_source);
    let file = name("file");
    crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.create_file(&file, FileMetadata::default(), None, work)
    })?;
    around.write_range(&file, 0, Bytes::from_static(b"first"))?;
    let temporary = tempfile::tempdir()?;
    let mut session = mount(
        Arc::clone(&mounted_source) as Arc<dyn MountFilesystem>,
        mounted_source.volume_id()?,
        temporary.path(),
    )?;
    let mounted = temporary.path().join("file");
    let added = temporary.path().join("added");
    // Cache data, a negative entry, and the listing in the kernel.
    assert_eq!(std::fs::read(&mounted)?, b"first");
    assert!(!added.exists());
    assert_eq!(listing(temporary.path())?, ["file"]);

    // Same length, same (unavailable) times: only invalidation can tell.
    around.write_range(&file, 0, Bytes::from_static(b"other"))?;
    crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.create_file(&name("added"), FileMetadata::default(), None, work)
    })?;
    session.revalidate(&fuse_fixture_work())?;
    assert_eq!(std::fs::read(&mounted)?, b"other");
    assert!(added.is_file());
    assert_eq!(listing(temporary.path())?, ["added", "file"]);

    // Without the barrier the change still arrives, promptly.
    crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.remove(&name("added"), None, work)
    })?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while added.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "a removal around the mount never reached the kernel"
        );
        std::thread::yield_now();
    }
    assert!(session.stop()?);
    Ok(())
}

#[test]
#[ignore = "requires a live Linux FUSE mount"]
pub(super) fn linux_an_open_reader_sees_a_write_through_another_handle()
-> Result<(), Box<dyn std::error::Error>> {
    use std::io::{Read as _, Seek as _, Write as _};

    let (inner, _) = shared_sources(posix_config())?;
    let volume_id = inner.volume_id()?;
    let source = Arc::new(inner);
    let temporary = tempfile::tempdir()?;
    let mut session = mount(
        Arc::clone(&source) as Arc<dyn MountFilesystem>,
        volume_id,
        temporary.path(),
    )?;
    let shared = temporary.path().join("shared");
    std::fs::write(&shared, b"before")?;
    assert_eq!(std::fs::read(&shared)?, b"before");
    let mut writer = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&shared)?;
    let mut reader = std::fs::File::open(&shared)?;
    writer.seek(std::io::SeekFrom::End(0))?;
    writer.write_all(b"-after")?;
    writer.sync_all()?;
    let mut observed = Vec::new();
    reader.read_to_end(&mut observed)?;
    assert_eq!(observed, b"before-after");
    drop((writer, reader));
    assert!(session.stop()?);
    Ok(())
}

#[test]
#[ignore = "requires a live Linux FUSE mount"]
pub(super) fn linux_writes_keep_node_facts_exact() -> Result<(), Box<dyn std::error::Error>> {
    let (inner, _) = shared_sources(posix_config())?;
    let volume_id = inner.volume_id()?;
    let temporary = tempfile::tempdir()?;
    let mut session = mount(Arc::new(inner), volume_id, temporary.path())?;
    let written = temporary.path().join("written");
    std::fs::write(&written, b"first")?;
    for body in [b"second write".as_slice(), b"third".as_slice()] {
        let mut file = std::fs::OpenOptions::new().write(true).open(&written)?;
        std::io::Write::write_all(&mut file, body)?;
        drop(file);
        assert_eq!(std::fs::metadata(&written)?.len(), 12);
    }
    assert_eq!(std::fs::read(&written)?, b"thirdd write");
    assert!(session.stop()?);
    Ok(())
}

#[test]
#[ignore = "requires a live Linux FUSE mount"]
pub(super) fn linux_listings_resume_across_pages_and_readers()
-> Result<(), Box<dyn std::error::Error>> {
    let (inner, around) = shared_sources(posix_config())?;
    let volume_id = inner.volume_id()?;
    crate::native_mount::adapter::boundary(inner.cancellation(), |work| {
        inner.create_directory(&name("many"), FileMetadata::default(), work)
    })?;
    let mut expected = (0..700)
        .map(|index| format!("entry-{index:04}"))
        .collect::<Vec<_>>();
    for entry in &expected {
        crate::native_mount::adapter::boundary(inner.cancellation(), |work| {
            inner.create_file(
                &name("many").child(entry.as_bytes().to_vec()),
                FileMetadata::default(),
                None,
                work,
            )
        })?;
    }
    let temporary = tempfile::tempdir()?;
    let mut session = mount(
        Arc::new(inner) as Arc<dyn MountFilesystem>,
        volume_id,
        temporary.path(),
    )?;
    let many = temporary.path().join("many");
    let names = |directory: &std::path::Path| -> std::io::Result<Vec<String>> {
        let mut names = listing(directory)?
            .into_iter()
            .map(|name| name.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        names.sort();
        Ok(names)
    };
    // Several readers list concurrently, each across many kernel pages.
    std::thread::scope(|scope| {
        let readers = (0..4)
            .map(|_| scope.spawn(|| names(&many)))
            .collect::<Vec<_>>();
        for reader in readers {
            let listed = reader.join().map_err(|_| "reader panicked")??;
            assert_eq!(listed, expected);
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    // A change around the mount reaches the cached listing.
    crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.create_file(
            &name("many").child(b"added".to_vec()),
            FileMetadata::default(),
            None,
            work,
        )
    })?;
    session.revalidate(&fuse_fixture_work())?;
    expected.push("added".to_owned());
    expected.sort();
    assert_eq!(names(&many)?, expected);
    assert!(session.stop()?);
    Ok(())
}

#[test]
pub(super) fn only_folding_volumes_with_case_variants_fold_names()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::model::{CaseSensitivity, FilesystemProfile, Lifecycle, VolumeConfig};
    let folds = |profile, case_sensitivity| -> Result<bool, Box<dyn std::error::Error>> {
        let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
        config.profile = profile;
        config.case_sensitivity = case_sensitivity;
        let (source, _) = shared_sources(config)?;
        let folded = source.folded_path(&name("Dir").child(b"MiXeD".to_vec()));
        assert_eq!(
            folded.is_some(),
            source.folded_path(&MountPath::root()).is_some(),
            "folding is a property of the volume, not of one path"
        );
        if let Some(folded) = folded {
            assert_eq!(folded, name("dir").child(b"mixed".to_vec()));
        }
        Ok(source.folded_path(&MountPath::root()).is_some())
    };
    assert!(folds(
        FilesystemProfile::Portable,
        CaseSensitivity::ProfileFolded
    )?);
    assert!(!folds(
        FilesystemProfile::Portable,
        CaseSensitivity::Sensitive
    )?);
    // POSIX names are opaque bytes, so they have no case variants.
    assert!(!folds(
        FilesystemProfile::Posix,
        CaseSensitivity::ProfileFolded
    )?);
    Ok(())
}

#[test]
#[ignore = "requires a live Linux FUSE mount"]
pub(super) fn linux_folded_names_stay_exact_through_the_mount()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::MetadataExt as _;

    let mut config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    config.case_sensitivity = crate::model::CaseSensitivity::ProfileFolded;
    let (source, _) = shared_sources(config)?;
    assert_eq!(
        source.folded_path(&name("Foo")),
        Some(name("foo")),
        "a folding volume keys every spelling alike"
    );
    let volume_id = source.volume_id()?;
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(&name("foo"), FileMetadata::default(), None, work)
    })?;
    let temporary = tempfile::tempdir()?;
    let mut session = mount(Arc::new(source), volume_id, temporary.path())?;
    let at = |name: &str| temporary.path().join(name);
    let exists = |name: &str| at(name).exists();

    // Every spelling resolves to the one node.
    let inode = std::fs::metadata(at("foo"))?.ino();
    assert_eq!(std::fs::metadata(at("FOO"))?.ino(), inode);
    assert_eq!(std::fs::metadata(at("Foo"))?.ino(), inode);
    // A rename through one spelling reaches every other spelling at once.
    std::fs::rename(at("foo"), at("bar"))?;
    assert!(!exists("FOO") && !exists("Foo") && !exists("foo"));
    assert_eq!(std::fs::metadata(at("BAR"))?.ino(), inode);
    // So does a create after absences were looked up in other spellings.
    assert!(!exists("NEW") && !exists("New"));
    std::fs::write(at("new"), b"new")?;
    assert_eq!(std::fs::read(at("NEW"))?, b"new");
    assert_eq!(std::fs::read(at("New"))?, b"new");
    // And a removal through yet another spelling.
    std::fs::remove_file(at("NeW"))?;
    assert!(!exists("new") && !exists("NEW"));
    std::fs::remove_file(at("Bar"))?;
    assert!(!exists("bar") && !exists("BAR"));
    assert!(session.stop()?);
    Ok(())
}

#[test]
#[ignore = "requires a live Linux FUSE mount"]
pub(super) fn linux_rebind_reaches_every_kernel_cache() -> Result<(), Box<dyn std::error::Error>> {
    let (reader, writer) = tracking_sources()?;
    let reader = Arc::new(reader);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let file = name("file");
    crate::native_mount::adapter::boundary(writer.cancellation(), |work| {
        writer.create_file(&file, FileMetadata::default(), None, work)
    })?;
    writer.write_range(&file, 0, Bytes::from_static(b"first"))?;
    writer.sync()?;
    runtime.block_on(reader.advance_to_head_async())?;
    let temporary = tempfile::tempdir()?;
    let mut session = mount(
        Arc::clone(&reader) as Arc<dyn MountFilesystem>,
        reader.volume_id()?,
        temporary.path(),
    )?;
    let mounted = temporary.path().join("file");
    let added = temporary.path().join("added");
    assert_eq!(std::fs::read(&mounted)?, b"first");
    assert!(!added.exists());
    assert_eq!(listing(temporary.path())?, ["file"]);

    writer.write_range(&file, 0, Bytes::from_static(b"other"))?;
    crate::native_mount::adapter::boundary(writer.cancellation(), |work| {
        writer.create_file(&name("added"), FileMetadata::default(), None, work)
    })?;
    writer.sync()?;
    runtime.block_on(reader.advance_to_head_async())?;
    session.revalidate(&fuse_fixture_work())?;
    assert_eq!(std::fs::read(&mounted)?, b"other");
    assert!(added.is_file());
    assert_eq!(listing(temporary.path())?, ["added", "file"]);
    assert!(session.stop()?);
    Ok(())
}

pub(super) struct GatedSource {
    pub(super) inner: Arc<dyn MountFilesystem>,
    pub(super) gated_lookup: Vec<u8>,
    pub(super) gated_directory: Vec<u8>,
    pub(super) entered: SyncSender<()>,
    pub(super) released: Mutex<Receiver<()>>,
}

impl GatedSource {
    pub(super) fn pass(&self, path: &MountPath, gated: &[u8]) {
        if path.components().last().is_some_and(|last| last == gated) {
            let _ = self.entered.send(());
            let released = self
                .released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let _ = released.recv_timeout(Duration::from_secs(10));
        }
    }
}

macro_rules! source_methods {
    ($($name:ident($($arg:ident: $ty:ty),*) $(-> $result:ty)?;)*) => {
        $(fn $name(&self, $($arg: $ty),*) $(-> $result)? {
            self.inner.$name($($arg),*)
        })*
    };
}

impl MountFilesystem for GatedSource {
    source_methods! {
        native_callback_guard() -> Option<Arc<dyn crate::native_mount::NativeMountCallbackGuard>>;
        native_original_work() -> Result<Option<crate::NativeMaterializationWork>, MountSourceError>;
        supports_posix_named_attributes() -> bool;
        flush_on_handle_close() -> bool;
        view_is_stable() -> bool;
        view_stamp() -> Option<ViewStamp>;
        node_unchanged_since(file_id: FileId, stamp: ViewStamp) -> bool;
        unchanged_since(path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp) -> bool;
        listing_unchanged_since(path: &MountPath, stamp: ViewStamp) -> bool;
        observe_view(observer: Weak<dyn ViewObserver>);
        observe_view_with_meter(observer: Weak<dyn ViewObserver>, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        fence_changes_with_meter(work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        acquire_binding_lease_with_meter(epoch: Option<u64>, work: &crate::NativeMaterializationWork) -> Result<Box<dyn Send>, MountSourceError>;
        capture_host_paths_with_meter(root: &Path, paths: &[MountPath], identity: crate::NativeRootIdentity, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        capture_host_subtree_with_meter(root: &Path, path: &MountPath, identity: crate::NativeRootIdentity, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        read_pinned_with_meter(path: &MountPath, pin: crate::native_mount::MountContentPin, offset: u64, length: u64, piece: u32, sink: &mut crate::native_mount::NativeMountContentSink<'_>, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        read_link(path: &MountPath, work: &crate::NativeMaterializationWork) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
        read_range_with_meter(path: &MountPath, offset: u64, length: u32, work: &crate::NativeMaterializationWork) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
        read_directory_with_meter(path: &MountPath, cursor: Option<&[u8]>, maximum: u32, work: &crate::NativeMaterializationWork) -> Result<MountDirectoryPage, MountSourceError>;
        binding_epoch() -> Option<u64>;
        acquire_view_lease() -> Result<Box<dyn Send>, MountSourceError>;
        acquire_binding_lease(expected_epoch: Option<u64>) -> Result<Box<dyn Send>, MountSourceError>;
        open_file(path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError>;
        detach_file(path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError>;
        read_range(path: &MountPath, offset: u64, length: u32) -> Result<acyclic_stream::OwnedBytes, MountSourceError>;
        seek(path: &MountPath, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError>;
        read_directory(path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32) -> Result<MountDirectoryPage, MountSourceError>;
        list_attributes(path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32) -> Result<MountAttributePage, MountSourceError>;
        write_range(path: &MountPath, offset: u64, bytes: Bytes) -> Result<(), MountSourceError>;
        resize(path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError>;
        allocate_range(path: &MountPath, offset: u64, length: u64, operation: MountRangeAllocation) -> Result<(), MountSourceError>;
        clone_range(source: &MountPath, source_offset: u64, destination: &MountPath, destination_offset: u64, length: u64) -> Result<(), MountSourceError>;
        capture_host_path(source_root: &std::path::Path, path: &MountPath) -> Result<(), MountSourceError>;
        capture_host_paths(source_root: &std::path::Path, paths: &[MountPath]) -> Result<(), MountSourceError>;
        capture_host_subtree(source_root: &std::path::Path, path: &MountPath) -> Result<(), MountSourceError>;
        remove(path: &MountPath, expected: Option<FileId>, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        rename(source: &MountPath, destination: &MountPath, replace: bool, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        set_attributes(path: &MountPath, metadata: FileMetadata, logical_bytes: Option<u64>, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        flush(work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        open_file_with_meter(path: &MountPath, work: &crate::NativeMaterializationWork) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
        open_file_with_lookup_with_meter(path: &MountPath, work: &crate::NativeMaterializationWork) -> Result<((Arc<dyn MountOpenFile>, u64), MountLookup), MountSourceError>;
        open_created_with_meter(path: &MountPath, created: &MountLookup, work: &crate::NativeMaterializationWork) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
        detach_file_with_meter(path: &MountPath, work: &crate::NativeMaterializationWork) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
        create_file(path: &MountPath, metadata: FileMetadata, stamp: Option<ViewStamp>, work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError>;
        create_symbolic_link(path: &MountPath, target: crate::kernel::OwnedSourceChunk, metadata: FileMetadata, work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError>;
        create_special(path: &MountPath, kind: MountNodeKind, device: Option<(u32,u32)>, metadata: FileMetadata, work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError>;
        list_attributes_with_meter(path: &MountPath, cursor: Option<&[u8]>, maximum: u32, work: &crate::NativeMaterializationWork) -> Result<(crate::native_mount::MountAttributePage, crate::NativeAllocation), MountSourceError>;
        write_attribute(path: &MountPath, name: &[u8], value: crate::kernel::OwnedSourceChunk, mode: crate::native_mount::MountAttributeWriteMode, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        remove_attribute(path: &MountPath, name: &[u8], work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        clone_range_by_id(source: FileId, source_offset: u64, destination: FileId, destination_offset: u64, length: u64, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        hard_link(source: &MountPath, destination: &MountPath, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        folded_path_with_meter(path: &MountPath, work: &crate::NativeMaterializationWork) -> Result<Option<(MountPath,u64)>, MountSourceError>;
        view_is_stable_with_meter(work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError>;
        view_stamp_with_meter(work: &crate::NativeMaterializationWork) -> Result<Option<ViewStamp>, MountSourceError>;
        binding_epoch_with_meter(work: &crate::NativeMaterializationWork) -> Result<Option<u64>, MountSourceError>;
        unchanged_since_with_meter(path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp, work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError>;
        reported_unchanged_since_with_meter(path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp, work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError>;
        shutdown_with_meter(work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError>;
        node_unchanged_since_with_meter(file_id: FileId, stamp: ViewStamp, work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError>;
        listing_unchanged_since_with_meter(path: &MountPath, stamp: ViewStamp, work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError>;
    }

    fn lookup_with_meter(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<MountLookup>, MountSourceError> {
        self.pass(path, &self.gated_lookup);
        self.inner.lookup_with_meter(path, work)
    }
    fn lookup_pinned_with_meter(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<(MountLookup, Option<crate::native_mount::MountContentPin>)>, MountSourceError>
    {
        self.pass(path, &self.gated_lookup);
        self.inner.lookup_pinned_with_meter(path, work)
    }

    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        self.pass(path, &self.gated_lookup);
        self.inner.lookup(path)
    }

    fn create_directory(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        self.pass(path, &self.gated_directory);
        self.inner.create_directory(path, metadata, work)
    }

    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        self.inner.read_attribute(path, name, work)
    }
}

pub(super) fn finishes_within(limit: Duration, operation: impl FnOnce() + Send + 'static) -> bool {
    let (done, finished) = sync_channel(1);
    std::thread::spawn(move || {
        operation();
        let _ = done.send(());
    });
    finished.recv_timeout(limit).is_ok()
}

#[test]
#[ignore = "requires a live Linux FUSE mount"]
pub(super) fn linux_callbacks_proceed_while_others_block_in_the_source()
-> Result<(), Box<dyn std::error::Error>> {
    let (inner, _) = shared_sources(posix_config())?;
    let volume_id = inner.volume_id()?;
    for directory in ["slow", "busy", "free"] {
        crate::native_mount::adapter::boundary(inner.cancellation(), |work| {
            inner.create_directory(&name(directory), FileMetadata::default(), work)
        })?;
    }
    let present = name("free").child(b"present".to_vec());
    crate::native_mount::adapter::boundary(inner.cancellation(), |work| {
        inner.create_file(&present, FileMetadata::default(), None, work)
    })?;
    inner.write_range(&present, 0, Bytes::from_static(b"present"))?;
    let (entered, entries) = sync_channel(4);
    let (release, released) = sync_channel(4);
    let gated = Arc::new(GatedSource {
        inner: Arc::new(inner),
        gated_lookup: b"blocked-lookup".to_vec(),
        gated_directory: b"blocked-mkdir".to_vec(),
        entered,
        released: Mutex::new(released),
    });
    let temporary = tempfile::tempdir()?;
    let mut session = mount(gated, volume_id, temporary.path())?;
    let root = temporary.path().to_path_buf();

    let slow = root.join("slow").join("blocked-lookup");
    let blocked_lookup = std::thread::spawn(move || slow.exists());
    let busy = root.join("busy").join("blocked-mkdir");
    let blocked_mkdir = std::thread::spawn(move || std::fs::create_dir(busy));
    for _ in 0..2 {
        entries.recv_timeout(Duration::from_secs(5))?;
    }
    let free = root.join("free");
    assert!(
        finishes_within(Duration::from_secs(5), move || {
            assert!(!free.join("absent").exists());
            assert_eq!(
                std::fs::read(free.join("present")).ok().as_deref(),
                Some(b"present".as_slice())
            );
            assert!(listing(&free).is_ok_and(|names| names == ["present"]));
        }),
        "a callback waited on another callback's source work"
    );
    for _ in 0..2 {
        release.send(())?;
    }
    assert!(!blocked_lookup.join().map_err(|_| "lookup panicked")?);
    blocked_mkdir.join().map_err(|_| "mkdir panicked")??;
    assert!(session.stop()?);
    Ok(())
}

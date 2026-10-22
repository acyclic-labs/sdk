use super::*;
use std::io::{Read as _, Write as _};

#[test]
fn a_staged_replacement_keeps_the_identity_guard_and_existing_reader()
-> Result<(), Box<dyn std::error::Error>> {
    replace_with_reader(false, true, b"old contents", b"new contents")
}

#[test]
fn a_read_only_replacement_keeps_the_identity_guard_and_existing_reader()
-> Result<(), Box<dyn std::error::Error>> {
    replace_with_reader(true, false, b"old contents", b"new contents")
}

struct ReplacementDirectories {
    _root: tempfile::TempDir,
    staging: std::path::PathBuf,
    target: std::path::PathBuf,
    next: std::sync::atomic::AtomicUsize,
}

impl ReplacementDirectories {
    fn new() -> std::io::Result<Self> {
        let root = tempfile::tempdir()?;
        let staging = root.path().join("staging");
        let target = root.path().join("target");
        std::fs::create_dir(&staging)?;
        std::fs::create_dir(&target)?;
        Ok(Self {
            _root: root,
            staging,
            target,
            next: Default::default(),
        })
    }
}

fn replace_with_reader(
    destination_read_only: bool,
    replacement_read_only: bool,
    old_contents: &[u8],
    new_contents: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    replace_with_reader_in(
        &ReplacementDirectories::new()?,
        destination_read_only,
        replacement_read_only,
        old_contents,
        new_contents,
    )
}

fn replace_with_reader_in(
    directories: &ReplacementDirectories,
    destination_read_only: bool,
    replacement_read_only: bool,
    old_contents: &[u8],
    new_contents: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .map_err(|(error, _)| error)?;
    let (staging, target) = (&directories.staging, &directories.target);
    let ordinal = directories
        .next
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let leaf = format!("leaf-{ordinal}");
    let new = format!("new-{ordinal}");
    let from = HostRoot::open(staging, &mut &work)?;
    let to = HostRoot::open(target, &mut &work)?;
    std::fs::write(target.join(&leaf), old_contents)?;
    let set_read_only = |path: &std::path::Path, value: bool| -> std::io::Result<()> {
        let mut permissions = std::fs::metadata(path)?.permissions();
        permissions.set_readonly(value);
        std::fs::set_permissions(path, permissions)
    };
    set_read_only(&target.join(&leaf), destination_read_only)?;
    let mut reader = std::fs::File::open(target.join(&leaf))?;
    let identity = handle_state(&reader)?;
    let guard = held_while(&target.join(&leaf), identity)?.ok_or("target identity changed")?;
    let mut replacement = create_ordinary_file(&from.directory, std::ffi::OsStr::new(&new), &work)?;
    replacement.write_all(new_contents)?;
    set_read_only(&staging.join(&new), replacement_read_only)?;
    publish_ordinary_file(
        &replacement,
        &from.directory,
        std::ffi::OsStr::new(&new),
        &to.directory,
        std::ffi::OsStr::new(&leaf),
        Some((&guard, identity)),
        &work,
    )?;
    crate::native_host::keep_windows_created_file(&replacement, &mut &work)?;
    assert!(
        handle_state(&guard)? == identity,
        "the held target changed identity or metadata"
    );
    assert_eq!(std::fs::read(target.join(&leaf))?, new_contents);
    assert_eq!(
        std::fs::metadata(target.join(&leaf))?
            .permissions()
            .readonly(),
        replacement_read_only
    );
    let mut old = Vec::new();
    reader.read_to_end(&mut old)?;
    assert_eq!(old, old_contents);
    drop(replacement);
    assert!(!staging.join(&new).exists());
    assert_eq!(std::fs::read(target.join(&leaf))?, new_contents);
    set_read_only(&target.join(&leaf), false)?;
    drop(reader);
    drop(guard);
    std::fs::remove_file(target.join(&leaf))?;
    Ok(())
}

// Directory setup and teardown dominate repeated NTFS replacements. Retain
// only the parents for this worker; every example owns new leaves and handles.
fn with_replacement_directories<T>(run: impl FnOnce(&ReplacementDirectories) -> T) -> T {
    thread_local! {
        static DIRECTORIES: std::cell::OnceCell<ReplacementDirectories> = const { std::cell::OnceCell::new() };
    }
    DIRECTORIES.with(|slot| run(slot.get_or_init(|| ReplacementDirectories::new().unwrap())))
}

#[test]
fn replacement_examples_reuse_parents_without_retaining_files()
-> Result<(), Box<dyn std::error::Error>> {
    let mut previous = None;
    for (destination_read_only, replacement_read_only, old, new) in [
        (true, false, b"before".as_slice(), b"after".as_slice()),
        (false, true, b"different".as_slice(), b"".as_slice()),
    ] {
        with_replacement_directories(|directories| -> Result<(), Box<dyn std::error::Error>> {
            replace_with_reader_in(
                directories,
                destination_read_only,
                replacement_read_only,
                old,
                new,
            )?;
            let parents = (directories.staging.clone(), directories.target.clone());
            if let Some(previous) = &previous {
                assert_eq!(
                    &parents, previous,
                    "an example recreated its parent directories"
                );
            }
            for parent in [&parents.0, &parents.1] {
                assert_eq!(
                    std::fs::read_dir(parent)?.count(),
                    0,
                    "an example retained a file"
                );
            }
            previous = Some(parents);
            Ok(())
        })?;
    }
    Ok(())
}

proptest::proptest! {
    // For arbitrary payloads and independent destination/source attributes, replacement
    // preserves old readers and guarded metadata while publishing the new bytes.
    #[test]
    fn replacement_preserves_readers_and_publishes_exact_source_bytes(
        old in proptest::collection::vec(proptest::num::u8::ANY, 0..256),
        new in proptest::collection::vec(proptest::num::u8::ANY, 0..256),
        destination_read_only in proptest::bool::ANY,
        replacement_read_only in proptest::bool::ANY,
    ) {
        with_replacement_directories(|directories| {
            replace_with_reader_in(directories, destination_read_only, replacement_read_only, &old, &new)
        }).unwrap();
    }
}

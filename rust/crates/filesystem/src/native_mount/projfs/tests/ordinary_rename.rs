use super::*;
use std::io::{Read as _, Write as _};

#[test]
fn a_staged_replacement_keeps_the_identity_guard_and_existing_reader()
-> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .map_err(|(error, _)| error)?;
    let root = tempfile::tempdir()?;
    let staging = root.path().join("staging");
    let target = root.path().join("target");
    std::fs::create_dir(&staging)?;
    std::fs::create_dir(&target)?;
    let from = HostRoot::open(&staging, &mut &work)?;
    let to = HostRoot::open(&target, &mut &work)?;
    std::fs::write(target.join("leaf"), b"old contents")?;
    let mut reader = std::fs::File::open(target.join("leaf"))?;
    let identity = handle_state(&reader)?;
    let guard = held_while(&target.join("leaf"), identity)?.ok_or("target identity changed")?;
    let mut replacement =
        create_ordinary_file(&from.directory, std::ffi::OsStr::new("new"), &work)?;
    replacement.write_all(b"new contents")?;
    publish_ordinary_file(
        &replacement,
        &from.directory,
        std::ffi::OsStr::new("new"),
        &to.directory,
        std::ffi::OsStr::new("leaf"),
        Some(&guard),
        &work,
    )?;
    crate::native_host::keep_windows_created_file(&replacement, &mut &work)?;
    assert!(
        handle_state(&guard)? == identity,
        "the held target changed identity or metadata"
    );
    assert_eq!(std::fs::read(target.join("leaf"))?, b"new contents");
    let mut old = Vec::new();
    reader.read_to_end(&mut old)?;
    assert_eq!(old, b"old contents");
    drop(replacement);
    assert!(!staging.join("new").exists());
    assert_eq!(std::fs::read(target.join("leaf"))?, b"new contents");
    Ok(())
}

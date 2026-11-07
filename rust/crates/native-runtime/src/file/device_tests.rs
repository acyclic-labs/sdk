use super::*;
use std::os::fd::AsFd as _;

/// The actual owned flush target retains classification across requests. The
/// borrowed target is the negative control: it must classify every request,
/// because its caller can supply a different descriptor on the next call.
#[test]
fn owned_flush_reuses_device_only_for_the_retained_file() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let import = || temporary.reopen().map(HeldFile::new);
    let original = import()?;
    let before = crate::device_flush::classification_work();
    original.sync_once()?;
    let resolved = crate::device_flush::classification_work();
    assert_eq!(resolved, before.map(|count| count + 1));
    for _ in 0..2 {
        original.sync_once()?;
    }
    assert_eq!(crate::device_flush::classification_work(), resolved);

    // The same inode and device do not confer another owner's cached lease.
    let replacement = import()?;
    replacement.sync_once()?;
    let replaced = crate::device_flush::classification_work();
    assert_eq!(replaced, resolved.map(|count| count + 1));
    drop(original);
    replacement.sync_once()?;
    assert_eq!(crate::device_flush::classification_work(), replaced);

    for _ in 0..2 {
        sync_file(replacement.as_file(), Durability::Full, None)?;
    }
    assert_eq!(
        crate::device_flush::classification_work(),
        replaced.map(|count| count + 2),
        "restoring borrowed classification must add two syscalls and a lookup per flush"
    );

    // Importing into the async runtime retains the same shared file owner.
    let native = NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error)?;
    operations::sync_owned_file(&native.file, Durability::Full, None)?;
    let admitted = crate::device_flush::classification_work();
    assert_eq!(admitted, replaced.map(|count| count + 3));
    native.file.sync_once()?;
    operations::sync_owned_file(&native.file, Durability::Full, None)?;
    assert_eq!(crate::device_flush::classification_work(), admitted);

    // A real unsupported descriptor cannot borrow the previous file's success.
    let invalid = HeldFile::new(File::open("/dev/null")?);
    let before = crate::device_flush::classification_work();
    let owned = invalid
        .sync_once()
        .expect_err("device cannot perform a full flush");
    let borrowed = crate::sync_descriptor_once(invalid.as_file().as_fd())
        .expect_err("same original native refusal");
    assert_eq!(owned.raw_os_error(), borrowed.raw_os_error());
    assert_eq!(owned.raw_os_error(), Some(libc::ENODEV));
    assert_eq!(crate::device_flush::classification_work(), before);

    // A public cached lease retains evidence on its physical handle. Its
    // closing flush shares that evidence; a new owner of the same inode does not.
    let cached = crate::CachedFile::new(temporary.reopen()?, temporary.path(), true, true)?;
    let lease = cached.lease()?;
    let before = crate::device_flush::classification_work();
    let completed = lease.flush(|file| file.sync_data(Durability::Barrier))?;
    let after_barrier = crate::device_flush::classification_work();
    assert_eq!(
        after_barrier,
        if completed == Durability::Full {
            before.map(|count| count + 1)
        } else {
            before
        },
        "ordering alone must not classify the cached descriptor"
    );
    lease.flush(|file| file.sync_file(Durability::Full))?;
    let resolved = crate::device_flush::classification_work();
    assert_eq!(
        resolved,
        before.map(|count| count + 1),
        "cached flush reclassified its original physical descriptor"
    );
    lease.flush(|file| file.sync_data(Durability::Full))?;
    lease.flush(|file| file.flush_data())?;
    assert_eq!(
        crate::device_flush::classification_work(),
        resolved,
        "cached flush reclassified its original physical descriptor"
    );
    lease.write(|file| crate::write_all_at(file, 0, b"closing", None))?;
    drop(cached);
    drop(lease);
    assert_eq!(
        crate::device_flush::classification_work(),
        resolved,
        "closing flush discarded its original device evidence"
    );
    let replacement = crate::CachedFile::new(temporary.reopen()?, temporary.path(), true, true)?;
    replacement.lease()?.flush(|file| file.flush_data())?;
    assert_eq!(
        crate::device_flush::classification_work(),
        resolved.map(|count| count + 1),
        "replacement descriptor inherited another physical owner's evidence"
    );

    let invalid = crate::CachedFile::new(
        File::open("/dev/null")?,
        Path::new("/dev/null"),
        false,
        true,
    )?;
    let before = crate::device_flush::classification_work();
    invalid
        .lease()?
        .flush(|file| file.flush_data())
        .expect_err("original cached descriptor cannot perform a full flush");
    assert_eq!(crate::device_flush::classification_work(), before);
    Ok(())
}

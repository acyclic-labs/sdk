use super::*;
use std::os::fd::AsFd as _;

/// The actual owned flush target retains classification across requests. The
/// borrowed target is the negative control: it must classify every request,
/// because its caller can supply a different descriptor on the next call.
#[test]
fn owned_flush_reuses_device_only_for_the_retained_file() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let import = || NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error);
    let original = import()?;
    let before = crate::device_flush::classification_work();
    operations::sync_owned_file(&original.file, Durability::Full, None)?;
    let resolved = crate::device_flush::classification_work();
    assert_eq!(resolved, before.map(|count| count + 1));
    for _ in 0..2 {
        operations::sync_owned_file(&original.file, Durability::Full, None)?;
    }
    assert_eq!(crate::device_flush::classification_work(), resolved);

    // The same inode and device do not confer another owner's cached lease.
    let replacement = import()?;
    operations::sync_owned_file(&replacement.file, Durability::Full, None)?;
    let replaced = crate::device_flush::classification_work();
    assert_eq!(replaced, resolved.map(|count| count + 1));
    drop(original);
    operations::sync_owned_file(&replacement.file, Durability::Full, None)?;
    assert_eq!(crate::device_flush::classification_work(), replaced);

    for _ in 0..2 {
        sync_file(replacement.as_file(), Durability::Full, None)?;
    }
    assert_eq!(
        crate::device_flush::classification_work(),
        replaced.map(|count| count + 2),
        "restoring borrowed classification must add two syscalls and a lookup per flush"
    );

    // A real unsupported descriptor cannot borrow the previous file's success.
    let invalid =
        NativeFile::from_file(File::open("/dev/null")?, None).map_err(|(error, _)| error)?;
    let before = crate::device_flush::classification_work();
    let owned = operations::sync_owned_file(&invalid.file, Durability::Full, None)
        .expect_err("device cannot perform a full flush");
    let borrowed = crate::sync_descriptor_once(invalid.as_file().as_fd())
        .expect_err("same original native refusal");
    assert_eq!(owned.raw_os_error(), borrowed.raw_os_error());
    assert_eq!(owned.raw_os_error(), Some(libc::ENODEV));
    assert_eq!(crate::device_flush::classification_work(), before);
    Ok(())
}

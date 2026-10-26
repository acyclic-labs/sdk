use super::*;

#[test]
fn restored_fixtures_are_independent_of_the_seed_and_each_other() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let source = temporary.path().join("source");
    std::fs::create_dir_all(source.join("nested/empty"))?;
    std::fs::write(source.join("nested/é&%.bin"), b"captured\0\r\n")?;
    let seed = Snapshot::capture(&source)?;
    let first = temporary.path().join("first");
    let second = temporary.path().join("second");
    seed.restore(&first)?;
    seed.restore(&second)?;
    std::fs::write(first.join("nested/é&%.bin"), b"independent edit")?;
    std::fs::remove_dir(first.join("nested/empty"))?;
    assert_eq!(Snapshot::capture(&source)?, seed);
    assert_eq!(Snapshot::capture(&second)?, seed);
    assert_ne!(Snapshot::capture(&first)?, seed);
    Ok(())
}

#[cfg(unix)]
#[test]
fn readonly_directory_permissions_are_applied_after_its_descendants() -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let temporary = tempfile::tempdir()?;
    let source = temporary.path().join("source");
    std::fs::create_dir_all(source.join("nested"))?;
    std::fs::write(source.join("nested/file"), b"bytes")?;
    std::fs::set_permissions(
        source.join("nested"),
        std::fs::Permissions::from_mode(0o555),
    )?;
    // Restore cleanup access even if a later assertion unwinds.
    struct WritableOnDrop(PathBuf);
    impl Drop for WritableOnDrop {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
        }
    }
    let _source = WritableOnDrop(source.join("nested"));
    let restored = temporary.path().join("restored");
    let _restored = WritableOnDrop(restored.join("nested"));
    let seed = Snapshot::capture(&source)?;
    seed.restore(&restored)?;
    assert_eq!(Snapshot::capture(&restored)?, seed);
    Ok(())
}

#[cfg(unix)]
#[test]
fn snapshot_refuses_a_link_outside_the_owned_fixture() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let source = temporary.path().join("source");
    let foreign = temporary.path().join("foreign");
    std::fs::create_dir(&source)?;
    std::fs::write(&foreign, b"must remain outside the snapshot")?;
    std::os::unix::fs::symlink(&foreign, source.join("link"))?;
    assert_eq!(
        Snapshot::capture(&source).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(std::fs::read(foreign)?, b"must remain outside the snapshot");
    Ok(())
}

use super::{Result, Snapshot, storage};
use acyclic_native_runtime::private_file;
use std::io::Write as _;

#[test]
fn restores_private_custody_without_changing_public_entries() -> Result<()> {
    let temporary = tempfile::tempdir().map_err(storage)?;
    let source = temporary.path().join("source");
    let restored = temporary.path().join("restored");
    std::fs::create_dir(&source).map_err(storage)?;
    let public = source.join("public & %.txt");
    std::fs::write(&public, b"ordinary bytes\0\r\n").map_err(storage)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        // Make the public controls independent of the build host's umask.
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o755))
            .map_err(storage)?;
        std::fs::set_permissions(&public, std::fs::Permissions::from_mode(0o644))
            .map_err(storage)?;
    }
    let directory = source.join("private & %");
    private_file::create_directory(&directory).map_err(storage)?;
    private_file::create(&directory.join("owner"))
        .map_err(storage)?
        .write_all(b"original fixture owner")
        .map_err(storage)?;
    assert!(
        private_file::is_private(
            &private_file::open(&directory.join("owner"), false).map_err(storage)?
        )
        .map_err(storage)?
    );
    assert!(
        !private_file::is_private(&std::fs::File::open(&public).map_err(storage)?)
            .map_err(storage)?
    );
    let snapshot = Snapshot::capture(&source)?;
    snapshot.restore(&restored)?;
    assert!(
        private_file::validate_directory(&restored.join("private & %")).is_ok(),
        "snapshot lost private directory custody"
    );
    let owner = private_file::open(&restored.join("private & %/owner"), false).map_err(storage)?;
    assert!(private_file::is_private(&owner).map_err(storage)?);
    assert_eq!(
        std::fs::read(restored.join("private & %/owner")).map_err(storage)?,
        b"original fixture owner"
    );
    assert!(
        !private_file::is_private(
            &std::fs::File::open(restored.join("public & %.txt")).map_err(storage)?
        )
        .map_err(storage)?
    );
    assert_eq!(Snapshot::capture(&restored)?, snapshot);
    Ok(())
}

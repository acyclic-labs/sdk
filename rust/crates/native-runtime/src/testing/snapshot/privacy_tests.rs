use super::Snapshot;
use crate::private_file;
use std::io::Result;
use std::io::Write as _;

#[test]
fn restores_private_custody_without_changing_public_entries() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let source = temporary.path().join("source");
    let restored = temporary.path().join("restored");
    std::fs::create_dir(&source)?;
    let public = source.join("public & %.txt");
    std::fs::write(&public, b"ordinary bytes\0\r\n")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        // Make the public controls independent of the build host's umask.
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o755))?;
        std::fs::set_permissions(&public, std::fs::Permissions::from_mode(0o644))?;
    }
    let directory = source.join("private & %");
    private_file::create_directory(&directory)?;
    private_file::create(&directory.join("owner"))?.write_all(b"original fixture owner")?;
    assert!(private_file::is_private(&private_file::open(
        &directory.join("owner"),
        false
    )?)?);
    assert!(!private_file::is_private(&std::fs::File::open(&public)?)?);
    let snapshot = Snapshot::capture(&source)?;
    snapshot.restore(&restored)?;
    assert!(
        private_file::validate_directory(&restored.join("private & %")).is_ok(),
        "snapshot lost private directory custody"
    );
    let owner = private_file::open(&restored.join("private & %/owner"), false)?;
    assert!(private_file::is_private(&owner)?);
    assert_eq!(
        std::fs::read(restored.join("private & %/owner"))?,
        b"original fixture owner"
    );
    assert!(!private_file::is_private(&std::fs::File::open(
        restored.join("public & %.txt")
    )?)?);
    assert_eq!(Snapshot::capture(&restored)?, snapshot);
    Ok(())
}

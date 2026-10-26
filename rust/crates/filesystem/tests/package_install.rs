//! Internal artifact bytes exercise the public publication API; these are not
//! fabricated command output, provider events or native qualification reports.
#![cfg(not(target_arch = "wasm32"))]
use acyclic_fs::package_install::{self, Identity, Request, Stage};
use acyclic_native_runtime::{FileLock, FileLockMode, private_file};
use sha2::{Digest as _, Sha256};
use std::{
    fs,
    io::{self, Write as _},
    path::Path,
    sync::Arc,
};

fn request(root: &Path) -> io::Result<Request> {
    let source = root.join("source");
    fs::write(&source, b"internal artifact bytes")?;
    let directory = root.join("bin");
    fs::create_dir(&directory)?;
    Ok(Request {
        source,
        directory,
        identity: Identity {
            version: "test-version".into(),
            target: "internal-target".into(),
            sha256: hex::encode(Sha256::digest(b"internal artifact bytes")),
        },
    })
}

#[test]
fn every_publication_cut_rolls_forward_without_pathnames_or_backups() -> io::Result<()> {
    for cut in [
        Stage::Intent,
        Stage::Ready,
        Stage::Published,
        Stage::Activated,
        Stage::IdentityPublished,
    ] {
        let root = tempfile::tempdir()?;
        let request = request(root.path())?;
        assert!(
            package_install::install(&request, |stage| if stage == cut {
                Err(io::Error::other("cut"))
            } else {
                Ok(())
            })
            .is_err()
        );
        let installed = package_install::install(&request, |_| Ok(()))?;
        assert_eq!(fs::read(&installed)?, fs::read(&request.source)?);
        let saved: Identity =
            serde_json::from_slice(&fs::read(request.directory.join("installed-binary.json"))?)
                .map_err(io::Error::other)?;
        assert_eq!(saved, request.identity);
        let mut names = fs::read_dir(&request.directory)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<io::Result<Vec<_>>>()?;
        names.sort();
        assert_eq!(
            names.len(),
            3,
            "only executable, identity and stable lock survive: {names:?}"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(fs::metadata(installed)?.permissions().mode() & 0o777, 0o755);
        }
    }
    Ok(())
}

#[test]
fn identity_admission_preserves_original_bytes_on_tamper_or_release_conflict() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let mut request = request(root.path())?;
    let installed = package_install::install(&request, |_| Ok(()))?;
    let identity_path = request.directory.join("installed-binary.json");
    let original = fs::read(&identity_path)?;
    request.identity.sha256 = "0".repeat(64);
    assert!(
        package_install::install(&request, |_| Ok(()))
            .err()
            .ok_or_else(|| io::Error::other("expected installer refusal"))?
            .to_string()
            .contains("immutable")
    );
    assert_eq!(fs::read(&identity_path)?, original);
    request.identity.sha256 = hex::encode(Sha256::digest(b"internal artifact bytes"));
    fs::write(&installed, b"tampered internal bytes")?;
    assert!(
        package_install::install(&request, |_| Ok(()))
            .err()
            .ok_or_else(|| io::Error::other("expected installer refusal"))?
            .to_string()
            .contains("durable identity")
    );
    assert_eq!(fs::read(&identity_path)?, original);
    assert_eq!(fs::read(&installed)?, b"tampered internal bytes");
    Ok(())
}

#[test]
fn private_journal_refuses_paths_and_lock_aliases_without_mutation() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let request = request(root.path())?;
    let installed = package_install::install(&request, |_| Ok(()))?;
    let journal = request.directory.join("install-transaction.json");
    let mut held = private_file::create(&journal)?;
    held.write_all(
        br#"{"version":"test-version","target":"internal-target","sha256":"none","backup":".."}"#,
    )?;
    drop(held);
    let original = fs::read(&installed)?;
    assert!(package_install::install(&request, |_| Ok(())).is_err());
    assert_eq!(fs::read(&installed)?, original);
    fs::remove_file(journal)?;
    let lock = request.directory.join("install.lock");
    fs::hard_link(&lock, root.path().join("alias"))?;
    assert!(package_install::install(&request, |_| Ok(())).is_err());
    assert_eq!(fs::read(&installed)?, original);
    Ok(())
}

#[test]
fn concurrent_installers_share_one_stable_kernel_lock() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let request = Arc::new(request(root.path())?);
    let mut workers = Vec::new();
    for _ in 0..8 {
        let request = Arc::clone(&request);
        workers.push(std::thread::spawn(move || {
            package_install::install(&request, |_| Ok(()))
        }));
    }
    for worker in workers {
        worker
            .join()
            .map_err(|_| io::Error::other("installer panicked"))??;
    }
    let lock = request.directory.join("install.lock");
    let owner = FileLock::try_acquire(private_file::open(&lock, true)?, FileLockMode::Exclusive)?;
    assert!(
        FileLock::try_acquire(private_file::open(&lock, true)?, FileLockMode::Exclusive).is_err()
    );
    drop(owner);
    assert!(
        FileLock::try_acquire(private_file::open(&lock, true)?, FileLockMode::Exclusive).is_ok()
    );
    Ok(())
}

#[test]
fn package_transaction_refuses_every_aliased_sidecar_without_mutation() -> io::Result<()> {
    for name in [
        ".install-binary.next",
        ".installed-binary.next",
        ".install-transaction.next",
    ] {
        let root = tempfile::tempdir()?;
        let request = request(root.path())?;
        let stage = request.directory.join(name);
        let alias = root.path().join("alias");
        let mut held = private_file::create(&stage)?;
        held.write_all(b"other owner")?;
        drop(held);
        fs::hard_link(&stage, &alias)?;
        assert!(package_install::install(&request, |_| Ok(())).is_err());
        assert_eq!(fs::read(&stage)?, b"other owner");
        assert_eq!(fs::read(&alias)?, b"other owner");
        assert!(!request.directory.join("installed-binary.json").exists());
    }
    Ok(())
}

#[test]
fn preexisting_private_artifact_finishes_permissions_before_identity() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let request = request(root.path())?;
    let path = request.directory.join(if cfg!(windows) {
        "acyclic.exe"
    } else {
        "acyclic"
    });
    let mut held = private_file::create(&path)?;
    held.write_all(&fs::read(&request.source)?)?;
    drop(held);
    assert_eq!(package_install::install(&request, |_| Ok(()))?, path);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(fs::metadata(path)?.permissions().mode() & 0o777, 0o755);
    }
    Ok(())
}

#[test]
fn a_different_release_cannot_take_over_an_interrupted_intent() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let mut request = request(root.path())?;
    assert!(
        package_install::install(&request, |stage| {
            if stage == Stage::Intent {
                Err(io::Error::other("cut"))
            } else {
                Ok(())
            }
        })
        .is_err()
    );
    let journal = request.directory.join("install-transaction.json");
    let original = fs::read(&journal)?;
    request.identity.version = "different-version".into();
    assert!(package_install::install(&request, |_| Ok(())).is_err());
    assert_eq!(fs::read(&journal)?, original);
    request.identity.version = "test-version".into();
    package_install::install(&request, |_| Ok(()))?;
    Ok(())
}

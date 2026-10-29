//! Real production custody selection in isolated child processes, without an
//! interactive credential store or fixture backend.
#![cfg(not(feature = "test-support"))]

use acyclic_native_runtime::{private_file::PublicationStage, signing_key::SigningKeyReference};
use std::{io, path::Path, process::Command};

#[test]
fn explicit_private_custody_reopens_and_refuses_public_access() -> io::Result<()> {
    if let Some(operation) = std::env::var_os("CUSTODY_CONTROL_OPERATION") {
        let record = std::env::var_os("CUSTODY_CONTROL_RECORD").ok_or(io::ErrorKind::InvalidInput)?;
        return match operation.to_str() {
            Some("probe-native") => {
                match SigningKeyReference::create() {
                    Ok(key) => {
                        key.reference().with_seed(|_| ())?;
                        // Unpublished probe custody is retired by its guard.
                        drop(key);
                        println!("NATIVE_CUSTODY_AVAILABLE");
                    }
                    Err(error) => {
                        assert!(error.get_ref().is_some_and(|error| error.is::<acyclic_native_runtime::signing_key::CustodyFailure>()));
                        println!("NATIVE_CUSTODY_REFUSED {error}");
                    }
                }
                Ok(())
            }
            Some("create") => {
                let key = SigningKeyReference::create()?;
                key.reference().with_seed(|_| ())?;
                std::fs::write(record, serde_json::to_vec(&key.reference())?)?;
                key.observe_publication(PublicationStage::Installed);
                Ok(())
            }
            Some("read") => {
                let reference: SigningKeyReference = serde_json::from_slice(&std::fs::read(record)?)?;
                reference.with_seed(|_| ())
            }
            _ => Err(io::ErrorKind::InvalidInput.into()),
        };
    }
    let root = tempfile::tempdir()?;
    let record = root.path().join("reference.json");
    let invoke = |operation: &str, policy: &str| -> io::Result<std::process::Output> {
        Command::new(std::env::current_exe()?)
            .args(["--exact", "explicit_private_custody_reopens_and_refuses_public_access", "--nocapture"])
            .env("CUSTODY_CONTROL_OPERATION", operation)
            .env("CUSTODY_CONTROL_RECORD", &record)
            .env("ACYCLIC_SIGNING_CUSTODY", policy)
            .env("ACYCLIC_SIGNING_CUSTODY_DIRECTORY", root.path())
            .env_remove("DBUS_SESSION_BUS_ADDRESS")
            .output()
    };
    for operation in ["create", "read"] {
        let output = invoke(operation, "private-file")?;
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    }
    let entries = || std::fs::read_dir(root.path().join("acyclic/signing-seeds"));
    let paths = entries()?.map(|entry| entry.map(|entry| entry.path())).collect::<io::Result<Vec<_>>>()?;
    assert_eq!(paths.len(), 1);
    let path = &paths[0];
    assert!(acyclic_native_runtime::private_file::is_private(
        &acyclic_native_runtime::private_file::open(path, false)?
    )?);
    make_public(path)?;
    let refused = invoke("read", "private-file")?;
    assert!(!refused.status.success(), "public signing seed reached bootstrap");
    assert!(String::from_utf8_lossy(&refused.stderr).contains("CustodyFailure"), "{}", String::from_utf8_lossy(&refused.stderr));
    assert_eq!(entries()?.count(), 1, "refusal replaced the original identity");
    // An explicit native policy with a file-only setting is refused, without
    // probing either service or silently interpreting a typo as file custody.
    let refused = invoke("create", "native")?;
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("InvalidPolicy"));
    assert_eq!(entries()?.count(), 1);
    Ok(())
}

#[cfg(unix)]
fn make_public(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644))
}

#[cfg(windows)]
fn make_public(path: &Path) -> io::Result<()> {
    // Exercise the real ACL, not a synthetic backend refusal. The well-known
    // Everyone SID works independently of the machine's display language.
    let output = Command::new("icacls").arg(path).args(["/grant", "*S-1-1-0:R"]).output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!("step=public-access-control; recovery: icacls <isolated-seed-file> /grant *S-1-1-0:R; {}", String::from_utf8_lossy(&output.stderr))))
    }
}

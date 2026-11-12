//! Real custody selection in isolated child processes, without an interactive
//! credential store. Every build honours the same explicit policy.

use acyclic_native_runtime::signing_key::{SigningCustody, SigningKeyReference};
use std::{io, path::Path, process::Command};
#[path = "signing_reference/oracle.rs"]
mod reference_oracle;

#[test]
fn explicit_private_custody_reopens_and_refuses_public_access() -> io::Result<()> {
    if let Some(operation) = std::env::var_os("CUSTODY_CONTROL_OPERATION") {
        let record =
            std::env::var_os("CUSTODY_CONTROL_RECORD").ok_or(io::ErrorKind::InvalidInput)?;
        return match operation.to_str() {
            Some("create") => {
                let signing = SigningKeyReference::create(SigningCustody::Configured)?;
                let reference = signing.reference();
                reference.with_seed(SigningCustody::Configured, |_| ())?;
                let encoded = serde_json::to_vec(&reference)?;
                reference_oracle::check(&encoded);
                std::fs::write(record, encoded)?;
                signing.observe_publication(
                    acyclic_native_runtime::private_file::PublicationStage::Installed,
                );
                Ok(())
            }
            Some("read") | Some("mismatch") => {
                let reference: SigningKeyReference =
                    serde_json::from_slice(&std::fs::read(record)?)?;
                if operation == "mismatch" {
                    let error = reference
                        .with_seed(SigningCustody::Configured, |_| {
                            panic!("foreign store reached seed")
                        })
                        .unwrap_err();
                    let cause = error.get_ref().and_then(|cause| {
                        cause.downcast_ref::<acyclic_native_runtime::signing_key::CustodyFailure>()
                    });
                    assert!(
                        cause.is_some_and(|cause| matches!(
                            cause.cause,
                            acyclic_native_runtime::signing_key::CustodyCause::StoreMismatch
                        )),
                        "{error}"
                    );
                    Ok(())
                } else {
                    reference.with_seed(SigningCustody::Configured, |_| ())
                }
            }
            _ => Err(io::ErrorKind::InvalidInput.into()),
        };
    }
    let root = tempfile::tempdir()?;
    let records = [0, 1, 2].map(|owner| root.path().join(format!("owner-{owner}.json")));
    let invoke =
        |operation: &str, policy: &str, record: &Path| -> io::Result<std::process::Output> {
            Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "explicit_private_custody_reopens_and_refuses_public_access",
                    "--nocapture",
                ])
                .env("CUSTODY_CONTROL_OPERATION", operation)
                .env("CUSTODY_CONTROL_RECORD", record)
                .env("ACYCLIC_SIGNING_CUSTODY", policy)
                .env("ACYCLIC_SIGNING_CUSTODY_DIRECTORY", root.path())
                .env_remove("DBUS_SESSION_BUS_ADDRESS")
                .output()
        };
    // Owners created by separate processes retain distinct store-owned entries.
    for operation in ["create", "read"] {
        for record in &records {
            let output = invoke(operation, "private-file", record)?;
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
    // A different private directory and a different backend both refuse at
    // selection, before a native service or missing credential is consulted.
    for policy in ["private-file", "native"] {
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args([
                "--exact",
                "explicit_private_custody_reopens_and_refuses_public_access",
                "--nocapture",
            ])
            .env("CUSTODY_CONTROL_OPERATION", "mismatch")
            .env("CUSTODY_CONTROL_RECORD", &records[0])
            .env("ACYCLIC_SIGNING_CUSTODY", policy)
            .env_remove("ACYCLIC_SIGNING_CUSTODY_DIRECTORY");
        if policy == "private-file" {
            command.env(
                "ACYCLIC_SIGNING_CUSTODY_DIRECTORY",
                root.path().join("other-store"),
            );
        }
        let output = command.output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!root.path().join("other-store").exists());
    }
    let entries = || std::fs::read_dir(root.path().join("acyclic/signing-seeds.v2"));
    let paths = entries()?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<io::Result<Vec<_>>>()?;
    assert_eq!(paths.len(), records.len());
    for path in &paths {
        assert!(acyclic_native_runtime::private_file::is_private(
            &acyclic_native_runtime::private_file::open(path, false)?
        )?);
        make_public(path)?;
    }
    let refused = invoke("read", "private-file", &records[0])?;
    assert!(
        !refused.status.success(),
        "public signing seed reached bootstrap"
    );
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("CustodyFailure"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
    assert_eq!(
        entries()?.count(),
        records.len(),
        "refusal replaced the original identity"
    );
    // An explicit native policy with a file-only setting is refused, without
    // probing either service or silently interpreting a typo as file custody.
    let refused = invoke("create", "native", &records[0])?;
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("InvalidPolicy"));
    assert_eq!(entries()?.count(), records.len());
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
    // Canonical Windows paths retain the extended namespace for long roots.
    let output = Command::new("icacls")
        .arg(path.canonicalize()?)
        .args(["/grant", "*S-1-1-0:R"])
        .output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "step=public-access-control; recovery: icacls <isolated-seed-file> /grant *S-1-1-0:R; {}",
            String::from_utf8_lossy(&output.stderr)
        )))
    }
}

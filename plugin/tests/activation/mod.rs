use serde_json::{Value, json};
use std::{fs, process::Command};

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[test]
fn shipped_installer_commands_use_durable_activation_without_a_service() -> Result {
    let directory = tempfile::tempdir()?;
    let from = directory.path().join("staged");
    let to = directory.path().join("installed");
    for mode in ["no-replace", "replace"] {
        fs::write(&from, mode)?;
        let output = Command::new(env!("CARGO_BIN_EXE_acyclic"))
            .arg("__installer-rename")
            .arg(&from)
            .arg(&to)
            .arg(mode)
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(fs::read_to_string(&to)?, mode);
        assert!(!from.exists());
    }
    fs::write(&from, "must remain")?;
    let conflict = Command::new(env!("CARGO_BIN_EXE_acyclic"))
        .arg("__installer-rename")
        .arg(&from)
        .arg(&to)
        .arg("no-replace")
        .output()?;
    assert!(!conflict.status.success());
    assert_eq!(fs::read_to_string(&from)?, "must remain");
    assert_eq!(fs::read_to_string(&to)?, "replace");
    Ok(())
}

#[test]
fn shipped_certification_command_verifies_the_actual_executable() -> Result {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("certification.json");
    let mut value: Value =
        serde_json::from_str(include_str!("../fixtures/native-certification.json"))?;
    let mut hash = blake3::Hasher::new();
    hash.update_reader(fs::File::open(env!("CARGO_BIN_EXE_acyclic"))?)?;
    let backend = match std::env::consts::OS {
        "linux" => "linux-fuse",
        "macos" => "macos-nfs",
        "windows" => "windows-projfs",
        os => return Err(format!("unsupported certification platform: {os}").into()),
    };
    let object = value.as_object_mut().ok_or("fixture object")?;
    object.insert("os".into(), json!(std::env::consts::OS));
    object.insert("arch".into(), json!(std::env::consts::ARCH));
    object.insert("release_version".into(), json!(env!("CARGO_PKG_VERSION")));
    object.insert(
        "executable_blake3".into(),
        json!(hash.finalize().to_hex().as_str()),
    );
    object.insert("required_kind".into(), json!(backend));
    object.insert("capability".into(), json!({"kind":backend,"available":true,"writable":true,
        "provider_process_io_observable":cfg!(not(windows)),"session_isolation":"SharedProcess","unavailable_reason":null}));
    fs::write(&path, serde_json::to_vec(&value)?)?;
    let valid = Command::new(env!("CARGO_BIN_EXE_acyclic"))
        .arg("__verify-certification")
        .arg(&path)
        .output()?;
    assert!(
        valid.status.success(),
        "{}",
        String::from_utf8_lossy(&valid.stderr)
    );
    value
        .as_object_mut()
        .ok_or("fixture object")?
        .insert("executable_blake3".into(), json!("0".repeat(64)));
    fs::write(&path, serde_json::to_vec(&value)?)?;
    let forged = Command::new(env!("CARGO_BIN_EXE_acyclic"))
        .arg("__verify-certification")
        .arg(&path)
        .output()?;
    assert!(!forged.status.success());
    assert!(String::from_utf8_lossy(&forged.stderr).contains("does not match"));
    Ok(())
}

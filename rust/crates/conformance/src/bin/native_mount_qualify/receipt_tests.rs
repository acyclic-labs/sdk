use super::{COVERAGE, Failure, file_blake3, verify_receipt};
use serde_json::Value;
use std::ffi::OsString;

fn receipt_array<'a>(document: &'a mut Value, name: &str) -> Result<&'a mut Vec<Value>, Failure> {
    document
        .get_mut(name)
        .and_then(Value::as_array_mut)
        .ok_or_else(|| format!("missing {name} array").into())
}

#[test]
fn verifies_the_release_target_architecture_not_the_verifier_host() -> Result<(), Failure> {
    let directory = tempfile::tempdir()?;
    let executable = directory.path().join("acyclic");
    let receipt = directory.path().join("macos-nfs.json");
    std::fs::write(&executable, b"release executable")?;
    let digest = file_blake3(&executable)?;
    let mut document = serde_json::json!({
        "schema": "acyclic-native-mount-qualification-v2",
        "os": "macos",
        "arch": "aarch64",
        "coverage": COVERAGE,
        "capability": {
            "kind": "macos-nfs",
            "available": true,
            "writable": true,
            "provider_process_io_observable": true,
            "session_isolation": "SharedProcess",
            "unavailable_reason": null
        },
        "required_kind": "macos-nfs",
        "release_version": "0.1.5",
        "executable_blake3": digest,
        "passed": true,
        "cases": [
            {"name": "real-mount-mutation-matrix", "status": "passed", "reason": null},
            {"name": "crash-detach-recovery", "status": "passed", "reason": null},
            {"name": "checkout-and-git-untouched", "status": "passed", "reason": null}
        ]
    });
    std::fs::write(&receipt, serde_json::to_vec(&document)?)?;
    let mut args = vec![
        OsString::from("--verify-receipt"),
        receipt.clone().into_os_string(),
        OsString::from("--release-executable"),
        executable.into_os_string(),
        OsString::from("--require-kind"),
        OsString::from("macos-nfs"),
        OsString::from("--release-version"),
        OsString::from("0.1.5"),
        OsString::from("--release-arch"),
        OsString::from("aarch64"),
    ];
    verify_receipt(&args)?;
    // The former typed verifier silently ignored extra top-level fields and
    // decoded a missing optional reason as null. Neither is the emitted schema.
    let mut extra = document.clone();
    extra
        .as_object_mut()
        .ok_or("object")?
        .insert("unknown".into(), serde_json::json!(true));
    std::fs::write(&receipt, serde_json::to_vec(&extra)?)?;
    assert!(verify_receipt(&args).is_err());
    let mut missing_reason = document.clone();
    missing_reason
        .get_mut("capability")
        .and_then(Value::as_object_mut)
        .ok_or("capability object")?
        .remove("unavailable_reason");
    std::fs::write(&receipt, serde_json::to_vec(&missing_reason)?)?;
    assert!(verify_receipt(&args).is_err());
    receipt_array(&mut document, "coverage")?.reverse();
    receipt_array(&mut document, "coverage")?.push(serde_json::json!("future-coverage"));
    receipt_array(&mut document, "cases")?.reverse();
    receipt_array(&mut document, "cases")?
        .push(serde_json::json!({"name":"future-case","status":"passed","reason":null}));
    std::fs::write(&receipt, serde_json::to_vec(&document)?)?;
    verify_receipt(&args)?;
    let valid_extension = document.clone();
    receipt_array(&mut document, "coverage")?.push(serde_json::json!("future-coverage"));
    std::fs::write(&receipt, serde_json::to_vec(&document)?)?;
    assert!(verify_receipt(&args).is_err());
    document = valid_extension;
    let duplicate_case = receipt_array(&mut document, "cases")?
        .last()
        .cloned()
        .ok_or("missing future case")?;
    receipt_array(&mut document, "cases")?.push(duplicate_case);
    std::fs::write(&receipt, serde_json::to_vec(&document)?)?;
    assert!(verify_receipt(&args).is_err());
    receipt_array(&mut document, "cases")?.pop();
    *receipt_array(&mut document, "cases")?
        .last_mut()
        .and_then(|case| case.get_mut("status"))
        .ok_or("future case status")? = serde_json::json!("failed");
    std::fs::write(&receipt, serde_json::to_vec(&document)?)?;
    assert!(verify_receipt(&args).is_err());
    *receipt_array(&mut document, "cases")?
        .last_mut()
        .and_then(|case| case.get_mut("status"))
        .ok_or("future case status")? = serde_json::json!("passed");
    std::fs::write(&receipt, serde_json::to_vec(&document)?)?;
    *args.last_mut().ok_or("missing release architecture")? = OsString::from("x86_64");
    let error = match verify_receipt(&args) {
        Ok(()) => return Err("mismatched release architecture was accepted".into()),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("does not match the release artifact")
    );
    Ok(())
}

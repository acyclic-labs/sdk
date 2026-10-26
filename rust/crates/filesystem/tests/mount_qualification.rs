//! Public API regressions for release-artifact receipt admission.
use acyclic_fs::mount_qualification::{COVERAGE, ReceiptError, ReleaseTarget, verify};
use serde_json::Value;
use std::io;
const MAXIMUM_RECEIPT_BYTES: usize = 65_536; // Existing native ingress contract.
const CASES: &[&str] = &[
    "real-mount-mutation-matrix",
    "crash-detach-recovery",
    "checkout-and-git-untouched",
];
use serde_json::json;

fn check(
    document: &Value,
    executable: impl io::Read,
    target: ReleaseTarget<'_>,
) -> Result<Value, ReceiptError> {
    verify(serde_json::to_vec(document)?.as_slice(), executable, target)
}

// These are internal policy inputs, not captured OS reports or live receipts.
fn input(os: &str, arch: &str) -> Value {
    let kind = match os {
        "linux" => "linux-fuse",
        "macos" => "macos-nfs",
        _ => "windows-projfs",
    };
    json!({"schema":"acyclic-native-mount-qualification-v2", "os":os, "arch":arch,
        "coverage":COVERAGE, "required_kind":kind, "release_version":"test-release",
        "executable_blake3":blake3::hash(b"artifact").to_hex().to_string(), "passed":true,
        "capability":{"kind":kind,"available":true,"writable":true,
            "provider_process_io_observable":os!="windows", "session_isolation":"SharedProcess",
            "unavailable_reason":null},
        "cases":CASES.iter().map(|name|json!({"name":name,"status":"passed","reason":null})).collect::<Vec<_>>()})
}

#[test]
fn policy_refuses_duplicate_known_fields_before_json_map_materialization()
-> Result<(), Box<dyn std::error::Error>> {
    let target = ReleaseTarget {
        os: "macos",
        arch: "aarch64",
        version: "test-release",
    };
    let original = serde_json::to_string(&input(target.os, target.arch))?;
    for (field, value) in [
        ("arch", "\"aarch64\""),
        ("available", "true"),
        ("status", "\"passed\""),
    ] {
        let key = format!("\"{field}\":");
        let duplicate = original.replacen(&key, &format!("{key}{value},{key}"), 1);
        assert!(
            verify(duplicate.as_bytes(), &b"artifact"[..], target).is_err(),
            "duplicate {field}"
        );
    }
    Ok(())
}

#[test]
fn policy_preserves_the_native_receipt_size_bound() -> Result<(), Box<dyn std::error::Error>> {
    let target = ReleaseTarget {
        os: "linux",
        arch: "x86_64",
        version: "test-release",
    };
    let mut bytes = serde_json::to_vec(&input(target.os, target.arch))?;
    bytes.resize(MAXIMUM_RECEIPT_BYTES, b' ');
    verify(bytes.as_slice(), &b"artifact"[..], target)?;
    bytes.push(b' ');
    assert!(matches!(
        verify(bytes.as_slice(), &b"artifact"[..], target),
        Err(ReceiptError::TooLarge)
    ));
    Ok(())
}

#[test]
fn policy_binds_each_release_target_and_every_required_field()
-> Result<(), Box<dyn std::error::Error>> {
    for os in ["linux", "macos", "windows"] {
        for arch in ["x86_64", "aarch64"] {
            let target = ReleaseTarget {
                os,
                arch,
                version: "test-release",
            };
            let document = input(os, arch);
            check(&document, &b"artifact"[..], target)?;
            assert!(check(&document, &b"different artifact"[..], target).is_err());
            for key in document.as_object().ok_or("object")?.keys() {
                let mut missing = document.clone();
                missing.as_object_mut().ok_or("object")?.remove(key);
                assert!(
                    check(&missing, &b"artifact"[..], target).is_err(),
                    "missing {key}"
                );
                let mut changed = document.clone();
                *changed.get_mut(key).ok_or("known field")? = Value::Null;
                assert!(
                    check(&changed, &b"artifact"[..], target).is_err(),
                    "null {key}"
                );
            }
            let capability = document
                .get("capability")
                .and_then(Value::as_object)
                .ok_or("capability")?;
            for key in capability.keys() {
                let mut missing = document.clone();
                missing
                    .get_mut("capability")
                    .and_then(Value::as_object_mut)
                    .ok_or("capability")?
                    .remove(key);
                assert!(
                    check(&missing, &b"artifact"[..], target).is_err(),
                    "missing capability {key}"
                );
            }
            let mut extra = document.clone();
            extra
                .as_object_mut()
                .ok_or("object")?
                .insert("unrecognized".into(), json!(true));
            assert!(check(&extra, &b"artifact"[..], target).is_err());
            assert!(
                check(
                    &document,
                    &b"artifact"[..],
                    ReleaseTarget {
                        version: "other",
                        ..target
                    }
                )
                .is_err()
            );
            assert!(
                check(
                    &document,
                    &b"artifact"[..],
                    ReleaseTarget {
                        arch: "unknown",
                        ..target
                    }
                )
                .is_err()
            );
            assert!(
                check(
                    &document,
                    &b"artifact"[..],
                    ReleaseTarget {
                        os: "unknown",
                        ..target
                    }
                )
                .is_err()
            );
        }
    }
    Ok(())
}

#[test]
fn policy_rejects_missing_duplicate_or_failed_coverage_but_accepts_reordering_and_extensions()
-> Result<(), Box<dyn std::error::Error>> {
    let target = ReleaseTarget {
        os: "macos",
        arch: "aarch64",
        version: "test-release",
    };
    let document = input(target.os, target.arch);
    for name in ["coverage", "cases"] {
        let original = document
            .get(name)
            .and_then(Value::as_array)
            .ok_or("array")?;
        for index in 0..original.len() {
            let mut missing = document.clone();
            missing
                .get_mut(name)
                .and_then(Value::as_array_mut)
                .ok_or("array")?
                .remove(index);
            assert!(
                check(&missing, &b"artifact"[..], target).is_err(),
                "missing {name} {index}"
            );
        }
        let mut duplicate = document.clone();
        duplicate
            .get_mut(name)
            .and_then(Value::as_array_mut)
            .ok_or("array")?
            .push(original.first().ok_or("item")?.clone());
        assert!(check(&duplicate, &b"artifact"[..], target).is_err());
    }
    let mut extended = document;
    let coverage = extended
        .get_mut("coverage")
        .and_then(Value::as_array_mut)
        .ok_or("coverage")?;
    coverage.reverse();
    coverage.push(json!("future-coverage"));
    let cases = extended
        .get_mut("cases")
        .and_then(Value::as_array_mut)
        .ok_or("cases")?;
    cases.reverse();
    cases.push(json!({"name":"future-case","status":"passed","reason":null,"elapsed_ms":1}));
    check(&extended, &b"artifact"[..], target)?;
    for field in ["status", "reason"] {
        let mut failed = extended.clone();
        *failed
            .get_mut("cases")
            .and_then(Value::as_array_mut)
            .ok_or("cases")?
            .last_mut()
            .ok_or("last")?
            .get_mut(field)
            .ok_or("field")? = json!("failed");
        assert!(check(&failed, &b"artifact"[..], target).is_err());
    }
    Ok(())
}

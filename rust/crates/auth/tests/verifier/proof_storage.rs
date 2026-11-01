//! The child process inspects live allocator storage before glibc releases it.
#![cfg(all(target_os = "linux", target_env = "gnu"))]

use super::signed::*;
use acyclic_auth::{contract::*, verify::verify};
use prost::Message;
use std::process::Command;
use zeroize::Zeroizing;

#[test]
fn protected_buffers_are_scrubbed_before_deallocation() {
    let directory = tempfile::tempdir().unwrap();
    let library = directory.path().join("scrub.so");
    let compiler = Command::new("cc")
        .args(["-shared", "-fPIC", "-O2", "-Wall", "-Wextra", "-Werror"])
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/common/scrub_allocator.c"
        ))
        .arg("-o")
        .arg(&library)
        .output()
        .unwrap();
    assert!(
        compiler.status.success(),
        "{}",
        String::from_utf8_lossy(&compiler.stderr)
    );
    for case in [
        "valid",
        "duplicate-secret",
        "duplicate-sealed",
        "truncated",
        "bad-key",
        "encode",
        "broken",
        "body-grow",
        "body-failed-growth",
        "body-unprotected-growth",
        "json-client",
        "json-failure",
        "json-unprotected",
    ] {
        let active = directory.path().join(case);
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "verifier::proof_storage::scrub_child",
                "--nocapture",
            ])
            .env("LD_PRELOAD", &library)
            .env("AUTH_SCRUB_ACTIVE", &active)
            .env("AUTH_SCRUB_CASE", case)
            .env(
                "AUTH_SCRUB_BYTE",
                if case.starts_with("json-") { "90" } else { "4" },
            )
            .output()
            .unwrap();
        let expected = if matches!(
            case,
            "broken" | "body-unprotected-growth" | "json-unprotected"
        ) {
            86
        } else {
            0
        };
        assert_eq!(
            output.status.code(),
            Some(expected),
            "{case}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn scrub_child() {
    let Ok(case) = std::env::var("AUTH_SCRUB_CASE") else {
        return;
    };
    if case.starts_with("body-") {
        body_storage(&case);
        return;
    }
    if case.starts_with("json-") {
        json_storage(&case);
        return;
    }
    let mut bytes = Zeroizing::new(root(|_| {}));
    let trust = Snapshot::default();
    let cap = verify(&bytes, &trust, NOW).unwrap();
    match case.as_str() {
        "duplicate-secret" => {
            bytes.extend([vec![18, 32], vec![4; 32]].concat());
        }
        "duplicate-sealed" | "broken" => {
            bytes.extend([vec![26, 64], vec![0; 64]].concat());
        }
        "truncated" => {
            bytes.pop();
        }
        "bad-key" => {
            let end = bytes.len() - 1;
            *bytes.get_mut(end).unwrap() = 5;
        }
        _ => {}
    }
    std::fs::write(std::env::var_os("AUTH_SCRUB_ACTIVE").unwrap(), b"active").unwrap();
    match case.as_str() {
        "broken" => {
            drop(CapabilityProto::decode(bytes.as_slice()).unwrap());
        }
        "encode" => {
            drop(cap.encode());
            drop(cap.seal().unwrap().encode());
        }
        "valid" => {
            drop(verify(&bytes, &trust, NOW).unwrap());
        }
        _ => assert!(verify(&bytes, &trust, NOW).is_err()),
    }
}

fn json_storage(case: &str) {
    use acyclic_auth::{
        credential::CredentialProvider,
        operation::ProtectedBytes,
        secrets::{Client, Response, Transport, TransportFuture},
    };
    use zeroize::Zeroize as _;

    struct Capture<'a>(&'a [u8]);
    impl Transport for Capture<'_> {
        fn post<'a>(
            &'a self,
            path: &'static str,
            _: &'a str,
            body: ProtectedBytes,
        ) -> TransportFuture<'a> {
            assert_eq!(path, "/v1/identity/secrets/put");
            assert_eq!(body.as_ref(), self.0);
            Box::pin(async move {
                drop(body);
                Ok(Response {
                    status: 200,
                    body: Zeroizing::new(
                        serde_json::to_vec(&SecretMutationReply {
                            generation: 1,
                            name: "item".into(),
                            version: 1,
                            kind: SecretKind::Protected,
                        })
                        .unwrap(),
                    ),
                })
            })
        }
    }
    let mut request = SecretPutRequest {
        scope: "Z".repeat(512),
        name: "item".into(),
        kind: SecretKind::Protected,
        value: "Z".repeat(4096),
        expected_generation: 0,
    };
    let expected = Zeroizing::new(serde_json::to_vec(&request).unwrap());
    std::fs::write(std::env::var_os("AUTH_SCRUB_ACTIVE").unwrap(), b"active").unwrap();
    match case {
        "json-client" => {
            let client = Client::new(
                Capture(&expected),
                CredentialProvider::new(|| async { Ok("fixture".into()) }),
            );
            let result = futures::executor::block_on(client.put(&request));
            assert!(
                result.is_ok(),
                "original client refused protected encoding: {result:?}"
            );
        }
        "json-failure" => {
            struct Partial<'a>(&'a str);
            impl serde::Serialize for Partial<'_> {
                fn serialize<S: serde::Serializer>(
                    &self,
                    serializer: S,
                ) -> Result<S::Ok, S::Error> {
                    use serde::ser::{Error as _, SerializeSeq as _};
                    let mut sequence = serializer.serialize_seq(None)?;
                    sequence.serialize_element(self.0)?;
                    Err(S::Error::custom("deliberate failure after partial output"))
                }
            }
            assert!(ProtectedBytes::from_json(&Partial(&request.value)).is_err());
        }
        "json-unprotected" => {
            // The former encoder must fail the same relocation/free oracle,
            // even though its completed output would be protected afterwards.
            drop(Zeroizing::new(serde_json::to_vec(&request).unwrap()));
        }
        _ => unreachable!(),
    }
    // The caller retains its original input; it is outside the encoded body.
    request.scope.zeroize();
    request.value.zeroize();
}

fn body_storage(case: &str) {
    use acyclic_auth::operation::reserve_protected_bytes;

    let mut bytes = Zeroizing::new(Vec::new());
    reserve_protected_bytes(&mut bytes, 64).unwrap();
    bytes.extend_from_slice(&[4; 64]);
    let original = bytes.as_ptr();
    let capacity = bytes.capacity();
    std::fs::write(std::env::var_os("AUTH_SCRUB_ACTIVE").unwrap(), b"active").unwrap();
    match case {
        "body-grow" => {
            let cost = acyclic_test_support::measure(|| {
                reserve_protected_bytes(&mut bytes, capacity * 2).unwrap();
            });
            assert_eq!(cost.count_total, 1);
            assert_eq!(cost.bytes_total, bytes.capacity() as u64);
            assert_ne!(original, bytes.as_ptr());
            assert_eq!(bytes.as_slice(), &[4; 64]);
            let current = bytes.as_ptr();
            let cost = acyclic_test_support::measure(|| {
                reserve_protected_bytes(&mut bytes, capacity).unwrap();
            });
            assert_eq!(cost.count_total, 0);
            assert_eq!(current, bytes.as_ptr());
        }
        "body-failed-growth" => {
            assert!(reserve_protected_bytes(&mut bytes, usize::MAX).is_err());
            assert_eq!(original, bytes.as_ptr());
            assert_eq!(capacity, bytes.capacity());
            assert_eq!(bytes.as_slice(), &[4; 64]);
        }
        "body-unprotected-growth" => {
            // The old backing is live until replacement construction completes.
            // Omitting its scrub must trip the same pre-free allocator oracle.
            let unprotected = std::mem::take(&mut *bytes);
            let replacement = unprotected.clone();
            drop(unprotected);
            drop(Zeroizing::new(replacement));
        }
        _ => unreachable!(),
    }
}

//! The child process inspects live allocator storage before glibc releases it.
#![cfg(all(target_os = "linux", target_env = "gnu"))]

use super::signed::*;
use acyclic_auth::{contract::*, verify::verify};
use prost::Message;
use std::process::Command;
use zeroize::Zeroizing;

/// A failure that carries its cause into the test report.
type Outcome = Result<(), Box<dyn std::error::Error>>;

/// Marks the start of the window the preloaded allocator inspects.
fn activate() -> Outcome {
    let active = std::env::var_os("AUTH_SCRUB_ACTIVE").ok_or("AUTH_SCRUB_ACTIVE is unset")?;
    std::fs::write(active, b"active")?;
    Ok(())
}

#[test]
fn protected_buffers_are_scrubbed_before_deallocation() -> Outcome {
    let directory = tempfile::tempdir()?;
    let library = directory.path().join("scrub.so");
    let compiler = Command::new("cc")
        .args(["-shared", "-fPIC", "-O2", "-Wall", "-Wextra", "-Werror"])
        .arg(acyclic_test_support::crate_path!(
            "tests/common/scrub_allocator.c"
        ))
        .arg("-o")
        .arg(&library)
        .output()?;
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
        "bearer-header",
        "bearer-header-invalid",
        "bearer-unprotected",
        #[cfg(feature = "grpc-metadata")]
        "bearer-metadata",
        #[cfg(feature = "grpc-metadata")]
        "bearer-metadata-invalid",
    ] {
        let active = directory.path().join(case);
        let output = Command::new(std::env::current_exe()?)
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
                if case.starts_with("json-") || case.starts_with("bearer-") {
                    "90"
                } else {
                    "4"
                },
            )
            .output()?;
        let expected = if matches!(
            case,
            "broken" | "body-unprotected-growth" | "json-unprotected" | "bearer-unprotected"
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
    Ok(())
}

#[test]
fn scrub_child() -> Outcome {
    let Ok(case) = std::env::var("AUTH_SCRUB_CASE") else {
        return Ok(());
    };
    if case.starts_with("body-") {
        return body_storage(&case);
    }
    if case.starts_with("json-") {
        return json_storage(&case);
    }
    if case.starts_with("bearer-") {
        return bearer_storage(&case);
    }
    let mut bytes = Zeroizing::new(root(|_| {}));
    let trust = Snapshot::default();
    let cap = verify(&bytes, &trust, NOW)?;
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
            *bytes.last_mut().ok_or("the signed root is empty")? = 5;
        }
        _ => {}
    }
    activate()?;
    match case.as_str() {
        "broken" => {
            drop(CapabilityProto::decode(bytes.as_slice())?);
        }
        "encode" => {
            drop(cap.encode());
            drop(cap.seal()?.encode());
        }
        "valid" => {
            drop(verify(&bytes, &trust, NOW)?);
        }
        _ => assert!(verify(&bytes, &trust, NOW).is_err()),
    }
    Ok(())
}

fn bearer_storage(case: &str) -> Outcome {
    use acyclic_auth::credential::{CredentialError, bearer_header};
    use std::error::Error as _;

    let mut token = Zeroizing::new("Z".repeat(8192));
    if case.ends_with("-invalid") {
        token.push('\u{7f}');
    }
    activate()?;
    match case {
        "bearer-unprotected" => drop(format!("Bearer {}", token.as_str())),
        "bearer-header" => {
            let header = bearer_header(&token)?;
            let retained = header.clone();
            assert!(header.is_sensitive());
            assert_eq!(header.as_bytes().as_ptr(), retained.as_bytes().as_ptr());
            drop(header);
            assert_eq!(
                retained.as_bytes().strip_prefix(b"Bearer "),
                Some(token.as_bytes())
            );
            drop(retained);
        }
        "bearer-header-invalid" => {
            let Err(error) = bearer_header(&token) else {
                return Err("an invalid bearer token formed a header".into());
            };
            assert!(matches!(error, CredentialError::Header(_)));
            assert!(error.source().is_some());
        }
        #[cfg(feature = "grpc-metadata")]
        "bearer-metadata" => {
            let metadata = acyclic_auth::credential::bearer_metadata(&token)?;
            let retained = metadata.clone();
            assert!(metadata.is_sensitive());
            assert_eq!(
                metadata.as_encoded_bytes().as_ptr(),
                retained.as_encoded_bytes().as_ptr()
            );
            drop(metadata);
            assert_eq!(
                retained.as_encoded_bytes().strip_prefix(b"Bearer "),
                Some(token.as_bytes())
            );
            drop(retained);
        }
        #[cfg(feature = "grpc-metadata")]
        "bearer-metadata-invalid" => {
            let Err(error) = acyclic_auth::credential::bearer_metadata(&token) else {
                return Err("an invalid bearer token formed metadata".into());
            };
            assert!(matches!(error, CredentialError::Metadata(_)));
            assert!(error.source().is_some());
        }
        _ => return Err(format!("unknown bearer storage case {case}").into()),
    }
    Ok(())
}

fn json_storage(case: &str) -> Outcome {
    use acyclic_auth::{
        credential::CredentialProvider,
        operation::ProtectedBytes,
        secrets::{Client, Response, Transport, TransportFuture},
    };
    use zeroize::Zeroize as _;

    /// The expected request body and the encoded reply.
    struct Capture<'a>(&'a [u8], Zeroizing<Vec<u8>>);
    impl Transport for Capture<'_> {
        fn post<'a>(
            &'a self,
            path: &'static str,
            _: &'a str,
            body: ProtectedBytes,
        ) -> TransportFuture<'a> {
            assert_eq!(path, "/v1/identity/secrets/put");
            assert_eq!(body.as_ref(), self.0);
            let reply = self.1.clone();
            Box::pin(async move {
                drop(body);
                Ok(Response {
                    status: 200,
                    body: reply,
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
    let expected = Zeroizing::new(serde_json::to_vec(&request)?);
    let reply = Zeroizing::new(serde_json::to_vec(&SecretMutationReply {
        generation: 1,
        name: "item".into(),
        version: 1,
        kind: SecretKind::Protected,
    })?);
    activate()?;
    match case {
        "json-client" => {
            let (context, _stop) = crate::operation_context::invocation();
            let client = Client::new(
                Capture(&expected, reply),
                CredentialProvider::new(|| async { Ok("fixture".into()) }),
                Some(context),
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
            drop(Zeroizing::new(serde_json::to_vec(&request)?));
        }
        _ => return Err(format!("unknown json storage case {case}").into()),
    }
    // The caller retains its original input; it is outside the encoded body.
    request.scope.zeroize();
    request.value.zeroize();
    Ok(())
}

fn body_storage(case: &str) -> Outcome {
    use acyclic_auth::operation::reserve_protected_bytes;

    let mut bytes = Zeroizing::new(Vec::new());
    reserve_protected_bytes(&mut bytes, 64)?;
    bytes.extend_from_slice(&[4; 64]);
    let original = bytes.as_ptr();
    let capacity = bytes.capacity();
    activate()?;
    match case {
        "body-grow" => {
            let mut grown = Ok(());
            let cost = acyclic_test_support::measure(|| {
                grown = reserve_protected_bytes(&mut bytes, capacity * 2);
            });
            grown?;
            assert_eq!(cost.count_total, 1);
            assert_eq!(cost.bytes_total, bytes.capacity() as u64);
            assert_ne!(original, bytes.as_ptr());
            assert_eq!(bytes.as_slice(), &[4; 64]);
            let current = bytes.as_ptr();
            let mut kept = Ok(());
            let cost = acyclic_test_support::measure(|| {
                kept = reserve_protected_bytes(&mut bytes, capacity);
            });
            kept?;
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
        _ => return Err(format!("unknown body storage case {case}").into()),
    }
    Ok(())
}

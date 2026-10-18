//! Restore request codec, validation and retry-identity properties.
use acyclic_objects::v1::{MemoryObjects, MemoryOptions, ObjectsProvider, request, wire};
use proptest::prelude::*;
use prost::Message;

fn requests() -> impl Strategy<Value = wire::RestoreObjectsRequest> {
    (
        "[a-z]{3,63}",
        "[^\\x00]{0,80}",
        0_i64..=253_402_300_799,
        1_i32..1_000_000,
        "[a-z0-9]{1,128}",
    )
        .prop_map(
            |(name, prefix, seconds, micros, key)| wire::RestoreObjectsRequest {
                bucket: Some(wire::BucketRef { name }),
                prefix,
                at: Some(prost_types::Timestamp {
                    seconds,
                    nanos: micros * 1000,
                }),
                mutation: Some(wire::MutationIdentity {
                    idempotency_key: key,
                }),
            },
        )
}

// Each generated valid request is perturbed into every invalid field class.
fn invalid_requests(query: &wire::RestoreObjectsRequest) -> Vec<wire::RestoreObjectsRequest> {
    let mut invalid = Vec::new();
    let mut add = |edit: &dyn Fn(&mut wire::RestoreObjectsRequest)| {
        let mut value = query.clone();
        edit(&mut value);
        invalid.push(value);
    };
    add(&|q| q.bucket = None);
    for name in [
        "",
        "ab",
        "UPPER",
        "127.0.0.1",
        "bad..name",
        "xn--abc",
        "abc--x-s3",
    ] {
        add(&|q| q.bucket = Some(wire::BucketRef { name: name.into() }));
    }
    add(&|q| q.prefix = "x".repeat(1025));
    add(&|q| q.prefix.push('\0'));
    add(&|q| q.at = None);
    for (seconds, nanos) in [
        (-1, 0),
        (0, 0),
        (253_402_300_800, 0),
        (42, -1),
        (42, 1_000_000_000),
        (42, 1),
        (0, 999),
    ] {
        add(&|q| q.at = Some(prost_types::Timestamp { seconds, nanos }));
    }
    add(&|q| q.mutation = None);
    for key in [String::new(), "x".repeat(257), "bad\0key".into()] {
        add(&|q| {
            q.mutation = Some(wire::MutationIdentity {
                idempotency_key: key.clone(),
            });
        });
    }
    invalid
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn restore_round_trip_and_invalid_fields(query in requests()) {
        let bytes = query.encode_to_vec();
        prop_assert_eq!(&wire::RestoreObjectsRequest::decode(bytes.as_slice()).unwrap(), &query);
        prop_assert!(request::validate_binary("objects/restore", &bytes, 0).is_ok());
        prop_assert_eq!(request::validate_binary("objects/restore", &bytes, 1).unwrap_err().code, wire::ErrorCode::InvalidArgument);
        for invalid in invalid_requests(&query) {
            prop_assert_eq!(request::restore_digest(&invalid).unwrap_err().code, wire::ErrorCode::InvalidArgument);
            prop_assert_eq!(request::validate_binary("objects/restore", &invalid.encode_to_vec(), 0).unwrap_err().code, wire::ErrorCode::InvalidArgument);
        }
    }
    #[test]
    fn restore_digest_ignores_only_retry_key(query in requests()) {
        let digest = request::restore_digest(&query).unwrap();
        let mut changed = query.clone();
        changed.mutation.as_mut().unwrap().idempotency_key = "different-key".into();
        prop_assert_eq!(request::restore_digest(&changed).unwrap(), digest);
        for field in 0..4 {
            let mut changed = query.clone();
            match field {
                0 => changed.bucket.as_mut().unwrap().name = if query.bucket.as_ref().unwrap().name == "other" { "another" } else { "other" }.into(),
                1 => changed.prefix.push('x'),
                2 => changed.at.as_mut().unwrap().seconds = if query.at.as_ref().unwrap().seconds == 0 { 1 } else { 0 },
                _ => changed.at.as_mut().unwrap().nanos = if query.at.as_ref().unwrap().nanos == 1000 { 2000 } else { 1000 },
            }
            prop_assert_ne!(request::restore_digest(&changed).unwrap(), digest);
        }
    }
}

#[test]
fn restore_property_detects_dropped_prefix_negative_control() {
    use proptest::test_runner::{Config, TestCaseError, TestRunner};
    let mut runner = TestRunner::new(Config {
        cases: 8,
        failure_persistence: None,
        ..Config::default()
    });
    let result = runner.run(&requests(), |query| {
        let mut changed = query.clone();
        changed.prefix.push('x');
        // Deliberately broken digest excludes the prefix as well as the retry key.
        let broken = |mut q: wire::RestoreObjectsRequest| {
            q.prefix.clear();
            request::restore_digest(&q).unwrap()
        };
        if broken(query) == broken(changed) {
            return Err(TestCaseError::fail("lost prefix"));
        }
        Ok(())
    });
    assert!(result.is_err());
}

#[tokio::test]
async fn restore_memory_is_unsupported() {
    let provider = MemoryObjects::new(MemoryOptions::default()).unwrap();
    let query = wire::RestoreObjectsRequest {
        bucket: Some(wire::BucketRef {
            name: "customer.inputs".into(),
        }),
        prefix: String::new(),
        at: Some(prost_types::Timestamp {
            seconds: 42,
            nanos: 1000,
        }),
        mutation: Some(wire::MutationIdentity {
            idempotency_key: "restore".into(),
        }),
    };
    assert_eq!(
        provider.restore(query).await.unwrap_err().code,
        wire::ErrorCode::Unsupported
    );
}

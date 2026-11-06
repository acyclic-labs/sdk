#![no_main]

// Exercise the shared production codec with the harness's exact core types.
use acyclic_harness_core::{Error, InteractionRejection, OperationId, Result, wire};
#[path = "../../harness/src/client/command_result.rs"]
mod command_result;
use command_result::{HeadlessOutcome, decode_command_result, encode_command_result};
use serde_json::{Value, json};

fn check(sequence: u64, result: &Result<Value>, completion: &str, exit: u8) {
    let operation = OperationId::from_bytes([3; 16]);
    let full = match result {
        Ok(value) => {
            json!({"version":1,"operation":operation,"sequence":sequence,"completion":completion,"ok":value})
        }
        Err(error) => {
            json!({"version":1,"operation":operation,"sequence":sequence,"completion":completion,
                "error":error.classification(),"diagnostic":error.to_string()})
        }
    };
    let mut omitted = json!({"version":1,"operation":operation,"sequence":sequence,
        "completion":completion,"output_omitted":true});
    if let Err(error) = result {
        omitted["error"] = serde_json::to_value(error.classification()).unwrap();
    }
    let full_size = serde_json::to_vec(&full).unwrap().len() as u64;
    let omitted_size = serde_json::to_vec(&omitted).unwrap().len() as u64;
    for maximum in [
        0,
        full_size - 1,
        full_size,
        omitted_size - 1,
        omitted_size,
        u64::MAX,
    ] {
        let expected = if full_size <= maximum {
            Some(&full)
        } else if omitted_size <= maximum {
            Some(&omitted)
        } else {
            None
        };
        match (
            encode_command_result(sequence, operation, result, maximum),
            expected,
        ) {
            (Ok(bytes), expected) => {
                assert!(
                    bytes.len() as u64 <= maximum,
                    "encoded command result must remain within the admitted byte bound"
                );
                let decoded: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(
                    Some(&decoded),
                    expected,
                    "settled result must retain completion and payload"
                );
                let completion: HeadlessOutcome =
                    serde_json::from_value(decoded["completion"].clone()).unwrap();
                assert_eq!(completion.exit_code(), exit);
                let received = decode_command_result(operation, sequence, &bytes);
                match (result, received) {
                    (Ok(value), Ok(received)) => assert_eq!(&received, value),
                    (Ok(_), Err(Error::OutputOmitted(id))) if decoded["output_omitted"] == true => {
                        assert_eq!(id, operation);
                    }
                    (Err(original), Err(mut received)) => {
                        assert_eq!(
                            received.operation_id(),
                            original.operation_id(),
                            "receiver must preserve the original uncertain operation"
                        );
                        assert_eq!(
                            received.code(),
                            original.code(),
                            "receiver must preserve the original failure code"
                        );
                        assert_eq!(received.uncertain(), original.uncertain());
                        assert_eq!(
                            received.leaves_outcome_uncertain(),
                            original.leaves_outcome_uncertain()
                        );
                        assert_eq!(received.is_native_failure(), original.is_native_failure());
                        assert_eq!(
                            received.request_operation_id(),
                            Some(operation),
                            "receiver must preserve the original request identity separately"
                        );
                        assert_eq!(
                            received.classification(),
                            original.classification(),
                            "receiver must preserve the original failure's semantic facts"
                        );
                        match &received {
                            Error::Command(failure) if decoded["output_omitted"] == true => {
                                assert!(
                                    failure.diagnostic().is_none(),
                                    "omitted display must remain absent"
                                );
                            }
                            Error::Command(failure) => {
                                assert_eq!(
                                    failure.diagnostic(),
                                    Some(original.to_string().as_str())
                                );
                            }
                            _ => panic!("a valid failure must remain a typed remote projection"),
                        }
                        for _ in 0..2 {
                            received = Error::after_cleanup::<()>(
                                Err(received),
                                Err(Error::Conflict("retirement refused".into())),
                            )
                            .unwrap_err();
                            assert_eq!(
                                received.request_operation_id(),
                                Some(operation),
                                "cleanup must retain the original request identity"
                            );
                            assert_eq!(
                                received.operation_id(),
                                original.operation_id(),
                                "cleanup must retain the distinct uncertain effect identity"
                            );
                        }
                    }
                    (_, received) => panic!("receiver changed the original result: {received:?}"),
                }
                assert!(
                    matches!(decode_command_result(OperationId::from_bytes([4; 16]), sequence, &bytes),
                    Err(error) if error.request_operation_id() == Some(OperationId::from_bytes([4; 16])) && error.code() == wire::ErrorCode::Indeterminate),
                    "a reply cannot settle a different request"
                );
                assert!(
                    matches!(decode_command_result(operation, sequence.wrapping_add(1), &bytes),
                    Err(error) if error.request_operation_id() == Some(operation) && error.code() == wire::ErrorCode::Indeterminate),
                    "a reply cannot settle a different sequence"
                );
                for (field, value) in [
                    ("version", json!(2)),
                    ("unknown", json!(true)),
                    ("completion", json!("unrecognised")),
                ] {
                    let mut invalid = decoded.clone();
                    invalid[field] = value;
                    assert!(
                        matches!(decode_command_result(operation, sequence, &serde_json::to_vec(&invalid).unwrap()),
                        Err(error) if error.request_operation_id() == Some(operation) && error.code() == wire::ErrorCode::Indeterminate),
                        "malformed replies must retain the request's reconciliation identity"
                    );
                }
                let invalid = if result.is_err() {
                    let mut invalid = decoded.clone();
                    invalid["error"]["code"] = json!(i32::MAX);
                    invalid
                } else {
                    let mut invalid = decoded.clone();
                    invalid["diagnostic"] = json!("success cannot contain a failure diagnostic");
                    invalid
                };
                assert!(
                    matches!(decode_command_result(operation, sequence, &serde_json::to_vec(&invalid).unwrap()),
                    Err(error) if error.request_operation_id() == Some(operation) && error.code() == wire::ErrorCode::Indeterminate)
                );
                let mut duplicate = b"{\"version\":1,".to_vec();
                duplicate.extend_from_slice(&bytes[1..]);
                assert!(
                    matches!(decode_command_result(operation, sequence, &duplicate),
                    Err(error) if error.request_operation_id() == Some(operation) && error.code() == wire::ErrorCode::Indeterminate),
                    "duplicate terminal fields must not be admitted"
                );
            }
            (Err(_), None) => {}
            (Err(error), Some(_)) => panic!("an admitted terminal outcome disappeared: {error}"),
        }
    }
}

fn check_cleanup_outcomes() {
    use acyclic_harness_core::goal::GoalBlockedReason;

    for cleanup in 0..3 {
        for (original, outcomes) in [
            #[cfg(any(feature = "runtime", feature = "native-skill-records"))]
            (
                Error::NativeBoundary {
                    code: acyclic_harness_core::wire::ErrorCode::Invalid,
                    reason: "original native boundary failure",
                },
                [("failed", 1), ("indeterminate", 4), ("failed", 1)],
            ),
            #[cfg(any(feature = "runtime", feature = "native-skill-records"))]
            (
                Error::NativeJson {
                    code: acyclic_harness_core::wire::ErrorCode::Invalid,
                    source: serde_json::from_slice::<Value>(b"{").unwrap_err(),
                    custody: None,
                },
                [("failed", 1), ("indeterminate", 4), ("failed", 1)],
            ),
            (
                Error::GoalBlocked(GoalBlockedReason::Declared),
                [("blocked", 3), ("indeterminate", 4), ("failed", 1)],
            ),
            (
                Error::GoalBlocked(GoalBlockedReason::TaskViewUnreachable),
                [("blocked", 3), ("indeterminate", 4), ("failed", 1)],
            ),
            (
                Error::InteractionRejected(InteractionRejection::Denied),
                [("blocked", 3), ("indeterminate", 4), ("failed", 1)],
            ),
            (
                Error::InteractionRejected(InteractionRejection::Declined),
                [("blocked", 3), ("indeterminate", 4), ("failed", 1)],
            ),
            (
                Error::InteractionRejected(InteractionRejection::Expired),
                [("blocked", 3), ("indeterminate", 4), ("failed", 1)],
            ),
            (
                Error::InteractionRejected(InteractionRejection::Cancelled),
                // An interaction refusal is settled; uncertain cleanup takes precedence.
                [("cancelled", 2), ("indeterminate", 4), ("failed", 1)],
            ),
            (Error::Cancelled, [("cancelled", 2); 3]),
            (
                Error::Unsupported("missing capability".into()),
                [("failed", 1), ("indeterminate", 4), ("failed", 1)],
            ),
            (
                Error::Storage("provider write failed".into()),
                [("failed", 1); 3],
            ),
            (
                Error::Indeterminate(OperationId::from_bytes([7; 16])),
                [("indeterminate", 4); 3],
            ),
        ] {
            let (terminal, exit) = outcomes[cleanup];
            let cleanup = match cleanup {
                1 => Error::Indeterminate(OperationId::from_bytes([8; 16])),
                2 => Error::Storage("retirement write failed".into()),
                _ => Error::Conflict("retirement failed".into()),
            };
            let mut result = Error::after_cleanup::<Value>(Err(original), Err(cleanup));
            for _ in 0..3 {
                check(3, &result, terminal, exit);
                result = Error::after_cleanup(result, Err(Error::Conflict("x".repeat(1024))));
            }
        }
    }
}
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    use wire::ErrorCode;
    // Caller-supplied typed refusal classes, not fabricated provider responses.
    let classes = [
        (ErrorCode::Unspecified, "failed", 1),
        (ErrorCode::NotFound, "failed", 1),
        (ErrorCode::Conflict, "failed", 1),
        (ErrorCode::Unsupported, "failed", 1),
        (ErrorCode::Invalid, "failed", 1),
        (ErrorCode::Unauthorized, "failed", 1),
        (ErrorCode::Unavailable, "failed", 1),
        (ErrorCode::Storage, "failed", 1),
        (ErrorCode::Indeterminate, "indeterminate", 4),
        (ErrorCode::InteractionCancelled, "cancelled", 2),
        (ErrorCode::Cancelled, "cancelled", 2),
        (ErrorCode::InteractionDeclined, "blocked", 3),
        (ErrorCode::InteractionExpired, "blocked", 3),
        (ErrorCode::InteractionDenied, "blocked", 3),
    ];
    let sequence = u64::from_le_bytes(
        bytes
            .get(..8)
            .and_then(|bytes| bytes.try_into().ok())
            .unwrap_or_default(),
    );
    // Reach the actual receiver with arbitrary bytes, including historical writer captures.
    let _ = decode_command_result(OperationId::from_bytes([3; 16]), sequence, bytes);
    let value = serde_json::from_slice(bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(bytes).into_owned()));
    check(sequence, &Ok(value), "completed", 0);
    check(
        sequence,
        &Err(Error::Storage(String::from_utf8_lossy(bytes).into_owned())),
        "failed",
        1,
    );
    let (code, completion, exit) =
        classes[usize::from(bytes.first().copied().unwrap_or_default()) % classes.len()];
    check(
        sequence,
        &Err(Error::ToolRejected {
            code,
            reason: "caller refusal",
        }),
        completion,
        exit,
    );

    let uncertain_operation = OperationId::from_bytes([7; 16]);
    let cleanup = Error::after_cleanup::<Value>(
        Err(Error::Indeterminate(uncertain_operation)),
        Err(Error::Storage(String::from_utf8_lossy(bytes).into_owned())),
    );
    check(sequence, &cleanup, "indeterminate", 4);
    let cause = cleanup.unwrap_err();
    let classification = cause.classification();
    let scoped = cause.for_request(OperationId::from_bytes([3; 16]));
    assert_eq!(
        scoped.classification(),
        classification,
        "scoping a request must preserve the actual local cause classification"
    );
    let Error::Command(ref retained) = scoped else {
        panic!("local request must retain its actual cause")
    };
    let retained_pointer = retained.as_ref() as *const _;
    let original = retained.original().expect("local cause is retained");
    assert_eq!(original.classification(), classification);
    let scoped = scoped.for_request(OperationId::from_bytes([3; 16]));
    let Error::Command(ref retained) = scoped else {
        unreachable!()
    };
    assert_eq!(
        retained.as_ref() as *const _,
        retained_pointer,
        "already scoped request must not allocate or wrap its cause again"
    );
    check(sequence, &Err(scoped), "indeterminate", 4);

    static ADMISSION: std::sync::Once = std::sync::Once::new();
    ADMISSION.call_once(|| {
        check_cleanup_outcomes();
        #[cfg(feature = "runtime")]
        {
            check(
                0,
                &Err(Error::NativeBoundary {
                    code: ErrorCode::Invalid,
                    reason: "original native boundary failure",
                }),
                "failed",
                1,
            );
            check(
                0,
                &Err(Error::NativeJson {
                    code: ErrorCode::Invalid,
                    source: serde_json::from_slice::<Value>(b"{").unwrap_err(),
                    custody: None,
                }),
                "failed",
                1,
            );
        }
        let capture = std::env::var_os("ACYCLIC_FUZZ_CORPUS").map(std::path::PathBuf::from);
        for (index, (code, completion, exit)) in classes.into_iter().enumerate() {
            let result = Err(Error::ToolRejected {
                code,
                reason: "caller refusal",
            });
            for sequence in [0, u64::MAX] {
                check(sequence, &result, completion, exit);
            }
            if let Some(directory) = &capture {
                std::fs::write(
                    directory.join(format!("class-{index}")),
                    encode_command_result(
                        u64::MAX,
                        OperationId::from_bytes([3; 16]),
                        &result,
                        u64::MAX,
                    )
                    .unwrap(),
                )
                .unwrap();
            }
        }
        for (name, result, completion, exit) in [
            ("null", Ok(Value::Null), "completed", 0),
            (
                "task-failed",
                Err(Error::TaskFailed {
                    task: acyclic_harness_core::TaskId::from_bytes([8; 16]),
                    message: "retained task failure".into(),
                }),
                "failed",
                1,
            ),
            (
                "native-process",
                Err(Error::Process(acyclic_harness_core::ProcessError::Io {
                    exit_code: None,
                    message: "original process failure".into(),
                })),
                "failed",
                1,
            ),
            (
                "model-provider",
                Err(Error::ModelUnavailable {
                    failure: acyclic_harness_core::ModelTransportFailure::Tls,
                    excerpt: "original provider failure".into(),
                }),
                "failed",
                1,
            ),
            ("cancelled", Err(Error::Cancelled), "cancelled", 2),
            (
                "interaction-declined",
                Err(Error::InteractionRejected(InteractionRejection::Declined)),
                "blocked",
                3,
            ),
            (
                "interaction-cancelled",
                Err(Error::InteractionRejected(InteractionRejection::Cancelled)),
                "cancelled",
                2,
            ),
            (
                "interaction-expired",
                Err(Error::InteractionRejected(InteractionRejection::Expired)),
                "blocked",
                3,
            ),
            (
                "interaction-denied",
                Err(Error::InteractionRejected(InteractionRejection::Denied)),
                "blocked",
                3,
            ),
            (
                "storage",
                Err(Error::Storage("caller diagnostic".into())),
                "failed",
                1,
            ),
            (
                "goal-blocked",
                Err(Error::GoalBlocked(
                    acyclic_harness_core::goal::GoalBlockedReason::Declared,
                )),
                "blocked",
                3,
            ),
            (
                "escaped",
                Ok(json!({"stdout":"\"\nλ".repeat(128)})),
                "completed",
                0,
            ),
        ] {
            check(u64::MAX, &result, completion, exit);
            if let Some(directory) = &capture {
                std::fs::write(
                    directory.join(name),
                    encode_command_result(
                        u64::MAX,
                        OperationId::from_bytes([3; 16]),
                        &result,
                        u64::MAX,
                    )
                    .unwrap(),
                )
                .unwrap();
            }
        }
    });
});

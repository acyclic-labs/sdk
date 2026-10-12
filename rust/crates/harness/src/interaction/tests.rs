use super::*;

use super::*;
use crate::{
    AgentId,
    conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};
use serde_json::json;

#[test]
fn outcome_rejects_unrecognized_fields() {
    assert!(
        serde_json::from_str::<InteractionOutcome>(r#"{"kind":"approved","answer":"hidden"}"#)
            .is_err()
    );
    assert!(serde_json::from_str::<InteractionOutcome>(
            r#"{"kind":"indeterminate","operation_id":"00000000-0000-0000-0000-000000000001","extra":true}"#).is_err());
}

fn file(path: &str, bytes: &[u8]) -> Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
            "interaction-private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([8; 16])),
            &mut None,
        )?,
        path.into(),
        "immutable-1".into(),
        FileDescriptor::from_bytes(bytes, "application/json", &mut None)?,
        "interaction.json".into(),
        &mut None,
    )
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_budget() -> acyclic_fs::WorkBudget {
    acyclic_fs::WorkBudget {
        source_bytes_read: 1_048_576,
        bytes_hashed: 1_048_576,
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        items_examined: 1_048_576,
        allocation_operations: 1_048_576,
        peak_allocation_bytes: 1_048_576,
        ..acyclic_fs::WorkBudget::default()
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_typed_request_error_keeps_original_serde_source_and_custody() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};
    let bytes = br#"{"type":"approval","prompt":"Proceed?","operation_id":"00000000-0000-0000-0000-000000000001","action_digest":[3]}"#;
    let ticket = InteractionTicket {
        id: Uuid::from_bytes([1; 16]),
        kind: InteractionKind::Approval,
        request: file("native-approval.json", bytes)?,
        deadline_unix_ms: None,
        approval: Some(ApprovalBinding {
            operation_id: OperationId::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
            action_digest: [3; 32],
        }),
    };
    let original = serde_json::from_slice::<Interaction>(bytes).expect_err("fixed digest");
    let cancellation = CancellationToken::new();
    let native =
        NativeMaterializationWork::new(native_budget(), &cancellation).expect("original ledger");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    let error = ticket
        .validate_request_bytes(bytes, &mut Some(&mut io))
        .expect_err("original typed digest rejects malformed length");
    let Error::NativeJson {
        code,
        source,
        custody: Some(custody),
    } = error
    else {
        panic!("native request must preserve its original typed cause and guard");
    };
    assert_eq!(code, crate::wire::ErrorCode::Invalid);
    assert_eq!(source.classify(), original.classify());
    assert_eq!(
        (source.line(), source.column()),
        (original.line(), original.column())
    );
    assert!(custody.bytes() > 0);
    assert!(native.shares_ledger(custody.work()));
    assert_eq!(io.into_custody()?.bytes(), 0);
    drop(source);
    drop(custody);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_unit_choice_validation_retires_all_temporary_custody() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};
    let bytes = br#"{"type":"choice","prompt":"Choose","options":[{"id":"one","label":"One"},{"id":"two","label":"Two"}],"min_selections":1,"max_selections":1}"#;
    let ticket = InteractionTicket {
        id: Uuid::from_bytes([1; 16]),
        kind: InteractionKind::Choice,
        request: file("native-choice.json", bytes)?,
        deadline_unix_ms: None,
        approval: None,
    };
    ticket.validate_request_bytes(bytes, &mut None)?;
    let cancellation = CancellationToken::new();
    let native =
        NativeMaterializationWork::new(native_budget(), &cancellation).expect("original ledger");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    ticket.validate_request_bytes(bytes, &mut Some(&mut io))?;
    assert_eq!(io.into_custody()?.bytes(), 0);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_request_preserves_original_raw_value_schema_semantics() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};
    let schema = json!({"const": vec![false; 64]});
    let embedded =
        serde_json::to_string(&schema).map_err(|error| Error::Invalid(error.to_string()))?;
    let bytes = serde_json::to_vec(&json!({
        "type": "question",
        "prompt": "Choose",
        "response_schema": {"$serde_json::private::RawValue": embedded},
    }))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let ticket = InteractionTicket {
        id: Uuid::from_bytes([1; 16]),
        kind: InteractionKind::Question,
        request: file("native-raw-schema.json", &bytes)?,
        deadline_unix_ms: None,
        approval: None,
    };
    let Interaction::Question {
        response_schema, ..
    } = ticket.validate_request_bytes(&bytes, &mut None)?.0
    else {
        panic!("original question");
    };
    assert_eq!(response_schema, schema);
    let cancellation = CancellationToken::new();
    let native =
        NativeMaterializationWork::new(native_budget(), &cancellation).expect("original ledger");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    ticket.validate_request_bytes(&bytes, &mut Some(&mut io))?;
    assert_eq!(io.into_custody()?.bytes(), 0);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_answer_error_retains_original_response_cause_after_request_drop() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};
    let request_bytes = br#"{"type":"choice","prompt":"Choose","options":[{"id":"one","label":"One"}],"min_selections":1,"max_selections":1}"#;
    let bytes = br#"{"type":"choice","option_ids":[1]}"#;
    let ticket = InteractionTicket {
        id: Uuid::from_bytes([1; 16]),
        kind: InteractionKind::Choice,
        request: file("native-request.json", request_bytes)?,
        deadline_unix_ms: None,
        approval: None,
    };
    let answer = file("native-answer.json", bytes)?;
    let original = serde_json::from_slice::<InteractionResponse>(bytes).expect_err("string ids");
    let cancellation = CancellationToken::new();
    let native =
        NativeMaterializationWork::new(native_budget(), &cancellation).expect("original ledger");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    let request = ticket.validate_request_bytes(request_bytes, &mut Some(&mut io))?;
    let error = ticket
        .validate_answer_bytes(&request.0, &answer, bytes, &mut Some(&mut io))
        .expect_err("original typed response rejects integer ids");
    drop(request);
    let Error::NativeJson {
        code,
        source,
        custody: Some(custody),
    } = error
    else {
        panic!("original response cause and custody");
    };
    assert_eq!(code, crate::wire::ErrorCode::Invalid);
    assert_eq!(source.classify(), original.classify());
    assert_eq!(
        (source.line(), source.column()),
        (original.line(), original.column())
    );
    assert!(custody.bytes() > 0);
    assert!(native.shares_ledger(custody.work()));
    assert_eq!(io.into_custody()?.bytes(), 0);
    drop(source);
    drop(custody);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_request_carrier_retires_parser_and_keeps_actual_fields_through_response() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};
    let padding = "x".repeat(30_000);
    let request_bytes = serde_json::to_vec(&json!({
        "type": "choice", "prompt": padding,
        "options": [{"id": "one", "label": "One"}],
        "min_selections": 1, "max_selections": 1,
    }))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let bytes = serde_json::to_vec(&json!({
        "type": "choice", "option_ids": [{"label": padding}],
    }))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let ticket = InteractionTicket {
        id: Uuid::from_bytes([1; 16]),
        kind: InteractionKind::Choice,
        request: file("native-large-request.json", &request_bytes)?,
        deadline_unix_ms: None,
        approval: None,
    };
    let answer = file("native-large-answer.json", &bytes)?;
    let cancellation = CancellationToken::new();
    let native =
        NativeMaterializationWork::new(native_budget(), &cancellation).expect("original ledger");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    let request = ticket.validate_request_bytes(&request_bytes, &mut Some(&mut io))?;
    let guard = request.2.as_ref().expect("original native DTO guard");
    assert_eq!(request.1, 0);
    assert_eq!(
        guard.bytes(),
        interaction_request_owned_bytes(&request.0, &None)?
    );
    assert!(native.shares_ledger(guard.work()));
    let error = ticket
        .validate_answer_bytes(&request.0, &answer, &bytes, &mut Some(&mut io))
        .expect_err("original typed string id rejects the funded response object");
    assert!(matches!(
        error,
        Error::NativeJson {
            code: crate::wire::ErrorCode::Invalid,
            ..
        }
    ));
    assert_eq!(io.into_custody()?.bytes(), 0);
    drop(error);
    drop(request);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_decision_artifact_keeps_exact_original_outcome_binding() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};
    let operation_id = OperationId::from_bytes([4; 16]);
    let request = Interaction::approval("Proceed?".into(), operation_id, [5; 32])?;
    let request_bytes =
        serde_json::to_vec(&request).map_err(|error| Error::Invalid(error.to_string()))?;
    let ticket = InteractionTicket {
        id: Uuid::from_bytes([1; 16]),
        kind: InteractionKind::Approval,
        request: file("native-approval-request.json", &request_bytes)?,
        deadline_unix_ms: None,
        approval: Some(ApprovalBinding {
            operation_id,
            action_digest: [5; 32],
        }),
    };
    let bytes = br#"{"type":"approval","approved":false,"reason":"Declined"}"#;
    let detail = file("native-approval-decision.json", bytes)?;
    let cancellation = CancellationToken::new();
    let native =
        NativeMaterializationWork::new(native_budget(), &cancellation).expect("original ledger");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    let request = ticket.validate_request_bytes(&request_bytes, &mut Some(&mut io))?;
    ticket.validate_decision_bytes(
        &request.0,
        &InteractionOutcome::Declined,
        &detail,
        bytes,
        &mut Some(&mut io),
    )?;
    drop(request);
    let request = ticket.validate_request_bytes(&request_bytes, &mut Some(&mut io))?;
    assert!(matches!(
        ticket.validate_decision_bytes(
            &request.0,
            &InteractionOutcome::Approved,
            &detail,
            bytes,
            &mut Some(&mut io),
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Conflict,
            reason: "approval artifact disagrees with outcome",
        })
    ));
    drop(request);
    assert_eq!(io.into_custody()?.bytes(), 0);
    Ok(())
}

#[test]
fn staged_question_and_answer_are_schema_validated_before_ref_publication() -> Result<()> {
    let request = Interaction::question(
        "Choose a positive number".into(),
        json!({"type": "integer", "minimum": 1}),
    )?;
    let request_bytes =
        serde_json::to_vec(&request).map_err(|error| Error::Invalid(error.to_string()))?;
    let ticket = InteractionTicket {
        id: Uuid::from_bytes([1; 16]),
        kind: InteractionKind::Question,
        request: file("request.json", &request_bytes)?,
        deadline_unix_ms: None,
        approval: None,
    };
    let admitted = ticket.validate_request_bytes(&request_bytes, &mut None)?.0;
    let response_bytes = br#"{"type":"question","value":2}"#;
    let answer = file("answer.json", response_bytes)?;
    ticket.validate_answer_bytes(&admitted, &answer, response_bytes, &mut None)?;
    let non_json = FileRef::new(
        answer.volume().clone(),
        "answer.txt".into(),
        "immutable-1".into(),
        FileDescriptor::from_bytes(response_bytes, "text/plain", &mut None)?,
        "answer.txt".into(),
        &mut None,
    )?;
    assert!(matches!(
        (InteractionResolution {
            id: ticket.id,
            expected_version: 1,
            outcome: InteractionOutcome::Answered {
                answer: Box::new(non_json)
            },
            detail: None,
        })
        .validate(&ticket),
        Err(Error::Invalid(_))
    ));
    let invalid_bytes = br#"{"type":"question","value":0}"#;
    let invalid = file("invalid-answer.json", invalid_bytes)?;
    assert!(matches!(
        ticket.validate_answer_bytes(&admitted, &invalid, invalid_bytes, &mut None),
        Err(Error::Invalid(_))
    ));
    assert!(
        ticket
            .validate_answer_bytes(&admitted, &answer, invalid_bytes, &mut None)
            .is_err()
    );
    Ok(())
}

#[test]
fn approval_binding_cannot_change_with_display_json() -> Result<()> {
    let operation_id = OperationId::from_bytes([4; 16]);
    let request = Interaction::approval("Proceed?".into(), operation_id, [5; 32])?;
    let request_bytes =
        serde_json::to_vec(&request).map_err(|error| Error::Invalid(error.to_string()))?;
    let mut ticket = InteractionTicket {
        id: Uuid::from_bytes([2; 16]),
        kind: InteractionKind::Approval,
        request: file("approval.json", &request_bytes)?,
        deadline_unix_ms: Some(10),
        approval: Some(ApprovalBinding {
            operation_id,
            action_digest: [6; 32],
        }),
    };
    assert!(matches!(
        ticket.validate_request_bytes(&request_bytes, &mut None),
        Err(Error::Conflict(_))
    ));
    ticket
        .approval
        .as_mut()
        .ok_or_else(|| Error::Invalid("missing approval".into()))?
        .action_digest = [5; 32];
    ticket.validate_request_bytes(&request_bytes, &mut None)?;
    assert!(matches!(
        InteractionResolution {
            id: ticket.id,
            expected_version: 1,
            outcome: InteractionOutcome::Answered {
                answer: Box::new(file("answer.json", b"null")?)
            },
            detail: None,
        }
        .validate(&ticket),
        Err(Error::Invalid(_))
    ));
    Ok(())
}

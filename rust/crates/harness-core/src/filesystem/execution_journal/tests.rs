use super::*;

use crate::filesystem::*;
use crate::{AgentId, conversation::VolumeOwner, executor::ModelPurpose, resources::ProviderRef};

mod staged_ref;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one production journal roundtrip and isolated persisted-header adversaries"
)]
async fn observation_reopen_preserves_original_turn_and_rejects_header_substitution() -> Result<()>
{
    let provider = ProviderRef::new(
        "observation-identity".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let agent = AgentId::new();
    let volume = VolumeRef::new(
        provider,
        "observation-identity".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityRoot::new(
        "observation-identity",
        [81; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: crate::ConversationId::new().to_string(),
        },
    );
    let scope = issuer
        .bootstrap_scope(
            "owner",
            &crate::Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            &mut None,
        )
        .expect("scope proof");
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let open = |operation, turn| {
        FilesystemExecutionJournal::new(
            stream.clone(),
            host.clone(),
            crate::filesystem::FilesystemJournalBinding {
                volume: &volume,
                verifier: &issuer.verifier(&mut None).expect("verifier copy"),
                scope: &scope,
                io: None,
            },
        )
        .map(|journal| journal.with_turn(operation, turn))
    };
    let turn = OperationId::new();
    let operation = OperationId::new();
    assert_ne!(operation, turn);
    let event = ExecutionEvent::Started {
        request_digest: [9; 32],
    };
    open(operation, turn)?
        .append(operation, "start", &event, None)?
        .await?;
    let reopened = open(operation, turn)?;
    let records = reopened.replay(operation, 0, 1, None)?.await?;
    assert_eq!(records.len(), 1);
    assert_eq!(records.first().map(|record| &record.event), Some(&event));
    reopened.append(operation, "start", &event, None)?.await?;
    assert_eq!(reopened.replay(operation, 0, 2, None)?.await?.len(), 1);
    assert!(matches!(
        open(operation, OperationId::new())?
            .replay(operation, 0, 1, None)?
            .await,
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        reopened.replay(turn, 0, 1, None)?.await,
        Err(Error::Unauthorized(_))
    ));
    let summary_operation = ModelPurpose::Summary.attempt_operation(operation, None)?;
    assert_ne!(summary_operation, operation);
    for mutation in [
        "wrong_execution",
        "summary_attempt_as_execution",
        "wrong_turn",
        "missing_turn",
        "missing_version",
        "old_version",
        "unknown_version",
        "wrong_version_type",
        "unknown_field",
    ] {
        let target = OperationId::new();
        let mut wire = serde_json::to_value(Observation {
            version: OBSERVATION_VERSION,
            operation_id: target,
            turn_operation: turn,
            retry_digest: blake3::hash(b"start").to_hex().to_string(),
            event: event.clone(),
        })
        .map_err(|error| Error::Invalid(error.to_string()))?;
        match mutation {
            "wrong_execution" => wire["operation_id"] = serde_json::json!(operation),
            "summary_attempt_as_execution" => {
                wire["operation_id"] = serde_json::json!(summary_operation);
            }
            "wrong_turn" => wire["turn_operation"] = serde_json::json!(OperationId::new()),
            "missing_turn" => {
                wire.as_object_mut()
                    .ok_or_else(|| Error::Invalid("envelope".into()))?
                    .remove("turn_operation");
            }
            "missing_version" => {
                wire.as_object_mut()
                    .ok_or_else(|| Error::Invalid("envelope".into()))?
                    .remove("version");
            }
            "old_version" => wire["version"] = serde_json::json!(1),
            "unknown_version" => wire["version"] = serde_json::json!(3),
            "wrong_version_type" => wire["version"] = serde_json::json!("2"),
            "unknown_field" => wire["event_operation"] = serde_json::json!(operation),
            _ => unreachable!("enumerated envelope mutation"),
        }
        let journal = open(target, turn)?;
        journal
            .path(target)?
            .append_batch(
                vec![
                    Bytes::from(
                        serde_json::to_vec(&wire)
                            .map_err(|error| Error::Invalid(error.to_string()))?,
                    )
                    .into(),
                ],
                Some(0),
                crate::test_support::fresh_key()?,
            )
            .await?;
        assert!(
            matches!(
                open(target, turn)?.replay(target, 0, 1, None)?.await,
                Err(Error::Storage(_) | Error::Conflict(_))
            ),
            "accepted {mutation}"
        );
    }
    Ok(())
}

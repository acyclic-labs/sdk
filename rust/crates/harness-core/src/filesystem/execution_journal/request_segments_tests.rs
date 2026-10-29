use super::*;
use crate::executor::ModelPurpose;
use crate::model::{Model, ModelMessage, ModelRequest, ModelRole, PreparedModelRequest};

fn prepare(messages: &[ModelMessage]) -> Result<PreparedModelRequest> {
    PreparedModelRequest::prepare(
        ModelRequest {
            model: Model::new(
                "test".into(),
                "segmented".into(),
                "1".into(),
                serde_json::Value::Null,
            )?,
            messages: messages.to_vec(),
            tools: Vec::new(),
            max_output_tokens: None,
        },
        Limits::default(),
        None,
    )
}

#[tokio::test]
async fn admitted_request_append_reads_only_the_manifest_at_every_history_length() -> Result<()> {
    let (journal, _issuer) = test_support::journal().await?;
    let operation = OperationId::new();
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    let mut retained: Option<PreparedModelRequest> = None;
    let mut previous = None;
    for step in 1..=1000 {
        let message = ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text(format!("turn {step}")),
        };
        let prepared = match retained.take() {
            Some(previous) => {
                let mut continued = previous.into_continuation();
                continued.messages().push(message);
                continued.finish(Limits::default(), None)?
            }
            None => prepare(&[message])?,
        };
        let key = format!("request:{step}");
        let request = journal
            .stage_prepared_model_request(operation, &key, &prepared, previous.as_ref(), None)?
            .ok_or_else(|| Error::Invalid("missing request admission".into()))?
            .await?;
        assert_eq!(
            request.descriptor().media_type(),
            if step == 1 {
                "application/json"
            } else {
                crate::executor::transcript::REQUEST_MEDIA
            }
        );
        if [1, 100, 1000].contains(&step) {
            let io = PreparationIo::from_budget(
                acyclic_fs::performance::work_budget(64 * 1024 * 1024, 8 * 1024 * 1024),
                &acyclic_fs::CancellationToken::new(),
            )?;
            let event = ExecutionEvent::ModelStarted {
                step,
                purpose: ModelPurpose::Response,
                request: request.clone(),
                request_digest: prepared.request_digest(),
            };
            let before = io.receipt();
            journal
                .append(operation, &format!("model:{step}"), &event, Some(&io))?
                .await?;
            let loaded = io.receipt().object_bytes_read - before.object_bytes_read;
            assert!(
                loaded < 4096,
                "history {step} loaded {loaded} payload bytes"
            );
            eprintln!("request_admission steps={step} payload_bytes={loaded}");
            if step == 100 {
                check_unattested_and_forged(
                    &journal,
                    operation,
                    &request,
                    prepared.request_digest(),
                )
                .await?;
            }
        }
        previous = Some(request);
        retained = Some(prepared);
    }
    Ok(())
}

async fn check_unattested_and_forged<P, A, O>(
    journal: &FilesystemExecutionJournal<P, A, O>,
    operation: OperationId,
    reference: &FileRef,
    digest: [u8; 32],
) -> Result<()>
where
    P: crate::store::JournalProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let bytes = journal.load(reference, None)?.await?;
    let manifest: serde_json::Value = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    for (key, forged) in [("unattested", false), ("forged", true)] {
        let mut changed = manifest.clone();
        if forged {
            // Retain the genuine proof while changing the admitted content.
            changed["request_digest"] = serde_json::json!(vec![7u8; 32]);
        } else {
            changed["proof"] = serde_json::Value::Null;
        }
        let bytes = crate::contract::canonical_json_bytes(&changed)?;
        let reference = journal
            .stage(
                operation,
                key,
                &bytes,
                crate::executor::transcript::REQUEST_MEDIA,
                None,
            )?
            .await?;
        let event = ExecutionEvent::ModelStarted {
            step: 1001,
            purpose: ModelPurpose::Response,
            request: reference,
            request_digest: if forged { [7; 32] } else { digest },
        };
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(64 * 1024 * 1024, 8 * 1024 * 1024),
            &acyclic_fs::CancellationToken::new(),
        )?;
        let result = journal.verify_event_refs_with_io(&event, Some(&io)).await;
        assert_eq!(
            result.is_ok(),
            !forged,
            "{key}: {result:?}; receipt={:?}",
            io.receipt()
        );
        // Removing the production attestation restores the former walk over
        // every payload and violates the positive test's identical bound.
        assert!(io.receipt().object_bytes_read > 4096);
        if forged {
            assert_eq!(io.owned_bytes()?, 0);
        }
    }
    Ok(())
}

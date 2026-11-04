use super::*;
use crate::executor::ModelPurpose;
use crate::model::{Model, ModelMessage, ModelRequest, ModelRole, PreparedModelRequest};

fn prepare(messages: &[ModelMessage]) -> Result<PreparedModelRequest> {
    PreparedModelRequest::prepare(
        ModelRequest {
            summary: false,
            compose_tools: false,
            model: Model::new("test".into(), "segmented".into(), "1".into())?,
            messages: messages.to_vec(),
            tools: Vec::new(),
            sampling: Default::default(),
            max_output_tokens: None,
        },
        Limits::default(),
        None,
    )
}

#[tokio::test]
async fn admitted_request_append_reads_only_the_manifest_as_history_grows() -> Result<()> {
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
    // Grow the same admitted history to 1,000 messages in geometric batches.
    // The receipt measures the append itself; publishing every intermediate
    // message adds setup work without strengthening that bound.
    for step in [
        1, 2, 4, 8, 16, 32, 64, 100, 128, 192, 256, 384, 512, 640, 768, 1000,
    ] {
        let messages = |start| {
            (start..step).map(|index| ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(format!("turn {}", index + 1)),
            })
        };
        let prepared = match retained.take() {
            Some(previous) => {
                let count = previous.request().messages.len() as u32;
                let mut continued = previous.into_continuation();
                continued.messages().extend(messages(count));
                continued.finish(Limits::default(), None)?
            }
            None => prepare(&messages(0).collect::<Vec<_>>())?,
        };
        assert_eq!(prepared.request().messages.len(), step as usize);
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
                let admitted =
                    crate::executor::ExecutionPayloadField::admitted_request(&journal, &event)
                        .await?;
                assert_eq!(admitted.bytes(), prepared.bytes());
                let expected = crate::model::replay_key(prepared.bytes())?;
                assert_eq!(crate::model::replay_key(admitted.bytes())?, expected);
                let stored = journal.load(&request, None)?.await?;
                assert_ne!(
                    stored.as_ref(),
                    admitted.bytes(),
                    "stored manifest is not the admitted request"
                );
                let mut foreign = event.clone();
                if let ExecutionEvent::ModelStarted { request_digest, .. } = &mut foreign {
                    request_digest[0] ^= 1;
                }
                assert!(
                    crate::executor::ExecutionPayloadField::admitted_request(&journal, &foreign)
                        .await
                        .is_err()
                );
                #[cfg(feature = "metrics-export")]
                {
                    use arrow_array::{Array, StringArray};
                    let mut after = 0;
                    let mut found = false;
                    loop {
                        let page = crate::executor::metrics::journal_page(
                            &journal,
                            operation,
                            "agents/generated",
                            after,
                            |_| None,
                        )
                        .await?;
                        for batch in page.batches {
                            if batch.table == crate::executor::metrics::Table::ModelCalls {
                                let keys = batch
                                    .data
                                    .column_by_name("replay_key")
                                    .unwrap()
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .unwrap();
                                found |= (0..keys.len()).any(|row| {
                                    !keys.is_null(row)
                                        && keys.value(row)
                                            == blake3::Hash::from(expected).to_hex().as_str()
                                });
                            }
                        }
                        if let Some(next) = page.next {
                            after = next
                        } else {
                            break;
                        }
                    }
                    assert!(found, "export must identify reconstructed request content");
                }
            }
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
        let result = journal
            .verify_event_refs_with_io(operation, &event, Some(&io))
            .await;
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

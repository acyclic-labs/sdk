use super::*;
use crate::executor::transcript as segments;

fn request(messages: &[ModelMessage]) -> Result<crate::model::PreparedModelRequest> {
    crate::model::PreparedModelRequest::prepare(
        ModelRequest {
            model: Model::new("test".into(), "segments".into(), "1".into(), Value::Null)?,
            messages: messages.to_vec(),
            tools: Vec::new(),
            max_output_tokens: None,
        },
        Limits::default(),
        None,
    )
}

fn message(index: usize) -> ModelMessage {
    ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text(format!("message {index:04}")),
    }
}

#[tokio::test]
#[cfg(feature = "filesystem")]
async fn a_thousand_request_deltas_write_only_the_suffix_and_reopen_exact_bytes() -> Result<()> {
    let issuer = crate::core::AuthorityIssuer::new(
        "segments",
        [91; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: crate::ConversationId::from_bytes([91; 16]).to_string(),
        },
    );
    let journal = Journal {
        8: Some(issuer.verifier(&mut None)?),
        ..Default::default()
    };
    let operation = OperationId::new();
    let mut messages = Vec::new();
    let mut previous = None;
    let mut largest_delta = 0;
    for step in 1..=1000 {
        messages.push(message(step));
        let prepared = request(&messages)?;
        let key = format!("request:{step}");
        let reads = journal.4.load(Ordering::SeqCst);
        let reference = segments::stage_request(
            &journal,
            operation,
            &key,
            &prepared,
            previous.as_ref(),
            None,
        )
        .await?;
        assert_eq!(
            journal.4.load(Ordering::SeqCst) - reads,
            usize::from(step > 1)
        );
        let prefix = format!("{operation}:{key}");
        let (files, bytes) = journal
            .1
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(&format!("{prefix}:")) || **key == prefix)
            .fold((0, 0), |(files, bytes), (_, (_, value))| {
                (files + 1, bytes + value.len())
            });
        assert_eq!(files, if step == 1 { 3 } else { 2 });
        assert!(bytes < 4096, "step {step} encoded {bytes} bytes");
        largest_delta = largest_delta.max(bytes);
        if [1, 100, 1000].contains(&step) {
            let reopened =
                load_model_request(&journal, &reference, Limits::default(), None).await?;
            assert_eq!(reopened.bytes(), prepared.bytes());
            assert_eq!(reopened.request_digest(), prepared.request_digest());
            if step == 1000 {
                // The former pair of full transcript writes fails exactly the
                // same byte bound; this is actual canonical encoding, not an
                // estimate based on the number of turns.
                let old_request = crate::contract::canonical_json_bytes(prepared.request())?;
                let old_projection = crate::contract::canonical_json_bytes(&ResponseProjection {
                    context: crate::context::Context {
                        messages: messages.clone(),
                        ..Default::default()
                    },
                    canonical: None,
                    checkpoint: None,
                    working: None,
                })?;
                assert!(old_request.len() + old_projection.len() > 4096);
                eprintln!(
                    "transcript_delta turns={step} max_written={largest_delta} old_written={}",
                    old_request.len() + old_projection.len()
                );
            }
        }
        previous = Some(reference);
    }
    Ok(())
}

#[tokio::test]
async fn rewritten_prefixes_and_shorter_compaction_roots_never_reuse_stale_messages() -> Result<()>
{
    let journal = Journal::default();
    let operation = OperationId::new();
    let first = request(&[message(1), message(2)])?;
    let first = segments::stage_request(&journal, operation, "first", &first, None, None).await?;
    for (key, messages) in [
        ("rewrite", vec![message(9), message(2), message(3)]),
        ("compact", vec![message(4)]),
        ("same", vec![message(1), message(2)]),
    ] {
        let prepared = request(&messages)?;
        let reference =
            segments::stage_request(&journal, operation, key, &prepared, Some(&first), None)
                .await?;
        let reopened = load_model_request(&journal, &reference, Limits::default(), None).await?;
        assert_eq!(reopened.request(), prepared.request());
        assert_eq!(reopened.bytes(), prepared.bytes());
    }
    Ok(())
}

#[test]
fn recovered_requests_must_match_the_selected_header() -> Result<()> {
    let prepared = request(&[message(1)])?;
    let executor = uncompacted_executor(
        prepared.request().model.clone(),
        Arc::new(ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let execution = StockExecution::ordinary(&executor);
    execution.validate_response_request(&prepared)?;
    for field in ["model", "tools", "output"] {
        let mut changed = prepared.request().clone();
        match field {
            "model" => changed.model.revision = "2".into(),
            "tools" => changed.tools.push(crate::tool::ToolDefinition {
                name: "example.echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: json!({"type": "object"}),
                output_schema: json!({"type": "object"}),
                projection_schema: crate::tool::json_projection_schema(json!({"type": "object"})),
            }),
            _ => changed.max_output_tokens = Some(512),
        }
        // Every mutation is an independently valid request. Reusing admitted
        // bytes without checking the selected header accepts these controls.
        let changed =
            crate::model::PreparedModelRequest::prepare(changed, Limits::default(), None)?;
        assert!(
            execution.validate_response_request(&changed).is_err(),
            "{field}"
        );
    }
    Ok(())
}

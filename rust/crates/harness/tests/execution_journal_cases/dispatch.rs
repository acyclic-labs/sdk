use super::*;

#[tokio::test]
async fn two_hosts_cannot_both_claim_one_tool_dispatch() -> Result<()> {
    let agent = AgentId::from_bytes([55; 16]);
    let (host, private, issuer) =
        filesystem("tool-claim-e2e", "tool-claim-owner", agent, 55).await?;
    let scope = scope(&issuer, &private, agent, &[])?;
    let stream = Arc::new(MemoryStream::default());
    let journal_a = journal(stream.clone(), host.clone(), &private, &issuer, &scope)?;
    let journal_b = journal(stream, host, &private, &issuer, &scope)?;
    let operation = OperationId::from_bytes([56; 16]);
    journal_a
        .append(
            operation,
            &"tool:started",
            &ExecutionEvent::Started {
                request_digest: [57; 32],
            },
            None,
        )?
        .await?;
    let invocation = journal_a
        .stage(
            operation,
            "tool:invocation",
            b"{}",
            "application/json",
            None,
        )?
        .await?;
    let event = ExecutionEvent::ToolStarted {
        step: 0,
        call_id: operation.to_string(),
        invocation,
    };
    let (first, second) = tokio::join!(
        journal_a.append_if_tail(operation, 1, &"claim-a", &event, None)?,
        journal_b.append_if_tail(operation, 1, &"claim-b", &event, None)?,
    );
    assert_ne!(first?, second?);
    assert_eq!(journal_a.replay(operation, 0, 64, None)?.await?.len(), 2);
    let rejected = ExecutionEvent::ToolFailed {
        step: 0,
        call_id: operation.to_string(),
        reason: ToolFailureKind::ExecutorRejected,
        cause: acyclic_harness::executor::ToolFailureCause {
            class: acyclic_harness::executor::ToolErrorClass::Failed,
            message: "tool failed".into(),
        },
    };
    let invalid_output = ExecutionEvent::ToolFailed {
        step: 0,
        call_id: operation.to_string(),
        reason: ToolFailureKind::InvalidOutput,
        cause: acyclic_harness::executor::ToolFailureCause {
            class: acyclic_harness::executor::ToolErrorClass::Failed,
            message: "tool failed".into(),
        },
    };
    let (first_terminal, second_terminal) = tokio::join!(
        journal_a.append_if_tail(operation, 2, "terminal-a", &rejected, None)?,
        journal_b.append_if_tail(operation, 2, "terminal-b", &invalid_output, None)?,
    );
    assert_ne!(first_terminal?, second_terminal?);
    assert_eq!(journal_a.replay(operation, 0, 64, None)?.await?.len(), 3);
    let paged = OperationId::from_bytes([57; 16]);
    let invalid_request = journal_a
        .stage(paged, "invalid-request", b"null", "application/json", None)?
        .await?;
    assert!(
        journal_a
            .append(
                paged,
                &"invalid-model-start",
                &ExecutionEvent::ModelStarted {
                    step: 0,
                    purpose: acyclic_harness::executor::ModelPurpose::Response,
                    request_digest: *blake3::hash(b"null").as_bytes(),
                    request: invalid_request,
                },
                None
            )?
            .await
            .is_err()
    );
    assert!(journal_a.replay(paged, 0, 64, None)?.await?.is_empty());
    let prepared = PreparedModelRequest::prepare(
        ModelRequest {
            model: Model::new("fixture".into(), "paged".into(), "1".into(), Value::Null)?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("paged request".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: Some(1),
        },
        Limits::default(),
        None,
    )?;
    let request_digest = prepared.request_digest();
    let paged_request = journal_a
        .stage(
            paged,
            "paged-request",
            &prepared.bytes(),
            "application/json",
            None,
        )?
        .await?;
    assert!(
        journal_a
            .append(
                paged,
                &"model-without-execution",
                &ExecutionEvent::ModelStarted {
                    step: 0,
                    purpose: acyclic_harness::executor::ModelPurpose::Response,
                    request_digest,
                    request: paged_request.clone(),
                },
                None
            )?
            .await
            .is_err()
    );
    assert!(journal_a.replay(paged, 0, 64, None)?.await?.is_empty());
    journal_a
        .append(
            paged,
            &"paged-execution-start",
            &ExecutionEvent::Started {
                request_digest: [57; 32],
            },
            None,
        )?
        .await?;
    for step in 0..70 {
        journal_a
            .append(
                paged,
                &format!("paged-{step}"),
                &ExecutionEvent::ModelStarted {
                    step,
                    purpose: acyclic_harness::executor::ModelPurpose::Response,
                    request_digest,
                    request: paged_request.clone(),
                },
                None,
            )?
            .await?;
    }
    let first_page = journal_b.replay(paged, 0, 64, None)?.await?;
    let second_page = journal_b.replay(paged, 64, 64, None)?.await?;
    assert_eq!(first_page.len(), 64);
    assert_eq!(second_page.len(), 7);
    assert_eq!(first_page[63].sequence, 64);
    assert_eq!(second_page[0].sequence, 65);
    assert_eq!(journal_b.replay(paged, 0, 65, None)?.await?.len(), 64);
    assert!(journal_b.replay(paged, 0, 0, None)?.await.is_err());
    journal_b
        .append(
            paged,
            &"paged-0",
            &ExecutionEvent::ModelStarted {
                step: 0,
                purpose: acyclic_harness::executor::ModelPurpose::Response,
                request_digest,
                request: paged_request.clone(),
            },
            None,
        )?
        .await?;
    assert!(journal_b.replay(paged, 71, 64, None)?.await?.is_empty());
    let mut cursor = acyclic_harness::executor::ExecutionReplay::new(paged);
    while cursor.next_page(&journal_b, None).await?.is_some() {}
    assert_eq!(cursor.tail(), 71);
    assert!(
        journal_a
            .append_if_tail(
                paged,
                cursor.tail(),
                &"paged-cas-70",
                &ExecutionEvent::ModelStarted {
                    step: 70,
                    purpose: acyclic_harness::executor::ModelPurpose::Response,
                    request_digest,
                    request: paged_request.clone(),
                },
                None
            )?
            .await?
    );
    assert_eq!(journal_b.replay(paged, 71, 64, None)?.await?.len(), 1);
    Ok(())
}

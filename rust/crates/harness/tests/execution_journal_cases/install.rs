use super::*;

#[tokio::test]
async fn scoped_tool_install_reads_the_durable_approval_not_a_caller_claim() -> Result<()> {
    let agent = AgentId::from_bytes([46; 16]);
    let (host, private, issuer) = filesystem("tool-approval-e2e", "tool-owner", agent, 10).await?;
    let scope = scope(
        &issuer,
        &private,
        agent,
        &[
            "tool:install:example.echo".into(),
            "interaction:open".into(),
        ],
    )?;
    let stream = Arc::new(MemoryStream::default());
    bind_conversation(
        &stream,
        &issuer,
        "tool-owner",
        AgentId::from_bytes([46; 16]),
    )
    .await?;
    let journal = journal(stream, host, &private, &issuer, &scope)?;
    let definition = ToolDefinition {
        name: "example.echo".into(),
        revision: "1".into(),
        description: "Echo".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({}),
        projection_schema: acyclic_harness::tool::json_projection_schema(json!({})),
    };
    let digest = definition.digest()?;
    let operation = OperationId::from_bytes([14; 16]);
    let denied_id = InteractionId::from_bytes([15; 16]);
    let approved_id = InteractionId::from_bytes([16; 16]);
    let responder = issuer
        .issue(
            "human",
            &Capabilities::new([
                "interaction:resolve".to_owned(),
                format!("interaction:respond:{denied_id}"),
                format!("interaction:respond:{approved_id}"),
            ]),
            None,
            None,
            &mut None,
        )
        .expect("scope proof");
    let approval = Interaction::approval("Install echo".into(), operation, digest)?;
    journal
        .open_interaction(denied_id, &approval, None)?
        .await?;
    journal
        .open_interaction(approved_id, &approval, None)?
        .await?;
    let tool = Tool {
        hook_names: &[],
        definition,
        executor: Arc::new(NoopTool),
        projection: Arc::new(acyclic_harness::tool::JsonToolProjection),
    };
    let mut registry = ToolRegistry::default();
    assert!(
        registry
            .install_scoped(
                tool.clone(),
                operation,
                &scope,
                &issuer.verifier(&mut None).expect("verifier copy"),
                approved_id,
                &journal
            )
            .await
            .is_err()
    );
    journal
        .resolve_interaction(
            denied_id,
            InteractionResponse::Approval {
                approved: false,
                reason: None,
            },
            &responder,
        )
        .await?;
    assert!(
        registry
            .install_scoped(
                tool.clone(),
                operation,
                &scope,
                &issuer.verifier(&mut None).expect("verifier copy"),
                denied_id,
                &journal
            )
            .await
            .is_err()
    );
    journal
        .resolve_interaction(
            approved_id,
            InteractionResponse::Approval {
                approved: true,
                reason: None,
            },
            &responder,
        )
        .await?;
    assert!(
        registry
            .install_scoped(
                tool.clone(),
                OperationId::from_bytes([17; 16]),
                &scope,
                &issuer.verifier(&mut None).expect("verifier copy"),
                approved_id,
                &journal
            )
            .await
            .is_err()
    );
    registry
        .install_scoped(
            tool,
            operation,
            &scope,
            &issuer.verifier(&mut None).expect("verifier copy"),
            approved_id,
            &journal,
        )
        .await?;
    assert!(registry.get("example.echo", None, None)?.is_some());
    Ok(())
}

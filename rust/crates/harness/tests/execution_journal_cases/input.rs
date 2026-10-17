use super::*;

#[tokio::test]
async fn typed_file_input_requires_resident_authorized_bytes_before_journaling() -> Result<()> {
    let agent = AgentId::from_bytes([44; 16]);
    let (host, private, issuer) =
        filesystem("file-input-e2e", "file-input-owner", agent, 9).await?;
    let provider = private.provider().clone();
    let foreign = VolumeRef::new(
        provider.clone(),
        "foreign".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([45; 16])),
        &mut None,
    )?;
    let project = VolumeRef::new(
        provider,
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )?;
    host.create_volume(&foreign).await?;
    host.create_volume(&project).await?;
    let scope = scope(
        &issuer,
        &private,
        agent,
        &[
            foreign.capability(VolumeOperation::Read, &mut None)?,
            foreign.capability(VolumeOperation::Write, &mut None)?,
            project.capability(VolumeOperation::Read, &mut None)?,
            project.capability(VolumeOperation::Write, &mut None)?,
        ],
    )?;
    let grant = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &project,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let file = host
        .put_content(
            &project,
            &grant,
            "images/chart.png",
            &[1, 2, 3],
            "image/png",
            "chart.png",
            4_096,
            &IdempotencyKey::new("chart-upload")?,
            None,
            None,
            None,
        )
        .await?;
    let foreign_issuer = AuthorityIssuer::new(
        "foreign-owner",
        [45; 32],
        Authority {
            kind: AggregateKind::Agent,
            id: AgentId::from_bytes([45; 16]).to_string(),
        },
    );
    let foreign_scope = foreign_issuer
        .issue(
            "foreign-owner",
            &Capabilities::new([foreign.capability(VolumeOperation::Write, &mut None)?]),
            Some(AgentId::from_bytes([45; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    assert!(
        ContentGrant::verify(
            &issuer.verifier(&mut None).expect("verifier copy"),
            &scope,
            &foreign,
            VolumeOperation::Write,
            u64::MAX,
            &mut None
        )
        .is_err()
    );
    let foreign_grant = ContentGrant::verify(
        &foreign_issuer.verifier(&mut None).expect("verifier copy"),
        &foreign_scope,
        &foreign,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let foreign_file = host
        .put_content(
            &foreign,
            &foreign_grant,
            "secret.txt",
            b"secret",
            "text/plain",
            "secret.txt",
            4_096,
            &IdempotencyKey::new("foreign-upload")?,
            None,
            None,
            None,
        )
        .await?;
    let stream = Arc::new(MemoryStream::default());
    let journal = journal(stream, host, &private, &issuer, &scope)?;
    let model = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: Value::Null,
        }),
    ])]));
    let executor = StockExecutor::new(
        Model::new("test".into(), "capture".into(), "1".into(), Value::Null)?,
        model.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_compaction_policy(acyclic_harness::context::CompactionPolicy::Disabled);
    let content = ModelContent::Parts(vec![
        ModelContentPart::Text {
            text: "describe".into(),
        },
        ModelContentPart::File {
            file: file.clone(),
            policy: FileProjectionPolicy::Native(Box::new(
                acyclic_harness::model::NativeMediaPolicy {
                    intent: acyclic_harness::model::NativeMediaIntent::Image {
                        detail: acyclic_harness::model::ImageDetail::Auto,
                    },
                    maximum_bytes: acyclic_harness::conversation::MAX_EXACT_JS_INTEGER,
                    maximum_work: 4096,
                    configuration: None,
                },
            )),
        },
    ]);
    let invalid = FileRef::new(
        project,
        "images/chart.png".into(),
        file.version().into(),
        FileDescriptor::from_bytes(b"other", "image/png", &mut None)?,
        "chart.png".into(),
        &mut None,
    )?;
    let operation_id = OperationId::from_bytes([12; 16]);
    assert!(
        executor
            .execute(
                TurnInput {
                    operation_id,
                    input: ModelContent::Part(ModelContentPart::File {
                        file: invalid,
                        policy: FileProjectionPolicy::Native(Box::new(
                            acyclic_harness::model::NativeMediaPolicy {
                                intent: acyclic_harness::model::NativeMediaIntent::Image {
                                    detail: acyclic_harness::model::ImageDetail::Auto
                                },
                                maximum_bytes: acyclic_harness::conversation::MAX_EXACT_JS_INTEGER,
                                maximum_work: 4096,
                                configuration: None
                            }
                        ))
                    }),
                    selected_context: None,
                    max_steps: 1
                },
                &journal
            )
            .await
            .is_err()
    );
    assert!(journal.replay(operation_id, 0, 64, None)?.await?.is_empty());
    assert!(
        executor
            .execute(
                TurnInput {
                    operation_id,
                    input: ModelContent::Part(ModelContentPart::File {
                        file: foreign_file,
                        policy: FileProjectionPolicy::BoundedFull
                    }),
                    selected_context: None,
                    max_steps: 1
                },
                &journal
            )
            .await
            .is_err()
    );
    assert!(journal.replay(operation_id, 0, 64, None)?.await?.is_empty());
    executor
        .execute(
            TurnInput {
                operation_id,
                input: content.clone(),
                selected_context: None,
                max_steps: 1,
            },
            &journal,
        )
        .await?;
    let requests = model.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].messages.len(), 1);
    assert_eq!(requests[0].messages[0].content, content);
    Ok(())
}

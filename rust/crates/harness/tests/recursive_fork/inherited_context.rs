use super::*;

pub(super) async fn inherited_context(
    aggregate: &mut StreamAggregate<MemoryStream>,
    seed: &ForkSeed,
    child_agent: AgentId,
    root_agent: AgentId,
    grant_scope: &Scope,
    previous_file: &FileRef,
    child_issuer: &AuthorityIssuer,
    host: &Arc<FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>>,
) -> Result<()> {
    let reader = aggregate.history_reader()?;
    let page_limits = acyclic_harness::store::HistoryReadLimits {
        maximum_events: 2, // one publication plus one canonical message
        maximum_bytes: 64 * 1_024,
    };
    let first = reader
        .inherited_conversation_page(&seed, child_agent, 0, page_limits)
        .await?;
    let second = reader
        .inherited_conversation_page(&seed, child_agent, 1, page_limits)
        .await?;
    let parent_messages = aggregate
        .reducer()
        .conversation()
        .ok_or_else(|| {
            acyclic_harness::Error::Invalid("fixture parent conversation missing".into())
        })?
        .messages();
    assert_eq!(first.as_slice(), &parent_messages[..1]);
    assert_eq!(second.as_slice(), &parent_messages[1..2]);
    assert!(
        reader
            .inherited_conversation_page(&seed, child_agent, 2, page_limits)
            .await?
            .is_empty()
    );
    assert!(
        reader
            .inherited_conversation_page(&seed, child_agent, 3, page_limits)
            .await
            .is_err()
    );
    assert!(
        reader
            .inherited_conversation_page(&seed, root_agent, 0, page_limits)
            .await
            .is_err()
    );
    assert!(
        reader
            .inherited_conversation_page(
                &seed,
                child_agent,
                0,
                acyclic_harness::store::HistoryReadLimits {
                    maximum_events: 1,
                    ..page_limits
                }
            )
            .await
            .is_err()
    );
    let mut changed = seed.clone();
    changed.inherited_through_sequence = 1;
    assert!(
        reader
            .inherited_conversation_page(&changed, child_agent, 0, page_limits)
            .await
            .is_err()
    );
    aggregate
        .execute(
            command(
                4_090,
                aggregate.reducer().revision(),
                &grant_scope,
                Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id: Uuid::from_bytes([0xE1; 16]),
                        sequence: 3,
                        kind: MessageKind::Interaction,
                        content: previous_file.clone(),
                        attachments: acyclic_harness::conversation::ReferencedAttachments::Inline {
                            items: Vec::new(),
                        },
                        reply_to: None,
                        tool_call_id: None,
                        extensions: BTreeMap::new(),
                    }),
                },
            )?,
            &mut None,
        )
        .await?;
    assert_eq!(
        reader
            .inherited_conversation_page(&seed, child_agent, 0, page_limits)
            .await?,
        first
    );
    assert!(
        reader
            .inherited_conversation_page(&seed, child_agent, 2, page_limits)
            .await?
            .is_empty()
    );
    use acyclic_harness::context::{
        Context, ContextInput, ContextPipeline, ContextPlacement, ContextStage, SourceStage,
    };
    let mut loaded = acyclic_harness::conversation::ConversationState::default();
    loaded.bind(root_agent)?;
    for message in first.into_iter().chain(second) {
        loaded.append(message, &mut None)?;
    }
    let selection = ModelContextSelection {
        conversation_revision: seed.inherited_through_sequence,
        message_ids: loaded.messages().iter().map(|message| message.id).collect(),
        checkpoint: None,
    };
    let source_scope = child_issuer
        .issue(
            "inherited-source",
            &seed.reference_capabilities(child_agent)?,
            Some(child_agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let source_reader = FilesystemContentVerifier::new(
        host.clone(),
        &child_issuer.verifier(&mut None).expect("verifier copy"),
        &source_scope,
        1_024,
        None,
    )?;
    let projected = select_model_context(
        &loaded,
        (&loaded).logical_revision(),
        selection.clone(),
        source_reader.as_ref(),
        acyclic_harness::conversation::Limits {
            context_messages: 2,
            attachments: 4,
            render_bytes: 64 * 1_024,
            ..Default::default()
        },
        4,
        None,
    )
    .await?;
    let denied_reader = FilesystemContentVerifier::new(
        host.clone(),
        &child_issuer.verifier(&mut None).expect("verifier copy"),
        &child_issuer
            .issue(
                "denied-source",
                &Capabilities::default(),
                Some(child_agent),
                None,
                &mut None,
            )
            .expect("scope proof"),
        1_024,
        None,
    )?;
    assert!(
        select_model_context(
            &loaded,
            (&loaded).logical_revision(),
            selection,
            denied_reader.as_ref(),
            acyclic_harness::conversation::Limits {
                context_messages: 2,
                attachments: 4,
                render_bytes: 64 * 1_024,
                ..Default::default()
            },
            4,
            None
        )
        .await
        .is_err()
    );
    let source = Context {
        messages: projected.messages.clone(),
        metadata: BTreeMap::new(),
        current_input_index: None,
    };
    let pipeline = ContextPipeline::new([Arc::new(SourceStage::new(
        "inherited-history",
        seed.operation_id.to_string(),
        Arc::new(source),
        ContextPlacement::Prepend,
    )) as Arc<dyn ContextStage>]);
    let input = ContextInput {
        input: ModelContent::Text("child current input".into()),
        selected_context: None,
        step: 0,
        prior_messages: Vec::new(),
    };
    let assembled = pipeline.run(&input, Limits::default(), None).await?;
    assert_eq!(&assembled.messages[..2], projected.messages.as_slice());
    assert_eq!(assembled.current_input_index, Some(2));
    assert_eq!(assembled.messages[2].content, input.input);
    assert_eq!(
        aggregate
            .reducer()
            .conversation()
            .map(|state| state.messages().len()),
        Some(3)
    );

    Ok(())
}

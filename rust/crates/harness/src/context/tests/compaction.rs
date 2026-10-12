use super::*;

#[cfg(feature = "filesystem")]
#[test]
fn owned_projected_validation_observes_source_without_inventory_or_copy() -> Result<()> {
    let source = Context {
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("current input".into()),
        }],
        current_input_index: Some(0),
        ..Context::default()
    };
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut readonly = owned_context_budget();
    readonly.bytes_copied = 0;
    readonly.allocation_operations = 0;
    let io = crate::executor::PreparationIo::from_budget(readonly, &cancellation)?;
    validate_projected_context(&source, Limits::default(), Some(&io))?;
    assert_eq!(io.owned_bytes()?, 0);
    let mut budget = owned_context_budget();
    budget.items_examined = 0;
    let denied = crate::executor::PreparationIo::from_budget(budget, &cancellation)?;
    assert!(validate_projected_context(&source, Limits::default(), Some(&denied)).is_err());
    assert_eq!(denied.owned_bytes()?, 0);
    let cancelled =
        crate::executor::PreparationIo::from_budget(owned_context_budget(), &cancellation)?;
    cancellation.cancel();
    assert!(validate_projected_context(&source, Limits::default(), Some(&cancelled)).is_err());
    assert_eq!(cancelled.owned_bytes()?, 0);
    Ok(())
}
#[cfg(feature = "filesystem")]
#[test]
fn owned_compaction_proof_reconstructs_exactly_and_releases_actual_fields() -> Result<()> {
    let source = Context {
        messages: (0..4)
            .map(|index| ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(format!("message {index}")),
            })
            .collect(),
        current_input_index: Some(3),
        ..Context::default()
    };
    let prefix = compaction::clone_context(&source, Some(2), &mut None)?.0;
    let output = message(ModelRole::Assistant, "summary")?
        .content
        .file_refs()
        .next()
        .unwrap()
        .clone();
    let summary = ContextSummary {
        operation_id: crate::OperationId::new(),
        step: 0,
        source_messages: 2,
        source_digest: crate::contract::canonical_json_digest(&prefix, u64::MAX, &mut None)?,
        output,
    };
    let (retained, reference) = DurableContextProvider::compact(
        &source,
        3,
        Some(summary),
        CompactionRetention::default(),
        None,
    )?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let io = crate::executor::PreparationIo::from_budget(owned_context_budget(), &cancellation)?;
    validate_compaction(&reference, &source, &retained, Some(&io))?;
    assert_eq!(io.owned_bytes()?, 0);
    let mut changed = retained.clone();
    changed
        .messages
        .last_mut()
        .expect("retained current input")
        .content = ModelContent::Text("changed".into());
    assert!(validate_compaction(&reference, &source, &changed, Some(&io)).is_err());
    assert_eq!(io.owned_bytes()?, 0);
    let mut changed = reference.clone();
    changed
        .summary
        .as_mut()
        .expect("summary proof")
        .source_digest[0] ^= 1;
    assert!(validate_compaction(&changed, &source, &retained, Some(&io)).is_err());
    assert_eq!(io.owned_bytes()?, 0);
    let mut budget = owned_context_budget();
    budget.items_examined = 0;
    let denied = crate::executor::PreparationIo::from_budget(budget, &cancellation)?;
    assert!(validate_compaction(&reference, &source, &retained, Some(&denied)).is_err());
    assert_eq!(denied.owned_bytes()?, 0);
    let cancelled =
        crate::executor::PreparationIo::from_budget(owned_context_budget(), &cancellation)?;
    cancellation.cancel();
    assert!(validate_compaction(&reference, &source, &retained, Some(&cancelled)).is_err());
    assert_eq!(cancelled.owned_bytes()?, 0);
    Ok(())
}
#[test]
fn native_call_retention_closes_tool_pair_before_compaction_budget() -> Result<()> {
    let file: FileRef = message(ModelRole::Assistant, "media reference")?
        .content
        .file_refs()
        .into_iter()
        .next()
        .ok_or_else(|| crate::Error::Invalid("media reference missing".into()))?
        .clone();
    for (results, native_in_result) in
        (1..=3).flat_map(|results| [false, true].map(move |nested| (results, nested)))
    {
        let source = native_call_context(&file, results, native_in_result)?;
        let summary = ContextSummary {
            operation_id: crate::OperationId::new(),
            step: 0,
            source_messages: u32::try_from(source.messages.len())
                .map_err(|_| crate::Error::Invalid("fixture count exceeds u32".into()))?,
            source_digest: crate::contract::canonical_json_digest(&source, u64::MAX, &mut None)?,
            output: file.clone(),
        };
        let (compacted, reference) = DurableContextProvider::compact(
            &source,
            results + 3,
            Some(summary.clone()),
            CompactionRetention::default(),
            None,
        )?;
        let mut expected = vec![source.messages[1].clone()];
        expected.extend_from_slice(&source.messages[3..]);
        assert_eq!(compacted.messages.get(1..), Some(expected.as_slice()));
        assert_eq!(
            compacted.current_input_index,
            Some(
                u32::try_from(results + 2)
                    .map_err(|_| crate::Error::Invalid("fixture index exceeds u32".into()))?
            )
        );
        validate_compaction(&reference, &source, &compacted, None)?;
        assert!(
            DurableContextProvider::compact(
                &source,
                results + 2,
                Some(summary.clone()),
                CompactionRetention::default(),
                None
            )
            .is_err()
        );
        let retention = CompactionRetention {
            native_media: false,
            ..CompactionRetention::default()
        };
        let (without_media, _) =
            DurableContextProvider::compact(&source, 2, Some(summary), retention, None)?;
        assert_eq!(
            without_media.messages.get(1..),
            source.messages.get(source.messages.len() - 1..)
        );
    }
    Ok(())
}
#[test]
fn compaction_keeps_mandatory_messages_and_never_splits_tool_pairs() -> Result<()> {
    let source = Context {
        messages: vec![
            ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("instructions".into()),
            },
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("old".into()),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("old answer".into()),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall {
                    call_id: "call".into(),
                    name: "example.tool".into(),
                    arguments: Value::Null,
                }),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "call".into(),
                    name: "example.tool".into(),
                    content: crate::model::ToolResultContent::Json { value: Value::Null },
                }),
            },
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("current".into()),
            },
        ],
        metadata: BTreeMap::new(),
        current_input_index: None,
    };
    let summary = ContextSummary {
        operation_id: crate::OperationId::new(),
        step: 0,
        source_messages: 6,
        source_digest: crate::contract::canonical_json_digest(&source, u64::MAX, &mut None)?,
        output: message(ModelRole::System, "summary")?
            .content
            .file_refs()
            .into_iter()
            .next()
            .ok_or_else(|| crate::Error::Invalid("summary file is missing".into()))?
            .clone(),
    };
    let prefix = Context {
        messages: source.messages[..3].to_vec(),
        metadata: source.metadata.clone(),
        current_input_index: None,
    };
    let prefix_summary = ContextSummary {
        source_messages: 3,
        source_digest: crate::contract::canonical_json_digest(&prefix, u64::MAX, &mut None)?,
        ..summary.clone()
    };
    let (prefix_compacted, prefix_reference) = DurableContextProvider::compact(
        &source,
        5,
        Some(prefix_summary.clone()),
        CompactionRetention::default(),
        None,
    )?;
    assert_eq!(prefix_compacted.messages.get(2..), source.messages.get(3..));
    validate_compaction(&prefix_reference, &source, &prefix_compacted, None)?;
    assert!(
        DurableContextProvider::compact(
            &source,
            4,
            Some(prefix_summary.clone()),
            CompactionRetention::default(),
            None
        )
        .is_err(),
        "unsummarized messages cannot be dropped to meet the budget"
    );
    for extent in [0, 2, 7] {
        assert!(
            DurableContextProvider::compact(
                &source,
                5,
                Some(ContextSummary {
                    source_messages: extent,
                    ..prefix_summary.clone()
                }),
                CompactionRetention::default(),
                None
            )
            .is_err()
        );
    }
    for budget in 1..=6 {
        let result = DurableContextProvider::compact(
            &source,
            budget,
            Some(summary.clone()),
            CompactionRetention::default(),
            None,
        );
        if matches!(budget, 1 | 2 | 4) {
            assert!(result.is_err());
            continue;
        }
        let (projected, reference) = result?;
        assert!(projected.messages.contains(&source.messages[0]));
        assert_eq!(projected.messages.last(), source.messages.last());
        assert_eq!(
            projected.messages.contains(&source.messages[3]),
            projected.messages.contains(&source.messages[4])
        );
        validate_compaction(&reference, &source, &projected, None)?;
        let mut corrupt = projected;
        corrupt
            .messages
            .last_mut()
            .ok_or_else(|| crate::Error::Invalid("empty context".into()))?
            .content = ModelContent::Text("substitution".into());
        assert!(validate_compaction(&reference, &source, &corrupt, None).is_err());
    }
    assert!(
        DurableContextProvider::compact(&source, 3, None, CompactionRetention::default(), None)
            .is_err()
    );
    let configurable = Context {
        messages: vec![
            source.messages[0].clone(),
            source.messages[1].clone(),
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::File {
                    file: summary.output.clone(),
                    policy: native_retention_policy(),
                }),
            },
            source.messages[5].clone(),
        ],
        metadata: BTreeMap::new(),
        current_input_index: None,
    };
    let configured_summary = ContextSummary {
        source_digest: crate::contract::canonical_json_digest(&configurable, u64::MAX, &mut None)?,
        source_messages: 4,
        ..summary
    };
    assert!(
        DurableContextProvider::compact(
            &configurable,
            3,
            Some(configured_summary.clone()),
            CompactionRetention::default(),
            None
        )
        .is_err()
    );
    let retention = CompactionRetention {
        roles: vec![ModelRole::User],
        native_media: false,
    };
    let (projected, mut reference) = DurableContextProvider::compact(
        &configurable,
        3,
        Some(configured_summary),
        retention.clone(),
        None,
    )?;
    assert_eq!(
        projected.messages.get(1..),
        Some(
            &[
                configurable.messages[1].clone(),
                configurable.messages[3].clone(),
            ][..]
        )
    );
    assert_eq!(reference.retention, retention);
    validate_compaction(&reference, &configurable, &projected, None)?;
    reference.retention = CompactionRetention::default();
    assert!(validate_compaction(&reference, &configurable, &projected, None).is_err());
    assert!(
        CompactionRetention {
            roles: vec![ModelRole::User, ModelRole::User],
            native_media: true
        }
        .validate(None)
        .is_err()
    );
    Ok(())
}
#[test]
fn repeated_compaction_replaces_summaries_and_retains_instructions() -> Result<()> {
    let source = Context {
        messages: vec![
            message(ModelRole::System, "instructions")?,
            message(ModelRole::User, "old")?,
            message(ModelRole::Assistant, "old-answer")?,
            message(ModelRole::User, "current")?,
        ],
        metadata: BTreeMap::new(),
        current_input_index: Some(3),
    };
    let make_summary = |source: &Context, label: &str| -> Result<ContextSummary> {
        let output = message(ModelRole::Assistant, label)?;
        Ok(ContextSummary {
            operation_id: crate::OperationId::new(),
            step: 0,
            source_messages: u32::try_from(source.messages.len())
                .map_err(|error| crate::Error::Invalid(error.to_string()))?,
            source_digest: crate::contract::canonical_json_digest(source, u64::MAX, &mut None)?,
            output: output.content.file_refs().next().unwrap().clone(),
        })
    };
    let first_summary = make_summary(&source, "first-summary")?;
    let (mut next, reference) = DurableContextProvider::compact(
        &source,
        3,
        Some(first_summary.clone()),
        CompactionRetention::default(),
        None,
    )?;
    validate_compaction(&reference, &source, &next, None)?;
    assert_eq!(next.messages[0].role, ModelRole::Assistant);
    next.messages.extend([
        message(ModelRole::Assistant, "later-answer")?,
        message(ModelRole::User, "latest")?,
    ]);
    next.current_input_index = Some(4);
    let (compacted, reference) = DurableContextProvider::compact(
        &next,
        3,
        Some(make_summary(&next, "second-summary")?),
        CompactionRetention::default(),
        None,
    )?;
    validate_compaction(&reference, &next, &compacted, None)?;
    assert_eq!(compacted.messages.len(), 3);
    assert!(compacted.messages.contains(&source.messages[0]));
    assert_eq!(compacted.messages.last(), next.messages.last());
    assert_eq!(compacted.current_input_index, Some(2));
    assert!(compacted.messages.iter().all(|message| {
        !message
            .content
            .file_refs()
            .any(|file| file == &first_summary.output)
    }));
    Ok(())
}
#[test]
fn compaction_retains_current_user_before_trailing_tool_results() -> Result<()> {
    let source = Context {
        messages: vec![
            message(ModelRole::System, "instructions")?,
            message(ModelRole::User, "old")?,
            message(ModelRole::Assistant, "old-answer")?,
            message(ModelRole::User, "current")?,
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall {
                    call_id: "current-call".into(),
                    name: "tool".into(),
                    arguments: Value::Null,
                }),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "current-call".into(),
                    name: "tool".into(),
                    content: crate::model::ToolResultContent::Json { value: Value::Null },
                }),
            },
        ],
        metadata: BTreeMap::new(),
        current_input_index: Some(3),
    };
    let summary = ContextSummary {
        operation_id: crate::OperationId::new(),
        step: 0,
        source_messages: 6,
        source_digest: crate::contract::canonical_json_digest(&source, u64::MAX, &mut None)?,
        output: message(ModelRole::Assistant, "summary")?
            .content
            .file_refs()
            .next()
            .unwrap()
            .clone(),
    };
    assert!(
        DurableContextProvider::compact(
            &source,
            4,
            Some(summary.clone()),
            CompactionRetention::default(),
            None
        )
        .is_err()
    );
    let (compacted, reference) = DurableContextProvider::compact(
        &source,
        5,
        Some(summary),
        CompactionRetention::default(),
        None,
    )?;
    validate_compaction(&reference, &source, &compacted, None)?;
    assert!(compacted.messages.contains(&source.messages[3]));
    assert_eq!(compacted.messages.last(), source.messages.last());
    assert!(compacted.messages.contains(&source.messages[4]));
    assert!(compacted.messages.contains(&source.messages[5]));
    assert_eq!(compacted.current_input_index, Some(2));
    Ok(())
}

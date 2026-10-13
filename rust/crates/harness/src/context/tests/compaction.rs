use super::*;

#[test]
fn compaction_preserves_instructions_current_input_and_complete_exchanges() -> Result<()> {
    let call = ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::ToolCall {
            call_id: "call".into(),
            name: "tool".into(),
            arguments: Value::Null,
        }),
    };
    let result = ModelMessage {
        role: ModelRole::Tool,
        content: ModelContent::Part(ModelContentPart::ToolResult {
            call_id: "call".into(),
            name: "tool".into(),
            content: crate::model::ToolResultContent::Json { value: Value::Null },
        }),
    };
    let source = context(
        vec![
            text(ModelRole::System, "instructions"),
            text(ModelRole::User, "old"),
            text(ModelRole::Assistant, "answer"),
            call,
            result,
            text(ModelRole::User, "current"),
        ],
        Some(5),
    );
    for budget in 1..=6 {
        let projection = compact(&source, budget, 6);
        if matches!(budget, 1 | 2 | 4) {
            assert!(projection.is_err());
            continue;
        }
        let (retained, reference) = projection?;
        assert!(retained.messages.contains(&source.messages[0]));
        assert_eq!(retained.messages.last(), source.messages.last());
        assert_eq!(
            retained.messages.contains(&source.messages[3]),
            retained.messages.contains(&source.messages[4])
        );
        assert_eq!(
            retained.messages[retained.current_input_index.unwrap() as usize],
            source.messages[5]
        );
        validate_compaction(&reference, &source, &retained, None)?;
        let mut corrupt = retained;
        corrupt.messages.last_mut().unwrap().content = ModelContent::Text("substitution".into());
        assert!(validate_compaction(&reference, &source, &corrupt, None).is_err());
    }
    let (retained, reference) = compact(&source, 5, 3)?;
    assert_eq!(retained.messages.get(2..), source.messages.get(3..));
    validate_compaction(&reference, &source, &retained, None)?;
    assert!(compact(&source, 4, 3).is_err());
    for through in [0, 2, 7] {
        assert!(compact(&source, 5, through).is_err());
    }
    assert!(
        DurableContextProvider::compact(&source, 3, None, CompactionRetention::default(), None)
            .is_err()
    );
    let plain = context(
        vec![
            source.messages[0].clone(),
            source.messages[1].clone(),
            source.messages[2].clone(),
            source.messages[5].clone(),
        ],
        Some(3),
    );
    let (first, _) = compact(&plain, 3, 4)?;
    let (again, _) = compact(&first, 3, first.messages.len())?;
    assert_eq!(again.messages[0], source.messages[0]);
    assert_eq!(again.messages.last(), source.messages.last());
    let mut trailing = source.clone();
    trailing.messages.swap(4, 5);
    trailing.current_input_index = Some(4);
    let (retained, reference) = compact(&trailing, 5, 6)?;
    assert_eq!(
        retained.messages[retained.current_input_index.unwrap() as usize],
        source.messages[5]
    );
    assert_eq!(retained.messages.last(), source.messages.get(4));
    validate_compaction(&reference, &trailing, &retained, None)?;
    Ok(())
}

#[test]
fn declared_retention_preserves_native_tool_exchanges() -> Result<()> {
    let file = message(ModelRole::System, "native")?
        .content
        .file_refs()
        .next()
        .unwrap()
        .clone();
    for in_result in [false, true] {
        for results in 1..=3 {
            let source = native_call_context(&file, results, in_result)?;
            let (retained, proof) = compact(&source, results + 3, source.messages.len())?;
            let mut expected = vec![source.messages[1].clone()];
            expected.extend_from_slice(&source.messages[3..]);
            assert_eq!(retained.messages.get(1..), Some(expected.as_slice()));
            validate_compaction(&proof, &source, &retained, None)?;
            assert!(compact(&source, results + 2, source.messages.len()).is_err());
            let retention = CompactionRetention {
                roles: vec![ModelRole::User],
                native_media: false,
            };
            let (retained, mut reference) = DurableContextProvider::compact(
                &source,
                3,
                Some(summary(&source, source.messages.len())?),
                retention.clone(),
                None,
            )?;
            assert_eq!(
                retained.messages.get(1..),
                Some(
                    &[
                        source.messages[0].clone(),
                        source.messages.last().unwrap().clone()
                    ][..]
                )
            );
            validate_compaction(&reference, &source, &retained, None)?;
            reference.retention = CompactionRetention::default();
            assert!(validate_compaction(&reference, &source, &retained, None).is_err());
        }
    }
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
    let (retained, reference) = compact(&source, 3, 2)?;
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

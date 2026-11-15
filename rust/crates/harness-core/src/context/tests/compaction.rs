use super::*;

#[test]
fn compaction_budget_advances_past_an_irreducible_prefix() -> Result<()> {
    let source = context(
        vec![
            text(ModelRole::System, "instructions"),
            text(ModelRole::User, "older input"),
            text(ModelRole::Assistant, "older answer"),
            text(ModelRole::User, "current input"),
        ],
        Some(3),
    );
    let policy = CapacityCompaction::default();
    assert_eq!(policy.window_boundary(&source, 1, &mut None)?, Some(1));
    for limit in [None, Some(source.messages.len())] {
        let (through, maximum) = policy.projection_budget(&source, limit, 1, &mut None)?;
        assert_eq!(through, 2);
        let (retained, proof) = compact(&source, maximum, through)?;
        validate_compaction(&proof, &source, &retained, None)?;
        assert!(retained.messages.contains(&source.messages[0]));
        assert!(retained.messages.contains(&source.messages[3]));
        assert!(!retained.messages.contains(&source.messages[1]));
    }
    Ok(())
}

#[test]
fn every_cut_of_the_only_protected_exchange_retains_all_input() -> Result<()> {
    let payload = "old ".repeat(6_500);
    let source = context(
        vec![
            text(ModelRole::User, "question"),
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall(
                    crate::model::ToolCall::new(
                        "call".into(),
                        "tool".into(),
                        serde_json::json!({"value": payload}),
                    ),
                )),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "call".into(),
                    name: "tool".into(),
                    content: crate::model::ToolResultContent::Json {
                        value: serde_json::json!({"value": payload}),
                    },
                }),
            },
        ],
        Some(0),
    );
    let policy = CapacityCompaction::default();
    let capacity = ModelContextCapacity {
        context_tokens: 32_768,
        output_tokens: Some(4_096),
        output_shares_context: false,
    };
    let input_bytes = source.messages.iter().try_fold(100_u64, |total, message| {
        Ok::<_, crate::Error>(total + crate::contract::canonical_json_bytes(message)?.len() as u64)
    })?;
    assert!(policy.needs_compaction(capacity, input_bytes, 0)?);
    // Exhaust all prefix cuts through the production selector: the current
    // input and latest result pin the entire connected call/result exchange.
    for covered in 0..=source.messages.len() {
        let retained = mandatory_messages(&source, covered, &mut None)?;
        assert_eq!(
            retained, source.messages,
            "cut {covered} dropped protected input"
        );
    }
    for maximum in 1..=source.messages.len() {
        assert!(compact(&source, maximum, source.messages.len()).is_err());
    }
    Ok(())
}

#[test]
fn compaction_preserves_instructions_current_input_and_complete_exchanges() -> Result<()> {
    let call = ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::ToolCall(crate::model::ToolCall::new(
            "call".into(),
            "tool".into(),
            Value::Null,
        ))),
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
    let updates = [
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Part(ModelContentPart::Configuration {
                prefix: [7; 32],
                effort: acyclic_model_render::Effort::High,
            }),
        },
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Parts(vec![ModelContentPart::Configuration {
                prefix: [7; 32],
                effort: acyclic_model_render::Effort::Low,
            }]),
        },
    ];
    let mut configured = source.clone();
    configured.messages.insert(1, updates[0].clone());
    configured.messages.insert(4, updates[1].clone());
    configured.current_input_index = Some(7);
    let expected = vec![
        configured.messages[0].clone(),
        updates[0].clone(),
        updates[1].clone(),
        configured.messages[7].clone(),
    ];
    assert_eq!(mandatory_messages(&configured, 8, &mut None)?, expected);
    let (mut retained, proof) = compact(&configured, 5, 8)?;
    // The leading instructions stay the header and the summary follows them,
    // so a compacted request keeps the cached system prefix.
    assert_eq!(retained.messages.first(), expected.first());
    assert_eq!(retained.messages.get(2..), expected.get(1..));
    validate_compaction(&proof, &configured, &retained, None)?;
    // An unchanged final effort does not justify deleting an earlier admitted
    // transition: the original ordered context remains the replay contract.
    retained.messages.remove(2);
    assert!(validate_compaction(&proof, &configured, &retained, None).is_err());
    for budget in 1..=6 {
        let projection = compact(&source, budget, 6);
        if matches!(budget, 1 | 2) {
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
        if budget == 4 {
            assert!(
                !retained.messages.contains(&source.messages[3]),
                "spare slots cannot retain half an exchange"
            );
        }
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
    assert!(compact_projection(&source, 3, None, None).is_err());
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
fn compaction_always_preserves_native_tool_exchanges() -> Result<()> {
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
            let mut overridden = serde_json::to_value(&proof).unwrap();
            overridden["retention"] = serde_json::json!({"roles": [], "native_media": false});
            assert!(serde_json::from_value::<CompactionReference>(overridden).is_err());
            let mut corrupt = retained.clone();
            corrupt.messages.remove(1);
            assert!(validate_compaction(&proof, &source, &corrupt, None).is_err());
        }
    }
    Ok(())
}

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
    // Independent serialized-source oracle: admitting a full-prefix summary
    // needs one source hash. A second hash of the identical prefix exhausts
    // this original budget, while a changed digest must still be refused.
    let mut hash_budget = owned_context_budget();
    hash_budget.bytes_hashed = crate::contract::canonical_json_bytes(&source)?.len() as u64;
    let hash_io = crate::executor::PreparationIo::from_budget(hash_budget, &cancellation)?;
    let full_summary = summary(&source, source.messages.len())?;
    let (full_retained, full_reference) =
        compact_projection(&source, 3, Some(full_summary.clone()), Some(&hash_io))?;
    assert_eq!(full_reference.source_digest, full_summary.source_digest);
    assert_eq!(full_retained, compact(&source, 3, source.messages.len())?.0);
    assert_eq!(hash_io.remaining()?.bytes_hashed, 0);
    assert!(crate::executor::preparation_digest(&source, Some(&hash_io)).is_err());
    let hash_hold = hash_io.guard_owned(hash_io.owned_bytes()?)?;
    drop(full_retained);
    drop(full_reference);
    drop(hash_hold);
    assert_eq!(hash_io.owned_bytes()?, 0);
    let hash_io = crate::executor::PreparationIo::from_budget(hash_budget, &cancellation)?;
    let mut changed_summary = full_summary;
    changed_summary.source_digest[0] ^= 1;
    assert!(compact_projection(&source, 3, Some(changed_summary), Some(&hash_io)).is_err());
    assert_eq!(hash_io.owned_bytes()?, 0);
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
fn production_compaction_preserves_typed_project_instructions_with_role_control() -> Result<()> {
    use crate::model::{FileProjectionPolicy, InstructionIdentity};
    let file = message(ModelRole::System, "rules")?
        .content
        .file_refs()
        .next()
        .unwrap()
        .clone();
    let mut instructions = Vec::new();
    for name in ["AGENTS.md", "CLAUDE.md"] {
        instructions.push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Part(ModelContentPart::File {
                policy: FileProjectionPolicy::ProjectInstructions(Box::new(
                    InstructionIdentity::new(file.volume().clone(), format!("nested/{name}"))?,
                )),
                file: file.clone(),
            }),
        });
    }
    let mut messages = instructions.clone();
    messages.extend([
        text(ModelRole::User, "old"),
        text(ModelRole::Assistant, "answer"),
        text(ModelRole::User, "current"),
    ]);
    let source = context(messages, Some(4));
    let (retained, proof) = compact(&source, 4, source.messages.len())?;
    for instruction in &instructions {
        assert!(retained.messages.contains(instruction));
    }
    validate_compaction(&proof, &source, &retained, None)?;
    // Same role, bytes and positions; remove only the typed retention marker.
    let mut unmarked = source.clone();
    for message in &mut unmarked.messages[..2] {
        let ModelContent::Part(ModelContentPart::File { policy, .. }) = &mut message.content else {
            unreachable!()
        };
        *policy = FileProjectionPolicy::BoundedFull;
    }
    let (dropped, _) = compact(&unmarked, 2, unmarked.messages.len())?;
    assert!(dropped.messages.iter().all(|message| {
        message
            .content
            .file_refs()
            .all(|file| file != instructions[0].content.file_refs().next().unwrap())
    }));
    Ok(())
}

#[test]
fn compaction_keeps_fragmented_schemas_only_for_retained_exchanges() -> Result<()> {
    let definition = |revision: &str, kind: &str| crate::tool::ToolDefinition {
        name: "mcp.compaction.live".into(),
        revision: revision.into(),
        description: "A live schema".into(),
        input_schema: serde_json::json!({"type":"object","properties":{"value":{"type":kind}},"required":["value"],"additionalProperties":false}),
        output_schema: serde_json::json!({"type":kind}),
        projection_schema: serde_json::json!({"type":"object","properties":{"kind":{"const":"json"},"value":{"type":kind}},"required":["kind","value"],"additionalProperties":false}),
    };
    let fragments = |table: &[crate::tool::ToolDefinition]| -> Result<Vec<ModelMessage>> {
        let json = serde_json::to_string(table)
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let cut = json.len() / 2;
        Ok([(0, &json[..cut], false), (cut, &json[cut..], true)].into_iter().map(|(offset, fragment, complete)| ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Part(ModelContentPart::Notices {
                notices: vec![crate::conversation::wire::Notice {
                    source: "tools".into(), kind: "tools.changed".into(),
                    body: serde_json::json!({"offset":offset,"json":fragment,"complete":complete}),
                    wake: false, coalesce_key: None,
                }],
            }),
        }).collect())
    };
    let exchange = |id: &str, value: Value| {
        [
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall(
                    crate::model::ToolCall::new(
                        id.into(),
                        "mcp.compaction.live".into(),
                        serde_json::json!({"value":value}),
                    ),
                )),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: id.into(),
                    name: "mcp.compaction.live".into(),
                    content: crate::model::ToolResultContent::Json { value },
                }),
            },
        ]
    };
    let first = fragments(&[definition("1", "string")])?;
    let second = fragments(&[definition("2", "integer")])?;
    let mut messages = first.clone();
    messages.extend(exchange("a", serde_json::json!("old")));
    messages.push(text(ModelRole::User, "older padding"));
    messages.extend(exchange("b", serde_json::json!("still old")));
    messages.extend(second.clone());
    messages.extend(exchange("c", serde_json::json!(1)));
    messages.push(text(ModelRole::User, "recent padding"));
    messages.extend(exchange("d", serde_json::json!(2)));
    messages.extend(fragments(&[])?);
    messages.push(text(ModelRole::User, "current"));
    let current = messages.len() as u32 - 1;
    let source = context(messages, Some(current));
    let prepare = |messages, tools| {
        crate::model::PreparedModelRequest::prepare(
            crate::model::ModelRequest {
                summary: false,
                compose_tools: false,
                model: crate::model::Model::new("test".into(), "model".into(), "1".into())?,
                messages,
                tools,
                sampling: Default::default(),
            },
            Limits::default(),
            None,
        )
    };
    let has_call =
        |context: &Context, id: &str| {
            context.messages.iter().any(|message| {
                message.content.parts().iter().any(
                    |part| matches!(part, ModelContentPart::ToolCall(call) if call.call_id == id),
                )
            })
        };
    for maximum in 2..=source.messages.len() {
        let (retained, proof) = compact(&source, maximum, source.messages.len())?;
        validate_compaction(&proof, &source, &retained, None)?;
        for current in [Vec::new(), vec![definition("3", "boolean")]] {
            prepare(retained.messages.clone(), current)?;
        }
        if has_call(&retained, "d") {
            assert!(
                second
                    .iter()
                    .all(|fragment| retained.messages.contains(fragment))
            );
            let mut broken = retained.messages.clone();
            let first_fragment = broken
                .iter()
                .position(|message| message == &second[0])
                .expect("first schema fragment");
            broken.remove(first_fragment);
            assert!(prepare(broken, Vec::new()).is_err());
        }
        if has_call(&retained, "a") || has_call(&retained, "b") {
            assert!(
                first
                    .iter()
                    .all(|fragment| retained.messages.contains(fragment))
            );
        }
    }
    let policy = CapacityCompaction::default();
    let schema_position = source
        .messages
        .iter()
        .position(|message| message == &second[0])
        .expect("schema position");
    let mut planned_retained_schema = false;
    let mut unbounded_retained_schema = false;
    for requested_cut in 1..source.messages.len() {
        let mut finite_plan = false;
        let mut unbounded_plan = false;
        for limit in std::iter::once(None).chain((2..=source.messages.len()).map(Some)) {
            if let Ok((through, maximum)) =
                policy.projection_budget(&source, limit, requested_cut, &mut None)
            {
                finite_plan |= limit.is_some();
                unbounded_plan |= limit.is_none();
                let (retained, proof) = compact(&source, maximum, through)?;
                validate_compaction(&proof, &source, &retained, None)?;
                let retained_schema = through > schema_position && has_call(&retained, "d");
                planned_retained_schema |= retained_schema;
                unbounded_retained_schema |= limit.is_none() && retained_schema;
                prepare(retained.messages, Vec::new())?;
            }
        }
        assert!(
            !finite_plan || unbounded_plan,
            "removing message credit must not reject an otherwise valid projection"
        );
    }
    assert!(
        planned_retained_schema,
        "automatic planning must retain the original table across its summary cut"
    );
    assert!(
        unbounded_retained_schema,
        "unbounded planning must retain the original table across its summary cut"
    );
    // Independent, interleaved native and dynamic snapshots. The native table
    // becomes active last; it must neither replace the dynamic schema nor
    // allow compaction to retain only one environment or one fragment.
    let mut native_definition = definition("native", "integer");
    native_definition.name = "native.compaction.live".into();
    let mut native = fragments(&[native_definition])?;
    for message in &mut native {
        let ModelContent::Part(ModelContentPart::Notices { notices }) = &mut message.content else {
            unreachable!()
        };
        notices[0].kind = "tools.bound".into();
    }
    let mut native_call = exchange("native", serde_json::json!(7));
    for message in &mut native_call {
        match &mut message.content {
            ModelContent::Part(ModelContentPart::ToolCall(call)) => {
                call.name = "native.compaction.live".into()
            }
            ModelContent::Part(ModelContentPart::ToolResult { name, .. }) => {
                *name = "native.compaction.live".into()
            }
            _ => unreachable!(),
        }
    }
    let mut mixed = vec![
        first[0].clone(),
        native[0].clone(),
        first[1].clone(),
        native[1].clone(),
    ];
    mixed.extend(native_call);
    mixed.extend(exchange("dynamic", serde_json::json!("old")));
    mixed.push(text(ModelRole::User, "current"));
    let current = mixed.len() as u32 - 1;
    let mixed = context(mixed, Some(current));
    prepare(mixed.messages.clone(), Vec::new())?;
    let mut retained_one_call = false;
    for requested_cut in 1..mixed.messages.len() {
        let (through, maximum) =
            policy.projection_budget(&mixed, None, requested_cut, &mut None)?;
        let (retained, proof) = compact(&mixed, maximum, through)?;
        validate_compaction(&proof, &mixed, &retained, None)?;
        prepare(retained.messages.clone(), Vec::new())?;
        if has_call(&retained, "native") || has_call(&retained, "dynamic") {
            retained_one_call |= has_call(&retained, "native") != has_call(&retained, "dynamic");
            for fragment in first.iter().chain(&native) {
                assert!(retained.messages.contains(fragment));
                let mut broken = retained.messages.clone();
                broken.remove(
                    broken
                        .iter()
                        .position(|message| message == fragment)
                        .unwrap(),
                );
                assert!(
                    prepare(broken, Vec::new()).is_err(),
                    "a partial table was admitted"
                );
            }
        }
    }
    assert!(
        retained_one_call,
        "two tables must not pin every exchange together"
    );
    Ok(())
}

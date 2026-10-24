use super::*;

fn source_messages() -> Vec<ModelMessage> {
    let sparse = serde_json::json!({"outer": [{"inner": "value"}]});
    vec![
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text("old ".repeat(4_000)),
        },
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Parts(vec![
                ModelContentPart::ToolCall(
                    crate::model::ToolCall::new("call".into(), "tool".into(), sparse.clone())
                        .with_metadata(sparse.clone()),
                ),
                ModelContentPart::ToolResult {
                    call_id: "call".into(),
                    name: "tool".into(),
                    content: crate::model::ToolResultContent::Json { value: sparse },
                },
            ]),
        },
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text("new tail".into()),
        },
    ]
}

#[test]
fn retiring_a_cut_releases_original_payload_custody_on_both_ledgers() -> Result<()> {
    let source = source_messages();
    for native in [false, true] {
        for old_drain in [false, true] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = work.live_allocation_bytes();
            let io = if native {
                PreparationIo::from_native_work(&work)?
            } else {
                PreparationIo::from_budget(budget, &cancellation)?
            };
            let mut messages = crate::model::clone_messages(&source, Some(&io))?;
            let expected = (messages.capacity() * std::mem::size_of::<ModelMessage>()) as u64
                + source
                    .last()
                    .ok_or_else(|| Error::Invalid("missing tail".into()))?
                    .content
                    .retained_payload_bytes()?;
            let before = io.owned_bytes()?;
            let mut offset = 0;
            if old_drain {
                // Dropping history alone leaves the original ledger charged.
                drop(messages.drain(..2));
                assert!(
                    io.owned_bytes()? > expected,
                    "drain-only control released custody"
                );
            } else {
                retire_prefix(&mut messages, &mut offset, 2, Some(&io))?;
                assert_eq!(offset, 2);
                assert_eq!(
                    io.owned_bytes()?,
                    expected,
                    "retirement leaked sparse-map or payload capacity"
                );
                assert!(io.owned_bytes()? < before);
                retire_prefix(&mut messages, &mut offset, 2, Some(&io))?;
                assert_eq!(
                    io.owned_bytes()?,
                    expected,
                    "retiring the same cut twice released the live tail"
                );
            }
            assert_eq!(messages.as_slice(), &source[2..]);
            let hold = io.guard_owned(io.owned_bytes()?)?;
            drop(messages);
            drop(hold);
            assert_eq!(io.owned_bytes()?, 0);
            drop(io);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}

#[test]
fn invalid_or_unfunded_retirement_leaves_values_and_offset_intact() -> Result<()> {
    for native in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let mut messages = source_messages();
        let original = messages.clone();
        let mut offset = 0;
        for through in [4, 2] {
            assert!(retire_prefix(&mut messages, &mut offset, through, Some(&io)).is_err());
            assert_eq!(
                messages, original,
                "failed retirement destroyed unfunded values"
            );
            assert_eq!(offset, 0);
            assert_eq!(io.owned_bytes()?, 0);
        }
        offset = 3;
        assert!(retire_prefix(&mut messages, &mut offset, 2, Some(&io)).is_err());
        assert_eq!(offset, 3);
        assert_eq!(messages, original);
        drop(messages);
        drop(original);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

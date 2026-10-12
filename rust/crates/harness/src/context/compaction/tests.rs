use super::*;
use crate::model::{ModelContent, ModelMessage, ModelRole, ToolResultContent};

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_metadata_depth_denial_precedes_output_tree_allocation() {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let cancellation = CancellationToken::new();
    let budget = WorkBudget {
        allocation_operations: 1024,
        peak_allocation_bytes: 1_048_576,
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        items_examined: 4096,
        ..WorkBudget::default()
    };
    let work = NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
    let mut source = serde_json::Value::Null;
    for _ in 0..257 {
        source = serde_json::Value::Array(vec![source]);
    }
    let before = work.receipt();
    assert!(super::super::context_contract_native(&source, &work).is_err());
    assert_eq!(
        work.receipt().allocation_operations,
        before.allocation_operations
    );
    assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
    let control = NativeMaterializationWork::new(budget, &cancellation).expect("control ledger");
    let source = serde_json::json!({ "source": ["bounded"] });
    let (actual, custody) =
        super::super::context_contract_native(&source, &control).expect("bounded actual metadata");
    assert_eq!(actual, source);
    assert!(custody.work().shares_ledger(&control));
    assert!(custody.bytes() > 0);
    drop(actual);
    drop(custody);
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn observed_compaction_matches_ordinary_exchange_and_input_selection() -> Result<()> {
    use crate::{
        conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
        resources::ProviderRef,
    };
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let file = FileRef::new(
        VolumeRef::new(
            ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
            "context".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])),
            &mut None,
        )?,
        "context/summary.txt".into(),
        "pinned".into(),
        FileDescriptor::from_bytes(b"summary", "text/plain", &mut None)?,
        "summary.txt".into(),
        &mut None,
    )?;
    let context = Context {
        messages: vec![
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("old".into()),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall {
                    call_id: "call".into(),
                    name: "tool".into(),
                    arguments: serde_json::Value::Null,
                }),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "call".into(),
                    name: "tool".into(),
                    content: ToolResultContent::Json {
                        value: serde_json::Value::Null,
                    },
                }),
            },
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("current".into()),
            },
        ],
        current_input_index: Some(3),
        ..Context::default()
    };
    let summary = super::super::ContextSummary {
        operation_id: crate::OperationId::new(),
        step: 0,
        source_messages: 3,
        source_digest: [0; 32],
        output: file,
    };
    let retention = CompactionRetention {
        roles: vec![],
        native_media: false,
    };
    let expected = compact_context(&context, 4, &summary, &retention, &mut None)?.0;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: 1024,
            peak_allocation_bytes: 1_048_576,
            bytes_copied: 1_048_576,
            items_examined: 1024,
            ..WorkBudget::default()
        },
        &cancellation,
    )
    .expect("original ledger");
    let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
    let (actual, claim) =
        compact_context(&context, 4, &summary, &retention, &mut Some(&mut observer))?;
    let custody = observer.into_custody();
    assert_eq!(actual, expected);
    assert_eq!(claim, custody.bytes());
    assert!(custody.work().shares_ledger(&work));
    drop(actual);
    drop(custody);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn partial_field_copy_releases_original_custody_and_cancelled_copy_is_empty() {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: 3,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 4096,
            bytes_copied: 4096,
            items_examined: 128,
            ..WorkBudget::default()
        },
        &cancellation,
    )
    .expect("original ledger");
    let messages = vec![
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("first".into()),
        },
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("second".into()),
        },
    ];
    let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
    assert!(clone_messages(&messages, None, &[], &mut Some(&mut observer)).is_err());
    assert!(work.receipt().bytes_copied >= 5);
    let custody = observer.into_custody();
    assert_eq!(
        custody.bytes(),
        0,
        "failed partial payload drops before its quota releases"
    );
    drop(custody);
    let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
    cancellation.cancel();
    let before = work.receipt();
    assert!(clone_messages(&messages, None, &[], &mut Some(&mut observer)).is_err());
    assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
}

#[test]
fn summary_prefix_preserves_ordinary_input_selection() -> Result<()> {
    let mut context = Context {
        messages: vec![
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("first".into()),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Parts(vec![ModelContentPart::Text {
                    text: "second".into(),
                }]),
            },
        ],
        current_input_index: Some(1),
        ..Context::default()
    };
    let (prefix, claim) = clone_context_prefix(&context, 1, &mut None)?;
    assert_eq!(prefix.messages, context.messages[..1]);
    assert_eq!(prefix.current_input_index, None);
    assert_eq!(claim, 0);
    context.current_input_index = Some(0);
    assert_eq!(
        clone_context_prefix(&context, 1, &mut None)?
            .0
            .current_input_index,
        Some(0)
    );
    assert!(clone_context_prefix(&context, 3, &mut None).is_err());
    Ok(())
}

#[test]
fn count_pressure_preserves_instructions_and_complete_exchanges() -> Result<()> {
    let mut context = Context {
        messages: (0..8)
            .map(|index| ModelMessage {
                role: if index == 0 {
                    ModelRole::System
                } else {
                    ModelRole::User
                },
                content: ModelContent::Text(format!("message {index}")),
            })
            .collect(),
        current_input_index: Some(7),
        ..Context::default()
    };
    let count = ModelTokenCount {
        request_digest: [0; 32],
        fixed_tokens: 1,
        message_tokens: vec![1; 8],
    };
    let policy = ThresholdCompaction::default();
    assert_eq!(
        policy.projection_budget(&context, &count, 6, &mut None)?,
        (4, 6)
    );
    context.messages[3] = ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::ToolCall {
            call_id: "call".into(),
            name: "tool".into(),
            arguments: serde_json::Value::Null,
        }),
    };
    context.messages[4] = ModelMessage {
        role: ModelRole::Tool,
        content: ModelContent::Part(ModelContentPart::ToolResult {
            call_id: "call".into(),
            name: "tool".into(),
            content: ToolResultContent::Json {
                value: serde_json::Value::Null,
            },
        }),
    };
    assert_eq!(
        policy.projection_budget(&context, &count, 6, &mut None)?,
        (5, 5)
    );
    assert!(
        policy
            .projection_budget(&context, &count, 2, &mut None)
            .is_err()
    );
    // Explicit token-only callers retain their original no-pressure behavior.
    assert!(
        policy
            .projection_budget(&context, &count, usize::MAX, &mut None)
            .is_err()
    );
    Ok(())
}

use super::*;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use acyclic_fs::NativeMaterializationWork;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn original_work(
    budget: acyclic_fs::WorkBudget,
    cancellation: &acyclic_fs::CancellationToken,
) -> Result<NativeMaterializationWork> {
    NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_file_validation_and_descriptor_hash_use_original_static_denials() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeTreeMaterializationError, WorkError};
    let agent = AgentId::from_bytes([24; 16]);
    let mut source = file(agent, "native-validation.txt")?;
    let cancellation = CancellationToken::new();
    let native = original_work(native_conversation_budget(), &cancellation)?;
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    source.path = "../escape".into();
    assert!(matches!(
        source.validate(&mut Some(&mut io)),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            ..
        })
    ));
    source.path = "native-validation.txt".into();
    source.validate(&mut Some(&mut io))?;
    assert!(matches!(
        source.descriptor().verify(b"too long", &mut Some(&mut io)),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            ..
        })
    ));
    assert!(matches!(
        source.descriptor().verify(b"other", &mut Some(&mut io)),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            ..
        })
    ));
    source.descriptor().verify(b"hello", &mut Some(&mut io))?;
    let mut allowance = native_conversation_budget();
    allowance.bytes_hashed = 0;
    let denied = original_work(allowance, &cancellation)?;
    let mut denied_io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&denied)?,
    );
    assert!(matches!(
        source
            .descriptor()
            .verify(b"hello", &mut Some(&mut denied_io)),
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "bytes_hashed",
                ..
            }),
            custody: None,
        })
    ));
    assert_eq!(denied.receipt().bytes_hashed, 0);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_file_clone_partial_failure_retires_only_its_actual_strings() -> Result<()> {
    use crate::store::history::HistoryIo as _;
    use acyclic_fs::{CancellationToken, NativeTreeMaterializationError, WorkError};
    let source = file(AgentId::from_bytes([29; 16]), "native-partial-clone.txt")?;
    let cancellation = CancellationToken::new();
    let mut budget = native_conversation_budget();
    budget.bytes_copied = 4 + source.volume().provider().namespace().len() as u64;
    let native = original_work(budget, &cancellation)?;
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    let held = io.clone_text("held")?;
    assert!(matches!(
        source.copy(&mut Some(&mut io)),
        Err(Error::NativeFilesystem { source, .. }) if matches!(*source,
                acyclic_fs::FsError::NativeKernel(NativeTreeMaterializationError::Work(
                    WorkError::BudgetExceeded { counter: "bytes_copied", .. })))
    ));
    let guard = io.into_custody()?;
    assert_eq!(
        guard.bytes(),
        held.capacity() as u64,
        "failed file fields are physically retired"
    );
    drop(held);
    drop(guard);
    let available =
        budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
    native
        .admit_retained(available)
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_state_clone_and_suffix_transfer_only_actual_message_components() -> Result<()> {
    use crate::store::history::HistoryIo;
    use acyclic_fs::CancellationToken;
    let agent = AgentId::from_bytes([31; 16]);
    let content = file(agent, "native-component-copy.txt")?;
    let mut source = ConversationState::default();
    source.bind(agent)?;
    for sequence in 1..=4 {
        source.append(message(&content, sequence), &mut None)?;
    }
    let first_token = CancellationToken::new();
    let budget = native_conversation_budget();
    let first = original_work(budget, &first_token)?;
    let mut io =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&first)?);
    let (held, produced) = source.copy(&mut Some(&mut io), None)?;
    assert_eq!(held, source);
    let deep = io.into_custody()?;
    assert_eq!(
        deep.bytes(),
        produced,
        "only observer-owned deep payload is reported"
    );
    let original_receipt = first.receipt();
    first_token.cancel();
    let selection = ModelContextSelection {
        conversation_revision: 2,
        message_ids: vec![Uuid::from_u128(1), Uuid::from_u128(2)],
        checkpoint: Some(file(agent, "native-component-checkpoint.json")?),
    };
    let second_token = CancellationToken::new();
    let second = original_work(budget, &second_token)?;
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&second)?,
    );
    let mut components = Vec::with_capacity(2);
    let (suffix, produced) = {
        let mut retain = |sequence, bytes, work: &mut Option<&mut dyn HistoryIo>| -> Result<()> {
            assert!(components.len() < components.capacity());
            let observer = work
                .as_deref_mut()
                .ok_or_else(|| Error::Invalid("native suffix callback lost its observer".into()))?;
            let guard = observer.take_native_custody(bytes)?;
            components.push((sequence, guard));
            Ok(())
        };
        held.checkpointed_suffix(&selection, &mut Some(&mut io), Some(&mut retain))?
    };
    assert_eq!(produced, 0);
    assert_eq!(io.into_custody()?.bytes(), 0);
    assert_eq!(
        components
            .iter()
            .map(|(sequence, _)| *sequence)
            .collect::<Vec<_>>(),
        [3, 4]
    );
    assert!(
        components
            .iter()
            .all(|(_, guard)| guard.work().shares_ledger(&second))
    );
    assert_eq!(
        first.receipt(),
        original_receipt,
        "cancelled source ledger is neither rebound nor absorbed"
    );
    assert_eq!(suffix.history_digest(), source.history_digest());
    drop(suffix);
    drop(components);
    second
        .admit_retained(
            budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes(),
        )
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    drop(held);
    drop(deep);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_append_failed_hash_and_growth_preserve_actual_state() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeTreeMaterializationError, WorkError};
    let cancellation = CancellationToken::new();
    let budget = native_conversation_budget();
    let funded = original_work(budget, &cancellation)?;
    let agent = AgentId::from_bytes([22; 16]);
    let content = file(agent, "native-failed-growth.txt")?;
    let mut state = ConversationState::default();
    state.bind(agent)?;
    for sequence in 1..=4 {
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&funded)?,
        );
        state.append(message(&content, sequence), &mut Some(&mut io))?;
    }
    let head = state.history_digest();
    let mut denied_budget = budget;
    denied_budget.bytes_hashed = 0;
    let denied = original_work(denied_budget, &cancellation)?;
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&denied)?,
    );
    assert!(matches!(
        state.append(message(&content, 5), &mut Some(&mut io)),
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "bytes_hashed",
                ..
            }),
            custody: None,
        })
    ));
    assert_eq!(denied.receipt().bytes_hashed, 0);
    drop(io);
    assert_eq!(state.history_digest(), head);
    assert_eq!(state.messages.len(), 4);
    let probe = original_work(budget, &cancellation)?;
    let mut probe_io =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&probe)?);
    advance_history_digest(
        head,
        &message(&content, 5),
        acyclic_stream::MAX_RECORD_BYTES as u64,
        &mut Some(&mut probe_io),
    )?;
    denied_budget = budget;
    denied_budget.allocation_operations = probe.receipt().allocation_operations;
    drop(probe_io);
    let denied = original_work(denied_budget, &cancellation)?;
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&denied)?,
    );
    assert!(matches!(
        state.append(message(&content, 5), &mut Some(&mut io)),
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "allocation_operations",
                ..
            }),
            custody: None,
        })
    ));
    assert!(
        denied.receipt().bytes_hashed > 0,
        "the original message hash completed before growth"
    );
    drop(io);
    assert_eq!(state.history_digest(), head);
    assert_eq!(state.messages.len(), 4);
    assert!(state.message(Uuid::from_u128(5)).is_none());
    drop(state);
    let available =
        budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
    funded
        .admit_retained(available)
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    Ok(())
}

#[cfg(feature = "filesystem")]
#[test]
fn admitted_conversation_rebuild_observes_original_digests_and_negative_work() -> Result<()> {
    use acyclic_fs::{CancellationToken, WorkBudget};
    let agent = AgentId::from_bytes([8; 16]);
    let content = file(agent, "observed-rebuild.txt")?;
    let mut original = ConversationState::default();
    original.bind(agent)?;
    for (sequence, kind) in [
        (1_u64, MessageKind::User),
        (2, MessageKind::Assistant),
        (3, MessageKind::System),
    ] {
        let mut extensions = BTreeMap::new();
        if sequence == 3 {
            extensions.insert("acyclic.turn.outcome".into(), content.clone());
        }
        original.append(
            ConversationMessage {
                id: Uuid::from_u128(u128::from(sequence)),
                sequence,
                kind,
                content: content.clone(),
                attachments: ReferencedAttachments::Inline { items: Vec::new() },
                reply_to: (sequence > 1).then(|| Uuid::from_u128(1)),
                tool_call_id: None,
                extensions,
            },
            &mut None,
        )?;
    }
    let bytes = crate::contract::canonical_json_bytes(&original)?;
    let expected_hash_bytes = original
        .messages()
        .iter()
        .try_fold(0_u64, |total, message| {
            total
                .checked_add(crate::contract::canonical_json_bytes(message)?.len() as u64)
                .and_then(|total| {
                    total.checked_add(b"harness:conversation-prefix:v3\0".len() as u64 + 64)
                })
                .ok_or_else(|| Error::Invalid("test digest input overflow".into()))
        })?;
    let budget = WorkBudget {
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        bytes_hashed: expected_hash_bytes,
        items_examined: 100_000,
        allocation_operations: 100_000,
        peak_allocation_bytes: 1_048_576,
        ..WorkBudget::default()
    };
    let cancellation = CancellationToken::new();
    let wire: ConversationStateWire = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Storage(error.to_string()))?;
    let mut work = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        budget,
        &cancellation,
    )?);
    let rebuilt = ConversationState::from_wire(wire, bytes.len() as u64, &mut Some(&mut work));
    let receipt = work.finish(rebuilt).map_err(|failure| failure.error)?;
    assert_eq!(receipt.value.0, original);
    assert!(receipt.work.bytes_encoded > 0);
    let mut denied = budget;
    denied.bytes_hashed = 0;
    let mut work = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        denied,
        &cancellation,
    )?);
    let wire = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Storage(error.to_string()))?;
    let result = ConversationState::from_wire(wire, bytes.len() as u64, &mut Some(&mut work));
    let failed = work
        .finish(result)
        .expect_err("first original message hash must reject");
    assert_eq!(failed.work.bytes_hashed, 0);
    assert!(failed.work.bytes_encoded > 0);
    assert_eq!(failed.work.output_bytes, 0);
    let mut work = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        budget,
        &cancellation,
    )?);
    cancellation.cancel();
    let wire = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Storage(error.to_string()))?;
    let result = ConversationState::from_wire(wire, bytes.len() as u64, &mut Some(&mut work));
    let failed = work
        .finish(result)
        .expect_err("cancelled reconstruction must not start");
    assert_eq!(failed.work.bytes_encoded + failed.work.bytes_hashed, 0);
    Ok(())
}

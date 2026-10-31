use super::*;
use crate::executor::{ToolErrorClass, ToolFailureCause, ToolFailureKind};
use acyclic_stream::StreamProviderExt as _;

#[test]
fn checkpoint_frame_has_fixed_size_and_one_canonical_root_encoding() -> Result<()> {
    let mut checkpoint = Checkpoint {
        state: IndexState {
            operation: OperationId::from_bytes([1; 16]),
            turn: OperationId::from_bytes([2; 16]),
            volume: [3; 32],
            tail: u64::MAX,
            nodes_tail: u64::MAX,
            roots: [None; 5],
            pending_dispatches: u64::MAX,
            anchor: [4; 32],
            latest_response: Some(u32::MAX),
            model_dispatched: true,
        },
        proof: [5; 64],
    };
    for mask in 0..32_u8 {
        for (index, root) in checkpoint.state.roots.iter_mut().enumerate() {
            *root = (mask & (1 << index) != 0).then_some(NodeRef {
                sequence: u64::MAX - 1,
                digest: [u8::try_from(index).map_err(|_| missing(None))?; 32],
            });
        }
        let bytes = codec::encode(&checkpoint, None)?;
        assert_eq!(
            bytes.len(),
            399,
            "checkpoint size depends on history or root count"
        );
        let decoded = codec::decode(&bytes, None)?;
        assert_eq!(
            codec::state_bytes(&decoded.state, None)?,
            codec::state_bytes(&checkpoint.state, None)?
        );
        assert_eq!(decoded.proof, checkpoint.proof);
        for length in 0..bytes.len() {
            assert!(codec::decode(&bytes[..length], None).is_err());
        }
    }
    checkpoint.state.roots = [None; 5];
    let original = codec::encode(&checkpoint, None)?;
    let mut former = original.to_vec();
    former[..4].copy_from_slice(b"EXC3");
    assert!(codec::decode(&former, None).is_err());
    for index in 0..5 {
        let mut changed = original.to_vec();
        let root = 84 + index * 41;
        changed[root + 1] = 1;
        assert!(
            codec::decode(&changed, None).is_err(),
            "absent root carried a nonzero sequence"
        );
        changed[root + 1] = 0;
        changed[root + 9] = 1;
        assert!(
            codec::decode(&changed, None).is_err(),
            "absent root carried a digest"
        );
        changed[root] = 2;
        assert!(
            codec::decode(&changed, None).is_err(),
            "unknown root tag was accepted"
        );
    }
    let mut trailing = original.to_vec();
    trailing.push(0);
    assert!(codec::decode(&trailing, None).is_err());
    for ordinal in [None, Some(0), Some(u32::MAX)] {
        checkpoint.state.latest_response = ordinal;
        let bytes = codec::encode(&checkpoint, None)?;
        assert_eq!(codec::decode(&bytes, None)?.state.latest_response, ordinal);
    }
    checkpoint.state.latest_response = None;
    let mut absent = codec::encode(&checkpoint, None)?.to_vec();
    absent[330] = 1;
    assert!(
        codec::decode(&absent, None).is_err(),
        "absent ordinal carried nonzero padding"
    );
    absent[329] = 2;
    assert!(
        codec::decode(&absent, None).is_err(),
        "unknown ordinal tag was accepted"
    );
    absent[329] = 0;
    absent[330] = 0;
    absent[334] = 2;
    assert!(
        codec::decode(&absent, None).is_err(),
        "unknown dispatch flag was accepted"
    );
    checkpoint.state.latest_response = Some(0);
    checkpoint.state.model_dispatched = false;
    assert!(
        codec::decode(&codec::encode(&checkpoint, None)?, None).is_err(),
        "response cursor was accepted without a model dispatch"
    );
    Ok(())
}

fn event(step: u32) -> ExecutionEvent {
    ExecutionEvent::ToolFailed {
        step,
        call_id: "call".into(),
        reason: ToolFailureKind::Unavailable,
        cause: ToolFailureCause {
            class: ToolErrorClass::Invalid,
            message: "unavailable".into(),
        },
    }
}

fn bytes(
    operation: OperationId,
    retry: &str,
    event: &ExecutionEvent,
) -> Result<acyclic_stream::OwnedBytes> {
    serde_json::to_vec(&Observation {
        version: OBSERVATION_VERSION,
        operation_id: operation,
        turn_operation: operation,
        retry_digest: retry.into(),
        event: event.clone(),
    })
    .map(Into::into)
    .map_err(|_| missing(None))
}

fn accept(summary: &mut ExecutionSummary, retry: &str, event: ExecutionEvent) -> Result<[u8; 32]> {
    summary.accept(
        &ExecutionRecord {
            operation_id: summary.operation.ok_or_else(|| missing(None))?,
            sequence: summary.tail + 1,
            idempotency_key: retry.into(),
            event,
        },
        None,
        Limits::default(),
        None,
    )?;
    Ok(*blake3::hash(retry.as_bytes()).as_bytes())
}

#[tokio::test]
async fn checkpoint_and_observation_publish_at_one_authenticated_cut() -> Result<()> {
    for native in [None, Some(false), Some(true)] {
        check_cut(native).await?;
    }
    Ok(())
}

async fn check_cut(native: Option<bool>) -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, 4 * 1024 * 1024);
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = native
        .map(|native| {
            if native {
                PreparationIo::from_native_work(&work)
            } else {
                PreparationIo::from_budget(budget, &cancellation)
            }
        })
        .transpose()?;
    let (journal, _issuer) = crate::filesystem::execution_journal::test_support::journal().await?;
    let operation = OperationId::new();
    let mut summary = ExecutionSummary {
        operation: Some(operation),
        ..Default::default()
    };
    for (retry, event) in [
        (
            "start",
            ExecutionEvent::Started {
                request_digest: [1; 32],
            },
        ),
        ("tool", event(0)),
    ] {
        let retry_key = accept(&mut summary, retry, event.clone())?;
        let observation = bytes(operation, retry, &event)?;
        let recovery = journal
            .publish_indexed_observation(
                &mut summary,
                retry_key,
                (&event, &observation, None),
                io.as_ref(),
            )
            .await?
            .ok_or_else(|| missing(io.as_ref()))?;
        let loaded = journal
            .load_index_checkpoint(operation, io.as_ref())
            .await?
            .ok_or_else(|| missing(io.as_ref()))?;
        assert_eq!(loaded.0.tail, summary.tail);
        assert_eq!(loaded.0.roots, recovery.state.roots);
        assert_eq!(loaded.1, recovery.head_tail);
        assert!(
            journal
                .index_authenticates(loaded.0, summary.tail, retry, &event, io.as_ref())
                .await?
        );
        assert!(
            journal
                .index_authenticates(
                    loaded.0,
                    summary.tail,
                    "different-retry",
                    &event,
                    io.as_ref()
                )
                .await
                .is_err()
        );
        summary.recovery = Some(recovery);
    }
    assert_eq!(journal.replay(operation, 0, 4, None)?.await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn detached_and_forged_checkpoint_heads_cannot_authorize_recovery() -> Result<()> {
    let (journal, _issuer) = crate::filesystem::execution_journal::test_support::journal().await?;
    let operation = OperationId::new();
    let mut summary = ExecutionSummary {
        operation: Some(operation),
        ..Default::default()
    };
    let first = ExecutionEvent::Started {
        request_digest: [1; 32],
    };
    let retry = accept(&mut summary, "start", first.clone())?;
    let recovery = journal
        .publish_indexed_observation(
            &mut summary,
            retry,
            (&first, &bytes(operation, "start", &first)?, None),
            None,
        )
        .await?
        .expect("initial commit");
    let original = journal
        .encode_index_checkpoint(recovery.state, None)
        .await?;
    let head = journal.index_path(operation, "head", None)?;
    journal
        .stream
        .append(acyclic_stream::AppendRequest {
            path: (*head).clone(),
            records: vec![original.clone().into()],
            if_tail: Some(1),
            idempotency_key: acyclic_stream::IdempotencyKey::unique(),
        })
        .await?;
    assert!(
        journal
            .load_index_checkpoint(operation, None)
            .await
            .is_err(),
        "detached valid root was accepted without its anchor commit"
    );
    let observation = bytes(operation, "forged", &event(0))?;
    let mut forged = codec::decode(&original, None)?;
    forged.state.tail = 2;
    forged.state.anchor = *blake3::hash(&observation).as_bytes();
    let execution = crate::distributed::execution_path(operation, &mut None)?;
    let request = acyclic_stream::CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![
            acyclic_stream::CommitCondition::Tail {
                path: (*head).clone(),
                expected: 2,
            },
            acyclic_stream::CommitCondition::Tail {
                path: execution.clone(),
                expected: 1,
            },
        ],
        mutations: vec![
            acyclic_stream::CommitMutation::Append {
                path: (*head).clone(),
                records: vec![codec::encode(&forged, None)?.into()],
            },
            acyclic_stream::CommitMutation::Append {
                path: execution,
                records: vec![observation.into()],
            },
        ],
        idempotency_key: StreamKey::new(acyclic_stream::OwnedBytes::from_static(b"forged-head"))?,
    };
    assert!(matches!(
        journal.stream.commit(request).await?,
        acyclic_stream::CommitOutcome::Committed(_)
    ));
    assert!(
        journal
            .load_index_checkpoint(operation, None)
            .await
            .is_err(),
        "forged root was accepted merely because its anchor was atomic"
    );
    Ok(())
}

#[tokio::test]
async fn public_checkpoint_recovery_needs_no_live_signing_owner() -> Result<()> {
    let (mut journal, issuer) =
        crate::filesystem::execution_journal::test_support::journal().await?;
    let operation = OperationId::new();
    journal
        .append(
            operation,
            "start",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    let public = crate::core::AuthorityVerifier::from_public(
        "append-cost".into(),
        journal.verifier.audience().clone(),
        issuer.public_workload_trust()?,
        journal.verifier.public_checkpoint_key(),
        issuer.trust_changes(),
    )?;
    drop(issuer);
    journal.verifier = public;
    let recovered = journal.load_index_checkpoint(operation, None).await;
    assert!(
        matches!(&recovered, Ok(Some(_))),
        "public-only checkpoint verification attempted to sign"
    );
    let recovered = recovered?.ok_or_else(|| missing(None))?;
    assert_eq!(
        recovered.0.tail, 1,
        "public-only checkpoint verification attempted to sign"
    );
    assert!(
        journal
            .encode_index_checkpoint(recovered.0, None)
            .await
            .is_err()
    );
    Ok(())
}

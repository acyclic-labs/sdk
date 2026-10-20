use super::*;
use crate::conversation::{VolumeClass, VolumeOwner};
use crate::core::{AggregateKind, AuthorityIssuer};
use crate::filesystem::workspace_ref;
use acyclic_fs::Fs;

#[test]
fn workflow_decode_slots_follow_containers_and_cover_physical_peak() -> Result<()> {
    use crate::conversation::FileDescriptor;
    use crate::workflow::{
        MachineCheckpoint, MachineIdentity, MachineStatus, MachineTransition, WorkflowCommand,
        WorkflowRecord,
    };
    let payload = FileRef::new(
        VolumeRef::new(
            crate::resources::ProviderRef::new(
                "workflow-codec".into(),
                "filesystem".into(),
                "2".into(),
                &mut None,
            )?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([4; 16])),
            &mut None,
        )?,
        "commands/input.json".into(),
        "immutable-version".into(),
        FileDescriptor::from_bytes(b"{}", "application/json", &mut None)?,
        "input.json".into(),
        &mut None,
    )?;
    for count in [1, 2, 4, 8] {
        let record = WorkflowRecord {
            operation_id: crate::OperationId::from_bytes([5; 16]),
            idempotency_key: IdempotencyKey::new("workflow-codec")?,
            prior: MachineCheckpoint {
                machine: MachineIdentity {
                    name: "test.codec".into(),
                    version: "1".into(),
                    digest: [1; 32],
                },
                revision: 0,
                state: serde_json::json!({"text": "x".repeat(1024), "scalars": [1, 2, 3, 4]}),
            },
            input: serde_json::json!({"nested": [{"value": true}]}),
            transition: MachineTransition {
                state: serde_json::json!({"ready": true}),
                commands: (0..count)
                    .map(|id| WorkflowCommand {
                        operation_id: crate::OperationId::from_bytes([id; 16]),
                        kind: "test.command".into(),
                        payload: payload.clone(),
                    })
                    .collect(),
                status: match count {
                    2 => MachineStatus::Completed {
                        value: serde_json::json!({"result": ["done", {"nested": true}]}),
                    },
                    4 => MachineStatus::Failed {
                        message: "failed".repeat(20),
                    },
                    _ => MachineStatus::Suspended,
                },
            },
        };
        let bytes =
            serde_json::to_vec(&record).map_err(|error| Error::Invalid(error.to_string()))?;
        let native = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024),
            &CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = native.live_allocation_bytes();
        let io = PreparationIo::from_native_work(&native)?;
        // A resident Value has no parsed-container census. Its conservative
        // fallback must still cover actual typed decoding and release custody.
        if count == 1 {
            let value =
                serde_json::to_value(&record).map_err(|error| Error::Invalid(error.to_string()))?;
            let mut decoded = None;
            let physical = acyclic_test_support::measure(|| {
                decoded = Some(io.value_decode_preparation::<WorkflowRecord>(
                    &value,
                    crate::wire::ErrorCode::Storage,
                ));
            });
            let (actual, custody) = decoded.expect("resident decoder ran")?;
            assert_eq!(actual, record);
            assert!(physical.count_total > 0);
            assert!(physical.bytes_max <= native.receipt().peak_allocation_bytes - baseline);
            drop(actual);
            drop(custody);
            assert_eq!(native.live_allocation_bytes(), baseline);
        }
        let mut decoded = None;
        let physical = acyclic_test_support::measure(|| {
            decoded = Some(io.decode(
                std::marker::PhantomData::<WorkflowRecord>,
                &bytes,
                crate::wire::ErrorCode::Storage,
            ));
        });
        let (actual, custody) = decoded.expect("decoder ran")?;
        assert_eq!(actual, record);
        assert_eq!(
            custody.bytes(),
            actual
                .decoded_owned_bytes()?
                .expect("audited workflow ownership")
        );
        assert!(
            physical.bytes_current <= custody.bytes() as i64,
            "resident physical={physical:?}; custody={}",
            custody.bytes()
        );
        assert!(physical.count_total > 0, "allocation observer is active");
        assert!(
            physical.bytes_max <= native.receipt().peak_allocation_bytes - baseline,
            "physical={physical:?}; receipt={:?}",
            native.receipt()
        );
        drop(actual);
        drop(custody);
        drop(io);
        assert_eq!(native.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[test]
fn denied_vector_growth_does_not_retain_storage() -> Result<()> {
    let mut budget = acyclic_fs::performance::work_budget(100_000, 16_000_000);
    budget.bytes_copied = 0;
    let io = PreparationIo::from_budget(budget, &CancellationToken::new())?;
    let mut values = vec![1_u8];
    assert!(PreparationIo::reserve(&mut values, 4, Some(&io)).is_err());
    assert_eq!(values, [1]);
    assert_eq!(io.owned_bytes()?, 0);
    Ok(())
}

#[test]
fn denied_text_growth_releases_its_preclaim() -> Result<()> {
    use crate::store::history::HistoryIo;
    let mut budget = acyclic_fs::performance::work_budget(100_000, 16_000_000);
    budget.allocation_operations = 0;
    let io = PreparationIo::from_budget(budget, &CancellationToken::new())?;
    let mut text = String::new();
    assert!(io.history_io().reserve_text(&mut text, 32).is_err());
    assert_eq!(text.capacity(), 0);
    assert_eq!(io.owned_bytes()?, 0);
    Ok(())
}

#[tokio::test]
async fn dropping_pending_completion_releases_its_frame() -> Result<()> {
    let io = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(100_000, 16_000_000),
        &CancellationToken::new(),
    )?;
    let before = io.owned_bytes()?;
    let future = crate::conversation::BoundedContentFuture::new(Some(&io), || async {
        std::future::pending::<()>().await;
        Ok(())
    })?;
    assert!(io.owned_bytes()? > before);
    let mut completion = Box::pin(future);
    assert!(futures::poll!(&mut completion).is_pending());
    drop(completion);
    assert_eq!(io.owned_bytes()?, before);
    Ok(())
}

#[tokio::test]
async fn exact_read_gate_precedes_backend_and_pinned_fork_never_writes_back() -> Result<()> {
    let provider = crate::resources::ProviderRef::new(
        "source-read-control".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let project = |name: &str| {
        VolumeRef::new(
            provider.clone(),
            name.into(),
            VolumeClass::Project,
            VolumeOwner::Project("source-read-control".into()),
            &mut None,
        )
    };
    let source = project("selected")?;
    let caller = project("caller")?;
    let child = project("child")?;
    let denied_child = project("denied-child")?;
    let initial = host.create_volume(&source).await?;
    let caller_head = host.create_volume(&caller).await?;
    let selected_generation = host
        .apply(
            &initial.workspace,
            Some(&initial.generation),
            &[WorkspaceMutation::PutFile {
                path: "/value.txt".into(),
                bytes: b"selected".to_vec().into(),
            }],
            &IdempotencyKey::new("selected-generation")?,
        )
        .await?;
    let wrong_generation = host
        .apply(
            &caller_head.workspace,
            Some(&caller_head.generation),
            &[WorkspaceMutation::PutFile {
                path: "/other.txt".into(),
                bytes: b"other-source".to_vec().into(),
            }],
            &IdempotencyKey::new("other-source-generation")?,
        )
        .await?;
    let issuer = AuthorityIssuer::new(
        "source-read-control",
        [41; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "caller".into(),
        },
    );
    let scope = issuer
        .issue(
            "exact-project-read",
            &crate::Capabilities::new([
                source.capability(VolumeOperation::Read, &mut None)?,
                caller.capability(VolumeOperation::Read, &mut None)?,
            ]),
            None,
            None,
            &mut None,
        )
        .expect("scope proof");
    let selected_read = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &source,
        VolumeOperation::Read,
        MAX_CAPTURE_BYTES,
        &mut None,
    )?;
    let caller_read = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &caller,
        VolumeOperation::Read,
        MAX_CAPTURE_BYTES,
        &mut None,
    )?;
    assert!(matches!(
        host.fork_volume_pinned(
            &source,
            &caller_read,
            &selected_generation,
            &denied_child,
            None,
            &IdempotencyKey::new("denied-source-read")?,
            None,
            None
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    let denied_ref = workspace_ref(provider.clone(), &denied_child.storage_name(&mut None)?)?;
    assert!(host.resolve(&denied_ref, None).await.is_err());
    assert!(
        host.fork_volume_pinned(
            &source,
            &selected_read,
            &wrong_generation,
            &denied_child,
            None,
            &IdempotencyKey::new("wrong-generation")?,
            None,
            None
        )
        .await
        .is_err()
    );
    assert!(host.resolve(&denied_ref, None).await.is_err());
    let later_generation = host
        .apply(
            &initial.workspace,
            Some(&selected_generation),
            &[WorkspaceMutation::PutFile {
                path: "/value.txt".into(),
                bytes: b"later".to_vec().into(),
            }],
            &IdempotencyKey::new("later-generation")?,
        )
        .await?;
    let forked = host
        .fork_volume_pinned(
            &source,
            &selected_read,
            &selected_generation,
            &child,
            None,
            &IdempotencyKey::new("pinned-selected-fork")?,
            None,
            None,
        )
        .await?;
    assert_eq!(
        host.read(
            &forked.workspace,
            Some(&forked.generation),
            "/value.txt",
            32,
            None
        )
        .await?
        .as_ref(),
        b"selected"
    );
    host.apply(
        &forked.workspace,
        Some(&forked.generation),
        &[WorkspaceMutation::PutFile {
            path: "/value.txt".into(),
            bytes: b"child".to_vec().into(),
        }],
        &IdempotencyKey::new("isolated-child-edit")?,
    )
    .await?;
    assert_eq!(
        host.resolve(&initial.workspace, None).await?.generation,
        later_generation
    );
    assert_eq!(
        host.read(&initial.workspace, None, "/value.txt", 32, None)
            .await?
            .as_ref(),
        b"later"
    );
    assert_eq!(
        host.read(
            &initial.workspace,
            Some(&selected_generation),
            "/value.txt",
            32,
            None
        )
        .await?
        .as_ref(),
        b"selected"
    );
    Ok(())
}

#[test]
fn fired_cancellation_stays_cancelled_through_preparation_and_history() -> Result<()> {
    use crate::client::HeadlessOutcome;
    use crate::store::history::HistoryIo as _;
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(100_000, 16_000_000);
    let io = PreparationIo::from_budget(budget, &cancellation)?;
    let history =
        crate::store::HistoryWork::new(PreparationIo::from_budget(budget, &cancellation)?);
    io.check_cancellation()?;
    history.check_cancellation()?;
    cancellation.cancel();
    for failure in [
        PreparationIo::from_budget(budget, &cancellation).err(),
        PreparationIo::for_unmeasured(&cancellation).err(),
        io.check_cancellation().err(),
        history.check_cancellation().err(),
    ] {
        let Some(failure) = failure else {
            return Err(Error::Invalid("a fired cancellation was accepted".into()));
        };
        assert!(matches!(failure, Error::Cancelled), "{failure:?}");
        assert_eq!(failure.code(), crate::wire::ErrorCode::Cancelled);
        assert_eq!(
            HeadlessOutcome::from_error(&failure),
            HeadlessOutcome::Cancelled
        );
    }
    assert_eq!(
        HeadlessOutcome::from_error(&Error::Storage("provider write failed".into())),
        HeadlessOutcome::Failed
    );
    Ok(())
}

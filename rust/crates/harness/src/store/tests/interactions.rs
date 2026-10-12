use super::*;
use futures::TryStreamExt as _;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn admission_content_keeps_original_published_bytes_until_last_alias() -> Result<()> {
    use crate::conversation::VolumeOperation;
    use crate::filesystem::{
        FilesystemContentPublisher, FilesystemContentVerifier, FilesystemHost,
    };
    use acyclic_fs::{
        CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkBudget,
        WorkError,
    };

    let agent = crate::AgentId::from_bytes([94; 16]);
    let provider = ProviderRef::new(
        "admission-bytes".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let volume = VolumeRef::new(
        provider.clone(),
        "admission-bytes".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let host = Arc::new(FilesystemHost::new(acyclic_fs::Fs::memory(), provider)?);
    host.create_volume(&volume).await?;
    let issuer = issuer();
    let scope = issuer
        .issue(
            "content-owner",
            &Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let publisher = FilesystemContentPublisher::new(
        host.clone(),
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        65_536,
        None,
    )?;
    let reader = Arc::new(FilesystemContentVerifier::new(
        host,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        65_536,
        None,
    )?);
    let request = Interaction::question(
        "original published prompt".into(),
        json!({"type": "string"}),
    )?;
    let published = crate::contract::canonical_json_bytes(&request)?;
    let reference = publisher
        .stage_once(
            OperationId::from_bytes([95; 16]),
            "interactions/request.json",
            &published,
            "application/json",
            "request.json",
            None,
        )?
        .await?
        .0;
    let ticket = InteractionTicket {
        id: uuid::Uuid::from_bytes([96; 16]),
        kind: InteractionKind::Question,
        request: reference.clone(),
        deadline_unix_ms: None,
        approval: None,
    };
    let budget = WorkBudget {
        authority_records_read: 4_096,
        authority_bytes_read: 16_777_216,
        backend_read_operations: 4_096,
        source_bytes_read: 16_777_216,
        bytes_copied: 16_777_216,
        bytes_encoded: 16_777_216,
        bytes_hashed: 16_777_216,
        output_bytes: 16_777_216,
        items_examined: 1_000_000,
        items_returned: 4_096,
        allocation_operations: 1_000_000,
        peak_allocation_bytes: 16_777_216,
        ..WorkBudget::default()
    };
    let cancellation = CancellationToken::new();
    let native =
        NativeMaterializationWork::new(budget, &cancellation).expect("original content ledger");
    let available =
        budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)
            .expect("original content observer"),
    );
    let (bytes, claim) = StreamAggregate::<MemoryStream>::read_admission_content_with_work(
        reader.as_ref(),
        &reference,
        &mut Some(&mut io),
    )
    .await?;
    assert_eq!(bytes.as_ref(), published.as_slice());
    assert_eq!(claim, 0, "native carrier owns its claim, not the observer");
    let alias = bytes.clone();
    let empty_alias = alias.slice(0..0);
    ticket.validate_request_bytes(&alias, &mut Some(&mut io))?;
    drop(bytes);
    drop(alias);
    assert!(empty_alias.is_empty());
    assert!(
        matches!(
            native.admit_retained(available),
            Err(NativeTreeMaterializationError::Work(
                WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    ..
                }
            ))
        ),
        "even an empty final view keeps the original whole allocation guarded"
    );
    drop(empty_alias);
    native
        .admit_retained(available)
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;

    let client = Arc::new(MemoryStream::default());
    let aggregate = StreamAggregate::open(
        &client,
        authority(),
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(reader.clone());
    aggregate
        .validate_task_admission_content_with_work(
            &Action::OpenInteraction {
                ticket: ticket.clone(),
            },
            &mut Some(&mut io),
        )
        .await?;
    native
        .admit_retained(available)
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    let missing = FileRef::new(
        volume,
        "interactions/missing.json".into(),
        reference.version().into(),
        FileDescriptor::from_bytes(
            br#"{"type":"question","value":"yes"}"#,
            "application/json",
            &mut None,
        )?,
        "missing.json".into(),
        &mut None,
    )?;
    let error = aggregate
        .validate_interaction_part_with_work(&ticket, &missing, None, &mut Some(&mut io))
        .await
        .expect_err("real publisher has no second response file");
    assert!(native.receipt().source_bytes_read > 0);
    drop(error);
    native
        .admit_retained(available)
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    assert_eq!(io.into_custody()?.bytes(), 0);

    let mut ordinary = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_budget(budget, &cancellation)?,
    );
    let error = aggregate
        .validate_interaction_part_with_work(&ticket, &missing, None, &mut Some(&mut ordinary))
        .await
        .expect_err("ordinary second read preserves the missing-file denial");
    drop(error);
    history::HistoryIo::admit_owned(&ordinary, budget.peak_allocation_bytes)?;
    assert!(ordinary.receipt().source_bytes_read > 0);
    Ok(())
}

#[tokio::test]
async fn interaction_request_and_answer_commit_only_after_exact_content_validation() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let request = Interaction::question(
        "secret prompt".into(),
        json!({"type": "string", "minLength": 2}),
    )?;
    let request_bytes =
        serde_json::to_vec(&request).map_err(|error| Error::Invalid(error.to_string()))?;
    let answer_bytes = br#"{"type":"question","value":"yes"}"#;
    let ticket = InteractionTicket {
        id: uuid::Uuid::from_bytes([3; 16]),
        kind: InteractionKind::Question,
        request: interaction_file("interactions/request.json", &request_bytes)?,
        deadline_unix_ms: None,
        approval: None,
    };
    let answer = interaction_file("interactions/answer.json", answer_bytes)?;
    let mut bytes = std::collections::BTreeMap::new();
    bytes.insert(
        MigrationContentStore::key(&ticket.request)?,
        request_bytes.clone(),
    );
    bytes.insert(MigrationContentStore::key(&answer)?, answer_bytes.to_vec());
    let mut aggregate = StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(Arc::new(MigrationContentStore(Arc::new(
        std::sync::Mutex::new(bytes),
    ))));
    aggregate
        .execute(
            Command {
                operation_id: OperationId::from_bytes([9; 16]),
                idempotency_key: IdempotencyKey("bind-interaction-owner".into()),
                expected_revision: 0,
                scope: issuer()
                    .issue(
                        "owner",
                        &Capabilities::new(["conversation:bind"]),
                        None,
                        None,
                        &mut None,
                    )
                    .expect("scope proof"),
                causal_parent: None,
                action: Action::BindConversation {
                    agent: crate::AgentId::from_bytes([8; 16]),
                },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let open = Command {
        operation_id: OperationId::from_bytes([10; 16]),
        idempotency_key: IdempotencyKey("open-interaction".into()),
        expected_revision: 1,
        scope: issuer()
            .issue(
                "opener",
                &Capabilities::new(["interaction:open"]),
                None,
                None,
                &mut None,
            )
            .expect("scope proof"),
        causal_parent: None,
        action: Action::OpenInteraction {
            ticket: ticket.clone(),
        },
    };
    aggregate
        .execute(
            open,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let resolution = InteractionResolution {
        id: ticket.id,
        expected_version: 1,
        outcome: InteractionOutcome::Answered {
            answer: Box::new(answer),
        },
        detail: None,
    };
    let resolve = Command {
        operation_id: OperationId::from_bytes([11; 16]),
        idempotency_key: IdempotencyKey("resolve-interaction".into()),
        expected_revision: 2,
        scope: issuer()
            .issue(
                "responder",
                &Capabilities::new(["interaction:resolve", ticket.responder_grant().as_str()]),
                None,
                None,
                &mut None,
            )
            .expect("scope proof"),
        causal_parent: None,
        action: Action::ResolveInteraction {
            resolution: resolution.clone(),
        },
    };
    aggregate
        .execute(
            resolve,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let records = acyclic_stream::Stream::new(
        std::sync::Arc::clone(&client),
        authority().stream_path(&mut None)?,
    )?
    .read(0, 3)
    .await?
    .try_collect::<Vec<_>>()
    .await?;
    assert_eq!(records.len(), 3);
    assert!(records.iter().all(|record| {
        !record
            .value
            .windows(b"secret prompt".len())
            .any(|window| window == b"secret prompt")
    }));
    assert!(records.iter().all(|record| {
        !record
            .value
            .windows(answer_bytes.len())
            .any(|window| window == answer_bytes)
    }));
    let reopened = StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    assert_eq!(
        reopened
            .reducer()
            .interaction(&ticket.id)
            .and_then(|(_, outcome)| outcome.as_ref()),
        Some(&resolution)
    );
    let future = InteractionTicket {
        id: uuid::Uuid::from_bytes([4; 16]),
        deadline_unix_ms: Some(u64::MAX),
        ..ticket
    };
    aggregate
        .execute(
            Command {
                operation_id: OperationId::from_bytes([12; 16]),
                idempotency_key: IdempotencyKey("open-future-interaction".into()),
                expected_revision: 3,
                scope: issuer()
                    .issue(
                        "opener",
                        &Capabilities::new(["interaction:open"]),
                        None,
                        None,
                        &mut None,
                    )
                    .expect("scope proof"),
                causal_parent: None,
                action: Action::OpenInteraction {
                    ticket: future.clone(),
                },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let premature = Command {
        operation_id: OperationId::from_bytes([13; 16]),
        idempotency_key: IdempotencyKey("premature-expiry".into()),
        expected_revision: 4,
        scope: issuer()
            .issue(
                "responder",
                &Capabilities::new(["interaction:resolve", future.responder_grant().as_str()]),
                None,
                None,
                &mut None,
            )
            .expect("scope proof"),
        causal_parent: None,
        action: Action::ResolveInteraction {
            resolution: InteractionResolution {
                id: future.id,
                expected_version: 1,
                outcome: InteractionOutcome::Expired,
                detail: None,
            },
        },
    };
    assert!(matches!(
        aggregate
            .execute(
                premature,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(aggregate.reducer().revision(), 4);
    Ok(())
}

use super::*;
use futures::TryStreamExt as _;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn admission_content_keeps_original_published_bytes_until_last_alias() -> Result<()> {
    use crate::conversation::ContentPublisher as _;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let fixture = crate::durable_host::tests::fixture::Fixture::new(
        Arc::new(MemoryStream::default()),
        acyclic_fs::Fs::memory(),
        json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    let request =
        Interaction::question("original published prompt".into(), json!({"type":"string"}))?;
    let published = crate::contract::canonical_json_bytes(&request)?;
    let reference = fixture
        .publisher
        .stage(
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
        ..acyclic_fs::performance::work_budget(4_096, 16_777_216)
    };
    let cancellation = CancellationToken::new();
    let native =
        NativeMaterializationWork::new(budget, &cancellation).expect("finite fixture budget");
    let available =
        budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
    let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&native)?);
    let (bytes, _) = StreamAggregate::<MemoryStream>::read_admission_content_with_work(
        fixture.reader.as_ref(),
        &reference,
        &mut Some(&mut io),
    )
    .await?;
    assert_eq!(bytes.as_ref(), published.as_slice());
    let empty_alias = bytes.slice(0..0);
    ticket.validate_request_bytes(&bytes, &mut Some(&mut io))?;
    drop(bytes);
    assert!(native.admit_retained(available).is_err());
    drop(empty_alias);
    assert!(native.admit_retained(available).is_ok());
    let aggregate = with_content(&fixture.stream)
        .await?
        .with_content_verifier(fixture.reader.clone());
    aggregate
        .validate_task_admission_content_with_work(
            &Action::OpenInteraction {
                ticket: (ticket.clone()).into(),
            },
            &mut Some(&mut io),
        )
        .await?;
    assert!(native.admit_retained(available).is_ok());
    let missing = FileRef::new(
        reference.volume().clone(),
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
        .expect_err("missing response");
    assert!(native.receipt().source_bytes_read > 0);
    drop(error);
    assert!(native.admit_retained(available).is_ok());
    let mut ordinary = HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        budget,
        &cancellation,
    )?);
    let error = aggregate
        .validate_interaction_part_with_work(&ticket, &missing, None, &mut Some(&mut ordinary))
        .await
        .expect_err("missing response");
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
        json!({"type":"string", "minLength":2}),
    )?;
    let request_bytes = crate::contract::canonical_json_bytes(&request)?;
    let answer_bytes = br#"{"type":"question","value":"yes"}"#;
    let ticket = InteractionTicket {
        id: uuid::Uuid::from_bytes([3; 16]),
        kind: InteractionKind::Question,
        request: interaction_file("interactions/request.json", &request_bytes)?,
        deadline_unix_ms: None,
        approval: None,
    };
    let answer = interaction_file("interactions/answer.json", answer_bytes)?;
    let files = BTreeMap::from([
        (MigrationContentStore::key(&ticket.request)?, request_bytes),
        (MigrationContentStore::key(&answer)?, answer_bytes.to_vec()),
    ]);
    let mut aggregate =
        with_content(&client)
            .await?
            .with_content_verifier(Arc::new(MigrationContentStore(Arc::new(
                std::sync::Mutex::new(files),
            ))));
    let resolution = InteractionResolution {
        id: ticket.id,
        expected_version: 1,
        outcome: InteractionOutcome::Answered {
            answer: Box::new(answer),
        },
        detail: None,
    };
    let future = InteractionTicket {
        id: uuid::Uuid::from_bytes([4; 16]),
        deadline_unix_ms: Some(u64::MAX),
        ..ticket.clone()
    };
    let scope = issuer().issue(
        "owner",
        &Capabilities::new([
            "conversation:bind",
            "interaction:open",
            "interaction:resolve",
            ticket.responder_grant().as_str(),
            future.responder_grant().as_str(),
        ]),
        None,
        None,
        &mut None,
    )?;
    for (revision, action) in [
        Action::BindConversation {
            agent: crate::AgentId::from_bytes([8; 16]),
        },
        Action::OpenInteraction {
            ticket: (ticket.clone()).into(),
        },
        Action::ResolveInteraction {
            resolution: (resolution.clone()).into(),
        },
        Action::OpenInteraction {
            ticket: (future.clone()).into(),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let mut next = command(
            u8::try_from(revision).map_err(|error| Error::Invalid(error.to_string()))? + 9,
        )?;
        next.expected_revision = revision as u64;
        next.scope = scope.clone();
        next.action = action;
        aggregate
            .execute(
                next,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    let records = acyclic_stream::Stream::new(client.clone(), authority().stream_path(&mut None)?)?
        .read(0, 4)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    for record in &records {
        for secret in [b"secret prompt".as_slice(), answer_bytes.as_slice()] {
            assert!(
                !record
                    .value
                    .windows(secret.len())
                    .any(|window| window == secret)
            );
        }
    }
    let reopened = with_content(&client).await?;
    assert_eq!(
        reopened
            .reducer()
            .interaction(&ticket.id)
            .and_then(|(_, outcome)| outcome.as_ref()),
        Some(&resolution)
    );
    let mut premature = command(13)?;
    premature.expected_revision = 4;
    premature.scope = scope;
    premature.action = Action::ResolveInteraction {
        resolution: (InteractionResolution {
            id: future.id,
            expected_version: 1,
            outcome: InteractionOutcome::Expired {},
            detail: None,
        })
        .into(),
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

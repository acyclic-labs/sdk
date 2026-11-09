use super::*;
use crate::results::{ResultCursor, ResultCut};

// All positive fork metadata comes from the actual simulator's retained result.
// Malformed protocol controls mutate only that captured original publication.
#[tokio::test]
async fn machines_call_remote_pages_keep_original_cut_credit_and_observations()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use wire::{mutation_outcome::Result as Outcome, recovered_admission::Result as Admission};
    let original = SimulatedMachines::default();
    let create_key = IdempotencyKey::from_digest([41; 32]);
    let key = IdempotencyKey::from_digest([42; 32]);
    let MutationOutcome::Created(parent) = original
        .create(CreateMachine::new(
            create_key,
            Some(Image::custom([7; 32])?),
            [8; 32],
        ))
        .await?
    else {
        unreachable!("original create")
    };
    let outcome = original
        .fork_machine(
            parent.id,
            NonZeroU32::new(1_025).unwrap_or(NonZeroU32::MIN),
            key,
        )
        .await?;
    let MutationOutcome::Forked {
        provenance,
        results,
    } = outcome
    else {
        unreachable!("original fork")
    };
    let admission = wire::MachineAdmissions {
        operation: operation_request(results.operation).operation,
        provenance: Some(provenance.into()),
        count: results.count,
    };
    let terminal = wire::ForkedMachines {
        provenance: admission.provenance.clone(),
        results: Some(results.into()),
    };
    let (mut service, script, _) = scripted_operation_service(Vec::new(), Vec::new());
    script.lock().expect("original script").results = Some(original.clone());
    service.expected_key = key;
    service.expected_operation = results.operation;
    service.recovered = wire::RecoveredAdmission {
        operation: admission.operation.clone(),
        result: Some(Admission::Forked(admission.clone())),
    };
    let mut replies = vec![Some(Outcome::Forked(terminal.clone()))];
    for changed in 0..5 {
        let mut value = terminal.clone();
        let cut = value.results.as_mut().ok_or("original cut missing")?;
        match changed {
            0 => {
                cut.operation =
                    operation_request(original.recover_operation(create_key).await?).operation
            }
            1 => cut.count += 1,
            2 => cut.settlement = Some(wire::result_cut::Settlement::NativeRevision(0)),
            3 => cut.settlement = None,
            _ => value.provenance = None,
        }
        replies.push(Some(Outcome::Forked(value)));
    }
    replies.push(None);
    for reply in &replies {
        let mut state = operation_state(results.operation, wire::OperationStatus::Succeeded);
        state.outcome = reply.clone().map(|result| wire::MutationOutcome {
            result: Some(result),
        });
        script
            .lock()
            .expect("original script")
            .watches
            .push_back(WatchReply::Items(vec![WatchItem::State(state)]));
    }
    let (remote, shutdown, server) = serve_operation_service(service).await?;
    let cursor = ResultCursor {
        cut: results,
        from: 0,
    };
    let credit = NonZeroU32::new(3).unwrap_or(NonZeroU32::MIN);
    let page = original.read_results(cursor, credit).await?;
    #[cfg(feature = "test-support")]
    for observation in &page.machines {
        codec_fuzz::capture_machine(observation)?;
    }
    let child = page.machines.first().ok_or("original child missing")?.id;
    let MutationOutcome::Checkpointed(checkpoint) = original
        .checkpoint(child, IdempotencyKey::from_digest([46; 32]))
        .await?
    else {
        unreachable!("original checkpoint")
    };
    let operation = original.inspect_operation(results.operation).await?;
    #[cfg(feature = "test-support")]
    {
        codec_fuzz::capture_checkpoint(&checkpoint)?;
        codec_fuzz::capture_operation(operation);
    }
    assert_eq!(remote.inspect_checkpoint(checkpoint.id).await?, checkpoint);
    assert_eq!(
        remote.inspect_operation(results.operation).await?,
        operation
    );
    for step in 0..5 {
        match step {
            0 => original.set_online(child, false).await?,
            1 => original.set_online(child, true).await?,
            2 => {
                original
                    .suspend(child, IdempotencyKey::from_digest([43; 32]))
                    .await?;
            }
            3 => {
                original
                    .wake(child, IdempotencyKey::from_digest([44; 32]))
                    .await?;
            }
            _ => {
                original
                    .destroy_machine(child, IdempotencyKey::from_digest([45; 32]))
                    .await?;
            }
        }
        let observation = original.inspect_machine(child).await?;
        #[cfg(feature = "test-support")]
        {
            let bytes = codec_fuzz::capture_machine(&observation)?;
            for end in 0..=bytes.len() {
                codec_fuzz::check("acyclic.machines.v1.MachineState", &bytes[..end]);
            }
        }
        assert_eq!(remote.inspect_machine(child).await?, observation);
    }
    assert_eq!(remote.read_results(cursor, credit).await?, page);
    let next = page.next.ok_or("original continuation missing")?;
    assert_eq!(page.machines.len(), 3);
    assert_eq!(next.from, 3);
    let successor = original.read_results(next, NonZeroU32::MIN).await?;
    assert_eq!(remote.read_results(next, NonZeroU32::MIN).await?, successor);
    assert_eq!(successor.machines.len(), 1);
    assert_eq!(
        successor.next.ok_or("successor continuation missing")?.from,
        4
    );
    assert_eq!(remote.recover(key).await?, outcome);
    for _ in 1..replies.len() {
        assert_eq!(
            remote.recover(key).await,
            Err(ProviderError::Indeterminate(key))
        );
    }
    let mut after = None;
    for _ in 0..2 {
        let page = original.list_machines(after, credit.get()).await?;
        assert_eq!(remote.list_machines(after, credit.get()).await?, page);
        #[cfg(feature = "test-support")]
        {
            use prost::Message as _;
            let bytes = codec_fuzz::capture_machine_page(&page, after, credit.get())?;
            for end in 0..=bytes.len() {
                codec_fuzz::check("acyclic.machines.v1.MachinePage", &bytes[..end]);
            }
            let mut changed = wire::MachinePage::decode(bytes.as_slice())?;
            changed.next = changed
                .machines
                .first()
                .and_then(|machine| machine.machine.clone());
            codec_fuzz::check("acyclic.machines.v1.MachinePage", &changed.encode_to_vec());
        }
        after = page.next;
    }
    assert_eq!(
        script.lock().expect("original script").calls,
        [
            vec!["inspect_checkpoint", "inspect_operation"],
            vec!["inspect_machine"; 5],
            vec!["read_results"; 2],
            vec!["watch"; replies.len()],
            vec!["list_machines"; 2]
        ]
        .concat()
    );
    let _ = shutdown.send(());
    server.await??;

    // The production cut writer and decoder retain the exact settlement owner.
    assert_eq!(
        ResultCut::try_from(wire::ResultCut::from(results))?,
        results
    );
    assert_eq!(
        ResultCursor::try_from(wire::ResultCursor::from(cursor))?,
        cursor
    );
    let end = ResultCursor {
        from: results.count,
        ..cursor
    };
    let page = wire::ResultPage {
        cursor: Some(end.into()),
        machines: Vec::new(),
        next: None,
    };
    assert!(
        decode_result_page(page.clone(), end, NonZeroU32::MIN)?
            .machines
            .is_empty()
    );
    assert!(decode_result_page(page, cursor, NonZeroU32::MIN).is_err());
    let over_credit = wire::ResultPage {
        cursor: Some(cursor.into()),
        machines: vec![wire::MachineState::default(); 2],
        next: None,
    };
    assert!(decode_result_page(over_credit, cursor, NonZeroU32::MIN).is_err());
    let mut invalid = admission;
    invalid.count = 0;
    assert!(decode_fork_admissions(&invalid).is_err());
    invalid.count = results.count;
    invalid.operation = None;
    assert!(decode_fork_admissions(&invalid).is_err());
    Ok(())
}

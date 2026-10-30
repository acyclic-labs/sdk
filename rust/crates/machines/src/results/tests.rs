use super::*;
use crate::{
    CreateMachine, IdempotencyKey, Image, MachineState, MachinesProvider as _, MutationOutcome,
    SimulatedMachines,
};
use acyclic_stream::{
    AppendOutcome, AppendRequest, CommitOutcome, CommitRequest, IdempotencyKey as StreamKey,
    LocalStream, LocalStreamLimits,
};
use futures::FutureExt as _;

async fn append(
    streams: &LocalStream,
    source: &StreamPath,
    machine: &MachineObservation,
    from: u64,
    count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let records = (0..count)
        .map(|index| {
            let mut machine = machine.clone();
            let mut identity = [7; 32];
            identity[..8].copy_from_slice(&(from + u64::try_from(index).unwrap()).to_le_bytes());
            machine.id = crate::MachineId::from_digest(identity);
            record(&machine)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let result = streams
        .append(AppendRequest {
            path: source.clone(),
            records,
            if_tail: None,
            idempotency_key: StreamKey::unique(),
        })
        .await?;
    assert!(matches!(result, AppendOutcome::Committed(_)));
    Ok(())
}

#[tokio::test]
async fn settled_pages_pin_original_cut_across_source_changes_and_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let streams =
        Arc::new(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?);
    let MutationOutcome::Created(mut machine) = SimulatedMachines::default()
        .create(CreateMachine::new(
            IdempotencyKey::new(),
            Some(Image::custom([7; 32])?),
            [8; 32],
        ))
        .await?
    else {
        panic!("original creation must settle");
    };
    let root = StreamPath::new("machines-results")?;
    let source = StreamPath::new("staging/original")?;
    let operation = OperationId::new();
    // Several preparation appends, not a transport-sized terminal result array.
    append(&streams, &source, &machine, 0, 513).await?;
    append(&streams, &source, &machine, 513, 511).await?;
    machine.state = MachineState::Failed;
    append(&streams, &source, &machine, 1024, 1).await?;
    let count = 1025;
    let (condition, mutation) = publication(
        &root,
        operation,
        source.clone(),
        NonZeroU64::new(count).unwrap(),
    )?;
    let terminal = StreamPath::new(format!("{root}/operations/{operation}"))?;
    let request = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![condition],
        mutations: vec![
            mutation,
            CommitMutation::Append {
                path: terminal.clone(),
                records: vec![AppendRecord::from("original terminal success; count=1025")],
            },
        ],
        idempotency_key: StreamKey::unique(),
    };
    let mut beyond_source = request.clone();
    beyond_source.idempotency_key = StreamKey::unique();
    let CommitMutation::Fork { at_tail, .. } = &mut beyond_source.mutations[0] else {
        panic!("results must share the original staged prefix");
    };
    *at_tail += 1;
    assert!(streams.commit(beyond_source).await.is_err());
    assert!(streams.head(terminal.clone()).await.is_err());
    // Source mutation cannot change the already selected immutable prefix.
    machine.state = MachineState::Destroyed;
    append(&streams, &source, &machine, 1025, 1).await?;
    let CommitOutcome::Committed(receipt) = streams.commit(request.clone()).await? else {
        panic!("original terminal publication must commit atomically");
    };
    let settlement = receipt.commit_id;
    // Capture real provider output. Withhold only EOF: a page must finish at
    // its credit, while the former collect-to-EOF implementation stays pending.
    let mut stored = streams
        .read(ReadRequest {
            path: source.clone(),
            from: 0,
            limit: 1,
            at_least: Default::default(),
            key: None,
        })
        .await?;
    let original = stored.next().await.unwrap()?;
    drop(stored);
    let pending_eof = futures::stream::iter([Ok(original.clone())])
        .chain(futures::stream::pending())
        .boxed();
    let page = observations(pending_eof, 0, 1).now_or_never();
    assert!(matches!(page, Some(Ok(values)) if values.len() == 1));
    // Closed producer before credit is filled must return a typed error now.
    let closed = futures::stream::iter([Ok(original)]).boxed();
    assert!(matches!(
        observations(closed, 0, 2).now_or_never(),
        Some(Err(_))
    ));
    assert_eq!(
        streams.head(terminal).await?.last.unwrap().commit_id,
        settlement
    );
    append(&streams, &source, &machine, 1026, 1).await?;
    // Lost acknowledgment: a changed source cannot replace the original receipt.
    assert_eq!(
        streams.commit(request).await?,
        CommitOutcome::Committed(receipt)
    );
    let reader = SettledResults::open(streams.clone(), &root, operation, settlement, count).await?;
    let first = reader.page(0, NonZeroU32::new(1).unwrap()).await?;
    assert_eq!(first.machines.len(), 1);
    assert_eq!(first.machines[0].state, MachineState::Running);
    assert_eq!(first.next, Some(1));
    drop(reader);
    drop(streams);

    let streams =
        Arc::new(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?);
    let reader = SettledResults::open(streams.clone(), &root, operation, settlement, count).await?;
    // The former direct-credit request fails on the actual Streams boundary.
    assert!(matches!(
        streams
            .read(ReadRequest {
                path: path(&root, operation)?,
                from: 0,
                limit: u32::MAX,
                at_least: Default::default(),
                key: None,
            })
            .await,
        Err(acyclic_stream::StreamError::LimitExceeded)
    ));
    // More credit must still return a legal chunk and its exact continuation.
    let wide = reader.page(0, NonZeroU32::new(u32::MAX).unwrap()).await?;
    assert_eq!(wide.machines.len(), acyclic_stream::MAX_ITEMS);
    assert_eq!(wide.next, Some(u64::try_from(acyclic_stream::MAX_ITEMS)?));
    let rest = reader
        .page(wide.next.unwrap(), NonZeroU32::new(u32::MAX).unwrap())
        .await?;
    assert_eq!(rest.machines.len(), 1);
    assert_eq!(rest.machines[0].state, MachineState::Failed);
    assert_eq!(rest.next, None);
    let last = reader.page(1024, NonZeroU32::new(64).unwrap()).await?;
    assert_eq!(last.operation, operation);
    assert_eq!(last.settlement, settlement);
    assert_eq!(last.from, 1024);
    assert_eq!(last.machines.len(), 1);
    assert_eq!(last.machines[0].state, MachineState::Failed);
    assert_eq!(last.next, None);
    let mut from = 0;
    loop {
        let page = reader.page(from, NonZeroU32::new(63).unwrap()).await?;
        assert_eq!(page.from, from);
        assert!(page.machines.len() <= 63);
        for (index, machine) in page.machines.iter().enumerate() {
            let sequence = from + u64::try_from(index)?;
            let mut identity = [7; 32];
            identity[..8].copy_from_slice(&sequence.to_le_bytes());
            assert_eq!(machine.id, crate::MachineId::from_digest(identity));
        }
        from += u64::try_from(page.machines.len())?;
        match page.next {
            Some(next) => assert_eq!(next, from),
            None => break,
        }
    }
    assert_eq!(from, count, "all original results must remain reachable");
    assert!(
        reader
            .page(count, NonZeroU32::new(1).unwrap())
            .await?
            .machines
            .is_empty()
    );
    assert!(
        reader
            .page(count + 1, NonZeroU32::new(1).unwrap())
            .await
            .is_err()
    );
    // Wrong-operation and changed-publication negatives use real retained data.
    assert!(
        SettledResults::open(
            streams.clone(),
            &root,
            OperationId::new(),
            settlement,
            count,
        )
        .await
        .is_err()
    );
    assert!(
        SettledResults::open(
            streams.clone(),
            &root,
            operation,
            CommitId::from_bytes([9; 32]),
            count,
        )
        .await
        .is_err()
    );
    assert!(
        SettledResults::open(streams, &root, operation, settlement, count - 1)
            .await
            .is_err()
    );
    Ok(())
}

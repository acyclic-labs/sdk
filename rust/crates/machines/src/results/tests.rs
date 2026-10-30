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
use proptest::prelude::*;

proptest! {
    #[test]
    fn result_cursor_preserves_owner_and_bounded_progress(
        count in any::<u64>(),
        from in any::<u64>(),
        allowance in 1..=u32::MAX,
        revision in 1..=u64::MAX,
    ) {
        let cut = ResultCut {
            operation: OperationId(uuid::Uuid::from_bytes([7; 16])),
            settlement: Settlement::NativeController(NonZeroU64::new(revision).unwrap()),
            count,
        };
        let cursor = ResultCursor { cut, from };
        let credit = NonZeroU32::new(allowance).unwrap();
        let interval = cursor.interval(cut, credit);
        if from > count {
            prop_assert!(interval.is_err());
        } else {
            let (range, next) = interval.unwrap();
            prop_assert_eq!(range.start, from);
            prop_assert_eq!(range.end - from, (count - from).min(u64::from(allowance)));
            prop_assert!(range.end <= count);
            prop_assert_eq!(next.is_some(), range.end < count);
            if let Some(next) = next {
                prop_assert_eq!(next.cut, cut);
                prop_assert_eq!(next.from, range.end);
                prop_assert!(next.from > from);
            }
        }
        // A revision cast into commit bytes would erase the actual owner. The
        // same operation/index/extent must still refuse that substitution.
        let mut cast = [0; 32];
        cast[..8].copy_from_slice(&revision.to_le_bytes());
        let substituted = ResultCursor {
            cut: ResultCut { settlement: Settlement::Streams(CommitId::from_bytes(cast)), ..cut },
            from: from.min(count),
        };
        prop_assert!(substituted.interval(cut, credit).is_err());
        // Owner-erasing comparison is the failing negative control.
        prop_assert_eq!(substituted.cut.operation, cut.operation);
        prop_assert_eq!(u64::from_le_bytes(cast[..8].try_into().unwrap()), revision);
    }
}

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
    let first = reader
        .page(reader.cursor(), NonZeroU32::new(1).unwrap())
        .await?;
    assert_eq!(first.machines.len(), 1);
    assert_eq!(first.machines[0].state, MachineState::Running);
    assert_eq!(first.next.unwrap().from, 1);
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
    let cursor = reader.cursor();
    let wide = reader
        .page(cursor, NonZeroU32::new(u32::MAX).unwrap())
        .await?;
    assert_eq!(wide.machines.len(), acyclic_stream::MAX_ITEMS);
    assert_eq!(
        wide.next.unwrap().from,
        u64::try_from(acyclic_stream::MAX_ITEMS)?
    );
    let rest = reader
        .page(wide.next.unwrap(), NonZeroU32::new(u32::MAX).unwrap())
        .await?;
    assert_eq!(rest.machines.len(), 1);
    assert_eq!(rest.machines[0].state, MachineState::Failed);
    assert_eq!(rest.next, None);
    let last = reader
        .page(
            ResultCursor {
                from: 1024,
                ..cursor
            },
            NonZeroU32::new(64).unwrap(),
        )
        .await?;
    assert_eq!(last.cursor.cut.operation, operation);
    assert_eq!(last.cursor.cut.settlement, Settlement::Streams(settlement));
    assert_eq!(last.cursor.from, 1024);
    assert_eq!(last.machines.len(), 1);
    assert_eq!(last.machines[0].state, MachineState::Failed);
    assert_eq!(last.next, None);
    let mut from = 0;
    loop {
        let page = reader
            .page(
                ResultCursor { from, ..cursor },
                NonZeroU32::new(63).unwrap(),
            )
            .await?;
        assert_eq!(page.cursor.from, from);
        assert!(page.machines.len() <= 63);
        for (index, machine) in page.machines.iter().enumerate() {
            let sequence = from + u64::try_from(index)?;
            let mut identity = [7; 32];
            identity[..8].copy_from_slice(&sequence.to_le_bytes());
            assert_eq!(machine.id, crate::MachineId::from_digest(identity));
        }
        from += u64::try_from(page.machines.len())?;
        match page.next {
            Some(next) => assert_eq!(next.from, from),
            None => break,
        }
    }
    assert_eq!(from, count, "all original results must remain reachable");
    assert!(
        reader
            .page(
                ResultCursor {
                    from: count,
                    ..cursor
                },
                NonZeroU32::new(1).unwrap()
            )
            .await?
            .machines
            .is_empty()
    );
    assert!(
        reader
            .page(
                ResultCursor {
                    from: count + 1,
                    ..cursor
                },
                NonZeroU32::new(1).unwrap()
            )
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
    // The same actual retained observations can belong to a native controller
    // cut, without staging a second Streams publication or copying the array.
    let native_revision = NonZeroU64::new(17).unwrap();
    let native = NativeResults::open(operation, native_revision, &wide.machines)?;
    let original = native.cursor();
    let borrowed = native.page(original, NonZeroU32::new(1).unwrap())?;
    assert_eq!(borrowed.machines.as_ptr(), wide.machines.as_ptr());
    assert_eq!(borrowed.machines.len(), 1);
    assert_eq!(borrowed.next.unwrap().from, 1);
    // A whole-result clone fails the original-allocation oracle.
    let copied = wide.machines.clone();
    assert_ne!(copied.as_ptr(), borrowed.machines.as_ptr());
    let wrong_owner = ResultCursor {
        cut: ResultCut {
            settlement: cursor.cut.settlement,
            ..original.cut
        },
        ..original
    };
    assert!(
        native
            .page(wrong_owner, NonZeroU32::new(1).unwrap())
            .is_err()
    );
    let wrong_owner = ResultCursor {
        cut: ResultCut {
            settlement: original.cut.settlement,
            ..cursor.cut
        },
        ..cursor
    };
    assert!(
        reader
            .page(wrong_owner, NonZeroU32::new(1).unwrap())
            .await
            .is_err()
    );
    for cut in [
        ResultCut {
            operation: OperationId::new(),
            ..original.cut
        },
        ResultCut {
            settlement: Settlement::NativeController(NonZeroU64::new(18).unwrap()),
            ..original.cut
        },
        ResultCut {
            count: original.cut.count - 1,
            ..original.cut
        },
    ] {
        assert!(
            native
                .page(
                    ResultCursor { cut, ..original },
                    NonZeroU32::new(1).unwrap()
                )
                .is_err()
        );
    }
    // Dropping page interest changes no result. Reopening the same original
    // revision resumes exactly; no producer wait, cancellation or redispatch.
    let resumed = borrowed.next.unwrap();
    let reopened = NativeResults::open(operation, native_revision, &wide.machines)?;
    let remaining = reopened.page(resumed, NonZeroU32::new(u32::MAX).unwrap())?;
    assert_eq!(remaining.machines, &wide.machines[1..]);
    assert_eq!(remaining.machines.as_ptr(), wide.machines[1..].as_ptr());
    assert!(remaining.next.is_none());
    Ok(())
}

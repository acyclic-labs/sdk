use super::*;
use crate::foundation::{DurableCommit, authority_commit_digest};
use crate::simulation::{SimulatedStore, Simulation, SimulationFault, sites};
use crate::{
    AppendGroupOutcome, AppendOutcome, AuthorityStoreError, AuthorityWork, GuardedAppend,
    LogicalObjectStore, PublicationPermit, ReplayLimit, StreamAuthorityStore,
};
use acyclic_native_runtime::testing::{Schedule, drive, fill_seeded};
use acyclic_stream::MemoryStream;
use futures::FutureExt;
use futures::future::{Either, select};
use std::panic::AssertUnwindSafe;

type Store = SimulatedStore<StreamAuthorityStore<MemoryStream>>;
type Owner = ForeignOwner<Store>;

#[tokio::test]
async fn seeded_group_append_world() -> TestResult {
    const REPLAY: &str = "FS_GROUP_DST_SEED";
    let replay = std::env::var(REPLAY)
        .ok()
        .map(|seed| seed.parse())
        .transpose()?;
    for seed in replay.map_or(0..=31, |seed: u64| seed..=seed) {
        let (schedule, mut boundaries) = Schedule::seeded(seed);
        let run = drive(&schedule, &mut boundaries, |_| {}, async {
            if let Err(error) = world(&schedule).await {
                panic!("{error}");
            }
        });
        if let Err(error) = AssertUnwindSafe(Box::pin(run)).catch_unwind().await {
            let message = error
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| error.downcast_ref::<&str>().copied())
                .unwrap_or("world panicked");
            panic!(
                "replay: bazel test //rust/crates/filesystem:acyclic_fs_test_internal --test_filter=seeded_group_append_world --test_env={REPLAY}={seed}\n{message}"
            );
        }
    }
    Ok(())
}

async fn world(schedule: &Schedule) -> TestResult {
    let mut choices = [0; 4];
    assert!(fill_seeded(&mut choices));
    let cancel = CancellationToken::new();
    let (volume, mut setup) = fixture(serialized_config(), &cancel).await?;
    for name in ["a", "b", "c", "d"] {
        create(&mut setup, name, OwnedBytes::from_static(b"old"), &cancel).await?;
    }
    commit(&mut setup, &cancel).await?;
    let owner = || {
        Simulation::wrap(
            schedule,
            StreamAuthorityStore::new(volume.fs.inner.authority.provider()),
            LogicalObjectStore::new(Arc::clone(volume.fs.inner.objects.provider())),
        )
        .stores()
    };
    let (first, objects) = owner();
    let (second, _) = owner();
    let stores = [first, second];
    group_history(schedule, &stores, choices, &cancel).await?;
    combiner_history(schedule, (stores, objects), volume.id(), choices, &cancel).await
}

fn lose_next_append(schedule: &Schedule) {
    schedule.inject_next(
        sites::AUTHORITY_APPEND,
        SimulationFault::AmbiguousAuthorityAppend,
    );
}

fn head(record: &DurableCommit) -> Head {
    Head {
        epoch: record.epoch,
        sequence: record.sequence,
        digest: record.digest,
    }
}

fn predicted(request: &GuardedAppend) -> TestResult<DurableCommit> {
    let expected = request.expected;
    let commit = &request.commit;
    let sequence = expected.sequence.checked_next()?;
    let digest = authority_commit_digest(
        request.authority_id,
        request.epoch,
        sequence,
        commit.operation_id,
        commit.fingerprint,
        expected.digest,
        &commit.payload,
    );
    Ok(DurableCommit {
        epoch: request.epoch,
        sequence,
        operation_id: commit.operation_id,
        fingerprint: commit.fingerprint,
        previous_digest: expected.digest,
        digest,
        payload: commit.payload.clone(),
        settled_at_micros: None,
    })
}

async fn replay(
    store: &Store,
    authority: AuthorityId,
    cancel: &CancellationToken,
) -> TestResult<Vec<DurableCommit>> {
    let mut work = AuthorityWork::new(B, cancel);
    let (mut records, _) = store
        .replay(
            authority,
            Sequence::GENESIS,
            ReplayLimit {
                records: 32,
                payload_bytes: 65_536,
            },
            &mut work,
        )
        .await?;
    for record in &mut records {
        record.settled_at_micros = None;
    }
    Ok(records)
}

async fn group_history(
    schedule: &Schedule,
    stores: &[Store; 2],
    choices: [u8; 4],
    cancel: &CancellationToken,
) -> TestResult {
    let stores = [
        &stores[usize::from(choices[0] & 1)],
        &stores[usize::from(choices[0] & 1 ^ 1)],
    ];
    let id = AuthorityId::from_bytes([choices[1]; 16]);
    let genesis = Head::genesis(Epoch::GENESIS);
    let mut work = AuthorityWork::new(B, cancel);
    stores[0]
        .create_authority(id, genesis.epoch, &mut work, None::<&mut ()>)
        .await?;
    let request = |operation, expected: Head| GuardedAppend {
        authority_id: id,
        epoch: expected.epoch,
        expected,
        commit: crate::ProposedCommit {
            operation_id: OperationId::from_bytes([operation; 16]),
            fingerprint: expected.digest,
            payload: bytes::Bytes::from(vec![operation, choices[3]]),
            publication: None,
        },
        permit: PublicationPermit::Unrestricted,
    };
    let original = request(1, genesis);
    lose_next_append(schedule);
    let lost = stores[0]
        .compare_and_append_group(
            std::slice::from_ref(&original),
            &[],
            &mut work,
            None::<&mut ()>,
        )
        .await;
    assert!(matches!(
        lost,
        Err(crate::AuthorityError::Authority(
            AuthorityStoreError::Indeterminate { .. }
        ))
    ));

    let mut history = vec![predicted(&original)?];
    assert_eq!(replay(stores[1], id, cancel).await?, history);
    let mut tip = head(&history[0]);
    let mut fresh = Vec::new();
    for operation in 2..=2 + choices[1] % 3 {
        let member = request(operation, tip);
        tip = head(&predicted(&member)?);
        fresh.push(member);
    }
    let mut retry = original.clone();
    let members = if choices[2] & 1 == 0 {
        // Recovery at the transaction's locator check, after a fresh prefix.
        retry.expected = tip;
        fresh.push(retry);
        fresh
    } else {
        // Recovery at preparation, before a fresh chain.
        fresh.insert(0, retry);
        fresh
    };
    check_group(stores[1], id, &members, &mut history, cancel).await?;

    let mut changed = original;
    changed.expected = tip;
    changed.commit.fingerprint = tip.digest;
    check_group(stores[0], id, &[changed], &mut history, cancel).await?;
    check_group(stores[1], id, &[request(5, genesis)], &mut history, cancel).await
}

async fn check_group(
    store: &Store,
    authority: AuthorityId,
    members: &[GuardedAppend],
    history: &mut Vec<DurableCommit>,
    cancel: &CancellationToken,
) -> TestResult {
    let mut work = AuthorityWork::new(B, cancel);
    let actual = store
        .compare_and_append_group(members, &[], &mut work, None::<&mut ()>)
        .await?;
    let receipts = match actual {
        AppendGroupOutcome::Published { receipts, .. } => receipts,
        AppendGroupOutcome::Rejected {
            index: 0, outcome, ..
        } if members.len() == 1 => vec![outcome],
        AppendGroupOutcome::Rejected { index, outcome, .. } => {
            return Err(format!("group refused at member {index}: {outcome:?}").into());
        }
    };
    assert_eq!(receipts.len(), members.len());
    for (member, mut receipt) in members.iter().zip(receipts) {
        let actual_head = head(history.last().ok_or("missing durable original")?);
        let expected = match history
            .iter()
            .find(|record| record.operation_id == member.commit.operation_id)
        {
            Some(record) if record.fingerprint == member.commit.fingerprint => {
                AppendOutcome::AlreadyCommitted(record.clone())
            }
            Some(record) => AppendOutcome::IdempotencyConflict {
                committed_fingerprint: record.fingerprint,
            },
            None if member.expected == actual_head => {
                let record = predicted(member)?;
                history.push(record.clone());
                AppendOutcome::Committed(record)
            }
            None => AppendOutcome::Conflict {
                actual: actual_head,
            },
        };
        if let AppendOutcome::Committed(record) | AppendOutcome::AlreadyCommitted(record) =
            &mut receipt
        {
            record.settled_at_micros = None;
        }
        assert_eq!(receipt, expected, "member {member:?}");
    }
    assert_eq!(replay(store, authority, cancel).await?, *history);
    Ok(())
}

type Objects = SimulatedStore<LogicalObjectStore<acyclic_objects::MemoryObjects>>;

async fn combiner_history(
    schedule: &Schedule,
    backends: ([Store; 2], Objects),
    volume_id: crate::VolumeId,
    choices: [u8; 4],
    cancel: &CancellationToken,
) -> TestResult {
    let ([first, second], objects) = backends;
    let gate = Arc::new(DispatchGate::default());
    let make_owner = |store, dispatch_gate| {
        Fs::new(
            Owner {
                inner: Arc::new(store),
                claims: Default::default(),
                forbid_probe: Default::default(),
                racing_operation: None,
                forbid_replay: Default::default(),
                dispatch_gate,
                _retirement: None,
            },
            objects.clone(),
            EmbeddedCapabilities::MEMORY,
        )
    };
    let writer = make_owner(first.clone(), None)
        .open_volume(volume_id, B, cancel)
        .await?
        .value;
    let observer = make_owner(second.clone(), Some(Arc::clone(&gate)))
        .open_volume(volume_id, B, cancel)
        .await?
        .value;
    let authority = crate::kernel::volume_authority_id(volume_id);
    let mut history = replay(&first, authority, cancel).await?;
    let names = if choices[3] & 1 == 0 {
        ["a", "b", "c"]
    } else {
        ["c", "a", "b"]
    };
    let [lead, retried, successor, extra] = std::array::from_fn(|_| OperationId::new());
    let mut leading = edit(&observer, names[0], cancel).await?;
    let mut original = edit(&writer, names[1], cancel).await?;
    let mut retry = edit(&observer, names[1], cancel).await?;
    let mut fresh = edit(&observer, names[2], cancel).await?;
    let mut extra_fresh = edit(&observer, "d", cancel).await?;
    let mut held = Box::pin(submit_proposal(&mut leading, lead, false, cancel));
    assert!(
        matches!(
            select(Box::pin(gate.started.notified()), held.as_mut()).await,
            Either::Left(_)
        ),
        "leading proposal did not reach the dispatch gate"
    );
    assert_eq!(replay(&first, authority, cancel).await?, history);

    let use_extra = choices[0] & 2 != 0;
    let mut next = Box::pin(async {
        futures::join!(
            submit_proposal(&mut retry, retried, false, cancel),
            submit_proposal(&mut fresh, successor, false, cancel),
            async {
                if use_extra {
                    submit_proposal(&mut extra_fresh, extra, false, cancel)
                        .await
                        .map(Some)
                } else {
                    Ok(None)
                }
            }
        )
    });
    assert!(futures::poll!(next.as_mut()).is_pending());
    gate.release.notify_one();
    assert!(
        matches!(
            select(Box::pin(gate.started.notified()), next.as_mut()).await,
            Either::Left(_)
        ),
        "successor group did not reach the dispatch gate"
    );
    assert_eq!(
        gate.members.load(Ordering::SeqCst),
        2 + usize::from(use_extra),
        "held dispatch must contain the retry and its fresh successors"
    );
    check_publications(
        &first,
        authority,
        &mut history,
        &[(lead, Some(held.await?))],
        cancel,
    )
    .await?;

    // Lose the original's receipt after the other owner's entire successor
    // group is prepared. Its retry must be resolved by grouped admission.
    lose_next_append(schedule);
    let lost = original
        .commit(
            retried,
            crate::PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
            B,
            cancel,
            allow::<FsError>,
        )
        .await;
    assert!(
        matches!(
            lost,
            Err(failure) if matches!(failure.error, FsError::Authority(AuthorityStoreError::Indeterminate { .. }))
        ),
        "the original did not observe the scheduled lost acknowledgement"
    );
    check_publications(&first, authority, &mut history, &[(retried, None)], cancel).await?;
    let original_head = head(history.last().ok_or("missing lost-ack record")?);

    gate.release.notify_one();
    let Either::Right(((retry, fresh, extra_fresh), _)) =
        select(Box::pin(gate.started.notified()), next.as_mut()).await
    else {
        return Err("the combiner dispatched the successor group in multiple batches".into());
    };
    assert!(
        matches!(retry?, LivePublicationObservation::AlreadyCommitted { head, .. } if head == original_head)
    );
    let mut committed = vec![(successor, Some(fresh?))];
    if let Some(outcome) = extra_fresh? {
        committed.push((extra, Some(outcome)));
    }
    check_publications(&second, authority, &mut history, &committed, cancel).await?;

    // Apply the original edit to a checkout on the new base, keeping its ID.
    let mut changed = edit(&writer, names[1], cancel).await?;
    changed
        .change_content(
            path(names[1])?,
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"retry on another base"),
            },
            ContentTimes::Preserve,
            B,
            cancel,
        )
        .await?;
    let changed = submit_proposal(&mut changed, retried, false, cancel).await?;
    let original = history
        .iter()
        .find(|record| record.operation_id == retried)
        .ok_or("missing original")?;
    assert_eq!(
        changed,
        LivePublicationObservation::IdempotencyConflict {
            committed_fingerprint: original.fingerprint,
        }
    );
    assert_eq!(replay(&second, authority, cancel).await?, history);
    Ok(())
}

async fn check_publications(
    store: &Store,
    authority: AuthorityId,
    history: &mut Vec<DurableCommit>,
    expected: &[(OperationId, Option<LivePublicationObservation>)],
    cancel: &CancellationToken,
) -> TestResult {
    let actual = replay(store, authority, cancel).await?;
    assert_eq!(actual.len(), history.len() + expected.len());
    assert_eq!(&actual[..history.len()], history.as_slice());
    for ((operation, outcome), record) in expected.iter().zip(&actual[history.len()..]) {
        assert!(!history.iter().any(|prior| prior.operation_id == *operation));
        assert_eq!(record.operation_id, *operation);
        let previous = head(
            history
                .last()
                .ok_or("missing previous filesystem generation")?,
        );
        assert_eq!(record.previous_digest, previous.digest);
        assert_eq!(record.sequence, previous.sequence.checked_next()?);
        assert_eq!(
            record.digest,
            authority_commit_digest(
                authority,
                record.epoch,
                record.sequence,
                *operation,
                record.fingerprint,
                record.previous_digest,
                &record.payload,
            )
        );
        if let Some(outcome) = outcome {
            assert!(
                matches!(outcome, LivePublicationObservation::Committed { head: received, .. }
                if *received == head(record)),
                "expected a committed member: {outcome:?}"
            );
        }
        history.push(record.clone());
    }
    Ok(())
}

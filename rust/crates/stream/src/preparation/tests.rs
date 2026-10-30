use super::*;
use std::cell::Cell;

#[test]
fn backend_conditions_exhaust_presence_tail_and_forged_head_without_unneeded_reads()
-> Result<(), StreamError> {
    let path = StreamPath::new("facts")?;
    let mut absence_control_failed = false;
    let mut head_control_failed = false;
    for actual in [None, Some(0), Some(1), Some(2)] {
        for expected in 0..=2 {
            for matching_head in [false, true] {
                let expected_head = CommitId::from_bytes([1; 32]);
                let actual_head = CommitId::from_bytes([u8::from(matching_head); 32]);
                let conditions = [
                    CommitCondition::Tail {
                        path: path.clone(),
                        expected,
                    },
                    CommitCondition::Head {
                        path: path.clone(),
                        expected,
                        commit_id: expected_head,
                    },
                    CommitCondition::Absent { path: path.clone() },
                    CommitCondition::Present { path: path.clone() },
                ];
                let tail_matches = actual == Some(expected);
                let accepted = [
                    actual.unwrap_or(0) == expected,
                    tail_matches && matching_head,
                    actual.is_none(),
                    actual.is_some(),
                ];
                for (index, (condition, accepted)) in conditions.iter().zip(accepted).enumerate() {
                    let reads = Cell::new(0);
                    let conflict = condition_conflict(condition, actual, || {
                        reads.set(reads.get() + 1);
                        Ok(actual_head)
                    })?;
                    assert_eq!(conflict.is_none(), accepted);
                    assert_eq!(reads.get(), usize::from(index == 1 && tail_matches));
                    if index == 0 {
                        // Old presence-sensitive Tail rejects first writes at zero.
                        absence_control_failed |= (actual == Some(expected)) != accepted;
                    }
                    if index == 1 {
                        // A matching sequence is not proof of a matching head.
                        head_control_failed |= tail_matches != accepted;
                        if tail_matches && !matching_head {
                            assert_eq!(
                                conflict,
                                Some(CommitConflict::Head {
                                    path: path.clone(),
                                    expected: expected_head,
                                    actual: actual_head,
                                })
                            );
                        }
                    }
                }
            }
        }
    }
    assert!(
        absence_control_failed,
        "presence-sensitive Tail control escaped the state space"
    );
    assert!(
        head_control_failed,
        "tail-only Head control escaped the state space"
    );
    Ok(())
}

#[test]
fn backend_identity_and_time_survive_working_set_replacement() {
    let digest = [17; 32];
    let mut identities = std::collections::BTreeSet::new();
    let mut reset_control_duplicates = 0;
    let mut previous = 0;
    let mut clock_control_regresses = false;
    for decision in 1..=1_000 {
        let id = commit_identity(decision, digest);
        assert!(identities.insert(id));
        // A freshly seeded per-operation state would reset its global ordinal.
        reset_control_duplicates += usize::from(commit_identity(1, digest) == id);
        let sampled_millis = if decision % 2 == 0 { 1 } else { decision };
        let time = committed_time(previous, sampled_millis);
        assert!(time >= previous);
        assert!(time >= sampled_millis * 1_000);
        clock_control_regresses |= sampled_millis * 1_000 < previous;
        previous = time;
    }
    assert_eq!(reset_control_duplicates, 1);
    assert!(clock_control_regresses);
    assert_eq!(committed_time(0, u64::MAX), u64::MAX);
    assert_eq!(committed_time(u64::MAX, 0), u64::MAX);
}

#[test]
fn backend_facts_preserve_precommit_cuts_order_and_record_identity() -> Result<(), StreamError> {
    let source = StreamPath::new("source")?;
    let destination = StreamPath::new("destination")?;
    let exclusions = vec![RelativePath::new("private")?];
    let bodies = vec![AppendRecord::from(vec![1]), AppendRecord::from(vec![2])];
    let mutations = vec![
        CommitMutation::Append {
            path: source.clone(),
            records: bodies.clone(),
        },
        CommitMutation::Fork {
            exclude: exclusions.clone(),
            source: source.clone(),
            destination: destination.clone(),
            at_tail: 1,
            records: bodies.clone(),
        },
    ];
    let identity = commit_identity(73, [19; 32]);
    let tail_reads = Cell::new(0);
    let output = facts(
        &mutations,
        |path| {
            assert_eq!(path, &source);
            tail_reads.set(tail_reads.get() + 1);
            Ok(Some(4))
        },
        |origin, start, bodies, exclude| {
            records(
                origin,
                start,
                bodies,
                identity,
                9_000,
                Vec::with_capacity(bodies.len()),
            )
            .map(|records| (records, exclude.to_vec()))
        },
        Vec::with_capacity(mutations.len()),
    )?;
    assert_eq!(
        tail_reads.get(),
        1,
        "a fork uses its validated pre-commit cut"
    );
    assert_eq!(
        output,
        vec![
            CommittedMutation::Append(append_fact(
                source.clone(),
                4,
                records(&source, 4, &bodies, identity, 9_000, Vec::with_capacity(2))?
            )?),
            CommittedMutation::Fork(CommittedFork {
                // Evaluated before `destination` moves into the fact.
                records: records(
                    &destination,
                    1,
                    &bodies,
                    identity,
                    9_000,
                    Vec::with_capacity(2),
                )?,
                exclude: exclusions,
                source: source.clone(),
                destination,
                forked_at: 1,
                tail: 3,
            }),
        ]
    );
    // Seeding a fresh per-operation state would assign sequence zero to append.
    let reset = facts(
        &mutations,
        |_| Ok(None),
        |origin, start, bodies, exclude| {
            records(
                origin,
                start,
                bodies,
                identity,
                9_000,
                Vec::with_capacity(bodies.len()),
            )
            .map(|records| (records, exclude.to_vec()))
        },
        Vec::with_capacity(2),
    )?;
    assert_ne!(reset, output);
    assert_eq!(
        facts(
            &mutations,
            |_| Ok(Some(4)),
            |_, _, _, _| Err(StreamError::Unavailable),
            Vec::new()
        ),
        Err(StreamError::InvalidArgument),
        "the planner must not grow an unfunded output"
    );
    assert_eq!(
        records(&source, 0, &bodies, identity, 9_000, Vec::with_capacity(1)),
        Err(StreamError::InvalidArgument),
        "record construction must use admitted slots"
    );
    Ok(())
}

#[test]
fn backend_authority_distinguishes_missing_history_from_failed_storage() -> Result<(), StreamError>
{
    let source = StreamPath::new("source")?;
    let request = CommitRequest {
        deadline_unix_millis: None,
        conditions: Vec::new(),
        mutations: vec![CommitMutation::Fork {
            exclude: Vec::new(),
            source: source.clone(),
            destination: StreamPath::new("destination")?,
            at_tail: 3,
            records: Vec::new(),
        }],
        idempotency_key: crate::IdempotencyKey::new(vec![1])?,
    };
    assert_eq!(
        authority(&request, |_| Err(StreamError::Unavailable)),
        Err(StreamError::Unavailable)
    );
    assert_eq!(
        authority(&request, |_| Ok(None)),
        Err(StreamError::NotFound)
    );
    assert_eq!(
        authority(&request, |_| Ok(Some(2))),
        Err(StreamError::PrefixNotRetained)
    );
    assert_eq!(
        authority(&request, |path| {
            assert_eq!(path, &source);
            Ok(Some(3))
        }),
        Ok(())
    );
    // The missing-on-error control would disguise a disk failure as NotFound.
    assert_ne!(
        authority(&request, |_| Err(StreamError::Unavailable)),
        authority(&request, |_| Ok(None))
    );
    Ok(())
}

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
        &mutations.iter().collect::<Vec<_>>(),
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
        &mutations.iter().collect::<Vec<_>>(),
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
            &mutations.iter().collect::<Vec<_>>(),
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
    let fork = CommitMutation::Fork {
        exclude: Vec::new(),
        source: source.clone(),
        destination: StreamPath::new("destination")?,
        at_tail: 3,
        records: Vec::new(),
    };
    let request = [&fork];
    assert_eq!(
        authority(&request[..], |_| Err(StreamError::Unavailable)),
        Err(StreamError::Unavailable)
    );
    assert_eq!(
        authority(&request[..], |_| Ok(None)),
        Err(StreamError::NotFound)
    );
    assert_eq!(
        authority(&request[..], |_| Ok(Some(2))),
        Err(StreamError::PrefixNotRetained)
    );
    assert_eq!(
        authority(&request[..], |path| {
            assert_eq!(path, &source);
            Ok(Some(3))
        }),
        Ok(())
    );
    // The missing-on-error control would disguise a disk failure as NotFound.
    assert_ne!(
        authority(&request[..], |_| Err(StreamError::Unavailable)),
        authority(&request[..], |_| Ok(None))
    );
    Ok(())
}

#[test]
fn settling_reports_hard_conflicts_or_skips_soft_effects_and_views_the_rest_in_place()
-> Result<(), StreamError> {
    let [a, b, c, d, marker] = ["a", "b", "c", "d", "d-closed"].map(StreamPath::new);
    let (a, b, c, d, marker) = (a?, b?, c?, d?, marker?);
    let append = |path: &StreamPath| CommitMutation::Append {
        path: path.clone(),
        records: vec![AppendRecord::from(vec![1])],
    };
    let present = |path: &StreamPath| CommitCondition::Present { path: path.clone() };
    let soft_effect = |conditions, mutations| crate::CommitEffect {
        conditions,
        mutations,
        soft: true,
    };
    let mut request = CommitRequest {
        deadline_unix_millis: None,
        effects: vec![
            // Skipped: `b` is absent.
            soft_effect(vec![present(&b)], vec![append(&c), append(&b)]),
            crate::CommitEffect::hard(vec![present(&a)], vec![append(&a)]),
            // Skipped: `d` is closed.
            soft_effect(
                vec![
                    present(&d),
                    CommitCondition::Absent {
                        path: marker.clone(),
                    },
                ],
                vec![append(&d)],
            ),
        ],
        idempotency_key: crate::IdempotencyKey::new(bytes::Bytes::from_static(b"settle"))?,
    };
    crate::request::validate_commit(&mut request)?;
    let tail = |path: &StreamPath| [&a, &c, &d, &marker].contains(&path).then_some(1);
    let conflict = |condition: &CommitCondition| {
        condition_conflict(
            condition,
            tail(crate::CommitCondition::path(condition)),
            || Err(StreamError::Unavailable),
        )
    };
    // Negative control: an under-admitted output is refused, never grown.
    assert!(matches!(
        settle(&request, conflict, Vec::new(), Vec::with_capacity(1)),
        Err(StreamError::InvalidArgument)
    ));
    let (conflicts, skipped) = settle(
        &request,
        conflict,
        Vec::with_capacity(hard_conditions(&request)),
        Vec::with_capacity(soft(&request)),
    )?;
    assert!(conflicts.is_empty());
    assert_eq!(skipped, [0, 2]);
    let applied = applied(
        &request,
        &skipped,
        Vec::with_capacity(applied_len(&request, &skipped)),
    )?;
    let paths: Vec<_> = applied
        .iter()
        .map(|mutation| crate::CommitMutation::path(mutation))
        .collect();
    assert_eq!(paths, [&a]);
    // A failed hard condition conflicts, and no soft condition is evaluated.
    request.effects[1].conditions = vec![present(&b)];
    let (conflicts, skipped) = settle(
        &request,
        conflict,
        Vec::with_capacity(hard_conditions(&request)),
        Vec::with_capacity(soft(&request)),
    )?;
    assert_eq!(
        conflicts,
        [crate::CommitConflict::Missing { path: b.clone() }]
    );
    assert!(skipped.is_empty());
    Ok(())
}

#[test]
fn effect_selection_and_condition_membership_walk_the_original_slices_once()
-> Result<(), StreamError> {
    for count in [1, 2, 7, 31, 127] {
        for pattern in 0..32_u32 {
            let mut effects = Vec::new();
            for group in 0..count {
                let mut effect = crate::CommitEffect::hard(Vec::new(), Vec::new());
                effect.soft = group % 3 != 0;
                effect.conditions.push(CommitCondition::Present {
                    path: StreamPath::new(format!("item/{group:03}/a-unused"))?,
                });
                effect.conditions.push(CommitCondition::Present {
                    path: StreamPath::new(format!("item/{group:03}/z-unused"))?,
                });
                for slot in 0..4 {
                    let path = StreamPath::new(format!("item/{group:03}/m-{slot}"))?;
                    let records = vec![AppendRecord::from(vec![1])];
                    effect.mutations.push(if slot % 2 == 0 {
                        effect
                            .conditions
                            .push(CommitCondition::Absent { path: path.clone() });
                        CommitMutation::Fork {
                            source: StreamPath::new("source")?,
                            destination: path,
                            at_tail: 0,
                            records,
                            exclude: Vec::new(),
                        }
                    } else {
                        CommitMutation::Append { path, records }
                    });
                }
                effects.push(effect);
            }
            let mut request = CommitRequest {
                effects,
                deadline_unix_millis: None,
                idempotency_key: crate::IdempotencyKey::new(vec![1])?,
            };
            crate::request::validate_commit(&mut request)?;
            let skipped: Vec<_> = request
                .effects
                .iter()
                .enumerate()
                .filter(|(index, effect)| effect.soft && pattern & (1 << (index % 5)) != 0)
                .map(|(index, _)| u32::try_from(index).unwrap())
                .collect();
            let comparisons = Cell::new(0);
            let selected = settled_effects_with(&request, &skipped, |index, skip| {
                comparisons.set(comparisons.get() + 1);
                u32::try_from(index).unwrap().cmp(&skip)
            });
            assert_eq!(comparisons.get(), 0, "selection must stay lazy");
            let expected: Vec<_> = request
                .effects
                .iter()
                .enumerate()
                .map(|(index, effect)| (skipped.contains(&u32::try_from(index).unwrap()), effect))
                .collect();
            for ((omitted, original), (expected_skip, expected)) in selected.zip(&expected) {
                assert_eq!(omitted, *expected_skip);
                assert!(std::ptr::eq(original, *expected));
            }
            assert!(
                comparisons.get() <= request.effects.len(),
                "selection searched settled history again"
            );
            let mut selected = settled_effects(&request, &skipped);
            for position in 0..expected.len() {
                if position == expected.len() / 2 {
                    assert!(selected.clone().zip(&expected[position..]).all(
                        |((skip, actual), (expected_skip, expected))| skip == *expected_skip
                            && std::ptr::eq(actual, *expected)
                    ));
                }
                let (skip, actual) = selected.next().unwrap();
                assert_eq!(skip, expected[position].0);
                assert!(std::ptr::eq(actual, expected[position].1));
            }
            assert!(selected.next().is_none());
            let mut condition_comparisons = 0;
            crate::request::validate_commit_shape_with(&request, || condition_comparisons += 1)?;
            assert!(
                condition_comparisons <= request.conditions().count() + request.mutations().count(),
                "shape validation repeated condition searches"
            );
            let mut expected_mutations: Vec<_> = expected
                .iter()
                .filter(|(skip, _)| !skip)
                .flat_map(|(_, effect)| effect.mutations.iter())
                .collect();
            expected_mutations
                .sort_unstable_by_key(|mutation| crate::CommitMutation::path(mutation));
            let actual = applied(
                &request,
                &skipped,
                Vec::with_capacity(expected_mutations.len()),
            )?;
            assert!(
                actual
                    .iter()
                    .zip(expected_mutations)
                    .all(|(actual, expected)| std::ptr::eq(*actual, expected))
            );
        }
    }
    Ok(())
}

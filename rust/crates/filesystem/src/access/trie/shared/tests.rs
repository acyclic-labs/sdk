use super::*;

fn layer_work_oracle(repeated_scan: bool) {
    let layers: Vec<_> = (0..1024).map(|index| index..index + 1).collect();
    for matching in [
        (0..1024).collect::<Vec<_>>(),
        (0..1024).step_by(3).collect(),
    ] {
        let mut examined = 0;
        let actual: Vec<Vec<usize>> = if repeated_scan {
            layers
                .iter()
                .map(|layer| {
                    matching
                        .iter()
                        .filter(|index| {
                            examined += 1;
                            layer.contains(*index)
                        })
                        .copied()
                        .collect()
                })
                .collect()
        } else {
            matching_layers(&layers, &matching, |index, end| {
                examined += 1;
                index < end
            })
            .map(<[usize]>::to_vec)
            .collect()
        };
        for (layer, indexes) in layers.iter().zip(actual) {
            assert_eq!(
                indexes,
                matching
                    .iter()
                    .copied()
                    .filter(|index| layer.contains(index))
                    .collect::<Vec<_>>()
            );
        }
        assert!(
            examined <= matching.len() + layers.len(),
            "policy layers must consume matching indexes once: {examined}"
        );
    }
}

#[test]
fn policy_layers_partition_matching_rules_in_linear_work() {
    layer_work_oracle(false);
}

#[test]
#[should_panic(expected = "policy layers must consume matching indexes once")]
fn negative_control_policy_layers_rescan_all_matching_rules() {
    layer_work_oracle(true);
}

fn literal_view() -> Result<(Arc<CompiledPolicy>, PathTrie), InvalidPolicy> {
    let mut policy: Policy = serde_json::from_slice(crate::access::INITIAL_POLICY)
        .map_err(|_| InvalidPolicy("invalid initial policy"))?;
    let path = "a".repeat(80);
    for rule in policy.layers.iter_mut().flatten() {
        rule.resource.path.clone_from(&path);
    }
    let caller = Principal {
        kind: PrincipalKind::Agent,
        id: "caller".into(),
        lineage: vec![],
        groups: BTreeSet::new(),
        roles: BTreeSet::new(),
        tags: BTreeSet::new(),
        attributes: BTreeMap::new(),
    };
    let capability = Capability {
        layers: vec![vec![crate::access::Grant {
            actions: Action::ALL.into_iter().collect(),
            resource: ResourcePattern {
                lineage_root: None,
                volume: None,
                generations: Generations::All,
                path: path.clone(),
            },
        }]],
        expires_at: None,
    };
    let config = crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral);
    let resource = Resource {
        config,
        lineage: &[],
        volume: "project",
        generation: 1,
        head: 1,
        path: &path,
    };
    let context = Context {
        now: 10,
        approvals: 0,
    };
    let mut remaining = 4_194_304;
    let program = Arc::new(CompiledPolicy::compile_with_work(
        &policy,
        config,
        &mut remaining,
        |_| Ok(()),
    )?);
    let view = program.bind_with_work(
        &caller,
        resource,
        context,
        &capability,
        &mut remaining,
        |_| Ok(()),
    )?;
    for candidate in ["", "a", path.as_str(), &format!("{path}a"), "b"] {
        for action in Action::ALL {
            assert_eq!(
                view.decide(action, candidate, context.now),
                crate::access::decide(
                    &policy,
                    &caller,
                    action,
                    Resource {
                        path: candidate,
                        ..resource
                    },
                    context,
                    &capability,
                )
            );
        }
    }
    Ok((program, view))
}

#[test]
fn policy_cache_literal_binding_counts_only_reachable_pairs() -> Result<(), InvalidPolicy> {
    let (program, view) = literal_view()?;
    assert_eq!(program.states.len(), 82);
    assert_eq!(view.states.len(), 82);
    assert_eq!(view.state_count(), 82);
    assert!(view.state_ids().any(|state| state >= view.state_count()));
    // The subtree walk must translate sparse product identifiers to dense visits.
    assert_eq!(
        view.subtree_action_mask("", 10),
        (1 << Action::ALL.len()) - 1
    );
    assert!(!view.allows_every_path(&Action::ALL, 10));
    // Only the literal is granted, so no subtree is wholly readable.
    assert!(!view.allows_subtree("", &[Action::Read], 10));
    assert!(!view.allows_subtree("b", &[Action::Read], 10));
    Ok(())
}

#[test]
fn policy_cache_reachable_pair_traversal_continues_past_4096() -> Result<(), InvalidPolicy> {
    let (mut program, mut view) = literal_view()?;
    // Drop the binding's Arc before replacing the fixture's two automata with
    // independent 65-state cycles: all 4,225 pairs are reachable.
    view.shared = None;
    let program = Arc::get_mut(&mut program).ok_or(InvalidPolicy("program still bound"))?;
    program.alphabet = Alphabet::new(&['a', 'b'])?;
    view.alphabet = Alphabet::new(&['a', 'b'])?;
    let cycles = |policy: bool| {
        (0..65)
            .map(|state| {
                let next = (state + 1) % 65;
                terminal(
                    if policy {
                        vec![next, state, state]
                    } else {
                        vec![state, next, state]
                    },
                    [Outcome {
                        rule: None,
                        denial: None,
                    }; 12],
                )
            })
            .collect()
    };
    program.states = cycles(true);
    view.states = cycles(false);
    let mut remaining = usize::MAX;
    let reachable = program
        .reachable_pairs(&view, &mut remaining, &mut |_| Ok(()))?
        .ok_or(InvalidPolicy("missing reachable pairs"))?;
    assert_eq!(reachable, (0..65 * 65).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn policy_determinization_admits_discovered_states_and_preserves_finite_denials()
-> Result<(), InvalidPolicy> {
    let mut nfa = Nfa::default();
    let (start, end) = nfa.pattern(&"a".repeat(5000), false)?;
    let mut admitted = 0;
    let mut reserve = |bytes| {
        assert!(bytes > 0);
        admitted += 1;
        Ok(())
    };
    let mut admission = StateAdmission {
        reserve: &mut reserve,
        symbols: 1,
        state_bytes: size_of::<State>(),
    };
    let decisions = |subset: &[usize]| {
        [Outcome {
            rule: None,
            denial: (!subset.contains(&end)).then_some(Denial::Capability),
        }; 12]
    };
    let mut remaining = usize::MAX;
    let states = determinize(
        &nfa,
        vec![start],
        &['a'],
        'b',
        decisions,
        1,
        &mut remaining,
        &mut admission,
    )?;
    assert_eq!(admitted, states.len());
    assert!(states.len() > 4096);
    let mut state = 0;
    for _ in 0..5000 {
        assert_eq!(states[state].actions, 0);
        state = states[state].transitions[0];
    }
    assert_eq!(states[state].actions, (1 << 12) - 1);
    assert_eq!(states[states[state].transitions[0]].actions, 0);
    assert_eq!(states[states[0].transitions[1]].actions, 0);

    let mut accepted = 0;
    let denied = InvalidPolicy("fixture storage admission denied");
    let mut reserve = |_| {
        accepted += 1;
        if accepted == 3 {
            Err(denied.clone())
        } else {
            Ok(())
        }
    };
    let mut admission = StateAdmission {
        reserve: &mut reserve,
        symbols: 1,
        state_bytes: size_of::<State>(),
    };
    let mut remaining = usize::MAX;
    assert_eq!(
        determinize(
            &nfa,
            vec![start],
            &['a'],
            'b',
            decisions,
            1,
            &mut remaining,
            &mut admission
        )
        .err(),
        Some(denied)
    );
    assert_eq!(accepted, 3);
    let mut admission = StateAdmission {
        reserve: &mut |_| Ok(()),
        symbols: 1,
        state_bytes: size_of::<State>(),
    };
    let mut remaining = 0;
    assert!(
        determinize(
            &nfa,
            vec![start],
            &['a'],
            'b',
            decisions,
            1,
            &mut remaining,
            &mut admission
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn policy_alphabet_indexes_the_full_unicode_scalar_domain() -> Result<(), InvalidPolicy> {
    let symbols: Vec<_> = (1..=70_000).filter_map(char::from_u32).collect();
    let alphabet = Alphabet::new(&symbols)?;
    for (index, ch) in symbols.iter().enumerate() {
        assert_eq!(alphabet.symbol(*ch), Some(index));
    }
    assert_eq!(alphabet.symbol('\u{10ffff}'), Some(symbols.len()));
    let mut nfa = Nfa::default();
    nfa.pattern(&"a".repeat(33_000), false)?;
    assert!(nfa.nodes.len() > 32_768);
    Ok(())
}

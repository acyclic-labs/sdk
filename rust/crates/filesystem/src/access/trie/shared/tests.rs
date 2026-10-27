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
    let mut remaining = MAXIMUM_WORK;
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
    assert!(view.state_ids().any(|state| state >= MAXIMUM_STATES));
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
#[should_panic(expected = "unreachable Cartesian pairs reject an allowed literal")]
fn policy_cache_negative_control_cartesian_bound_rejects_a_reachable_literal() {
    let (program, view) = literal_view().expect("the reachable literal must compile");
    assert!(
        program.states.len() * view.states.len() <= MAXIMUM_STATES,
        "unreachable Cartesian pairs reject an allowed literal",
    );
}

#[test]
fn policy_cache_reachable_pair_limit_still_rejects_an_oversized_product()
-> Result<(), InvalidPolicy> {
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
    let result = program.reachable_pairs(&view, &mut remaining, &mut |_| Ok(()));
    assert_eq!(
        result.err(),
        Some(InvalidPolicy("combined policy exceeds DFA state limit"))
    );
    Ok(())
}

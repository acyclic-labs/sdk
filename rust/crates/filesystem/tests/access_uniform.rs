use super::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Universal path patterns have one terminal outcome per action. Arbitrary
    // layers, denies, claims, expiry and history still use the shared reducer.
    #[test]
    fn uniform_terminals_agree_with_reference(
        layers in prop::collection::vec(prop::collection::vec(
            (any::<bool>(), 0u16..4096, 0u8..5, any::<bool>()), 0..4), 0..3),
        grants in prop::collection::vec(prop::collection::vec(0u16..4096, 0..4), 0..3),
        generation in 0u64..11,
        has_role in any::<bool>(),
        anonymous in any::<bool>(),
        expired in any::<bool>(),
        path in prop::collection::vec("[a-zΣé]{1,8}", 0..5),
    ) {
        let mut caller = principal();
        if has_role { caller.roles.insert("curator".into()); }
        if anonymous { caller.kind = PrincipalKind::Anonymous; }
        let mut policy = policy(Vec::new());
        policy.layers = layers.into_iter().enumerate().map(|(layer, rules)| {
            rules.into_iter().enumerate().map(|(index, (deny, mask, selector, expires))| {
                let mut rule = rule(&format!("{layer}/{index}"),
                    if deny { Effect::Deny } else { Effect::Allow }, &[], "**");
                rule.actions = Action::ALL.into_iter()
                    .filter(|action| mask & (1 << *action as u16) != 0).collect();
                match selector {
                    1 => rule.conditions.role = Some("curator".into()),
                    2 => rule.principal.id = Some("worker".into()),
                    3 => rule.resource.generations = Generations::Head,
                    4 => rule.resource.volume = Some("other".into()),
                    _ => {}
                }
                if expires { rule.conditions.expires_at = Some(100); }
                rule
            }).collect()
        }).collect();
        let cap = Capability {
            layers: grants.into_iter().map(|layer| layer.into_iter().map(|mask| Grant {
                actions: Action::ALL.into_iter()
                    .filter(|action| mask & (1 << *action as u16) != 0).collect(),
                resource: pattern("**"),
            }).collect()).collect(),
            expires_at: expired.then_some(100),
        };
        let path = path.join("/");
        let res = resource(&caller.lineage, &path, generation);
        let context = Context { now: 100, approvals: 0 };
        let trie = PathTrie::compile(&policy, &caller, res, context, &cap).unwrap();
        for action in Action::ALL {
            prop_assert_eq!(trie.decide(action, &path, context.now),
                decide(&policy, &caller, action, res, context, &cap));
        }
    }
}

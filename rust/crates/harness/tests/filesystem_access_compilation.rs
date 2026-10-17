use acyclic_fs::access::*;
use proptest::prelude::*;

fn caller() -> Principal {
    Principal {
        kind: PrincipalKind::Service,
        id: "host".into(),
        lineage: vec![],
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    }
}

fn resource() -> Resource<'static> {
    Resource {
        config: acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
        lineage: &[],
        volume: "project",
        generation: 1,
        head: 1,
        path: "",
    }
}

fn input(pattern: &str) -> Result<(Policy, Capability), serde_json::Error> {
    let mut policy: Policy = serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY)?;
    let mut deny = policy.layers[0][0].clone();
    deny.id = "deny".into();
    deny.effect = Effect::Deny;
    deny.resource.path = pattern.into();
    policy.layers[0].push(deny);
    let capability =
        serde_json::from_slice(include_bytes!("../src/filesystem/access/legacy-read.json"))?;
    Ok((policy, capability))
}

#[test]
fn compiler_admits_storage_before_allocation_and_honors_work()
-> Result<(), Box<dyn std::error::Error>> {
    let (policy, cap) = input("secrets/**")?;
    let caller = caller();
    let context = Context {
        now: 10,
        approvals: 0,
    };
    let mut result = None;
    let mut remaining = 100_000;
    let mut calls = 0;
    let allocation = acyclic_test_support::measure(|| {
        result = Some(PathTrie::compile_with_work(
            &policy,
            &caller,
            resource(),
            context,
            &cap,
            &mut remaining,
            |_| {
                calls += 1;
                Err(InvalidPolicy("unfunded"))
            },
        ));
    });
    assert!(result.ok_or("compiler did not run")?.is_err());
    assert_eq!(calls, 1);
    assert_eq!(allocation.count_total, 0);
    assert!(remaining < 100_000);
    let mut remaining = 0;
    let mut called = false;
    assert!(
        PathTrie::compile_with_work(
            &policy,
            &caller,
            resource(),
            context,
            &cap,
            &mut remaining,
            |_| {
                called = true;
                Ok(())
            }
        )
        .is_err()
    );
    assert!(!called);
    assert_eq!(remaining, 0);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn compiler_reservations_cover_live_storage_and_bound_failed_work(
        pattern in prop_oneof![
            prop::collection::vec(prop_oneof![Just("**"), Just("?"), Just("a*"), Just("Σ"), Just("b")], 1..6)
                .prop_map(|parts| parts.join("/")),
            (0..15_usize).prop_map(|depth| format!("**/a{}", "/?".repeat(depth))),
        ],
        work in 0..100_001_usize,
        refuse_dfa in any::<bool>(),
        folded in any::<bool>(),
    ) {
        let (policy, capability) = input(&pattern)?;
        let caller = caller();
        let context = Context { now: 10, approvals: 0 };
        let mut resource = resource();
        if folded { resource.config.case_sensitivity = acyclic_fs::model::CaseSensitivity::ProfileFolded; }
        let mut remaining = work;
        let mut reserved = 0;
        let mut stages = 0;
        let mut compiled = None;
        let measured = acyclic_test_support::measure(|| {
            compiled = Some(PathTrie::compile_with_work(&policy, &caller, resource, context, &capability,
                &mut remaining, |bytes| {
                    stages += 1;
                    if refuse_dfa && stages == 2 { return Err(InvalidPolicy("unfunded DFA")); }
                    reserved += bytes;
                    Ok(())
                }));
        });
        prop_assert!(remaining <= work);
        prop_assert!(stages <= 2);
        prop_assert!(measured.bytes_max <= reserved as u64, "{} live bytes exceed {} reserved for {}", measured.bytes_max, reserved, pattern);
        if let Some(Ok(trie)) = compiled {
            prop_assert_eq!(trie.logical_owned_bytes(), measured.bytes_current as u64);
            for path in ["", "a/b", "secrets/key", "Σ/b", "a/x/y/b"] {
                prop_assert_eq!(trie.decide(Action::Read, path, 10),
                    decide(&policy, &caller, Action::Read, Resource { path, ..resource }, context, &capability));
            }
            let hot = acyclic_test_support::measure(|| {
                for _ in 0..32 { std::hint::black_box(trie.decide(Action::Read, "secrets/key", 10)); }
            });
            prop_assert_eq!(hot.count_total, 0);
        }
    }
}

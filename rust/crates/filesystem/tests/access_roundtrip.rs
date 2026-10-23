use super::*;
#[path = "access_input.rs"]
mod input;

#[test]
fn shared_access_oracle_detects_a_corrupted_decision() -> Result<(), Box<dyn std::error::Error>> {
    let value = input::Input {
        policy: policy(vec![rule("all", Effect::Allow, &Action::ALL, "**")]),
        capability: capability(),
        principal: principal(),
        path: "public/file".into(),
        profile: 0,
        folded: false,
        now: 100,
        native_name: None,
    };
    let bytes = serde_json::to_vec(&value)?;
    input::check(&bytes);
    std::panic::catch_unwind(|| input::check_with_corrupted_decision(&bytes))
        .expect_err("the shared oracle must detect a corrupted decision");
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Serialized policies and complete delegation chains round-trip without
    // changing any decision, across native profiles and malformed path inputs.
    #[test]
    fn serialized_authority_preserves_decisions(
        patterns in prop::collection::vec((any::<bool>(), glob_strategy()), 0..5),
        ceiling in glob_strategy(),
        path in prop::collection::vec(prop_oneof!["[^/\\\\]{0,8}", Just("..".into()), Just("ΟΣΑ".into())], 0..4),
        profile in any::<u8>(), folded in any::<bool>(), expiry in any::<Option<u16>>(),
        native_name in prop::option::of((any::<u8>(), prop::collection::vec(any::<u8>(), 0..32))),
    ) {
        let rules = patterns.into_iter().enumerate().map(|(i, (deny, path))|
            rule(&i.to_string(), if deny { Effect::Deny } else { Effect::Allow }, &Action::ALL, &path)).collect();
        let capability = capability().attenuate(vec![Grant {
            actions: [Action::List, Action::Read].into_iter().collect(), resource: pattern(&ceiling),
        }], expiry.map(u64::from));
        let value = input::Input { policy: policy(rules), capability, principal: principal(),
            path: path.join("/"), profile, folded, now: 100, native_name };
        let bytes = serde_json::to_vec(&value)?;
        prop_assert_eq!(&serde_json::from_slice::<input::Input>(&bytes)?, &value);
        input::check(&bytes);
        input::check(value.capability.to_scope_label()?.as_bytes());
    }
}

#[test]
fn access_fuzz_corpus() -> Result<(), Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(acyclic_test_support::source_path(
        "rust/crates/filesystem/fuzz/corpus/access",
    ))? {
        let entry = entry?;
        let bytes = std::fs::read(entry.path())?;
        if entry.file_name() != "scope" && entry.file_name() != "malformed" {
            // A schema change must update the corpus, never turn it into no-ops.
            let _: input::Input = serde_json::from_slice(&bytes)?;
        }
        input::check(&bytes);
    }
    Ok(())
}

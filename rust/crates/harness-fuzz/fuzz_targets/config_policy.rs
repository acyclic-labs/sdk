//! Exercise policy export decoding and attenuation through the shared compiler.
#![no_main]

use acyclic_fs::access;
use acyclic_harness::config::{Policy, policy};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok((parent, child, path)) =
        serde_json::from_slice::<(serde_json::Value, serde_json::Value, String)>(data)
    else {
        return;
    };
    let (Ok(parent), Ok(child)) = (policy::decode(parent), policy::decode(child)) else {
        return;
    };
    for original in [&parent, &child] {
        let encoded = serde_json::to_value(original).expect("policy serialization");
        assert_eq!(policy::decode(encoded).as_ref().ok(), Some(original));
    }
    let (Ok(issuer), Ok(narrowed)) = (
        policy::meet([("parent", &parent)], 1),
        policy::meet([("parent", &parent), ("child", &child)], 1),
    ) else {
        return;
    };
    let principal = access::Principal {
        kind: access::PrincipalKind::Agent,
        id: "subject".into(),
        lineage: vec![],
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    };
    let resource = access::Resource {
        config: acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
        lineage: &[],
        volume: "home",
        generation: 1,
        head: 1,
        path: "",
    };
    let context = access::Context {
        now: 0,
        approvals: 0,
    };
    let all = access::Capability::whole_volume(None, &access::Action::ALL);
    let read_only = access::Capability::whole_volume(None, &[access::Action::Read]);
    let locked = Policy {
        only: Some(vec![]),
        ..Policy::default()
    };
    let blocked = policy::meet([("parent", &parent), ("lock", &locked)], 1)
        .expect("adding an empty only list preserves validity");
    let open = policy::meet([], 1).expect("empty issuer meet");
    let compile = |issuer, advisory, ceiling| {
        policy::Permissions::compile(issuer, advisory, &principal, resource, context, ceiling)
            .expect("validated portable policy compilation")
    };
    let issuer = compile(&issuer, None, &all);
    let narrowed = compile(&narrowed, None, &all);
    let advisory = compile(&blocked, Some(&child), &all);
    let ceiling = compile(&open, Some(&child), &read_only);
    // Use the same production decision path for both sides of attenuation.
    // The lock also covers the special advisory recovery entries: a subject
    // may never use those entries to bypass its issuer or static ceiling.
    for path in [path.as_str(), "policy.ts", "plugins.ts", "plugins/copy.ts"] {
        for action in [
            access::Action::Read,
            access::Action::Create,
            access::Action::Write,
            access::Action::Append,
            access::Action::Delete,
            access::Action::Rename,
        ] {
            assert!(!narrowed.allows(action, path, 0) || issuer.allows(action, path, 0));
            assert!(!advisory.allows(action, path, 0));
            if action != access::Action::Read {
                assert!(!ceiling.allows(action, path, 0));
            }
        }
    }
});

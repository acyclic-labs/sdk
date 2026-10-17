//! Scenario matrix and randomized access invariants.
use acyclic_fs::access::*;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

#[path = "access_review.rs"]
mod review;

#[path = "access_roundtrip.rs"]
mod roundtrip;

#[path = "access_binding.rs"]
mod binding;
#[path = "access_uniform.rs"]
mod uniform;

fn principal() -> Principal {
    Principal {
        kind: PrincipalKind::Agent,
        id: "worker".into(),
        lineage: vec!["task".into(), "worker".into()],
        groups: BTreeSet::new(),
        roles: BTreeSet::new(),
        tags: BTreeSet::new(),
        attributes: BTreeMap::new(),
    }
}

fn pattern(path: &str) -> ResourcePattern {
    ResourcePattern {
        lineage_root: None,
        volume: Some("project".into()),
        generations: Generations::All,
        path: path.into(),
    }
}

fn rule(id: &str, effect: Effect, actions: &[Action], path: &str) -> Rule {
    Rule {
        id: id.into(),
        effect,
        principal: PrincipalPattern::default(),
        actions: actions.iter().copied().collect(),
        resource: pattern(path),
        conditions: Conditions::default(),
    }
}

fn policy(rules: Vec<Rule>) -> Policy {
    Policy {
        schema_version: POLICY_SCHEMA_VERSION,
        version: 1,
        layers: vec![rules],
    }
}

fn capability() -> Capability {
    Capability {
        layers: vec![vec![Grant {
            actions: Action::ALL.into_iter().collect(),
            resource: pattern("**"),
        }]],
        expires_at: None,
    }
}

fn resource<'a>(lineage: &'a [String], path: &'a str, generation: u64) -> Resource<'a> {
    Resource {
        config: acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Durable),
        lineage,
        volume: "project",
        generation,
        head: 10,
        path,
    }
}

struct Case {
    name: &'static str,
    policy: Policy,
    principal: Principal,
    capability: Capability,
    action: Action,
    path: &'static str,
    generation: u64,
    context: Context,
    allowed: bool,
}

impl Case {
    fn new(
        name: &'static str,
        rules: Vec<Rule>,
        action: Action,
        path: &'static str,
        allowed: bool,
    ) -> Self {
        Self {
            name,
            policy: policy(rules),
            principal: principal(),
            capability: capability(),
            action,
            path,
            generation: 10,
            context: Context {
                now: 100,
                approvals: 0,
            },
            allowed,
        }
    }
}

#[test]
fn scenario_table() -> Result<(), Box<dyn std::error::Error>> {
    let mut own = rule("own", Effect::Allow, &[Action::Write], "**");
    own.conditions.relation = Some(Relation::SelfTask);
    let mut coordinator = rule("coordinator", Effect::Allow, &[Action::MergeInto], "**");
    coordinator.conditions.role = Some("coordinator".into());
    let swarm = vec![
        rule("read-agents", Effect::Allow, &[Action::Read], "**"),
        own,
        coordinator,
    ];
    let mut role = rule("maintainer", Effect::Allow, &[Action::MergeInto], "**");
    role.conditions.role = Some("maintainer".into());
    let mut approvals = rule("approved", Effect::Allow, &[Action::MergeInto], "**");
    approvals.conditions.approvals = Some(2);
    let protected = vec![role, approvals];
    let mut pinned = rule(
        "pinned",
        Effect::Allow,
        &[Action::Read, Action::ReadHistory],
        "**",
    );
    pinned.resource.generations = Generations::Range(GenerationRange { first: 4, last: 4 });
    let ci = vec![
        pinned,
        rule(
            "artifacts",
            Effect::Allow,
            &[Action::Create, Action::Write],
            "artifacts/**",
        ),
    ];
    let mut curator = rule("curator", Effect::Allow, &[Action::Write], "**");
    curator.conditions.role = Some("curator".into());
    let dataset = vec![
        curator,
        rule("ingest", Effect::Allow, &[Action::Append], "logs/**"),
    ];
    let inbox = vec![rule("inbox", Effect::Allow, &[Action::Create], "inbox/**")];
    let secrets = vec![
        rule("read", Effect::Allow, &[Action::Read, Action::List], "**"),
        rule(
            "hidden",
            Effect::Deny,
            &[Action::Read, Action::List],
            "secrets/**",
        ),
    ];
    let mut public = rule(
        "public",
        Effect::Allow,
        &[Action::Read, Action::ReadHistory],
        "**",
    );
    public.principal.kind = Some(PrincipalKind::Anonymous);
    public.resource.generations = Generations::Range(GenerationRange { first: 4, last: 4 });
    let mut cases = vec![
        Case::new("swarm read", swarm.clone(), Action::Read, "src/a.rs", true),
        Case::new(
            "swarm own write",
            swarm.clone(),
            Action::Write,
            "src/a.rs",
            true,
        ),
        Case::new(
            "swarm other write",
            swarm.clone(),
            Action::Write,
            "src/a.rs",
            false,
        ),
        Case::new(
            "swarm coordinator merge",
            swarm,
            Action::MergeInto,
            "",
            true,
        ),
        Case::new(
            "protected main denied",
            protected.clone(),
            Action::MergeInto,
            "",
            false,
        ),
        Case::new(
            "protected main role",
            protected.clone(),
            Action::MergeInto,
            "",
            true,
        ),
        Case::new(
            "protected main approvals",
            protected,
            Action::MergeInto,
            "",
            true,
        ),
        Case::new(
            "CI pinned read",
            ci.clone(),
            Action::Read,
            "source.rs",
            true,
        ),
        Case::new(
            "CI unpinned read",
            ci.clone(),
            Action::Read,
            "source.rs",
            false,
        ),
        Case::new(
            "CI artifacts",
            ci.clone(),
            Action::Write,
            "artifacts/test.json",
            true,
        ),
        Case::new("CI source write", ci, Action::Write, "source.rs", false),
        Case::new(
            "dataset curator",
            dataset.clone(),
            Action::Write,
            "data/table",
            true,
        ),
        Case::new(
            "dataset ingest",
            dataset.clone(),
            Action::Append,
            "logs/ingest",
            true,
        ),
        Case::new(
            "dataset replace denied",
            dataset,
            Action::Write,
            "logs/ingest",
            false,
        ),
        Case::new(
            "inbox create",
            inbox.clone(),
            Action::Create,
            "inbox/message",
            true,
        ),
        Case::new(
            "inbox read denied",
            inbox,
            Action::Read,
            "inbox/message",
            false,
        ),
        Case::new(
            "secrets read",
            secrets.clone(),
            Action::Read,
            "secrets/key",
            false,
        ),
        Case::new(
            "secrets list",
            secrets.clone(),
            Action::List,
            "secrets",
            false,
        ),
        Case::new("ordinary list", secrets, Action::List, "src", true),
        Case::new(
            "list-only metadata",
            vec![rule("list", Effect::Allow, &[Action::List], "**")],
            Action::List,
            "src",
            true,
        ),
        Case::new(
            "list-only no contents",
            vec![rule("list", Effect::Allow, &[Action::List], "**")],
            Action::Read,
            "src",
            false,
        ),
        Case::new(
            "expired credentials",
            vec![rule("read", Effect::Allow, &[Action::Read], "**")],
            Action::Read,
            "src",
            false,
        ),
        Case::new(
            "public snapshot",
            vec![public.clone()],
            Action::Read,
            "src",
            true,
        ),
        Case::new(
            "public head denied",
            vec![public],
            Action::Read,
            "src",
            false,
        ),
    ];
    for case in &mut cases {
        match case.name {
            "swarm other write" => case.principal.lineage = vec!["task".into(), "other".into()],
            "swarm coordinator merge" => {
                case.principal.roles.insert("coordinator".into());
            }
            "protected main role" => {
                case.principal.roles.insert("maintainer".into());
            }
            "protected main approvals" => case.context.approvals = 2,
            "CI pinned read" => case.generation = 4,
            "dataset curator" => {
                case.principal.roles.insert("curator".into());
            }
            "expired credentials" => case.capability.expires_at = Some(100),
            "public snapshot" | "public head denied" => {
                case.principal.kind = PrincipalKind::Anonymous;
                if case.name == "public snapshot" {
                    case.generation = 4;
                }
            }
            _ => {}
        }
        let lineage = vec!["task".into(), "worker".into()];
        let resource = resource(&lineage, case.path, case.generation);
        let direct = decide(
            &case.policy,
            &case.principal,
            case.action,
            resource,
            case.context,
            &case.capability,
        );
        assert_eq!(direct.allowed, case.allowed, "{}", case.name);
        let trie = PathTrie::compile(
            &case.policy,
            &case.principal,
            resource,
            case.context,
            &case.capability,
        )?;
        assert_eq!(
            trie.decide(case.action, case.path, case.context.now),
            direct,
            "{}",
            case.name
        );
    }
    Ok(())
}

#[test]
fn globs_and_deny_precedence() -> Result<(), Box<dyn std::error::Error>> {
    let principal = principal();
    for glob in [
        "", "**", "a/**", "**/a", "a/**/b", "**/**/b", "a*", "?/é", "a/*/b", "a/**/**", "*", "**/*",
    ] {
        let policy = policy(vec![rule("glob", Effect::Allow, &Action::ALL, glob)]);
        let cap = capability();
        let context = Context {
            now: 100,
            approvals: 0,
        };
        let trie = PathTrie::compile(
            &policy,
            &principal,
            resource(&principal.lineage, "", 10),
            context,
            &cap,
        )?;
        for path in [
            "", "a", "ab", "a/b", "a/x/b", "a/x/y/b", "b", "x/a", "é/é", "x/a/b", "a/", "a/../b",
        ] {
            for action in Action::ALL {
                assert_eq!(
                    trie.decide(action, path, 100),
                    decide(
                        &policy,
                        &principal,
                        action,
                        resource(&principal.lineage, path, 10),
                        context,
                        &cap
                    ),
                    "{glob} {path}"
                );
            }
        }
    }
    let policy = policy(vec![
        rule("allow", Effect::Allow, &Action::ALL, "**"),
        rule("deny", Effect::Deny, &[Action::Read], "a/**"),
    ]);
    let cap = capability();
    let decision = decide(
        &policy,
        &principal,
        Action::Read,
        resource(&principal.lineage, "a", 10),
        Context {
            now: 0,
            approvals: 0,
        },
        &cap,
    );
    assert_eq!(decision.rule_id, Some("deny"));
    assert_eq!(decision.denial, Some(Denial::Rule));
    Ok(())
}

#[test]
fn expiry_and_revocation_require_new_open_views() -> Result<(), Box<dyn std::error::Error>> {
    let principal = principal();
    let mut rule = rule("expiring", Effect::Allow, &[Action::Read], "**");
    rule.conditions.expires_at = Some(101);
    let mut policy = policy(vec![rule]);
    let cap = capability();
    let res = resource(&principal.lineage, "a", 10);
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let trie = PathTrie::compile(&policy, &principal, res, context, &cap)?;
    assert!(trie.decide(Action::Read, "a", 100).allowed);
    assert!(!trie.decide(Action::Read, "a", 101).allowed);
    assert!(
        !decide(
            &policy,
            &principal,
            Action::Read,
            res,
            Context {
                now: 101,
                ..context
            },
            &cap
        )
        .allowed
    );
    drop(trie);
    policy.version += 1;
    policy.layers.clear();
    let revoked = PathTrie::compile(&policy, &principal, res, context, &cap)?;
    assert!(!revoked.decide(Action::Read, "a", 100).allowed);
    Ok(())
}

#[test]
fn lineage_and_authenticated_claims() {
    let mut caller = principal();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let cap = capability();
    let tasks = [
        vec!["task".into()],
        caller.lineage.clone(),
        vec!["task".into(), "other".into()],
        vec!["task".into(), "worker".into(), "child".into()],
        vec!["other".into()],
    ];
    for (relation, expected) in [
        (Relation::SelfTask, [false, true, false, false, false]),
        (Relation::Parent, [false, false, false, true, false]),
        (Relation::Child, [true, false, false, false, false]),
        (Relation::Sibling, [false, false, true, false, false]),
        (Relation::Ancestor, [false, false, false, true, false]),
        (Relation::Descendant, [true, false, false, false, false]),
        (Relation::SameTaskTree, [true, true, true, true, false]),
    ] {
        let mut selected = rule("relation", Effect::Allow, &[Action::Read], "**");
        selected.conditions.relation = Some(relation);
        let policy = policy(vec![selected]);
        for (lineage, allowed) in tasks.iter().zip(expected) {
            assert_eq!(
                decide(
                    &policy,
                    &caller,
                    Action::Read,
                    resource(lineage, "a", 10),
                    context,
                    &cap
                )
                .allowed,
                allowed,
                "{relation:?} {lineage:?}"
            );
        }
    }
    let mut selected = rule("claims", Effect::Allow, &[Action::Read], "**");
    selected.principal.group = Some("team".into());
    selected
        .principal
        .attributes
        .insert("department".into(), "research".into());
    selected.conditions.role = Some("curator".into());
    selected.conditions.tag = Some("trusted".into());
    let policy = policy(vec![selected]);
    caller.groups.insert("team".into());
    caller.roles.insert("curator".into());
    caller.tags.insert("trusted".into());
    caller
        .attributes
        .insert("department".into(), "research".into());
    assert!(
        decide(
            &policy,
            &caller,
            Action::Read,
            resource(&caller.lineage, "a", 10),
            context,
            &cap
        )
        .allowed
    );
    caller.kind = PrincipalKind::Anonymous;
    assert!(
        !decide(
            &policy,
            &caller,
            Action::Read,
            resource(&caller.lineage, "a", 10),
            context,
            &cap
        )
        .allowed
    );
}

#[test]
fn inheritance_history_and_invalid_inputs_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let caller = principal();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let cap = capability();
    let mut inherited = policy(vec![rule(
        "parent",
        Effect::Allow,
        &[Action::Read],
        "src/**",
    )]);
    inherited
        .layers
        .push(vec![rule("child", Effect::Allow, &Action::ALL, "**")]);
    assert!(
        !decide(
            &inherited,
            &caller,
            Action::Write,
            resource(&caller.lineage, "src/lib.rs", 10),
            context,
            &cap
        )
        .allowed
    );
    assert!(
        !decide(
            &inherited,
            &caller,
            Action::Read,
            resource(&caller.lineage, "secret", 10),
            context,
            &cap
        )
        .allowed
    );
    assert!(
        decide(
            &inherited,
            &caller,
            Action::Read,
            resource(&caller.lineage, "src/lib.rs", 10),
            context,
            &cap
        )
        .allowed
    );
    assert!(
        !decide(
            &inherited,
            &caller,
            Action::Read,
            resource(&caller.lineage, "src/lib.rs", 4),
            context,
            &cap
        )
        .allowed
    );
    for path in [
        "/src",
        "src/../secret",
        "src/./a",
        "src//a",
        "src\\a",
        "a\0b",
    ] {
        assert_eq!(
            decide(
                &inherited,
                &caller,
                Action::Read,
                resource(&caller.lineage, path, 10),
                context,
                &cap
            )
            .denial,
            Some(Denial::Invalid)
        );
    }
    inherited.schema_version += 1;
    assert!(
        PathTrie::compile(
            &inherited,
            &caller,
            resource(&caller.lineage, "", 10),
            context,
            &cap
        )
        .is_err()
    );
    let empty = Capability {
        layers: vec![],
        expires_at: None,
    };
    let delegated = empty
        .clone()
        .attenuate(cap.layers.first().cloned().unwrap_or_default(), None);
    assert!(delegated.is_attenuation_of(&empty));
    let all = policy(vec![rule("all", Effect::Allow, &Action::ALL, "**")]);
    assert!(
        !decide(
            &all,
            &caller,
            Action::Read,
            resource(&caller.lineage, "a", 10),
            context,
            &delegated
        )
        .allowed
    );
    assert_eq!(
        Capability::from_scope_label(&cap.to_scope_label()?)?,
        Some(cap)
    );
    Ok(())
}

#[test]
fn adversarial_globs_have_bounded_compilation() {
    let caller = principal();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let explosive = format!("*a{}", "?".repeat(24));
    for (policy_glob, capability_glob) in [(explosive.as_str(), "**"), ("**", explosive.as_str())] {
        let policy = policy(vec![rule(
            "bounded",
            Effect::Allow,
            &Action::ALL,
            policy_glob,
        )]);
        let cap = Capability {
            layers: vec![vec![Grant {
                actions: Action::ALL.into_iter().collect(),
                resource: pattern(capability_glob),
            }]],
            expires_at: None,
        };
        assert!(policy.validate().is_ok());
        assert!(cap.validate().is_ok());
        assert!(
            PathTrie::compile(
                &policy,
                &caller,
                resource(&caller.lineage, "", 10),
                context,
                &cap
            )
            .is_err()
        );
    }
    let oversized = "a".repeat(32_769);
    let policy = policy(vec![rule(
        "oversized",
        Effect::Allow,
        &Action::ALL,
        &oversized,
    )]);
    assert!(policy.validate().is_ok());
    assert!(
        PathTrie::compile(
            &policy,
            &caller,
            resource(&caller.lineage, "", 10),
            context,
            &capability()
        )
        .is_err()
    );
}

#[test]
fn compiled_rule_identifiers_have_a_bound() {
    let caller = principal();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let long_id = "x".repeat(1_048_577);
    let policy = policy(vec![rule(&long_id, Effect::Allow, &Action::ALL, "**")]);
    assert!(
        PathTrie::compile(
            &policy,
            &caller,
            resource(&caller.lineage, "", 10),
            context,
            &capability()
        )
        .is_err()
    );
}

#[test]
fn malformed_generation_denials_fail_closed() {
    let caller = principal();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let cap = capability();
    let reversed = GenerationRange { first: 10, last: 9 };
    for condition in [false, true] {
        let mut deny = rule("invalid-deny", Effect::Deny, &Action::ALL, "**");
        if condition {
            deny.conditions.generations = Some(reversed);
        } else {
            deny.resource.generations = Generations::Range(reversed);
        }
        let policy = policy(vec![rule("allow", Effect::Allow, &Action::ALL, "**"), deny]);
        let res = resource(&caller.lineage, "a", 10);
        assert_eq!(
            decide(&policy, &caller, Action::Read, res, context, &cap).denial,
            Some(Denial::Invalid)
        );
        assert!(PathTrie::compile(&policy, &caller, res, context, &cap).is_err());
    }
    let mut selected = pattern("**");
    selected.generations = Generations::Range(reversed);
    let cap = Capability {
        layers: vec![vec![Grant {
            actions: Action::ALL.into_iter().collect(),
            resource: selected,
        }]],
        expires_at: None,
    };
    let policy = policy(vec![rule("allow", Effect::Allow, &Action::ALL, "**")]);
    assert_eq!(
        decide(
            &policy,
            &caller,
            Action::Read,
            resource(&caller.lineage, "a", 10),
            context,
            &cap
        )
        .denial,
        Some(Denial::Invalid)
    );
}

#[test]
fn native_mount_modes_share_the_policy_decisions() -> Result<(), Box<dyn std::error::Error>> {
    let caller = principal();
    let cap = capability();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let policy = policy(vec![
        rule("read", Effect::Allow, &[Action::List, Action::Read], "**"),
        rule(
            "hidden",
            Effect::Deny,
            &[Action::List, Action::Read],
            "secrets/**",
        ),
        rule("inbox-no-read", Effect::Deny, &[Action::Read], "inbox/**"),
        rule("inbox", Effect::Allow, &[Action::Create], "inbox/**"),
        rule("log", Effect::Allow, &[Action::Append], "logs/**"),
    ]);
    let trie = PathTrie::compile(
        &policy,
        &caller,
        resource(&caller.lineage, "", 10),
        context,
        &cap,
    )?;
    let source = trie.mount_mode("src/lib.rs", 100);
    assert!(source.visible());
    assert!(source.read_only());
    assert!(!source.create_only());
    let secret = trie.mount_mode("secrets/key", 100);
    assert!(!secret.visible());
    assert!(!secret.allows(Action::Read));
    let inbox = trie.mount_mode("inbox/message", 100);
    assert!(inbox.create_only());
    assert!(!inbox.read_only());
    assert!(!inbox.allows(Action::Read));
    let log = trie.mount_mode("logs/events", 100);
    assert!(log.allows(Action::Append));
    assert!(!log.allows(Action::Write));
    assert!(!log.read_only());
    for path in ["src/lib.rs", "secrets/key", "inbox/message", "logs/events"] {
        for action in Action::ALL {
            assert_eq!(
                trie.mount_mode(path, 100).allows(action),
                decide(
                    &policy,
                    &caller,
                    action,
                    resource(&caller.lineage, path, 10),
                    context,
                    &cap
                )
                .allowed
            );
        }
    }
    let expiring = Capability {
        expires_at: Some(101),
        ..cap
    };
    let trie = PathTrie::compile(
        &policy,
        &caller,
        resource(&caller.lineage, "", 10),
        context,
        &expiring,
    )?;
    assert!(!trie.mount_mode("src/lib.rs", 101).visible());
    assert!(trie.mount_mode("src/lib.rs", 101).read_only());
    assert!(!trie.mount_mode("src/../secrets/key", 100).visible());
    assert!(!trie.mount_mode("src/lib.rs", 99).visible());
    Ok(())
}

fn glob_strategy() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            Just("a"),
            Just("A"),
            Just("b"),
            Just("*"),
            Just("**"),
            Just("a?"),
            Just("é*"),
            Just("É*"),
            Just("ΟΣ")
        ],
        0..4,
    )
    .prop_map(|parts| parts.join("/"))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn trie_agrees_with_reference(
        rules in prop::collection::vec((any::<bool>(), any::<u16>(), glob_strategy()), 0..8),
        path in prop::collection::vec(prop_oneof![Just("a"), Just("A"), Just("b"), Just("aa"), Just("é"), Just("É"), Just("ΟΣ"), Just("ος")], 0..5),
        grant_path in glob_strategy(),
        folded in any::<bool>(),
        profile in 0..4usize,
    ) {
        let principal = principal();
        let rules = rules.into_iter().enumerate().map(|(index, (deny, bits, path))|
            rule(&index.to_string(), if deny { Effect::Deny } else { Effect::Allow },
                &Action::ALL.into_iter().enumerate().filter_map(|(bit, action)| ((bits & (1 << bit)) != 0).then_some(action)).collect::<Vec<_>>(), &path)).collect();
        let policy = policy(rules);
        let cap = capability().attenuate(vec![Grant { actions: Action::ALL.into_iter().collect(), resource: pattern(&grant_path) }], None);
        let context = Context { now: 100, approvals: 0 };
        let path = path.join("/");
        let mut res = resource(&principal.lineage, &path, 10);
        res.config.profile = [
            acyclic_fs::model::FilesystemProfile::Portable,
            acyclic_fs::model::FilesystemProfile::Browser,
            acyclic_fs::model::FilesystemProfile::Posix,
            acyclic_fs::model::FilesystemProfile::Windows,
        ][profile];
        if folded { res.config.case_sensitivity = acyclic_fs::model::CaseSensitivity::ProfileFolded; }
        let trie = PathTrie::compile(&policy, &principal, res, context, &cap)?;
        let prepared = trie.prepare(&path)?;
        let mode = prepared.mount_mode(100);
        prop_assert_eq!(mode.visible(), decide(&policy, &principal, Action::List, res, context, &cap).allowed);
        for action in Action::ALL {
            prop_assert_eq!(mode.allows(action), decide(&policy, &principal, action, res, context, &cap).allowed);
            prop_assert_eq!(trie.decide(action, &path, 100), decide(&policy, &principal, action, res, context, &cap));
            prop_assert_eq!(prepared.decide(action, 100), decide(&policy, &principal, action, res, context, &cap));
        }
    }

    #[test]
    fn attenuation_never_widens(parent_glob in glob_strategy(), child_glob in glob_strategy(),
        path in prop::collection::vec("[ab]{1,3}", 0..5), parent_expiry in any::<Option<u16>>(), child_expiry in any::<Option<u16>>()) {
        let principal = principal();
        let policy = policy(vec![rule("all", Effect::Allow, &Action::ALL, "**")]);
        let parent = Capability { layers: vec![vec![Grant { actions: Action::ALL.into_iter().collect(), resource: pattern(&parent_glob) }]], expires_at: parent_expiry.map(u64::from) };
        let child = parent.clone().attenuate(vec![Grant { actions: [Action::Read].into_iter().collect(), resource: pattern(&child_glob) }], child_expiry.map(u64::from));
        prop_assert!(child.is_attenuation_of(&parent));
        let path = path.join("/");
        let context = Context { now: 100, approvals: 0 };
        for action in Action::ALL {
            let res = resource(&principal.lineage, &path, 10);
            prop_assert!(!decide(&policy, &principal, action, res, context, &child).allowed
                || decide(&policy, &principal, action, res, context, &parent).allowed);
        }
    }
}

#[test]
fn unrestricted_projection_accepts_only_universal_decisions() {
    let principal = principal();
    let policy = policy(vec![rule("all", Effect::Allow, &Action::ALL, "**")]);
    let context = Context {
        now: 10,
        approvals: 0,
    };
    let resource = Resource {
        config: acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
        lineage: &[],
        volume: "project",
        generation: 1,
        head: 1,
        path: "",
    };
    let mut capability = capability();
    let trie = PathTrie::compile(&policy, &principal, resource, context, &capability).unwrap();
    assert!(trie.allows_every_path(&Action::ALL, 10));
    capability.expires_at = Some(11);
    let trie = PathTrie::compile(&policy, &principal, resource, context, &capability).unwrap();
    assert!(trie.allows_every_path(&Action::ALL, 10));
    assert!(!trie.allows_every_path(&Action::ALL, 11));
    capability.layers[0][0].resource.path = "public/**".into();
    let trie = PathTrie::compile(&policy, &principal, resource, context, &capability).unwrap();
    assert!(!trie.allows_every_path(&[Action::Read], 10));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // A universal proof must imply decide() allows every generated action/path;
    // any actual deny or attenuated path must prevent raw-adapter admission.
    #[test]
    fn unrestricted_projection_implies_every_decision(
        denied in "[a-z]{1,6}", path in "[a-z]{1,6}(/[a-z]{1,6}){0,3}",
        deny_action in 0usize..12, restricted_capability in any::<bool>(),
    ) {
        let principal = principal();
        let context = Context { now: 10, approvals: 0 };
        let mut resource = Resource { config: acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
            lineage: &[], volume: "project", generation: 1, head: 1, path: &path };
        let action = Action::ALL[deny_action];
        let mut policy = policy(vec![rule("all", Effect::Allow, &Action::ALL, "**")]);
        let mut capability = capability();
        if restricted_capability { capability.layers[0][0].resource.path = format!("{denied}/**"); }
        else { policy.layers[0].push(rule("deny", Effect::Deny, &[action], &denied)); }
        let trie = PathTrie::compile(&policy, &principal, resource, context, &capability).unwrap();
        prop_assert!(!trie.allows_every_path(&Action::ALL, 10));
        for action in Action::ALL {
            resource.path = &path;
            if trie.allows_every_path(&[action], 10) {
                prop_assert!(decide(&policy, &principal, action, resource, context, &capability).allowed);
                resource.path = &denied;
                prop_assert!(decide(&policy, &principal, action, resource, context, &capability).allowed);
            }
        }
    }
}

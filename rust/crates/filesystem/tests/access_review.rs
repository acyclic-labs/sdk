use super::*;
use acyclic_fs::kernel::{LogicalName, NameEncoding};
use acyclic_fs::model::{CaseSensitivity, FilesystemProfile, UnicodePolicy};

#[test]
fn a_later_head_revokes_head_only_reads_without_a_policy_edit() -> Result<(), InvalidPolicy> {
    let caller = principal();
    let policy = super::policy(vec![rule("read", Effect::Allow, &[Action::Read], "**")]);
    let cap = capability();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let selected = Resource {
        head: 1,
        ..resource(&caller.lineage, "", 1)
    };
    let old = PathTrie::compile(&policy, &caller, selected, context, &cap)?;
    let view = AccessView::new(old);
    let initial = view.current()?;
    assert!(initial.decide(Action::Read, "key", 100).allowed);
    let advanced = Resource {
        head: 2,
        ..selected
    };
    view.publish(PathTrie::compile(
        &policy, &caller, advanced, context, &cap,
    )?)?;
    let expected = decide(
        &policy,
        &caller,
        Action::Read,
        Resource {
            path: "key",
            ..advanced
        },
        context,
        &cap,
    );
    assert!(!expected.allowed);
    assert_eq!(view.current()?.decide(Action::Read, "key", 100), expected);
    assert!(
        view.publish(PathTrie::compile(
            &policy, &caller, selected, context, &cap
        )?)
        .is_err()
    );
    assert_eq!(view.current()?.head_position(), 2);
    assert!(initial.decide(Action::Read, "key", 100).allowed);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn publication_never_rewinds_either_policy_or_volume_head(
        publications in prop::collection::vec((any::<u64>(), any::<u64>()), 1..8),
    ) {
        let caller = principal();
        let mut policy = super::policy(vec![rule("read", Effect::Allow, &Action::ALL, "**")]);
        let cap = capability();
        let context = Context { now: 100, approvals: 0 };
        let mut res = Resource { head: 1, ..resource(&caller.lineage, "", 1) };
        let view = AccessView::new(PathTrie::compile(&policy, &caller, res, context, &cap)?);
        let (mut revision, mut head) = (1, 1);
        for (next_revision, next_head) in publications {
            policy.version = next_revision.max(1);
            res.head = next_head.max(1);
            let expected = policy.version >= revision && res.head >= head;
            prop_assert_eq!(view.publish(PathTrie::compile(&policy, &caller, res, context, &cap)?).is_ok(), expected);
            if expected { revision = policy.version; head = res.head; }
            let current = view.current()?;
            prop_assert_eq!(current.policy_version(), revision);
            prop_assert_eq!(current.head_position(), head);
        }
    }
}

#[test]
fn read_does_not_authorize_name_or_metadata_disclosure() -> Result<(), InvalidPolicy> {
    let caller = principal();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    for policy_denies in [false, true] {
        let mut rules = vec![rule("allow", Effect::Allow, &Action::ALL, "**")];
        let cap = if policy_denies {
            rules.push(rule("hidden", Effect::Deny, &[Action::List], "secrets/**"));
            capability()
        } else {
            capability().attenuate(
                vec![Grant {
                    actions: [Action::Read].into_iter().collect(),
                    resource: pattern("**"),
                }],
                None,
            )
        };
        let policy = super::policy(rules);
        let res = resource(&caller.lineage, "secrets/key", 10);
        let trie = PathTrie::compile(&policy, &caller, res, context, &cap)?;
        let path = trie.prepare(res.path)?;
        assert!(path.decide(Action::Read, 100).allowed);
        assert!(!path.decide(Action::List, 100).allowed);
        assert!(!path.mount_mode(100).visible());
        assert!(!trie.mount_mode(res.path, 100).visible());
        assert!(path.mount_mode(100).allows(Action::Read));
    }
    Ok(())
}

#[test]
fn full_case_folding_preserves_glob_literal_matches() -> Result<(), InvalidPolicy> {
    let caller = principal();
    let cap = capability();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    for (glob, path) in [
        ("ΟΣ*", "ΟΣΑ"),
        ("ΟΣ?", "ΟΣΑ"),
        ("ΟΣ*/**", "ΟΣΑ/key"),
        ("**/ΟΣ*", "folder/ΟΣΑ"),
        ("Straße*", "STRASSE.txt"),
        ("ﬃ*", "FFI.txt"),
        ("file?.txt", "fileß.txt"),
        ("file?.txt", "filess.txt"),
        ("file?.txt", "fileﬃ.txt"),
        ("file?.txt", "fileffi.txt"),
    ] {
        let policy = super::policy(vec![
            rule("allow", Effect::Allow, &Action::ALL, "**"),
            rule("hidden", Effect::Deny, &[Action::List, Action::Read], glob),
        ]);
        let mut res = resource(&caller.lineage, path, 10);
        res.config.case_sensitivity = CaseSensitivity::ProfileFolded;
        let trie = PathTrie::compile(&policy, &caller, res, context, &cap)?;
        for action in [Action::List, Action::Read] {
            let decision = decide(&policy, &caller, action, res, context, &cap);
            assert!(!decision.allowed, "{glob}: {path}");
            assert_eq!(decision.rule_id, Some("hidden"));
            assert_eq!(trie.decide(action, path, 100), decision);
            assert_eq!(trie.prepare(path)?.decide(action, 100), decision);
        }
    }
    Ok(())
}

fn native_key(
    profile: FilesystemProfile,
    path: &str,
) -> Result<Vec<Vec<u8>>, acyclic_fs::kernel::TreePageError> {
    path.split('/')
        .map(|part| {
            let (encoding, bytes) = match profile {
                FilesystemProfile::Portable | FilesystemProfile::Browser => {
                    (NameEncoding::Utf8, part.as_bytes().to_vec())
                }
                FilesystemProfile::Posix => (NameEncoding::PosixBytes, part.as_bytes().to_vec()),
                FilesystemProfile::Windows => (
                    NameEncoding::WindowsUtf16Le,
                    part.encode_utf16().flat_map(u16::to_le_bytes).collect(),
                ),
            };
            LogicalName::new(encoding, bytes, 255).map(|name| name.case_fold_key())
        })
        .collect()
}

fn folded_atom() -> impl Strategy<Value = (String, String)> {
    prop_oneof![
        3 => prop::sample::select(vec![
            ("ΟΣ", "ος"), ("ΟΣ", "οσ"), ("ß", "ss"), ("ẞ", "SS"),
            ("ﬃ", "ffi"), ("İ", "i\u{307}"), ("K", "k"),
            ("É", "é"), ("Σ", "ς"), ("𐐀", "𐐨"), ("A", "a"), ("é", "e\u{301}"),
        ]).prop_map(|(left, right)| (left.to_owned(), right.to_owned())),
        1 => any::<char>().prop_filter("literal name scalar", |ch| !matches!(ch, '\0' | '/' | '\\' | '.' | '*' | '?' | '[' | ']' | '{' | '}'))
            .prop_map(|ch| (ch.to_string(), ch.to_uppercase().collect())),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn folded_literal_globs_agree_with_volume_name_identity(
        parts in prop::collection::vec(folded_atom(), 1..8),
        profile in prop::sample::select(vec![FilesystemProfile::Portable, FilesystemProfile::Browser, FilesystemProfile::Posix, FilesystemProfile::Windows]),
        suffix in "[a-z]{1,4}",
        capability_grant in any::<bool>(),
    ) {
        let literal = parts.iter().map(|(part, _)| part.as_str()).collect::<String>();
        let alias = parts.iter().map(|(_, part)| part.as_str()).collect::<String>();
        let caller = principal();
        let context = Context { now: 100, approvals: 0 };
        let selected = format!("{literal}/{literal}");
        let cap = if capability_grant {
            capability().attenuate(vec![Grant { actions: [Action::Read].into_iter().collect(), resource: pattern(&selected) }], None)
        } else { capability() };
        let policy = super::policy(vec![rule("literal", Effect::Allow, &[Action::Read], if capability_grant { "**" } else { &selected })]);
        for candidate in [format!("{alias}/{alias}"), format!("{alias}/{alias}{suffix}")] {
            let mut res = resource(&caller.lineage, &candidate, 10);
            res.config.profile = profile;
            res.config.case_sensitivity = CaseSensitivity::ProfileFolded;
            let expected = native_key(profile, &selected)? == native_key(profile, &candidate)?;
            let reference = decide(&policy, &caller, Action::Read, res, context, &cap);
            prop_assert_eq!(reference.allowed, expected);
            let trie = PathTrie::compile(&policy, &caller, res, context, &cap)?;
            prop_assert_eq!(trie.decide(Action::Read, &candidate, 100), reference);
            prop_assert_eq!(trie.prepare(&candidate)?.decide(Action::Read, 100), reference);
        }
    }
}

#[test]
fn published_policy_refreshes_new_opens_and_preserves_pinned_handles()
-> Result<(), Box<dyn std::error::Error>> {
    let caller = principal();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let mut cap = capability();
    cap.expires_at = Some(200);
    let res = resource(&caller.lineage, "", 10);
    let mut policy = super::policy(vec![rule("allow", Effect::Allow, &Action::ALL, "**")]);
    let view = AccessView::new(PathTrie::compile(&policy, &caller, res, context, &cap)?);
    let name = LogicalName::new(NameEncoding::Utf8, b"public".to_vec(), 255)?;
    let path = acyclic_fs::kernel::NamespacePath::new(vec![name], res.config.limits)?;
    let opened = view.current()?.pin(&path)?;
    assert!(opened.decide(Action::Read, 100).allowed);
    policy.version = 2;
    policy
        .layers
        .push(vec![rule("revoked", Effect::Deny, &Action::ALL, "public")]);
    view.publish(PathTrie::compile(&policy, &caller, res, context, &cap)?)?;
    let reopened = view.current()?.pin(&path)?;
    assert!(!reopened.decide(Action::Read, 100).allowed);
    assert_eq!(reopened.decide(Action::Read, 100).rule_id, Some("revoked"));
    assert!(opened.decide(Action::Read, 199).allowed);
    assert!(!opened.decide(Action::Read, 200).allowed);
    assert!(!opened.mount_mode(200).visible());
    policy.version = 1;
    policy.layers.pop();
    assert!(
        view.publish(PathTrie::compile(&policy, &caller, res, context, &cap)?)
            .is_err()
    );
    policy.version = 2;
    let mut other_volume = res;
    other_volume.volume = "different-volume";
    assert!(
        view.publish(PathTrie::compile(
            &policy,
            &caller,
            other_volume,
            context,
            &cap
        )?)
        .is_err()
    );
    let mut other_subject = caller.clone();
    other_subject.id = "different-principal".into();
    assert!(
        view.publish(PathTrie::compile(
            &policy,
            &other_subject,
            res,
            context,
            &cap
        )?)
        .is_err()
    );
    let mut wider = cap.clone();
    wider.expires_at = Some(300);
    assert!(
        view.publish(PathTrie::compile(&policy, &caller, res, context, &wider)?)
            .is_err()
    );
    let mut other = res;
    other.config.case_sensitivity = CaseSensitivity::ProfileFolded;
    assert!(
        view.publish(PathTrie::compile(&policy, &caller, other, context, &cap)?)
            .is_err()
    );
    assert!(
        !view
            .current()?
            .pin(&path)?
            .decide(Action::Read, 100)
            .allowed
    );
    Ok(())
}

fn native_exact_grant(
    profile: FilesystemProfile,
    capability_grant: bool,
) -> Result<(), InvalidPolicy> {
    let caller = principal();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let policy = super::policy(vec![rule(
        "allow",
        Effect::Allow,
        &[Action::Read],
        if capability_grant { "**" } else { "public" },
    )]);
    let cap = if capability_grant {
        capability().attenuate(
            vec![Grant {
                actions: [Action::Read].into_iter().collect(),
                resource: pattern("public"),
            }],
            None,
        )
    } else {
        capability()
    };
    let mut res = resource(&caller.lineage, "public", 10);
    res.config.profile = profile;
    res.config.case_sensitivity = CaseSensitivity::ProfileFolded;
    let encoding = match profile {
        FilesystemProfile::Posix => NameEncoding::PosixBytes,
        FilesystemProfile::Windows => NameEncoding::WindowsUtf16Le,
        _ => return Err(InvalidPolicy("fixture requires a native profile")),
    };
    let name = |text: &str| {
        LogicalName::new(
            encoding,
            if encoding == NameEncoding::WindowsUtf16Le {
                text.encode_utf16().flat_map(u16::to_le_bytes).collect()
            } else {
                text.as_bytes().to_vec()
            },
            res.config.limits.maximum_component_bytes,
        )
        .map_err(|_| InvalidPolicy("invalid fixture name"))
    };
    assert_ne!(
        name("public")?.case_fold_key(),
        name("PUBLIC")?.case_fold_key()
    );
    let trie = PathTrie::compile(&policy, &caller, res, context, &cap)?;
    for (path, allowed) in [("public", true), ("PUBLIC", false)] {
        res.path = path;
        let reference = decide(&policy, &caller, Action::Read, res, context, &cap);
        assert_eq!(
            reference.allowed, allowed,
            "{profile:?}, capability={capability_grant}: {path}"
        );
        assert_eq!(trie.decide(Action::Read, path, 100), reference);
        assert_eq!(
            trie.prepare("")?
                .child_name(&name(path)?)?
                .decide(Action::Read, 100),
            reference
        );
    }
    Ok(())
}

#[test]
fn posix_policy_grants_preserve_native_identity() -> Result<(), InvalidPolicy> {
    native_exact_grant(FilesystemProfile::Posix, false)
}

#[test]
fn posix_capability_grants_preserve_native_identity() -> Result<(), InvalidPolicy> {
    native_exact_grant(FilesystemProfile::Posix, true)
}

#[test]
fn windows_policy_grants_preserve_native_identity() -> Result<(), InvalidPolicy> {
    native_exact_grant(FilesystemProfile::Windows, false)
}

#[test]
fn windows_capability_grants_preserve_native_identity() -> Result<(), InvalidPolicy> {
    native_exact_grant(FilesystemProfile::Windows, true)
}

#[test]
fn native_name_views_reject_unsupported_encodings_and_names() -> Result<(), InvalidPolicy> {
    let caller = principal();
    let policy = super::policy(vec![rule("allow", Effect::Allow, &Action::ALL, "**")]);
    let cap = capability();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    for (profile, encoding, invalid) in [
        (
            FilesystemProfile::Posix,
            NameEncoding::PosixBytes,
            vec![0xff],
        ),
        (
            FilesystemProfile::Windows,
            NameEncoding::WindowsUtf16Le,
            vec![0, 0xd8],
        ),
    ] {
        let mut res = resource(&caller.lineage, "", 10);
        res.config.profile = profile;
        res.config.unicode = UnicodePolicy::RequireNfc;
        let trie = PathTrie::compile(&policy, &caller, res, context, &cap)?;
        let root = trie.prepare("")?;
        let utf8 = LogicalName::new(NameEncoding::Utf8, b"public".to_vec(), 255)
            .map_err(|_| InvalidPolicy("invalid fixture name"))?;
        assert!(root.child_name(&utf8).is_err());
        let non_unicode = LogicalName::new(encoding, invalid, 255)
            .map_err(|_| InvalidPolicy("invalid fixture name"))?;
        assert!(root.child_name(&non_unicode).is_err());
        // NFC admission applies only to UTF-8 logical names in the filesystem.
        res.path = "e\u{301}";
        assert!(decide(&policy, &caller, Action::Read, res, context, &cap).allowed);
        assert!(trie.decide(Action::Read, res.path, 100).allowed);
    }
    Ok(())
}

#[test]
fn windows_access_paths_obey_encoded_name_and_path_limits() -> Result<(), InvalidPolicy> {
    let caller = principal();
    let policy = super::policy(vec![rule("allow", Effect::Allow, &Action::ALL, "**")]);
    let cap = capability();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let mut res = resource(&caller.lineage, "", 10);
    res.config.profile = FilesystemProfile::Windows;
    res.config.limits.maximum_component_bytes = 4;
    res.config.limits.maximum_path_bytes = 8;
    let trie = PathTrie::compile(&policy, &caller, res, context, &cap)?;
    for (path, allowed) in [
        ("ab", true),
        ("abc", false),
        ("ab/c", true),
        ("ab/cd", false),
        ("😀", true),
    ] {
        res.path = path;
        assert_eq!(
            decide(&policy, &caller, Action::Read, res, context, &cap).allowed,
            allowed,
            "{path}"
        );
        assert_eq!(
            trie.decide(Action::Read, path, 100).allowed,
            allowed,
            "{path}"
        );
    }
    Ok(())
}

#[test]
fn volume_name_keys_apply_to_rules_and_capabilities() -> Result<(), InvalidPolicy> {
    let caller = principal();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let cap = capability();
    for (deny, alias) in [
        ("secrets/**", "Secrets/key"),
        ("ÉCOLE/**", "école/key"),
        ("ΟΣ/**", "ος/key"),
        ("İ/**", "i\u{307}/key"),
    ] {
        let policy = super::policy(vec![
            rule("allow", Effect::Allow, &Action::ALL, "**"),
            rule("hidden", Effect::Deny, &[Action::List, Action::Read], deny),
        ]);
        let mut res = resource(&caller.lineage, alias, 10);
        res.config.case_sensitivity = CaseSensitivity::ProfileFolded;
        let trie = PathTrie::compile(&policy, &caller, res, context, &cap)?;
        for action in [Action::List, Action::Read] {
            let reference = decide(&policy, &caller, action, res, context, &cap);
            assert!(!reference.allowed, "{deny}: {alias}");
            assert_eq!(reference.rule_id, Some("hidden"));
            assert_eq!(trie.prepare(alias)?.decide(action, 100), reference);
        }
    }
    let policy = super::policy(vec![
        rule("allow", Effect::Allow, &Action::ALL, "**"),
        rule("hidden", Effect::Deny, &[Action::Read], "secrets/**"),
    ]);
    let res = resource(&caller.lineage, "Secrets/key", 10);
    assert!(decide(&policy, &caller, Action::Read, res, context, &cap).allowed);
    let cap = cap.attenuate(
        vec![Grant {
            actions: [Action::Read].into_iter().collect(),
            resource: pattern("SECRETS/**"),
        }],
        None,
    );
    let mut res = resource(&caller.lineage, "secrets/key", 10);
    res.config.case_sensitivity = CaseSensitivity::ProfileFolded;
    let policy = super::policy(vec![rule("allow", Effect::Allow, &Action::ALL, "**")]);
    assert!(decide(&policy, &caller, Action::Read, res, context, &cap).allowed);
    assert!(
        PathTrie::compile(&policy, &caller, res, context, &cap)?
            .prepare(res.path)?
            .decide(Action::Read, 100)
            .allowed
    );
    Ok(())
}

#[test]
fn unicode_admission_matches_the_volume() -> Result<(), InvalidPolicy> {
    let caller = principal();
    let policy = super::policy(vec![rule("allow", Effect::Allow, &Action::ALL, "**")]);
    let cap = capability();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    for (unicode, path, allowed) in [
        (UnicodePolicy::RequireNfc, "é/key", true),
        (UnicodePolicy::RequireNfc, "e\u{301}/key", false),
        (UnicodePolicy::Preserve, "e\u{301}/key", true),
    ] {
        let mut res = resource(&caller.lineage, path, 10);
        res.config.unicode = unicode;
        let trie = PathTrie::compile(&policy, &caller, res, context, &cap)?;
        assert_eq!(
            decide(&policy, &caller, Action::Read, res, context, &cap).allowed,
            allowed
        );
        assert_eq!(trie.decide(Action::Read, path, 100).allowed, allowed);
    }
    Ok(())
}

#[test]
fn review_exponential_globs_are_bounded_in_policy_and_delegation() {
    let caller = principal();
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let adversarial = format!("**/a{}", "/?".repeat(24));
    let policy = super::policy(vec![rule("allow", Effect::Allow, &Action::ALL, "**")]);
    let mut restricted = policy.clone();
    restricted.layers.push(vec![rule(
        "expansion",
        Effect::Allow,
        &Action::ALL,
        &adversarial,
    )]);
    let cap = capability();
    assert!(
        PathTrie::compile(
            &restricted,
            &caller,
            resource(&caller.lineage, "", 10),
            context,
            &cap
        )
        .is_err()
    );
    let delegated = cap.attenuate(
        vec![Grant {
            actions: Action::ALL.into_iter().collect(),
            resource: pattern(&adversarial),
        }],
        None,
    );
    assert!(
        PathTrie::compile(
            &policy,
            &caller,
            resource(&caller.lineage, "", 10),
            context,
            &delegated
        )
        .is_err()
    );
}

#[test]
fn prepared_paths_retain_deadlines_and_view_identity() -> Result<(), InvalidPolicy> {
    let caller = principal();
    let policy = super::policy(vec![rule("allow", Effect::Allow, &Action::ALL, "**")]);
    let mut cap = capability();
    cap.expires_at = Some(110);
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let trie = PathTrie::compile(
        &policy,
        &caller,
        resource(&caller.lineage, "", 10),
        context,
        &cap,
    )?;
    assert_eq!(trie.valid_until(), Some(110));
    let path = trie
        .prepare("")?
        .child("agents")?
        .child("worker")?
        .child("é.txt")?;
    assert!(path.decide(Action::Read, 109).allowed);
    assert!(!path.decide(Action::Read, 110).allowed);
    assert!(!path.decide(Action::Read, 99).allowed);
    assert_eq!(
        path.decide(Action::Read, 109),
        trie.decide(Action::Read, "agents/worker/é.txt", 109)
    );
    let revoked = Policy {
        version: 2,
        layers: Vec::new(),
        ..policy
    };
    let revoked = PathTrie::compile(
        &revoked,
        &caller,
        resource(&caller.lineage, "", 10),
        context,
        &cap,
    )?;
    assert!(
        !revoked
            .prepare("agents/worker/é.txt")?
            .decide(Action::Read, 109)
            .allowed
    );
    assert!(path.decide(Action::Read, 109).allowed);
    assert!(trie.prepare("a").and_then(|p| p.child("../b")).is_err());
    assert!(trie.prepare("a//b").is_err());
    Ok(())
}

#[test]
fn full_case_fold_question_preserves_native_scalar_aliases()
-> Result<(), Box<dyn std::error::Error>> {
    let caller = principal();
    let cap = capability();
    let policy = super::policy(vec![
        rule("allow", Effect::Allow, &Action::ALL, "**"),
        rule("single", Effect::Deny, &[Action::Read], "file?.txt"),
    ]);
    let context = Context {
        now: 100,
        approvals: 0,
    };
    let mut res = resource(&caller.lineage, "", 10);
    res.config.case_sensitivity = CaseSensitivity::ProfileFolded;
    res.config.unicode = UnicodePolicy::Preserve;
    let trie = PathTrie::compile(&policy, &caller, res, context, &cap)?;
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(128));
    let scalar = prop_oneof![
        Just('ß'),
        Just('ﬃ'),
        Just('ΐ'),
        Just('İ'),
        any::<char>().prop_filter("alphabetic name", |ch| ch.is_alphabetic())
    ];
    runner.run(&scalar, |ch| {
        let path = format!("file{ch}.txt");
        let name = LogicalName::new(NameEncoding::Utf8, path.as_bytes().to_vec(), 255)
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
        let alias = String::from_utf8(name.case_fold_key())
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
        for path in [&path, &alias] {
            let res = Resource { path, ..res };
            let decision = decide(&policy, &caller, Action::Read, res, context, &cap);
            prop_assert!(!decision.allowed, "{path}");
            prop_assert_eq!(trie.decide(Action::Read, path, 100), decision);
        }
        Ok(())
    })?;
    assert!(trie.decide(Action::Read, "fileab.txt", 100).allowed);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn policy_fence_and_native_custody_preserve_pinned_views(origin in any::<[u8; 32]>()) {
      // Exhaust every fence/history combination for each shrinking storage identity.
      for mask in 0u8..16 {
        let mut next = origin;
        next[0] ^= u8::from(mask & 1 != 0);
        let generation_changed = mask & 2 != 0;
        let restricted = mask & 4 != 0;
        let history = mask & 8 != 0;
        let caller = principal();
        let mut policy = super::policy(vec![rule("read", Effect::Allow, &[Action::Read], "**")]);
        if history { policy.layers[0][0].actions.insert(Action::ReadHistory); }
        if restricted { policy.layers[0][0].resource.generations = Generations::Head; }
        let mut cap = capability();
        for grant in cap.layers.iter_mut().flatten() { grant.resource.generations = Generations::All; }
        let generation = |byte| acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes([byte; 32]));
        let revision = |byte| acyclic_fs::kernel::MetadataField::Value(acyclic_fs::ObjectId {
            kind: acyclic_fs::ObjectKind::TreePage, digest: acyclic_fs::Digest::from_bytes(byte),
        });
        let compile = || PathTrie::compile(&policy, &caller, Resource { head: 1, ..resource(&caller.lineage, "", 1) }, Context { now: 100, approvals: 0 }, &cap)
            .map(|view| view.with_policy_revision(generation(1), revision(origin)));
        let first = compile()?;
        prop_assert_eq!(first.matches_policy_revision(generation(if generation_changed { 2 } else { 1 }), revision(next)),
            origin == next && (!(restricted || !history) || !generation_changed));
        let cancel = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancel)
            .map_err(|(cause, _)| proptest::test_runner::TestCaseError::fail(cause.to_string()))?;
        let baseline = work.live_allocation_bytes();
        prop_assert_eq!(baseline, acyclic_fs::NativeMaterializationWork::accounting_state_bytes());
        let bytes = first.logical_owned_bytes();
        let custody = work.claim(bytes, 1, acyclic_fs::WorkCounters::UNCHARGED)?;
        let view = AccessView::new_native(first, custody)?;
        let pinned = view.current()?;
        let before = work.live_allocation_bytes();
        let next = compile()?;
        view.publish_native(next, work.claim(bytes, 1, acyclic_fs::WorkCounters::UNCHARGED)?)?;
        prop_assert!(work.live_allocation_bytes() > before);
        drop(view);
        prop_assert!(work.live_allocation_bytes() >= baseline + bytes);
        prop_assert!(pinned.decide(Action::Read, "file", 100).allowed);
        drop(pinned);
        prop_assert_eq!(work.live_allocation_bytes(), baseline);
      }
    }
}

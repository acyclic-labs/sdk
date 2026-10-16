use super::*;
use acyclic_fs::kernel::{LogicalName, NameEncoding};
use acyclic_fs::model::{CaseSensitivity, FilesystemProfile, UnicodePolicy};

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

//! Shared parser/decision oracle for proptest, corpus replay and cargo-fuzz.
use acyclic_fs::access::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub policy: Policy,
    pub capability: Capability,
    pub principal: Principal,
    pub path: String,
    pub profile: u8,
    pub folded: bool,
    pub now: u64,
    #[serde(default)]
    pub native_name: Option<(u8, Vec<u8>)>,
    #[serde(default)]
    pub publications: Vec<Publication>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Publication {
    pub version: u64,
    pub head: u64,
    pub binding: u8,
    pub data_generation: Option<[u8; 32]>,
}

pub fn check(bytes: &[u8]) {
    assert!(check_decisions::<false>(bytes).is_ok());
}

#[cfg(test)]
pub fn check_with_corrupted_decision(bytes: &[u8]) {
    assert!(check_decisions::<true>(bytes).is_ok());
}

fn check_decisions<const CORRUPT_DECISION: bool>(
    bytes: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    // Scope labels are also accepted directly, including malformed JSON chains.
    if let Ok(label) = std::str::from_utf8(bytes)
        && let Ok(Some(capability)) = Capability::from_scope_label(label)
    {
        assert_eq!(
            Capability::from_scope_label(&capability.to_scope_label()?)?,
            Some(capability)
        );
    }
    let Ok(input) = serde_json::from_slice::<Input>(bytes) else {
        return Ok(());
    };
    let encoded = serde_json::to_vec(&input)?;
    assert_eq!(serde_json::from_slice::<Input>(&encoded)?, input);
    let label = input.capability.to_scope_label()?;
    assert_eq!(
        Capability::from_scope_label(&label)?,
        Some(input.capability.clone())
    );
    let mut config = acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Durable);
    config.profile = match input.profile % 4 {
        0 => acyclic_fs::model::FilesystemProfile::Portable,
        1 => acyclic_fs::model::FilesystemProfile::Browser,
        2 => acyclic_fs::model::FilesystemProfile::Posix,
        _ => acyclic_fs::model::FilesystemProfile::Windows,
    };
    config.unicode = acyclic_fs::model::UnicodePolicy::Preserve;
    if input.folded {
        config.case_sensitivity = acyclic_fs::model::CaseSensitivity::ProfileFolded;
    }
    let resource = Resource {
        config,
        lineage: &input.principal.lineage,
        volume: "project",
        generation: 10,
        head: 10,
        path: &input.path,
    };
    let context = Context {
        now: input.now,
        approvals: 2,
    };
    let Ok(trie) = PathTrie::compile(
        &input.policy,
        &input.principal,
        resource,
        context,
        &input.capability,
    ) else {
        return Ok(()); // Invalid input and policies exceeding compilation budgets fail closed.
    };
    let mut work = usize::MAX;
    let shared = CompiledPolicy::compile_with_work(&input.policy, config, &mut work, |_| Ok(()));
    let shared = shared.and_then(|policy| {
        std::sync::Arc::new(policy).bind_with_work(
            &input.principal,
            resource,
            context,
            &input.capability,
            &mut work,
            |_| Ok(()),
        )
    });
    // The same bounded construction may refuse a larger principal-independent
    // automaton. Successful constructions must match every decision and name.
    let shared = shared.ok().map(std::sync::Arc::new);
    let published = AccessView::new(trie);
    let trie = published.current()?;
    let prepared = trie.prepare(&input.path);
    for action in Action::ALL {
        let expected = decide(
            &input.policy,
            &input.principal,
            action,
            resource,
            context,
            &input.capability,
        );
        #[cfg(test)]
        let expected = if CORRUPT_DECISION {
            Decision {
                allowed: !expected.allowed,
                ..expected
            }
        } else {
            expected
        };
        assert_eq!(trie.decide(action, &input.path, input.now), expected);
        if let Some(shared) = &shared {
            assert_eq!(shared.decide(action, &input.path, input.now), expected);
        }
        if let Ok(path) = prepared {
            assert_eq!(path.decide(action, input.now), expected);
        }
    }
    check_native_name(&input, &trie, resource, context)?;
    if let Some(shared) = &shared {
        check_native_name(&input, shared, resource, context)?;
    }
    check_publications(&input, &published, resource, context)?;
    Ok(())
}

fn check_publications(
    input: &Input,
    published: &AccessView,
    resource: Resource<'_>,
    context: Context,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut version = input.policy.version;
    let mut head = resource.head;
    for update in &input.publications {
        let mut policy = input.policy.clone();
        policy.version = update.version;
        let mut principal = input.principal.clone();
        let mut capability = input.capability.clone();
        let mut next = resource;
        next.head = update.head;
        // Each nonzero selector changes one independently authenticated field.
        let binding = update.binding % 5;
        match binding {
            1 => next.volume = "other-project",
            2 => principal.id.push_str("/other"),
            3 => capability.layers.push(Vec::new()),
            4 => {
                use acyclic_fs::model::CaseSensitivity;
                next.config.case_sensitivity = match next.config.case_sensitivity {
                    CaseSensitivity::Sensitive => CaseSensitivity::ProfileFolded,
                    CaseSensitivity::ProfileFolded => CaseSensitivity::Sensitive,
                };
            }
            _ => {}
        }
        let Ok(mut candidate) = PathTrie::compile(&policy, &principal, next, context, &capability)
        else {
            continue;
        };
        if let Some(generation) = update.data_generation {
            candidate = candidate.with_data_generation(acyclic_fs::GenerationId::new(
                acyclic_fs::Digest::from_bytes(generation),
            ));
        }
        let previous = published.current()?;
        // This relation uses the input history, not the production binding
        // predicate or the result of a second publication implementation.
        let accepted = binding == 0
            && update.data_generation.is_none()
            && update.version >= version
            && update.head >= head;
        assert_eq!(
            published.publish(candidate).is_ok(),
            accepted,
            "published access changed binding or rolled back a revision"
        );
        let current = published.current()?;
        if accepted {
            version = update.version;
            head = update.head;
        } else {
            assert!(std::sync::Arc::ptr_eq(&previous, &current));
        }
        assert_eq!(current.policy_version(), version);
        assert_eq!(current.head_position(), head);
    }
    Ok(())
}

fn check_native_name(
    input: &Input,
    trie: &std::sync::Arc<PathTrie>,
    resource: Resource<'_>,
    context: Context,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = resource.config;
    if let Some((encoding, bytes)) = input.native_name.clone() {
        use acyclic_fs::kernel::{LogicalName, NameEncoding};
        let encoding = match encoding % 3 {
            0 => NameEncoding::Utf8,
            1 => NameEncoding::PosixBytes,
            _ => NameEncoding::WindowsUtf16Le,
        };
        if let Ok(name) = LogicalName::new(
            encoding,
            bytes.clone(),
            config.limits.maximum_component_bytes,
        ) {
            assert_eq!(name.as_bytes(), bytes);
            let decoded = match encoding {
                NameEncoding::Utf8 | NameEncoding::PosixBytes => String::from_utf8(bytes).ok(),
                NameEncoding::WindowsUtf16Le => String::from_utf16(
                    &bytes
                        .chunks_exact(2)
                        .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                        .collect::<Vec<_>>(),
                )
                .ok(),
            };
            let native = trie.prepare("")?.child_name(&name);
            let expected_encoding = match config.profile {
                acyclic_fs::model::FilesystemProfile::Portable
                | acyclic_fs::model::FilesystemProfile::Browser => NameEncoding::Utf8,
                acyclic_fs::model::FilesystemProfile::Posix => NameEncoding::PosixBytes,
                acyclic_fs::model::FilesystemProfile::Windows => NameEncoding::WindowsUtf16Le,
            };
            assert_eq!(
                native.is_ok(),
                encoding == expected_encoding
                    && decoded
                        .as_ref()
                        .is_some_and(|text| trie.prepare(text).is_ok())
            );
            let namespace = acyclic_fs::kernel::NamespacePath::new(vec![name], config.limits)?;
            let namespace_access = trie.prepare_namespace(&namespace);
            let pinned = trie.pin(&namespace);
            assert_eq!(namespace_access.is_ok(), native.is_ok());
            assert_eq!(pinned.is_ok(), native.is_ok());
            if let (Ok(native), Some(path)) = (native, decoded) {
                for action in Action::ALL {
                    let expected = native.decide(action, input.now);
                    assert_eq!(
                        namespace_access
                            .as_ref()
                            .map_err(Clone::clone)?
                            .decide(action, input.now),
                        expected
                    );
                    assert_eq!(
                        pinned
                            .as_ref()
                            .map_err(Clone::clone)?
                            .decide(action, input.now),
                        expected
                    );
                    assert_eq!(
                        expected,
                        decide(
                            &input.policy,
                            &input.principal,
                            action,
                            Resource {
                                path: &path,
                                ..resource
                            },
                            context,
                            &input.capability
                        )
                    );
                }
            }
        }
    }
    Ok(())
}

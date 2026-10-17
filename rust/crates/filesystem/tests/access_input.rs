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
}

pub fn check(bytes: &[u8]) {
    check_decisions::<false>(bytes);
}

#[cfg(test)]
pub fn check_with_corrupted_decision(bytes: &[u8]) {
    check_decisions::<true>(bytes);
}

fn check_decisions<const CORRUPT_DECISION: bool>(bytes: &[u8]) {
    // Scope labels are also accepted directly, including malformed JSON chains.
    if let Ok(label) = std::str::from_utf8(bytes)
        && let Ok(Some(capability)) = Capability::from_scope_label(label)
    {
        assert_eq!(
            Capability::from_scope_label(&capability.to_scope_label().expect("serialize scope"))
                .expect("parse scope"),
            Some(capability)
        );
    }
    let Ok(input) = serde_json::from_slice::<Input>(bytes) else {
        return;
    };
    let encoded = serde_json::to_vec(&input).expect("serialize access input");
    assert_eq!(
        serde_json::from_slice::<Input>(&encoded).expect("parse access input"),
        input
    );
    let label = input.capability.to_scope_label().expect("serialize scope");
    assert_eq!(
        Capability::from_scope_label(&label).expect("parse scope"),
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
        return; // Invalid input and policies exceeding compilation budgets fail closed.
    };
    let trie = std::sync::Arc::new(trie);
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
        if let Ok(path) = prepared {
            assert_eq!(path.decide(action, input.now), expected);
        }
    }
    if let Some((encoding, bytes)) = input.native_name {
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
            let native = trie.prepare("").expect("root").child_name(&name);
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
            let namespace = acyclic_fs::kernel::NamespacePath::new(vec![name], config.limits)
                .expect("one validated native component");
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
                            .expect("same native namespace")
                            .decide(action, input.now),
                        expected
                    );
                    assert_eq!(
                        pinned
                            .as_ref()
                            .expect("same pinned namespace")
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
}

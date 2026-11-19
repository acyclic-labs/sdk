//! What decoding a [`Policy`] record can allocate, counted from its bytes
//! before anything is allocated.
//!
//! The derived decoder reads `serde_json` directly. Every policy type is a
//! plain record or an externally tagged enum, so no `Content` copy of the
//! record is ever buffered. It allocates the decoded value, serde_json's
//! escape scratch, and on refusal one rendered error.

use super::{Policy, Rule};
use std::mem::size_of;

/// Rendered text and boxed record of one refusal, excluding the echoed
/// input: the longest expected-name list of any policy type plus its fixed
/// phrasing. `diagnostic_labels_cover_every_policy_refusal` proves it.
const DIAGNOSTIC_LABEL_BYTES: u64 = 1024;

/// One B-tree node of `slot`-byte entries: eleven slots and a header, plus
/// twelve edges when internal. The first entry allocates the root leaf, and
/// beyond eleven entries every non-root node holds at least five, so a tree
/// never has more nodes than entries.
const fn tree_node(slot: usize) -> u64 {
    (slot * 11 + 32 + 12 * size_of::<usize>()) as u64
}

/// Bytes a vector of `count` elements of `slot` bytes holds at once while it
/// grows by doubling from four slots: at most twice `count`, plus the buffer
/// it moves out of.
const fn vector(slot: usize) -> u64 {
    4 * slot as u64
}

impl Policy {
    /// Upper bound on bytes live at once while `serde_json::from_slice`
    /// decodes `bytes` as a policy, whether it accepts or refuses them.
    /// `None` only on arithmetic overflow.
    pub(crate) fn decode_allocation_bound(bytes: &[u8]) -> Option<u64> {
        // Lexical counts outside strings: each record is a `{`, each map entry
        // a `:`, and each array element follows a `[` or a `,`. `text` counts
        // the raw bytes inside strings, which no decoded string exceeds.
        let (mut records, mut entries, mut elements, mut text) = (0u64, 0u64, 0u64, 0u64);
        let (mut quoted, mut escaped) = (false, false);
        for &byte in bytes {
            if quoted {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    quoted = false;
                    continue;
                }
                text += 1;
                continue;
            }
            match byte {
                b'"' => quoted = true,
                b'{' => records += 1,
                b':' => entries += 1,
                b'[' | b',' => elements += 1,
                _ => {}
            }
        }
        // Each decoded string is exact-capacity and no longer than its raw
        // text. The escape scratch doubles up to the longest string, moving
        // out of its previous buffer.
        let strings = text;
        let scratch = text.checked_mul(3)?.checked_add(8)?;
        // Rules are records; layers are arrays.
        let rules = records.checked_mul(vector(size_of::<Rule>()))?;
        let layers = elements.checked_mul(vector(size_of::<Vec<Rule>>()))?;
        // Attribute maps and action sets: one node per entry at most, each no
        // larger than a node of the widest entry.
        let trees = entries
            .checked_add(elements)?
            .checked_mul(tree_node(size_of::<(String, String)>()))?;
        // A refusal echoes at most one input string, escaped at under three
        // bytes per raw byte, and renders through a doubling buffer that is
        // then shrunk into its box.
        let refusal = text
            .checked_mul(3)?
            .checked_add(DIAGNOSTIC_LABEL_BYTES)?
            .checked_mul(3)?;
        strings
            .checked_add(scratch)?
            .checked_add(rules)?
            .checked_add(layers)?
            .checked_add(trees)?
            .checked_add(refusal)
    }
}

#[cfg(test)]
#[cfg(any(not(unit_test_split), unit_test_foundation))]
mod tests {
    use super::super::{
        Action, Conditions, Effect, GenerationRange, Generations, PrincipalKind, PrincipalPattern,
        ResourcePattern,
    };
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn diagnostic_labels_cover_every_policy_refusal() {
        let rule = |principal: &str,
                    resource: &str,
                    conditions: &str,
                    effect: &str,
                    action: &str| {
            format!(
                r#"{{"schema_version":1,"version":1,"layers":[[{{"id":"r","effect":"{effect}","principal":{principal},"actions":["{action}"],"resource":{resource},"conditions":{conditions}}}]]}}"#
            )
        };
        let principal = r#"{"attributes":{}}"#;
        let resource = r#"{"lineage_root":null,"volume":null,"generations":"all","path":"**"}"#;
        // An unknown name in every record and every enum renders that type's
        // complete expected-name list.
        let refusals = [
            r#"{"x":0}"#.to_owned(),
            rule(principal, resource, "{}", "allow", "read").replace(r#""id""#, r#""x":0,"id""#),
            rule(
                r#"{"x":0,"attributes":{}}"#,
                resource,
                "{}",
                "allow",
                "read",
            ),
            rule(
                principal,
                &resource.replace(r#""path""#, r#""x":0,"path""#),
                "{}",
                "allow",
                "read",
            ),
            rule(principal, resource, r#"{"x":0}"#, "allow", "read"),
            rule(
                principal,
                resource,
                r#"{"generations":{"x":0}}"#,
                "allow",
                "read",
            ),
            rule(principal, resource, "{}", "x", "read"),
            rule(principal, resource, "{}", "allow", "x"),
            rule(principal, resource, r#"{"relation":"x"}"#, "allow", "read"),
            rule(
                r#"{"kind":"x","attributes":{}}"#,
                resource,
                "{}",
                "allow",
                "read",
            ),
            rule(
                principal,
                &resource.replace(r#""all""#, r#""x""#),
                "{}",
                "allow",
                "read",
            ),
        ];
        for refusal in refusals {
            let error = serde_json::from_slice::<Policy>(refusal.as_bytes())
                .expect_err("unknown names are refused");
            // The rendered text, its boxed record and the one-byte echoed name.
            let rendered = error.to_string().len() + 64;
            assert!(
                rendered as u64 <= DIAGNOSTIC_LABEL_BYTES,
                "{rendered} bytes: {error}"
            );
        }
    }

    fn action() -> impl Strategy<Value = Action> {
        prop::sample::select(vec![
            Action::List,
            Action::Read,
            Action::Create,
            Action::Write,
            Action::Append,
            Action::Delete,
            Action::Rename,
            Action::Fork,
            Action::MergeInto,
            Action::ReadHistory,
            Action::Lock,
            Action::Admin,
            Action::Call,
            Action::Spawn,
        ])
    }

    /// Strings with escapes, multi-byte and control characters, or nothing.
    fn text() -> impl Strategy<Value = String> {
        prop::collection::vec(
            prop_oneof![
                any::<char>(),
                Just('"'),
                Just('\\'),
                Just('\u{1}'),
                Just('\u{200b}'),
            ],
            0..10,
        )
        .prop_map(|characters| characters.into_iter().collect())
    }

    fn generations() -> impl Strategy<Value = Generations> {
        prop_oneof![
            Just(Generations::Head),
            Just(Generations::All),
            (any::<u64>(), any::<u64>())
                .prop_map(|(first, last)| Generations::Range(GenerationRange { first, last })),
        ]
    }

    fn rule() -> impl Strategy<Value = Rule> {
        (
            text(),
            any::<bool>(),
            (
                prop::option::of(prop::sample::select(vec![
                    PrincipalKind::User,
                    PrincipalKind::Service,
                ])),
                prop::option::of(text()),
                prop::option::of(text()),
                prop::collection::btree_map(text(), text(), 0..40),
            ),
            prop::collection::btree_set(action(), 0..15),
            (
                prop::option::of(text()),
                prop::option::of(text()),
                generations(),
                text(),
            ),
            (
                prop::option::of(any::<u64>()),
                prop::option::of(text()),
                prop::option::of(text()),
                prop::option::of(any::<u32>()),
            ),
        )
            .prop_map(
                |(
                    id,
                    allow,
                    (kind, principal, group, attributes),
                    actions,
                    resource,
                    conditions,
                )| {
                    Rule {
                        id,
                        effect: if allow { Effect::Allow } else { Effect::Deny },
                        principal: PrincipalPattern {
                            kind,
                            id: principal,
                            group,
                            attributes,
                        },
                        actions,
                        resource: ResourcePattern {
                            lineage_root: resource.0,
                            volume: resource.1,
                            generations: resource.2,
                            path: resource.3,
                        },
                        conditions: Conditions {
                            expires_at: conditions.0,
                            generations: None,
                            relation: None,
                            role: conditions.1,
                            tag: conditions.2,
                            approvals: conditions.3,
                        },
                    }
                },
            )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        /// Measured on the real allocator: no accepted or refused policy decode
        /// holds more than its admitted bound at any instant.
        #[test]
        fn decode_bound_covers_every_measured_decode(
            layers in prop::collection::vec(prop::collection::vec(rule(), 0..12), 0..4),
            version in any::<u64>(),
            cut in any::<prop::sample::Index>(),
        ) {
            let policy = Policy { schema_version: 1, version, layers };
            let compact = serde_json::to_vec(&policy).unwrap();
            let pretty = serde_json::to_vec_pretty(&policy).unwrap();
            let truncated = compact[..cut.index(compact.len())].to_vec();
            let unknown = [&compact[..compact.len() - 1], br#","x":0}"#.as_slice()].concat();
            for bytes in [compact, pretty, truncated, unknown] {
                let bound = Policy::decode_allocation_bound(&bytes).unwrap();
                let measured = acyclic_test_support::measure(|| {
                    drop(serde_json::from_slice::<Policy>(&bytes));
                });
                prop_assert!(
                    measured.bytes_max <= bound,
                    "{} bytes measured over a {bound}-byte bound for {} input bytes",
                    measured.bytes_max,
                    bytes.len()
                );
            }
        }
    }
}

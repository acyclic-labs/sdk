use super::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeStruct};

impl Hooks {
    /// Encodes normalized rules under a positive finite configuration-file bound.
    pub fn encode(&self, maximum_bytes: u64) -> Result<Vec<u8>> {
        if maximum_bytes == 0 || maximum_bytes == u64::MAX {
            return Err(Error::Invalid(
                "hook configuration requires a finite positive byte bound".into(),
            ));
        }
        crate::executor::json::json_bytes_bounded(
            self,
            crate::executor::json::JsonBounds {
                maximum_bytes,
                maximum_items: maximum_bytes,
                maximum_depth: 16,
            },
            None,
            &mut |_| Ok(()),
        )
    }
}

// Serialize compiled configuration directly: no second rule catalog, JSON tree
// or command-string copies. Matchers already discarded as irrelevant normalize
// to the same wildcard consumed by add_rules.
impl Serialize for Binding {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let matcher = match &self.matcher {
            Some(Matcher::Exact(name)) => name.as_str(),
            Some(Matcher::Pattern(pattern)) => pattern.as_str(),
            None => "",
        };
        let mut rule = serializer.serialize_struct("HookRule", 3)?;
        rule.serialize_field("on", &self.event)?;
        rule.serialize_field("matcher", matcher)?;
        rule.serialize_field("hooks", &Commands(&self.commands))?;
        rule.end()
    }
}

struct Commands<'a>(&'a [Arc<HookSpec>]);
impl Serialize for Commands<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(AsRef::as_ref))
    }
}

impl Serialize for Hooks {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_seq(&self.bindings)
    }
}

impl<'de> Deserialize<'de> for Hooks {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let rules = Vec::<HookRule>::deserialize(deserializer)?;
        let mut hooks = Self::default();
        hooks.add_rules(rules).map_err(serde::de::Error::custom)?;
        Ok(hooks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // Property: normalized files preserve selection, order, commands and
        // failure policy for every event/subject, and re-encode byte-identically.
        #[test]
        fn normalized_snapshot_preserves_selection(
            rules in prop::collection::vec((0usize..4, prop::sample::select(vec!["", "*", "Bash", "Edit|Write", "^Read$", "[invalid"]), ".{0,24}", 0u64..1000, any::<bool>()), 0..8),
            maximum in prop_oneof![0u64..2048, Just(u64::MAX)],
            subject in prop_oneof!["[a-zA-Z]{0,12}", Just("Bash".into()), Just("Edit".into()), Just("Write".into()), Just("Read".into())],
        ) {
            let rules: Vec<_> = rules.into_iter().map(|(event, matcher, command, timeout_ms, block)| HookRule {
                on: [HookEvent::SessionStart, HookEvent::PreToolUse, HookEvent::PostToolUse, HookEvent::Stop][event],
                matcher: matcher.into(),
                hooks: vec![HookSpec { run: HookRun::Executable { executable: command, argv: vec!["one argument".into()] }, timeout_ms, failure: if block { HookFailure::Block } else { HookFailure::Report } }],
            }).collect();
            let input = serde_json::to_vec(&rules)?;
            let decoded = serde_json::from_slice::<Hooks>(&input);
            let mut original = Hooks::default();
            let admitted = original.add_rules(rules);
            prop_assert_eq!(admitted.is_ok(), decoded.is_ok());
            if admitted.is_err() {
                prop_assert!(original.is_empty(), "failed imports must remain atomic");
                return Ok(());
            }
            let snapshot = serde_json::to_vec(&original)?;
            let bounded = original.encode(maximum);
            prop_assert_eq!(bounded.is_ok(), snapshot.len() as u64 <= maximum && maximum > 0 && maximum != u64::MAX);
            if let Ok(encoded) = bounded { prop_assert_eq!(encoded, snapshot.clone()); }
            let decoded: Hooks = serde_json::from_slice(&snapshot)?;
            prop_assert_eq!(serde_json::to_vec(&decoded)?, snapshot);
            prop_assert_eq!(original.is_empty(), decoded.is_empty());
            let registry = ToolRegistry::default();
            let input = serde_json::json!({"source":subject,"tool_name":subject});
            for event in [HookEvent::SessionStart, HookEvent::PreToolUse, HookEvent::PostToolUse, HookEvent::Stop] {
                let selected = |hooks: &Hooks| -> Result<Vec<HookSpec>> { Ok(hooks.commands(event, &input, &registry)?.map(|hook| hook.as_ref().clone()).collect()) };
                prop_assert_eq!(selected(&original)?, selected(&decoded)?);
            }
        }
    }
}

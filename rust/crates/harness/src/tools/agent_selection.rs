//! Total selection of registered-agent inspections from an fs command.
//!
//! Selection never fails: a command that does not parse, or names anything
//! outside the exact `.volumes/agents` namespace, is not an inspection and is
//! left to the fs facade owner, which alone reports rejected input.
use super::fs_command::parse_fs_command;

pub(crate) const AGENTS: &str = ".volumes/agents";
const VOLUMES: &str = ".volumes/";

/// What an inspection reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentTarget {
    /// `ls .volumes/agents`
    Children,
    /// `ls .volumes/agents/<child>`
    Child,
    /// `cat .volumes/agents/<child>/<file>`
    File,
}

/// A selected inspection; owns the parsed operand without copying it.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AgentInspection {
    pub(crate) target: AgentTarget,
    pub(crate) path: String,
}

impl AgentInspection {
    /// The child component for `Child`, the task-file key for `File`, and the
    /// empty string for `Children`.
    pub(crate) fn operand(&self) -> &str {
        match self.target {
            AgentTarget::Children => "",
            AgentTarget::Child => &self.path[AGENTS.len() + 1..],
            AgentTarget::File => &self.path[VOLUMES.len()..],
        }
    }
}

pub(crate) fn selection(command: &str) -> Option<AgentInspection> {
    let command = parse_fs_command(command).ok()?;
    if command.volume.is_some() {
        return None;
    }
    let [verb, path]: [String; 2] = command.arguments.try_into().ok()?;
    let nested = path
        .strip_prefix(AGENTS)
        .and_then(|rest| rest.strip_prefix('/'))
        .is_some();
    let target = match verb.as_str() {
        "ls" if path == AGENTS => AgentTarget::Children,
        "ls" if nested => AgentTarget::Child,
        "cat" if nested => AgentTarget::File,
        _ => return None,
    };
    Some(AgentInspection { target, path })
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Independent spelling of the namespace rule: split on '/' instead of
    // matching prefixes.
    fn reference(command: &str) -> Option<AgentTarget> {
        let parsed = parse_fs_command(command).ok()?;
        if parsed.volume.is_some() || parsed.arguments.len() != 2 {
            return None;
        }
        let parts: Vec<&str> = parsed.arguments[1].split('/').collect();
        let inside = parts.len() > 2 && parts[0] == ".volumes" && parts[1] == "agents";
        match (parsed.arguments[0].as_str(), parts.as_slice()) {
            ("ls", [".volumes", "agents"]) => Some(AgentTarget::Children),
            ("ls", _) if inside => Some(AgentTarget::Child),
            ("cat", _) if inside => Some(AgentTarget::File),
            _ => None,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        // Selection is total and equals the reference for parseable, unparseable,
        // namespace-adjacent and volume-qualified commands alike.
        #[test]
        fn selection_matches_the_exact_namespace_reference(
            verb in prop::sample::select(vec!["ls", "cat", "git ls", "status", "diff"]),
            path in prop_oneof![
                prop::sample::select(vec![
                    ".volumes/agents", ".volumes/agents/", ".volumes/agents-extra", ".volumes/agentsx/w",
                    ".volumes/agents/worker", ".volumes/agents/worker/status.json", ".volumes/agent",
                    "agents", ".volumes/agents/../x", "'.volumes/agents/a b'",
                ]).prop_map(String::from),
                "[a-z./ -]{0,24}",
            ],
            suffix in prop::sample::select(vec!["", " | cat", "; ls", " && ls", " `x`", " < in", " > out",
                " 'open", " \\", " \0", " extra", " --volume", " --volume v", " --volume ''"]),
        ) {
            let command = format!("{verb} {path}{suffix}");
            let selected = selection(&command);
            prop_assert_eq!(selected.as_ref().map(|inspection| inspection.target), reference(&command), "{}", command);
            if let Some(inspection) = selected {
                prop_assert!(inspection.path == AGENTS || inspection.path.starts_with(".volumes/agents/"));
            }
        }
    }

    // The cargo-fuzz `command` target checks the same rule; its checked-in
    // seeds replay here so the selector's boundary cases run in every test pass.
    #[test]
    fn checked_in_command_corpus_matches_the_reference() {
        let corpus =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../harness-fuzz/corpus/command");
        let mut seeds = 0;
        for entry in std::fs::read_dir(corpus).unwrap() {
            let bytes = std::fs::read(entry.unwrap().path()).unwrap();
            if let Ok(command) = std::str::from_utf8(&bytes) {
                let selected = selection(command).map(|inspection| inspection.target);
                assert_eq!(selected, reference(command), "{command}");
                seeds += 1;
            }
        }
        assert!(seeds >= 15);
    }

    #[test]
    fn adjacent_names_and_unparseable_commands_are_left_to_the_facade() {
        for command in [
            "ls .volumes/agents-extra",
            "cat .volumes/agentsx/status.json",
            "cat .volumes/agents",
            "ls .volumes/agents | cat",
            "ls .volumes/agents --volume v",
        ] {
            assert_eq!(selection(command), None, "{command}");
        }
        let child = selection("ls .volumes/agents/worker").unwrap();
        assert_eq!(
            (child.target, child.operand()),
            (AgentTarget::Child, "worker")
        );
        let file = selection("cat .volumes/agents/worker/status.json").unwrap();
        assert_eq!(
            (file.target, file.operand()),
            (AgentTarget::File, "agents/worker/status.json")
        );
    }

    #[test]
    fn prefix_selection_negative_control_diverges_from_the_reference() {
        let command = "ls .volumes/agents-extra";
        assert_eq!(
            selection(command).map(|inspection| inspection.target),
            reference(command)
        );
        let parsed = parse_fs_command(command).unwrap();
        let old = parsed.arguments[1]
            .starts_with(AGENTS)
            .then_some(AgentTarget::Child);
        assert_ne!(
            old,
            reference(command),
            "plain-prefix mutant escaped the reference invariant"
        );
    }

    // Selection moves the parsed operand: it allocates exactly what parsing
    // allocates. The control adds back the former path copy and must differ by
    // exactly one allocation of the path's length.
    #[test]
    fn selection_moves_the_parsed_operand_without_copying_it() {
        let command = "cat .volumes/agents/worker/results/12.json";
        let path = ".volumes/agents/worker/results/12.json";
        let parse = acyclic_test_support::measure(|| drop(parse_fs_command(command)));
        let select = acyclic_test_support::measure(|| drop(selection(command)));
        assert_eq!(
            (select.count_total, select.bytes_total),
            (parse.count_total, parse.bytes_total)
        );
        let copied = acyclic_test_support::measure(|| {
            drop(selection(command).map(|inspection| inspection.path.clone()))
        });
        assert_eq!(
            (copied.count_total, copied.bytes_total),
            (parse.count_total + 1, parse.bytes_total + path.len() as u64)
        );
    }
}

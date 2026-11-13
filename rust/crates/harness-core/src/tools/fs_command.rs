use super::command::parse_command;
use crate::{Error, Result};

#[derive(Debug, PartialEq, Eq)]
#[doc(hidden)]
pub struct FsCommand {
    #[doc(hidden)]
    pub arguments: Vec<String>,
    #[doc(hidden)]
    pub volume: Option<String>,
}

/// The child operand shared by admission, name resolution and execution.
/// The optional suffix is the file path in `show CHILD:PATH`.
#[doc(hidden)]
pub fn child_branch_operand(args: &[String]) -> Option<(usize, &str, Option<&str>)> {
    let (index, operand, path) = match args {
        [command, branch] if matches!(command.as_str(), "diff" | "merge" | "discard") => {
            (1, branch.as_str(), None)
        }
        [command, branch, flag] if command == "diff" && flag == "--stat" => {
            (1, branch.as_str(), None)
        }
        [command, flag, branch] if command == "diff" && flag == "--stat" => {
            (2, branch.as_str(), None)
        }
        [command, object] | [command, object, _, _] if command == "show" => {
            show_offset(args)?;
            let (branch, path) = object.split_once(':')?;
            if path.is_empty() {
                return None;
            }
            (1, branch, Some(path))
        }
        _ => return None,
    };
    (operand
        .strip_prefix("agents/")
        .is_some_and(|name| !name.is_empty())
        || operand.starts_with("harness/agents/")
        || (path.is_some() && operand != "HEAD" && !operand.is_empty()))
    .then_some((index, operand, path))
}

pub(crate) fn agent_diff(args: &[String]) -> Option<(&str, bool)> {
    let (_, branch, _) = child_branch_operand(args)?;
    (args[0] == "diff").then_some((branch, args.iter().any(|arg| arg == "--stat")))
}

pub(crate) fn agent_show(args: &[String]) -> Option<(&str, &str, u64)> {
    let (_, branch, path) = child_branch_operand(args)?;
    (args[0] == "show").then_some((branch, path?, show_offset(args)?))
}

fn show_offset(args: &[String]) -> Option<u64> {
    match args {
        [command, _] if command == "show" => Some(0),
        [command, _, flag, offset] if command == "show" && flag == "--offset" => {
            offset.parse().ok()
        }
        _ => None,
    }
}

#[doc(hidden)]
pub fn parse_fs_command(command: &str) -> Result<FsCommand> {
    let mut arguments = parse_command(command)?;
    if arguments.first().is_some_and(|argument| argument == "git") {
        arguments.remove(0);
    }
    let mut volume = None;
    let mut index = 0;
    while index < arguments.len() && arguments[index] != "--" {
        let value_option = matches!(
            (
                arguments.first().map(String::as_str),
                arguments[index].as_str()
            ),
            (Some("commit"), "-m" | "--message" | "--author")
                | (Some("restore"), "--source")
                | (Some("log"), "-n" | "--max-count")
        );
        if value_option {
            index += 2;
            continue;
        }
        if arguments[index] != "--volume" {
            index += 1;
            continue;
        }
        if volume.is_some()
            || arguments
                .get(index + 1)
                .is_none_or(|name| name.is_empty() || name.starts_with('-'))
        {
            return Err(Error::Invalid(
                "fs requires one nonempty --volume qualifier".into(),
            ));
        }
        let mut qualifier = arguments.drain(index..index + 2);
        qualifier.next();
        volume = qualifier.next();
    }
    if arguments.is_empty() {
        return Err(Error::Invalid("fs command is empty".into()));
    }
    Ok(FsCommand { arguments, volume })
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        // Only the exact registered-agent inspection syntax takes this path.
        #[test]
        fn agent_diff_selects_only_its_exact_operand(
            command in prop::sample::select(vec!["diff", "show", "merge"]),
            agent in any::<bool>(),
            canonical in any::<bool>(),
            name in "[a-z0-9_-]{1,32}",
            extra in prop::collection::vec(".{0,16}", 0..3),
        ) {
            let branch = if agent {
                if canonical {
                    let parent = crate::agent::AgentPath::root(
                        &crate::agent::AgentId::from_bytes([1; 16]),
                    );
                    crate::tools::spawned_child(&parent, &name).unwrap().to_string()
                } else {
                    format!("agents/{name}")
                }
            } else {
                name
            };
            let mut args = vec![command.to_owned(), branch.clone()];
            args.extend(extra);
            let selected = agent_diff(&args);
            let supported = args.len() == 2 || (args.len() == 3 && args[2] == "--stat");
            prop_assert_eq!(selected.is_some(), command == "diff" && agent && supported);
            if let Some((selected, _)) = selected {
                prop_assert_eq!(selected, branch);
            }
        }

        // Removing the routing qualifier preserves every facade argument,
        // including operands named --volume after the path delimiter.
        #[test]
        fn volume_qualifier_round_trips(
            name in "[a-z][a-z0-9_-]{0,20}",
            operand in "[a-zA-Z0-9 _-]{0,32}",
            prefix in any::<bool>(),
        ) {
            let command = format!("{}status --volume '{}' -- '{}' --volume", if prefix { "git " } else { "" }, name, operand);
            let parsed = parse_fs_command(&command).unwrap();
            prop_assert_eq!(parsed.volume.as_deref(), Some(name.as_str()));
            prop_assert_eq!(parsed.arguments, ["status", "--", &operand, "--volume"]);
        }

        #[test]
        fn option_values_are_never_mount_qualifiers(
            flag in prop::sample::select(vec!["-m", "--message", "--author"]),
            name in "[a-z][a-z0-9_-]{0,20}",
            qualifier_first in any::<bool>(),
        ) {
            let args = if flag == "--author" { " --message message" } else { "" };
            let qualifier = format!("--volume {name}");
            let command = if qualifier_first {
                format!("{qualifier} commit {flag} '--volume'{args}")
            } else {
                format!("commit {flag} '--volume'{args} {qualifier}")
            };
            let parsed = parse_fs_command(&command).unwrap();
            prop_assert_eq!(parsed.volume.as_deref(), Some(name.as_str()));
            prop_assert_eq!(&parsed.arguments[..3], ["commit", flag, "--volume"]);
            let plain = parse_fs_command(&format!("commit {flag} '--volume'{args}")).unwrap();
            prop_assert_eq!(plain.volume, None);
            prop_assert_eq!(plain.arguments, parsed.arguments);
        }
    }

    #[test]
    fn volume_qualifier_requires_a_command_and_one_value() {
        for command in [
            "git",
            "--volume data",
            "status --volume",
            "status --volume ''",
            "status --volume a --volume b",
            "status --volume --",
        ] {
            assert!(parse_fs_command(command).is_err(), "{command}");
        }
    }
}

use super::command::parse_command;
use crate::{Error, Result};

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FsCommand {
    pub(crate) arguments: Vec<String>,
    pub(crate) volume: Option<String>,
}

#[cfg(all(
    not(target_arch = "wasm32"),
    any(test, all(feature = "filesystem-local", feature = "native-execution"))
))]
pub(crate) fn agent_diff(args: &[String]) -> Option<&str> {
    match args {
        [command, branch] if command == "diff" && branch.starts_with("agents/") => Some(branch),
        _ => None,
    }
}

pub(crate) fn parse_fs_command(command: &str) -> Result<FsCommand> {
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

#[cfg(all(test, not(target_arch = "wasm32")))]
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
            name in "[a-z0-9_-]{1,32}",
            extra in prop::collection::vec(".{0,16}", 0..3),
        ) {
            let branch = if agent { format!("agents/{name}") } else { name };
            let mut args = vec![command.to_owned(), branch.clone()];
            args.extend(extra);
            let selected = agent_diff(&args);
            prop_assert_eq!(selected.is_some(), command == "diff" && agent && args.len() == 2);
            if let Some(selected) = selected {
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

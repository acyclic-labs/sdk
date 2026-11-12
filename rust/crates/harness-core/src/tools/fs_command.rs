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

/// One literal query, shared by the record and volume adapters.
pub(crate) struct Grep<'a> {
    pub scope: Option<&'a str>,
    pub path: Option<(crate::tool::ReadPathKind, &'a str)>,
    pub pattern: &'a str,
    pub size: std::num::NonZeroU32,
    pub cursor: Option<&'a str>,
}

impl FsCommand {
    pub(crate) fn grep(&self) -> Result<Grep<'_>> {
        let mut arguments = self.arguments.iter();
        if arguments.next().is_none_or(|command| command != "grep") {
            return Err(Error::Invalid("search requires grep".into()));
        }
        let mut scope = None;
        let mut path = None;
        let mut pattern = None;
        let mut size = None;
        let mut cursor = None;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--" => {
                    let literal = arguments
                        .next()
                        .ok_or_else(|| Error::Invalid("grep requires a literal after --".into()))?;
                    if pattern.replace(literal.as_str()).is_some() || arguments.next().is_some() {
                        return Err(Error::Invalid("grep requires exactly one literal".into()));
                    }
                    break;
                }
                "--scope" | "--file" | "--directory" | "--page-size" | "--cursor" => {
                    let value = arguments
                        .next()
                        .ok_or_else(|| Error::Invalid("grep option requires a value".into()))?;
                    match argument.as_str() {
                        "--scope" if scope.is_none() => scope = Some(value.as_str()),
                        "--file" if path.is_none() => {
                            path = Some((crate::tool::ReadPathKind::File, value.as_str()))
                        }
                        "--directory" if path.is_none() => {
                            path = Some((crate::tool::ReadPathKind::Directory, value.as_str()))
                        }
                        "--page-size" if size.is_none() => {
                            size = Some(value.parse().map_err(|_| {
                                Error::Invalid("search page size must be nonzero".into())
                            })?)
                        }
                        "--cursor" if cursor.is_none() => cursor = Some(value.as_str()),
                        _ => return Err(Error::Invalid("duplicate grep option".into())),
                    }
                }
                literal if pattern.is_none() => pattern = Some(literal),
                _ => return Err(Error::Invalid("grep requires exactly one literal".into())),
            }
        }
        if scope.is_some() && (path.is_some() || self.volume.is_some()) {
            return Err(Error::Invalid(
                "grep selects one agent scope or one mounted volume".into(),
            ));
        }
        Ok(Grep {
            scope,
            path,
            pattern: pattern.ok_or_else(|| Error::Invalid("grep requires a literal".into()))?,
            size: size.unwrap_or(
                std::num::NonZeroU32::new(64)
                    .ok_or_else(|| Error::Invalid("invalid search chunk".into()))?,
            ),
            cursor,
        })
    }
}

pub(crate) fn agent_diff(args: &[String]) -> Option<&str> {
    match args {
        [command, branch]
            if command == "diff"
                && (branch.starts_with("agents/") || branch.starts_with("harness/agents/")) =>
        {
            Some(branch)
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
                | (
                    Some("grep"),
                    "--scope" | "--file" | "--directory" | "--page-size" | "--cursor"
                )
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

    #[test]
    fn literal_search_preserves_option_operands_and_explicit_path_kinds() {
        let parsed = parse_fs_command(
            "grep --file 'folder/name' --volume private --page-size 2 -- '--volume'",
        )
        .unwrap();
        let query = parsed.grep().unwrap();
        assert_eq!(query.pattern, "--volume");
        assert_eq!(
            query.path,
            Some((crate::tool::ReadPathKind::File, "folder/name"))
        );
        assert_eq!(query.size.get(), 2);
        assert_eq!(parsed.volume.as_deref(), Some("private"));
        for invalid in [
            "grep --scope . --file body needle",
            "grep --file a --directory b needle",
            "grep needle --page-size 0",
            "grep needle extra",
            "grep --scope . needle --volume private",
        ] {
            assert!(
                parse_fs_command(invalid).unwrap().grep().is_err(),
                "{invalid}"
            );
        }
    }

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

use super::*;

#[allow(
    clippy::too_many_lines,
    reason = "the argv compatibility matrix is intentionally exhaustive and centralized"
)]
#[allow(
    clippy::cognitive_complexity,
    reason = "the argv compatibility matrix is intentionally exhaustive and centralized"
)]
pub(super) fn parse_argv<E: std::error::Error + 'static>(
    argv: &[String],
    default_author: &str,
    now_seconds: i64,
) -> Result<GitCommand, GitCompatError<E>> {
    reject_shell_composition(argv)?;
    let Some(command) = argv.first().map(String::as_str) else {
        return Err(GitCompatError::InvalidCommand("missing command".to_owned()));
    };
    let args = argv.get(1..).unwrap_or_default();
    match command {
        "status" => {
            require_only_options(command, args, &["--short", "--porcelain", "--porcelain=v1"])?;
            Ok(GitCommand::Status)
        }
        "diff" => {
            require_only_options(command, args, &["--cached", "--staged"])?;
            Ok(GitCommand::Diff {
                cached: args
                    .iter()
                    .any(|arg| arg == "--cached" || arg == "--staged"),
            })
        }
        "log" => Ok(GitCommand::Log {
            maximum: parse_log_maximum(args)?,
        }),
        "show" => Ok(GitCommand::Show {
            object: optional_single_value(command, args)?,
        }),
        "add" => {
            if args.is_empty() {
                return Err(GitCompatError::InvalidCommand(
                    "add requires a path or -A/--all".to_owned(),
                ));
            }
            let mut options_ended = false;
            let mut stages_without_paths = false;
            let mut paths = Vec::new();
            for arg in args {
                if !options_ended && arg == "--" {
                    options_ended = true;
                } else if !options_ended && arg.starts_with('-') {
                    if matches!(arg.as_str(), "-A" | "--all" | "-u" | "--update") {
                        stages_without_paths = true;
                    } else {
                        return unsupported_option(command, arg);
                    }
                } else {
                    paths.push(arg.clone());
                }
            }
            if paths.is_empty() && !stages_without_paths {
                return Err(GitCompatError::InvalidCommand(
                    "add requires a path or -A/--all".to_owned(),
                ));
            }
            Ok(GitCommand::Add { paths })
        }
        "commit" => {
            let mut message = None;
            let mut author = default_author.to_owned();
            let mut index = 0;
            while index < args.len() {
                let Some(argument) = args.get(index) else {
                    break;
                };
                let (name, inline) = split_long_option(argument);
                if name == "-m" || name == "--message" {
                    let value = inline
                        .map(str::to_owned)
                        .or_else(|| args.get(index + 1).cloned())
                        .ok_or_else(|| {
                            GitCompatError::InvalidCommand("commit requires a message after -m".to_owned())
                        })?;
                    if message.replace(value).is_some() {
                        return Err(GitCompatError::InvalidCommand(
                            "multiple commit messages are not supported".to_owned(),
                        ));
                    }
                    index += usize::from(inline.is_none());
                } else if name == "--author" {
                    author = inline
                        .map(str::to_owned)
                        .or_else(|| args.get(index + 1).cloned())
                        .ok_or_else(|| {
                            GitCompatError::InvalidCommand(
                                "commit requires an author after --author".to_owned(),
                            )
                        })?;
                    index += usize::from(inline.is_none());
                } else {
                    return unsupported_option(command, argument);
                }
                index += 1;
            }
            let message = message.ok_or_else(|| {
                GitCompatError::InvalidCommand("commit requires -m".to_owned())
            })?;
            Ok(GitCommand::Commit {
                message,
                author,
                authored_at_seconds: now_seconds,
            })
        }
        "branch" => Ok(GitCommand::Branch {
            create: optional_single_value(command, args)?,
        }),
        "switch" => {
            let (create, values) = positional_with_options(command, args, &["-c"])?;
            let branch = exactly_one(command, &values)?;
            Ok(GitCommand::Switch { branch, create })
        }
        "checkout" => {
            if let Some(separator) = args.iter().position(|argument| argument == "--") {
                if separator > 1 {
                    return Err(GitCompatError::InvalidCommand(
                        "checkout path restoration accepts at most one source object".to_owned(),
                    ));
                }
                let paths = args.get(separator + 1..).unwrap_or_default().to_vec();
                if paths.is_empty() {
                    return Err(GitCompatError::InvalidCommand(
                        "checkout -- requires at least one path".to_owned(),
                    ));
                }
                return Ok(GitCommand::Restore {
                    source: args.first().cloned(),
                    paths,
                });
            }
            let (create, values) = positional_with_options(command, args, &["-b"])?;
            let branch = exactly_one(command, &values)?;
            Ok(GitCommand::Switch { branch, create })
        }
        "restore" => {
            let mut source = None;
            let mut paths = Vec::new();
            let mut options_ended = false;
            let mut index = 0;
            while index < args.len() {
                let Some(argument) = args.get(index) else {
                    break;
                };
                if !options_ended && argument == "--" {
                    options_ended = true;
                } else if !options_ended {
                    let (name, inline) = split_long_option(argument);
                    if name == "--source" {
                        let value = inline
                            .map(str::to_owned)
                            .or_else(|| args.get(index + 1).cloned())
                            .ok_or_else(|| {
                                GitCompatError::InvalidCommand(
                                    "restore requires an object after --source".to_owned(),
                                )
                            })?;
                        if source.replace(value).is_some() {
                            return Err(GitCompatError::InvalidCommand(
                                "restore accepts only one --source".to_owned(),
                            ));
                        }
                        index += usize::from(inline.is_none());
                    } else if argument.starts_with('-') {
                        return unsupported_option(command, argument);
                    } else {
                        paths.push(argument.clone());
                    }
                } else {
                    paths.push(argument.clone());
                }
                index += 1;
            }
            if paths.is_empty() {
                return Err(GitCompatError::InvalidCommand(
                    "restore requires at least one path".to_owned(),
                ));
            }
            Ok(GitCommand::Restore { source, paths })
        }
        "reset" => {
            require_options(command, args, &["--hard", "--soft", "--mixed"])?;
            let modes = args
                .iter()
                .filter(|arg| matches!(arg.as_str(), "--hard" | "--soft" | "--mixed"))
                .count();
            if modes > 1 {
                return Err(GitCompatError::InvalidCommand(
                    "reset accepts only one mode".to_owned(),
                ));
            }
            let mode = if args.iter().any(|arg| arg == "--hard") {
                GitResetMode::Hard
            } else if args.iter().any(|arg| arg == "--soft") {
                GitResetMode::Soft
            } else {
                GitResetMode::Mixed
            };
            let targets = args
                .iter()
                .filter(|arg| !arg.starts_with('-'))
                .cloned()
                .collect::<Vec<_>>();
            let target = match targets.as_slice() {
                [] => "HEAD".to_owned(),
                [target] => target.clone(),
                _ => return Err(GitCompatError::InvalidCommand(
                    "reset accepts only one target; path reset is not supported".to_owned(),
                )),
            };
            Ok(GitCommand::Reset {
                target: target,
                mode,
            })
        }
        "merge" if matches!(args, [operation] if operation == "--continue") => {
            Ok(GitCommand::MergeContinue)
        }
        "merge" if matches!(args, [operation] if operation == "--abort") => {
            Ok(GitCommand::MergeAbort)
        }
        "merge" | "rebase" => {
            let branch = exactly_one(command, args)?;
            if command == "merge" {
                Ok(GitCommand::Merge { branch })
            } else {
                Ok(GitCommand::Rebase { branch })
            }
        }
        "cherry-pick" | "revert" => {
            let object = exactly_one(command, args)?;
            if command == "cherry-pick" {
                Ok(GitCommand::CherryPick { object })
            } else {
                Ok(GitCommand::Revert { object })
            }
        }
        "stash" => match args {
            [] => Ok(GitCommand::StashPush),
            [operation] if operation == "push" => Ok(GitCommand::StashPush),
            [operation] if operation == "pop" => Ok(GitCommand::StashPop),
            [other] => Err(GitCompatError::InvalidCommand(format!(
                "unsupported stash operation '{other}'"
            ))),
            _ => Err(GitCompatError::InvalidCommand(
                "stash accepts only push or pop".to_owned(),
            )),
        },
        "tag" => {
            let delete = args.iter().any(|arg| arg == "-d" || arg == "--delete");
            require_options(command, args, &["-d", "--delete"])?;
            let values: Vec<_> = args.iter().filter(|arg| !arg.starts_with('-')).cloned().collect();
            if values.len() > 2 || (delete && values.len() != 1) {
                return Err(GitCompatError::InvalidCommand(
                    "tag accepts [name [target]] or --delete name".to_owned(),
                ));
            }
            Ok(GitCommand::Tag {
                name: values.first().cloned(),
                target: values.get(1).cloned(),
                delete,
            })
        }
        "blame" => Ok(GitCommand::Blame {
            path: exactly_one(command, args)?,
        }),
        "grep" => match args {
            [pattern] => Ok(GitCommand::Grep {
                pattern: pattern.clone(),
                path: None,
            }),
            [pattern, path] => Ok(GitCommand::Grep {
                pattern: pattern.clone(),
                path: Some(path.clone()),
            }),
            _ => Err(GitCompatError::InvalidCommand(
                "grep requires a pattern and at most one path".to_owned(),
            )),
        },
        "clean" => {
            require_only_options(
                command,
                args,
                &["-n", "--dry-run", "-f", "--force"],
            )?;
            let dry_run = args
                .iter()
                .any(|arg| matches!(arg.as_str(), "-n" | "--dry-run"));
            let force = args
                .iter()
                .any(|arg| matches!(arg.as_str(), "-f" | "--force"));
            if !dry_run && !force {
                return Err(GitCompatError::InvalidCommand(
                    "clean requires -f or --force unless --dry-run is used".to_owned(),
                ));
            }
            Ok(GitCommand::Clean { dry_run })
        }
        "archive" => Ok(GitCommand::Archive {
            object: optional_single_value(command, args)?,
        }),
        "apply" => Err(GitCompatError::Unsupported {
            command: command.to_owned(),
            reason: "argv cannot safely turn a patch filename into patch bytes; load the file and use the typed Apply command".to_owned(),
        }),
        "bisect" => Ok(GitCommand::Bisect {
            arguments: args.to_vec(),
        }),
        "rev-parse" => match args {
            [argument] if matches!(argument.as_str(), "--is-inside-work-tree" | "HEAD") => {
                Ok(GitCommand::RevParse {
                    argument: argument.clone(),
                })
            }
            [option, head]
                if head == "HEAD"
                    && matches!(
                        option.as_str(),
                        "--abbrev-ref" | "--symbolic-full-name"
                    ) => Ok(GitCommand::RevParse {
                argument: format!("{option} HEAD"),
            }),
            [object] if !object.starts_with('-') => Ok(GitCommand::RevParse {
                argument: object.clone(),
            }),
            _ => Err(GitCompatError::InvalidCommand(
                "rev-parse supports HEAD, one object, --is-inside-work-tree, --abbrev-ref HEAD, or --symbolic-full-name HEAD".to_owned(),
            )),
        },
        "symbolic-ref" => {
            let short = args.iter().any(|argument| argument == "--short");
            require_options(command, args, &["--short"])?;
            let values = args
                .iter()
                .filter(|argument| !argument.starts_with('-'))
                .collect::<Vec<_>>();
            if values.as_slice() != [&"HEAD"] {
                return Err(GitCompatError::InvalidCommand(
                    "symbolic-ref supports only HEAD".to_owned(),
                ));
            }
            Ok(GitCommand::SymbolicRef { short })
        }
        "merge-base" => match args {
            [left, right] => Ok(GitCommand::MergeBase {
                left: left.clone(),
                right: right.clone(),
            }),
            _ => Err(GitCompatError::InvalidCommand(
                "merge-base requires exactly two objects".to_owned(),
            )),
        },
        "ls-files" => {
            require_only_options(command, args, &["--cached"])?;
            Ok(GitCommand::LsFiles)
        }
        "check-ignore" => {
            let mut paths = Vec::new();
            for argument in args {
                if argument == "--" {
                    continue;
                }
                if argument.starts_with('-') {
                    return unsupported_option(command, argument);
                }
                paths.push(argument.clone());
            }
            if paths.is_empty() {
                return Err(GitCompatError::InvalidCommand(
                    "check-ignore requires at least one path".to_owned(),
                ));
            }
            Ok(GitCommand::CheckIgnore { paths })
        }
        "clone" | "fetch" | "pull" | "push" | "remote" | "gc" | "repack" | "cat-file"
        | "hash-object" | "init" | "fsck" | "prune" | "pack-objects" | "index-pack"
        | "receive-pack" | "upload-pack" | "read-tree" | "write-tree" | "commit-tree"
        | "update-index" | "worktree" | "submodule" => Err(GitCompatError::Unsupported {
            command: command.to_owned(),
            reason: "the compatibility layer has no Git object database or transport".to_owned(),
        }),
        other => Err(GitCompatError::InvalidCommand(format!(
            "unknown command '{other}'"
        ))),
    }
}

pub(super) fn reject_shell_composition<E: std::error::Error + 'static>(
    argv: &[String],
) -> Result<(), GitCompatError<E>> {
    if let Some(token) = argv.iter().find(|token| {
        matches!(token.as_str(), "&&" | "||" | ";" | "|" | "&")
            || token.starts_with('>')
            || token.starts_with('<')
            || token.contains('`')
            || token.contains("$(")
    }) {
        return Err(GitCompatError::Unsupported {
            command: token.clone(),
            reason: "shell composition is outside the command-level compatibility façade"
                .to_owned(),
        });
    }
    Ok(())
}

pub(super) fn require_options<E: std::error::Error + 'static>(
    command: &str,
    args: &[String],
    allowed: &[&str],
) -> Result<(), GitCompatError<E>> {
    if let Some(argument) = args
        .iter()
        .find(|argument| argument.starts_with('-') && !allowed.contains(&argument.as_str()))
    {
        return unsupported_option(command, argument);
    }
    Ok(())
}

pub(super) fn require_only_options<E: std::error::Error + 'static>(
    command: &str,
    args: &[String],
    allowed: &[&str],
) -> Result<(), GitCompatError<E>> {
    require_options(command, args, allowed)?;
    if args.iter().any(|argument| !argument.starts_with('-')) {
        return Err(GitCompatError::InvalidCommand(format!(
            "{command} does not accept positional arguments"
        )));
    }
    Ok(())
}

pub(super) fn unsupported_option<T, E: std::error::Error + 'static>(
    command: &str,
    argument: &str,
) -> Result<T, GitCompatError<E>> {
    Err(GitCompatError::Unsupported {
        command: command.to_owned(),
        reason: format!("option '{argument}' cannot be represented by the compatibility façade"),
    })
}

pub(super) fn optional_single_value<E: std::error::Error + 'static>(
    command: &str,
    args: &[String],
) -> Result<Option<String>, GitCompatError<E>> {
    match args {
        [] => Ok(None),
        [value] if !value.starts_with('-') => Ok(Some(value.clone())),
        [option] => unsupported_option(command, option),
        _ => Err(GitCompatError::InvalidCommand(format!(
            "{command} accepts at most one argument"
        ))),
    }
}

pub(super) fn exactly_one<E: std::error::Error + 'static>(
    command: &str,
    values: &[String],
) -> Result<String, GitCompatError<E>> {
    match values {
        [value] if !value.starts_with('-') => Ok(value.clone()),
        [option] => unsupported_option(command, option),
        _ => Err(GitCompatError::InvalidCommand(format!(
            "{command} requires exactly one argument"
        ))),
    }
}

pub(super) fn positional_with_options<E: std::error::Error + 'static>(
    command: &str,
    args: &[String],
    allowed: &[&str],
) -> Result<(bool, Vec<String>), GitCompatError<E>> {
    require_options(command, args, allowed)?;
    Ok((
        args.iter()
            .any(|argument| allowed.contains(&argument.as_str())),
        args.iter()
            .filter(|argument| !allowed.contains(&argument.as_str()))
            .cloned()
            .collect(),
    ))
}

pub(super) fn split_long_option(argument: &str) -> (&str, Option<&str>) {
    argument
        .split_once('=')
        .map_or((argument, None), |(name, value)| (name, Some(value)))
}

pub(super) fn parse_log_maximum<E: std::error::Error + 'static>(
    args: &[String],
) -> Result<u32, GitCompatError<E>> {
    if args.is_empty() {
        return Ok(100);
    }
    let value = match args {
        [option, value] if option == "-n" || option == "--max-count" => value.as_str(),
        [option] if option.starts_with("--max-count=") => option
            .split_once('=')
            .map(|(_, value)| value)
            .unwrap_or_default(),
        [option] if option.len() > 1 => option
            .strip_prefix('-')
            .ok_or_else(|| GitCompatError::InvalidCommand("invalid log count".to_owned()))?,
        [option] => return unsupported_option("log", option),
        _ => {
            return Err(GitCompatError::InvalidCommand(
                "log accepts only -n <count>, -<count>, or --max-count=<count>".to_owned(),
            ));
        }
    };
    value.parse::<u32>().map_err(|_| {
        GitCompatError::InvalidCommand("log count must be an unsigned 32-bit integer".to_owned())
    })
}

//! Native host launch policy, shared by the root launcher and its children.
//! No provider, native tool replacement, or agent loop: the native host runs
//! those under the policy arguments it was launched with.
use super::*;
use std::process::{Command, Stdio};

fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

/// Admits only explicit `--option VALUE` policy pairs the host allows, with
/// exactly one sandbox and approval choice where the host has them.
pub fn validate(host: &str, args: &[String]) -> Result<()> {
    let (sandbox, approval, allowed): (&str, &str, &[&str]) = match host {
        "codex" => (
            "--sandbox",
            "--ask-for-approval",
            &[
                "--sandbox",
                "--ask-for-approval",
                "--config",
                "--model",
                "--profile",
                "--add-dir",
            ],
        ),
        "claude-code" => (
            "--settings",
            "--permission-mode",
            &[
                "--settings",
                "--permission-mode",
                "--model",
                "--allowedTools",
                "--disallowedTools",
                "--tools",
                "--setting-sources",
                "--plugin-dir",
                "--add-dir",
            ],
        ),
        "pi" => (
            "",
            "",
            &[
                "--model",
                "--provider",
                "--thinking",
                "--tools",
                "--extension",
                "--session-dir",
            ],
        ),
        _ => return Err(invalid("host launcher supports codex, claude-code or pi")),
    };
    if args
        .chunks(2)
        .any(|pair| !matches!(pair, [name, _] if allowed.contains(&name.as_str())))
    {
        return Err(invalid(
            "provide explicit native policy arguments as --option VALUE pairs; prompts belong in the native host",
        ));
    }
    for name in [sandbox, approval] {
        if name.is_empty() {
            continue;
        }
        if args
            .chunks(2)
            .filter(|pair| matches!(*pair, [key, _] if key == name))
            .count()
            != 1
        {
            return Err(invalid(format!(
                "host requires exactly one {name} VALUE; children inherit it unchanged"
            )));
        }
    }
    Ok(())
}
/// The native executable for `host`.
pub fn program(host: &str) -> Result<&'static str> {
    match host {
        "codex" => Ok("codex"),
        "claude-code" => Ok("claude"),
        "pi" => Ok("pi"),
        _ => Err(invalid("unsupported native host")),
    }
}

/// The root's policy arguments, with relative paths anchored at `cwd` so a
/// child started in a descendant view keeps their targets.
pub fn parent_flags(host: &str, args: &[String], cwd: &Path) -> Result<Vec<String>> {
    validate(host, args)?;
    let mut flags = args.to_vec();
    for [name, value] in flags.as_chunks_mut::<2>().0 {
        let inherited_path = match (host, name.as_str()) {
            (_, "--add-dir") | ("claude-code", "--plugin-dir") | ("pi", "--session-dir") => true,
            ("claude-code", "--settings") => !value.trim_start().starts_with('{'),
            ("pi", "--extension") => {
                ![
                    "npm:", "git:", "github:", "http:", "https:", "ssh:", "builtin:",
                ]
                .iter()
                .any(|prefix| value.trim().starts_with(prefix))
                    && cwd.join(value.as_str()).exists()
            }
            _ => false,
        };
        if inherited_path && Path::new(value.as_str()).is_relative() {
            *value = cwd
                .join(value.as_str())
                .to_str()
                .ok_or_else(|| invalid("non-Unicode native configuration path"))?
                .into();
        }
    }
    if matches!(host, "codex" | "claude-code") {
        // The parent already admits its cwd. Preserve that same root when a
        // separate child starts in a descendant view; admit no additional path.
        flags.extend([
            "--add-dir".into(),
            cwd.to_str()
                .ok_or_else(|| invalid("non-Unicode workspace"))?
                .into(),
        ]);
    }
    Ok(flags)
}

/// The non-interactive child command for one turn in `cwd`, resuming the
/// native `session` reference when one is held.
pub fn child(host: &str, args: &[String], cwd: &Path, session: Option<&str>) -> Result<Command> {
    let arguments = child_arguments(host, args, cwd, session)?;
    let mut command = acyclic_native_runtime::host_command(program(host)?).map_err(storage)?;
    command
        .args(arguments)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    Ok(command)
}

/// The arguments of [`child`]: the inherited policy, the host's
/// non-interactive event mode and its resume reference, never an option.
pub fn child_arguments(
    host: &str,
    args: &[String],
    cwd: &Path,
    session: Option<&str>,
) -> Result<Vec<std::ffi::OsString>> {
    validate(host, args)?;
    if session.is_some_and(|reference| reference.starts_with('-')) {
        return Err(invalid("native resume reference cannot be an option"));
    }
    let owned = |values: &[&str]| values.iter().map(Into::into).collect::<Vec<_>>();
    let policy = args.iter().map(Into::into);
    let mut arguments: Vec<std::ffi::OsString> = Vec::new();
    match host {
        "codex" => {
            arguments.extend(policy);
            arguments.extend(owned(&["exec", "--json", "--cd"]));
            arguments.push(cwd.into());
            if let Some(id) = session {
                arguments.extend(owned(&["resume", id]));
            }
            arguments.push("-".into());
        }
        "claude-code" => {
            arguments.extend(owned(&[
                "-p",
                "--output-format",
                "stream-json",
                "--verbose",
            ]));
            arguments.extend(policy);
            if let Some(id) = session {
                arguments.extend(owned(&["--resume", id]));
            }
        }
        _ => {
            arguments.extend(owned(&["--mode", "json", "-p"]));
            arguments.extend(policy);
            if let Some(file) = session {
                arguments.extend(owned(&["--session", file]));
            }
        }
    }
    Ok(arguments)
}

/// Runs `acyclic host HOST -- POLICY_ARGS`: the interactive native root under
/// its validated policy, which every managed child inherits unchanged.
pub fn launch(args: &[String]) -> Result<()> {
    let host = args
        .first()
        .ok_or_else(|| invalid("host requires codex, claude-code or pi"))?;
    let flags = args.get(1..).unwrap_or_default();
    let flags = flags.strip_prefix(&["--".to_owned()]).unwrap_or(flags);
    let cwd = std::env::current_dir().map_err(storage)?;
    let flags = parent_flags(host, flags, &cwd)?;
    let status = run_root_session(&cwd, host, std::ffi::OsStr::new(program(host)?), &flags)?;
    if !status.success() {
        return Err(invalid(format!("native {host} exited {status}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

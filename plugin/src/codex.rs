//! Native launch arguments. No provider, native tool replacement, or agent loop.
use super::*;
use std::process::{Command, Stdio};

pub(super) fn validate(host: &str, args: &[String]) -> Result<()> {
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
        _ => return Err("host launcher supports codex, claude-code or pi".into()),
    };
    if args
        .chunks(2)
        .any(|pair| !matches!(pair, [name, _] if allowed.contains(&name.as_str())))
    {
        return Err("provide explicit native policy arguments as --option VALUE pairs; prompts belong in the native host".into());
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
            return Err(format!(
                "host requires exactly one {name} VALUE; children inherit it unchanged"
            )
            .into());
        }
    }
    Ok(())
}
pub(super) fn program(host: &str) -> Result<&'static str> {
    match host {
        "codex" => Ok("codex"),
        "claude-code" => Ok("claude"),
        "pi" => Ok("pi"),
        _ => Err("unsupported native host".into()),
    }
}

pub(super) fn parent_flags(host: &str, args: &[String], cwd: &Path) -> Result<Vec<String>> {
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
                .ok_or("non-Unicode native configuration path")?
                .into();
        }
    }
    if matches!(host, "codex" | "claude-code") {
        // The parent already admits its cwd. Preserve that same root when a
        // separate child starts in a descendant view; admit no additional path.
        flags.extend([
            "--add-dir".into(),
            cwd.to_str().ok_or("non-Unicode workspace")?.into(),
        ]);
    }
    Ok(flags)
}

pub(super) fn child(
    host: &str,
    args: &[String],
    cwd: &Path,
    session: Option<&str>,
) -> Result<Command> {
    validate(host, args)?;
    if session.is_some_and(|reference| reference.starts_with('-')) {
        return Err("native resume reference cannot be an option".into());
    }
    let mut command = acyclic_native_runtime::host_command(program(host)?)?;
    command
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if host == "codex" {
        command.args(args).args(["exec", "--json", "--cd"]).arg(cwd);
        if let Some(id) = session {
            command.args(["resume", id]);
        }
        command.arg("-");
    } else if host == "claude-code" {
        command
            .args(["-p", "--output-format", "stream-json", "--verbose"])
            .args(args);
        if let Some(id) = session {
            command.args(["--resume", id]);
        }
    } else {
        command.args(["--mode", "json", "-p"]).args(args);
        if let Some(file) = session {
            command.args(["--session", file]);
        }
    }
    Ok(command)
}

pub(super) fn launch(args: &[String]) -> Result<()> {
    let host = args.first().ok_or("host requires codex or claude-code")?;
    let flags = args.get(1..).unwrap_or_default();
    let flags = flags.strip_prefix(&["--".to_owned()]).unwrap_or(flags);
    let cwd = env::current_dir()?;
    let flags = parent_flags(host, flags, &cwd)?;
    let status = acyclic_harness::local::host_process::run_root_session(
        &cwd,
        host,
        std::ffi::OsStr::new(program(host)?),
        &flags,
    )?;
    if !status.success() {
        return Err(format!("native {host} exited {status}").into());
    }
    Ok(())
}

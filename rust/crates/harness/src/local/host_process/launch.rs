//! Native host launch policy, shared by the root launcher and its children.
//! No provider, native tool replacement, or agent loop: the native host runs
//! those under the policy arguments it was launched with.
use super::*;
use acyclic_native_runtime::{ProcessCommand, ProcessStdio};
use std::ffi::{OsStr, OsString};
mod agy;

fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

fn is_switch(host: &str, name: &str) -> bool {
    matches!(
        (host, name),
        ("pi", "--no-extensions") | ("agy", "--sandbox")
    )
}

/// Decodes the root's serialized argument vector at the child-owner boundary.
pub fn inherited_arguments(encoded: &str) -> Result<Vec<String>> {
    serde_json::from_str(encoded).map_err(storage)
}

/// Admits only explicit native policy options the host allows, with
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
        "opencode" => ("", "", &["--model", "--agent"]),
        "agy" => (
            "--sandbox",
            "--mode",
            &[
                "--settings",
                "--mode",
                "--model",
                "--agent",
                "--effort",
                "--print-timeout",
                "--add-dir",
            ],
        ),
        _ => {
            return Err(invalid(
                "host launcher supports codex, claude-code, pi, opencode or agy",
            ));
        }
    };
    let mut required = [(sandbox, 0), (approval, 0)];
    let mut timeout = false;
    let mut arguments = args.iter();
    while let Some(name) = arguments.next() {
        if host == "agy" && name == "--print-timeout" && std::mem::replace(&mut timeout, true) {
            return Err(invalid(
                "step=admit native lifetime: duplicate --print-timeout; recovery: acyclic host agy -- --sandbox --mode accept-edits --print-timeout VALUE",
            ));
        }
        if is_switch(host, name) {
            for (option, count) in &mut required {
                *count += usize::from(name.as_str() == *option);
            }
            continue;
        }
        if !allowed.contains(&name.as_str()) || arguments.next().is_none() {
            return Err(invalid(
                "provide admitted native policy switches or --option VALUE pairs; prompts belong in the native host",
            ));
        }
        for (option, count) in &mut required {
            *count += usize::from(name.as_str() == *option);
        }
    }
    for (name, count) in required {
        if name.is_empty() {
            continue;
        }
        if count != 1 {
            let value = if is_switch(host, name) { "" } else { " VALUE" };
            return Err(invalid(format!(
                "host requires exactly one {name}{value}; children inherit it unchanged"
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
        "opencode" => Ok("opencode"),
        "agy" => Ok("agy"),
        _ => Err(invalid("unsupported native host")),
    }
}

/// The root's policy arguments, with relative paths anchored at `cwd` so a
/// child started in a descendant view keeps their targets.
pub fn parent_flags(host: &str, args: &[String], cwd: &Path) -> Result<Vec<String>> {
    validate(host, args)?;
    let mut flags = args.to_vec();
    let mut arguments = flags.iter_mut();
    while let Some(name) = arguments.next() {
        if is_switch(host, name) {
            continue;
        }
        let Some(value) = arguments.next() else {
            return Err(invalid("native policy option requires a value"));
        };
        let inherited_path = match (host, name.as_str()) {
            (_, "--add-dir") | ("claude-code", "--plugin-dir") | ("pi", "--session-dir") => true,
            ("claude-code" | "agy", "--settings") => !value.trim_start().starts_with('{'),
            ("pi", "--extension") => ![
                "npm:", "git:", "github:", "http:", "https:", "ssh:", "builtin:",
            ]
            .iter()
            .any(|prefix| value.trim().starts_with(prefix)),
            _ => false,
        };
        if inherited_path
            && !(host == "pi"
                && (value.starts_with("file://")
                    || value.as_str() == "~"
                    || value.starts_with("~/")
                    || (cfg!(windows) && value.starts_with("~\\"))))
            && Path::new(value.as_str()).is_relative()
        {
            *value = cwd
                .join(value.as_str())
                .to_str()
                .ok_or_else(|| invalid("non-Unicode native configuration path"))?
                .into();
        }
    }
    if matches!(host, "codex" | "claude-code" | "agy") {
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
pub fn child(
    host: &str,
    args: &[String],
    cwd: &Path,
    session: Option<&str>,
) -> Result<ProcessCommand> {
    let arguments = child_arguments(host, args, cwd, session)?;
    let resolved = acyclic_native_runtime::host_command(program(host)?).map_err(storage)?;
    let mut command = ProcessCommand::new(resolved.get_program());
    command.args(resolved.get_args());
    command
        .args(arguments)
        .current_dir(cwd)
        .stdin(ProcessStdio::piped())
        .stdout(ProcessStdio::piped())
        .stderr(ProcessStdio::piped());
    Ok(command)
}

/// The arguments of [`child`]: the inherited policy, the host's
/// non-interactive event mode and its resume reference, never an option.
pub fn child_arguments(
    host: &str,
    args: &[String],
    cwd: &Path,
    session: Option<&str>,
) -> Result<Vec<OsString>> {
    validate(host, args)?;
    if session.is_some_and(|reference| reference.starts_with('-')) {
        return Err(invalid("native resume reference cannot be an option"));
    }
    let native_arguments = match host {
        "codex" => 9,
        "claude-code" => 6,
        "opencode" => 5,
        "agy" => 6,
        _ => 3,
    };
    let mut arguments =
        Vec::with_capacity(args.len() + native_arguments + 2 * usize::from(session.is_some()));
    match host {
        "codex" => {
            extend_policy(&mut arguments, host, args, cwd)?;
            arguments.extend(["exec", "--json", "--cd"].into_iter().map(OsString::from));
            arguments.push(cwd.into());
            if let Some(id) = session {
                arguments.extend(["resume", id].into_iter().map(OsString::from));
            }
            arguments.push("-".into());
        }
        "claude-code" => {
            arguments.extend(
                ["-p", "--output-format", "stream-json", "--verbose"]
                    .into_iter()
                    .map(OsString::from),
            );
            extend_policy(&mut arguments, host, args, cwd)?;
            if let Some(id) = session {
                arguments.extend(["--resume", id].into_iter().map(OsString::from));
            }
        }
        "pi" => {
            arguments.extend(["--mode", "json", "-p"].into_iter().map(OsString::from));
            extend_policy(&mut arguments, host, args, cwd)?;
            // Each turn runs in a fresh view directory and the previous one
            // is retired, so Pi refuses to reopen a session bound to it.
            // A fork carries the whole conversation into this turn's cwd.
            if let Some(file) = session {
                arguments.extend(["--fork", file].into_iter().map(OsString::from));
            }
        }
        "opencode" => {
            arguments.extend(
                ["run", "--format", "json", "--dir"]
                    .into_iter()
                    .map(OsString::from),
            );
            arguments.push(cwd.into());
            extend_policy(&mut arguments, host, args, cwd)?;
            if let Some(id) = session {
                arguments.extend(["--session", id].into_iter().map(OsString::from));
            }
        }
        "agy" => {
            arguments.extend(
                [
                    "--input-format",
                    "stream-json",
                    "--output-format",
                    "stream-json",
                ]
                .into_iter()
                .map(OsString::from),
            );
            extend_policy(&mut arguments, host, args, cwd)?;
            if let Some(id) = session {
                arguments.extend(["--conversation", id].into_iter().map(OsString::from));
            }
        }
        _ => return Err(invalid("unsupported native host")),
    }
    Ok(arguments)
}

fn extend_policy(
    output: &mut Vec<OsString>,
    host: &str,
    args: &[String],
    cwd: &Path,
) -> Result<()> {
    let mut timeout = false;
    let mut denied = false;
    let mut arguments = args.iter();
    while let Some(name) = arguments.next() {
        output.push(name.into());
        if !is_switch(host, name) {
            let value = arguments.next().ok_or_else(|| invalid("step=resolve native policy: option requires a value; recovery: acyclic host HOST -- POLICY_ARGS"))?;
            timeout |= host == "agy" && name == "--print-timeout";
            output.push(match (host, name.as_str()) {
                ("agy", "--settings") => agy::settings(value, cwd)?,
                // Keep every user denial when adding the mandatory delegation fence.
                ("claude-code", "--disallowedTools") => {
                    denied = true;
                    let mut denied = OsString::with_capacity(value.len() + ",Agent,Task".len());
                    denied.push(value);
                    denied.push(",Agent,Task");
                    denied
                }
                _ => value.into(),
            });
        }
    }
    match host {
        "agy" if !timeout => {
            // Zero is the native unbounded lifetime: completion owns the wait.
            output.extend(["--print-timeout", "0"].map(OsString::from));
        }
        "codex" => output.extend(
            [
                "--config",
                "features.multi_agent_v2=false",
                "--config",
                "agents.enabled=false",
            ]
            .map(OsString::from),
        ),
        "claude-code" if !denied => {
            output.extend(["--disallowedTools", "Agent,Task"].map(OsString::from))
        }
        _ => {}
    }
    Ok(())
}

/// Resolve native file permissions against this invocation's admitted workspace.
/// The serialized parent policy retains relative targets for later child views.
pub(super) fn policy_arguments(host: &str, args: &[String], cwd: &Path) -> Result<Vec<OsString>> {
    let mut output = Vec::with_capacity(args.len() + if host == "codex" { 4 } else { 2 });
    extend_policy(&mut output, host, args, cwd)?;
    Ok(output)
}

/// One prompt on the native host's stdin. Antigravity consumes a framed user
/// event; the other hosts consume the prompt bytes directly.
pub(super) fn child_input(host: &str, prompt: &str) -> Result<Vec<u8>> {
    if host != "agy" {
        return Ok(prompt.as_bytes().to_vec());
    }
    #[derive(serde::Serialize)]
    struct Message<'a> {
        content: &'a str,
    }
    #[derive(serde::Serialize)]
    struct User<'a> {
        event: &'static str,
        message: Message<'a>,
    }
    let mut input = serde_json::to_vec(&User {
        event: "user",
        message: Message { content: prompt },
    })
    .map_err(storage)?;
    input.push(b'\n');
    Ok(input)
}

/// Runs `acyclic host HOST -- POLICY_ARGS`: the interactive native root under
/// its validated policy. Each invocation resolves native relative file rules
/// in its own admitted view; children otherwise inherit the selected policy.
pub fn launch(args: &[String]) -> Result<()> {
    let host = args
        .first()
        .ok_or_else(|| invalid("host requires codex, claude-code, pi, opencode or agy"))?;
    let flags = args.get(1..).unwrap_or_default();
    let flags = match flags {
        [separator, rest @ ..] if separator == "--" => rest,
        _ => flags,
    };
    let cwd = std::env::current_dir().map_err(storage)?;
    let flags = parent_flags(host, flags, &cwd)?;
    let status = run_root_session(&cwd, host, OsStr::new(program(host)?), &flags)?;
    if !status.success() {
        return Err(invalid(format!("native {host} exited {status}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

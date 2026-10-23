//! Thin native-host workspace client. `GraphCoder` owns the shared workspace.
mod certification;
mod codex;
mod install;
#[cfg(test)]
mod tests;

use acyclic_harness::{
    TaskId,
    local::{host_files, host_owner::HostAction},
};
use serde_json::{Value, json};
use std::{
    env,
    io::{self, Read as _},
    path::{Path, PathBuf},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    let Some(command) = args.first().cloned() else {
        return help();
    };
    if acyclic_plugin_identity::is_command(&command) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(acyclic_plugin_identity::run(&args))?;
        return Ok(());
    }
    if internal(&args)? {
        return Ok(());
    }
    let task = if args.first().map(String::as_str) == Some("--task") {
        if args.len() < 3 {
            return Err("--task requires ID COMMAND".into());
        }
        let id = args.get(1).ok_or("--task requires ID COMMAND")?;
        let task = parse_task(id)?;
        args.drain(..2);
        task
    } else {
        serde_json::from_value(
            host_files::request(
                &env::current_dir()?,
                HostAction::Locate {
                    cwd: env::current_dir()?,
                },
            )?
            .get("task")
            .cloned()
            .ok_or("harness omitted task identity")?,
        )?
    };
    let action = action(task, &args)?;
    let value = host_files::request(&env::current_dir()?, action)?;
    if let Some(stdout) = value.get("stdout").and_then(Value::as_str) {
        print!("{stdout}");
    } else {
        println!("{value}");
    }
    Ok(())
}

fn internal(args: &[String]) -> Result<bool> {
    match args.first().map(String::as_str).unwrap_or_default() {
        "--help" | "-h" => {
            help()?;
        }
        "--version" | "-V" => {
            println!("acyclic {}", env!("CARGO_PKG_VERSION"));
        }
        "__mcp" => {
            codex::serve()?;
        }
        "__hook" => {
            let [_, host, event] = args else {
                return Err("__hook requires HOST EVENT".into());
            };
            let answer = match read_json(io::stdin().lock()) {
                Ok(input) => hook(host, event, input),
                Err(error) => hook_failure(event, &error.to_string()),
            };
            if event == "WorktreeCreate" {
                let path = answer
                    .get("cwd")
                    .and_then(Value::as_str)
                    .ok_or_else(|| answer.to_string())?;
                println!("{path}");
            } else {
                println!("{answer}");
            }
        }
        "__workspace" => {
            let action = serde_json::from_value(read_json(io::stdin().lock())?)?;
            println!("{}", host_files::request(&env::current_dir()?, action)?);
        }
        "install" | "uninstall" | "doctor" => {
            install::run(args)?;
        }
        "__verify-certification" => {
            let [_, path] = args else {
                return Err("certification verification requires RECEIPT".into());
            };
            certification::verify(Path::new(path))?;
        }
        "__installer-rename" => {
            let [_, from, to, mode] = args else {
                return Err("installer rename requires FROM TO MODE".into());
            };
            let mode = match mode.as_str() {
                "replace" => acyclic_native_runtime::RenameMode::Replace,
                "no-replace" => acyclic_native_runtime::RenameMode::NoReplace,
                _ => return Err("invalid rename mode".into()),
            };
            acyclic_native_runtime::durable_rename(Path::new(from), Path::new(to), mode, None)?;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn action(task: TaskId, args: &[String]) -> Result<HostAction> {
    Ok(match args.first().map(String::as_str) {
        Some("fs" | "discard") => HostAction::Fs {
            task,
            command: args
                .iter()
                .skip(usize::from(args.first().map(String::as_str) == Some("fs")))
                .map(|arg| format!("'{}'", arg.replace('\'', "'\"'\"'")))
                .collect::<Vec<_>>()
                .join(" "),
        },
        Some("agents") => HostAction::Query {
            task,
            query: acyclic_harness::task_volume::Query::Agents,
        },
        Some("log") => HostAction::Fs {
            task,
            command: "log".into(),
        },
        Some("cancel") if args.len() == 2 => HostAction::Cancel {
            task: parse_task(args.get(1).ok_or("missing task ID")?)?,
        },
        Some("reopen") if args.len() == 2 => HostAction::Reopen {
            task: parse_task(args.get(1).ok_or("missing task ID")?)?,
        },
        Some("capture") => HostAction::Capture { task },
        _ => {
            return Err(
                "use fs, discard, agents, log, capture, cancel or reopen; see --help".into(),
            );
        }
    })
}

fn help() -> Result<()> {
    println!(
        "Acyclic {}\nNative hosts own their tools, permissions and agent loops.\nStart graphcoder --host-workspace --host-state-directory PATH outside the host sandbox.\nacyclic install|uninstall|doctor HOST [--plugin-root PATH] [--settings PATH]\nacyclic [--task ID] fs COMMAND... | discard REF | agents | log | capture | cancel ID | reopen ID",
        env!("CARGO_PKG_VERSION")
    );
    Ok(())
}

fn parse_task(value: &str) -> Result<TaskId> {
    Ok(serde_json::from_value(json!(value))?)
}

fn read_json(input: impl io::Read) -> Result<Value> {
    let mut bytes = Vec::new();
    input.take(65_537).read_to_end(&mut bytes)?;
    if bytes.len() > 65_536 {
        return Err("native hook exceeds shared command file bound".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn hook(host: &str, event: &str, input: Value) -> Value {
    let result = (|| -> Result<Value> {
        let cwd = PathBuf::from(
            input
                .get("cwd")
                .and_then(Value::as_str)
                .ok_or("native hook lacks cwd")?,
        );
        Ok(host_files::request(
            &cwd,
            HostAction::Hook {
                host: host.into(),
                event: event.into(),
                input,
            },
        )?)
    })();
    result.unwrap_or_else(|error| hook_failure(event, &error.to_string()))
}

fn hook_failure(event: &str, error: &str) -> Value {
    if event == "PreToolUse" {
        json!({"hookSpecificOutput":{"hookEventName":event,"permissionDecision":"deny","permissionDecisionReason":error}})
    } else {
        json!({"continue":false,"stopReason":error})
    }
}

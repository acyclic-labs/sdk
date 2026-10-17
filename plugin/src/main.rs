//! Native hook clients and installation; all task ownership belongs to the harness.
mod identity;
mod install;
mod pi;
#[cfg(test)]
mod tests;

use acyclic_native_runtime::{Durability, RenameMode, durable_rename, sync_file, sync_parent};
use install::*;
use pi::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    env, fs,
    io::{self, Read as _, Write as _},
    path::{Path, PathBuf},
};

fn main() -> std::process::ExitCode {
    match main_result() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            if env::args().nth(1).as_deref() == Some("__hook") {
                std::process::ExitCode::from(2)
            } else {
                std::process::ExitCode::FAILURE
            }
        }
    }
}

fn main_result() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    match arguments.as_slice() {
        [command, ..] if command == "__service" => {
            owner_runtime()?.block_on(acyclic_harness::external_executor::run_local_owner())?;
        }
        [command] if command == "__owner-stop" => {
            runtime()?.block_on(acyclic_harness::external_executor::stop_local_owner())?;
        }
        [command] if command == "__tools" => {
            runtime()?.block_on(acyclic_harness::external_executor::serve_native_tools())?;
        }
        [command] if command == "__catalog" => {
            println!("{}",acyclic_harness::external_executor::native_tool_catalog()?);
        }
        [command, rest @ ..] if identity::is_command(command) => {
            runtime()?.block_on(identity::run(&std::iter::once(command.clone()).chain(rest.iter().cloned()).collect::<Vec<_>>()))?;
        }
        [command, host, event] if command == "__hook" => {
            let answer = runtime()?.block_on(acyclic_harness::external_executor::run_hook(host, event));
            match answer {
                Ok(answer) => println!("{}", serde_json::to_string(&answer)?),
                Err(error) => {
                    println!("{}", acyclic_harness::external_executor::hook_failure(host, event, &error.to_string()));
                    return Err(error.into());
                }
            }
        }
        [command, task, path] if command == "__files" => {
            runtime()?.block_on(acyclic_harness::external_executor::run_native_files(task, path))?;
        }
        [command, rest @ ..] if command == "install" => install_command(rest).map_err(io::Error::other)?,
        [command, rest @ ..] if matches!(command.as_str(), "__installer-rename" | "__verify-certification") => {
            install::activation(command, rest)?;
        }
        [command, host] if command == "uninstall" => uninstall_host(host).map_err(io::Error::other)?,
        [command] if command == "--version" || command == "version" => println!("acyclic {}", env!("CARGO_PKG_VERSION")),
        _ => return Err(io::Error::other("usage: acyclic install HOST [--project] | uninstall HOST | __hook HOST EVENT | login | whoami").into()),
    }
    Ok(())
}

fn runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}
fn owner_runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
}
fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn current_executable() -> Result<PathBuf, String> {
    env::current_exe()
        .and_then(fs::canonicalize)
        .map_err(display)
}
fn default_data_directory() -> PathBuf {
    #[cfg(windows)]
    if let Some(root) = env::var_os("LOCALAPPDATA") {
        return PathBuf::from(root).join("Acyclic/state-v5");
    }
    #[cfg(not(windows))]
    if let Some(root) = env::var_os("XDG_STATE_HOME") {
        return PathBuf::from(root).join("acyclic/state-v5");
    }
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir)
        .join(".local/state/acyclic/state-v5")
}

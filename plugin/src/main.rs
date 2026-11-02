//! Product entry point over the shared agent frontend and native-host adapters.
mod install;
#[cfg(test)]
mod tests;

use acyclic_harness::{
    agent::AgentPath,
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
    let arguments = match acyclic_plugin::ingress::parse(env::args_os().collect()) {
        Ok(acyclic_plugin::ingress::Input::Agent(arguments)) => {
            return std::process::Termination::report(acyclic_plugin::main_from(
                arguments,
                std::convert::identity,
                None,
            ));
        }
        Ok(acyclic_plugin::ingress::Input::Native(arguments)) => arguments,
        Err(error) => {
            eprintln!("{error}");
            return std::process::ExitCode::from(2);
        }
    };
    match run(arguments) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run(args: Vec<String>) -> Result<()> {
    let Some(command) = args.first() else {
        return help();
    };
    if acyclic_plugin_identity::is_command(command) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(acyclic_plugin_identity::run(
                &args,
                Some(acyclic_plugin_identity::interrupt_context()),
            ))?;
        return Ok(());
    }
    if internal(&args)? {
        return Ok(());
    }
    let (selected, arguments) = match args.as_slice() {
        [flag, task, rest @ ..] if flag == "--task" && !rest.is_empty() => (Some(task), rest),
        [flag, ..] if flag == "--task" => {
            return Err("step=admit native command: --task requires ID COMMAND; recovery: run acyclic --help".into());
        }
        _ => (None, args.as_slice()),
    };
    let mut directory = None;
    let action = acyclic_plugin::ingress::workspace_action(arguments, || {
        if let Some(task) = selected {
            return parse_task(task);
        }
        if let Ok(task) = env::var("ACYCLIC_TASK") {
            return parse_task(&task);
        }
        let cwd = env::current_dir()?;
        let response = host_files::request(&cwd, HostAction::Locate { cwd: cwd.clone() })?;
        directory = Some(cwd);
        Ok(serde_json::from_value(
            response
                .get("task")
                .cloned()
                .ok_or("harness omitted task identity")?,
        )?)
    })?;
    let directory = match directory {
        Some(directory) => directory,
        None => env::current_dir()?,
    };
    let value = host_files::request(&directory, action)?;
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
            acyclic_harness::local::host_process::children::serve(
                args.get(1).map_or("codex", String::as_str),
            )?;
        }
        "host" => {
            acyclic_harness::local::host_process::launch::launch(
                args.get(1..).unwrap_or_default(),
            )?;
        }
        "__hook" => {
            let [_, host, event] = args else {
                return Err("__hook requires HOST EVENT".into());
            };
            let answer = match read_json(io::stdin().lock()) {
                Ok(input) => hook(host, event, input),
                Err(error) => hook_failure(event, &error.to_string()),
            };
            println!("{answer}");
        }
        "__workspace" => {
            let action = serde_json::from_value(read_json(io::stdin().lock())?)?;
            println!("{}", host_files::request(&env::current_dir()?, action)?);
        }
        "install" | "uninstall" | "doctor" => {
            install::run(args)?;
        }
        "__verify-certification" => {
            let path = match args {
                [_, path] => path,
                [_, path, version] if version == env!("CARGO_PKG_VERSION") => path,
                _ => return Err("step=admit installed certification version; recovery: install a package whose release version matches its executable, then run acyclic __verify-certification RECEIPT VERSION".into()),
            };
            let receipt = verify_certification(
                path,
                &env::current_exe()?,
                env::consts::OS,
                env::consts::ARCH,
                env!("CARGO_PKG_VERSION"),
            )?;
            if args.len() == 3 {
                println!("{receipt}");
            }
        }
        "__install-package" => install_package()?,
        "__verify-release-certification" => {
            let [_, receipt, executable, os, arch, version] = args else {
                return Err("release certification verification requires RECEIPT EXECUTABLE OS ARCH VERSION; recovery: run acyclic __verify-release-certification with all five arguments".into());
            };
            verify_certification(receipt, Path::new(executable), os, arch, version)?;
        }
        command if command.starts_with("__") => {
            return Err(format!("step=admit internal command: unknown {command}; recovery: run acyclic --help and use the installed package entry points").into());
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn install_package() -> Result<()> {
    let request: acyclic_fs::package_install::Request =
        serde_json::from_value(read_json(io::stdin().lock())?)?;
    let recovery = serde_json::to_string(&json!(["node", request.directory.join("install.js")]))?;
    let test = env::var("NODE_ENV").as_deref() == Ok("test");
    use acyclic_fs::package_install::Stage;
    acyclic_fs::package_install::install(&request, |stage| {
        let point = match stage {
            Stage::Intent => "staged",
            Stage::Ready => "ready",
            Stage::Published => "published",
            Stage::Activated => "activated",
            Stage::IdentityPublished => "identity-published",
        };
        if test && env::var("ACYCLIC_INSTALL_TEST_CRASH").as_deref() == Ok(point) {
            std::process::exit(86);
        }
        if test
            && stage == Stage::IdentityPublished
            && env::var("ACYCLIC_INSTALL_TEST_FAIL").as_deref() == Ok("cleanup-after-identity")
        {
            return Err(io::Error::other(
                "injected Acyclic installer failure at cleanup-after-identity",
            ));
        }
        Ok(())
    })
    .map_err(|error| format!("step=publish native package: {error}; recovery argv: {recovery}"))?;
    Ok(())
}

fn verify_certification(
    receipt: &str,
    executable: &Path,
    os: &str,
    arch: &str,
    version: &str,
) -> Result<Value> {
    let recovery = serde_json::to_string(&json!([
        env::current_exe()?,
        "__verify-release-certification",
        receipt,
        executable,
        os,
        arch,
        version
    ]))?;
    let verify = || -> Result<Value> {
        let document = acyclic_fs::mount_qualification::verify(
            std::fs::File::open(receipt)?,
            std::fs::File::open(executable)?,
            acyclic_fs::mount_qualification::ReleaseTarget { os, arch, version },
        )?;
        Ok(document)
    };
    verify().map_err(|error| {
        format!(
            "step=verify native mount release certification: {error}; recovery argv: {recovery}"
        )
        .into()
    })
}

fn help() -> Result<()> {
    acyclic_plugin::command().print_help()?;
    println!(
        "\nNative hosts own their tools, permissions and agent loops.\nStart acyclic run --host-workspace --host-state-directory PATH outside the host sandbox.\nacyclic install|uninstall|doctor HOST [--plugin-root PATH] [--settings PATH]\nacyclic host HOST -- POLICY_ARGS\nacyclic [--task ID] fs COMMAND... | discard REF | agents | log | capture | reopen ID"
    );
    Ok(())
}

fn parse_task(value: &str) -> Result<AgentPath> {
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
        let input = hook_input(input, env::var_os("ACYCLIC_TASK").as_deref())?;
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

fn hook_input(mut input: Value, task: Option<&std::ffi::OsStr>) -> Result<Value> {
    if let Some(task) = task {
        let task = parse_task(task.to_str().ok_or("native task identity is not UTF-8")?)?;
        input
            .as_object_mut()
            .ok_or("native hook input must be an object")?
            .insert("task".into(), json!(task));
    }
    Ok(input)
}

fn hook_failure(_event: &str, error: &str) -> Value {
    json!({"continue":false,"stopReason":error})
}

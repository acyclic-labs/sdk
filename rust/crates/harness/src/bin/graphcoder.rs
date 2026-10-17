//! Argv, the harness client, and shared TUI composition.

#[path = "graphcoder/args.rs"]
mod args;
#[path = "graphcoder/view.rs"]
mod view;

use acyclic_harness::{
    OperationId, client,
    local::{LocalHarness, LocalHarnessConfig},
};
use acyclic_harness_providers::{Credentials, Registry};
use args::{Args, InputFormat};
use clap::Parser;
use std::{process::ExitCode, sync::Arc};

fn main() -> ExitCode {
    let args = Args::parse();
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
        .and_then(|runtime| {
            let result = runtime.block_on(run(args));
            runtime.shutdown_background();
            result
        });
    match result {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("graphcoder: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<u8, Box<dyn std::error::Error>> {
    let operation = args
        .operation
        .as_deref()
        .map(OperationId::parse)
        .transpose()?;
    let credentials = args
        .credentials
        .as_deref()
        .map(Credentials::from_file)
        .transpose()?
        .unwrap_or_default();
    let mut registry = Registry::new(credentials)?;
    if let Some(path) = &args.catalog {
        registry.load_catalog(path)?;
    }
    let provider = Arc::new(registry.resolve(&args.model)?);
    let mut config = LocalHarnessConfig::new(&args.workspace, (provider.model().clone(), provider));
    if let Some(root) = &args.settings_root {
        config = config.settings(
            acyclic_harness::native_settings::SettingsConfig::new(&args.workspace, root)
                .codex_home(root.join(".codex")),
        );
    }
    let local = Arc::new(LocalHarness::open(config).await?);
    let operation = client::local_identity(&local, args.prompt.as_deref(), operation).await?;
    let command = args
        .prompt
        .as_deref()
        .map(|prompt| client::prepare_local_prompt(operation, prompt))
        .transpose()?;
    if args.prompt.is_none()
        && args.input_format != InputFormat::Jsonl
        && !acyclic_harness::tui::term::Environment::capture().is_dumb()
    {
        let result = client::with_local_commands(
            local.clone(),
            operation,
            view::run(local.clone(), operation, args.inline),
        )
        .await;
        client::disconnect_local(local).await?;
        result?;
        return Ok(0);
    }
    let (mut connection, target) = client::connect_local(local, operation).await?;
    let outcome = if let Some(command) = command {
        client::run_headless(
            &mut connection,
            command,
            target,
            &mut std::io::stdout().lock(),
        )
        .await
    } else {
        client::run_jsonl(
            &mut connection,
            target,
            client::stdin_jsonl(),
            &mut std::io::stdout().lock(),
        )
        .await
    };
    Ok(connection.finish(outcome).await?.exit_code())
}

#[cfg(test)]
#[path = "graphcoder/tests.rs"]
mod tests;

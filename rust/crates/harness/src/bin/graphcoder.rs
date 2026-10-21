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
use futures::{FutureExt as _, future::LocalBoxFuture};
use std::{future::Future, process::ExitCode, sync::Arc};

// Construct each phase on its own call frame. Boxing an already constructed
// future in the awaiting frame still retains its construction scratch space
// throughout nested polls, and devirtualization can inline its poll there too.
#[inline(never)]
fn phase<'a, F: Future + 'a>(build: impl FnOnce() -> F) -> LocalBoxFuture<'a, F::Output> {
    build().boxed_local()
}

fn main() -> ExitCode {
    let args = Args::parse();
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
        .and_then(|runtime| {
            // Keep Tokio's entry frame independent of the size of the CLI future.
            let result = runtime.block_on(phase(|| run(args)));
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

/// The local owner exactly as the shipping binary composes it from argv and
/// resolved credentials: the provider registry, its catalog and the
/// workspace's native settings.
async fn open(
    args: &Args,
    credentials: Credentials,
) -> Result<Arc<LocalHarness>, Box<dyn std::error::Error>> {
    let mut registry = Registry::new(credentials)?;
    if let Some(path) = &args.catalog {
        registry.load_catalog(path)?;
    }
    let home = args
        .settings_root
        .clone()
        .or_else(dirs::home_dir)
        .ok_or("user settings directory is unavailable")?;
    let mut settings =
        acyclic_harness::native_settings::SettingsConfig::new(&args.workspace, &home);
    if args.settings_root.is_some() {
        settings = settings.codex_home(home.join(".codex"));
    }
    let selection = match &args.model {
        Some(selection) => selection.clone(),
        None => settings
            .configured_model()?
            .unwrap_or_else(|| registry.default_model().into()),
    };
    let provider = Arc::new(registry.resolve(&selection)?);
    let mut config = LocalHarnessConfig::new(&args.workspace, (provider.model().clone(), provider));
    config = config.settings(settings).web_tools();
    Ok(Arc::new(phase(|| LocalHarness::open(config)).await?))
}

// Own native phase futures separately from the CLI orchestration frame.
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
    let local = phase(|| open(&args, credentials)).await?;
    let operation =
        phase(|| client::local_identity(&local, args.prompt.as_deref(), operation)).await?;
    let command = args
        .prompt
        .as_deref()
        .map(|prompt| client::prepare_local_prompt(operation, prompt))
        .transpose()?;
    if args.prompt.is_none()
        && args.input_format != InputFormat::Jsonl
        && !acyclic_harness::tui::term::Environment::capture().is_dumb()
    {
        let result = phase(|| {
            client::with_local_commands(
                local.clone(),
                operation,
                view::run(local.clone(), operation, args.inline),
            )
        })
        .await;
        phase(|| client::disconnect_local(local)).await?;
        result?;
        return Ok(0);
    }
    let (mut connection, target) = phase(|| client::connect_local(local, operation)).await?;
    let mut output = std::io::stdout().lock();
    let outcome = if let Some(command) = command {
        phase(|| client::run_headless(&mut connection, command, target, &mut output)).await
    } else {
        phase(|| client::run_jsonl(&mut connection, target, client::stdin_jsonl(), &mut output))
            .await
    };
    Ok(phase(|| connection.finish(outcome)).await?.exit_code())
}

#[cfg(test)]
#[path = "graphcoder/tests.rs"]
mod tests;

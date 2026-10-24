//! Argv, the harness client, and shared TUI composition.
#[cfg(test)]
acyclic_test_support::counting_allocator!();

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
    let models = settings.configured_models(|name| registry.alloys().contains_key(name))?;
    let alloys = models
        .alloys
        .into_iter()
        .map(|(name, spec)| serde_json::from_value(spec).map(|spec| (name, spec)))
        .collect::<Result<_, _>>()?;
    registry.register_alloys(alloys)?;
    let selection = match &args.model {
        Some(selection) => selection.clone(),
        None => models
            .selection
            .unwrap_or_else(|| registry.default_model().into()),
    };
    let provider = Arc::new(registry.resolve_selection(
        &selection,
        acyclic_harness_providers::ModelFeatures {
            tools: true,
            images: false,
        },
    )?);
    let mut config = LocalHarnessConfig::new(&args.workspace, (provider.model().clone(), provider));
    let search = acyclic_harness::search::SearchConfig::from_file(&settings.search_config_path())?;
    let search = if args.settings_root.is_some() {
        search.without_native_sources()
    } else {
        search
    };
    if let Some(provider) = search.provider()? {
        config = config.search_provider(provider);
    }
    config = config
        .agent_roles(models.roles.root, models.roles.child)?
        .settings(settings)
        .web_tools();
    let code = acyclic_harness::package::harness::HarnessCode::new()?;
    Ok(Arc::new(phase(|| code.open_local(config)).await?))
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
    let operation = if args.resume {
        phase(|| client::resume_local_identity(&local, operation)).await?
    } else {
        client::local_identity(operation)
    };
    let command = args
        .prompt
        .as_deref()
        .map(|prompt| client::prepare_local_prompt(operation, prompt))
        .transpose()?;
    if args.prompt.is_none()
        && args.input_format != InputFormat::Jsonl
        && !acyclic_harness::tui::term::Environment::capture().is_dumb()
    {
        phase(|| client::select_local_identity(&local, operation)).await?;
        let result = phase(|| {
            client::with_local_commands(
                local.clone(),
                operation,
                view::run(local.clone(), operation, args.inline),
            )
        })
        .await;
        phase(|| client::disconnect_local(local)).await?;
        return Ok(result?.exit_code());
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

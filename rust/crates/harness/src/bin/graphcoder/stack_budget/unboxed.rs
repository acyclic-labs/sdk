//! The pre-fix CLI boundary, with its awaited futures retained inline.
use crate::{
    args::{Args, InputFormat},
    view,
};
use acyclic_harness::{
    OperationId, client,
    local::{LocalHarness, LocalHarnessConfig},
};
use acyclic_harness_providers::{Credentials, Registry};
use std::sync::Arc;

pub(super) fn serve(
    args: Args,
    credentials: Option<Credentials>,
    state: Option<std::path::PathBuf>,
) -> Result<u8, Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run(args, credentials, state));
    runtime.shutdown_background();
    result
}

async fn run(
    args: Args,
    explicit: Option<Credentials>,
    state: Option<std::path::PathBuf>,
) -> Result<u8, Box<dyn std::error::Error>> {
    let operation = args
        .operation
        .as_deref()
        .map(OperationId::parse)
        .transpose()?;
    let credentials = if let Some(explicit) = explicit {
        explicit
    } else {
        args.credentials
            .as_deref()
            .map(Credentials::from_file)
            .transpose()?
            .unwrap_or_default()
    };
    let mut registry = Registry::new(credentials)?;
    if let Some(path) = &args.catalog {
        registry.load_catalog(path)?;
    }
    let provider = Arc::new(
        registry.resolve(
            args.model
                .as_deref()
                .unwrap_or_else(|| registry.default_model()),
        )?,
    );
    let mut config = LocalHarnessConfig::new(&args.workspace, (provider.model().clone(), provider));
    if let Some(state) = state {
        config = config.state_directory(state);
    }
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
        // Built here, as before the fix: the presentation's whole state moves
        // through this frame.
        let presentation = view::run(local.clone(), operation, args.inline);
        let result =
            client::with_local_commands(local.clone(), operation, move || presentation).await;
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

//! Native composition: inputs write files; query publications drive dispatch/rendering.
use super::*;
use acyclic_harness::{
    Error, OperationId, Result, TaskId,
    client::{CommandFileRoot, CommandResolution, CommandSession, DEFAULT_COMMANDS_JSON},
    conversation::ContentResidencyVerifier,
    task_volume::TaskVolume,
};
use acyclic_stream::{StreamPath, StreamProvider};
use serde_json::{Value, json};
use std::{
    io::{IsTerminal as _, Write as _},
    sync::Arc,
};

struct NoReads;
impl ContentResidencyVerifier for NoReads {}

pub(super) async fn run(backend: Backend, headless: bool) -> Result<()> {
    let fixture = matches!(backend, Backend::Fixture(_));
    let interactive = !headless && std::io::stdin().is_terminal();
    let mut output = std::io::stdout().lock();
    let (provider, identity): (Arc<dyn StreamProvider>, Vec<u8>) = match &backend {
        Backend::Fixture(_) => (
            Arc::new(acyclic_stream::MemoryStream::default()),
            b"fixture".to_vec(),
        ),
        #[cfg(feature = "filesystem-local")]
        Backend::Native(native) => {
            use acyclic_harness::conversation::{ContentGrant, VolumeOperation};
            let verifier = native.config.owner.verifier()?;
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                ContentGrant::verify(
                    &verifier,
                    &native.config.owner.scope,
                    &native.config.private_volume,
                    operation,
                    terminal::MAX_FRAME_BYTES as u64,
                    &mut None,
                )?;
            }
            (
                native.stream.clone(),
                serde_json::to_vec(&native.config.private_volume).map_err(invalid)?,
            )
        }
    };
    let digest = blake3::hash(&identity);
    let session = match &backend {
        #[cfg(feature = "filesystem-local")]
        Backend::Native(native) => native.config.ui_session.unwrap_or_else(uuid::Uuid::new_v4),
        _ => uuid::Uuid::new_v4(),
    };
    let path = StreamPath::new(format!("graphcoder-files/{}", digest.to_hex())).map_err(invalid)?;
    let volume = Arc::new(TaskVolume::open(provider, path).await.map_err(invalid)?);
    let mut client = CommandSession::open(
        volume.clone(),
        session,
        terminal::MAX_FRAME_BYTES as u64,
        4096,
    )
    .await?;
    let defaults: Value = serde_json::from_slice(DEFAULT_COMMANDS_JSON).map_err(invalid)?;
    let mut specs = defaults
        .as_array()
        .ok_or_else(|| Error::Invalid("bootstrap catalog shape".into()))?
        .clone();
    let sdk: Vec<Value> =
        serde_json::from_str(include_str!("commands/sdk.json")).map_err(invalid)?;
    specs.extend(sdk);
    let base = serde_json::to_vec(&specs).map_err(invalid)?;
    #[cfg(feature = "filesystem-local")]
    let owner = if let Backend::Native(native) = &backend {
        let mut config = acyclic_harness::LocalHarnessConfig::new(
            native
                .config
                .command_workspace
                .clone()
                .unwrap_or_else(|| native.config.filesystem_root.clone()),
            (
                crate::provider::model(native.config.deterministic_delay_ms),
                native.model_provider.clone(),
            ),
        );
        if native.config.command_workspace.is_none() {
            config = config.volume(native.config.private_volume.clone());
        }
        Some(acyclic_harness::LocalHarness::open(config).await?)
    } else {
        None
    };
    #[cfg(feature = "filesystem-local")]
    let mut sources = if let Backend::Native(native) = &backend {
        let reader = acyclic_harness::filesystem::FilesystemContentVerifier::new(
            native.files.clone(),
            &native.config.owner.verifier()?,
            &native.config.owner.scope,
            terminal::MAX_FRAME_BYTES as u64,
            None,
        )?;
        Some(
            acyclic_harness::client::CommandSources::observe(
                &mut client,
                &volume,
                base.clone(),
                native.config.command_roots.clone(),
                native.config.skill_roots.clone(),
                reader,
                Default::default(),
                terminal::MAX_FRAME_BYTES as u64,
                4096,
            )
            .await?,
        )
    } else {
        client.install_catalog(base.clone()).await?;
        None
    };
    #[cfg(not(feature = "filesystem-local"))]
    client.install_catalog(base).await?;
    let mut layout = client
        .state(CommandFileRoot::Session, "layout.json")?
        .unwrap_or(json!({}));
    layout["exit_requested"] = json!(false);
    client
        .write_state(CommandFileRoot::Session, "layout.json", &layout)
        .await?;
    #[cfg(feature = "filesystem-local")]
    if let Backend::Native(native) = &backend {
        let mut settings = client
            .state(CommandFileRoot::Config, "settings.json")?
            .unwrap_or(json!({}));
        if settings.get("workspace").is_none()
            && let Some(workspace) = &native.config.command_workspace
        {
            settings["workspace"] = json!(workspace);
            client
                .write_state(CommandFileRoot::Config, "settings.json", &settings)
                .await?;
        }
    }
    let mut cursor = backend.cursor();
    let mut backend = Some(backend);
    let mut input = input_lines()?;
    #[cfg(feature = "filesystem-local")]
    let mut workers = tokio::task::JoinSet::<Result<String>>::new();
    writeln!(output, "terminal-mode stdin-is-terminal={} interactive={interactive} headless={headless} process-id={}", std::io::stdin().is_terminal(), std::process::id()).map_err(invalid)?;
    writeln!(
        output,
        "GraphCoder — {}",
        if fixture {
            "fixture wire; UI playback only, no durable effects"
        } else {
            "actual local Harness runtime; commands follow volume files"
        }
    )
    .map_err(invalid)?;
    loop {
        while let Some(request) = client.take_request() {
            if let Some(saved) = client.activity_cursor() {
                use base64::Engine as _;
                use prost::Message as _;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(saved)
                    .map_err(invalid)?;
                cursor = acyclic_harness::wire::ReplayCursor::decode(bytes.as_slice())
                    .map_err(invalid)?;
            }
            let settings = client.request_settings(&request).await?;
            let selected = client.selected_task()?;
            let mut parent = selected;
            let sdk = serde_json::from_slice::<CommandResolution>(&request.bytes)
                .is_ok_and(|resolution| matches!(resolution, CommandResolution::Sdk { .. }));
            #[cfg(feature = "filesystem-local")]
            let owned_result = if !sdk && let Some(owner) = owner.as_ref() {
                Some(
                    acyclic_harness::client::consume_local_command(&mut client, owner, &request)
                        .await,
                )
            } else {
                None
            };
            #[cfg(not(feature = "filesystem-local"))]
            let owned_result = None;
            let result = if let Some(result) = owned_result {
                result
            } else {
                client
                    .consume(&request, |operation, target, resolution| {
                        dispatch(
                            #[cfg(feature = "filesystem-local")]
                            owner.as_ref(),
                            operation,
                            target,
                            resolution,
                            &settings,
                            &mut backend,
                            &mut cursor,
                            #[cfg(feature = "filesystem-local")]
                            &mut parent,
                            #[cfg(feature = "filesystem-local")]
                            &mut workers,
                            &mut output,
                        )
                    })
                    .await
            };
            match result {
                Ok(Some(value)) => {
                    if let Some(text) = value.get("rendered").and_then(Value::as_str) {
                        write!(output, "{}", terminal::text(text)).map_err(invalid)?;
                    } else {
                        writeln!(output, "command-json={value}").map_err(invalid)?;
                    }
                }
                Ok(None) => {}
                Err(Error::Indeterminate(operation)) => {
                    writeln!(output, "request outcome uncertain operation={operation}; inspect its files before reconciliation").map_err(invalid)?;
                }
                Err(error) if interactive => {
                    writeln!(output, "{}", terminal::text(&error.to_string())).map_err(invalid)?
                }
                Err(error) => return Err(error),
            }
            #[cfg(feature = "filesystem-local")]
            if parent != selected {
                let mut layout = client
                    .state(CommandFileRoot::Session, "layout.json")?
                    .unwrap_or(json!({}));
                layout["selected_task"] = json!(parent);
                client
                    .write_state(CommandFileRoot::Session, "layout.json", &layout)
                    .await?;
            }
        }
        if client.exit_requested()? {
            break;
        }
        if interactive {
            write!(output, "graphcoder> ").map_err(invalid)?;
        }
        output.flush().map_err(invalid)?;
        let previous_output = client.output_sequence();
        #[cfg(feature = "filesystem-local")]
        let line = tokio::select! {
            line = input.recv() => line,
            changed = client.observe_change() => {
                changed?;
                if client.output_sequence() != previous_output && let Some(value) = client.output_page(0)? { render_record(&mut output, &value)?; }
                continue;
            }
            changed = async {
                match sources.as_mut() { Some(sources) => sources.changed().await, None => std::future::pending().await }
            } => {
                changed?;
                if let Some(sources) = &mut sources { sources.refresh(&mut client).await?; }
                continue;
            }
            outcome = workers.join_next(), if !workers.is_empty() => {
                let rendered = outcome.ok_or_else(|| Error::Conflict("worker handle missing".into()))?.map_err(invalid)??;
                write!(output, "{}", terminal::text(&rendered)).map_err(invalid)?;
                continue;
            }
        };
        #[cfg(not(feature = "filesystem-local"))]
        let line = tokio::select! {
            line = input.recv() => line,
            changed = client.observe_change() => {
                changed?;
                if client.output_sequence() != previous_output && let Some(value) = client.output_page(0)? { render_record(&mut output, &value)?; }
                continue;
            }
        };
        let Some(line) = line else {
            break;
        };
        let line = line?;
        let text = line.trim_end_matches(['\r', '\n']);
        if text.is_empty() {
            continue;
        }
        let old_layout = client.state(CommandFileRoot::Session, "layout.json")?;
        let reader: Arc<dyn ContentResidencyVerifier> = match backend.as_ref() {
            #[cfg(feature = "filesystem-local")]
            Some(Backend::Native(native)) => {
                acyclic_harness::filesystem::FilesystemContentVerifier::new(
                    native.files.clone(),
                    &native.config.owner.verifier()?,
                    &native.config.owner.scope,
                    terminal::MAX_FRAME_BYTES as u64,
                    None,
                )?
            }
            _ => Arc::new(NoReads),
        };
        if let Err(error) = client.submit(text, reader.as_ref()).await {
            if interactive {
                writeln!(output, "{}", terminal::text(&error.to_string())).map_err(invalid)?;
            } else {
                return Err(error);
            }
        } else {
            let layout = client.state(CommandFileRoot::Session, "layout.json")?;
            if layout
                .as_ref()
                .and_then(|layout| layout.get("view_request"))
                == old_layout
                    .as_ref()
                    .and_then(|layout| layout.get("view_request"))
            {
                continue;
            }
            let view = layout
                .as_ref()
                .and_then(|layout| layout.get("open_view"))
                .and_then(Value::as_str);
            if view == Some("history") {
                let back = layout
                    .as_ref()
                    .and_then(|layout| layout.get("history_page"))
                    .and_then(Value::as_u64)
                    .and_then(|back| usize::try_from(back).ok())
                    .filter(|back| *back < terminal::MAX_DISPLAY_PAGES);
                let value = back
                    .map(|back| client.output_page(back))
                    .transpose()?
                    .flatten();
                if let Some(value) = value {
                    if let Some(rendered) = value
                        .get("ok")
                        .and_then(|value| value.get("rendered"))
                        .and_then(Value::as_str)
                    {
                        write!(output, "{}", terminal::text(rendered)).map_err(invalid)?;
                    } else {
                        writeln!(output, "command-json={value}").map_err(invalid)?;
                    }
                } else {
                    writeln!(
                        output,
                        "display page unavailable; request an explicit archival page"
                    )
                    .map_err(invalid)?;
                }
            }
            if view == Some("shortcuts") {
                for spec in client.catalog().commands() {
                    writeln!(
                        output,
                        "/{} — {}",
                        spec.name,
                        terminal::text(&spec.description)
                    )
                    .map_err(invalid)?;
                }
            }
        }
    }
    Ok(())
}

fn invalid(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

fn render_record(output: &mut impl std::io::Write, value: &Value) -> Result<()> {
    if let Some(rendered) = value
        .get("ok")
        .and_then(|value| value.get("rendered"))
        .and_then(Value::as_str)
    {
        write!(output, "{}", terminal::text(rendered)).map_err(invalid)?;
    } else {
        writeln!(output, "command-json={value}").map_err(invalid)?;
    }
    Ok(())
}

async fn dispatch(
    #[cfg(feature = "filesystem-local")] owner: Option<&acyclic_harness::LocalHarness>,
    operation: OperationId,
    target: Option<TaskId>,
    resolution: CommandResolution,
    settings: &Value,
    backend: &mut Option<Backend>,
    cursor: &mut acyclic_harness::wire::ReplayCursor,
    #[cfg(feature = "filesystem-local")] parent: &mut Option<TaskId>,
    #[cfg(feature = "filesystem-local")] workers: &mut tokio::task::JoinSet<Result<String>>,
    output: &mut impl std::io::Write,
) -> Result<Value> {
    if let CommandResolution::Sdk { command, arguments } = resolution {
        let before = cursor.clone();
        let rendered = commands::execute_sdk(
            &command,
            &arguments,
            backend,
            cursor,
            #[cfg(feature = "filesystem-local")]
            parent,
            #[cfg(feature = "filesystem-local")]
            workers,
            output,
        )
        .await?;
        let mut value = json!({"rendered":rendered});
        if *cursor != before {
            use base64::Engine as _;
            use prost::Message as _;
            value["activity_cursor"] =
                json!(base64::engine::general_purpose::STANDARD.encode(cursor.encode_to_vec()));
        }
        return Ok(value);
    }
    #[cfg(feature = "filesystem-local")]
    if let Some(owner) = owner {
        return owner.dispatch_command(operation, target, resolution).await;
    }
    let _ = (operation, target, resolution, settings);
    Err(Error::Invalid(
        "fixture playback has no command owner".into(),
    ))
}

//! Local defaults, durable stock turns and checkout selection through the public API.
#![cfg(feature = "filesystem-local")]

use crate as acyclic_harness;
use crate::test_support::{
    ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn,
};
use crate::{
    Admission, Error, LocalHarness, LocalHarnessConfig, OperationId, Result,
    context::ModelContextCapacity,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    filesystem::FilesystemHost,
    model::{Model, ModelEvent, ModelProvider},
    resources::ProviderRef,
    runtime::{TaskStateProvider, ToolContext},
    tool::{JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::{Value, json};
mod bootstrap;
mod compact;
mod compaction;
mod context;
#[cfg(feature = "native-execution")]
mod environment;
mod goals;
mod hermetic;
#[cfg(feature = "hooks")]
mod hook_configuration;
#[cfg(all(feature = "native-execution", unix))]
mod hook_lifecycle;
#[cfg(feature = "native-execution")]
mod input;
#[cfg(feature = "native-execution")]
mod input_user;
#[cfg(all(feature = "native-execution", unix))]
mod tool_hooks;
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

// Independent disk/credential fixtures must execute on workers, rather than
// monopolize a current-thread join. Share the worker pool across both sweeps.
pub(super) static WORKERS: std::sync::LazyLock<tokio::runtime::Runtime> =
    std::sync::LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    });

fn scripted(turns: impl IntoIterator<Item = ScriptedTurn>) -> Arc<ScriptedModelProvider> {
    Arc::new(ScriptedModelProvider::new(turns).with_context_capacity(capacity()))
}
fn text() -> Arc<ScriptedModelProvider> {
    scripted([ScriptedTurn::Text("done".into())])
}

#[cfg(feature = "native-execution")]
fn checkout_config(
    workspace: impl Into<std::path::PathBuf>,
    model: (Model, Arc<dyn ModelProvider>),
) -> Result<LocalHarnessConfig> {
    // These scripts exercise filesystem commands, not summary-model requests.
    Ok(crate::test_support::local_harness_config(workspace, model)?
        .compaction(crate::context::CompactionPolicy::Disabled))
}

fn capacity() -> ModelContextCapacity {
    ModelContextCapacity {
        context_tokens: 32_768,
        output_tokens: Some(4096),
    }
}

fn model() -> Result<Model> {
    Model::new("scripted".into(), "test".into(), "1".into(), Value::Null)
}

#[test]
fn automatic_compaction_fits_small_models_without_rewriting_explicit_budgets() -> Result<()> {
    let capacity = ModelContextCapacity {
        context_tokens: 32_768,
        output_tokens: Some(4096),
    };
    let explicit = acyclic_harness::context::ThresholdCompaction::default();
    let automatic = acyclic_harness::context::ThresholdCompaction::for_capacity(capacity, 24_000)?;
    assert_eq!(
        Some(automatic.validate(capacity, None)?),
        capacity.output_tokens
    );
    assert!(automatic.recent_tokens < explicit.recent_tokens);
    assert!(
        24_000 + automatic.response_reserve_tokens + 2 * automatic.recent_tokens
            <= capacity.context_tokens
    );
    assert!(explicit.validate(capacity, None).is_err());
    assert!(
        acyclic_harness::context::ThresholdCompaction::for_capacity(
            ModelContextCapacity {
                context_tokens: 2,
                output_tokens: Some(1)
            },
            0
        )
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn incompatible_explicit_compaction_fails_before_owner_creation() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let mut config = volume_config(directory.path(), project, provider.clone())?;
    let owner_path = config.resolved_state_directory()?.join("owner.json");
    config = config.compaction(acyclic_harness::context::CompactionPolicy::default());
    assert!(matches!(
        LocalHarness::open(config).await,
        Err(Error::Invalid(_))
    ));
    assert!(!owner_path.exists());
    assert_eq!(provider.requests().len(), 0);
    Ok(())
}

#[tokio::test]
async fn oversized_tool_catalog_fails_before_owner_creation_or_model_dispatch() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let mut oversized = tool("test.oversized");
    oversized.definition.description = "x".repeat(capacity().context_tokens as usize);
    let config = volume_config(directory.path(), project, provider.clone())?.tool(oversized);
    let owner = config.resolved_state_directory()?.join("owner.json");
    assert!(matches!(
        LocalHarness::open(config).await,
        Err(Error::Invalid(_))
    ));
    assert!(!owner.exists());
    assert!(provider.requests().is_empty());
    Ok(())
}

struct Echo;
impl ToolExecutor for Echo {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Ok(ToolResult {
                value: json!({"text":"echo"}),
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}
fn tool(name: &str) -> Tool {
    Tool {
        hook_names: &[],
        definition: ToolDefinition {
            name: name.into(),
            revision: "1".into(),
            description: "echo".into(),
            input_schema: json!({"type":"object"}),
            output_schema: json!({"type":"object"}),
            projection_schema: acyclic_harness::tool::json_projection_schema(
                json!({"type":"object"}),
            ),
        },
        executor: Arc::new(Echo),
        projection: Arc::new(JsonToolProjection),
    }
}

pub(super) fn volume_config(
    directory: &Path,
    volume: VolumeRef,
    provider: Arc<dyn ModelProvider>,
) -> Result<LocalHarnessConfig> {
    let state = directory
        .join("state")
        .join(volume.storage_name(&mut None)?);
    Ok(crate::test_support::local_harness_config(
        directory.join("filesystem"),
        (model()?, provider),
    )?
    .volume(volume)
    .state_directory(state))
}

pub(super) async fn volume(directory: &Path) -> Result<VolumeRef> {
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let provider = ProviderRef::new(
        "local-test".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let volume = VolumeRef::new(
        provider.clone(),
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("test".into()),
        &mut None,
    )?;
    let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.join("filesystem")))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    FilesystemHost::new(fs, &provider, None)?
        .create_volume(&volume)
        .await?;
    Ok(volume)
}

#[tokio::test]
async fn explicit_state_directory_reopens_the_durable_task_without_a_model_request() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::storage)?;
    let project = volume(directory.path()).await?;
    let state = directory.path().join("state");
    let harness = LocalHarness::open(
        volume_config(directory.path(), project.clone(), text())?.state_directory(&state),
    )
    .await?;
    assert!(state.join("owner.json").is_file());
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
        harness.submit(OperationId::new(), "persist").await?
    else {
        return Err(Error::Conflict("stock admission rejected".into()));
    };
    let output = harness.resume(task_id).await?;
    assert_eq!(output.text, "done");
    drop(harness);
    let provider = scripted([]);
    let reopened = LocalHarness::open(
        volume_config(directory.path(), project, provider.clone())?.state_directory(&state),
    )
    .await?;
    assert_eq!(reopened.resume(task_id).await?, output);
    assert!(provider.requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn defaults_are_additive_and_opt_out_retains_custom_grants() -> Result<()> {
    for defaults in [true, false] {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let project = volume(directory.path()).await?;
        let mut config =
            volume_config(directory.path(), project, text())?.tools(vec![tool("test.custom")]);
        if !defaults {
            config = config.without_default_tools();
        }
        let harness = LocalHarness::open(config).await?;
        assert_eq!(
            harness.tools().get("write", None, None)?.is_some(),
            defaults
        );
        assert!(harness.tools().get("test.custom", None, None)?.is_some());
        let Admission::Accepted(acyclic_harness::runtime::RuntimeTask::Durable { task_id, .. }) =
            harness.submit(OperationId::new(), "inspect grants").await?
        else {
            return Err(Error::Conflict("stock admission rejected".into()));
        };
        let admission = harness
            .runtime
            .task_host()
            .observe_admission(task_id)
            .await?;
        assert_eq!(admission.grants.contains("tool:call:write"), defaults);
        assert!(admission.grants.contains("tool:call:test.custom"));
        assert!(admission.grants.contains("mail:read"));
        assert!(admission.grants.contains("mail:send"));
        assert!(admission.grants.contains("operation:wake"));
        assert!(
            admission.grants.contains(
                &harness
                    .journal_volume()
                    .directory_read_capability("inputs", &mut None)?
            )
        );
        assert!(
            !admission.grants.contains(
                &harness
                    .journal_volume()
                    .directory_read_capability("", &mut None)?
            )
        );
        assert!(
            !admission.grants.contains(
                &harness
                    .journal_volume()
                    .capability(VolumeOperation::Read, &mut None)?
            )
        );
        assert!(
            harness.signed.capabilities().contains(
                &harness
                    .journal_volume()
                    .capability(VolumeOperation::Write, &mut None)?
            )
        );
    }
    Ok(())
}

const MAX_LOCAL_FUTURE_BYTES: usize = 4096;

fn bounded_future<F: std::future::Future>(name: &str, future: F) -> F {
    let bytes = std::mem::size_of_val(&future);
    eprintln!("{name} future: {bytes} bytes");
    assert!(
        bytes <= MAX_LOCAL_FUTURE_BYTES,
        "{name} future is {bytes} bytes; API futures must fit {MAX_LOCAL_FUTURE_BYTES} bytes"
    );
    future
}

#[tokio::test]
async fn reopen_retains_owner_and_resumes_admitted_stock_turn_once() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::storage)?;
    let provider = text();
    #[cfg(feature = "native-execution")]
    let selection = {
        let checkout = directory.path().join("checkout");
        std::fs::create_dir(&checkout).map_err(super::storage)?;
        checkout
    };
    // Reopen must find the same owner: every open shares one injected state directory outside the checkout.
    #[cfg(feature = "native-execution")]
    let state = directory.path().join("state");
    #[cfg(not(feature = "native-execution"))]
    let project = volume(directory.path()).await?;
    let config = || {
        #[cfg(feature = "native-execution")]
        {
            Ok::<_, Error>(
                crate::test_support::local_harness_config(
                    selection.join("."),
                    (model()?, provider.clone()),
                )?
                .state_directory(&state),
            )
        }
        #[cfg(not(feature = "native-execution"))]
        {
            volume_config(directory.path(), project.clone(), provider.clone())
        }
    };
    let harness = bounded_future("open", LocalHarness::open(config()?)).await?;
    #[cfg(feature = "native-execution")]
    assert_checkout(&harness, &state, directory.path()).await?;
    let operation = OperationId::new();
    let Admission::Accepted(acyclic_harness::runtime::RuntimeTask::Durable {
        task_id: task, ..
    }) = bounded_future("submit", harness.submit(operation, "finish this task")).await?
    else {
        return Err(Error::Conflict("stock admission rejected".into()));
    };
    let scope = harness.signed.clone();
    let project = harness.project().clone();
    drop(harness);
    let reopened = bounded_future("reopen", LocalHarness::open(config()?)).await?;
    assert_eq!(&reopened.signed, &scope);
    assert_eq!(reopened.project(), &project);
    assert_eq!(
        bounded_future("recovery", reopened.resume(task))
            .await?
            .text,
        "done"
    );
    assert_eq!(reopened.resume(task).await?.text, "done");
    assert_eq!(provider.requests().len(), 1);
    drop(bounded_future(
        "turn",
        reopened.run("measure without dispatch"),
    ));
    assert_pending_command_returns_control(&reopened).await?;
    assert_eq!(provider.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn reopen_recovers_inflight_model_with_the_original_reservation() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let (started, boundary) = tokio::sync::oneshot::channel();
    let started = Mutex::new(Some(started));
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Events(vec![ScriptedEvent::Pending {}])])
            .with_context_capacity(capacity())
            .with_dispatch_observer(move |_| {
                if let Some(started) = started.lock().unwrap().take() {
                    started.send(()).unwrap();
                }
            })
            .with_reconciliations([ScriptedReconciliation {
                orphan: false,
                observed: vec![],
                continuation: Some(vec![
                    ModelEvent::Content {
                        delta: "recovered".into(),
                    },
                    ModelEvent::Completed {
                        metadata: json!({}),
                    },
                ]),
            }]),
    );
    let config = || {
        Ok::<_, Error>(volume_config(
            directory.path(),
            project.clone(),
            provider.clone(),
        )?)
    };
    let harness = LocalHarness::open(config()?).await?;
    let Admission::Accepted(acyclic_harness::runtime::RuntimeTask::Durable {
        task_id: task, ..
    }) = harness
        .submit(OperationId::new(), "recover this request")
        .await?
    else {
        return Err(Error::Conflict("stock admission rejected".into()));
    };
    match futures::future::select(
        bounded_future("inflight recovery", harness.resume(task)),
        boundary,
    )
    .await
    {
        futures::future::Either::Right((Ok(()), execution)) => drop(execution),
        _ => {
            return Err(Error::Conflict(
                "execution did not reach the interruption boundary".into(),
            ));
        }
    }
    let acyclic_harness::distributed::WorkPull::Claimed(original) = harness
        .runtime
        .task_host()
        .recover_work(task, &mut None)
        .await?
    else {
        return Err(Error::Conflict(
            "interrupted task lost its reservation".into(),
        ));
    };
    drop(harness);
    let reopened = LocalHarness::open(config()?).await?;
    let acyclic_harness::distributed::WorkPull::Claimed(retained) = reopened
        .runtime
        .task_host()
        .recover_work(task, &mut None)
        .await?
    else {
        return Err(Error::Conflict("reopened task lost its reservation".into()));
    };
    assert_eq!(retained.reservation.id, original.reservation.id);
    assert_eq!(reopened.resume(task).await?.text, "recovered");
    assert_eq!(reopened.resume(task).await?.text, "recovered");
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(provider.reconciliations().len(), 1);
    Ok(())
}

#[tokio::test]
async fn duplicate_default_and_custom_contracts_are_rejected() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let config = volume_config(directory.path(), project, text())?.tool(tool("write"));
    assert!(LocalHarness::open(config).await.is_err());
    Ok(())
}

#[tokio::test]
async fn stock_executor_calls_default_and_custom_tools() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let provider = scripted([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                "default".into(),
                "write".into(),
                json!({"path":"default.txt","text":"default output","media_type":"text/plain","display_name":"default"}),
            ))),
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                "custom".into(),
                "test.custom".into(),
                json!({}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ]),
        ScriptedTurn::Text("both tools ran".into()),
    ]);
    let harness = LocalHarness::open(
        volume_config(directory.path(), project, provider.clone())?
            .tools(vec![tool("test.custom")]),
    )
    .await?;
    assert_eq!(harness.run("call both tools").await?.text, "both tools ran");
    assert_eq!(provider.requests().len(), 2);
    Ok(())
}

#[tokio::test]
async fn corrupt_owner_is_rejected_without_reissuing_credentials() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let config = || Ok::<_, Error>(volume_config(directory.path(), project.clone(), text())?);
    let path = config()?.resolved_state_directory()?.join("owner.json");
    drop(LocalHarness::open(config()?).await?);
    assert!(
        path.is_file(),
        "the corruption probe must address the persisted owner"
    );
    std::fs::write(&path, b"invalid owner").map_err(|error| Error::Storage(error.to_string()))?;
    assert!(matches!(
        LocalHarness::open(config()?).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        std::fs::read(&path).map_err(|error| Error::Storage(error.to_string()))?,
        b"invalid owner"
    );
    Ok(())
}

async fn output_runtime(
    harness: &LocalHarness,
    grants: acyclic_harness::Capabilities,
) -> Result<acyclic_harness::local::LocalTaskRuntime> {
    let limits = acyclic_harness::conversation::Limits::default();
    acyclic_harness::local::LocalTaskRuntime::open(
        harness.runtime.task_host().workflow_stream(),
        harness.filesystem().clone(),
        harness.journal_volume(),
        Some(harness.scratch()),
        &harness.verifier,
        &harness.signed,
        &acyclic_harness::runtime::RuntimeScope::new(grants, limits)?,
        &acyclic_harness::runtime::TaskRegistry::default(),
        &acyclic_harness::workflow::MachineRegistry::default(),
        &acyclic_harness::tool::ToolRegistry::default(),
        acyclic_harness::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 1,
            model_steps: 1,
        },
        1,
        limits.file_bytes,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        None,
        &[],
        &acyclic_harness::durable_tool::ResumableToolRegistry::default(),
        None,
    )
    .await
}

#[tokio::test]
async fn root_output_requires_task_grants_and_exclusive_configuration() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let harness = LocalHarness::open(volume_config(directory.path(), project, text())?).await?;
    // Summaries publish to the private volume, so a runtime cannot open without it.
    assert!(matches!(
        output_runtime(&harness, acyclic_harness::Capabilities::default()).await,
        Err(Error::Unauthorized(_))
    ));
    let grants = acyclic_harness::Capabilities::new([
        harness
            .scratch()
            .capability(VolumeOperation::Read, &mut None)?,
        harness
            .scratch()
            .capability(VolumeOperation::Write, &mut None)?,
    ]);
    let denied = output_runtime(&harness, grants.clone()).await?;
    assert!(matches!(
        denied.with_output_volume(harness.journal_volume(), None),
        Err(Error::Unauthorized(_))
    ));
    let configured = output_runtime(&harness, grants)
        .await?
        .with_output_volume(harness.scratch(), None)?;
    let retained = configured.harness().clone();
    assert!(matches!(
        configured.with_output_volume(harness.scratch(), None),
        Err(Error::Conflict(_))
    ));
    drop(retained);
    Ok(())
}

#[cfg(feature = "native-execution")]
async fn assert_checkout(harness: &LocalHarness, state: &Path, other: &Path) -> Result<()> {
    let checkout = harness
        .workspace_path()
        .ok_or_else(|| Error::NotFound("checkout".into()))?;
    let state = std::fs::canonicalize(state).map_err(super::storage)?;
    assert!(!state.starts_with(checkout));
    assert!(!checkout.join(".acyclic").exists());
    assert!(!checkout.join("owner.json").exists());
    // Default workspace tools bind their declared scratch volume, but its
    // native projection waits for bash. The namespace parent is the only
    // checkout entry; authority and owner state still live outside it.
    assert!(checkout.join(".volumes").is_dir());
    assert!(
        !checkout.join(".volumes/scratch").exists(),
        "scratch was projected before bash needed it"
    );
    assert_eq!(
        std::fs::read_dir(checkout)
            .map_err(super::storage)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<std::collections::BTreeSet<_>>>()
            .map_err(super::storage)?,
        std::collections::BTreeSet::from([std::ffi::OsString::from(".volumes")])
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            std::fs::metadata(state.join("owner.json"))
                .map_err(super::storage)?
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(&state).map_err(super::storage)?.mode() & 0o777,
            0o700
        );
    }
    // The view must read a source file created after open without a checkout scan.
    std::fs::write(checkout.join("hello.txt"), "original checkout").map_err(super::storage)?;
    let workspace = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("checkout view".into()))?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
        work: &work,
        cancellation: &cancellation,
    };
    let bytes = workspace
        .view()
        .read("/hello.txt", 1024, &mut accounting)
        .await
        .map_err(super::storage)?;
    assert_eq!(bytes.bytes.as_ref(), b"original checkout");
    let captured = super::owner::Checkout::capture(other)?;
    let other = crate::test_support::local_harness_config(other, (model()?, text()))?;
    assert!(matches!(
        super::owner::Owner::open(&state, &other.project, Some(&captured)),
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}

#[cfg(feature = "native-execution")]
#[test]
fn checkout_attach_has_a_stable_distinct_operation_identity() {
    let task = crate::TaskId::from_bytes([7; 16]);
    let operation = super::workspace::attach_operation(task);
    assert_eq!(operation, super::workspace::attach_operation(task));
    assert_ne!(operation.into_bytes(), task.into_bytes());
    assert_ne!(
        operation,
        super::workspace::attach_operation(crate::TaskId::from_bytes([8; 16]))
    );
}

struct PendingCommand(std::sync::atomic::AtomicUsize);

impl crate::filesystem::TaskCommandHost for PendingCommand {
    fn execute<'a>(
        &'a self,
        _: crate::runtime::TaskContext,
        _: crate::scheduler::LeaseFence,
        command: crate::workflow::WorkflowCommand,
        _: Value,
    ) -> BoxProviderFuture<'a, Result<crate::filesystem::TaskCommandProgress>> {
        Box::pin(async move {
            use crate::filesystem::TaskCommandProgress;
            assert_eq!(
                command.kind,
                crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND
            );
            let calls = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert!(calls < 2, "pending command was retried without progress");
            Ok(TaskCommandProgress::Pending)
        })
    }

    fn reconcile<'a>(
        &'a self,
        context: crate::runtime::TaskContext,
        fence: crate::scheduler::LeaseFence,
        command: crate::workflow::WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<crate::filesystem::TaskCommandProgress>> {
        self.execute(context, fence, command, payload)
    }
}

async fn assert_pending_command_returns_control(harness: &LocalHarness) -> Result<()> {
    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id: task, .. }) =
        harness.submit(operation, "wait for external work").await?
    else {
        return Err(Error::Conflict("stock admission rejected".into()));
    };
    let commands = PendingCommand(std::sync::atomic::AtomicUsize::new(0));
    Box::pin(harness.advance(task, &commands)).await?;
    assert_eq!(commands.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(harness.runtime.task_host().outcome(task).await?.is_none());
    let suspension = harness
        .runtime
        .task_host()
        .workflow_suspension(task, &mut None)
        .await?
        .ok_or_else(|| Error::NotFound("pending command suspension".into()))?;
    let admission = harness.runtime.task_host().observe_admission(task).await?;
    let receive = serde_json::from_value::<crate::workflow::WorkflowCommand>(
        admission.input["receive"].clone(),
    )
    .map_err(super::storage)?;
    assert_eq!(suspension.waiting_command, Some(receive.operation_id));
    assert!(!harness.runtime.poll_task_wake(task, None).await?);
    let worker = crate::distributed::Worker {
        id: "pending-test".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    assert!(
        bounded_future(
            "worker operation",
            harness
                .runtime
                .run_operation(&worker, task, &commands, 1, None),
        )
        .await?
        .is_none()
    );
    assert_eq!(commands.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    Ok(())
}

#[path = "tests/interrupted_turns.rs"]
mod interrupted_turns;
#[path = "tests/retained_failure.rs"]
mod retained_failure;

#[cfg(feature = "native-execution")]
mod checked_commands;
#[cfg(all(feature = "native-execution", unix))]
mod checkout;
#[cfg(all(feature = "native-execution", windows))]
mod powershell;

#[cfg(all(feature = "native-execution", unix))]
mod lazy_volumes;
#[cfg(all(feature = "native-execution", unix))]
mod volumes;

#[cfg(feature = "native-execution")]
#[path = "tests/checkout_reopen.rs"]
mod checkout_reopen;

#[cfg(feature = "native-execution")]
#[path = "tests/checkout_incarnation.rs"]
mod checkout_incarnation;

#[cfg(all(feature = "native-execution", unix))]
mod checkout_reconciliation;
mod git_view;

#[cfg(feature = "native-execution")]
mod merge_recovery;

#[cfg(all(feature = "native-execution", unix))]
mod volume_tools;

mod children;
#[cfg(all(feature = "native-execution", unix))]
mod reconciliation_interruptions;
mod scale;

#[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
mod mcp;

#[path = "tests/command_files.rs"]
mod command_files;

#[cfg(all(feature = "tui", feature = "terminal"))]
#[path = "tests/documents.rs"]
mod documents;

#[path = "tests/headless.rs"]
mod headless;

#[path = "tests/task_files.rs"]
mod task_files;

#[cfg(all(feature = "tui", feature = "terminal"))]
#[path = "tests/live_ui.rs"]
mod live_ui;

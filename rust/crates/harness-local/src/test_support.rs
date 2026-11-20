//! Deterministic model scripts for harness consumers and scenario tests.

pub use acyclic_harness_core::test_support::*;

#[cfg(not(target_arch = "wasm32"))]
pub use crate::local::host_process::children::answer::validation as native_answer;

pub use acyclic_harness_search::test_support::{ScriptedSearch, ScriptedSearchProvider};

/// Runs a test world seeded, so every run draws the same identities and a
/// failure prints the same text each time.
#[cfg(not(target_arch = "wasm32"))]
pub fn seeded<F: std::future::Future<Output = crate::Result<()>>>(
    world: impl FnOnce() -> F,
) -> crate::Result<()> {
    acyclic_native_runtime::testing::seeded_test(0x4841_524e, world)
        .map_err(|error| crate::Error::Storage(error.to_string()))?
}

/// The scripted model every local fixture opens with.
#[cfg(not(target_arch = "wasm32"))]
pub fn model() -> crate::Result<crate::model::Model> {
    crate::model::Model::new("scripted".into(), "test".into(), "1".into())
}

/// A project volume backed by a real local filesystem below `directory`.
#[cfg(not(target_arch = "wasm32"))]
pub async fn volume(directory: &std::path::Path) -> crate::Result<crate::conversation::VolumeRef> {
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(|error| crate::Error::Storage(error.to_string()))?;
    let provider = crate::resources::ProviderRef::new(
        "local-test".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let volume = crate::conversation::VolumeRef::new(
        provider.clone(),
        "project".into(),
        crate::conversation::VolumeClass::Project,
        crate::conversation::VolumeOwner::Project("test".into()),
        &mut None,
    )?;
    let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.join("filesystem")))
        .await
        .map_err(|error| crate::Error::Storage(error.to_string()))?;
    crate::filesystem::FilesystemHost::new(fs, &provider, None)?
        .create_volume(&volume)
        .await?;
    Ok(volume)
}

/// A harness config over `volume`, with private file signing and its own state.
#[cfg(not(target_arch = "wasm32"))]
pub fn volume_config(
    directory: &std::path::Path,
    volume: crate::conversation::VolumeRef,
    provider: std::sync::Arc<dyn crate::model::ModelProvider>,
) -> crate::Result<crate::LocalHarnessConfig> {
    let state = directory
        .join("state")
        .join(volume.storage_name(&mut None)?);
    Ok(
        local_harness_config(directory.join("filesystem"), (model()?, provider))?
            .volume(volume)
            .private_file_signing()
            .state_directory(state),
    )
}

/// The tree's one execution target: this host's checkout, run by `tools`
/// and guarded by its one human.
#[cfg(not(target_arch = "wasm32"))]
pub use crate::local::home;

/// Writes fixture bytes through the native host rather than the provider
/// process, so a projection's own callbacks observe the change.
#[cfg(not(target_arch = "wasm32"))]
pub fn native_write(path: &std::path::Path, bytes: &[u8]) -> crate::Result<()> {
    #[cfg(not(windows))]
    {
        std::fs::write(path, bytes).map_err(crate::local::storage)
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt as _;
        // ProjFS deliberately excludes provider-process writes from callbacks.
        // Exercise an actual native host process instead of that bypass.
        let staging = tempfile::tempdir().map_err(crate::local::storage)?;
        let input = staging.path().join("authored.bin");
        std::fs::write(&input, bytes).map_err(crate::local::storage)?;
        let output = std::process::Command::new("cmd.exe")
            .args(["/d", "/v:off", "/c"])
            .raw_arg(r#"copy /b /y "%ACYCLIC_COPY_SOURCE%" "%ACYCLIC_COPY_DESTINATION%""#)
            .env("ACYCLIC_COPY_SOURCE", input)
            .env("ACYCLIC_COPY_DESTINATION", path)
            .creation_flags(0x0800_0000)
            .output()
            .map_err(crate::local::storage)?;
        if !output.status.success() {
            return Err(crate::Error::Conflict(
                "native fixture write was denied".into(),
            ));
        }
        Ok(())
    }
}

/// Replaces the next checkout capture's root identity with a fixture. The
/// world owns this thread-local input and sets it before opening its harness.
#[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
pub fn inject_checkout_identity(
    object: [u8; 16],
    generation: acyclic_fs_native_contract::NativeRootGeneration,
) {
    crate::local::owner::INJECTED.with(|injected| injected.set(Some((object, generation))));
}

/// Clears the fixture root identity after a world retires.
#[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
pub fn clear_checkout_identity() {
    crate::local::owner::INJECTED.with(|injected| injected.set(None));
}

/// Runs the production local state-directory separation and preparation boundary.
#[cfg(not(target_arch = "wasm32"))]
pub fn separated_local_state_directory(
    path: &std::path::Path,
    checkout: Option<&std::path::Path>,
) -> crate::Result<std::path::PathBuf> {
    crate::local::owner::separated_directory(path, checkout)
}

/// Checks arbitrary sequential schedules against the native child ready queue.
/// This exercises the production registrations, queue and waiter callbacks;
/// physical event-hub concurrency remains a separate contract.
#[cfg(not(target_arch = "wasm32"))]
pub fn check_native_readiness(input: &[u8]) {
    crate::local::host_process::children::readiness_validation::verify(input, 16);
}

/// Checks the native mount-reference parser used before committing a spawn.
/// Arbitrary bytes cover refusal; generated portable components also require
/// admission, so rejecting every input cannot satisfy the safety oracle.
#[cfg(not(target_arch = "wasm32"))]
pub fn check_native_child_reference(input: &[u8]) {
    let result = check_child_reference(input);
    assert!(
        result.is_ok(),
        "native child reference check failed: {result:?}"
    );
}

#[cfg(not(target_arch = "wasm32"))]
fn check_child_reference(input: &[u8]) -> crate::Result<()> {
    use crate::agent::{AgentId, AgentPath};

    crate::agent::codec_fuzz::check(input);

    let mut identity = [0; 16];
    for (destination, source) in identity.iter_mut().zip(input) {
        *destination = *source;
    }
    let parent = AgentPath::root(&AgentId::from_bytes(identity));
    if let Ok(reference) = std::str::from_utf8(input) {
        check_arbitrary_child_reference(&parent, reference)?;
    }
    check_generated_child_reference(&parent, input)
}

#[cfg(not(target_arch = "wasm32"))]
fn check_arbitrary_child_reference(
    parent: &crate::agent::AgentPath,
    reference: &str,
) -> crate::Result<()> {
    use crate::local::host_owner::child;
    use crate::local::host_workspace::child_reference;
    use std::path::{Component, Path};

    for operation in ["diff", "merge", "discard"] {
        let mut arguments = [operation.into(), reference.into()];
        match crate::tools::resolve_child_branch(parent, &mut arguments, child) {
            Ok(Some(admitted)) => {
                assert!(admitted.is_child_of(parent));
                assert_eq!(arguments[1], admitted.as_str());
            }
            Ok(None) | Err(crate::Error::Invalid(_)) => {}
            Err(error) => {
                return Err(crate::Error::Invalid(format!(
                    "child branch lost its invalid-input type: {error}"
                )));
            }
        }
    }
    if reference.contains('/') {
        match child(parent, reference) {
            Ok(admitted) => {
                assert_eq!(admitted.as_str(), reference);
                assert!(admitted.is_child_of(parent));
            }
            Err(crate::Error::Invalid(_)) => {}
            Err(error) => {
                return Err(crate::Error::Invalid(format!(
                    "child path lost its invalid-input type: {error}"
                )));
            }
        }
    }
    match child_reference(reference) {
        Ok(()) => {
            let mut components = Path::new(reference).components();
            assert!(
                matches!(components.next(), Some(Component::Normal(name)) if name == reference)
                    && components.next().is_none(),
                "admitted reference escaped its single child component"
            );
            assert!(
                reference.len() <= 255
                    && reference
                        .chars()
                        .all(|character| character.is_ascii_alphanumeric()
                            || matches!(character, '-' | '_' | '.')),
                "admitted reference cannot be represented by the portable mount alphabet"
            );
        }
        Err(crate::Error::Invalid(message)) => assert!(
            message.len() <= 64,
            "child-reference refusal retained unbounded input: {} bytes",
            message.len()
        ),
        Err(error) => {
            return Err(crate::Error::Invalid(format!(
                "child-reference refusal lost its invalid-input type: {error}"
            )));
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn check_generated_child_reference(
    parent: &crate::agent::AgentPath,
    input: &[u8],
) -> crate::Result<()> {
    use crate::local::host_owner::child;
    use crate::local::host_workspace::child_reference;

    // This is an input generator, not a second parser. Its bounded output is
    // one filesystem component; the original arbitrary input is never cut.
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.";
    let mut portable = String::from("a");
    portable.extend(
        input
            .iter()
            .take(254)
            .map(|byte| char::from(ALPHABET[usize::from(*byte) % ALPHABET.len()])),
    );
    assert!(
        child_reference(&portable).is_ok(),
        "generated portable child component was refused"
    );
    let admitted = child(parent, &portable)?;
    assert!(admitted.is_child_of(parent));
    assert_eq!(
        child(parent, admitted.as_str())?,
        admitted,
        "local and canonical references must name the same child"
    );
    let grandchild = child(&admitted, &portable)?;
    for operation in ["diff", "merge", "discard"] {
        for reference in [format!("agents/{portable}"), admitted.to_string()] {
            let mut arguments = [operation.into(), reference];
            assert_eq!(
                crate::tools::resolve_child_branch(parent, &mut arguments, child)?,
                Some(admitted.clone()),
            );
            assert_eq!(arguments[1], admitted.as_str());
        }
        for unrelated in [parent, &grandchild] {
            let mut arguments = [operation.into(), unrelated.to_string()];
            assert!(
                matches!(
                    crate::tools::resolve_child_branch(parent, &mut arguments, child),
                    Err(crate::Error::Invalid(_))
                ),
                "a canonical branch bypassed direct-child ownership"
            );
        }
    }
    // Every show alias preserves the path and paging operands while resolving
    // to exactly the same owned child, including under a nested parent.
    // Spawn identities also contain their prefix and the generated suffix.
    let spawn_name = &portable[..portable
        .len()
        .min(crate::agent::MAX_INTENT_BYTES - "spawn:".len() - "/worker".len())];
    for owner in [parent, &admitted] {
        for name in [spawn_name.to_owned(), format!("{spawn_name}/worker")] {
            let expected = crate::tools::spawned_child(owner, &name)?;
            for reference in [name.clone(), format!("agents/{name}"), expected.to_string()] {
                for suffix in [vec![], vec!["--offset".into(), "65536".into()]] {
                    let mut arguments =
                        vec!["show".into(), format!("{reference}:reports/result.md")];
                    arguments.extend(suffix.clone());
                    assert_eq!(
                        crate::tools::resolve_child_branch(
                            owner,
                            &mut arguments,
                            crate::tools::spawned_child
                        )?,
                        Some(expected.clone()),
                    );
                    assert_eq!(arguments[1], format!("{expected}:reports/result.md"));
                    assert_eq!(&arguments[2..], suffix.as_slice());
                }
            }
        }
    }
    let mut head = ["show".into(), "HEAD:reports/result.md".into()];
    assert_eq!(
        crate::tools::resolve_child_branch(parent, &mut head, child)?,
        None
    );
    for unrelated in [parent, &grandchild] {
        assert!(
            matches!(
                child(parent, unrelated.as_str()),
                Err(crate::Error::Invalid(_))
            ),
            "a canonical non-child must be rejected before any storage access"
        );
    }
    Ok(())
}

/// The one fixture catalog: `binding` serves its own selection and every
/// model a shipped role selects. Role policy wins at admission, so a fixture
/// with one provider supplies the role models through this resolver, and each
/// request keeps the binding's own model identity, which its provider serves.
///
/// # Errors
/// When a shipped role template does not parse.
pub fn role_models(
    binding: crate::model::ModelBinding,
) -> crate::Result<crate::model::ModelResolver> {
    let own = format!("{}/{}", binding.0.provider, binding.0.name);
    let roles = crate::templates::role_models()?;
    Ok(crate::model::ModelResolver::new(
        own.clone(),
        move |selection: &str| {
            if selection == own || roles.contains(selection) {
                Ok(binding.clone())
            } else {
                Err(crate::Error::NotFound(format!("fixture model {selection}")))
            }
        },
    ))
}

/// Local test composition with temporary state and settings, independent of the per-user store, HOME and CODEX_HOME.
#[cfg(not(target_arch = "wasm32"))]
pub fn local_harness_config(
    workspace: impl Into<std::path::PathBuf>,
    model: (
        crate::model::Model,
        std::sync::Arc<dyn crate::model::ModelProvider>,
    ),
) -> crate::Result<crate::LocalHarnessConfig> {
    let workspace = workspace.into();
    #[cfg(any(test, not(target_arch = "wasm32")))]
    let settings = crate::native_settings::SettingsConfig::for_test(&workspace)?;
    // Store opening supplies temporary state only when no explicit root was selected.
    let config = crate::LocalHarnessConfig::new(workspace, model)
        .hermetic()
        .approval_policy(crate::preset::Preset::graphcoder().approval_policy);
    #[cfg(any(test, not(target_arch = "wasm32")))]
    let config = {
        let mut config = config;
        config.environment_platform = crate::native_environment::PlatformPolicy::Fixture {
            os: std::env::consts::OS.into(),
            shell: crate::native_environment::PlatformPolicy::Native
                .facts()
                .1
                .into(),
        };
        config
    };
    #[cfg(any(test, not(target_arch = "wasm32")))]
    let config = config.settings(settings);
    // A simulation starts no native mount, so its worlds use physical views.
    #[cfg(not(target_arch = "wasm32"))]
    let config = if acyclic_native_runtime::testing::Schedule::current().is_some() {
        config.workspace_mounting(crate::local::WorkspaceMounting::Physical)
    } else {
        config
    };
    Ok(config)
}

/// How one root-agent prompt settled.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub enum PromptOutcome {
    /// The turn settled; its conversation entry and decoded output.
    Settled(crate::agent::ConversationEntry, crate::executor::TurnOutput),
    /// The turn failed (the agent stays open), or a host failure ended the
    /// root agent.
    Failed(String),
}

/// Prompts `harness`'s root agent as message `id`, then follows its conversation
/// and phase until that message's turn settles or fails. The one prompt path of
/// every local scenario, DST replay and provider test.
#[cfg(not(target_arch = "wasm32"))]
pub async fn prompt_settled(
    harness: &std::sync::Arc<crate::LocalHarness>,
    id: &str,
    prompt: &str,
) -> crate::Result<PromptOutcome> {
    use crate::agent::{AgentOutcome, Phase, Reply, TurnOutcome};
    use crate::local::agent_conversation::Settlement;
    let conversation =
        crate::local::agent_conversation::Conversation::open(harness.connect().await?);
    let mut events = conversation.observe(None).await?;
    let inbox = conversation.send(id, prompt).await?;
    let (sequence, entry) = match conversation.wait_reply(id, inbox, &mut events).await? {
        Settlement::Reply(Reply::Answered {
            sequence, entry, ..
        }) => (sequence, entry),
        Settlement::Reply(Reply::Dropped) | Settlement::Ended(_) => {
            match conversation.phase().await? {
                Phase::Done {
                    outcome: AgentOutcome::Failed { message },
                } => return Ok(PromptOutcome::Failed(message)),
                Phase::Done {
                    outcome: AgentOutcome::FailedDiagnostic { diagnostic },
                } => {
                    return Ok(PromptOutcome::Failed(format!(
                        "diagnostic: {} ({} bytes)",
                        diagnostic.path()?,
                        diagnostic.bytes
                    )));
                }
                Phase::Done { outcome } => {
                    return Err(crate::Error::Conflict(format!(
                        "root agent ended before its prompt settled: {outcome:?}"
                    )));
                }
                _ => return Err(crate::Error::Conflict(format!("prompt {id} was cancelled"))),
            }
        }
        Settlement::Reply(_) | Settlement::Approval { .. } => {
            return Err(crate::Error::Conflict("unsettled prompt reply".into()));
        }
    };
    if let TurnOutcome::Failed { message } = &entry.outcome {
        return Ok(PromptOutcome::Failed(message.clone()));
    }
    let output = conversation.output_at(sequence, &entry).await?;
    Ok(PromptOutcome::Settled(entry, output))
}

/// Drives the accepted native boundaries while running a scoped regression.
#[cfg(not(target_arch = "wasm32"))]
pub async fn run_native_schedule<F: std::future::Future>(
    schedule: &acyclic_native_runtime::testing::Schedule,
    mut boundaries: tokio::sync::mpsc::UnboundedReceiver<acyclic_native_runtime::testing::Boundary>,
    work: F,
) -> crate::Result<F::Output> {
    let work = schedule.scope(work);
    tokio::pin!(work);
    loop {
        tokio::select! {
            result = &mut work => return Ok(result),
            boundary = boundaries.recv() => {
                let boundary = boundary.ok_or_else(|| {
                    crate::Error::Storage("native boundary driver closed".into())
                })?;
                boundary.release();
            }
        }
    }
}

//! Deterministic model scripts for harness consumers and scenario tests.

pub use acyclic_harness_core::test_support::*;
pub use acyclic_harness_search::test_support::{ScriptedSearch, ScriptedSearchProvider};

/// Checks arbitrary sequential schedules against the native child ready queue.
/// This exercises the production registrations, queue and waiter callbacks;
/// physical event-hub concurrency remains a separate contract.
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub fn check_native_readiness(input: &[u8]) {
    crate::local::host_process::children::readiness_validation::verify(input, 16);
}

/// Checks the native mount-reference parser used before committing a spawn.
/// Arbitrary bytes cover refusal; generated portable components also require
/// admission, so rejecting every input cannot satisfy the safety oracle.
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub fn check_native_child_reference(input: &[u8]) {
    use crate::local::host_workspace::child_reference;
    use std::path::{Component, Path};

    if let Ok(reference) = std::str::from_utf8(input) {
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
            Err(error) => panic!("child-reference refusal lost its invalid-input type: {error}"),
        }
    }

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
}

/// Local test composition with temporary state and settings, independent of the per-user store, HOME and CODEX_HOME.
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub fn local_harness_config(
    workspace: impl Into<std::path::PathBuf>,
    model: (
        crate::model::Model,
        std::sync::Arc<dyn crate::model::ModelProvider>,
    ),
) -> crate::Result<crate::LocalHarnessConfig> {
    let workspace = workspace.into();
    #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    let settings = crate::native_settings::SettingsConfig::for_test(&workspace)?;
    // Store opening supplies temporary state only when no explicit root was selected.
    let policy = crate::approvals::ApprovalPolicy::accept_all("graphcoder-default");
    let config = crate::LocalHarnessConfig::new(workspace, model)
        .hermetic()
        .approval_policy(policy);
    #[cfg(any(test, feature = "native-execution"))]
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
    #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    let config = config.settings(settings);
    Ok(config)
}

/// How one root-agent prompt settled.
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
#[derive(Debug)]
pub enum PromptOutcome {
    /// The turn settled; its conversation entry and decoded output.
    Settled(crate::agent::ConversationEntry, crate::executor::TurnOutput),
    /// The turn failed, which ends the root agent.
    Failed(String),
}

/// Prompts `harness`'s root agent as message `id`, then follows its conversation
/// and phase until that message's turn settles or fails. The one prompt path of
/// every local scenario, DST replay and provider test.
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub async fn prompt_settled(
    harness: &std::sync::Arc<crate::LocalHarness>,
    id: &str,
    prompt: &str,
) -> crate::Result<PromptOutcome> {
    use crate::agent::{AgentOutcome, Phase, Reply};
    use crate::local::Event;
    let conversation =
        crate::local::agent_conversation::Conversation::open(harness.connect().await?)?;
    let mut events = conversation.observe(conversation.settled().await?).await?;
    let inbox = conversation.send(id, prompt).await?;
    let mut phase = None;
    let entry = loop {
        let dropped = match conversation.reply(id, inbox).await? {
            Reply::Answered { entry, .. } => break entry,
            Reply::Dropped => true,
            Reply::Pending | Reply::Continuing { .. } => false,
        };
        if dropped {
            // A failure may already have committed before the reply query.
            phase = Some(conversation.phase().await?);
        }
        match phase.take() {
            Some(Phase::Done {
                outcome: AgentOutcome::Failed { message },
            }) => return Ok(PromptOutcome::Failed(message)),
            Some(Phase::Done { outcome }) => {
                return Err(crate::Error::Conflict(format!(
                    "root agent ended before its prompt settled: {outcome:?}"
                )));
            }
            _ => {}
        }
        if dropped {
            return Err(crate::Error::Conflict(format!("prompt {id} was cancelled")));
        }
        loop {
            match events.next().await? {
                Event::Status(status) => {
                    // The original reply wins over a later terminal event.
                    phase = Some(status.phase);
                    break;
                }
                Event::Turn(_) => break,
                Event::Output => {}
            }
        }
    };
    let bytes = harness
        .client_reader()?
        .read(&entry.output, 1 << 20, None, None)?
        .await?;
    let output =
        serde_json::from_slice(&bytes).map_err(|error| crate::Error::Invalid(error.to_string()))?;
    Ok(PromptOutcome::Settled(entry, output))
}

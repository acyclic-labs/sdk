//! Deterministic model scripts for harness consumers and scenario tests.

pub use acyclic_harness_core::test_support::*;
pub use acyclic_harness_search::test_support::{ScriptedSearch, ScriptedSearchProvider};

mod client_views;
pub use client_views::empty_client_views;
#[cfg(feature = "test-support")]
pub(crate) mod model_view_work;
#[cfg(feature = "test-support")]
pub use model_view_work::measure as measure_model_view_work;

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
    let policy = serde_json::from_slice(include_bytes!("../bin/graphcoder/approval-policy.json"))
        .map_err(|error| crate::Error::Invalid(error.to_string()))?;
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

/// Prompts `harness`'s root agent as message `id`, then follows its status
/// stream until that message's turn settles or fails. The one prompt path of
/// every local scenario, DST replay and provider test.
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub async fn prompt_settled(
    harness: &std::sync::Arc<crate::LocalHarness>,
    id: &str,
    prompt: &str,
) -> crate::Result<PromptOutcome> {
    use crate::agent::{
        AgentOutcome, AgentRecord as _, ConversationEntry, Delivered, IntentId, Phase,
        StatusRecord, TurnInput,
    };
    use acyclic_stream::{FollowFilter, ReadRequest, StreamProvider as _};
    use futures::TryStreamExt as _;
    let agent = |error: crate::agent::Error| crate::Error::Storage(error.to_string());
    let stream = |error: acyclic_stream::StreamError| crate::Error::Storage(error.to_string());
    let inbox = match harness
        .prompt(&IntentId::try_from(id).map_err(agent)?, prompt)
        .await
        .map_err(agent)?
    {
        Delivered::Committed { inbox } => inbox,
        Delivered::Closed => return Err(crate::Error::Conflict("root agent is closed".into())),
    };
    let (agents, root) = harness.agents().await?;
    let provider = agents.provider();
    let mut statuses = provider
        .follow(root.status().map_err(agent)?, 0, FollowFilter::default())
        .await
        .map_err(stream)?;
    loop {
        let record = statuses
            .try_next()
            .await
            .map_err(stream)?
            .ok_or_else(|| crate::Error::Storage("agent status follow ended".into()))?;
        let status = StatusRecord::decode(&record.value).map_err(agent)?;
        match status.phase {
            Phase::Done {
                outcome: AgentOutcome::Failed { message },
            } => return Ok(PromptOutcome::Failed(message)),
            Phase::Done { outcome } => {
                return Err(crate::Error::Conflict(format!(
                    "root agent ended before its prompt settled: {outcome:?}"
                )));
            }
            Phase::Idle if status.cursor > inbox => break,
            _ => {}
        }
    }
    let path = root.conversation().map_err(agent)?;
    // The prompt's turn is among the last entries: later input is still queued.
    const RECENT: u32 = 16;
    let tail = provider.tail(path.clone()).await.map_err(stream)?;
    let entries: Vec<_> = provider
        .read(ReadRequest {
            path,
            from: tail.saturating_sub(u64::from(RECENT)),
            limit: RECENT,
        })
        .await
        .map_err(stream)?
        .try_collect()
        .await
        .map_err(stream)?;
    for record in entries.iter().rev() {
        let entry = ConversationEntry::decode(&record.value).map_err(agent)?;
        if matches!(entry.input, TurnInput::Message { inbox: settled, .. } if settled == inbox) {
            let bytes = harness
                .client_reader()?
                .read(&entry.output, 1 << 20, None, None)?
                .await?;
            let output = serde_json::from_slice(&bytes)
                .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            return Ok(PromptOutcome::Settled(entry, output));
        }
    }
    Err(crate::Error::NotFound(format!("settled turn of prompt {id}")))
}

/// The composition's actual task provider, for scenario wire-frame assertions.
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub fn local_task_stream(
    harness: &crate::LocalHarness,
) -> std::sync::Arc<acyclic_stream::LocalStream> {
    harness.task_stream()
}

#[cfg(all(
    test,
    feature = "filesystem-local",
    any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http")
))]
mod tests {
    // The local test composition must select the isolated settings snapshot,
    // never the user's settings (see `native_settings` isolation properties).
    #[test]
    fn local_test_config_selects_isolated_settings() {
        let workspace = std::path::PathBuf::from("workspace");
        let config = super::local_harness_config(
            workspace,
            (
                crate::model::Model::new(
                    "test".into(),
                    "settings".into(),
                    "1".into(),
                    serde_json::Value::Null,
                )
                .unwrap(),
                std::sync::Arc::new(super::ScriptedModelProvider::new([])),
            ),
        )
        .unwrap();
        assert!(
            config.settings.is_some(),
            "test configuration must select settings"
        );
        #[cfg(any(test, feature = "native-execution"))]
        assert!(matches!(
            config.environment_platform,
            crate::native_environment::PlatformPolicy::Fixture { shell, .. }
                if shell == crate::native_environment::PlatformPolicy::Native.facts().1
        ));
    }
}

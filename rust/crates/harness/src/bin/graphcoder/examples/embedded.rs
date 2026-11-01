//! A local embedding of the shared GraphCoder preset, without the GraphCoder frontend.
use acyclic_harness::{
    Error, Result,
    agent::{Phase, Reply},
    local::{Event, LocalHarness, LocalHarnessConfig, agent_conversation::Conversation},
};
use std::sync::Arc;

/// Runs one prompt through the original served conversation and its event stream.
pub async fn answer(config: LocalHarnessConfig, operation: &str, prompt: &str) -> Result<String> {
    let local = Arc::new(LocalHarness::open(config).await?);
    let conversation = Conversation::open(local.connect().await?)?;
    let result = receive(conversation, operation, prompt).await;
    let disconnected = match Arc::try_unwrap(local) {
        Ok(local) => local.disconnect().await,
        Err(_) => Err(Error::Conflict(
            "the embedding still has a live connection".into(),
        )),
    };
    result.and_then(|answer| disconnected.map(|()| answer))
}

async fn receive(conversation: Arc<Conversation>, operation: &str, prompt: &str) -> Result<String> {
    let mut events = conversation.observe(conversation.settled().await?).await?;
    let inbox = conversation.send(operation, prompt).await?;
    let mut status: Option<acyclic_harness::agent::StatusRecord> = None;
    loop {
        match conversation.reply(operation, inbox).await? {
            Reply::Answered { entry, .. } => return Ok(conversation.turn(&entry).await?.1),
            Reply::Dropped => return Err(Error::Conflict(conversation.dropped().await?)),
            Reply::Pending => {}
        }
        if let Some(status) = status.take() {
            if matches!(status.phase, Phase::WaitingApproval { .. }) {
                return Err(Error::Unsupported(
                    "this noninteractive embedding requires a settled approval policy".into(),
                ));
            }
            if let Some(reason) = acyclic_harness::local::agent_conversation::ended(&status) {
                return Err(Error::Conflict(reason));
            }
        }
        // Recheck the committed reply before acting on a terminal status. Output
        // notifications do not require another control read in this text frontend.
        status = loop {
            match events.next().await? {
                Event::Status(status) => break Some(status),
                Event::Turn(_) => break None,
                Event::Output => {}
            }
        };
    }
}

#[cfg(not(test))]
fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness_providers::{Credentials, Registry};
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    let [workspace, catalog, state, operation, prompt] = arguments.as_slice() else {
        return Err("usage: embedded WORKSPACE CATALOG STATE OPERATION PROMPT".into());
    };
    let workspace = std::path::Path::new(workspace);
    let operation = operation.to_str().ok_or("operation must be UTF-8")?;
    let prompt = prompt.to_str().ok_or("prompt must be UTF-8")?;
    let mut registry = Registry::new(Credentials::default())?;
    registry.load_catalog(std::path::Path::new(catalog))?;
    let config = registry
        .graphcoder(workspace, None)
        .without_user_settings()
        .state_directory(std::path::Path::new(state))
        .working_directory(workspace)?;
    let runtime = acyclic_native_runtime::coordination_runtime()?;
    let result = runtime.block_on(async {
        let mut text = answer(config, operation, prompt).await?;
        text.push('\n');
        acyclic_native_runtime::InheritedOutput::stdout()?
            .write_owned(text.into_bytes())
            .await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    });
    runtime.shutdown_background();
    result
}

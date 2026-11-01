//! ACP v1 prompt ingress for an already attached harness session.
//!
//! The editor's connection owner supplies JSON-RPC framing, initialization and
//! session admission. This adapter does not advertise a complete ACP server:
//! new-session workspace/profile admission and outgoing lifecycle notifications
//! belong to their shared harness owners. In particular an inbox delivery is
//! never an ACP prompt completion.
//!
//! Session IDs are the original [`crate::AgentPath`], not editor-local aliases.
//! Reattachment supplies the original [`crate::AgentSelection`] cursors. The
//! same [`crate::AgentClient`] owns observation and viewport retirement, and the
//! same [`crate::AgentControls`] owns every authenticated command.
//!
//! Wire shapes follow <https://agentclientprotocol.com/protocol/v1/content> and
//! <https://agentclientprotocol.com/protocol/v1/prompt-turn>. Resource display
//! metadata cannot change the original publisher's file identity or policy.

use crate::{
    AgentClient, AgentClientError, AgentCommand, AgentControls, AgentPath, AgentSelection,
    CommandError, IntentId, ModelContent, PreparedAgentInvocation, PreparedCommand,
};
use acyclic_harness_core::{
    conversation::FileRef,
    model::{FileProjectionPolicy, ModelContentPart},
};
use serde::Deserialize;
use std::future::Future;

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "acp_tests.rs"]
mod tests;

/// Baseline ACP v1 prompt content. Optional presentation metadata is ignored;
/// unsupported content variants fail decoding instead of silently disappearing.
/// Embedded documents, selections and diagnostics must first be published by
/// the host as authorized immutable files and supplied as resource links.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    /// Exact user text; no role, policy or configuration can be injected here.
    Text {
        /// Original text.
        text: String,
    },
    /// A host-admitted document/query snapshot, resolved at the input boundary.
    ResourceLink {
        /// Opaque original resource identity, never opened by this adapter.
        uri: String,
        /// Required ACP display label; never used as a path or authority.
        name: String,
    },
}

/// ACP `session/prompt` parameters. JSON-RPC request IDs are transport-local;
/// the caller separately retains a canonical [`IntentId`] across reconnects.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptRequest {
    /// Original harness agent identity.
    pub session_id: String,
    /// Ordered text and authorized resource references.
    pub prompt: Vec<ContentBlock>,
}

/// ACP `session/cancel` notification parameters. Cancellation uses its own
/// caller-owned stable intent; it never removes or declares a prompt completed.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelNotification {
    /// Original harness agent identity.
    pub session_id: String,
}

/// Admission failure before any command is sent. Resource resolution retains
/// the original owner's typed cause, including authorization/producer failure.
#[derive(Debug, thiserror::Error)]
pub enum InputError<E> {
    /// A request cannot retarget an attached editor or its prepared operations.
    #[error("ACP session differs from the attached harness agent; attach the requested session")]
    SessionMismatch,
    /// The host could not publish/resolve the authorized resource snapshot.
    #[error("ACP context admission failed: {0}")]
    Context(E),
    /// Canonical user-content validation or frame preparation refused input.
    #[error(transparent)]
    Command(#[from] CommandError),
}

/// One active editor attachment, with no model loop, transport, task, timer,
/// request map or document cache. Dropping it retires the original follows.
pub struct AttachedSession {
    agent: AgentPath,
    controls: AgentControls,
    observer: AgentClient,
}

impl AttachedSession {
    /// Attach using an existing authenticated pool and explicit replay cursors.
    /// No new connection, capability, workspace or execution profile is created.
    pub fn new(
        controls: AgentControls,
        selection: AgentSelection,
    ) -> Result<Self, AgentClientError> {
        let agent = selection.agent.clone();
        let observer = controls.observe(selection)?;
        Ok(Self {
            agent,
            controls,
            observer,
        })
    }

    /// Original session identity, shared by prompts, cancellation and reconnect.
    pub fn agent(&self) -> &AgentPath {
        &self.agent
    }

    /// Existing authorized operations, including paged children, file reads,
    /// approvals and exact uncertain-result recovery. No ACP-specific dispatcher.
    pub const fn controls(&self) -> &AgentControls {
        &self.controls
    }

    /// Original event/view owner. The caller drives its event future alongside
    /// stdin/socket notifications; no independent read loop or history is added.
    /// View changes do not retarget this attachment's prepared input.
    pub fn observer(&mut self) -> &mut AgentClient {
        &mut self.observer
    }

    /// Prepare, but do not send, an ACP prompt. The host's original resource
    /// publisher resolves each supplied link once, in input order. It must bind
    /// editor revisions/selections/diagnostics to an authorized immutable file;
    /// a URI or editor notification does not grant filesystem access.
    ///
    /// No local file, profile or credential is read here. The returned original
    /// invocation remains with the caller across every execute outcome. Resolve
    /// failure sends no command. Work and temporary storage scale with supplied
    /// content bytes only, never open documents, agents or conversation history.
    /// The future owns its input handles: keep and poll it alongside the original
    /// observer when publication is pending; observing events cannot cancel it.
    pub fn prepare_prompt<E, F, R>(
        &self,
        id: IntentId,
        request: PromptRequest,
        mut resolve: F,
    ) -> impl Future<Output = Result<PreparedAgentInvocation<'static>, InputError<E>>> + use<E, F, R>
    where
        F: FnMut(String) -> R,
        R: Future<Output = Result<FileRef, E>>,
    {
        let agent = self.agent.clone();
        let controls = self.controls.clone();
        async move {
            if request.session_id != agent.as_str() {
                return Err(InputError::SessionMismatch);
            }
            let mut parts = Vec::with_capacity(request.prompt.len());
            for block in request.prompt {
                parts.push(match block {
                    ContentBlock::Text { text } => ModelContentPart::Text { text },
                    ContentBlock::ResourceLink { uri, .. } => {
                        let file = resolve(uri).await.map_err(InputError::Context)?;
                        let policy = FileProjectionPolicy::for_attachment(&file);
                        ModelContentPart::File { file, policy }
                    }
                });
            }
            Ok(
                controls.prepare(PreparedCommand::new(&AgentCommand::Prompt {
                    agent,
                    id,
                    body: ModelContent::Parts(parts),
                })?)?,
            )
        }
    }

    /// Prepare cancellation through the original command and authorization
    /// path. A delivery receipt does not certify that physical work has stopped.
    pub fn prepare_cancel(
        &self,
        id: IntentId,
        notification: CancelNotification,
    ) -> Result<PreparedAgentInvocation<'static>, InputError<std::convert::Infallible>> {
        if notification.session_id != self.agent().as_str() {
            return Err(InputError::SessionMismatch);
        }
        Ok(self
            .controls
            .prepare(PreparedCommand::new(&AgentCommand::Cancel {
                agent: self.agent().clone(),
                id,
            })?)?)
    }
}

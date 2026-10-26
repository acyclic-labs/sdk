//! Trusted local decision ingress over the agent journal's approval owner.

use super::*;
use crate::{agent::Approval, approvals::ApprovalResponder};

impl LocalHarness {
    /// Durably decide an agent request, then wake its waiting turn.
    ///
    /// The embedding must authenticate `responder` from its human connection or
    /// running parent actor. Never construct it from model arguments or a file's
    /// principal. Only human and parent roles enter here; automatic reviews run
    /// through the journal's existing approval chain.
    ///
    /// Retrying identical arguments repairs a crash between decision and wake.
    /// An uncertain wake returns `agent::Error::Unknown`: retain and settle its
    /// exact request through `agent::submit`. A different decision never replaces
    /// the durable file. `Closed` means the decision persisted but no wake was
    /// admitted because the recipient was archived.
    pub async fn decide(
        self: &Arc<Self>,
        agent: &AgentPath,
        request: &IntentId,
        responder: ApprovalResponder<'_>,
        approved: bool,
        reason: Option<String>,
    ) -> std::result::Result<Delivered, crate::agent::Error> {
        let failure = |error| crate::agent::Error::Turn(Box::new(error));
        self.agents().await.map_err(failure)?;
        let hosted = self.agents.get().ok_or(crate::agent::Error::NoSuchAgent)?;
        if agent != &hosted.root
            && !agent
                .as_str()
                .starts_with(&format!("{}/children/", hosted.root.as_str()))
        {
            return Err(crate::agent::Error::NoSuchAgent);
        }
        let state = hosted.agents.state(agent).await?;
        let from = match responder {
            ApprovalResponder::Human(principal) => Sender::User {
                principal: principal.to_owned(),
            },
            ApprovalResponder::Parent(parent) => {
                let path = state
                    .parent
                    .as_ref()
                    .filter(|path| &path.id() == parent)
                    .ok_or(crate::agent::Error::Record(
                        "approval responder is not the parent",
                    ))?;
                Sender::Agent { path: path.clone() }
            }
            _ => {
                return Err(crate::agent::Error::Record(
                    "external approval role is required",
                ));
            }
        };
        hosted
            .turns
            .decide(&state, request.as_str(), responder, approved, reason)
            .await
            .map_err(failure)?;
        hosted
            .agents
            .send(
                agent,
                &from,
                &InboxRecord::Approval(Approval {
                    id: request.clone(),
                }),
            )
            .await
    }
}

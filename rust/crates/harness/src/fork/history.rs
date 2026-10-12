//! history fork protocol.
use super::*;
/// Child-owned pointer to one immutable revision of authoritative parent history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InheritedConversationPrefix {
    /// Version of this direct-parent reference contract.
    pub format_version: u32,
    /// Parent conversation from which this prefix was selected.
    pub parent: Authority,
    /// Exact parent revision observed during selection.
    pub parent_revision: u64,
    /// Agent bound to the parent conversation.
    pub parent_agent: AgentId,
    /// Inclusive final sequence selected for inheritance.
    pub through_sequence: u64,
    /// Readers bound to this preparation retry identity; grants live in the seed.
    pub attached_agents: Vec<AgentId>,
    /// Version 3 domain-separated chain of canonical per-message digests.
    pub message_digest: [u8; 32],
}

impl InheritedConversationPrefix {
    /// Binds one contiguous parent prefix without retaining its message list.
    pub fn select(
        parent: Authority,
        parent_revision: u64,
        through_sequence: u64,
        attached_agents: &[AgentId],
        conversation: &crate::conversation::ConversationState,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let mut owned = CodecQuota::observed(work)?;
        let parent_agent = conversation
            .agent
            .ok_or_else(|| Error::Invalid("inherited prefix parent is unbound".into()))?;
        if through_sequence > conversation.logical_revision() {
            return Err(Error::Invalid(
                "inherited prefix exceeds parent history".into(),
            ));
        }
        if attached_agents.len() as u64 > MAX_FORK_AGENTS as u64 {
            return Err(Error::Invalid(
                "too many attached agents for inherited context".into(),
            ));
        }
        for (index, reader) in attached_agents.iter().enumerate() {
            owned.checkpoint()?;
            for prior in &attached_agents[..index] {
                owned.checkpoint()?;
                if prior == reader {
                    return Err(owned.invalid("inherited attached agents are invalid"));
                }
            }
        }
        let message_digest = conversation.history_prefix_digest(through_sequence, &mut None)?;
        let attached_agents = crate::core::copy_copy_vector(attached_agents, &mut owned)?;
        owned.transfer();
        Ok(Self {
            format_version: 3,
            parent,
            parent_revision,
            parent_agent,
            through_sequence,
            attached_agents,
            message_digest,
        })
    }
}

/// A finite, authenticated archive selection used only during explicit fork preparation.
#[derive(Debug)]
pub struct PreparedForkHistory {
    pub(in crate::fork) parent: Authority,
    pub(in crate::fork) parent_revision: u64,
    pub(in crate::fork) conversation: crate::conversation::ConversationState,
    #[cfg(feature = "filesystem")]
    pub(in crate::fork) attestation: [u8; 32],
}

impl PreparedForkHistory {
    pub(crate) fn from_archive(
        parent: Authority,
        parent_revision: u64,
        conversation: crate::conversation::ConversationState,
        #[cfg(feature = "filesystem")] attestation: [u8; 32],
    ) -> Self {
        Self {
            parent,
            parent_revision,
            conversation,
            #[cfg(feature = "filesystem")]
            attestation,
        }
    }

    /// Original parent whose indexed records were authenticated.
    #[must_use]
    pub const fn parent(&self) -> &Authority {
        &self.parent
    }

    /// Exact canonical event cut of the archive selection.
    #[must_use]
    pub const fn parent_revision(&self) -> u64 {
        self.parent_revision
    }

    /// Inclusive logical message cut, independent of parent residency.
    #[must_use]
    pub fn through_sequence(&self) -> u64 {
        self.conversation.logical_revision()
    }

    #[cfg(feature = "filesystem")]
    pub(crate) const fn conversation(&self) -> &crate::conversation::ConversationState {
        &self.conversation
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn verify(
        &self,
        verifier: &crate::core::AuthorityVerifier,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        verifier.verify_audience(&self.parent, work)?;
        let digest = self
            .conversation
            .history_prefix_digest(self.through_sequence(), work)?;
        if self.attestation
            != verifier.fork_history_selection_proof_with_work(
                self.parent_revision,
                self.through_sequence(),
                self.conversation.agent,
                digest,
                work,
            )?
        {
            #[cfg(not(target_arch = "wasm32"))]
            if work
                .as_ref()
                .is_some_and(|work| work.original_native_work().is_some())
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "fork archive belongs to another issuer",
                });
            }
            return Err(Error::Unauthorized(
                "fork archive belongs to another issuer".into(),
            ));
        }
        Ok(())
    }
}

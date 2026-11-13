//! Peer authority is a capability over canonical agent paths, independent of
//! discovery. Discover authorized subtrees using Streams children pages.

use super::AgentPath;

/// Root-only append-message authority. Membership remains host-owned state.
pub const SEND_PEER: &str = "agent.send-peer";

/// The latest user decision for a root's peer participation, committed with
/// creation, an explicit user action, or archive. It confers no other authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerMembership {
    /// Whether this root may see, send to, and receive from peer sessions.
    pub enabled: bool,
}

impl super::AgentRecord for PeerMembership {}

/// The two enabled membership versions admitted for an automatic peer send.
/// Publication must still match both versions, so a later opt-out fences it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerAdmission {
    /// Sender's exact enabled head.
    pub sender: PeerMembershipHead,
    /// Recipient's exact enabled head.
    pub recipient: PeerMembershipHead,
}

/// An immutable membership record's publication identity.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerMembershipHead {
    /// Exclusive sequence following this membership record.
    pub tail: u64,
    /// Original Streams commit identity.
    pub commit: [u8; 32],
}

impl PeerAdmission {
    /// Verify that a caller-retained admission refers to enabled immutable
    /// records. Current heads are checked by the inbox commit, not this read:
    /// exact replay must still recover its original result after opt-out.
    pub async fn validate<P: crate::store::JournalProvider + ?Sized>(
        &self,
        provider: &P,
        sender: &AgentPath,
        recipient: &AgentPath,
    ) -> Result<(), super::Error> {
        for (root, head) in [(sender, &self.sender), (recipient, &self.recipient)] {
            head.validate(provider, root).await?;
        }
        Ok(())
    }

    pub(super) fn conditions(
        &self,
        sender: &AgentPath,
        recipient: &AgentPath,
    ) -> Result<Vec<acyclic_stream::CommitCondition>, super::Error> {
        if sender == recipient {
            return Err(super::Error::Record(
                "automatic peers require distinct roots",
            ));
        }
        let mut conditions = std::collections::BTreeMap::new();
        for (path, head) in [
            (sender.peer_membership()?, &self.sender),
            (recipient.peer_membership()?, &self.recipient),
        ] {
            conditions.insert(path.clone(), head.condition(path));
        }
        Ok(conditions.into_values().collect())
    }
}

impl PeerMembershipHead {
    pub(super) fn condition(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> acyclic_stream::CommitCondition {
        acyclic_stream::CommitCondition::Head {
            path,
            expected: self.tail,
            commit_id: acyclic_stream::CommitId::from_bytes(self.commit),
        }
    }

    /// Check the retained enabled record, independently of its current head.
    /// A publication guard fences opt-out, while exact retries can still
    /// recover a receipt after a later membership change.
    pub(super) async fn validate<P: crate::store::JournalProvider + ?Sized>(
        &self,
        provider: &P,
        root: &AgentPath,
    ) -> Result<(), super::Error> {
        if !self.retained(provider, root).await? {
            return Err(super::Error::Record("peer membership record is missing"));
        }
        Ok(())
    }

    /// Absence is distinct from an invalid retained witness. Only an owning
    /// delivery host can turn absence into a refusal, fenced by inbox absence
    /// in that same publication; it must never bypass a disabled record.
    pub(super) async fn retained<P: crate::store::JournalProvider + ?Sized>(
        &self,
        provider: &P,
        root: &AgentPath,
    ) -> Result<bool, super::Error> {
        use super::AgentRecord as _;
        use acyclic_stream::StreamProviderExt as _;
        let sequence = self
            .tail
            .checked_sub(1)
            .ok_or(super::Error::Record("peer membership head is empty"))?;
        let path = root.peer_membership()?;
        let Some(record) = provider
            .record_at(path.clone(), sequence)
            .await
            .map_err(super::Error::Stream)?
        else {
            return Ok(false);
        };
        if record.origin != path
            || record.commit_id.as_bytes() != &self.commit
            || !PeerMembership::decode(&record.value)?.enabled
        {
            return Err(super::Error::Record("peer membership was not enabled"));
        }
        Ok(true)
    }
}

/// The immutable launch choice, independently of later user membership edits.
/// Session creation replay must compare this original record, not today's head.
pub async fn initial_peer_membership<P: crate::store::JournalProvider + ?Sized>(
    provider: &P,
    root: &AgentPath,
) -> Result<PeerMembership, super::Error> {
    use super::AgentRecord as _;
    use futures::TryStreamExt as _;
    let path = root.peer_membership()?;
    let mut records = provider
        .read(acyclic_stream::ReadRequest {
            key: None,
            path: path.clone(),
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
        .await
        .map_err(super::Error::Stream)?;
    let record = records
        .try_next()
        .await
        .map_err(super::Error::Stream)?
        .filter(|record| record.sequence == 0)
        .ok_or(super::Error::Record("root has no original peer choice"))?;
    if record.origin != path {
        return Err(super::Error::Record("peer membership origin differs"));
    }
    PeerMembership::decode(&record.value)
}

/// Check both membership heads at one authority cut, without directory reads.
/// A child, absent record, or opted-out root cannot acquire automatic peer reach.
pub async fn session_peer_allowed<P: crate::store::JournalProvider + ?Sized>(
    provider: &P,
    grants: &crate::Capabilities,
    sender: &AgentPath,
    recipient: &AgentPath,
) -> Result<bool, super::Error> {
    Ok(session_peer_admission(provider, grants, sender, recipient)
        .await?
        .is_some())
}

/// Selects enabled membership and the exact publication guards in one view.
pub async fn session_peer_admission<P: crate::store::JournalProvider + ?Sized>(
    provider: &P,
    grants: &crate::Capabilities,
    sender: &AgentPath,
    recipient: &AgentPath,
) -> Result<Option<PeerAdmission>, super::Error> {
    use super::AgentRecord as _;
    use acyclic_stream::{HeadProjection, HeadsRequest};

    if !grants.contains(SEND_PEER)
        || sender.depth() != 0
        || recipient.depth() != 0
        || sender == recipient
    {
        return Ok(None);
    }
    let request = HeadsRequest {
        retained: Default::default(),
        durable_through: None,
        projection: HeadProjection::History,
        paths: [sender.peer_membership()?, recipient.peer_membership()?]
            .into_iter()
            .collect(),
        children: None,
        limit: 2,
        at_least: Default::default(),
    };
    let view = provider
        .heads(request.clone())
        .await
        .map_err(super::Error::Stream)?;
    view.validate_for(&request).map_err(super::Error::Stream)?;
    let mut admitted = Vec::with_capacity(2);
    for (path, head) in request.paths.iter().zip(view.heads) {
        let Some(head) = head else {
            return Ok(None);
        };
        let Some(record) = head.last else {
            return Ok(None);
        };
        if record.origin != *path || record.sequence.checked_add(1) != Some(head.tail) {
            return Err(super::Error::Record(
                "peer membership head provenance differs",
            ));
        }
        if !PeerMembership::decode(&record.value)?.enabled {
            return Ok(None);
        }
        admitted.push(PeerMembershipHead {
            tail: head.tail,
            commit: *record.commit_id.as_bytes(),
        });
    }
    let mut admitted = admitted.into_iter();
    let sender = admitted
        .next()
        .ok_or(super::Error::Record("sender membership missing"))?;
    let recipient = admitted
        .next()
        .ok_or(super::Error::Record("recipient membership missing"))?;
    Ok(Some(PeerAdmission { sender, recipient }))
}

/// A sibling capability follows its name across released setup failures. The
/// caller supplies the sender's parent; a child cannot confer a foreign name.
pub async fn sibling_send_allowed(
    resolver: &dyn super::TurnChildren,
    grants: &crate::Capabilities,
    parent: &AgentPath,
    recipient: &AgentPath,
) -> crate::Result<bool> {
    if !recipient.is_child_of(parent) {
        return Ok(false);
    }
    for grant in grants.iter() {
        if let Some(name) = grant.strip_prefix("agent.send-sibling:") {
            if super::spawn_names::spawn_intent(name, 0).is_ok()
                && resolver.resolve(parent, name).await?.live().as_ref() == Some(recipient)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Explicit extra authority beyond an agent's parent and open children.
/// `agent.send:<path>` grants one inbox; `agent.send-subtree:<path>` grants
/// that inbox and its descendants. Unrelated string prefixes never match.
#[must_use]
pub fn peer_send_allowed(grants: &crate::Capabilities, recipient: &AgentPath) -> bool {
    if grants.contains(&format!("agent.send:{recipient}")) {
        return true;
    }
    // Indexed membership for the canonical ancestry, never a scan over the
    // caller's grants or over any agent roster. One buffer serves every prefix.
    let mut label = format!("agent.send-subtree:{recipient}");
    loop {
        if grants.contains(&label) {
            return true;
        }
        let Some((parent, _)) = label.rsplit_once("/children/") else {
            return false;
        };
        label.truncate(parent.len());
    }
}

/// Boundary chosen by the sender. Both modes append to the original inbox.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum MessageDelivery {
    /// Include at the next model boundary, without cancelling active tools.
    #[default]
    Steer,
    /// Start a subsequent turn after the current turn settles.
    FollowUp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_grants_use_canonical_ancestry_and_attenuate() -> Result<(), super::super::Error> {
        let root = AgentPath::root(&super::super::AgentId::from_bytes([1; 16]));
        let child = root.child(&super::super::AgentId::from_bytes([2; 16]))?;
        let other = AgentPath::root(&super::super::AgentId::from_bytes([3; 16]));
        let exact = crate::Capabilities::new([format!("agent.send:{root}")]);
        let subtree = crate::Capabilities::new([format!("agent.send-subtree:{root}")]);
        assert!(peer_send_allowed(&exact, &root));
        assert!(!peer_send_allowed(&exact, &child));
        assert!(peer_send_allowed(&subtree, &child));
        assert!(!peer_send_allowed(&subtree, &other));
        assert!(!peer_send_allowed(&crate::Capabilities::default(), &root));
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod provenance_tests {
    use super::*;
    use crate::agent::{AgentId, AgentRecord};
    use acyclic_stream::{
        CommitCondition, CommitEffect, CommitMutation, CommitOutcome, CommitRequest,
        IdempotencyKey, MemoryStream, StreamProviderExt as _,
    };

    #[tokio::test]
    async fn inherited_membership_cannot_admit_either_peer()
    -> Result<(), Box<dyn std::error::Error>> {
        for inherited_sender in [false, true] {
            let provider = MemoryStream::default();
            let sender = AgentPath::root(&AgentId::from_bytes([1; 16]));
            let recipient = AgentPath::root(&AgentId::from_bytes([2; 16]));
            let original = AgentPath::root(&AgentId::from_bytes([3; 16]));
            let inherited = if inherited_sender {
                &sender
            } else {
                &recipient
            };
            let direct = if inherited_sender {
                &recipient
            } else {
                &sender
            };
            let enabled = PeerMembership { enabled: true };
            let publish = async |effect| {
                let outcome = provider
                    .commit(CommitRequest {
                        deadline_unix_millis: None,
                        idempotency_key: IdempotencyKey::unique(),
                        effects: vec![effect],
                    })
                    .await
                    .map_err(super::super::Error::Stream)?;
                assert!(matches!(outcome, CommitOutcome::Committed(_)));
                Ok::<_, super::super::Error>(())
            };
            publish(CommitEffect::hard(
                Vec::new(),
                vec![
                    CommitMutation::Append {
                        path: original.peer_membership()?,
                        records: vec![enabled.encode()?.into()],
                    },
                    CommitMutation::Append {
                        path: direct.peer_membership()?,
                        records: vec![enabled.encode()?.into()],
                    },
                ],
            ))
            .await?;
            publish(CommitEffect::hard(
                vec![CommitCondition::Absent {
                    path: inherited.peer_membership()?,
                }],
                vec![CommitMutation::Fork {
                    source: original.peer_membership()?,
                    destination: inherited.peer_membership()?,
                    at_tail: 1,
                    exclude: Vec::new(),
                    records: Vec::new(),
                }],
            ))
            .await?;
            let grants = crate::Capabilities::new([SEND_PEER.to_owned()]);
            // Identical enabled bytes on their original streams are admitted.
            assert!(
                session_peer_admission(&provider, &grants, direct, &original)
                    .await?
                    .is_some()
            );
            assert_eq!(initial_peer_membership(&provider, direct).await?, enabled);
            // Removing the provenance check admits this real inherited head;
            // its enabled value and retained commit otherwise match exactly.
            assert!(
                session_peer_admission(&provider, &grants, &sender, &recipient)
                    .await
                    .is_err()
            );
            assert!(initial_peer_membership(&provider, inherited).await.is_err());
        }
        Ok(())
    }
}

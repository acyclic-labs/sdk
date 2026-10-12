//! request fork protocol.
use super::*;
/// Idempotent request to capture selected resources for a new child.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ForkRequest {
    /// Stable identity used for preparation and publication reconciliation.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub operation_id: OperationId,
    /// Publishing parent aggregate.
    pub parent: Authority,
    /// Exact parent revision before publication.
    #[cfg_attr(feature = "wasm", tsify(type = "bigint"))]
    pub parent_revision: u64,
    /// Fresh child aggregate.
    pub child: Authority,
    /// Fresh child agent identity.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub child_agent: AgentId,
    /// Additional agents allowed to attach to the child environment as readers.
    #[cfg_attr(feature = "wasm", tsify(type = "readonly string[]"))]
    pub attached_agents: Vec<AgentId>,
    /// Exact child allocation and bounded inherited prefix chosen before preparation.
    pub preparation: ForkPreparation,
    /// Ordered required and optional resource selections.
    #[cfg_attr(feature = "wasm", tsify(type = "readonly ForkSelection[]"))]
    pub selections: Vec<ForkSelection>,
    /// Present only when one provider attests a common capture boundary.
    pub boundary: Option<AttestedBoundary>,
    /// Operation-sealed workspace read; never grants source transcript access.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorized_source: Option<crate::source_authorization::ProjectSourceAuthorization>,
}

impl ForkRequest {

    /// Resolves the context policy against the exact parent before preparation.
    pub fn with_history_policy(
        mut self,
        parent: &crate::core::Reducer,
        policy: ForkHistoryPolicy,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        let policy_error = |code, reason| boundary_error(work, code, reason);
        if &self.parent != parent.authority() || self.parent_revision != parent.revision() {
            return Err(policy_error(crate::wire::ErrorCode::Conflict,
                "fork context policy requires the exact requested parent revision"));
        }
        let conversation = parent.conversation().ok_or_else(|| policy_error(
            crate::wire::ErrorCode::Invalid, "fork context policy requires a parent conversation"))?;
        if conversation.agent.is_none() {
            return Err(policy_error(crate::wire::ErrorCode::Invalid,
                "fork context policy requires a bound parent agent"));
        }
        if let Some(observer) = work.as_deref_mut() {
            observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        }
        let full_cut = conversation.logical_revision();
        let (through_sequence, summary) = match policy {
            ForkHistoryPolicy::Fork => (full_cut, None),
            ForkHistoryPolicy::Fresh => (0, None),
            ForkHistoryPolicy::Summary(selection) => (full_cut, Some(*selection)),
        };
        self.preparation.inherited_through_sequence = through_sequence;
        self.preparation.summary = summary;
        self.validate(work)?;
        Ok(self)
    }

    /// Checks identities, resource selections and optional attestation metadata.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut work = CodecQuota::observed(work)?;
        if self.attached_agents.len() as u64 > MAX_FORK_AGENTS as u64
            || self.selections.len() as u64 > MAX_FORK_RESOURCES as u64
        {
            return Err(work.invalid("fork request exceeds protocol limits"));
        }
        if self.parent == self.child {
            return Err(work.invalid("fork child cannot equal its parent"));
        }
        if self.parent.kind != crate::core::AggregateKind::Conversation
            || self.child.kind != crate::core::AggregateKind::Conversation
        {
            return Err(work.invalid("fork must link parent and child conversations"));
        }
        let parent_path = work.stream_path(&self.parent)?;
        let _child_path = work.stream_path(&self.child)?;
        self.preparation.validate(self.child_agent, work.work())?;
        if let Some(source) = &self.authorized_source {
            source.validate(work.work())?;
            if source.operation_id != self.operation_id
                || !self.selections.iter().any(|selection| matches!(
                    &selection.revision,
                    ResourceRevision::Project { volume, generation }
                        if volume == &source.project && generation == &source.generation))
                || (source.source_task != source.caller_task
                    && (self.preparation.inherited_through_sequence != 0
                        || self.preparation.summary.is_some()))
            {
                return Err(boundary_error(work.work(), crate::wire::ErrorCode::Unauthorized,
                    "authorized source must select its exact project and history-free cross-task fork"));
            }
        }
        work.slots::<AgentId>(self.attached_agents.len())?;
        let attached: BTreeSet<_> = self.attached_agents.iter().copied().collect();
        if attached.len() != self.attached_agents.len() || attached.contains(&self.child_agent) {
            return Err(work.invalid("fork attached agents are not unique"));
        }
        work.slots::<Vec<u8>>(self.selections.len())?;
        let mut selected = BTreeSet::new();
        let mut histories = 0_usize;
        let mut projects = 0_usize;
        let mut private = 0_usize;
        let mut contexts = 0_usize;
        let mut processes = 0_usize;
        for selection in &self.selections {
            work.checkpoint()?;
            selection.revision.validate(work.work())?;
            let identity = work.canonical(&selection.revision)?;
            if !selected.insert(identity) {
                return Err(work.invalid("fork resource is selected more than once"));
            }
            match &selection.revision {
                ResourceRevision::History(reference) => {
                    histories += 1;
                    if !selection.required
                        || reference.as_resource().key() != parent_path.as_bytes()
                        || reference
                            .as_resource()
                            .version()
                            .and_then(|version| version.parse::<u64>().ok())
                            != Some(self.parent_revision)
                    {
                        return Err(work.invalid("fork history is not the required exact parent prefix"));
                    }
                }
                ResourceRevision::Project { volume, .. }
                | ResourceRevision::PrivateVolume { volume, .. } => {
                    let (destination, count) = match &selection.revision {
                        ResourceRevision::Project { .. } => {
                            (&self.preparation.child_project_volume, &mut projects)
                        }
                        _ => (&self.preparation.child_private_volume, &mut private),
                    };
                    *count += 1;
                    if !selection.required
                        || volume == destination
                        || volume.provider() != destination.provider()
                    {
                        return Err(work.invalid("fork selected volume does not match child allocation"));
                    }
                }
                ResourceRevision::Context(_) => contexts += 1,
                ResourceRevision::Process(_) => processes += 1,
                _ => {}
            }
        }
        if histories != 1 || projects != 1 || private != 1 || contexts > 1 || processes > 1 {
            return Err(work.invalid("fork requires one history, project and scratch selection"));
        }
        if let Some(boundary) = &self.boundary {
            boundary.validate(work.work())?;
        }
        Ok(())
    }

    pub(crate) fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<(Self, u64)> {
        crate::wire_codec::CodecQuota::produce(work, |quota| copy_request(self, quota))
    }

}

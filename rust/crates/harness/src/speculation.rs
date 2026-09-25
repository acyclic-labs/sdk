//! Speculative execution: several attempts at one task, each on its own workspace
//! fork, with one judged winner merged into the target workspace.
//!
//! A speculation is a structured scheduler operation whose orchestration is
//! [`Orchestration::Speculate`]. Its children are the attempts. The pure scheduler
//! records every durable step so a restarted coordinator reproduces the outcome
//! without judging again:
//!
//! 1. [`SchedulerEvent::AttemptEvaluated`] pins a succeeded attempt's workspace
//!    generation, its diff summary, and its check report.
//! 2. [`SchedulerEvent::Judged`] records the verdict bound to the digest of the exact
//!    [`JudgeRequest`] and cancels every loser that is still running.
//! 3. [`SchedulerEvent::SpeculationSettled`] records the merged target generation
//!    after losers were discarded and the winner was merged. The winner's fork is
//!    discarded only after this record is durable.
//!
//! A parent decision that cancels an undecided speculation cancels the node and
//! its attempts at once; settling it then discards every attempt workspace.
//!
//! Nothing reaches the target workspace unless an attempt qualifies and the judge
//! chooses it.

use crate::{
    Error, OperationId, Outcome, Result,
    resources::{GenerationRef, WorkspaceRef},
    scheduler::{
        DurableOwner, EntrypointRef, OperationPhase, OperationSpec, Orchestration,
        OrchestrationDecision, ParentLink, ResourceRequest, Scheduler, SchedulerEvent,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// Largest retained check output; longer output keeps its tail.
pub const MAXIMUM_CHECK_OUTPUT_BYTES: usize = 16 * 1024;
/// Largest retained changed-path list in one diff summary.
pub const MAXIMUM_SUMMARY_PATHS: usize = 4_096;
/// Largest retained judge rationale.
pub const MAXIMUM_RATIONALE_BYTES: usize = 16 * 1024;

/// User-set bounds on recursive fork-join and speculation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SwarmLimits {
    /// Most structured children one operation may declare, including speculation attempts.
    pub max_width: u32,
    /// Deepest structured nesting. A speculation node does not consume a level: its
    /// attempts sit one level below the operation that requested it.
    pub max_depth: u32,
    /// Most operations admitted or running at once on one coordinator.
    pub max_running: u32,
}

impl SwarmLimits {
    /// Defaults used for Acyclic dogfooding.
    pub const DOGFOOD: Self = Self {
        max_width: 16,
        max_depth: 3,
        max_running: 64,
    };
}

impl Default for SwarmLimits {
    fn default() -> Self {
        Self::DOGFOOD
    }
}

/// When the judge is consulted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JudgeTiming {
    /// Only after every attempt settled; the judge must then decide.
    AllSettled,
    /// Also whenever a qualified attempt settles; the judge may decide early or defer.
    EachSettled,
}

/// What happens to attempts that did not win.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoserPolicy {
    /// Losers still running are cancelled when the verdict commits; every loser
    /// workspace is discarded when the speculation settles.
    CancelOnDecision,
}

/// Immutable judging rules pinned when a speculation is declared.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SpeculationPolicy {
    /// Exact judge contract; hosts must supply a [`Judge`] with this identity.
    pub judge: EntrypointRef,
    /// Host-interpreted check run in a throwaway fork of each attempt. When present,
    /// only attempts whose check passed qualify.
    pub check: Option<Value>,
    /// When the judge is consulted.
    pub timing: JudgeTiming,
    /// Treatment of attempts that did not win.
    pub losers: LoserPolicy,
}

/// One attempt's isolated workspace and the generation it was forked at.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AttemptWorkspace {
    /// Mutable fork the attempt works in.
    pub workspace: WorkspaceRef,
    /// Fork's first generation; diffs are computed from here.
    pub base: GenerationRef,
}

/// Complete durable plan of one speculation node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SpeculationPlan {
    /// Judging rules.
    pub policy: SpeculationPolicy,
    /// Workspace the winner is merged into.
    pub target: WorkspaceRef,
    /// Attempt workspaces by stable child slot.
    pub attempts: BTreeMap<String, AttemptWorkspace>,
}

impl SpeculationPlan {
    /// Validates identities and references after deserialization.
    pub fn validate(&self) -> Result<()> {
        if self.attempts.is_empty() {
            return Err(Error::Invalid("speculation requires an attempt".into()));
        }
        if self.policy.judge.name.trim().is_empty() || self.policy.judge.version.trim().is_empty() {
            return Err(Error::Invalid(
                "speculation judge name and version are required".into(),
            ));
        }
        self.target.validate()?;
        let mut workspaces = Vec::with_capacity(self.attempts.len());
        for (slot, attempt) in &self.attempts {
            if slot.trim().is_empty() {
                return Err(Error::Invalid("speculation slot is empty".into()));
            }
            attempt.workspace.validate()?;
            attempt.base.validate()?;
            if attempt.workspace == self.target || workspaces.contains(&&attempt.workspace) {
                return Err(Error::Invalid(
                    "every attempt requires its own workspace fork".into(),
                ));
            }
            workspaces.push(&attempt.workspace);
        }
        Ok(())
    }
}

/// One attempt of a [`SpeculationRequest`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AttemptSpec {
    /// Stable child slot.
    pub slot: String,
    /// Stable attempt operation identity.
    pub operation_id: OperationId,
    /// Prepared workspace fork.
    pub workspace: AttemptWorkspace,
    /// Restartable attempt implementation.
    pub entrypoint: EntrypointRef,
    /// Logical capacity requirements.
    pub resources: ResourceRequest,
    /// Provider-neutral placement constraints.
    pub placement: Value,
    /// Attempt orchestration; attempts may fork further.
    pub orchestration: Orchestration,
    /// Schema-defined initial state, such as the task and approach.
    pub state: Value,
}

/// Complete declaration of one speculation and its attempts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpeculationRequest {
    /// Stable speculation node identity.
    pub operation_id: OperationId,
    /// Operation that requested the speculation, if any.
    pub parent: Option<ParentLink>,
    /// Durable owner shared by the node and every attempt.
    pub owner: DurableOwner,
    /// Judging rules.
    pub policy: SpeculationPolicy,
    /// Workspace the winner is merged into.
    pub target: WorkspaceRef,
    /// Attempts in declaration order.
    pub attempts: Vec<AttemptSpec>,
}

impl SpeculationRequest {
    /// Returns the node declaration followed by one declaration per attempt.
    pub fn declarations(&self) -> Result<Vec<OperationSpec>> {
        let mut attempts = BTreeMap::new();
        for attempt in &self.attempts {
            if attempts
                .insert(attempt.slot.clone(), attempt.workspace.clone())
                .is_some()
            {
                return Err(Error::Invalid("speculation slots must be unique".into()));
            }
        }
        let plan = SpeculationPlan {
            policy: self.policy.clone(),
            target: self.target.clone(),
            attempts,
        };
        plan.validate()?;
        let mut declarations = Vec::with_capacity(self.attempts.len() + 1);
        declarations.push(OperationSpec {
            operation_id: self.operation_id,
            parent: self.parent.clone(),
            owner: self.owner.clone(),
            entrypoint: self.policy.judge.clone(),
            dependencies: BTreeSet::new(),
            resources: ResourceRequest::default(),
            placement: Value::Null,
            orchestration: Orchestration::Speculate {
                plan: Box::new(plan),
            },
            state: Value::Null,
        });
        declarations.extend(self.attempts.iter().map(|attempt| OperationSpec {
            operation_id: attempt.operation_id,
            parent: Some(ParentLink {
                operation_id: self.operation_id,
                slot: attempt.slot.clone(),
            }),
            owner: self.owner.clone(),
            entrypoint: attempt.entrypoint.clone(),
            dependencies: BTreeSet::new(),
            resources: attempt.resources.clone(),
            placement: attempt.placement.clone(),
            orchestration: attempt.orchestration.clone(),
            state: attempt.state.clone(),
        }));
        Ok(declarations)
    }
}

/// Bounded summary of what one attempt changed since its fork.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DiffSummary {
    /// Changed absolute paths in stable order, excluding host metadata artifacts.
    pub changed_paths: Vec<String>,
    /// More paths changed than were retained.
    pub truncated: bool,
}

/// Result of one check run in a throwaway fork of an attempt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckReport {
    /// Whether the check passed.
    pub passed: bool,
    /// Tail of the combined check output.
    pub output: String,
}

impl CheckReport {
    /// Builds a report that retains at most [`MAXIMUM_CHECK_OUTPUT_BYTES`] of output tail.
    #[must_use]
    pub fn new(passed: bool, output: &str) -> Self {
        let mut start = output.len().saturating_sub(MAXIMUM_CHECK_OUTPUT_BYTES);
        while !output.is_char_boundary(start) {
            start += 1;
        }
        Self {
            passed,
            output: output.get(start..).unwrap_or_default().to_owned(),
        }
    }
}

/// Durable evidence about one succeeded attempt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AttemptEvaluation {
    /// Attempt generation pinned for judging and merging.
    pub generation: GenerationRef,
    /// Changes since the attempt's fork.
    pub diff: DiffSummary,
    /// Check report, present exactly when the policy has a check.
    pub check: Option<CheckReport>,
}

/// Everything the judge knows about one settled attempt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AttemptEvidence {
    /// Stable child slot.
    pub slot: String,
    /// Attempt operation.
    pub operation_id: OperationId,
    /// Terminal outcome; a success carries the attempt's final message.
    pub outcome: Outcome<Value>,
    /// Evaluation of a succeeded attempt.
    pub evaluation: Option<AttemptEvaluation>,
    /// Whether the judge may choose this attempt.
    pub qualified: bool,
}

/// Exact judge input; its digest binds the recorded verdict.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JudgeRequest {
    /// Pinned judge contract.
    pub judge: EntrypointRef,
    /// Pinned check description, if any.
    pub check: Option<Value>,
    /// Every attempt settled, so the judge must decide.
    pub complete: bool,
    /// Settled attempts in stable slot order.
    pub attempts: Vec<AttemptEvidence>,
}

impl JudgeRequest {
    /// Canonical digest of this exact judge invocation.
    pub fn digest(&self) -> Result<[u8; 32]> {
        let bytes = serde_json::to_vec(self).map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(*blake3::hash(&bytes).as_bytes())
    }

    /// Iterates attempts the judge may choose.
    pub fn qualified(&self) -> impl Iterator<Item = &AttemptEvidence> {
        self.attempts.iter().filter(|attempt| attempt.qualified)
    }
}

/// Judge decision: the winning slot, or none, with a rationale.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Verdict {
    /// Winning slot; `None` merges nothing.
    pub winner: Option<String>,
    /// Human-readable reason, retained for analysis.
    pub rationale: String,
}

/// Durable projection of one speculation node.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SpeculationState {
    /// Evaluations of succeeded attempts by slot.
    pub evaluations: BTreeMap<String, AttemptEvaluation>,
    /// Committed verdict.
    pub verdict: Option<Verdict>,
    /// Digest of the judge request the verdict answered.
    pub judgment_digest: Option<[u8; 32]>,
    /// Losers were discarded and the winner, if any, was merged; or, after a
    /// cancellation before the verdict, every attempt workspace was discarded.
    pub settled: bool,
    /// Target generation produced by merging the winner.
    pub merged: Option<GenerationRef>,
}

/// Identity of the deterministic [`fewest_changes`] chooser.
#[must_use]
pub fn fewest_changes_entrypoint() -> EntrypointRef {
    EntrypointRef {
        name: "acyclic.speculation.fewest_changes".into(),
        version: "1".into(),
        digest: *blake3::hash(b"acyclic.speculation.fewest_changes.v1").as_bytes(),
        result_schema: json!({"type": "object"}),
    }
}

/// Deterministic chooser: the qualified attempt that changed the fewest paths,
/// ties going to the earliest slot.
#[must_use]
pub fn fewest_changes(request: &JudgeRequest) -> Verdict {
    let winner = request.qualified().min_by_key(|attempt| {
        attempt
            .evaluation
            .as_ref()
            .map_or((true, usize::MAX), |evaluation| {
                (
                    evaluation.diff.truncated,
                    evaluation.diff.changed_paths.len(),
                )
            })
    });
    match winner {
        Some(attempt) => Verdict {
            winner: Some(attempt.slot.clone()),
            rationale: format!(
                "{} changed the fewest paths among qualified attempts",
                attempt.slot
            ),
        },
        None => Verdict {
            winner: None,
            rationale: "no attempt qualified".into(),
        },
    }
}

impl Scheduler {
    /// Returns one speculation node's durable projection.
    #[must_use]
    pub fn speculation(&self, operation_id: OperationId) -> Option<&SpeculationState> {
        self.speculations.get(&operation_id)
    }

    /// Returns one speculation node's immutable plan.
    #[must_use]
    pub fn speculation_plan(&self, operation_id: OperationId) -> Option<&SpeculationPlan> {
        match &self.operations.get(&operation_id)?.spec.orchestration {
            Orchestration::Speculate { plan } => Some(plan),
            _ => None,
        }
    }

    pub(crate) fn speculation_decision(
        &self,
        parent: OperationId,
        plan: &SpeculationPlan,
    ) -> OrchestrationDecision {
        let decided = self
            .speculations
            .get(&parent)
            .is_none_or(|state| state.verdict.is_some());
        let waiting = self
            .operations
            .get(&parent)
            .is_some_and(|state| state.phase == OperationPhase::WaitingForChildren);
        if decided || !waiting {
            return OrchestrationDecision::Wait;
        }
        let request = self.judge_request(parent, plan);
        let early =
            plan.policy.timing == JudgeTiming::EachSettled && request.qualified().next().is_some();
        if request.complete || early {
            OrchestrationDecision::Judge {
                request: Box::new(request),
            }
        } else {
            OrchestrationDecision::Wait
        }
    }

    fn judge_request(&self, parent: OperationId, plan: &SpeculationPlan) -> JudgeRequest {
        let evaluations = self
            .speculations
            .get(&parent)
            .map(|state| &state.evaluations);
        let mut attempts = Vec::new();
        let mut declared = 0;
        for (slot, child) in self.children(parent) {
            declared += 1;
            let Some(outcome) = child.outcome.clone() else {
                continue;
            };
            if child.phase != OperationPhase::Terminal {
                continue;
            }
            let evaluation = evaluations.and_then(|values| values.get(slot)).cloned();
            let succeeded = matches!(outcome, Outcome::Succeeded(_));
            if succeeded && evaluation.is_none() {
                continue;
            }
            let qualified = succeeded
                && evaluation
                    .as_ref()
                    .is_some_and(|value| value.check.as_ref().is_none_or(|report| report.passed));
            attempts.push(AttemptEvidence {
                slot: slot.to_owned(),
                operation_id: child.spec.operation_id,
                outcome,
                evaluation,
                qualified,
            });
        }
        JudgeRequest {
            judge: plan.policy.judge.clone(),
            check: plan.policy.check.clone(),
            complete: declared == plan.attempts.len() && attempts.len() == declared,
            attempts,
        }
    }

    pub(crate) fn validate_speculation_child(&self, parent: &ParentLink) -> Result<()> {
        let Some(plan) = self.speculation_plan(parent.operation_id) else {
            return Ok(());
        };
        if !plan.attempts.contains_key(&parent.slot) {
            return Err(Error::Invalid(
                "speculation child slot is not in the plan".into(),
            ));
        }
        if self
            .speculation(parent.operation_id)
            .is_some_and(|state| state.verdict.is_some())
        {
            return Err(Error::Conflict(
                "decided speculation cannot accept attempts".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn apply_speculation(&mut self, event: SchedulerEvent) -> Result<()> {
        match event {
            SchedulerEvent::AttemptEvaluated {
                operation_id,
                slot,
                evaluation,
            } => self.apply_attempt_evaluated(operation_id, slot, *evaluation),
            SchedulerEvent::Judged {
                operation_id,
                expected_revision,
                verdict,
                judgment_digest,
                cancel,
            } => self.apply_judged(
                operation_id,
                expected_revision,
                verdict,
                judgment_digest,
                &cancel,
            ),
            SchedulerEvent::SpeculationSettled {
                operation_id,
                merged,
            } => self.apply_settled(operation_id, merged),
            _ => Err(Error::Invalid("not a speculation event".into())),
        }
    }

    fn apply_attempt_evaluated(
        &mut self,
        parent: OperationId,
        slot: String,
        evaluation: AttemptEvaluation,
    ) -> Result<()> {
        let plan = self
            .speculation_plan(parent)
            .ok_or_else(|| Error::Invalid("operation is not a speculation".into()))?;
        if !plan.attempts.contains_key(&slot) {
            return Err(Error::Invalid("speculation slot is not in the plan".into()));
        }
        if plan.policy.check.is_some() != evaluation.check.is_some() {
            return Err(Error::Invalid(
                "attempt check report must match the speculation policy".into(),
            ));
        }
        evaluation.generation.validate()?;
        if evaluation.diff.changed_paths.len() > MAXIMUM_SUMMARY_PATHS
            || evaluation
                .check
                .as_ref()
                .is_some_and(|report| report.output.len() > MAXIMUM_CHECK_OUTPUT_BYTES)
        {
            return Err(Error::Invalid(
                "attempt evaluation exceeds its bounds".into(),
            ));
        }
        let child = self
            .child_slots
            .get(&(parent, slot.clone()))
            .and_then(|child| self.operations.get(child))
            .ok_or_else(|| Error::NotFound(format!("speculation attempt {slot}")))?;
        if !matches!(child.outcome, Some(Outcome::Succeeded(_))) {
            return Err(Error::Conflict(
                "only a succeeded attempt can be evaluated".into(),
            ));
        }
        let state = self.speculations.entry(parent).or_default();
        if state.verdict.is_some() || state.evaluations.contains_key(&slot) {
            return Err(Error::Conflict("attempt is already evaluated".into()));
        }
        state.evaluations.insert(slot, evaluation);
        Ok(())
    }

    fn apply_judged(
        &mut self,
        parent: OperationId,
        expected_revision: u64,
        verdict: Verdict,
        judgment_digest: [u8; 32],
        cancel: &[OperationId],
    ) -> Result<()> {
        let state = self
            .operations
            .get(&parent)
            .ok_or_else(|| Error::NotFound(format!("operation {parent}")))?;
        if state.revision != expected_revision || state.cancellation_requested {
            return Err(Error::Conflict("stale speculation verdict".into()));
        }
        let OrchestrationDecision::Judge { request } = self.orchestration(parent) else {
            return Err(Error::Conflict("speculation is not ready to judge".into()));
        };
        if request.digest()? != judgment_digest {
            return Err(Error::Conflict(
                "verdict answers another judge request".into(),
            ));
        }
        if verdict.rationale.len() > MAXIMUM_RATIONALE_BYTES {
            return Err(Error::Invalid("verdict rationale is too long".into()));
        }
        let winner = match &verdict.winner {
            Some(slot) => Some(
                request
                    .qualified()
                    .find(|attempt| &attempt.slot == slot)
                    .ok_or_else(|| Error::Invalid("verdict winner did not qualify".into()))?
                    .operation_id,
            ),
            None if request.complete => None,
            None => {
                return Err(Error::Invalid(
                    "only a complete speculation may have no winner".into(),
                ));
            }
        };
        let losers = self
            .children(parent)
            .filter(|(_, child)| {
                child.phase != OperationPhase::Terminal && Some(child.spec.operation_id) != winner
            })
            .map(|(_, child)| child.spec.operation_id)
            .collect::<Vec<_>>();
        if losers.as_slice() != cancel {
            return Err(Error::Conflict(
                "verdict cancellation does not match running losers".into(),
            ));
        }
        for loser in losers {
            self.cancel_child(loser)?;
        }
        let node = self.mutable(parent)?;
        node.reservation = None;
        if winner.is_some() {
            node.phase = OperationPhase::Reconciling;
        } else {
            node.phase = OperationPhase::Terminal;
            node.outcome = Some(Outcome::Failed {
                message: format!("no speculative attempt was chosen: {}", verdict.rationale),
            });
            self.completion_order.push(parent);
        }
        let speculation = self.speculations.entry(parent).or_default();
        speculation.verdict = Some(verdict);
        speculation.judgment_digest = Some(judgment_digest);
        Ok(())
    }

    fn apply_settled(&mut self, parent: OperationId, merged: Option<GenerationRef>) -> Result<()> {
        let speculation = self
            .speculations
            .get(&parent)
            .ok_or_else(|| Error::NotFound(format!("speculation {parent}")))?;
        if speculation.settled {
            return Err(Error::Conflict("speculation is already settled".into()));
        }
        let node = self
            .operations
            .get(&parent)
            .ok_or_else(|| Error::NotFound(format!("operation {parent}")))?;
        let phase = node.phase;
        let Some(verdict) = speculation.verdict.as_ref() else {
            // A speculation cancelled before its verdict settles by discarding
            // every attempt workspace; nothing is merged.
            if phase == OperationPhase::Terminal
                && node.outcome == Some(Outcome::Cancelled)
                && merged.is_none()
            {
                self.speculations.entry(parent).or_default().settled = true;
                return Ok(());
            }
            return Err(Error::Conflict("speculation has no verdict".into()));
        };
        let outcome = match (&verdict.winner, &merged) {
            (Some(slot), Some(generation)) if phase == OperationPhase::Reconciling => {
                generation.validate()?;
                let value = self
                    .child_slots
                    .get(&(parent, slot.clone()))
                    .and_then(|child| self.operations.get(child))
                    .and_then(|child| match &child.outcome {
                        Some(Outcome::Succeeded(value)) => Some(value.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| Error::Conflict("winning attempt has no result".into()))?;
                Some(Outcome::Succeeded(json!({
                    "winner": slot,
                    "value": value,
                    "generation": generation,
                })))
            }
            (None, None) if phase == OperationPhase::Terminal => None,
            _ => {
                return Err(Error::Invalid(
                    "settlement does not match the speculation verdict".into(),
                ));
            }
        };
        if let Some(outcome) = outcome {
            let node = self.mutable(parent)?;
            node.phase = OperationPhase::Terminal;
            node.outcome = Some(outcome);
            self.completion_order.push(parent);
        }
        let speculation = self.speculations.entry(parent).or_default();
        speculation.settled = true;
        speculation.merged = merged;
        Ok(())
    }

    /// Checks a new declaration against user-set swarm limits.
    ///
    /// Limits are admission policy, not history: replay never re-checks them.
    pub fn admit_declaration(&self, spec: &OperationSpec, limits: &SwarmLimits) -> Result<()> {
        let width = usize::try_from(limits.max_width).unwrap_or(usize::MAX);
        if let Orchestration::Speculate { plan } = &spec.orchestration
            && plan.attempts.len() > width
        {
            return Err(Error::Conflict(format!(
                "speculation exceeds the width limit of {width}"
            )));
        }
        let Some(parent) = &spec.parent else {
            return Ok(());
        };
        if self.children(parent.operation_id).count() >= width {
            return Err(Error::Conflict(format!(
                "operation already has the width limit of {width} children"
            )));
        }
        let depth = self.child_depth(parent.operation_id);
        if depth > usize::try_from(limits.max_depth).unwrap_or(usize::MAX) {
            return Err(Error::Conflict(format!(
                "declaration at depth {depth} exceeds the depth limit of {}",
                limits.max_depth
            )));
        }
        Ok(())
    }

    /// Returns the number of operations currently admitted or running.
    #[must_use]
    pub fn running(&self) -> usize {
        self.operations
            .values()
            .filter(|state| {
                matches!(
                    state.phase,
                    OperationPhase::Admitted | OperationPhase::Running
                )
            })
            .count()
    }

    /// Whether every structured descendant of `operation_id` is terminal.
    #[cfg(feature = "host")]
    pub(crate) fn descendants_stopped(&self, operation_id: OperationId) -> bool {
        let mut pending = vec![operation_id];
        while let Some(current) = pending.pop() {
            for (_, child) in self.children(current) {
                if child.phase != OperationPhase::Terminal {
                    return false;
                }
                pending.push(child.spec.operation_id);
            }
        }
        true
    }

    /// Depth of a new child of `parent`: its ancestors, not counting speculation nodes.
    fn child_depth(&self, parent: OperationId) -> usize {
        let mut depth = 0;
        let mut current = Some(parent);
        while let Some(state) = current.and_then(|id| self.operations.get(&id)) {
            if !matches!(state.spec.orchestration, Orchestration::Speculate { .. }) {
                depth += 1;
            }
            current = state.spec.parent.as_ref().map(|link| link.operation_id);
        }
        depth
    }
}

#[cfg(feature = "host")]
pub use host::*;

#[cfg(feature = "host")]
mod host {
    use super::{
        AttemptEvaluation, AttemptWorkspace, CheckReport, DiffSummary, JudgeRequest, Verdict,
        fewest_changes, fewest_changes_entrypoint,
    };
    use crate::{
        Error, IdempotencyKey, OperationId, Outcome, Result,
        distributed::{CoordinatorApply, DistributedCoordinator},
        resources::{GenerationRef, WorkspaceRef},
        scheduler::{EntrypointRef, OperationPhase, OrchestrationDecision, SchedulerEvent},
        tool::ToolDefinition,
    };
    use acyclic_stream::StreamProvider;
    use futures::future::BoxFuture;
    use serde_json::{Value, json};

    /// Judge output.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum Judgment {
        /// Commit this verdict.
        Decided(Verdict),
        /// Wait for more attempts; only valid while the request is incomplete.
        Deferred,
    }

    /// Pluggable winner selection: an LLM judge, a shell check, or a deterministic chooser.
    pub trait Judge: Send + Sync {
        /// Exact judge identity pinned by the speculation policy.
        fn entrypoint(&self) -> &EntrypointRef;

        /// Chooses among the qualified attempts of one exact request.
        fn judge<'a>(&'a self, request: &'a JudgeRequest) -> BoxFuture<'a, Result<Judgment>>;
    }

    /// Deterministic [`Judge`] backed by [`fewest_changes`].
    pub struct FewestChangesJudge {
        entrypoint: EntrypointRef,
    }

    impl FewestChangesJudge {
        /// Creates the judge under [`fewest_changes_entrypoint`].
        #[must_use]
        pub fn new() -> Self {
            Self {
                entrypoint: fewest_changes_entrypoint(),
            }
        }
    }

    impl Default for FewestChangesJudge {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Judge for FewestChangesJudge {
        fn entrypoint(&self) -> &EntrypointRef {
            &self.entrypoint
        }

        fn judge<'a>(&'a self, request: &'a JudgeRequest) -> BoxFuture<'a, Result<Judgment>> {
            Box::pin(async move { Ok(Judgment::Decided(fewest_changes(request))) })
        }
    }

    /// Host-interpreted check run against a throwaway probe fork.
    pub trait AttemptCheck: Send + Sync {
        /// Runs `check` in `probe`; anything it writes is discarded with the probe.
        fn run<'a>(
            &'a self,
            check: &'a Value,
            probe: &'a WorkspaceRef,
        ) -> BoxFuture<'a, Result<CheckReport>>;
    }

    /// Workspace operations a speculation needs; the Filesystem adapter implements it.
    ///
    /// Implementations must reopen workspaces by reference on every call rather
    /// than caching handles, because merges advance heads underneath them.
    pub trait SpeculationWorkspaces: Send + Sync {
        /// Forks `source` at `generation` (or its head) under a name derived from `key`.
        fn fork<'a>(
            &'a self,
            source: &'a WorkspaceRef,
            generation: Option<&'a GenerationRef>,
            key: &'a IdempotencyKey,
        ) -> BoxFuture<'a, Result<AttemptWorkspace>>;

        /// Resolves a workspace's current head.
        fn head<'a>(&'a self, workspace: &'a WorkspaceRef) -> BoxFuture<'a, Result<GenerationRef>>;

        /// Summarizes changes from the attempt's base to `generation`.
        fn diff<'a>(
            &'a self,
            attempt: &'a AttemptWorkspace,
            generation: &'a GenerationRef,
        ) -> BoxFuture<'a, Result<DiffSummary>>;

        /// Idempotently merges the pinned attempt generation into `target`.
        /// A merge conflict is [`Error::Conflict`].
        fn merge<'a>(
            &'a self,
            attempt: &'a AttemptWorkspace,
            generation: &'a GenerationRef,
            target: &'a WorkspaceRef,
            key: &'a IdempotencyKey,
        ) -> BoxFuture<'a, Result<GenerationRef>>;

        /// Idempotently discards a workspace; an already discarded one succeeds.
        fn discard<'a>(
            &'a self,
            workspace: &'a WorkspaceRef,
            key: &'a IdempotencyKey,
        ) -> BoxFuture<'a, Result<()>>;
    }

    /// Model-visible definition of the speculation tool, bounded by `limits`.
    ///
    /// Hosts bind it to a [`crate::tool::ToolExecutor`] that builds a
    /// [`super::SpeculationRequest`] and drives the coordinator methods below.
    pub fn speculate_tool(limits: &super::SwarmLimits) -> Result<ToolDefinition> {
        let definition = ToolDefinition {
            name: "acyclic.speculate".into(),
            revision: "1".into(),
            description: "Run the same task several ways at once, each on its own fork of \
                          the workspace, and keep only the attempt the judge chooses."
                .into(),
            input_schema: json!({
                "type": "object",
                "required": ["task", "attempts"],
                "additionalProperties": false,
                "properties": {
                    "task": {"type": "string", "minLength": 1},
                    "attempts": {"type": "integer", "minimum": 1, "maximum": limits.max_width},
                    "approaches": {
                        "type": "array",
                        "maxItems": limits.max_width,
                        "items": {"type": "string"}
                    },
                    "check": {"type": "string"}
                }
            }),
            output_schema: json!({
                "type": "object",
                "required": ["winner", "rationale"],
                "properties": {
                    "winner": {"type": ["string", "null"]},
                    "rationale": {"type": "string"},
                    "value": {}
                }
            }),
        };
        definition.validate()?;
        Ok(definition)
    }

    fn step_key(key: &IdempotencyKey, step: &str) -> Result<IdempotencyKey> {
        let digest = blake3::hash(key.as_str().as_bytes()).to_hex();
        IdempotencyKey::new(format!(
            "speculation:{}:{step}",
            digest.get(..32).unwrap_or("")
        ))
    }

    impl<P: StreamProvider> DistributedCoordinator<P> {
        /// Declares a speculation node and all of its attempts, checking swarm limits
        /// for the whole request before anything is appended.
        pub async fn open_speculation(
            &mut self,
            request: &super::SpeculationRequest,
            idempotency_key: &IdempotencyKey,
        ) -> Result<CoordinatorApply> {
            let declarations = request.declarations()?;
            let mut projected = self.scheduler().clone();
            for spec in &declarations {
                if projected.operation(spec.operation_id).is_none() {
                    projected.admit_declaration(spec, &self.limits())?;
                    projected.apply(projected.declare(spec.clone())?)?;
                }
            }
            let mut result = CoordinatorApply::Replayed;
            for (index, spec) in declarations.into_iter().enumerate() {
                let applied = self
                    .apply_internal(
                        spec.operation_id,
                        step_key(idempotency_key, &format!("declare:{index}"))?,
                        SchedulerEvent::Declared {
                            spec: Box::new(spec),
                        },
                    )
                    .await?;
                if applied == CoordinatorApply::Applied {
                    result = CoordinatorApply::Applied;
                }
            }
            Ok(result)
        }

        /// Pins a succeeded attempt's generation, summarizes its diff, and runs the
        /// policy check in a throwaway grandchild fork that is always discarded.
        pub async fn evaluate_attempt(
            &mut self,
            parent: OperationId,
            slot: &str,
            workspaces: &dyn SpeculationWorkspaces,
            check: Option<&dyn AttemptCheck>,
            idempotency_key: &IdempotencyKey,
        ) -> Result<CoordinatorApply> {
            if self
                .scheduler()
                .speculation(parent)
                .is_some_and(|state| state.evaluations.contains_key(slot))
            {
                return Ok(CoordinatorApply::Replayed);
            }
            let plan = self
                .scheduler()
                .speculation_plan(parent)
                .ok_or_else(|| Error::Invalid("operation is not a speculation".into()))?;
            let attempt = plan
                .attempts
                .get(slot)
                .cloned()
                .ok_or_else(|| Error::NotFound(format!("speculation attempt {slot}")))?;
            let policy_check = plan.policy.check.clone();
            if !self.scheduler().children(parent).any(|(candidate, child)| {
                candidate == slot && matches!(child.outcome, Some(Outcome::Succeeded(_)))
            }) {
                return Err(Error::Conflict(
                    "only a succeeded attempt can be evaluated".into(),
                ));
            }
            let generation = workspaces.head(&attempt.workspace).await?;
            let diff = workspaces.diff(&attempt, &generation).await?;
            let check = match (&policy_check, check) {
                (None, _) => None,
                (Some(_), None) => {
                    return Err(Error::Unsupported(
                        "speculation policy requires an attempt check".into(),
                    ));
                }
                (Some(description), Some(runner)) => Some(
                    run_check(
                        workspaces,
                        runner,
                        description,
                        &attempt,
                        &generation,
                        idempotency_key,
                    )
                    .await?,
                ),
            };
            self.apply_internal(
                parent,
                step_key(idempotency_key, &format!("evaluate:{slot}"))?,
                SchedulerEvent::AttemptEvaluated {
                    operation_id: parent,
                    slot: slot.to_owned(),
                    evaluation: Box::new(AttemptEvaluation {
                        generation,
                        diff,
                        check,
                    }),
                },
            )
            .await
        }

        /// Consults the judge when the speculation is ready and commits its verdict,
        /// cancelling every loser still running. A committed verdict is returned
        /// without consulting the judge again.
        pub async fn judge_speculation(
            &mut self,
            parent: OperationId,
            judge: &dyn Judge,
            idempotency_key: &IdempotencyKey,
        ) -> Result<Option<Verdict>> {
            if let Some(verdict) = self
                .scheduler()
                .speculation(parent)
                .and_then(|state| state.verdict.clone())
            {
                return Ok(Some(verdict));
            }
            let expected_revision = self
                .scheduler()
                .operation(parent)
                .ok_or_else(|| Error::NotFound(format!("operation {parent}")))?
                .revision;
            let OrchestrationDecision::Judge { request } = self.scheduler().orchestration(parent)
            else {
                return Ok(None);
            };
            if judge.entrypoint() != &request.judge {
                return Err(Error::Unsupported(
                    "judge does not match the pinned speculation judge".into(),
                ));
            }
            let verdict = if request.complete && request.qualified().next().is_none() {
                Verdict {
                    winner: None,
                    rationale: "no attempt qualified".into(),
                }
            } else {
                match judge.judge(&request).await? {
                    Judgment::Decided(verdict) => verdict,
                    Judgment::Deferred if request.complete => {
                        return Err(Error::Invalid(
                            "judge must decide once every attempt settled".into(),
                        ));
                    }
                    Judgment::Deferred => return Ok(None),
                }
            };
            let winner = verdict.winner.as_deref();
            let cancel = self
                .scheduler()
                .children(parent)
                .filter(|(slot, child)| {
                    child.phase != OperationPhase::Terminal && Some(*slot) != winner
                })
                .map(|(_, child)| child.spec.operation_id)
                .collect();
            self.apply_internal(
                parent,
                step_key(idempotency_key, "judge")?,
                SchedulerEvent::Judged {
                    operation_id: parent,
                    expected_revision,
                    verdict: verdict.clone(),
                    judgment_digest: request.digest()?,
                    cancel,
                },
            )
            .await?;
            Ok(Some(verdict))
        }

        /// Discards every loser workspace, merges the winner into the target, records
        /// the settlement, and only then discards the merged fork. Returns the merged
        /// generation.
        ///
        /// The winner's fork outlives the merge until the settlement is durable, so a
        /// retry after a crash between the two can still reopen it and replay the
        /// idempotent merge. A settled retry only repeats the idempotent discard.
        ///
        /// A speculation cancelled before its verdict settles by discarding every
        /// attempt workspace. A merge conflict fails the speculation and leaves the
        /// winner's fork intact for inspection.
        ///
        /// No workspace is discarded while any attempt, or any nested operation under
        /// one, is still stopping, since its worker may still be using the attempt's
        /// fork: settlement returns [`Error::Conflict`] until the whole subtree is
        /// terminal. A Race or Quorum attempt can finish before its cancelled
        /// children stop.
        pub async fn settle_speculation(
            &mut self,
            parent: OperationId,
            workspaces: &dyn SpeculationWorkspaces,
            idempotency_key: &IdempotencyKey,
        ) -> Result<Option<GenerationRef>> {
            let state = self
                .scheduler()
                .speculation(parent)
                .cloned()
                .ok_or_else(|| Error::NotFound(format!("speculation {parent}")))?;
            let plan = self
                .scheduler()
                .speculation_plan(parent)
                .cloned()
                .ok_or_else(|| Error::Invalid("operation is not a speculation".into()))?;
            let winner = match &state.verdict {
                Some(verdict) => verdict.winner.clone(),
                None if self.scheduler().operation(parent).is_some_and(|node| {
                    node.phase == OperationPhase::Terminal
                        && node.outcome == Some(Outcome::Cancelled)
                }) =>
                {
                    None
                }
                None => return Err(Error::Conflict("speculation has no verdict".into())),
            };
            if !state.settled {
                if !self.scheduler().descendants_stopped(parent) {
                    return Err(Error::Conflict(
                        "speculation attempts are still stopping".into(),
                    ));
                }
                for (slot, attempt) in &plan.attempts {
                    if winner.as_ref() != Some(slot) {
                        workspaces
                            .discard(
                                &attempt.workspace,
                                &step_key(idempotency_key, &format!("discard:{slot}"))?,
                            )
                            .await?;
                    }
                }
                let merged = match &winner {
                    None => None,
                    Some(slot) => Some(
                        self.merge_winner(
                            parent,
                            slot,
                            &plan,
                            &state.evaluations,
                            workspaces,
                            idempotency_key,
                        )
                        .await?,
                    ),
                };
                self.apply_internal(
                    parent,
                    step_key(idempotency_key, "settle")?,
                    SchedulerEvent::SpeculationSettled {
                        operation_id: parent,
                        merged,
                    },
                )
                .await?;
            }
            if let Some(slot) = &winner
                && let Some(attempt) = plan.attempts.get(slot)
            {
                workspaces
                    .discard(
                        &attempt.workspace,
                        &step_key(idempotency_key, &format!("discard:{slot}"))?,
                    )
                    .await?;
            }
            Ok(self
                .scheduler()
                .speculation(parent)
                .and_then(|settled| settled.merged.clone()))
        }

        async fn merge_winner(
            &mut self,
            parent: OperationId,
            slot: &str,
            plan: &super::SpeculationPlan,
            evaluations: &std::collections::BTreeMap<String, AttemptEvaluation>,
            workspaces: &dyn SpeculationWorkspaces,
            idempotency_key: &IdempotencyKey,
        ) -> Result<GenerationRef> {
            let (Some(attempt), Some(evaluation)) =
                (plan.attempts.get(slot), evaluations.get(slot))
            else {
                return Err(Error::Conflict(
                    "winning attempt was never evaluated".into(),
                ));
            };
            let merge_key = step_key(idempotency_key, "merge")?;
            match workspaces
                .merge(attempt, &evaluation.generation, &plan.target, &merge_key)
                .await
            {
                Err(Error::Conflict(message)) => {
                    self.apply_internal(
                        parent,
                        step_key(idempotency_key, "merge-conflict")?,
                        SchedulerEvent::Completed {
                            operation_id: parent,
                            outcome: Outcome::Failed {
                                message: format!("winning attempt {slot} did not merge: {message}"),
                            },
                            fence: None,
                        },
                    )
                    .await?;
                    Err(Error::Conflict(message))
                }
                result => result,
            }
        }
    }

    async fn run_check(
        workspaces: &dyn SpeculationWorkspaces,
        runner: &dyn AttemptCheck,
        description: &Value,
        attempt: &AttemptWorkspace,
        generation: &GenerationRef,
        idempotency_key: &IdempotencyKey,
    ) -> Result<CheckReport> {
        let probe_key = step_key(idempotency_key, "probe")?;
        let probe = workspaces
            .fork(&attempt.workspace, Some(generation), &probe_key)
            .await?;
        let report = runner.run(description, &probe.workspace).await;
        workspaces
            .discard(
                &probe.workspace,
                &step_key(idempotency_key, "discard-probe")?,
            )
            .await?;
        report
    }
}

#[cfg(all(test, feature = "host"))]
mod tests {
    use super::*;
    use crate::{
        IdempotencyKey,
        core::{AggregateKind, Authority},
        distributed::{DistributedCoordinator, Worker},
        resources::ProviderRef,
        scheduler::{LeaseFence, ResourceSnapshot},
    };
    use acyclic_stream::{MemoryStream, StreamClient};
    use futures::future::BoxFuture;
    use std::sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    };

    /// Deterministic in-memory stand-in for a Filesystem deployment.
    #[derive(Default)]
    struct Recorded {
        changes: BTreeMap<Vec<u8>, Vec<String>>,
        passing: BTreeSet<Vec<u8>>,
        conflicting: BTreeSet<Vec<u8>>,
        probes: BTreeMap<Vec<u8>, Vec<u8>>,
        merges: Vec<(Vec<u8>, GenerationRef)>,
        discarded: Vec<Vec<u8>>,
        /// Workspaces whose next discard deletes them and then reports a crash.
        crash_after_discard: BTreeSet<Vec<u8>>,
    }

    struct MemoryWorkspaces(Mutex<Recorded>);

    impl MemoryWorkspaces {
        fn state(&self) -> Result<MutexGuard<'_, Recorded>> {
            self.0
                .lock()
                .map_err(|_| Error::Storage("fake state poisoned".into()))
        }
    }

    fn provider() -> Result<ProviderRef> {
        ProviderRef::new("example", "filesystem", "1")
    }

    fn generation(workspace: &[u8], suffix: &str) -> Result<GenerationRef> {
        let mut key = workspace.to_vec();
        key.extend_from_slice(suffix.as_bytes());
        GenerationRef::new(provider()?, key, None)
    }

    fn workspace(name: &str) -> Result<AttemptWorkspace> {
        Ok(AttemptWorkspace {
            workspace: WorkspaceRef::new(provider()?, name.as_bytes(), None)?,
            base: generation(name.as_bytes(), "@base")?,
        })
    }

    impl SpeculationWorkspaces for MemoryWorkspaces {
        fn fork<'a>(
            &'a self,
            source: &'a WorkspaceRef,
            _: Option<&'a GenerationRef>,
            key: &'a IdempotencyKey,
        ) -> BoxFuture<'a, Result<AttemptWorkspace>> {
            Box::pin(async move {
                let fork = workspace(&format!("probe-{}", key.as_str()))?;
                self.state()?.probes.insert(
                    fork.workspace.as_resource().key().to_vec(),
                    source.as_resource().key().to_vec(),
                );
                Ok(fork)
            })
        }

        fn head<'a>(&'a self, workspace: &'a WorkspaceRef) -> BoxFuture<'a, Result<GenerationRef>> {
            Box::pin(async move { generation(workspace.as_resource().key(), "@head") })
        }

        fn diff<'a>(
            &'a self,
            attempt: &'a AttemptWorkspace,
            _: &'a GenerationRef,
        ) -> BoxFuture<'a, Result<DiffSummary>> {
            Box::pin(async move {
                let key = attempt.workspace.as_resource().key();
                Ok(DiffSummary {
                    changed_paths: self.state()?.changes.get(key).cloned().unwrap_or_default(),
                    truncated: false,
                })
            })
        }

        fn merge<'a>(
            &'a self,
            attempt: &'a AttemptWorkspace,
            pinned: &'a GenerationRef,
            target: &'a WorkspaceRef,
            _: &'a IdempotencyKey,
        ) -> BoxFuture<'a, Result<GenerationRef>> {
            Box::pin(async move {
                let key = attempt.workspace.as_resource().key().to_vec();
                let mut state = self.state()?;
                // Merges reopen the attempt fork, so a discarded fork cannot merge.
                if state.discarded.contains(&key) {
                    return Err(Error::NotFound("attempt workspace was discarded".into()));
                }
                if state.conflicting.contains(&key) {
                    return Err(Error::Conflict("both sides changed /x".into()));
                }
                state.merges.push((key, pinned.clone()));
                generation(target.as_resource().key(), "@merged")
            })
        }

        fn discard<'a>(
            &'a self,
            workspace: &'a WorkspaceRef,
            _: &'a IdempotencyKey,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                let key = workspace.as_resource().key().to_vec();
                let mut state = self.state()?;
                state.discarded.push(key.clone());
                if state.crash_after_discard.remove(&key) {
                    return Err(Error::Storage("process stopped after the discard".into()));
                }
                Ok(())
            })
        }
    }

    impl AttemptCheck for MemoryWorkspaces {
        fn run<'a>(
            &'a self,
            _: &'a Value,
            probe: &'a WorkspaceRef,
        ) -> BoxFuture<'a, Result<CheckReport>> {
            Box::pin(async move {
                let state = self.state()?;
                let source = state
                    .probes
                    .get(probe.as_resource().key())
                    .ok_or_else(|| Error::Invalid("check must run in a probe fork".into()))?;
                Ok(CheckReport::new(
                    state.passing.contains(source),
                    "ran tests",
                ))
            })
        }
    }

    struct CountingJudge {
        inner: FewestChangesJudge,
        calls: AtomicUsize,
    }

    impl Judge for CountingJudge {
        fn entrypoint(&self) -> &EntrypointRef {
            self.inner.entrypoint()
        }

        fn judge<'a>(&'a self, request: &'a JudgeRequest) -> BoxFuture<'a, Result<Judgment>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.judge(request)
        }
    }

    fn counting_judge() -> CountingJudge {
        CountingJudge {
            inner: FewestChangesJudge::new(),
            calls: AtomicUsize::new(0),
        }
    }

    const NODE: OperationId = OperationId::from_bytes([10; 16]);

    fn attempt_id(index: u8) -> OperationId {
        OperationId::from_bytes([11 + index; 16])
    }

    fn key(value: &str) -> Result<IdempotencyKey> {
        IdempotencyKey::new(value)
    }

    fn entrypoint(name: &str) -> EntrypointRef {
        EntrypointRef {
            name: name.into(),
            version: "1".into(),
            digest: [1; 32],
            result_schema: json!({}),
        }
    }

    fn owner() -> DurableOwner {
        DurableOwner::Attached {
            authority: Authority {
                kind: AggregateKind::Task,
                id: "owner".into(),
            },
        }
    }

    fn request(slots: &[&str], check: bool, timing: JudgeTiming) -> Result<SpeculationRequest> {
        let mut attempts = Vec::new();
        for (index, slot) in (0_u8..).zip(slots) {
            attempts.push(AttemptSpec {
                slot: (*slot).to_owned(),
                operation_id: attempt_id(index),
                workspace: workspace(slot)?,
                entrypoint: entrypoint("example.agent"),
                resources: ResourceRequest::default(),
                placement: Value::Null,
                orchestration: Orchestration::Leaf,
                state: json!({"task": "fix the retry bug", "approach": slot}),
            });
        }
        Ok(SpeculationRequest {
            operation_id: NODE,
            parent: None,
            owner: owner(),
            policy: SpeculationPolicy {
                judge: fewest_changes_entrypoint(),
                check: check.then(|| json!({"shell": "cargo test"})),
                timing,
                losers: LoserPolicy::CancelOnDecision,
            },
            target: WorkspaceRef::new(provider()?, b"root".to_vec(), None)?,
            attempts,
        })
    }

    fn workspaces(changes: &[(&str, usize)], passing: &[&str]) -> MemoryWorkspaces {
        let mut recorded = Recorded::default();
        for (slot, count) in changes {
            recorded.changes.insert(
                slot.as_bytes().to_vec(),
                (0..*count)
                    .map(|index| format!("/{slot}/{index}"))
                    .collect(),
            );
        }
        recorded.passing = passing
            .iter()
            .map(|slot| slot.as_bytes().to_vec())
            .collect();
        MemoryWorkspaces(Mutex::new(recorded))
    }

    fn worker() -> Worker {
        Worker {
            id: "worker".into(),
            available: ResourceSnapshot::default(),
        }
    }

    /// Admits and starts up to `limit` attempts, returning their fences.
    async fn start<P: acyclic_stream::StreamProvider>(
        coordinator: &mut DistributedCoordinator<P>,
        limit: usize,
    ) -> Result<BTreeMap<OperationId, LeaseFence>> {
        let mut fences = BTreeMap::new();
        while fences.len() < limit
            && let Some(lease) = coordinator.pull(&worker()).await?
        {
            let id = lease.operation.operation_id;
            let fence = LeaseFence::from(&lease.reservation);
            coordinator
                .apply(
                    id,
                    key(&format!("start-{id}"))?,
                    SchedulerEvent::Started {
                        operation_id: id,
                        fence: fence.clone(),
                    },
                )
                .await?;
            fences.insert(id, fence);
        }
        Ok(fences)
    }

    async fn finish<P: acyclic_stream::StreamProvider>(
        coordinator: &mut DistributedCoordinator<P>,
        fences: &BTreeMap<OperationId, LeaseFence>,
        outcomes: &[(OperationId, Outcome<Value>)],
    ) -> Result<()> {
        for (id, outcome) in outcomes {
            coordinator
                .apply(
                    *id,
                    key(&format!("complete-{id}"))?,
                    SchedulerEvent::Completed {
                        operation_id: *id,
                        outcome: outcome.clone(),
                        fence: fences.get(id).cloned(),
                    },
                )
                .await?;
        }
        Ok(())
    }

    fn failed() -> Outcome<Value> {
        Outcome::Failed {
            message: "attempt crashed".into(),
        }
    }

    async fn open(
        client: &StreamClient<MemoryStream>,
        limits: SwarmLimits,
    ) -> Result<DistributedCoordinator<MemoryStream>> {
        Ok(DistributedCoordinator::open(client)
            .await?
            .with_limits(limits))
    }

    #[tokio::test]
    async fn winner_is_merged_and_losers_are_cancelled_and_discarded() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator = open(&client, SwarmLimits::DOGFOOD).await?;
        let request = request(&["a", "b", "c"], true, JudgeTiming::AllSettled)?;
        coordinator
            .open_speculation(&request, &key("speculate")?)
            .await?;
        let fences = start(&mut coordinator, 3).await?;
        finish(
            &mut coordinator,
            &fences,
            &[
                (attempt_id(0), Outcome::Succeeded(json!("a done"))),
                (attempt_id(1), Outcome::Succeeded(json!("b done"))),
                (attempt_id(2), failed()),
            ],
        )
        .await?;
        // `b` changed fewer paths, but its check failed, so only `a` qualifies.
        let fake = workspaces(&[("a", 2), ("b", 1)], &["a"]);
        for slot in ["a", "b"] {
            coordinator
                .evaluate_attempt(
                    NODE,
                    slot,
                    &fake,
                    Some(&fake),
                    &key(&format!("eval-{slot}"))?,
                )
                .await?;
        }
        let verdict = coordinator
            .judge_speculation(NODE, &FewestChangesJudge::new(), &key("judge")?)
            .await?;
        assert_eq!(verdict.and_then(|value| value.winner), Some("a".into()));
        let merged = coordinator
            .settle_speculation(NODE, &fake, &key("settle")?)
            .await?
            .ok_or_else(|| Error::NotFound("merged generation".into()))?;

        let recorded = fake.state()?;
        assert_eq!(
            recorded.merges,
            vec![(b"a".to_vec(), generation(b"a", "@head")?)]
        );
        for discarded in [&b"a"[..], b"b", b"c"] {
            assert!(recorded.discarded.iter().any(|value| value == discarded));
        }
        assert!(
            recorded
                .probes
                .keys()
                .all(|probe| recorded.discarded.contains(probe)),
            "every check probe fork is discarded"
        );
        let node = coordinator
            .scheduler()
            .operation(NODE)
            .ok_or_else(|| Error::NotFound("node".into()))?;
        assert_eq!(
            node.outcome,
            Some(Outcome::Succeeded(
                json!({"winner": "a", "value": "a done", "generation": merged})
            ))
        );
        assert!(
            coordinator
                .scheduler()
                .speculation(NODE)
                .is_some_and(|state| state.settled)
        );
        Ok(())
    }

    #[tokio::test]
    async fn early_verdict_cancels_running_and_unadmitted_losers() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let limits = SwarmLimits {
            max_running: 2,
            ..SwarmLimits::DOGFOOD
        };
        let mut coordinator = open(&client, limits).await?;
        let request = request(&["a", "b", "c"], false, JudgeTiming::EachSettled)?;
        coordinator
            .open_speculation(&request, &key("speculate")?)
            .await?;
        let fences = start(&mut coordinator, 3).await?;
        assert_eq!(fences.len(), 2, "running limit admits only two attempts");
        finish(
            &mut coordinator,
            &fences,
            &[(attempt_id(0), Outcome::Succeeded(json!("a done")))],
        )
        .await?;
        let fake = workspaces(&[("a", 1)], &[]);
        coordinator
            .evaluate_attempt(NODE, "a", &fake, None, &key("eval-a")?)
            .await?;
        let verdict = coordinator
            .judge_speculation(NODE, &FewestChangesJudge::new(), &key("judge")?)
            .await?;
        assert_eq!(verdict.and_then(|value| value.winner), Some("a".into()));
        let scheduler = coordinator.scheduler();
        assert!(
            scheduler
                .operation(attempt_id(1))
                .is_some_and(|state| state.cancellation_requested && state.outcome.is_none())
        );
        assert_eq!(
            scheduler
                .operation(attempt_id(2))
                .and_then(|state| state.outcome.clone()),
            Some(Outcome::Cancelled)
        );
        assert!(coordinator.pull(&worker()).await?.is_none());
        assert!(matches!(
            coordinator
                .settle_speculation(NODE, &fake, &key("settle")?)
                .await,
            Err(Error::Conflict(_))
        ));
        assert!(
            fake.state()?.discarded.is_empty(),
            "a loser still stopping keeps its fork"
        );
        let running = attempt_id(1);
        finish(&mut coordinator, &fences, &[(running, Outcome::Cancelled)]).await?;
        assert!(
            coordinator
                .settle_speculation(NODE, &fake, &key("settle")?)
                .await?
                .is_some()
        );
        Ok(())
    }

    #[tokio::test]
    async fn nothing_merges_when_no_attempt_qualifies() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator = open(&client, SwarmLimits::DOGFOOD).await?;
        let request = request(&["a", "b"], true, JudgeTiming::AllSettled)?;
        coordinator
            .open_speculation(&request, &key("speculate")?)
            .await?;
        let fences = start(&mut coordinator, 2).await?;
        finish(
            &mut coordinator,
            &fences,
            &[
                (attempt_id(0), Outcome::Succeeded(json!("a done"))),
                (attempt_id(1), failed()),
            ],
        )
        .await?;
        let fake = workspaces(&[("a", 1)], &[]);
        coordinator
            .evaluate_attempt(NODE, "a", &fake, Some(&fake), &key("eval-a")?)
            .await?;
        let judge = counting_judge();
        let verdict = coordinator
            .judge_speculation(NODE, &judge, &key("judge")?)
            .await?;
        assert_eq!(verdict.map(|value| value.winner), Some(None));
        assert_eq!(judge.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            coordinator
                .settle_speculation(NODE, &fake, &key("settle")?)
                .await?,
            None
        );
        let recorded = fake.state()?;
        assert!(recorded.merges.is_empty());
        assert!(recorded.discarded.iter().any(|value| value == b"a"));
        assert!(recorded.discarded.iter().any(|value| value == b"b"));
        assert!(matches!(
            coordinator
                .scheduler()
                .operation(NODE)
                .and_then(|state| state.outcome.clone()),
            Some(Outcome::Failed { .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn restart_reproduces_the_outcome_without_judging_again() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator = open(&client, SwarmLimits::DOGFOOD).await?;
        let request = request(&["a", "b"], false, JudgeTiming::AllSettled)?;
        coordinator
            .open_speculation(&request, &key("speculate")?)
            .await?;
        let fences = start(&mut coordinator, 2).await?;
        finish(
            &mut coordinator,
            &fences,
            &[
                (attempt_id(0), Outcome::Succeeded(json!("a done"))),
                (attempt_id(1), Outcome::Succeeded(json!("b done"))),
            ],
        )
        .await?;
        let fake = workspaces(&[("a", 3), ("b", 1)], &[]);
        for slot in ["a", "b"] {
            coordinator
                .evaluate_attempt(NODE, slot, &fake, None, &key(&format!("eval-{slot}"))?)
                .await?;
        }
        let judge = counting_judge();
        let verdict = coordinator
            .judge_speculation(NODE, &judge, &key("judge")?)
            .await?;
        assert_eq!(
            verdict.as_ref().and_then(|value| value.winner.clone()),
            Some("b".into())
        );

        // Crash after the verdict: a fresh coordinator replays it and settles.
        let mut restarted = open(&client, SwarmLimits::DOGFOOD).await?;
        assert_eq!(restarted.scheduler(), coordinator.scheduler());
        assert_eq!(
            restarted
                .judge_speculation(NODE, &judge, &key("judge")?)
                .await?,
            verdict
        );
        assert_eq!(judge.calls.load(Ordering::SeqCst), 1);
        restarted
            .open_speculation(&request, &key("speculate")?)
            .await?;
        let merged = restarted
            .settle_speculation(NODE, &fake, &key("settle")?)
            .await?;
        let reopened = open(&client, SwarmLimits::DOGFOOD).await?;
        assert_eq!(reopened.scheduler(), restarted.scheduler());
        assert_eq!(
            reopened
                .scheduler()
                .speculation(NODE)
                .and_then(|state| state.merged.clone()),
            merged
        );
        Ok(())
    }

    /// Runs two attempts to success and commits a verdict for `a`.
    async fn decide_for_a(
        coordinator: &mut DistributedCoordinator<MemoryStream>,
        fake: &MemoryWorkspaces,
    ) -> Result<()> {
        let request = request(&["a", "b"], false, JudgeTiming::AllSettled)?;
        coordinator
            .open_speculation(&request, &key("speculate")?)
            .await?;
        let fences = start(coordinator, 2).await?;
        finish(
            coordinator,
            &fences,
            &[
                (attempt_id(0), Outcome::Succeeded(json!("a done"))),
                (attempt_id(1), Outcome::Succeeded(json!("b done"))),
            ],
        )
        .await?;
        for slot in ["a", "b"] {
            coordinator
                .evaluate_attempt(NODE, slot, fake, None, &key(&format!("eval-{slot}"))?)
                .await?;
        }
        let verdict = coordinator
            .judge_speculation(NODE, &FewestChangesJudge::new(), &key("judge")?)
            .await?;
        assert_eq!(verdict.and_then(|value| value.winner), Some("a".into()));
        Ok(())
    }

    #[tokio::test]
    async fn settle_retry_after_a_crash_following_the_winner_discard_still_settles() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator = open(&client, SwarmLimits::DOGFOOD).await?;
        let fake = workspaces(&[("a", 1), ("b", 2)], &[]);
        decide_for_a(&mut coordinator, &fake).await?;
        fake.state()?.crash_after_discard.insert(b"a".to_vec());

        // The process stops right after the winner's fork is deleted.
        assert!(matches!(
            coordinator
                .settle_speculation(NODE, &fake, &key("settle")?)
                .await,
            Err(Error::Storage(_))
        ));
        let mut restarted = open(&client, SwarmLimits::DOGFOOD).await?;
        let merged = restarted
            .settle_speculation(NODE, &fake, &key("settle")?)
            .await?;
        assert!(merged.is_some());
        let state = restarted
            .scheduler()
            .speculation(NODE)
            .ok_or_else(|| Error::NotFound("speculation".into()))?;
        assert!(state.settled);
        assert_eq!(state.merged, merged);
        assert!(matches!(
            restarted
                .scheduler()
                .operation(NODE)
                .and_then(|node| node.outcome.clone()),
            Some(Outcome::Succeeded(_))
        ));
        assert_eq!(fake.state()?.merges.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn generic_completion_cannot_strand_a_committed_verdict() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator = open(&client, SwarmLimits::DOGFOOD).await?;
        let fake = workspaces(&[("a", 1), ("b", 2)], &[]);
        decide_for_a(&mut coordinator, &fake).await?;
        for (index, outcome) in [
            failed(),
            Outcome::Indeterminate { operation_id: NODE },
            Outcome::Cancelled,
        ]
        .into_iter()
        .enumerate()
        {
            assert!(matches!(
                coordinator
                    .apply(
                        NODE,
                        key(&format!("bypass-{index}"))?,
                        SchedulerEvent::Completed {
                            operation_id: NODE,
                            outcome,
                            fence: None,
                        },
                    )
                    .await,
                Err(Error::Unauthorized(_))
            ));
        }
        let merged = coordinator
            .settle_speculation(NODE, &fake, &key("settle")?)
            .await?;
        assert!(merged.is_some());
        assert!(matches!(
            coordinator
                .scheduler()
                .operation(NODE)
                .and_then(|node| node.outcome.clone()),
            Some(Outcome::Succeeded(_))
        ));
        Ok(())
    }

    /// Declares a race whose direct child wins while a speculation under it is
    /// still undecided, commits the race decision, and returns the leases.
    async fn race_past_a_speculation(
        coordinator: &mut DistributedCoordinator<MemoryStream>,
    ) -> Result<BTreeMap<OperationId, LeaseFence>> {
        let root = OperationId::from_bytes([1; 16]);
        let sibling = OperationId::from_bytes([2; 16]);
        let declare = |operation_id, parent, orchestration| OperationSpec {
            operation_id,
            parent,
            owner: owner(),
            entrypoint: entrypoint("example.agent"),
            dependencies: BTreeSet::new(),
            resources: ResourceRequest::default(),
            placement: Value::Null,
            orchestration,
            state: Value::Null,
        };
        for (id, spec) in [
            (root, declare(root, None, Orchestration::Race)),
            (
                sibling,
                declare(
                    sibling,
                    Some(ParentLink {
                        operation_id: root,
                        slot: "direct".into(),
                    }),
                    Orchestration::Leaf,
                ),
            ),
        ] {
            let event = coordinator.scheduler().declare(spec)?;
            coordinator
                .apply(id, key(&format!("declare-{id}"))?, event)
                .await?;
        }
        let mut speculation = request(&["a", "b"], false, JudgeTiming::AllSettled)?;
        speculation.parent = Some(ParentLink {
            operation_id: root,
            slot: "speculate".into(),
        });
        coordinator
            .open_speculation(&speculation, &key("speculate")?)
            .await?;
        let fences = start(coordinator, 4).await?;
        let root_fence = fences
            .get(&root)
            .cloned()
            .ok_or_else(|| Error::NotFound("root lease".into()))?;
        coordinator
            .apply(
                root,
                key("root-wait")?,
                SchedulerEvent::WaitingForChildren {
                    operation_id: root,
                    fence: root_fence,
                },
            )
            .await?;
        finish(
            coordinator,
            &fences,
            &[(sibling, Outcome::Succeeded(json!("direct done")))],
        )
        .await?;
        let OrchestrationDecision::Complete { outcome, cancel } =
            coordinator.scheduler().orchestration(root)
        else {
            return Err(Error::Conflict("race should be decided".into()));
        };
        assert_eq!(cancel, vec![NODE]);
        let expected_revision = coordinator
            .scheduler()
            .operation(root)
            .map_or(0, |state| state.revision);
        coordinator
            .apply(
                root,
                key("race")?,
                SchedulerEvent::Orchestrated {
                    operation_id: root,
                    expected_revision,
                    outcome,
                    cancel,
                    reducer: None,
                    reduction_digest: None,
                },
            )
            .await?;
        Ok(fences)
    }

    #[tokio::test]
    async fn race_parent_cancelling_an_undecided_speculation_cancels_its_attempts() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator = open(&client, SwarmLimits::DOGFOOD).await?;
        let fences = race_past_a_speculation(&mut coordinator).await?;

        let scheduler = coordinator.scheduler();
        assert_eq!(
            scheduler
                .operation(NODE)
                .and_then(|state| state.outcome.clone()),
            Some(Outcome::Cancelled)
        );
        for index in 0..2 {
            assert!(
                scheduler
                    .operation(attempt_id(index))
                    .is_some_and(|state| state.cancellation_requested),
                "every running attempt is asked to cancel"
            );
        }
        assert_eq!(
            coordinator
                .judge_speculation(NODE, &FewestChangesJudge::new(), &key("judge")?)
                .await?,
            None
        );
        let fake = workspaces(&[], &[]);
        assert!(matches!(
            coordinator
                .settle_speculation(NODE, &fake, &key("settle")?)
                .await,
            Err(Error::Conflict(_))
        ));
        assert!(
            fake.state()?.discarded.is_empty(),
            "no fork is discarded while its attempt may still be running"
        );
        finish(
            &mut coordinator,
            &fences,
            &[
                (attempt_id(0), Outcome::Cancelled),
                (attempt_id(1), Outcome::Cancelled),
            ],
        )
        .await?;
        assert_eq!(
            coordinator
                .settle_speculation(NODE, &fake, &key("settle")?)
                .await?,
            None
        );
        {
            let recorded = fake.state()?;
            assert!(recorded.merges.is_empty());
            for discarded in [&b"a"[..], b"b"] {
                assert!(recorded.discarded.iter().any(|value| value == discarded));
            }
        }
        assert!(
            coordinator
                .scheduler()
                .speculation(NODE)
                .is_some_and(|state| state.settled)
        );
        let reopened = open(&client, SwarmLimits::DOGFOOD).await?;
        assert_eq!(reopened.scheduler(), coordinator.scheduler());
        Ok(())
    }

    /// Runs attempt `a` as a race whose direct child wins while its sibling is
    /// still running, fails attempt `b`, and commits the verdict for `a`.
    async fn decide_for_a_racing_race(
        coordinator: &mut DistributedCoordinator<MemoryStream>,
        fake: &MemoryWorkspaces,
    ) -> Result<(OperationId, BTreeMap<OperationId, LeaseFence>)> {
        let mut request = request(&["a", "b"], false, JudgeTiming::AllSettled)?;
        if let Some(first) = request.attempts.first_mut() {
            first.orchestration = Orchestration::Race;
        }
        coordinator
            .open_speculation(&request, &key("speculate")?)
            .await?;
        let nested = [
            OperationId::from_bytes([30; 16]),
            OperationId::from_bytes([31; 16]),
        ];
        for (id, slot) in nested.into_iter().zip(["x", "y"]) {
            let event = coordinator.scheduler().declare(OperationSpec {
                operation_id: id,
                parent: Some(ParentLink {
                    operation_id: attempt_id(0),
                    slot: slot.into(),
                }),
                owner: owner(),
                entrypoint: entrypoint("example.agent"),
                dependencies: BTreeSet::new(),
                resources: ResourceRequest::default(),
                placement: Value::Null,
                orchestration: Orchestration::Leaf,
                state: Value::Null,
            })?;
            coordinator
                .apply(id, key(&format!("declare-{id}"))?, event)
                .await?;
        }
        let fences = start(coordinator, 4).await?;
        let race = attempt_id(0);
        let fence = fences
            .get(&race)
            .cloned()
            .ok_or_else(|| Error::NotFound("race lease".into()))?;
        coordinator
            .apply(
                race,
                key("race-wait")?,
                SchedulerEvent::WaitingForChildren {
                    operation_id: race,
                    fence,
                },
            )
            .await?;
        let [winner, straggler] = nested;
        finish(
            coordinator,
            &fences,
            &[
                (winner, Outcome::Succeeded(json!("x done"))),
                (attempt_id(1), failed()),
            ],
        )
        .await?;
        let OrchestrationDecision::Complete { outcome, cancel } =
            coordinator.scheduler().orchestration(race)
        else {
            return Err(Error::Conflict("race should be decided".into()));
        };
        assert_eq!(cancel, vec![straggler]);
        let expected_revision = coordinator
            .scheduler()
            .operation(race)
            .map_or(0, |state| state.revision);
        coordinator
            .apply(
                race,
                key("race")?,
                SchedulerEvent::Orchestrated {
                    operation_id: race,
                    expected_revision,
                    outcome,
                    cancel,
                    reducer: None,
                    reduction_digest: None,
                },
            )
            .await?;
        coordinator
            .evaluate_attempt(NODE, "a", fake, None, &key("eval-a")?)
            .await?;
        let verdict = coordinator
            .judge_speculation(NODE, &FewestChangesJudge::new(), &key("judge")?)
            .await?;
        assert_eq!(verdict.and_then(|value| value.winner), Some("a".into()));
        Ok((straggler, fences))
    }

    #[tokio::test]
    async fn settlement_waits_for_nested_workers_of_a_finished_attempt() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator = open(&client, SwarmLimits::DOGFOOD).await?;
        let fake = workspaces(&[("a", 1)], &[]);
        let (straggler, fences) = decide_for_a_racing_race(&mut coordinator, &fake).await?;
        assert!(
            coordinator
                .scheduler()
                .operation(straggler)
                .is_some_and(|state| state.cancellation_requested && state.outcome.is_none()),
            "the race attempt finished while its nested worker is still stopping"
        );
        assert!(matches!(
            coordinator
                .settle_speculation(NODE, &fake, &key("settle")?)
                .await,
            Err(Error::Conflict(_))
        ));
        {
            let recorded = fake.state()?;
            assert!(recorded.discarded.is_empty() && recorded.merges.is_empty());
        }
        finish(
            &mut coordinator,
            &fences,
            &[(straggler, Outcome::Cancelled)],
        )
        .await?;
        assert!(
            coordinator
                .settle_speculation(NODE, &fake, &key("settle")?)
                .await?
                .is_some()
        );
        Ok(())
    }

    #[tokio::test]
    async fn swarm_limits_bound_width_and_depth() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let limits = SwarmLimits {
            max_width: 2,
            max_depth: 1,
            max_running: 64,
        };
        let mut coordinator = open(&client, limits).await?;
        let wide = request(&["a", "b", "c"], false, JudgeTiming::AllSettled)?;
        assert!(matches!(
            coordinator.open_speculation(&wide, &key("wide")?).await,
            Err(Error::Conflict(_))
        ));
        assert!(coordinator.scheduler().operation(NODE).is_none());

        // A speculation node takes no depth level: its attempts sit at depth 1.
        let root = OperationId::from_bytes([1; 16]);
        let root_spec = OperationSpec {
            operation_id: root,
            parent: None,
            owner: owner(),
            entrypoint: entrypoint("example.agent"),
            dependencies: BTreeSet::new(),
            resources: ResourceRequest::default(),
            placement: Value::Null,
            orchestration: Orchestration::Join,
            state: Value::Null,
        };
        coordinator
            .apply(
                root,
                key("root")?,
                coordinator.scheduler().declare(root_spec.clone())?,
            )
            .await?;
        let mut nested = request(&["a", "b"], false, JudgeTiming::AllSettled)?;
        nested.parent = Some(ParentLink {
            operation_id: root,
            slot: "speculate".into(),
        });
        coordinator
            .open_speculation(&nested, &key("nested")?)
            .await?;
        let mut too_deep = root_spec;
        too_deep.operation_id = OperationId::from_bytes([2; 16]);
        too_deep.parent = Some(ParentLink {
            operation_id: attempt_id(0),
            slot: "child".into(),
        });
        assert!(matches!(
            coordinator
                .scheduler()
                .admit_declaration(&too_deep, &limits),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn speculation_steps_cannot_bypass_the_judge() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator = open(&client, SwarmLimits::DOGFOOD).await?;
        let request = request(&["a", "b"], false, JudgeTiming::AllSettled)?;
        coordinator
            .open_speculation(&request, &key("speculate")?)
            .await?;
        let fences = start(&mut coordinator, 2).await?;
        finish(
            &mut coordinator,
            &fences,
            &[
                (attempt_id(0), Outcome::Succeeded(json!("a done"))),
                (attempt_id(1), failed()),
            ],
        )
        .await?;
        let fake = workspaces(&[("a", 1)], &[]);
        coordinator
            .evaluate_attempt(NODE, "a", &fake, None, &key("eval-a")?)
            .await?;
        let OrchestrationDecision::Judge { request } = coordinator.scheduler().orchestration(NODE)
        else {
            return Err(Error::Conflict("speculation should be ready".into()));
        };
        let forged = |winner: &str| SchedulerEvent::Judged {
            operation_id: NODE,
            expected_revision: coordinator
                .scheduler()
                .operation(NODE)
                .map_or(0, |state| state.revision),
            verdict: Verdict {
                winner: Some(winner.into()),
                rationale: "forged".into(),
            },
            judgment_digest: request.digest().unwrap_or_default(),
            cancel: Vec::new(),
        };
        let unqualified = forged("b");
        let mut projected = coordinator.scheduler().clone();
        assert!(matches!(
            projected.apply(unqualified),
            Err(Error::Invalid(_))
        ));
        let bypass = forged("a");
        assert!(matches!(
            coordinator.apply(NODE, key("forged")?, bypass).await,
            Err(Error::Unauthorized(_))
        ));
        Ok(())
    }

    #[test]
    fn fewest_changes_prefers_the_earliest_slot_on_ties_and_bounds_output() -> Result<()> {
        let evidence = |slot: &str, paths: usize| -> Result<AttemptEvidence> {
            Ok(AttemptEvidence {
                slot: slot.into(),
                operation_id: OperationId::from_bytes([1; 16]),
                outcome: Outcome::Succeeded(Value::Null),
                evaluation: Some(AttemptEvaluation {
                    generation: generation(slot.as_bytes(), "@head")?,
                    diff: DiffSummary {
                        changed_paths: vec!["/x".into(); paths],
                        truncated: false,
                    },
                    check: None,
                }),
                qualified: true,
            })
        };
        let request = JudgeRequest {
            judge: fewest_changes_entrypoint(),
            check: None,
            complete: true,
            attempts: vec![evidence("a", 2)?, evidence("b", 1)?, evidence("c", 1)?],
        };
        assert_eq!(fewest_changes(&request).winner, Some("b".into()));
        let long = "é".repeat(MAXIMUM_CHECK_OUTPUT_BYTES);
        let report = CheckReport::new(false, &long);
        assert!(report.output.len() <= MAXIMUM_CHECK_OUTPUT_BYTES);
        assert!(report.output.chars().all(|character| character == 'é'));
        Ok(())
    }
}

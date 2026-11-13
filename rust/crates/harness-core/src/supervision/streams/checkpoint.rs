//! A source-owned fold checkpoint, committed with the observation it derives.
use super::*;
use crate::supervision::Action;
use serde::{Deserialize, Serialize};

const KEY: &[u8] = b"state";

/// Fixed-size durable query cut. No source history, command or model output is
/// copied here. The action remains latched until an owner publishes a resume.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    /// Checkpoint format, currently two. Older formats are refused.
    pub version: u32,
    /// Original source establishing this cut.
    pub source: Position,
    /// Original publication establishing the outstanding decision. Later
    /// output updates the fold cut without changing the cause of a pause.
    pub decision_source: Option<Position>,
    /// Incremental query state through that source.
    pub state: State,
    /// Strongest outstanding policy decision.
    pub action: Action,
    /// Signal responsible for the outstanding decision, if any.
    pub signal: Option<Kind>,
}

/// One original source's conditional contribution. It is never submitted by
/// the query: the source owner joins it to its original atomic publication.
pub struct Projection {
    run: crate::OperationId,
    /// Candidate durable decision.
    pub checkpoint: Checkpoint,
    /// Query counters, signals and checkpoint sharing the original commit.
    pub effect: CommitEffect,
}

/// One original dispatch admission. Its fence joins the dispatch's hard
/// publication, so a concurrent decision cannot be missed between read/write.
pub struct Admission {
    /// Original checkpoint condition, including absence for a new run.
    pub condition: CommitCondition,
    /// Outstanding owner decision; present means no new dispatch is admitted.
    pub control: Option<crate::supervision::Control>,
}

impl Projection {
    /// Append a lifecycle event to this source's already captured signal cut.
    /// The original producer joins this projection to its own publication;
    /// no second head observation can give the two records different fences.
    pub fn append_event(&mut self, event: &Event) -> Result<()> {
        let path = path(&event.agent)?;
        if event.version != 1
            || event.run != Some(self.run)
            || event.source != self.checkpoint.source
            || !self.effect.conditions.iter().any(|condition| {
                condition.path() == &path
                    && matches!(
                        condition,
                        CommitCondition::Tail { .. } | CommitCondition::Absent { .. }
                    )
            })
        {
            return Err(invalid("lifecycle event differs from its captured source"));
        }
        let records = self
            .effect
            .mutations
            .iter_mut()
            .find_map(|mutation| match mutation {
                CommitMutation::Append {
                    path: target,
                    records,
                } if target == &path => Some(records),
                _ => None,
            })
            .ok_or_else(|| invalid("lifecycle event has no captured signal append"))?;
        records.push(lifecycle_record(event)?);
        Ok(())
    }
}

impl Checkpoint {
    /// Original outstanding decision, independent of later activity cuts.
    pub fn control(&self) -> Option<crate::supervision::Control> {
        self.decision_source
            .as_ref()
            .map(|source| crate::supervision::Control {
                source: source.clone(),
                action: self.action,
                signal: self.signal,
            })
    }

    fn dispatch_control(&self, policy: &Policy) -> Result<Option<crate::supervision::Control>> {
        if let Some(control) = self.control() {
            return Ok(Some(control));
        }
        // Resume clears a decision, never the user's consumed capacity. Use
        // the retained cut rather than inventing a new, unpublished source.
        let checked = self
            .state
            .apply(
                self.state.through,
                &Observation::PolicyChanged,
                None,
                policy,
            )
            .map_err(invalid)?;
        Ok(
            (checked.action != Action::Alert).then(|| crate::supervision::Control {
                source: self.source.clone(),
                action: checked.action,
                signal: Some(Kind::Spend),
            }),
        )
    }
}

impl<P: StreamProvider + ?Sized> Queries<'_, P> {
    fn checkpoint_path(&self) -> Result<StreamPath> {
        StreamPath::new(format!(
            "{}/supervision/runs/{}/checkpoint",
            self.agent, self.run
        ))
        .map_err(Error::from)
    }

    async fn checkpoint_record(&self) -> Result<Option<(Checkpoint, u64)>> {
        let record = match self
            .provider
            .latest(self.checkpoint_path()?, KEY.into(), Default::default())
            .await
        {
            Ok(record) => record,
            Err(StreamError::NotFound) => None,
            Err(error) => return Err(error.into()),
        };
        record
            .map(|record| {
                let checkpoint: Checkpoint =
                    serde_json::from_slice(&record.value).map_err(invalid)?;
                let through = record
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| invalid("query checkpoint overflows"))?;
                if checkpoint.version != 2
                    || checkpoint.state.version != 2
                    || checkpoint.state.through != through
                    || (checkpoint.action == Action::Alert) != checkpoint.decision_source.is_none()
                {
                    return Err(invalid("query checkpoint differs from its original cut"));
                }
                Ok((checkpoint, through))
            })
            .transpose()
    }

    /// Select the latest decision with one point lookup, never a source scan.
    pub async fn checkpoint(&self) -> Result<Option<Checkpoint>> {
        Ok(self.checkpoint_record().await?.map(|(value, _)| value))
    }

    /// Capture an admission fence with one point lookup. It is never independently
    /// submitted: the original dispatch producer owns the conditional result.
    pub async fn admission(&self, policy: &Policy) -> Result<Admission> {
        let previous = self.checkpoint_record().await?;
        Ok(Admission {
            control: previous
                .as_ref()
                .map(|(checkpoint, _)| checkpoint.dispatch_control(policy))
                .transpose()?
                .flatten(),
            condition: condition(
                self.checkpoint_path()?,
                previous.map(|(_, through)| through),
            ),
        })
    }

    /// Clear an outstanding pause with the owner's original control source.
    /// Accounting and signature cells survive resume; a terminal stop stays
    /// terminal. The owner MUST join this effect to its admission/actor change
    /// and source checkpoint, discarding it on a concurrent-cut refusal.
    pub async fn resume(&self, source: &Position) -> Result<Option<CommitEffect>> {
        let Some((mut checkpoint, through)) = self.checkpoint_record().await? else {
            return Ok(None);
        };
        if checkpoint.action != Action::Pause {
            return Ok(None);
        }
        checkpoint.source = source.clone();
        checkpoint.decision_source = None;
        checkpoint.action = Action::Alert;
        checkpoint.signal = None;
        checkpoint.state.through = through
            .checked_add(1)
            .ok_or_else(|| invalid("resumed query checkpoint overflows"))?;
        let path = self.checkpoint_path()?;
        Ok(Some(CommitEffect::hard(
            vec![condition(path.clone(), Some(through))],
            vec![CommitMutation::Append {
                path,
                records: vec![AppendRecord::latest(
                    serde_json::to_vec(&checkpoint).map_err(invalid)?,
                    KEY,
                    false,
                )?],
            }],
        )))
    }

    /// Prepare an observation after the source owner's exact-retry check.
    /// Concurrent source producers fence the same checkpoint. A loser must
    /// discard this candidate along with its original source publication.
    pub async fn project(
        &self,
        source: &Position,
        observation: &Observation<'_>,
        policy: &Policy,
    ) -> Result<Projection> {
        let previous = self.checkpoint_record().await?;
        if matches!(observation, Observation::ToolStarted { .. })
            && let Some(control) = previous
                .as_ref()
                .map(|(checkpoint, _)| checkpoint.dispatch_control(policy))
                .transpose()?
                .flatten()
        {
            return Err(Error::ExecutionControlled(control));
        }
        let state = previous
            .as_ref()
            .map(|(checkpoint, _)| checkpoint.state.clone())
            .unwrap_or_default();
        let prepared = self
            .prepare(&state, state.through, source, observation, policy)
            .await?;
        let prior_action = previous
            .as_ref()
            .map(|(checkpoint, _)| checkpoint.action)
            .unwrap_or_default();
        let action = prior_action.max(prepared.transition.action);
        let signal = if prepared.transition.action > prior_action {
            prepared
                .transition
                .signals
                .iter()
                .find(|signal| policy.action(signal) == action)
                .map(|signal| signal.kind)
        } else {
            previous
                .as_ref()
                .and_then(|(checkpoint, _)| checkpoint.signal)
        };
        let checkpoint = Checkpoint {
            version: 2,
            source: source.clone(),
            decision_source: if prepared.transition.action > prior_action {
                Some(source.clone())
            } else {
                previous
                    .as_ref()
                    .and_then(|(checkpoint, _)| checkpoint.decision_source.clone())
            },
            state: prepared.transition.state,
            action,
            signal,
        };
        let path = self.checkpoint_path()?;
        let record = AppendRecord::latest(
            serde_json::to_vec(&checkpoint).map_err(invalid)?,
            KEY,
            false,
        )?;
        let mut effect = prepared
            .effect
            .unwrap_or_else(|| CommitEffect::hard(Vec::new(), Vec::new()));
        effect.conditions.push(condition(
            path.clone(),
            previous.map(|(_, through)| through),
        ));
        effect.mutations.push(CommitMutation::Append {
            path,
            records: vec![record],
        });
        Ok(Projection {
            run: self.run,
            checkpoint,
            effect,
        })
    }
}

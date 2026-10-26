//! Recoverable native-host mailbox transitions over the ordinary Stream commit.
use super::*;
use crate::{OperationId, Outcome, TaskId, executor::TurnOutput};

/// An exact, durable execution claim, safe to retain across a host restart.
/// Claims are issued by `TaskVolume::claim_child_turn`, never constructed by
/// an adapter. Deserialized claims are authenticated against the durable index
/// before either replay or completion.
///
/// ```compile_fail
/// use acyclic_harness_core::{task_volume::{ChildTurnClaim, Component}, TaskId, OperationId};
/// let claim = ChildTurnClaim {
///     reference: Component::new("worker").unwrap(),
///     task: TaskId::from_bytes([1; 16]), turn: 1,
///     operation: OperationId::from_bytes([2; 16]),
/// };
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildTurnClaim {
    /// Direct child in the owning parent's private volume.
    pub(super) reference: Component,
    /// Original admitted child task.
    pub(super) task: TaskId,
    /// Gapless accepted input sequence.
    pub(super) turn: u64,
    /// Fences a later claimant and identifies replay of this claim.
    pub(super) operation: OperationId,
}

impl ChildTurnClaim {
    /// Direct child selected by the winning claim.
    pub fn reference(&self) -> &Component {
        &self.reference
    }

    /// Original admitted child task.
    pub const fn task(&self) -> TaskId {
        self.task
    }

    /// Gapless accepted input sequence.
    pub const fn turn(&self) -> u64 {
        self.turn
    }

    /// Exact operation fence, retained unchanged across native host recovery.
    pub const fn operation(&self) -> OperationId {
        self.operation
    }
}

/// Existing owner conditions and budget for one exact task-volume transition.
pub struct ChildTurnPublication<'a> {
    /// Generation observed while resolving this direct child.
    pub expected: Generation,
    /// Existing execution-owner fences, committed with the task-volume change.
    pub conditions: Vec<CommitCondition>,
    /// Existing authority deadline, if any.
    pub deadline: Option<u64>,
    /// Original native invocation's allocation and work custody.
    pub work: NativeWork<'a>,
}

/// A claim replay cannot accidentally execute an already settled turn.
#[derive(Clone, Debug)]
pub enum ChildTurn {
    /// The exact input owned by the supplied claim operation.
    Claimed {
        /// Exact operation fence for this execution.
        claim: ChildTurnClaim,
        /// One original accepted input, granted only by the winning commit.
        input: FileValue,
    },
    /// An existing claim is an observation, never another execution grant.
    /// Recover it only after the native host proves that its previous process
    /// custody has stopped; no input is returned for accidental redelivery.
    InFlight {
        /// Existing operation to observe or recover after proving process custody.
        claim: ChildTurnClaim,
    },
    /// The original operation already completed; return its immutable outcome.
    Settled {
        /// Original immutable execution identity.
        claim: ChildTurnClaim,
        /// Its retained terminal outcome.
        result: ChildTurnResult,
    },
}

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    /// Atomically claims one ordered input and acknowledges delivery. The host
    /// supplies its existing execution-owner conditions, rechecked by the same
    /// commit as the volume cut. No process may start before this returns.
    pub async fn claim_child_turn(
        &self,
        reference: &Component,
        task: TaskId,
        operation: OperationId,
        publication: ChildTurnPublication<'_>,
    ) -> Result<Option<ChildTurn>> {
        let expected = publication.expected;
        let deadline = publication.deadline;
        let work = publication.work;
        self.refresh().await?;
        let index = self.index.lock().await;
        if let Some(claim) = index.child_claims.get(&operation) {
            if claim.reference != *reference || claim.task != task {
                return Err(Error::FenceConflict);
            }
            return Self::claimed_turn(&index, claim).map(Some);
        }
        if self.generation(&index) != expected {
            return Err(Error::Conflict(self.generation(&index)));
        }
        let key = |file| FileKey::Child {
            reference: reference.clone(),
            file,
        };
        if !index.files.contains_key(&key(ChildFile::Request)) {
            return Err(Error::InvalidSegment);
        }
        if child_state(&index, reference)? != ChildState::Running {
            return Ok(None);
        }
        if let Some(previous) = index.files.get(&key(ChildFile::Status)) {
            let previous = ChildStatus::parse(&previous.file.bytes)?;
            if previous.task != task || !previous.settled {
                return Err(Error::FenceConflict);
            }
        }
        let delivered = match index.files.get(&key(ChildFile::Delivered)) {
            Some(previous) => {
                let (owner, turn): (TaskId, u64) = serde_json::from_slice(&previous.file.bytes)
                    .map_err(|_| Error::InvalidSegment)?;
                if owner != task {
                    return Err(Error::FenceConflict);
                }
                turn
            }
            None => 0,
        };
        let Some(input) = index
            .child_inputs
            .get(reference)
            .and_then(|inputs| usize::try_from(delivered).ok().and_then(|n| inputs.get(n)))
            .and_then(|key| index.files.get(key))
        else {
            return Ok(None);
        };
        let input = input.file.clone();
        let turn = delivered.checked_add(1).ok_or(Error::LimitExceeded)?;
        if delivered > 0 && !index.files.contains_key(&key(ChildFile::Result(delivered))) {
            return Err(Error::FenceConflict);
        }
        let claim = ChildTurnClaim {
            reference: reference.clone(),
            task,
            turn,
            operation,
        };
        let mut status = FileValue::new(
            key(ChildFile::Status),
            segment::encode_json(
                &ChildStatus {
                    task,
                    state: ChildState::Running,
                    settled: false,
                    inputs: turn,
                },
                publication.work,
            )?,
        );
        status.publication = Some(operation);
        let mut delivered = FileValue::new(
            key(ChildFile::Delivered),
            segment::encode_json(&(task, turn), publication.work)?,
        );
        delivered.publication = Some(operation);
        let prepared = self.prepare_child_transition(
            &index,
            publication,
            &[status, delivered],
            &claim,
            b"claim",
        )?;
        drop(index);
        self.commit_append(expected, prepared, deadline, work)
            .await?;
        Ok(Some(ChildTurn::Claimed { claim, input }))
    }

    /// Atomically publishes the outcome and quiescent status of an exact claim.
    /// The native adapter must first stop/reap its process tree and capture its
    /// filesystem effects. Orphan recovery requires that same custody proof.
    pub async fn complete_child_turn(
        &self,
        claim: &ChildTurnClaim,
        outcome: Outcome<TurnOutput>,
        publication: ChildTurnPublication<'_>,
    ) -> Result<Generation> {
        let expected = publication.expected;
        let deadline = publication.deadline;
        let work = publication.work;
        self.refresh().await?;
        let index = self.index.lock().await;
        if index.child_claims.get(&claim.operation) != Some(claim) {
            return Err(Error::FenceConflict);
        }
        let key = |file| FileKey::Child {
            reference: claim.reference.clone(),
            file,
        };
        let result = ChildTurnResult {
            task: claim.task,
            turn: claim.turn,
            outcome,
        };
        result.validate()?;
        if let Some(previous) = index.files.get(&key(ChildFile::Result(claim.turn))) {
            if previous.file.publication != Some(claim.operation)
                || ChildTurnResult::parse(&previous.file.bytes)? != result
            {
                return Err(Error::ImmutableConflict(previous.file.key.clone()));
            }
            return Ok(self.generation(&index));
        }
        if self.generation(&index) != expected {
            return Err(Error::Conflict(self.generation(&index)));
        }
        let status = index
            .files
            .get(&key(ChildFile::Status))
            .ok_or(Error::FenceConflict)?;
        let current = ChildStatus::parse(&status.file.bytes)?;
        let delivered = index
            .files
            .get(&key(ChildFile::Delivered))
            .ok_or(Error::FenceConflict)?;
        let delivered: (TaskId, u64) =
            serde_json::from_slice(&delivered.file.bytes).map_err(|_| Error::InvalidSegment)?;
        if status.file.publication != Some(claim.operation)
            || current.settled
            || current.task != claim.task
            || current.inputs != claim.turn
            || delivered != (claim.task, claim.turn)
        {
            return Err(Error::FenceConflict);
        }
        let mut result = FileValue::new(
            key(ChildFile::Result(claim.turn)),
            segment::encode_json(&result, publication.work)?,
        );
        result.publication = Some(claim.operation);
        let mut status = FileValue::new(
            key(ChildFile::Status),
            segment::encode_json(
                &ChildStatus {
                    state: child_state(&index, &claim.reference)?,
                    settled: true,
                    ..current
                },
                publication.work,
            )?,
        );
        status.publication = Some(claim.operation);
        let prepared = self.prepare_child_transition(
            &index,
            publication,
            &[result, status],
            claim,
            b"complete",
        )?;
        drop(index);
        self.commit_append(expected, prepared, deadline, work).await
    }

    fn claimed_turn(index: &Index, claim: &ChildTurnClaim) -> Result<ChildTurn> {
        if let Some(result) = index.files.get(&FileKey::Child {
            reference: claim.reference.clone(),
            file: ChildFile::Result(claim.turn),
        }) {
            if result.file.publication != Some(claim.operation) {
                return Err(Error::FenceConflict);
            }
            return Ok(ChildTurn::Settled {
                claim: claim.clone(),
                result: ChildTurnResult::parse(&result.file.bytes)?,
            });
        }
        let status = index
            .files
            .get(&FileKey::Child {
                reference: claim.reference.clone(),
                file: ChildFile::Status,
            })
            .ok_or(Error::FenceConflict)?;
        let current = ChildStatus::parse(&status.file.bytes)?;
        if status.file.publication != Some(claim.operation)
            || current.settled
            || current.task != claim.task
            || current.inputs != claim.turn
        {
            return Err(Error::FenceConflict);
        }
        Ok(ChildTurn::InFlight {
            claim: claim.clone(),
        })
    }

    fn prepare_child_transition(
        &self,
        index: &Index,
        publication: ChildTurnPublication<'_>,
        writes: &[FileValue; 2],
        claim: &ChildTurnClaim,
        phase: &[u8],
    ) -> Result<PreparedAppend> {
        let ChildTurnPublication {
            expected,
            conditions,
            work,
            ..
        } = publication;
        for write in writes {
            validate_file(index, write)?;
        }
        let (record, files) = segment::encode_pair(writes, work)?;
        let next = expected
            .position
            .checked_add(1)
            .ok_or(Error::LimitExceeded)?;
        let fence = CommitCondition::Head {
            path: self.path.clone(),
            expected: expected.position,
            commit_id: CommitId::from_bytes(expected.commit),
        };
        let mut identity = blake3::Hasher::new();
        identity.update(b"harness:child-turn:v1");
        identity.update(&expected.volume);
        identity.update(&claim.operation.into_bytes());
        identity.update(phase);
        identity.update(&expected.commit);
        Ok(PreparedAppend {
            conditions,
            fence,
            record,
            files,
            next,
            key: *identity.finalize().as_bytes(),
        })
    }
}

pub(super) fn child_state(index: &Index, reference: &Component) -> Result<ChildState> {
    Ok(child_lifecycle(index, reference)?.0)
}

pub(super) fn child_lifecycle(
    index: &Index,
    reference: &Component,
) -> Result<(ChildState, Option<Control>)> {
    let key = FileKey::Child {
        reference: reference.clone(),
        file: ChildFile::Control,
    };
    let control = index
        .files
        .get(&key)
        .map(|file| Control::parse(&file.file.bytes))
        .transpose()?;
    Ok((
        control.map_or(ChildState::Running, |control| {
            ChildState::Running.apply(control)
        }),
        control,
    ))
}

#[cfg(test)]
mod tests;

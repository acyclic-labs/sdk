use super::*;
use crate::wire_codec::CodecQuota;
use physical::{add, push, slots};

/// Planned structured-concurrency result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OrchestrationDecision {
    /// Await more observations.
    Wait,
    /// Commit an outcome and cancel remaining losers.
    Complete {
        /// Derived parent outcome.
        outcome: Outcome<FileRef>,
        /// Nonterminal losers.
        cancel: Vec<OperationId>,
    },
    /// Materialize the ordered successful inputs.
    Assemble {
        /// Aggregate encoding.
        assembly: AssemblyKind,
        /// Ordered child values.
        values: Vec<(String, FileRef)>,
        /// Nonterminal quorum losers.
        cancel: Vec<OperationId>,
    },
    /// Invoke a pinned reducer with child-slot ordered inputs.
    Reduce {
        /// Reducer contract.
        reducer: EntrypointRef,
        /// Ordered inputs.
        values: Vec<(String, FileRef)>,
    },
}
/// Encoding of a materialized aggregate.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssemblyKind {
    /// Preserve named child slots.
    Join,
    /// Preserve successful completion order.
    Quorum,
}
impl OrchestrationDecision {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let (bytes, values) = match self {
            Self::Wait => return Ok(0),
            Self::Complete { outcome, cancel } => {
                return add(
                    outcome.logical_owned_bytes()?,
                    slots::<OperationId>(cancel.capacity())?,
                );
            }
            Self::Assemble { values, cancel, .. } => (
                add(
                    slots::<(String, FileRef)>(values.capacity())?,
                    slots::<OperationId>(cancel.capacity())?,
                )?,
                values,
            ),
            Self::Reduce { reducer, values } => (
                add(
                    reducer.logical_owned_bytes()?,
                    slots::<(String, FileRef)>(values.capacity())?,
                )?,
                values,
            ),
        };
        values.iter().try_fold(bytes, |bytes, (slot, file)| {
            add(
                add(bytes, slot.capacity() as u64)?,
                file.logical_owned_bytes()?,
            )
        })
    }
}
/// Canonical identity of an exact reducer and ordered inputs.
pub fn reduction_invocation_digest(
    reducer: &EntrypointRef,
    values: &[(String, FileRef)],
) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(&(reducer, values), u64::MAX, &mut None)
}
/// Canonical identity of an aggregate encoding and ordered inputs.
pub fn assembly_invocation_digest(
    assembly: AssemblyKind,
    values: &[(String, FileRef)],
) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(&(assembly, values), u64::MAX, &mut None)
}

#[derive(Clone, Copy)]
enum Plan<'a> {
    Wait,
    Assemble(AssemblyKind, usize),
    Reduce(&'a EntrypointRef),
    Succeeded(&'a FileRef),
    Failed(&'a str),
    ChildFailed(&'a str, &'a str),
    Cancelled,
    Indeterminate(OperationId),
}
impl Plan<'_> {
    fn matches(self, actual: &Outcome<FileRef>) -> bool {
        match (self, actual) {
            (Self::Succeeded(a), Outcome::Succeeded(b)) => a == b,
            (Self::Failed(a), Outcome::Failed { message }) => a == message,
            (Self::ChildFailed(slot, expected), Outcome::Failed { message }) => b"child "
                .iter()
                .chain(slot.as_bytes())
                .chain(b" failed: ")
                .chain(expected.as_bytes())
                .copied()
                .eq(message.bytes()),
            (Self::Cancelled, Outcome::Cancelled) => true,
            (Self::Indeterminate(a), Outcome::Indeterminate { operation_id }) => a == *operation_id,
            _ => false,
        }
    }
    fn copy(self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Outcome<FileRef>> {
        Ok(match self {
            Self::Succeeded(file) => Outcome::Succeeded(file.copy(work)?),
            Self::Failed(message) => Outcome::Failed {
                message: text_copy(message, work)?,
            },
            Self::Cancelled => Outcome::Cancelled,
            Self::Indeterminate(operation_id) => Outcome::Indeterminate { operation_id },
            Self::ChildFailed(slot, message) => Outcome::Failed {
                message: crate::contract::preparation_format(
                    format_args!("child {slot} failed: {message}"),
                    work,
                )?,
            },
            _ => unreachable!("only terminal plans produce outcomes"),
        })
    }
}

impl Scheduler {
    /// Plans orchestration using the optional work observer.
    pub fn orchestration(
        &self,
        parent: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<OrchestrationDecision> {
        let plan = self.inspect_orchestration(parent, work)?;
        let mut quota = CodecQuota::new(work, 0)?;
        let mut values = Vec::new();
        let mut cancel = Vec::new();
        let mut value_slots = 0;
        let mut cancel_slots = 0;
        let result = (|| {
            match plan {
                Plan::Wait => return Ok(OrchestrationDecision::Wait),
                Plan::Assemble(AssemblyKind::Quorum, count) => {
                    for (index, file) in self.successful_completions(parent).take(count).enumerate()
                    {
                        let mut digits = [0; 20];
                        let slot = text_copy(decimal_slot(index, &mut digits), quota.work())?;
                        quota.adopt(slot.capacity() as u64)?;
                        let file = file.clone_in_quota(&mut quota)?;
                        push(&mut values, (slot, file), &mut value_slots, quota.work())?;
                    }
                }
                Plan::Assemble(..) | Plan::Reduce(_) => {
                    for (slot, child) in self.children(parent) {
                        let Some(Outcome::Succeeded(file)) = &child.outcome else {
                            continue;
                        };
                        let slot = text_copy(slot, quota.work())?;
                        quota.adopt(slot.capacity() as u64)?;
                        let file = file.clone_in_quota(&mut quota)?;
                        push(&mut values, (slot, file), &mut value_slots, quota.work())?;
                    }
                }
                _ => {}
            }
            if matches!(
                plan,
                Plan::Succeeded(_) | Plan::Assemble(AssemblyKind::Quorum, _)
            ) {
                for (_, child) in self.children(parent) {
                    if child.phase != OperationPhase::Terminal {
                        push(
                            &mut cancel,
                            child.spec.operation_id,
                            &mut cancel_slots,
                            quota.work(),
                        )?;
                    }
                }
            }
            Ok(match plan {
                Plan::Wait => unreachable!("wait returned before construction"),
                outcome @ (Plan::Succeeded(_)
                | Plan::Failed(_)
                | Plan::ChildFailed(..)
                | Plan::Cancelled
                | Plan::Indeterminate(_)) => {
                    let outcome = outcome.copy(quota.work())?;
                    quota.adopt(outcome.logical_owned_bytes()?)?;
                    OrchestrationDecision::Complete {
                        outcome,
                        cancel: std::mem::take(&mut cancel),
                    }
                }
                Plan::Assemble(assembly, _) => OrchestrationDecision::Assemble {
                    assembly,
                    values: std::mem::take(&mut values),
                    cancel: std::mem::take(&mut cancel),
                },
                Plan::Reduce(reducer) => {
                    let reducer = reducer.clone_with_work(quota.work())?;
                    quota.adopt(reducer.logical_owned_bytes()?)?;
                    OrchestrationDecision::Reduce {
                        reducer,
                        values: std::mem::take(&mut values),
                    }
                }
            })
        })();
        if let Err(error) = result {
            drop(values);
            drop(cancel);
            if let Some(io) = quota.work().as_deref_mut() {
                let _ = io.release_owned(value_slots + cancel_slots);
            }
            return Err(error);
        }
        quota.adopt(value_slots + cancel_slots)?;
        quota.transfer();
        result
    }
    fn successful_completions(&self, parent: OperationId) -> impl Iterator<Item = &FileRef> {
        self.completion_order.iter().filter_map(move |id| {
            self.children(parent)
                .find(|(_, child)| child.spec.operation_id == *id)
                .and_then(|(_, child)| match &child.outcome {
                    Some(Outcome::Succeeded(file)) => Some(file),
                    _ => None,
                })
        })
    }
    fn inspect_orchestration(
        &self,
        parent: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Plan<'_>> {
        observe(work)?;
        let Some(state) = self.operation(parent) else {
            return Ok(Plan::Wait);
        };
        if state.phase != OperationPhase::WaitingForChildren || state.cancellation_requested {
            return Ok(Plan::Wait);
        }
        let mut count = 0;
        let mut terminal = true;
        let mut successful = true;
        let mut possible = 0;
        for (_, child) in self.children(parent) {
            observe(work)?;
            count += 1;
            let success = matches!(child.outcome, Some(Outcome::Succeeded(_)));
            terminal &= child.phase == OperationPhase::Terminal;
            successful &= success;
            possible += usize::from(child.phase != OperationPhase::Terminal || success);
        }
        if count == 0 {
            return Ok(Plan::Wait);
        }
        match &state.spec.orchestration {
            Orchestration::Join if terminal => self.inspect_ordered(parent, work),
            Orchestration::Reduce { reducer } if terminal && successful => {
                Ok(Plan::Reduce(reducer))
            }
            Orchestration::Reduce { .. } if terminal => self.inspect_ordered(parent, work),
            Orchestration::Race | Orchestration::Quorum { .. } => {
                let mut successes = 0;
                for completed in &self.completion_order {
                    observe(work)?;
                    for (_, child) in self.children(parent) {
                        observe(work)?;
                        if child.spec.operation_id == *completed {
                            if let Some(Outcome::Succeeded(file)) = &child.outcome {
                                if matches!(state.spec.orchestration, Orchestration::Race) {
                                    return Ok(Plan::Succeeded(file));
                                }
                                successes += 1;
                                if let Orchestration::Quorum { required } = state.spec.orchestration
                                    && successes >= required as usize
                                {
                                    return Ok(Plan::Assemble(
                                        AssemblyKind::Quorum,
                                        required as usize,
                                    ));
                                }
                            }
                            break;
                        }
                    }
                }
                match state.spec.orchestration {
                    Orchestration::Race if terminal => Ok(Plan::Failed("every raced child failed")),
                    Orchestration::Quorum { required } if successes >= required as usize => {
                        Ok(Plan::Assemble(AssemblyKind::Quorum, required as usize))
                    }
                    Orchestration::Quorum { required } if possible < required as usize => {
                        Ok(Plan::Failed("quorum is no longer reachable"))
                    }
                    _ => Ok(Plan::Wait),
                }
            }
            _ => Ok(Plan::Wait),
        }
    }
    fn inspect_ordered(
        &self,
        parent: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Plan<'_>> {
        let mut count = 0;
        for (slot, child) in self.children(parent) {
            observe(work)?;
            match &child.outcome {
                Some(Outcome::Succeeded(_)) => count += 1,
                Some(Outcome::Failed { message }) => {
                    return Ok(Plan::ChildFailed(slot, message));
                }
                Some(Outcome::Cancelled) => return Ok(Plan::Cancelled),
                Some(Outcome::Indeterminate { .. }) => {
                    return Ok(Plan::Indeterminate(parent));
                }
                None => return Ok(Plan::Wait),
            }
        }
        Ok(Plan::Assemble(AssemblyKind::Join, count))
    }
    pub(super) fn matches_orchestration(
        &self,
        parent: OperationId,
        outcome: &Outcome<FileRef>,
        cancel: &[OperationId],
        reducer: Option<&EntrypointRef>,
        digest: Option<[u8; 32]>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        let plan = self.inspect_orchestration(parent, work)?;
        let cancel_matches = match plan {
            Plan::Succeeded(_) | Plan::Assemble(AssemblyKind::Quorum, _) => self
                .children(parent)
                .filter(|(_, child)| child.phase != OperationPhase::Terminal)
                .map(|(_, child)| child.spec.operation_id)
                .eq(cancel.iter().copied()),
            _ => cancel.is_empty(),
        };
        if !cancel_matches {
            return Ok(false);
        }
        match plan {
            Plan::Wait => Ok(false),
            expected @ (Plan::Succeeded(_)
            | Plan::Failed(_)
            | Plan::ChildFailed(..)
            | Plan::Cancelled
            | Plan::Indeterminate(_)) => {
                if matches!(expected, Plan::ChildFailed(..)) {
                    observe(work)?;
                }
                observe(work)?;
                Ok(expected.matches(outcome) && reducer.is_none() && digest.is_none())
            }
            Plan::Assemble(assembly, count) => {
                if !matches!(outcome, Outcome::Succeeded(_)) || reducer.is_some() {
                    return Ok(false);
                }
                Ok(digest
                    == Some(crate::contract::canonical_json_digest(
                        &(
                            assembly,
                            Values {
                                scheduler: self,
                                parent,
                                count,
                            },
                        ),
                        acyclic_stream::MAX_RECORD_BYTES as u64,
                        work,
                    )?))
            }
            Plan::Reduce(expected) => {
                if reducer != Some(expected) || matches!(outcome, Outcome::Indeterminate { .. }) {
                    return Ok(false);
                }
                Ok(digest
                    == Some(crate::contract::canonical_json_digest(
                        &(
                            expected,
                            Values {
                                scheduler: self,
                                parent,
                                count: self.children(parent).count(),
                            },
                        ),
                        acyclic_stream::MAX_RECORD_BYTES as u64,
                        work,
                    )?))
            }
        }
    }
    pub(crate) fn cancellation_frontier(
        &self,
        id: OperationId,
        recursive: bool,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Vec<OperationId>> {
        let mut frontier = Vec::new();
        let mut held = 0;
        let result = (|| {
            push(&mut frontier, id, &mut held, work)?;
            if recursive {
                let owner = self.operation(id).map(|state| state.spec.owner.authority());
                let mut index = 0;
                while index < frontier.len() {
                    for (_, child) in self.children(frontier[index]) {
                        observe(work)?;
                        if owner == Some(child.spec.owner.authority())
                            && !frontier.contains(&child.spec.operation_id)
                        {
                            push(&mut frontier, child.spec.operation_id, &mut held, work)?;
                        }
                    }
                    index += 1;
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            drop(frontier);
            if let Some(io) = work.as_deref_mut() {
                let _ = io.release_owned(held);
            }
            return Err(error);
        }
        Ok(frontier)
    }
}

struct Values<'a> {
    scheduler: &'a Scheduler,
    parent: OperationId,
    count: usize,
}
impl Serialize for Values<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.count))?;
        if matches!(
            self.scheduler.operations[&self.parent].spec.orchestration,
            Orchestration::Quorum { .. }
        ) {
            for (index, file) in self
                .scheduler
                .successful_completions(self.parent)
                .take(self.count)
                .enumerate()
            {
                let mut digits = [0; 20];
                sequence.serialize_element(&(decimal_slot(index, &mut digits), file))?;
            }
        } else {
            for (slot, child) in self.scheduler.children(self.parent) {
                if let Some(Outcome::Succeeded(file)) = &child.outcome {
                    sequence.serialize_element(&(slot, file))?;
                }
            }
        }
        sequence.end()
    }
}
fn decimal_slot(mut index: usize, digits: &mut [u8; 20]) -> &str {
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (index % 10) as u8;
        index /= 10;
        if index == 0 {
            break;
        }
    }
    std::str::from_utf8(&digits[start..]).expect("decimal ASCII digits")
}
fn text_copy(text: &str, work: &mut Option<&mut dyn HistoryIo>) -> Result<String> {
    match work.as_deref_mut() {
        Some(io) => io.clone_text(text),
        None => Ok(text.into()),
    }
}

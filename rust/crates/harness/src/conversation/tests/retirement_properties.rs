use super::*;
use crate::conversation::history::PIN_GUARD_DISABLED;
use proptest::test_runner::TestCaseError;

struct Turn {
    input: Input,
    calls: Vec<(usize, Option<usize>)>,
    pending: Option<Uuid>,
    outcome: Option<usize>,
    digests: Vec<[u8; 32]>,
}

impl Turn {
    fn new(
        calls: u8,
        results: u8,
        flags: [bool; 4],
        salt: u8,
        outcome: bool,
        late: bool,
    ) -> (Self, ConversationState) {
        let [reasoning, closed, next_user, reverse] = flags;
        let input = Input::new(salt, 3);
        let mut state = input.empty_state();
        let mut turn = Self {
            input,
            calls: Vec::new(),
            pending: None,
            outcome: None,
            digests: Vec::new(),
        };
        turn.push(&mut state, MessageKind::User, None, None, None);
        let user = Uuid::from_u128(1);
        if reasoning {
            turn.push(
                &mut state,
                MessageKind::Assistant,
                Some(user),
                None,
                Some("acyclic.model.reasoning"),
            );
        }
        for index in 0..calls {
            let sequence = turn.push(
                &mut state,
                MessageKind::ToolCall,
                None,
                Some(format!("call-{index}")),
                None,
            );
            turn.calls.push((sequence, None));
        }
        let mut indices = (0..usize::from(calls)).collect::<Vec<_>>();
        if reverse {
            indices.reverse();
        }
        for index in indices {
            if results & (1 << index) != 0 {
                let parent = Uuid::from_u128(turn.calls[index].0 as u128);
                let sequence = turn.push(
                    &mut state,
                    MessageKind::ToolResult,
                    Some(parent),
                    Some(format!("call-{index}")),
                    None,
                );
                turn.calls[index].1 = Some(sequence);
            }
        }
        let mut next_id = None;
        if next_user && late {
            let sequence = turn.push(&mut state, MessageKind::User, None, None, None);
            next_id = Some(Uuid::from_u128(sequence as u128));
        }
        if closed {
            let (kind, extension) = if outcome {
                (MessageKind::System, Some("acyclic.turn.outcome"))
            } else {
                (MessageKind::Assistant, None)
            };
            let sequence = turn.push(&mut state, kind, Some(user), None, extension);
            turn.outcome = outcome.then_some(sequence);
        }
        if next_user && !late {
            let sequence = turn.push(&mut state, MessageKind::User, None, None, None);
            next_id = Some(Uuid::from_u128(sequence as u128));
        }
        // The submitted reply always belongs to user 1. A newer user has no reply.
        turn.pending = next_id.or_else(|| (!closed).then_some(user));
        turn.digests = turn.input.digests();
        (turn, state)
    }

    fn push(
        &mut self,
        state: &mut ConversationState,
        kind: MessageKind,
        reply: Option<Uuid>,
        call: Option<String>,
        extension: Option<&str>,
    ) -> usize {
        let mut next = self.input.message();
        next.kind = kind;
        next.reply_to = reply;
        if call.is_some() || extension.is_some() {
            next.content = FileRef::new(
                next.content.volume().clone(),
                format!("tool-{}.json", next.sequence),
                "one".into(),
                FileDescriptor::from_bytes(b"{}", "application/json", &mut None).unwrap(),
                "tool.json".into(),
                &mut None,
            )
            .unwrap();
        }
        next.tool_call_id = call;
        if let Some(extension) = extension {
            next.extensions
                .insert(extension.into(), next.content.clone());
        }
        self.input.messages.push(next.clone());
        state.append(next, &mut None).unwrap();
        self.input.messages.len()
    }

    fn selection(&self, cut: usize, pin: bool) -> ModelContextSelection {
        ModelContextSelection {
            checkpoint: pin.then(|| self.input.checkpoint.clone()),
            conversation_revision: cut as u64,
            message_ids: Vec::new(),
        }
    }

    fn expected_base(&self, base: usize, cut: usize) -> usize {
        let open = self
            .calls
            .iter()
            .any(|(call, result)| *call <= cut && result.is_none_or(|result| result > cut));
        if cut <= base || self.pending.is_some() || open {
            base
        } else {
            cut
        }
    }

    fn check(
        &self,
        state: &ConversationState,
        base: usize,
        cut: usize,
        pin: bool,
        fault: RetirementFault,
    ) -> std::result::Result<Option<ConversationState>, TestCaseError> {
        let disabled = matches!(fault, RetirementFault::DisablePinGuard);
        let previous = PIN_GUARD_DISABLED.replace(disabled);
        let result = state.checkpointed_suffix(&self.selection(cut, pin), &mut None, None);
        PIN_GUARD_DISABLED.set(previous);
        let mut actual = result
            .map(|(actual, _)| actual)
            .map_err(|error| error.to_string());
        match fault {
            RetirementFault::ForceRetirement => {
                // Deliberately bypass both retention guards while preserving the archive format.
                actual = Ok(ConversationState::from_wire(
                    ConversationStateWire {
                        agent: Some(self.input.agent),
                        archived_prefix: Some(ArchivedConversationPrefix {
                            through_sequence: cut as u64,
                            history_digest: self.digests[cut],
                            checkpoint: self.input.checkpoint.clone(),
                        }),
                        messages: self.input.messages[cut..].to_vec(),
                    },
                    acyclic_stream::MAX_RECORD_BYTES as u64,
                    &mut None,
                )
                .unwrap()
                .0);
            }
            RetirementFault::SkipRetirement => actual = Ok(state.copy(&mut None, None).unwrap().0),
            RetirementFault::DropSuffix => {
                actual.as_mut().unwrap().messages.pop();
            }
            RetirementFault::DropIndex => actual.as_mut().unwrap().by_id.clear(),
            RetirementFault::DropOutcomeIndex => {
                let index = actual.as_mut().unwrap().by_id.get_mut(&Uuid::from_u128(1));
                index.unwrap().1 = None;
            }
            RetirementFault::WrongCheckpoint => {
                let prefix = actual.as_mut().unwrap().archived_prefix.as_mut();
                prefix.unwrap().checkpoint = self.input.other_checkpoint.clone();
            }
            RetirementFault::WrongCut => {
                let prefix = actual.as_mut().unwrap().archived_prefix.as_mut();
                prefix.unwrap().through_sequence += 1;
            }
            RetirementFault::DropPending => actual.as_mut().unwrap().pending_user = None,
            RetirementFault::RestoreOldPending => {
                actual.as_mut().unwrap().pending_user = Some(Uuid::from_u128(1));
            }
            RetirementFault::None | RetirementFault::DisablePinGuard => {}
        }
        let expected_base = self.expected_base(base, cut);
        let users = || {
            self.input
                .messages
                .iter()
                .filter(|message| message.kind == MessageKind::User)
        };
        let observed = match &actual {
            Err(error) => Err(error.as_str()),
            Ok(actual) => {
                let archive = actual.archived_prefix.as_ref().map(|prefix| {
                    (
                        prefix.through_sequence,
                        prefix.history_digest,
                        &prefix.checkpoint,
                    )
                });
                let lookups = self
                    .input
                    .messages
                    .iter()
                    .map(|message| actual.message(message.id).cloned())
                    .collect::<Vec<_>>();
                let outcomes = users()
                    .map(|user| actual.turn_outcome(user.id).cloned())
                    .collect::<Vec<_>>();
                let prefixes = (expected_base..=self.input.messages.len())
                    .map(|through| actual.history_prefix_digest(through as u64, &mut None).ok())
                    .collect::<Vec<_>>();
                Ok((
                    state.unresolved_user(),
                    actual.agent,
                    actual.logical_revision(),
                    actual.resident_after_sequence(),
                    actual.unresolved_user(),
                    actual.messages(),
                    archive,
                    lookups,
                    outcomes,
                    prefixes,
                ))
            }
        };
        let missing;
        let expected = if !pin {
            // A missing pin must be rejected before any retirement decision.
            missing =
                Error::Invalid("history retirement requires a checkpoint pin".into()).to_string();
            Err(missing.as_str())
        } else {
            let digests = &self.digests;
            let archive = (expected_base > 0).then(|| {
                (
                    expected_base as u64,
                    digests[expected_base],
                    &self.input.checkpoint,
                )
            });
            let lookups = self
                .input
                .messages
                .iter()
                .map(|message| (message.sequence > expected_base as u64).then(|| message.clone()))
                .collect::<Vec<_>>();
            let outcomes = users()
                .map(|user| {
                    self.outcome
                        .filter(|sequence| user.sequence == 1 && *sequence > expected_base)
                        .map(|sequence| self.input.messages[sequence - 1].clone())
                })
                .collect::<Vec<_>>();
            let prefixes = digests[expected_base..]
                .iter()
                .copied()
                .map(Some)
                .collect::<Vec<_>>();
            Ok((
                self.pending,
                Some(self.input.agent),
                self.input.messages.len() as u64,
                expected_base as u64,
                self.pending,
                &self.input.messages[expected_base..],
                archive,
                lookups,
                outcomes,
                prefixes,
            ))
        };
        prop_assert_eq!(
            observed,
            expected,
            "retirement differs from input turn and call history"
        );
        Ok(actual.ok())
    }
}

#[derive(Clone, Copy)]
enum RetirementFault {
    None,
    DisablePinGuard,
    ForceRetirement,
    SkipRetirement,
    DropSuffix,
    DropIndex,
    DropOutcomeIndex,
    WrongCheckpoint,
    WrongCut,
    DropPending,
    RestoreOldPending,
}

fn run(
    calls: u8,
    results: u8,
    flags: [bool; 4],
    salt: u8,
    outcome: bool,
    late: bool,
) -> TestCaseResult {
    let (turn, state) = Turn::new(calls, results, flags, salt, outcome, late);
    let mut restored = reopen(&state);
    let mut base = 0;
    for cut in 0..=turn.input.messages.len() {
        turn.check(&state, 0, cut, true, RetirementFault::None)?;
        turn.check(&state, 0, cut, false, RetirementFault::None)?;
        let retired = turn.check(&restored, base, cut, true, RetirementFault::None)?;
        base = turn.expected_base(base, cut);
        restored = reopen(&retired.unwrap());
        turn.check(&restored, base, cut, true, RetirementFault::None)?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn retirement_preserves_pending_turns_and_complete_tool_exchanges(
        calls in 0..3_u8, results in 0..4_u8, flags in proptest::array::uniform4(any::<bool>()), salt in 0..16_u8,
        outcome in any::<bool>(), late in any::<bool>(),
    ) { run(calls, results, flags, salt, outcome, late)?; }
}

#[test]
fn every_bounded_turn_shape_preserves_retirement_guards() {
    for calls in 0..3 {
        for results in 0..4 {
            for flags in 0..16 {
                for outcome in [false, true] {
                    for late in [false, true] {
                        run(
                            calls,
                            results,
                            [
                                flags & 1 != 0,
                                flags & 2 != 0,
                                flags & 4 != 0,
                                flags & 8 != 0,
                            ],
                            1,
                            outcome,
                            late,
                        )
                        .unwrap();
                    }
                }
            }
        }
    }
}

fn expect_fault(result: TestCaseResult, target: &str) {
    let failure = result.unwrap_err();
    assert!(
        failure.to_string().contains(target),
        "wrong observation: {failure}"
    );
}

fn control(calls: u8, results: u8, flags: [bool; 4], cut: usize, fault: RetirementFault) {
    let (turn, state) = Turn::new(calls, results, flags, 1, false, false);
    expect_fault(
        turn.check(&state, 0, cut, true, fault).map(|_| ()),
        "retirement differs from input turn and call history",
    );
}

macro_rules! force_control {
    ($name:ident, $calls:expr, $results:expr, $flags:expr, $cut:expr) => {
        #[test]
        fn $name() {
            control(
                $calls,
                $results,
                $flags,
                $cut,
                RetirementFault::ForceRetirement,
            );
        }
    };
}
force_control!(
    negative_control_retires_pending_user,
    0,
    0,
    [false, false, false, false],
    1
);
force_control!(
    negative_control_retires_reasoning_user,
    0,
    0,
    [true, false, false, false],
    1
);
force_control!(
    negative_control_retires_before_new_pending_user,
    0,
    0,
    [false, true, true, false],
    1
);
force_control!(
    negative_control_retires_open_call,
    1,
    0,
    [false, true, false, false],
    2
);
force_control!(
    negative_control_splits_completed_call,
    1,
    1,
    [false, true, false, false],
    2
);
force_control!(
    negative_control_retires_one_of_two_open_calls,
    2,
    1,
    [false, true, false, false],
    3
);

macro_rules! closed_control {
    ($name:ident, $fault:ident) => {
        #[test]
        fn $name() {
            control(
                0,
                0,
                [false, true, false, false],
                1,
                RetirementFault::$fault,
            );
        }
    };
}
closed_control!(negative_control_skips_closed_retirement, SkipRetirement);
closed_control!(negative_control_drops_retained_suffix, DropSuffix);
closed_control!(negative_control_drops_resident_identity_index, DropIndex);
closed_control!(negative_control_changes_archive_checkpoint, WrongCheckpoint);
closed_control!(negative_control_changes_archive_cut, WrongCut);

#[test]
fn negative_control_drops_pending_turn_identity() {
    control(
        0,
        0,
        [true, false, false, false],
        0,
        RetirementFault::DropPending,
    );
}

#[test]
fn negative_control_disables_production_pin_guard() {
    let (turn, state) = Turn::new(1, 1, [false, true, false, false], 1, false, false);
    for cut in 0..=turn.input.messages.len() {
        turn.check(&state, 0, cut, false, RetirementFault::None)
            .unwrap();
        expect_fault(
            turn.check(&state, 0, cut, false, RetirementFault::DisablePinGuard)
                .map(|_| ()),
            "retirement differs from input turn and call history",
        );
    }
}

#[test]
fn negative_control_does_not_settle_system_outcome() {
    let (turn, state) = Turn::new(0, 0, [false, true, false, false], 1, true, false);
    expect_fault(
        turn.check(&state, 0, 0, true, RetirementFault::RestoreOldPending)
            .map(|_| ()),
        "retirement differs from input turn and call history",
    );
}

#[test]
fn negative_controls_stale_replies_settle_newer_user() {
    for outcome in [false, true] {
        let (turn, state) = Turn::new(0, 0, [false, true, true, false], 1, outcome, true);
        expect_fault(
            turn.check(&state, 0, 0, true, RetirementFault::DropPending)
                .map(|_| ()),
            "retirement differs from input turn and call history",
        );
    }
}

#[test]
fn negative_control_loses_system_outcome_index() {
    let (turn, state) = Turn::new(0, 0, [false, true, false, false], 1, true, false);
    expect_fault(
        turn.check(&state, 0, 0, true, RetirementFault::DropOutcomeIndex)
            .map(|_| ()),
        "retirement differs from input turn and call history",
    );
}

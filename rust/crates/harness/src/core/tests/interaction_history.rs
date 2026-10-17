use super::*;
use crate::interaction::{ApprovalBinding, Interaction, InteractionKind};
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    FutureVersion,
    StaleVersion,
    ForeignResponder,
    ForeignPending,
    Reopen,
    DoubleRetry,
    MutateRejected,
    RejectAll,
}

struct History {
    reducer: Reducer,
    replay: Reducer,
    ticket: InteractionTicket,
    expected: Vec<(OperationId, InteractionResolution)>,
    next_operation: u8,
}

impl History {
    fn new(index: u8) -> Self {
        let request = Interaction::approval("approve".into(), operation(12), [3; 32]).unwrap();
        let bytes = serde_json::to_vec(&request).unwrap();
        let mut id = [5; 16];
        id[0] = index;
        let ticket = InteractionTicket {
            id: uuid::Uuid::from_bytes(id),
            kind: InteractionKind::Approval,
            request: request_file(&bytes).unwrap(),
            deadline_unix_ms: Some(1_000),
            approval: Some(ApprovalBinding {
                operation_id: operation(12),
                action_digest: [3; 32],
            }),
        };
        let mut reducer = reducer_fixture();
        apply(
            &mut reducer,
            1,
            Action::BindConversation {
                agent: AgentId::from_bytes([1; 16]),
            },
        )
        .unwrap();
        apply(
            &mut reducer,
            2,
            Action::OpenInteraction {
                ticket: ticket.clone(),
            },
        )
        .unwrap();
        let replay = reducer.copy(&mut None).unwrap();
        Self {
            reducer,
            replay,
            ticket,
            expected: Vec::new(),
            next_operation: 3,
        }
    }

    fn check(&self, actual: &Reducer) -> TestCaseResult {
        let events = actual
            .events
            .iter()
            .filter_map(|event| {
                if let EventPayload::InteractionResolved { resolution } = &event.payload {
                    Some((event.revision, event.operation_id, resolution.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let expected_events = self
            .expected
            .iter()
            .enumerate()
            .map(|(index, (operation, resolution))| {
                (index as u64 + 3, *operation, resolution.clone())
            })
            .collect::<Vec<_>>();
        let expected_latest = (
            self.ticket.clone(),
            self.expected
                .last()
                .map(|(_, resolution)| resolution.clone()),
        );
        prop_assert_eq!(
            (
                actual.revision(),
                actual.projection.interactions.len(),
                actual.interaction(&self.ticket.id),
                events
            ),
            (
                2 + self.expected.len() as u64,
                1,
                Some(&expected_latest),
                expected_events
            ),
            "interaction independent history mismatch"
        );
        Ok(())
    }

    fn step(&mut self, version: u64, granted: bool, outcome: u8, fault: Fault) -> TestCaseResult {
        let id = operation(self.next_operation);
        self.next_operation += 1;
        let next_version = self.expected.len() as u64 + 1;
        // This is the input history, not the production terminal-state predicate.
        let closed = self.expected.last().is_some_and(|(_, resolution)| {
            matches!(
                resolution.outcome,
                InteractionOutcome::Approved {}
                    | InteractionOutcome::Declined {}
                    | InteractionOutcome::Cancelled {}
                    | InteractionOutcome::Expired {}
                    | InteractionOutcome::Denied {}
            )
        });
        let allowed = !closed && granted && version == next_version && outcome != 6;
        let resolution = InteractionResolution {
            id: self.ticket.id,
            expected_version: version,
            outcome: match outcome {
                0 => InteractionOutcome::Approved {},
                1 => InteractionOutcome::Declined {},
                2 => InteractionOutcome::Cancelled {},
                3 => InteractionOutcome::Expired {},
                4 => InteractionOutcome::Denied {},
                5 => InteractionOutcome::Indeterminate { operation_id: id },
                6 => InteractionOutcome::Indeterminate {
                    operation_id: operation(250),
                },
                _ => unreachable!("bounded outcome input"),
            },
            detail: (outcome < 2).then(|| request_file(b"{}").expect("decision file")),
        };
        if allowed {
            self.expected.push((id, resolution.clone()));
        }
        let mut submitted = resolution;
        if matches!(fault, Fault::FutureVersion | Fault::StaleVersion) {
            submitted.expected_version = next_version;
        }
        if fault == Fault::ForeignPending {
            submitted.outcome = InteractionOutcome::Indeterminate { operation_id: id };
            submitted.detail = None;
        }
        if fault == Fault::Reopen {
            self.reducer
                .projection
                .interactions
                .get_mut(&self.ticket.id)
                .unwrap()
                .1
                .as_mut()
                .unwrap()
                .outcome = InteractionOutcome::Indeterminate {
                operation_id: operation(249),
            };
        }
        let mut cmd = command(
            id,
            2 + next_version - 1,
            Action::ResolveInteraction {
                resolution: submitted,
            },
        );
        let grant = if (granted || fault == Fault::ForeignResponder) && fault != Fault::RejectAll {
            self.ticket.id
        } else {
            let mut foreign = *self.ticket.id.as_bytes();
            foreign[0] ^= 128;
            uuid::Uuid::from_bytes(foreign)
        };
        // Construct the exact grant independently of ticket.responder_grant().
        cmd.scope = issuer()
            .issue(
                "responder",
                &Capabilities::new([
                    "interaction:resolve".to_owned(),
                    format!("interaction:respond:{grant}"),
                ]),
                None,
                None,
                &mut None,
            )
            .unwrap();
        let before = self.reducer.copy(&mut None).unwrap();
        let planned = self.reducer.plan(&cmd);
        prop_assert_eq!(&self.reducer, &before, "interaction planning changed state");
        let applied = self.reducer.apply(&cmd);
        prop_assert_eq!(
            (planned.is_ok(), applied.is_ok()),
            (allowed, allowed),
            "interaction acceptance mismatch"
        );
        if matches!(
            fault,
            Fault::FutureVersion
                | Fault::StaleVersion
                | Fault::ForeignResponder
                | Fault::ForeignPending
                | Fault::Reopen
                | Fault::RejectAll
        ) {
            return Ok(());
        }
        if !allowed {
            if fault == Fault::MutateRejected {
                self.reducer.revision += 1;
            }
            prop_assert_eq!(
                &self.reducer,
                &before,
                "interaction rejection changed state"
            );
            return Ok(());
        }
        let applied = applied.unwrap();
        prop_assert_eq!(&applied, &planned.unwrap());
        let ApplyResult::Applied { event } = applied else {
            panic!("fresh interaction operation replayed");
        };
        self.check(&self.reducer)?;
        self.replay
            .apply_committed(event.clone(), &mut None)
            .unwrap();
        self.check(&self.replay)?;
        let restored = restore(&self.reducer, &issuer()).unwrap();
        self.check(&restored)?;
        prop_assert_eq!(&restored, &self.reducer);
        prop_assert_eq!(&self.replay, &self.reducer);
        let settled = self.reducer.copy(&mut None).unwrap();
        let retried = self.reducer.apply(&cmd).unwrap();
        prop_assert!(
            matches!(retried, ApplyResult::Replayed { .. }),
            "exact interaction retry did not replay"
        );
        if fault == Fault::DoubleRetry {
            self.reducer.events.push_back(event);
            self.reducer.revision += 1;
        }
        self.check(&self.reducer)?;
        if fault == Fault::DoubleRetry {
            return Ok(());
        }
        prop_assert_eq!(&self.reducer, &settled, "interaction retry changed state");
        Ok(())
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn interaction_resolutions_follow_an_independent_history(cycles in 0..7_u8, terminal in 0..5_u8, ticket in 0..16_u8) {
        let mut history = History::new(ticket);
        history.check(&history.reducer)?;
        for index in 0..=cycles {
            let version = u64::from(index) + 1;
            history.step(version + 1, true, 5, Fault::None)?;
            history.step(version - 1, true, 5, Fault::None)?;
            history.step(version, false, 5, Fault::None)?;
            history.step(version, true, 6, Fault::None)?;
            history.step(version, true, if index == cycles { terminal } else { 5 }, Fault::None)?;
        }
        history.step(u64::from(cycles) + 2, true, 5, Fault::None)?;
    }
}

fn expect_fault(result: TestCaseResult, target: &str) {
    let failure = result.unwrap_err();
    assert!(
        failure.to_string().contains(target),
        "wrong observation: {failure}"
    );
}

fn control(fault: Fault, target: &str) {
    let cases = if fault == Fault::DoubleRetry { 6 } else { 5 };
    for terminal in 0..cases {
        let mut history = History::new(terminal);
        if fault == Fault::StaleVersion {
            history.step(1, true, 5, Fault::None).unwrap();
        }
        if fault == Fault::Reopen {
            history.step(1, true, terminal, Fault::None).unwrap();
        }
        let version = if matches!(
            fault,
            Fault::FutureVersion | Fault::Reopen | Fault::MutateRejected
        ) {
            2
        } else {
            1
        };
        let outcome = match fault {
            Fault::ForeignPending => 6,
            Fault::DoubleRetry => terminal,
            _ => 5,
        };
        expect_fault(
            history.step(version, fault != Fault::ForeignResponder, outcome, fault),
            target,
        );
    }
}

macro_rules! negative_control {
    ($name:ident, $fault:ident, $target:literal) => {
        #[test]
        fn $name() {
            control(Fault::$fault, $target);
        }
    };
}

negative_control!(
    negative_control_accepts_future_version,
    FutureVersion,
    "interaction acceptance mismatch"
);
negative_control!(
    negative_control_accepts_stale_version,
    StaleVersion,
    "interaction acceptance mismatch"
);
negative_control!(
    negative_control_accepts_foreign_responder,
    ForeignResponder,
    "interaction acceptance mismatch"
);
negative_control!(
    negative_control_accepts_foreign_pending_operation,
    ForeignPending,
    "interaction acceptance mismatch"
);
negative_control!(
    negative_control_reopens_terminal_interaction,
    Reopen,
    "interaction acceptance mismatch"
);
negative_control!(
    negative_control_applies_exact_retry_twice,
    DoubleRetry,
    "interaction independent history mismatch"
);
negative_control!(
    negative_control_mutates_rejected_resolution,
    MutateRejected,
    "interaction rejection changed state"
);
negative_control!(
    negative_control_rejects_every_valid_resolution,
    RejectAll,
    "interaction acceptance mismatch"
);

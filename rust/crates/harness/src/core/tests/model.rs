//! tests model semantics.
use super::*;
proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]

        #[test]
        fn reducer_admission_is_replayable_and_atomic(
            steps in proptest::collection::vec(step(), 1..24),
        ) {
            use proptest::prelude::*;
            let fail = |error: Error| TestCaseError::fail(error.to_string());
            let lifecycle = [
                LifecycleState::Pending,
                LifecycleState::Active,
                LifecycleState::Waiting,
                LifecycleState::Completed,
                LifecycleState::Failed,
                LifecycleState::Cancelled,
            ];
            let authority = issuer().verifier(&mut None).expect("verifier copy");
            let mut reducer = Reducer::new(
                Authority {
                    kind: AggregateKind::Conversation,
                    id: "conversation-1".into(),
                },
                authority.clone(),
                schemas(),
            );
            let mut admitted: Vec<(Command, Event)> = Vec::new();
            for (index, step) in steps.into_iter().enumerate() {
                let id = OperationId::from_bytes(
                    u128::try_from(index + 1).unwrap_or_default().to_be_bytes(),
                );
                let revision = reducer.revision();
                let next = match step {
                    Step::Lifecycle(to) => command(id, revision, Action::TransitionLifecycle {
                        to: *lifecycle.get(to).ok_or_else(|| TestCaseError::fail("state"))?,
                        reason: None,
                    }),
                    Step::Custom(text) => command(id, revision, Action::AppendCustom {
                        schema: "example.message".into(),
                        version: 1,
                        content: request_file(format!(r#"{{"text":"{text}"}}"#).as_bytes())
                            .map_err(fail)?,
                    }),
                    Step::Bind(agent) => command(id, revision, Action::BindConversation {
                        agent: AgentId::from_bytes([agent; 16]),
                    }),
                    Step::Resubmit(_) | Step::ConflictingIntent(_) if admitted.is_empty() => {
                        continue;
                    }
                    Step::Resubmit(pick) => {
                        let (prior, event) = pick_admitted(&admitted, pick)?;
                        let replayed = reducer.apply(prior.clone()).map_err(fail)?;
                        prop_assert_eq!(replayed, ApplyResult::Replayed { event: event.clone() });
                        continue;
                    }
                    Step::ConflictingIntent(pick) => {
                        let (prior, _) = pick_admitted(&admitted, pick)?;
                        let conflicting = command(prior.operation_id, revision, Action::TransitionLifecycle {
                            to: LifecycleState::Active,
                            reason: Some("other intent".into()),
                        });
                        let before = reducer.copy(&mut None).map_err(fail)?;
                        prop_assert!(matches!(
                            reducer.apply(conflicting),
                            Err(Error::Conflict(_))
                        ));
                        prop_assert_eq!(&reducer, &before);
                        continue;
                    }
                    Step::StaleRevision => command(
                        id,
                        revision.wrapping_add(1),
                        Action::TransitionLifecycle {
                            to: LifecycleState::Active,
                            reason: None,
                        },
                    ),
                };
                let before = reducer.copy(&mut None).map_err(fail)?;
                let planned = reducer.plan(&next);
                prop_assert_eq!(&reducer, &before);
                match planned {
                    Ok(ApplyResult::Applied { event }) => {
                        prop_assert_eq!(event.revision, revision + 1);
                        let committed = reducer.apply_committed(event.clone(), &mut None).map_err(fail)?;
                        prop_assert_eq!(committed, ApplyResult::Applied { event: event.clone() });
                        prop_assert_eq!(reducer.revision(), revision + 1);
                        admitted.push((next, event));
                    }
                    Ok(ApplyResult::Replayed { .. }) => {
                        prop_assert!(false, "fresh operation replayed");
                    }
                    Err(_) => {
                        prop_assert!(reducer.apply(next).is_err());
                        prop_assert_eq!(&reducer, &before);
                    }
                }
            }
            let wire = crate::contract::canonical_json_bytes(&reducer.snapshot(usize::MAX, &mut None).map(|(snapshot, _)| snapshot).map_err(fail)?)
                .map_err(fail)?;
            let decoded = crate::contract::json_from_slice(&wire)
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            let restored = Reducer::restore(
                decoded,
                authority.clone(),
                schemas(), &mut None,
            )
            .map_err(fail)?;
            let mut replayed = Reducer::new(reducer.authority().clone(), authority, schemas());
            for (_, event) in &admitted {
                replayed.apply_committed(event.clone(), &mut None).map_err(fail)?;
            }
            prop_assert_eq!(&restored, &reducer);
            prop_assert_eq!(&replayed, &reducer);
        }
    }

#[test]
fn fixed_lifecycle_rejects_terminal_reopening() -> Result<()> {
    let owner = AuthorityIssuer::new("test", [7; 32], Authority { kind: AggregateKind::Turn, id: "turn-1".into() });
    let mut reducer = Reducer::new(owner.audience.clone(), owner.verifier(&mut None).expect("verifier copy"), schemas());
    let transition = |id, revision, to| {
        let mut command = command(operation(id), revision, Action::TransitionLifecycle { to, reason: None });
        command.scope = owner.issue("turn", &Capabilities::new(["lifecycle:manage"]), None, None, &mut None).expect("scope proof");
        command
    };
    reducer.apply(transition(40, 0, LifecycleState::Active))?;
    reducer.apply(transition(41, 1, LifecycleState::Completed))?;
    assert!(matches!(reducer.apply(transition(42, 2, LifecycleState::Active)), Err(Error::Conflict(_))));
    Ok(())
}

#[test]
fn malformed_admission_metadata_is_rejected() -> Result<()> {
    let mut reducer = reducer_fixture();
    let action = Action::AppendCustom { schema: "example.message".into(), version: 1, content: request_file(br#"{"text":"hello"}"#)? };
    let mut empty_key = command(operation(30), 0, action.clone()); empty_key.idempotency_key = IdempotencyKey(String::new());
    assert!(matches!(reducer.apply(empty_key), Err(Error::Invalid(_))));
    let mut future_cause = command(operation(31), 0, action);
    future_cause.causal_parent = Some(EventReference { authority: reducer.authority().clone(), revision: 1 });
    assert!(matches!(reducer.apply(future_cause), Err(Error::Invalid(_))));
    Ok(())
}

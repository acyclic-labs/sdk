use super::*;
use crate::{CommandIndeterminacy, CommandIndeterminacyReason, IndeterminateCause};
use proptest::prelude::*;

fn causes(command: String, code: String, error: String) -> [IndeterminateCause; 4] {
    [
        IndeterminateCause::Unobserved,
        IndeterminateCause::Command(CommandIndeterminacy {
            command: command.clone(),
            reason: CommandIndeterminacyReason::Failed { code, error },
        }),
        IndeterminateCause::Command(CommandIndeterminacy {
            command: command.clone(),
            reason: CommandIndeterminacyReason::Unsettled,
        }),
        IndeterminateCause::Command(CommandIndeterminacy {
            command,
            reason: CommandIndeterminacyReason::InputLimit,
        }),
    ]
}

#[test]
fn indeterminate_wire_status_never_copies_diagnostic_strings() -> Result<()> {
    fn fixture() -> Result<OperationState> {
        Ok(OperationState {
            spec: spec(id(2), Orchestration::Leaf)?,
            phase: OperationPhase::Reconciling,
            reservation: None,
            checkpoint: None,
            workflow: None,
            outcome: Some(Outcome::Indeterminate {
                operation_id: id(2),
                cause: causes("command".into(), "code".into(), "reason".into())[1].clone(),
            }),
            cancellation_requested: false,
            revision: 2,
        })
    }
    // Independent pre-fix projection, including its three redundant copies.
    fn copying_status(state: &OperationState) -> crate::wire::OperationStatus {
        let Some(Outcome::Indeterminate {
            operation_id,
            cause,
        }) = &state.outcome
        else {
            unreachable!()
        };
        let error = crate::wire_codec::encode_error(&cause.clone().into_error(*operation_id));
        crate::wire::OperationStatus {
            operation: Some(crate::wire::OperationIdentity {
                operation_id: state.spec.operation_id.to_string(),
                idempotency_key: String::new(),
            }),
            state: crate::wire::CompletionState::Indeterminate as i32,
            error: Some(error),
            protocol: Some(crate::wire_api::current_protocol()),
            owner: Some(crate::wire_codec::encode_authority(
                state.spec.owner.authority(),
            )),
            cancellation_requested: state.cancellation_requested,
            revision: state.revision,
            replay_through: None,
        }
    }
    let before = fixture()?;
    let after = fixture()?;
    let (_, [baseline, actual]) = acyclic_test_support::differential(
        || copying_status(&before),
        || crate::wire_api::operation_status(&after),
    );
    let avoids_copies = |work: acyclic_test_support::AllocationInfo| {
        work.count_total + 3 == baseline.count_total
            && work.bytes_total + 17 == baseline.bytes_total
    };
    assert!(
        avoids_copies(actual),
        "wire status copied diagnostic strings: {baseline:?} / {actual:?}"
    );
    // Reinstating the old production path fails this exact allocation oracle.
    assert!(!avoids_copies(baseline));
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    // Every cause variant survives the durable codec, scheduler copy, parent
    // orchestration and wire projection. Strings include escaping, Unicode and empties.
    #[test]
    fn indeterminate_cause_survives_every_projection(
        command in ".{0,32}", code in ".{0,32}", error in ".{0,128}",
    ) {
        for cause in causes(command, code, error) {
            let mut scheduler = two_slot_parent(Orchestration::Join).unwrap();
            let outcome = Outcome::Indeterminate { operation_id: id(2), cause: cause.clone() };
            let event = SchedulerEvent::Completed {
                operation_id: id(2), outcome: outcome.clone(), fence: Some(fence(0)),
                execution_duration_ns: None,
            };
            let encoded = serde_json::to_vec(&event).unwrap();
            let decoded: SchedulerEvent = serde_json::from_slice(&encoded).unwrap();
            prop_assert_eq!(&decoded, &event);
            scheduler.apply(&decoded, &mut None).unwrap();
            let state = scheduler.operation(id(2)).unwrap();
            let copied = state.clone_with_work(&mut None).unwrap();
            prop_assert_eq!(copied.outcome.as_ref(), Some(&outcome));
            #[cfg(feature = "filesystem")]
            {
                let work = acyclic_fs::NativeMaterializationWork::new(
                    acyclic_fs::performance::work_budget(4096, 1_048_576),
                    &acyclic_fs::CancellationToken::new(),
                ).map_err(|(cause, work)| Error::NativeConstruction { cause, work }).unwrap();
                let io = crate::filesystem::PreparationIo::from_native_work(&work).unwrap();
                let native_copy = outcome.clone_with_work(&mut Some(&mut io.history_io())).unwrap();
                prop_assert_eq!(&native_copy, &outcome);
            }
            let recovered: OperationState = serde_json::from_slice(&serde_json::to_vec(state).unwrap()).unwrap();
            prop_assert_eq!(recovered, copied);
            let Outcome::Indeterminate { cause: retained, .. } = &outcome else { unreachable!() };
            let owned: u64 = match retained {
                IndeterminateCause::Unobserved => 0,
                IndeterminateCause::Command(c) => c.command.capacity() as u64 + match &c.reason {
                    CommandIndeterminacyReason::Failed { code, error } => (code.capacity() + error.capacity()) as u64,
                    _ => 0,
                },
            };
            prop_assert_eq!(outcome.logical_owned_bytes().unwrap(), owned);
            let wire = crate::wire_api::operation_status(state);
            let caller_error = cause.clone().into_error(id(2));
            prop_assert_eq!(wire.error.unwrap(), crate::wire_codec::encode_error(&caller_error));
            prop_assert!(caller_error.leaves_effect_uncertain());
            prop_assert!(!caller_error.is_native_failure());

            // Uncertain children retain their lease and remain reconciling.
            // Even after the other child settles, Join must wait rather than
            // publish a terminal result that hides the retained diagnostic.
            complete(&mut scheduler, id(3), Outcome::Cancelled, Some(fence(0))).unwrap();
            prop_assert_eq!(scheduler.orchestration(id(1), &mut None).unwrap(),
                OrchestrationDecision::Wait);
            prop_assert_eq!(scheduler.operation(id(2)).unwrap().outcome.as_ref(), Some(&outcome));
            let revision = scheduler.operation(id(1)).unwrap().revision;
            rejected(&mut scheduler, &SchedulerEvent::Orchestrated {
                operation_id: id(1), expected_revision: revision,
                outcome: Outcome::Indeterminate { operation_id: id(1), cause },
                cancel: Vec::new(), reducer: None, reduction_digest: None,
            });
            complete(&mut scheduler, id(2), Outcome::Cancelled, Some(fence(0))).unwrap();
            prop_assert_eq!(scheduler.orchestration(id(1), &mut None).unwrap(),
                OrchestrationDecision::Complete { outcome: Outcome::Cancelled, cancel: Vec::new() });
        }
    }

    // The cause is a required field of the record: every variant, including
    // `Unobserved`, is written explicitly and round-trips byte for byte through
    // the canonical codec that retry matching authenticates. Negative control:
    // a record without `cause` is rejected rather than silently read as
    // `Unobserved`, so no cause can be dropped by a defaulted decode.
    #[test]
    fn indeterminate_cause_is_a_required_round_tripping_field(
        operation in any::<[u8;16]>(), command in ".{0,32}", code in ".{0,32}", error in ".{0,64}",
    ) {
        let operation_id = OperationId::from_bytes(operation);
        for cause in causes(command, code, error) {
            let event = SchedulerEvent::Completed {
                operation_id,
                outcome: Outcome::Indeterminate { operation_id, cause },
                fence: Some(fence(0)),
                execution_duration_ns: None,
            };
            let mut value = serde_json::to_value(&event).unwrap();
            prop_assert!(value["outcome"]["Indeterminate"].get("cause").is_some());
            let decoded: SchedulerEvent = serde_json::from_value(value.clone()).unwrap();
            prop_assert_eq!(&decoded, &event);
            prop_assert_eq!(crate::contract::canonical_json_bytes(&decoded).unwrap(),
                crate::contract::canonical_json_bytes(&event).unwrap());
            value["outcome"]["Indeterminate"].as_object_mut().unwrap().remove("cause");
            prop_assert!(serde_json::from_value::<SchedulerEvent>(value).is_err());
        }
    }
}

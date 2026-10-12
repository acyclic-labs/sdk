use super::*;
use prost::Message;

fn run_view() -> wire::RunView {
    wire::RunView {
        run_id: vec![2; 16],
        input: vec![3; 32],
        model: "model".to_owned(),
        ..Default::default()
    }
}

pub(crate) fn idle_view() -> wire::WarmView {
    wire::WarmView {
        commitment: vec![1; 32],
        context: vec![2; 32],
        model_profile: vec![3; 32],
        expires_at_ms: 110,
        state: wire::WarmState::Active.into(),
        evidence_digest: vec![4; 32],
        admission_receipt_id: vec![5; 32],
        sequence: 1,
        idle_kv: Some(wire::IdleKvRetention {
            policy: Some(wire::IdleKvPolicy {
                profile: vec![6; 32],
                idle_timeout_ms: 10,
            }),
            retained_at_ms: 100,
            last_used_at_ms: None,
            last_run_id: None,
        }),
        ..Default::default()
    }
}

#[test]
fn unsupported_warm_kinds_reject_both_retention_modes() {
    let mut view = idle_view();
    for idle in [true, false] {
        if !idle {
            view.idle_kv = None;
            view.latency_profile = vec![6; 32];
        }
        assert!(validate_warm_view(&view, None, None).is_ok());
        for kind in [
            "legacy_warm_context",
            "legacy_warm_commitment",
            "idle_warm_context",
            "idle_warm_commitment",
            "",
            "Warm_commitment",
        ] {
            let expected = if kind.ends_with("context") {
                [2; 32]
            } else {
                [1; 32]
            };
            assert!(validate_customer_wire(kind, &view.encode_to_vec(), &expected, &[]).is_err());
        }
        // Inspect and release remain mode-neutral for recovered handles.
        assert!(
            validate_customer_wire("warm_commitment", &view.encode_to_vec(), &[1; 32], &[]).is_ok()
        );
        assert!(
            validate_customer_wire("warm_commitment", &view.encode_to_vec(), &[9; 32], &[])
                .is_err()
        );
    }
}

#[test]
fn latency_admission_binds_authority_and_policy() {
    let mut view = idle_view();
    view.idle_kv = None;
    view.latency_profile = vec![6; 32];
    let request = wire::RetainWarmRequest {
        identity: Some(wire::RequestIdentity {
            client_instance: vec![1; 16],
            request_id: vec![2; 16],
        }),
        context: view.context.clone(),
        latency_profile: view.latency_profile.clone(),
        expires_at_ms: view.expires_at_ms,
        ..Default::default()
    };
    let bytes = view.encode_to_vec();
    assert!(validate_retain_response(&view, &request).is_ok());
    assert!(
        validate_customer_wire("warm_context", &bytes, &[2; 32], &request.encode_to_vec()).is_ok()
    );
    for invalid in [
        wire::RetainWarmRequest {
            identity: None,
            ..request.clone()
        },
        wire::RetainWarmRequest {
            context: vec![9; 32],
            ..request.clone()
        },
        wire::RetainWarmRequest {
            latency_profile: vec![9; 32],
            ..request.clone()
        },
        wire::RetainWarmRequest {
            expires_at_ms: 111,
            ..request.clone()
        },
        wire::RetainWarmRequest {
            expires_at_ms: 0,
            ..request.clone()
        },
    ] {
        assert!(validate_retain_response(&view, &invalid).is_err());
        assert!(
            validate_customer_wire("warm_context", &bytes, &[2; 32], &invalid.encode_to_vec())
                .is_err()
        );
    }
    assert!(
        validate_customer_wire("warm_context", &bytes, &[9; 32], &request.encode_to_vec()).is_err()
    );
    for related in [vec![], vec![255]] {
        assert!(validate_customer_wire("warm_context", &bytes, &[2; 32], &related).is_err());
    }
    assert!(
        validate_customer_wire(
            "warm_context",
            &idle_view().encode_to_vec(),
            &[2; 32],
            &request.encode_to_vec()
        )
        .is_err()
    );
}

#[test]
fn latency_renewal_binds_authority_and_policy() {
    let mut view = idle_view();
    view.idle_kv = None;
    view.latency_profile = vec![6; 32];
    let bytes = view.encode_to_vec();
    let renewal = wire::RenewWarmRequest {
        identity: Some(wire::RequestIdentity {
            client_instance: vec![1; 16],
            request_id: vec![2; 16],
        }),
        commitment: view.commitment.clone(),
        expires_at_ms: view.expires_at_ms,
        ..Default::default()
    };
    assert!(validate_renew_response(&view, &renewal).is_ok());
    assert!(
        validate_customer_wire(
            "warm_commitment",
            &bytes,
            &[1; 32],
            &renewal.encode_to_vec()
        )
        .is_ok()
    );
    for invalid in [
        wire::RenewWarmRequest {
            identity: None,
            ..renewal.clone()
        },
        wire::RenewWarmRequest {
            commitment: vec![9; 32],
            ..renewal.clone()
        },
        wire::RenewWarmRequest {
            expires_at_ms: 111,
            ..renewal.clone()
        },
        wire::RenewWarmRequest {
            idle_timeout_ms: Some(10),
            expires_at_ms: 0,
            ..renewal.clone()
        },
    ] {
        assert!(validate_renew_response(&view, &invalid).is_err());
        assert!(
            validate_customer_wire(
                "warm_commitment",
                &bytes,
                &[1; 32],
                &invalid.encode_to_vec()
            )
            .is_err()
        );
    }
    assert!(
        validate_customer_wire(
            "warm_commitment",
            &bytes,
            &[9; 32],
            &renewal.encode_to_vec()
        )
        .is_err()
    );
    assert!(validate_customer_wire("warm_commitment", &bytes, &[1; 32], &[255]).is_err());
}

#[test]
fn idle_evidence_distinguishes_initial_pin_from_verified_run_use() {
    let mut view = idle_view();
    assert!(validate_warm_view(&view, Some([2; 32]), Some([1; 32])).is_ok());
    let idle = view.idle_kv.as_mut().unwrap();
    idle.last_used_at_ms = Some(105);
    assert!(validate_warm_view(&view, None, None).is_err());
    view.idle_kv.as_mut().unwrap().last_run_id = Some(vec![7; 16]);
    view.expires_at_ms = 115;
    assert!(validate_warm_view(&view, None, None).is_ok());
    view.idle_kv.as_mut().unwrap().last_used_at_ms = Some(99);
    view.expires_at_ms = 109;
    assert!(validate_warm_view(&view, None, None).is_err());
    view.idle_kv.as_mut().unwrap().last_used_at_ms = Some(u64::MAX);
    view.expires_at_ms = 9;
    assert!(validate_warm_view(&view, None, None).is_err());
}

#[test]
fn idle_admission_and_renewal_bind_exact_policy_without_resetting_baseline() {
    let mut view = idle_view();
    let request = wire::RetainWarmRequest {
        identity: Some(wire::RequestIdentity {
            client_instance: vec![1; 16],
            request_id: vec![2; 16],
        }),
        context: vec![2; 32],
        idle_kv: view.idle_kv.as_ref().unwrap().policy.clone(),
        ..Default::default()
    };
    assert!(
        validate_customer_wire(
            "warm_context",
            &view.encode_to_vec(),
            &[2; 32],
            &request.encode_to_vec()
        )
        .is_ok()
    );
    view.idle_kv
        .as_mut()
        .unwrap()
        .policy
        .as_mut()
        .unwrap()
        .profile = vec![8; 32];
    assert!(
        validate_customer_wire(
            "warm_context",
            &view.encode_to_vec(),
            &[2; 32],
            &request.encode_to_vec()
        )
        .is_err()
    );
    let renewal = wire::RenewWarmRequest {
        identity: request.identity.clone(),
        commitment: vec![1; 32],
        idle_timeout_ms: Some(20),
        ..Default::default()
    };
    view.idle_kv
        .as_mut()
        .unwrap()
        .policy
        .as_mut()
        .unwrap()
        .idle_timeout_ms = 20;
    view.expires_at_ms = 120;
    assert!(
        validate_customer_wire(
            "warm_commitment",
            &view.encode_to_vec(),
            &[1; 32],
            &renewal.encode_to_vec()
        )
        .is_ok()
    );
    view.expires_at_ms = 130;
    assert!(
        validate_customer_wire(
            "warm_commitment",
            &view.encode_to_vec(),
            &[1; 32],
            &renewal.encode_to_vec()
        )
        .is_err()
    );
    let mixed = wire::RetainWarmRequest {
        latency_profile: vec![9; 32],
        ..request
    };
    assert!(validate_retain_request(&mixed).is_err());
    let mixed = wire::RenewWarmRequest {
        expires_at_ms: 120,
        ..renewal
    };
    assert!(validate_renew_request(&mixed).is_err());
}

#[test]
fn generated_run_is_bound_to_the_requested_context() {
    let view = run_view();
    let bytes = view.encode_to_vec();
    assert!(validate_generated_run_view(&view, [2; 16], [4; 32]).is_err());
    assert!(validate_customer_wire("generated_run_view", &bytes, &[2; 16], &[]).is_err());
    assert!(validate_customer_wire("generated_run_view", &bytes, &[2; 16], &[4; 32]).is_err());
    assert!(validate_customer_wire("generated_run_view", &bytes, &[2; 16], &[3; 32]).is_ok());
}

#[test]
fn completed_run_validates_the_full_result_context() {
    let context = wire::ContextView {
        revision: vec![10; 32],
        parent: Some(vec![4; 32]),
        lineage: vec![5; 32],
        execution_profile: vec![6; 32],
        content_digest: vec![7; 32],
        model: "model".to_owned(),
        provenance: Some(wire::ContextProvenance {
            origin: Some(wire::context_provenance::Origin::Created(wire::Empty {})),
        }),
        ..Default::default()
    };
    let view = wire::RunView {
        // Explicit fields: the host feature adds a scrubbing `Drop`.
        result: Some(wire::RunResult {
            output: Vec::new(),
            context: Some(context),
            terminal: wire::RunTerminal::Completed.into(),
            receipt: None,
        }),
        ..run_view()
    };
    assert!(validate_run_view(&view, [2; 16]).is_ok());
    assert!(validate_customer_wire("run_view", &view.encode_to_vec(), &[2; 16], &[]).is_ok());

    let mut missing_provenance = view.clone();
    missing_provenance
        .result
        .as_mut()
        .and_then(|result| result.context.as_mut())
        .unwrap()
        .provenance = None;
    assert!(validate_run_view(&missing_provenance, [2; 16]).is_err());
    assert!(
        validate_customer_wire(
            "run_view",
            &missing_provenance.encode_to_vec(),
            &[2; 16],
            &[]
        )
        .is_err()
    );

    let mut invalid_digest = view;
    invalid_digest
        .result
        .as_mut()
        .and_then(|result| result.context.as_mut())
        .unwrap()
        .content_digest
        .clear();
    assert!(validate_run_view(&invalid_digest, [2; 16]).is_err());
    assert!(
        validate_customer_wire("run_view", &invalid_digest.encode_to_vec(), &[2; 16], &[]).is_err()
    );
}

#[test]
fn completed_run_watch_start_is_shared_with_the_host() {
    let view = wire::RunView {
        last_sequence: 7,
        // Explicit fields: the host feature adds a scrubbing `Drop`.
        result: Some(wire::RunResult {
            output: Vec::new(),
            context: None,
            terminal: wire::RunTerminal::Completed.into(),
            receipt: None,
        }),
        ..run_view()
    };
    let bytes = view.encode_to_vec();
    assert_eq!(watch_run_start_wire(&bytes, &[2; 16], "7"), Ok(false));
    assert_eq!(watch_run_start_wire(&bytes, &[2; 16], "8"), Ok(true));
    assert!(watch_run_start_wire(&bytes, &[2; 16], "9").is_err());
    assert!(watch_run_start_wire(&bytes, &[3; 16], "8").is_err());
    assert!(watch_run_start_wire(&bytes, &[2; 16], "-1").is_err());
    assert!(watch_run_start_wire(&bytes, &[2; 16], "18446744073709551616").is_err());
    let mut exhausted = view;
    exhausted.last_sequence = u64::MAX;
    assert!(watch_run_start_wire(&exhausted.encode_to_vec(), &[2; 16], "0").is_err());
}

#[test]
fn watch_state_owns_order_terminal_eof_and_overflow_rules() -> Result<(), &'static str> {
    let view = run_view();
    let mut state = watch_run_start_state_wire(&view.encode_to_vec(), &[2; 16], "0")?;
    let progress = |sequence| wire::RunEvent {
        sequence,
        event: Some(wire::run_event::Event::Progress(wire::RunProgress {
            kind: "queued".to_owned(),
        })),
    };
    state.advance_wire(&progress(0).encode_to_vec())?;
    assert!(!state.is_terminal());
    assert!(state.advance_wire(&progress(0).encode_to_vec()).is_err());
    assert!(state.advance_wire(&progress(2).encode_to_vec()).is_err());
    assert!(state.finish().is_err());

    let terminal = wire::RunEvent {
        sequence: 1,
        event: Some(wire::run_event::Event::Terminal(
            wire::RunTerminal::Completed.into(),
        )),
    };
    state.advance_wire(&terminal.encode_to_vec())?;
    assert!(state.is_terminal());
    assert_eq!(state.finish(), Ok(()));
    assert!(state.advance_wire(&progress(2).encode_to_vec()).is_err());

    let mut max_state = WatchRunState {
        expected: u64::MAX,
        terminal: false,
    };
    assert!(
        max_state
            .advance_wire(
                &wire::RunEvent {
                    sequence: u64::MAX,
                    event: Some(wire::run_event::Event::Terminal(
                        wire::RunTerminal::Completed.into(),
                    )),
                }
                .encode_to_vec()
            )
            .is_err()
    );
    Ok(())
}

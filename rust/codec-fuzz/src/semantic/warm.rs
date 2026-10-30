//! Bind warm responses to the actual retention and renewal request validators.
use acyclic_inference::{MAXIMUM_MESSAGE_BYTES, validate_customer_wire, wire};
use prost::Message;

fn requests(view: &wire::WarmView) -> (wire::RetainWarmRequest, wire::RenewWarmRequest) {
    let identity = Some(wire::RequestIdentity {
        client_instance: vec![1; 16],
        request_id: vec![2; 16],
    });
    let expires_at_ms = if view.idle_kv.is_some() {
        0
    } else {
        view.expires_at_ms
    };
    let policy = view.idle_kv.as_ref().and_then(|idle| idle.policy.clone());
    (
        wire::RetainWarmRequest {
            identity: identity.clone(),
            context: view.context.clone(),
            latency_profile: view.latency_profile.clone(),
            expires_at_ms,
            idle_kv: policy.clone(),
        },
        wire::RenewWarmRequest {
            identity,
            commitment: view.commitment.clone(),
            expires_at_ms,
            idle_timeout_ms: policy.map(|policy| policy.idle_timeout_ms),
        },
    )
}

fn bound(
    kind: &str,
    bytes: &[u8],
    view: &wire::WarmView,
    expected: &[u8],
    related: &impl Message,
) -> bool {
    let related = related.encode_to_vec();
    let accepted = validate_customer_wire(kind, bytes, expected, &related).is_ok();
    if accepted {
        let canonical = view.encode_to_vec();
        if canonical.len() <= MAXIMUM_MESSAGE_BYTES {
            assert!(validate_customer_wire(kind, &canonical, expected, &related).is_ok());
        }
        assert!(validate_customer_wire(kind, bytes, expected, &[0xff]).is_err());
        let mut foreign = expected.to_vec();
        *foreign
            .first_mut()
            .expect("admission requires a 32-byte identity") ^= 1;
        assert!(validate_customer_wire(kind, bytes, &foreign, &related).is_err());
    }
    accepted
}

pub(super) fn check(bytes: &[u8]) {
    let Ok(view) = wire::WarmView::decode(bytes) else {
        return;
    };
    let (retain, renew) = requests(&view);
    if bound("warm_context", bytes, &view, &view.context, &retain) {
        assert!(
            validate_customer_wire("retain_warm_request", &retain.encode_to_vec(), &[], &[])
                .is_ok()
        );
        assert!(validate_customer_wire("warm_context", bytes, &view.context, &[]).is_err());
        for fault in 0..3 {
            let mut invalid = retain.clone();
            match fault {
                0 => invalid.identity = None,
                1 => invalid.context[0] ^= 1,
                _ => invalid.expires_at_ms = invalid.expires_at_ms.wrapping_add(1),
            }
            assert!(
                validate_customer_wire(
                    "warm_context",
                    bytes,
                    &view.context,
                    &invalid.encode_to_vec()
                )
                .is_err()
            );
        }
    }
    if bound("warm_commitment", bytes, &view, &view.commitment, &renew) {
        assert!(
            validate_customer_wire("renew_warm_request", &renew.encode_to_vec(), &[], &[]).is_ok()
        );
        for fault in 0..3 {
            let mut invalid = renew.clone();
            match fault {
                0 => invalid.identity = None,
                1 => invalid.commitment[0] ^= 1,
                _ => match &mut invalid.idle_timeout_ms {
                    Some(timeout) => *timeout = timeout.wrapping_add(1),
                    None => invalid.expires_at_ms = invalid.expires_at_ms.wrapping_add(1),
                },
            }
            assert!(
                validate_customer_wire(
                    "warm_commitment",
                    bytes,
                    &view.commitment,
                    &invalid.encode_to_vec()
                )
                .is_err()
            );
        }
    }
}

pub(super) fn seeds()
-> Result<Vec<(&'static str, &'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    let absolute = wire::WarmView {
        commitment: vec![1; 32],
        context: vec![2; 32],
        model_profile: vec![3; 32],
        latency_profile: vec![4; 32],
        expires_at_ms: 100,
        state: wire::WarmState::Active.into(),
        evidence_digest: vec![5; 32],
        admission_receipt_id: vec![6; 32],
        sequence: 1,
        idle_kv: None,
    };
    let mut idle = absolute.clone();
    idle.latency_profile.clear();
    idle.idle_kv = Some(wire::IdleKvRetention {
        policy: Some(wire::IdleKvPolicy {
            profile: vec![7; 32],
            idle_timeout_ms: 20,
        }),
        retained_at_ms: 80,
        last_used_at_ms: None,
        last_run_id: None,
    });
    let mut outputs = Vec::new();
    for (variant, view) in [("absolute", absolute), ("idle", idle)] {
        let (retain, renew) = requests(&view);
        let encoded = view.encode_to_vec();
        validate_customer_wire("warm_context", &encoded, &view.context, &retain.encode_to_vec())
            .map_err(|error| format!("warm retain seed admission failed: {error}; recovery: review semantic::warm::seeds, then run cargo run --example capture"))?;
        validate_customer_wire("warm_commitment", &encoded, &view.commitment, &renew.encode_to_vec())
            .map_err(|error| format!("warm renew seed admission failed: {error}; recovery: review semantic::warm::seeds, then run cargo run --example capture"))?;
        outputs.extend([
            ("inference.customer.v1.WarmView", variant, encoded),
            (
                "inference.customer.v1.RetainWarmRequest",
                variant,
                retain.encode_to_vec(),
            ),
            (
                "inference.customer.v1.RenewWarmRequest",
                variant,
                renew.encode_to_vec(),
            ),
        ]);
    }
    Ok(outputs)
}

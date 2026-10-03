use acyclic_harness::{
    Error, IdempotencyKey, OperationId,
    swarm_budget::{
        SwarmResourceRequest, SwarmUsage, SwarmUsageLimiter, SwarmUsageReceiptIssuer,
        SwarmUsageSource,
    },
};
use std::{collections::VecDeque, sync::Mutex};

fn operation(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16])
}

fn assert_conflict<T>(result: acyclic_harness::Result<T>) {
    assert!(
        matches!(result, Err(Error::Conflict(_))),
        "expected typed conflict"
    );
}

#[test]
fn public_usage_limiter_stops_before_each_dimension_crosses_its_ceiling() {
    let limits = SwarmResourceRequest {
        model_steps: 2,
        output_bytes: 8,
        execution_time_ms: 10,
    };
    let mut limiter = SwarmUsageLimiter::new(limits).expect("limiter");
    limiter.admit_model_step().expect("first model step");
    limiter.admit_model_step().expect("second model step");
    assert_conflict(limiter.admit_model_step());
    limiter.admit_output(8).expect("output ceiling");
    assert_conflict(limiter.admit_output(1));
    limiter.admit_execution_time(10).expect("execution ceiling");
    assert_conflict(limiter.admit_execution_time(1));
    assert_eq!(
        limiter.usage(),
        SwarmUsage {
            model_steps: 2,
            output_bytes: 8,
            execution_time_ms: 10,
        }
    );
}

struct SequenceSource {
    snapshots: Mutex<VecDeque<SwarmUsage>>,
}

impl SequenceSource {
    fn new(snapshots: impl IntoIterator<Item = SwarmUsage>) -> Self {
        Self {
            snapshots: Mutex::new(snapshots.into_iter().collect()),
        }
    }
}

impl SwarmUsageSource for SequenceSource {
    fn provider_identity(&self) -> &str {
        "public-budget-provider"
    }

    fn cumulative_usage(
        &self,
        _operation_id: OperationId,
        _dispatch_id: &IdempotencyKey,
    ) -> acyclic_harness::Result<SwarmUsage> {
        self.snapshots
            .lock()
            .expect("source lock")
            .pop_front()
            .ok_or_else(|| Error::Storage("usage snapshot exhausted".into()))
    }
}

#[test]
fn public_receipt_issuer_enforces_ceiling_and_monotonic_cursor() {
    let limits = SwarmResourceRequest {
        model_steps: 4,
        output_bytes: 40,
        execution_time_ms: 100,
    };
    let mut issuer = SwarmUsageReceiptIssuer::with_limits(
        SequenceSource::new([
            SwarmUsage {
                model_steps: 2,
                output_bytes: 20,
                execution_time_ms: 50,
            },
            SwarmUsage {
                model_steps: 5,
                output_bytes: 20,
                execution_time_ms: 50,
            },
        ]),
        operation(40),
        IdempotencyKey::new("receipt-dispatch").expect("dispatch"),
        limits,
    )
    .expect("issuer");
    assert_eq!(issuer.next_sequence().expect("sequence"), 1);
    issuer.issue().expect("first receipt");
    assert_eq!(issuer.next_sequence().expect("sequence"), 2);
    assert_conflict(issuer.issue());
    assert_eq!(
        issuer.next_sequence().expect("failed issue keeps cursor"),
        2
    );

    let mut backwards = SwarmUsageReceiptIssuer::with_limits(
        SequenceSource::new([
            SwarmUsage {
                model_steps: 3,
                output_bytes: 20,
                execution_time_ms: 50,
            },
            SwarmUsage {
                model_steps: 2,
                output_bytes: 20,
                execution_time_ms: 50,
            },
        ]),
        operation(41),
        IdempotencyKey::new("backwards-dispatch").expect("dispatch"),
        limits,
    )
    .expect("issuer");
    backwards.issue().expect("first receipt");
    assert_conflict(backwards.issue());
}

#[test]
fn public_resume_with_limits_rejects_restored_usage_over_ceiling() {
    let result = SwarmUsageReceiptIssuer::resume_with_limits(
        SequenceSource::new([]),
        operation(42),
        IdempotencyKey::new("resume-dispatch").expect("dispatch"),
        1,
        Some(SwarmUsage {
            model_steps: 5,
            output_bytes: 20,
            execution_time_ms: 50,
        }),
        SwarmResourceRequest {
            model_steps: 4,
            output_bytes: 40,
            execution_time_ms: 100,
        },
    );
    assert_conflict(result);
}

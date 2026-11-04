//! Seed actual wire encoders with values from public semantic constructors.
use acyclic_actors::{domain as d, wire};
use prost::Message;

pub(super) fn check(name: &str, bytes: &[u8]) {
    if name != "acyclic.actors.v1.Error" {
        return;
    }
    let detail = if bytes.is_empty() {
        None
    } else {
        wire::Error::decode(bytes).ok()
    };
    // Enter the same owned status conversion and diagnostic projection used by
    // native and browser clients, including future codes and malformed details.
    for (code, category) in [
        (tonic::Code::PermissionDenied, "permission_denied"),
        (tonic::Code::Unknown, "unknown"),
    ] {
        let error = acyclic_actors::client::Error::from(tonic::Status::with_details(
            code,
            "original status",
            bytes.to_vec().into(),
        ));
        let original_backing = match &error {
            acyclic_actors::client::Error::Service {
                grpc_code,
                message,
                raw_details,
                detail: actual,
            } => {
                assert_eq!(*grpc_code, code as i32);
                assert_eq!(message, "original status");
                assert_eq!(raw_details, bytes);
                assert_eq!(actual, &detail);
                actual.as_ref().map(|value| {
                    [
                        value.component.as_ptr(),
                        value.reason.as_ptr(),
                        value.request_id.as_ptr(),
                    ]
                })
            }
            other => panic!("expected original service failure, got {other:?}"),
        };
        let metadata = error.metadata();
        assert_eq!(metadata.code, category);
        assert_eq!(metadata.message, "original status");
        assert_eq!(metadata.grpc_code, Some(code as i32));
        assert_eq!(metadata.raw_details.as_deref(), Some(bytes));
        assert_eq!(
            metadata.service_code,
            detail.as_ref().map(|value| value.code)
        );
        assert_eq!(
            metadata.service_message.as_deref(),
            detail.as_ref().map(|value| value.message.as_str())
        );
        let expected = detail.as_ref().filter(|value| {
            !(value.component.is_empty() && value.reason.is_empty() && value.request_id.is_empty())
        });
        assert_eq!(
            metadata.cause.is_some(),
            expected.is_some(),
            "original Actors cause"
        );
        if let Some(cause) = &metadata.cause {
            let original = expected.expect("original attribution present");
            assert_eq!(cause.component, original.component);
            assert_eq!(cause.reason, original.reason);
            assert_eq!(cause.request_id, original.request_id);
            assert_eq!(
                Some([
                    cause.component.as_ptr(),
                    cause.reason.as_ptr(),
                    cause.request_id.as_ptr()
                ]),
                original_backing,
                "original Actors attribution backing"
            );
        }
        let projected = serde_json::to_value(metadata).expect("original diagnostic projection");
        let expected = expected.map_or(serde_json::Value::Null, |value| {
            serde_json::json!({
                "component":value.component, "reason":value.reason, "requestId":value.request_id,
            })
        });
        assert_eq!(projected["cause"], expected);
    }
    if let Some(detail) = detail
        && let Ok(code) = wire::ErrorCode::try_from(detail.code)
    {
        let expected = acyclic_auth::refusal::Cause::reported(
            detail.component,
            detail.reason,
            detail.request_id,
        );
        let status = acyclic_actors::grpc::error_status(code, expected.clone());
        let original = status.details().to_vec();
        let metadata = acyclic_actors::client::Error::from(status).metadata();
        assert_eq!(metadata.raw_details, Some(original));
        assert_eq!(metadata.service_code, Some(code as i32));
        assert_eq!(metadata.cause, expected, "original Actors writer cause");
    }
}

pub(super) fn seeds() -> Result<Vec<(&'static str, &'static str, Vec<u8>)>, d::DomainError> {
    let mut seeds = Vec::new();
    macro_rules! seed {
        ($kind:ident, $value:expr) => {
            seed!($kind, $kind, "semantic", $value)
        };
        ($kind:ident, $semantic:ident, $variant:literal, $value:expr) => {{
            let value = $value;
            let wire: wire::$kind = value.clone().into();
            assert_eq!(d::$semantic::try_from(wire.clone())?, value);
            seeds.push((
                concat!("acyclic.actors.v1.", stringify!($kind)),
                $variant,
                wire.encode_to_vec(),
            ));
        }};
    }
    let actor = d::ActorId::new("codec".into())?;
    let namespace = d::ActorNamespace::new("codec".into())?;
    let code = d::CodeSha256::new(vec![1; 32])?;
    let limits = d::ActorLimits::new(1, 1024, 4096)?;
    let worker_limits = d::WorkerLimits::new(1, 1024, 1)?;
    let header = d::Header::new("content-type".into(), "application/octet-stream".into());
    let binding = d::Binding::actor_state("state".into())?;
    let subscription = d::SubscriptionSpec::new(
        "inbox".into(),
        "codec/events".into(),
        d::SubscriptionStart::cursor(0),
        true,
    )?;
    seed!(Header, header.clone());
    seed!(Binding, binding.clone());
    seed!(ActorLimits, limits);
    seed!(WorkerLimits, worker_limits);
    seed!(SubscriptionStart, d::SubscriptionStart::cursor(0));
    seed!(
        SubscriptionStart,
        SubscriptionStart,
        "current-head",
        d::SubscriptionStart::current_head()
    );
    seed!(SubscriptionSpec, subscription.clone());
    seed!(
        EntrySource,
        d::EntrySource::new(
            acyclic_objects::Digest::from_bytes([1; 32]),
            22,
            "package".into(),
            "codec".into(),
        )?
    );
    seed!(
        SecretSelection,
        d::SecretSelection::new(
            "credential".into(),
            acyclic_auth::secrets::SecretRef::new("codec".into(), "key".into(), 1)?,
        )?
    );
    let egress = d::EgressRule::new("example.com".into(), 443)?;
    seed!(EgressRule, egress.clone());
    seed!(
        FetchBinding,
        d::FetchBinding::new("fetch".into(), vec![egress])?
    );
    seed!(
        Error,
        ServiceError,
        "semantic",
        d::ServiceError::new(d::ErrorCode::InvalidArgument, "refused".into())
    );
    seed!(
        Error,
        ServiceError,
        "attributed",
        d::ServiceError::new(d::ErrorCode::DependencyUnavailable, "refused".into()).with_cause(
            acyclic_auth::refusal::Cause {
                component: "actors.checkpoint".into(),
                reason: "dependency_failed".into(),
                request_id: "original-request".into(),
            }
        )
    );
    let status = acyclic_actors::grpc::error_status(
        d::ErrorCode::DependencyUnavailable,
        Some(acyclic_auth::refusal::Cause {
            component: "actors.checkpoint".into(),
            reason: "dependency_failed".into(),
            request_id: "original-request".into(),
        }),
    );
    seeds.push((
        "acyclic.actors.v1.Error",
        "status-attributed",
        status.details().to_vec(),
    ));
    seed!(
        DeploymentRequest,
        d::DeploymentRequest::new(
            namespace.clone(),
            code.clone(),
            vec![binding.clone()],
            limits,
            vec![subscription.clone()],
            "deploy".into(),
        )?
    );
    seed!(
        DeploymentRequest,
        DeploymentRequest,
        "worker",
        d::DeploymentRequest::worker(
            namespace.clone(),
            code.clone(),
            Vec::new(),
            worker_limits,
            "deploy-worker".into(),
        )?
    );
    seed!(
        UpdateNamespaceRequest,
        d::UpdateNamespaceRequest::new(
            namespace.clone(),
            code,
            vec![binding],
            limits,
            1,
            "update".into(),
        )?
    );
    seed!(
        InspectActorRequest,
        d::InspectActorRequest::new(actor.clone())
    );
    seed!(
        AddSubscriptionRequest,
        d::AddSubscriptionRequest::new(actor.clone(), subscription, "subscribe".into())?
    );
    seed!(
        RemoveSubscriptionRequest,
        d::RemoveSubscriptionRequest::new(actor.clone(), "inbox".into(), "remove".into())
    );
    seed!(
        ResumeSubscriptionRequest,
        d::ResumeSubscriptionRequest::new(actor.clone(), "inbox".into(), "resume".into())
    );
    seed!(
        CheckpointActorRequest,
        d::CheckpointActorRequest::new(actor.clone(), "checkpoint".into())
    );
    seed!(
        DeleteAllRequest,
        d::DeleteAllRequest::new(actor.clone(), "delete".into())?
    );
    seed!(
        DeleteWorkerRequest,
        d::DeleteWorkerRequest::new(namespace.clone(), "delete-worker".into())?
    );
    let order = d::InvocationOrder::new("0".repeat(32), 0)?;
    seed!(InvocationOrder, order.clone());
    seed!(
        InvokeActorRequest,
        d::InvokeActorRequest::new(
            actor,
            "POST".into(),
            "https://example.com/".into(),
            b"request".to_vec(),
            vec![header.clone()],
        )
        .with_invocation_order(order)
    );
    seed!(
        InvokeWorkerRequest,
        d::InvokeWorkerRequest::new(
            namespace,
            "POST".into(),
            "https://example.com/".into(),
            b"request".to_vec(),
            vec![header],
        )?
    );
    Ok(seeds)
}

#[cfg(test)]
mod tests {
    use prost::Message;

    #[test]
    fn canonical_error_writers_preserve_original_attribution() {
        use super::wire::ErrorCode as E;
        use tonic::Code as C;
        // Preserve the deployed transport contract as the writer moves into
        // the shared library. These are protocol values, not provider fixtures.
        for (code, transport, name) in [
            (E::Unspecified, C::Internal, "ERROR_CODE_UNSPECIFIED"),
            (
                E::InvalidArgument,
                C::InvalidArgument,
                "ERROR_CODE_INVALID_ARGUMENT",
            ),
            (
                E::CapabilityDenied,
                C::PermissionDenied,
                "ERROR_CODE_CAPABILITY_DENIED",
            ),
            (
                E::CapabilityExpired,
                C::Unauthenticated,
                "ERROR_CODE_CAPABILITY_EXPIRED",
            ),
            (E::ActorNotFound, C::NotFound, "ERROR_CODE_ACTOR_NOT_FOUND"),
            (
                E::SubscriptionNotFound,
                C::NotFound,
                "ERROR_CODE_SUBSCRIPTION_NOT_FOUND",
            ),
            (
                E::IdempotencyMismatch,
                C::FailedPrecondition,
                "ERROR_CODE_IDEMPOTENCY_MISMATCH",
            ),
            (E::Conflict, C::FailedPrecondition, "ERROR_CODE_CONFLICT"),
            (
                E::AdmissionDenied,
                C::PermissionDenied,
                "ERROR_CODE_ADMISSION_DENIED",
            ),
            (
                E::CheckpointFailed,
                C::Aborted,
                "ERROR_CODE_CHECKPOINT_FAILED",
            ),
            (
                E::DependencyUnavailable,
                C::Unavailable,
                "ERROR_CODE_DEPENDENCY_UNAVAILABLE",
            ),
        ] {
            let status = acyclic_actors::grpc::error_status(code, None);
            assert_eq!(status.code(), transport);
            assert_eq!(status.message(), name);
            let detail = super::wire::Error::decode(status.details()).expect("canonical writer");
            assert_eq!(detail.code, code as i32);
            assert_eq!(detail.message, name);
            super::check("acyclic.actors.v1.Error", status.details());
        }
        for bytes in [Vec::new(), vec![255, 0]] {
            super::check("acyclic.actors.v1.Error", &bytes);
        }
        for (name, _, bytes) in super::seeds().expect("canonical Actors writers") {
            if name != "acyclic.actors.v1.Error" {
                continue;
            }
            super::check(name, &bytes);
            let mut detail = super::wire::Error::decode(bytes.as_slice())
                .expect("canonical Actors error writer");
            for code in [i32::MIN, 0, 10, i32::MAX] {
                detail.code = code;
                super::check(name, &detail.encode_to_vec());
                let mut partial = detail.clone();
                partial.reason.clear();
                partial.request_id.clear();
                super::check(name, &partial.encode_to_vec());
                partial.component.clear();
                partial.reason = "future_dependency_reason".into();
                partial.request_id = "original-request".into();
                super::check(name, &partial.encode_to_vec());
            }
        }
    }
}

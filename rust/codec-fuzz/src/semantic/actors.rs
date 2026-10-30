//! Seed actual wire encoders with values from public semantic constructors.
use acyclic_actors::{domain as d, wire};
use prost::Message;

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
        d::SecretSelection::new("credential".into(), "secrets:/codec/key".into())?
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

//! Public Actors v1 wire surface.
//!
//! Wire types are generated Protobuf shadows from the executable Rust contract.

pub use crate::contract::{
    ActorLimitsProto as ActorLimits, ActorObservationProto as ActorObservation, ActorState,
    AddSubscriptionRequestProto as AddSubscriptionRequest,
    AddSubscriptionResponseProto as AddSubscriptionResponse, BindingProto as Binding,
    CheckpointActorRequestProto as CheckpointActorRequest,
    CheckpointActorResponseProto as CheckpointActorResponse,
    DeleteAllRequestProto as DeleteAllRequest, DeleteAllResponseProto as DeleteAllResponse,
    DeploymentObservationProto as DeploymentObservation,
    DeploymentRequestProto as DeploymentRequest, DeploymentResponseProto as DeploymentResponse,
    EgressRuleProto as EgressRule, ErrorCode, FetchBindingProto as FetchBinding,
    HeaderProto as Header, InspectActorRequestProto as InspectActorRequest,
    InspectActorResponseProto as InspectActorResponse, InvocationOrderProto as InvocationOrder,
    InvokeActorRequestProto as InvokeActorRequest, InvokeActorResponseProto as InvokeActorResponse,
    InvokeWorkerRequestProto as InvokeWorkerRequest,
    RemoveSubscriptionRequestProto as RemoveSubscriptionRequest,
    RemoveSubscriptionResponseProto as RemoveSubscriptionResponse,
    ResumeSubscriptionRequestProto as ResumeSubscriptionRequest,
    ResumeSubscriptionResponseProto as ResumeSubscriptionResponse,
    SecretSelectionProto as SecretSelection, ServiceErrorProto as Error,
    SubscriptionObservationProto as SubscriptionObservation,
    SubscriptionSpecProto as SubscriptionSpec, SubscriptionStartProto as SubscriptionStart,
    SubscriptionState, UpdateNamespaceRequestProto as UpdateNamespaceRequest,
    UpdateNamespaceResponseProto as UpdateNamespaceResponse, WorkerLimitsProto as WorkerLimits,
};

include!(concat!(env!("OUT_DIR"), "/rust/acyclic.actors.v1.rs"));

/// Generated wire oneof for subscription start.
pub mod subscription_start {
    pub use crate::contract::subscription_start::StartProto as Start;
}

pub use crate::contract::{
    DeleteWorkerRequestProto as DeleteWorkerRequest,
    DeleteWorkerResponseProto as DeleteWorkerResponse,
};

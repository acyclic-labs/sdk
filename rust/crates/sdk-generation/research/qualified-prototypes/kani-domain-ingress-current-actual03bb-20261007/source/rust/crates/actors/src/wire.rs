//! Public Actors v1 wire surface.
//!
//! Wire types are generated Protobuf shadows from the executable Rust contract.

pub use crate::contract::{
    ActorLimitsProto as ActorLimits, ActorObservationProto as ActorObservation, ActorState,
    AddSubscriptionRequestProto as AddSubscriptionRequest,
    AddSubscriptionResponseProto as AddSubscriptionResponse, BindingProto as Binding,
    CheckpointActorRequestProto as CheckpointActorRequest,
    CheckpointActorResponseProto as CheckpointActorResponse,
    CreateActorRequestProto as CreateActorRequest, CreateActorResponseProto as CreateActorResponse,
    ErrorCode, ServiceErrorProto as Error, HeaderProto as Header,
    InspectActorRequestProto as InspectActorRequest,
    InspectActorResponseProto as InspectActorResponse,
    InvokeActorRequestProto as InvokeActorRequest, InvokeActorResponseProto as InvokeActorResponse,
    RemoveSubscriptionRequestProto as RemoveSubscriptionRequest,
    RemoveSubscriptionResponseProto as RemoveSubscriptionResponse,
    ResumeSubscriptionRequestProto as ResumeSubscriptionRequest,
    ResumeSubscriptionResponseProto as ResumeSubscriptionResponse,
    SubscriptionObservationProto as SubscriptionObservation,
    SubscriptionSpecProto as SubscriptionSpec, SubscriptionStartProto as SubscriptionStart,
    SubscriptionState, UpdateActorRequestProto as UpdateActorRequest,
    UpdateActorResponseProto as UpdateActorResponse,
};

include!(concat!(env!("OUT_DIR"), "/rust/acyclic.actors.v1.rs"));

/// Generated wire oneof for subscription start.
pub mod subscription_start {
    pub use crate::contract::subscription_start::StartProto as Start;
}

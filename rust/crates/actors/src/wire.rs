//! Public Actors v1 wire surface.
//!
//! Wire types are generated Protobuf shadows from the executable Rust contract.

pub use crate::contract::{
    ActorAddressProto as ActorAddress, ActorCallProto as ActorCall,
    ActorCallResultProto as ActorCallResult, ActorLimitsProto as ActorLimits,
    ActorObservationProto as ActorObservation, ActorState,
    AddSubscriptionRequestProto as AddSubscriptionRequest,
    AddSubscriptionResponseProto as AddSubscriptionResponse, BindingProto as Binding,
    BroadcastProto as Broadcast, CheckpointActorRequestProto as CheckpointActorRequest,
    CheckpointActorResponseProto as CheckpointActorResponse, CloseProto as Close,
    ConnectProto as Connect, CreateActorRequestProto as CreateActorRequest,
    CreateActorResponseProto as CreateActorResponse, DeleteActorRequestProto as DeleteActorRequest,
    DeleteActorResponseProto as DeleteActorResponse, EffectIdProto as EffectId, ErrorCode,
    HeaderProto as Header, InspectActorRequestProto as InspectActorRequest,
    InspectActorResponseProto as InspectActorResponse,
    InvokeActorRequestProto as InvokeActorRequest, InvokeActorResponseProto as InvokeActorResponse,
    RemoveSubscriptionRequestProto as RemoveSubscriptionRequest,
    RemoveSubscriptionResponseProto as RemoveSubscriptionResponse,
    ResumeSubscriptionRequestProto as ResumeSubscriptionRequest,
    ResumeSubscriptionResponseProto as ResumeSubscriptionResponse, SendProto as Send,
    ServiceErrorProto as Error, SetAttachmentProto as SetAttachment,
    SetAutoResponseProto as SetAutoResponse, SetTagsProto as SetTags,
    SocketAcceptProto as SocketAccept, SocketAutoResponseProto as SocketAutoResponse,
    SocketAutoResponseUpdateProto as SocketAutoResponseUpdate,
    SocketBroadcastTargetProto as SocketBroadcastTarget, SocketCheckpointProto as SocketCheckpoint,
    SocketCloseInitiator, SocketCloseProto as SocketClose, SocketConfigProto as SocketConfig,
    SocketConnectCapabilityProto as SocketConnectCapability,
    SocketConnectionProto as SocketConnection, SocketConnectionSetProto as SocketConnectionSet,
    SocketDecisionProto as SocketDecision, SocketEffectProto as SocketEffect,
    SocketErrorProto as SocketError, SocketEventProto as SocketEvent,
    SocketHostRuleProto as SocketHostRule, SocketKeepaliveProto as SocketKeepalive,
    SocketMessageProto as SocketMessage, SocketMeteringProto as SocketMetering,
    SocketOpenProto as SocketOpen, SocketPayloadProto as SocketPayload,
    SocketPortRangeProto as SocketPortRange, SocketReconnectMode,
    SocketReconnectProto as SocketReconnect, SocketReconnectingProto as SocketReconnecting,
    SocketRejectProto as SocketReject, SocketScopeProto as SocketScope,
    SocketSecretHeaderProto as SocketSecretHeader, SocketTransport,
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

/// Generated wire variants.
pub mod socket_payload {
    pub use crate::contract::socket_payload::ValueProto as Value;
}

/// Generated wire variants.
pub mod socket_broadcast_target {
    pub use crate::contract::socket_broadcast_target::ValueProto as Value;
}

/// Generated wire variants.
pub mod socket_scope {
    pub use crate::contract::socket_scope::ValueProto as Value;
}

/// Generated wire variants.
pub mod socket_auto_response_update {
    pub use crate::contract::socket_auto_response_update::ValueProto as Value;
}

/// Generated wire variants.
pub mod socket_effect {
    pub use crate::contract::socket_effect::ValueProto as Value;
}

/// Generated wire variants.
pub mod socket_event {
    pub use crate::contract::socket_event::ValueProto as Value;
}

/// Generated wire variants.
pub mod socket_decision {
    pub use crate::contract::socket_decision::ValueProto as Value;
}

/// Wire variants for the common connect target.
pub mod connect {
    pub use crate::contract::connect::TargetProto as Target;
}

/// Wire application payload.
pub mod socket_message {
    pub use crate::contract::socket_message::PayloadProto as Payload;
}

/// Wire application payload.
pub mod send {
    pub use crate::contract::send::PayloadProto as Payload;
}

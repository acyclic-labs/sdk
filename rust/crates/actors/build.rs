//! Build Actors from the semantic contract.
pub(crate) use contract::{
    ContractError, validate_add_subscription, validate_create, validate_delete, validate_update,
};

#[allow(
    unused_imports,
    reason = "contract's build-time registration resolves these aliases from generated declarations"
)]
mod wire {
    pub use crate::contract::{
        ActorAddressProto as ActorAddress, ActorCallProto as ActorCall,
        ActorCallResultProto as ActorCallResult, ActorLimitsProto as ActorLimits,
        ActorObservationProto as ActorObservation,
        AddSubscriptionRequestProto as AddSubscriptionRequest,
        AddSubscriptionResponseProto as AddSubscriptionResponse, BindingProto as Binding,
        BroadcastProto as Broadcast, CheckpointActorRequestProto as CheckpointActorRequest,
        CheckpointActorResponseProto as CheckpointActorResponse, CloseProto as Close,
        ConnectProto as Connect, CreateActorRequestProto as CreateActorRequest,
        CreateActorResponseProto as CreateActorResponse,
        DeleteActorRequestProto as DeleteActorRequest,
        DeleteActorResponseProto as DeleteActorResponse, EffectIdProto as EffectId,
        HeaderProto as Header, InspectActorRequestProto as InspectActorRequest,
        InspectActorResponseProto as InspectActorResponse,
        InvokeActorRequestProto as InvokeActorRequest,
        InvokeActorResponseProto as InvokeActorResponse,
        RemoveSubscriptionRequestProto as RemoveSubscriptionRequest,
        RemoveSubscriptionResponseProto as RemoveSubscriptionResponse,
        ResumeSubscriptionRequestProto as ResumeSubscriptionRequest,
        ResumeSubscriptionResponseProto as ResumeSubscriptionResponse, SendProto as Send,
        ServiceErrorProto as Error, SetAttachmentProto as SetAttachment,
        SetAutoResponseProto as SetAutoResponse, SetTagsProto as SetTags,
        SocketAcceptProto as SocketAccept, SocketAutoResponseProto as SocketAutoResponse,
        SocketAutoResponseUpdateProto as SocketAutoResponseUpdate,
        SocketBroadcastTargetProto as SocketBroadcastTarget,
        SocketCheckpointProto as SocketCheckpoint, SocketCloseInitiator,
        SocketCloseProto as SocketClose, SocketConfigProto as SocketConfig,
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
        UpdateActorRequestProto as UpdateActorRequest,
        UpdateActorResponseProto as UpdateActorResponse,
    };
    pub mod subscription_start {
        pub use crate::contract::subscription_start::StartProto as Start;
    }
}

#[path = "src/codegen.rs"]
mod codegen;
#[path = "src/contract.rs"]
mod contract;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-check-cfg=cfg(kani)");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=src/contract.rs");
    println!("cargo:rerun-if-changed=src/contract_definitions.rs");
    println!("cargo:rerun-if-changed=src/domain");
    println!("cargo:rerun-if-changed=src/codegen.rs");
    let out_dir =
        std::env::var_os("OUT_DIR").ok_or_else(|| std::io::Error::other("OUT_DIR is not set"))?;
    codegen::generate(out_dir)?;
    Ok(())
}

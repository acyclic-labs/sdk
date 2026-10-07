//! Build the Actors descriptor and transport directly from the Rust contract.

/// Build-time marker used by fallible Protify contract conversions.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ContractError {
    /// A required value was absent or malformed.
    #[error("required field is absent or malformed")]
    InvalidArgument,
    /// A repeated field exceeded its contract limit.
    #[error("Actors v1 collection limit exceeded")]
    LimitExceeded,
    /// A repeated collection contained duplicate names.
    #[error("duplicate subscription or binding name")]
    DuplicateName,
}

impl From<std::convert::Infallible> for ContractError {
    fn from(value: std::convert::Infallible) -> Self {
        match value {}
    }
}

// The semantic source is included by the build-time schema module. These
// declarations satisfy its runtime admission hooks; the real validators live
// in the library crate and are never used while rendering metadata.
#[allow(
    unused_imports,
    reason = "Protify's build-time registration resolves these aliases from generated declarations"
)]
mod wire {
    pub use crate::contract::{
        ActorLimitsProto as ActorLimits, ActorObservationProto as ActorObservation,
        AddSubscriptionRequestProto as AddSubscriptionRequest,
        AddSubscriptionResponseProto as AddSubscriptionResponse, BindingProto as Binding,
        CheckpointActorRequestProto as CheckpointActorRequest,
        CheckpointActorResponseProto as CheckpointActorResponse,
        CreateActorRequestProto as CreateActorRequest,
        CreateActorResponseProto as CreateActorResponse, HeaderProto as Header,
        InspectActorRequestProto as InspectActorRequest,
        InspectActorResponseProto as InspectActorResponse,
        InvokeActorRequestProto as InvokeActorRequest,
        InvokeActorResponseProto as InvokeActorResponse,
        RemoveSubscriptionRequestProto as RemoveSubscriptionRequest,
        RemoveSubscriptionResponseProto as RemoveSubscriptionResponse,
        ResumeSubscriptionRequestProto as ResumeSubscriptionRequest,
        ResumeSubscriptionResponseProto as ResumeSubscriptionResponse, ServiceErrorProto as Error,
        SubscriptionObservationProto as SubscriptionObservation,
        SubscriptionSpecProto as SubscriptionSpec, SubscriptionStartProto as SubscriptionStart,
        UpdateActorRequestProto as UpdateActorRequest,
        UpdateActorResponseProto as UpdateActorResponse,
    };
    pub mod subscription_start {
        pub use crate::contract::subscription_start::StartProto as Start;
    }
}

#[allow(
    dead_code,
    reason = "The build-time Protify schema references these validator hooks by path"
)]
fn validate_create(_: &wire::CreateActorRequest) -> Result<(), ContractError> {
    Ok(())
}
#[allow(
    dead_code,
    reason = "The build-time Protify schema references these validator hooks by path"
)]
fn validate_update(_: &wire::UpdateActorRequest) -> Result<(), ContractError> {
    Ok(())
}
#[allow(
    dead_code,
    reason = "The build-time Protify schema references these validator hooks by path"
)]
fn validate_add_subscription(_: &wire::AddSubscriptionRequest) -> Result<(), ContractError> {
    Ok(())
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
    println!("cargo:rerun-if-changed=src/domain.rs");
    println!("cargo:rerun-if-changed=src/codegen.rs");
    let out_dir =
        std::env::var_os("OUT_DIR").ok_or_else(|| std::io::Error::other("OUT_DIR is not set"))?;
    codegen::generate(out_dir)?;
    Ok(())
}

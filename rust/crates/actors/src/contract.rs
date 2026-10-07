//! Rust-owned Actors v1 contract.
//!
//! Protify derives the prost wire implementation and the complete protobuf
//! schema from these declarations. The tonic service facade below remains a
//! generated transport adapter and consumes these same message types.

use protify::*;
use ts_rs::TS;

mod generated {
    #![allow(missing_docs)]

    use super::*;

    include!("contract_definitions.rs");
}

#[doc = "Actors protobuf package schema handle."]
pub use generated::ACTORS_PACKAGE;

/// Renders the canonical Actors protobuf input for the maintained prost/tonic
/// build. The rendered file is an intermediate artifact; these Rust
/// declarations remain the contract authority.
pub fn render_proto_files(root: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    let root = root.as_ref();
    std::fs::create_dir_all(root.join("actors/v1"))?;
    ACTORS_PACKAGE::get_package().render_files(root)
}

#[allow(unused_imports)]
pub use generated::{
    subscription_start,
    Binding,
    ActorLimits,
    SubscriptionStart,
    SubscriptionSpec,
    SubscriptionState,
    SubscriptionObservation,
    ActorState,
    ActorObservation,
    CreateActorRequest,
    CreateActorResponse,
    UpdateActorRequest,
    UpdateActorResponse,
    InspectActorRequest,
    InspectActorResponse,
    AddSubscriptionRequest,
    AddSubscriptionResponse,
    RemoveSubscriptionRequest,
    RemoveSubscriptionResponse,
    ResumeSubscriptionRequest,
    ResumeSubscriptionResponse,
    CheckpointActorRequest,
    CheckpointActorResponse,
    Header,
    InvokeActorRequest,
    InvokeActorResponse,
    ErrorCode,
    Error
};

/// Actors service operations generated from the protobuf contract.
#[allow(unused_imports)]
pub use generated::ActorsService;


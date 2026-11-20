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
    EgressRuleProto as EgressRule, EntrySourceProto as EntrySource, ErrorCode,
    FetchBindingProto as FetchBinding, ForkActorRequestProto as ForkActorRequest,
    ForkActorResponseProto as ForkActorResponse, HeaderProto as Header,
    InspectActorRequestProto as InspectActorRequest,
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

/// Reserved reply header marking a committed guest failure. Its only value is
/// `GUEST_FAILURE_VERSION`. The guest response encoder refuses it on a guest's
/// own HTTP response, so an ordinary guest 5xx is never read as a guest failure.
pub const GUEST_FAILURE_HEADER: &str = "acyclic-guest-failure";
/// Format version of a guest failure reply body.
const GUEST_FAILURE_VERSION: &str = "1";
const GUEST_FAILURE_STATUS: u32 = 500;

/// A committed guest failure as an Actors reply: status 500, exactly the
/// reserved [`GUEST_FAILURE_HEADER`], and the canonical `GuestError` JSON
/// category as body. A retained local cause contributes only its
/// classification, never its rendered source.
#[must_use]
pub fn guest_failure_reply(error: &crate::guest::GuestError) -> InvokeActorResponse {
    InvokeActorResponse {
        status: GUEST_FAILURE_STATUS,
        // A GuestError category always encodes; an empty body would be refused
        // by `guest_failure`, never read as another category.
        body: serde_json::to_vec(error).unwrap_or_default(),
        headers: vec![Header {
            name: GUEST_FAILURE_HEADER.into(),
            value: GUEST_FAILURE_VERSION.into(),
        }],
        ..Default::default()
    }
}

/// The guest failure `response` carries, when it is exactly a
/// [`guest_failure_reply`]: status 500, only the reserved header at its one
/// version, and a body that is the canonical encoding of its category.
/// Any other reply, including an ordinary guest 5xx, is `None`.
#[must_use]
pub fn guest_failure(response: &InvokeActorResponse) -> Option<crate::guest::GuestError> {
    let [header] = response.headers.as_slice() else {
        return None;
    };
    if response.status != GUEST_FAILURE_STATUS
        || header.name != GUEST_FAILURE_HEADER
        || header.value != GUEST_FAILURE_VERSION
    {
        return None;
    }
    let error: crate::guest::GuestError = serde_json::from_slice(&response.body).ok()?;
    (serde_json::to_vec(&error).ok()? == response.body).then_some(error)
}

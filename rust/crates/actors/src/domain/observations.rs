use super::*;

/// Lossless semantic view of a subscription observation.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/SubscriptionObservation.ts")]
#[ts(rename_all = "camelCase")]
pub struct SubscriptionObservation {
    #[wire(tag = 1)]

    subscription_id: String,
    #[wire(tag = 2)]
    stream_path: String,
    #[wire(tag = 3, enumeration)]
    state: SubscriptionState,
    #[ts(type = "bigint")]
    #[wire(tag = 4)]
    delivered_cursor: u64,
    #[ts(type = "bigint")]
    #[wire(tag = 5)]
    completed_cursor: u64,
    #[ts(type = "bigint")]
    #[wire(tag = 6)]
    recoverable_cursor: u64,
    #[wire(tag = 7)]
    placement_anchor: bool,
    #[wire(tag = 8)]
    retry_count: u32,
    #[wire(tag = 9)]
    failure_code: String,
    #[ts(optional)]
    #[wire(tag = 10)]
    failed_cursor: Option<u64>,
}

impl SubscriptionObservation {
    /// Returns the subscription identifier.
    pub fn subscription_id(&self) -> &str { &self.subscription_id }
    /// Returns the stream path.
    pub fn stream_path(&self) -> &str { &self.stream_path }
    /// Returns the typed subscription state.
    pub fn state(&self) -> SubscriptionState { self.state }
    /// Returns the delivered cursor without narrowing its `u64` range.
    pub fn delivered_cursor(&self) -> u64 { self.delivered_cursor }
    /// Returns the completed cursor without narrowing its `u64` range.
    pub fn completed_cursor(&self) -> u64 { self.completed_cursor }
    /// Returns the recoverable cursor without narrowing its `u64` range.
    pub fn recoverable_cursor(&self) -> u64 { self.recoverable_cursor }
    /// Returns whether this subscription is the placement anchor.
    pub fn placement_anchor(&self) -> bool { self.placement_anchor }
    /// Returns the number of recorded retries.
    pub fn retry_count(&self) -> u32 { self.retry_count }
    /// Returns the lossless failure code spelling.
    pub fn failure_code(&self) -> &str { &self.failure_code }
    /// Returns the optional failed cursor.
    pub fn failed_cursor(&self) -> Option<u64> { self.failed_cursor }
}

/// Lossless semantic view of a server Actor observation.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/ActorObservation.ts")]
#[ts(rename_all = "camelCase")]
pub struct ActorObservation {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
    #[wire(tag = 2, bytes)]
    code_sha256: CodeSha256,
    #[wire(tag = 3)]
    home_region: String,
    #[wire(tag = 4, enumeration)]
    state: ActorState,
    #[wire(tag = 5, message)]
    subscriptions: Vec<SubscriptionObservation>,
    #[ts(optional)]
    #[wire(tag = 6)]
    checkpoint_unix_millis: Option<u64>,
    #[ts(type = "bigint")]
    #[wire(tag = 7)]
    checkpoint_epoch: u64,
    #[ts(type = "bigint")]
    #[wire(tag = 8)]
    configuration_revision: u64,
}

impl ActorObservation {
    /// Returns the validated Actor identity.
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
    /// Returns the validated code digest.
    pub fn code_sha256(&self) -> &CodeSha256 { &self.code_sha256 }
    /// Returns the home region spelling.
    pub fn home_region(&self) -> &str { &self.home_region }
    /// Returns the typed Actor state.
    pub fn state(&self) -> ActorState { self.state }
    /// Returns the observed subscriptions in wire order.
    pub fn subscriptions(&self) -> &[SubscriptionObservation] { &self.subscriptions }
    /// Returns the optional checkpoint timestamp in Unix milliseconds.
    pub fn checkpoint_unix_millis(&self) -> Option<u64> { self.checkpoint_unix_millis }
    /// Returns the checkpoint epoch.
    pub fn checkpoint_epoch(&self) -> u64 { self.checkpoint_epoch }
    /// Returns the configuration revision.
    pub fn configuration_revision(&self) -> u64 { self.configuration_revision }
}

macro_rules! actor_response_type {
    ($name:ident, $wire:ident) => {
	        #[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

        #[derive(Clone, Debug, Eq, PartialEq, TS)]
        #[ts(export_to = concat!("actors/", stringify!($name), ".ts"))]
        #[ts(rename_all = "camelCase")]
        #[doc = "Typed response preserving the optional server Actor observation."]
        pub struct $name {
            #[wire(tag = 1, message)]
            #[ts(optional)]
            actor: Option<ActorObservation>,
        }

        impl $name {
            /// Returns the optional Actor observation carried by this response.
            pub fn actor(&self) -> Option<&ActorObservation> {
                self.actor.as_ref()
            }
        }

    };
}

actor_response_type!(CreateActorResponse, CreateActorResponse);
actor_response_type!(UpdateActorResponse, UpdateActorResponse);
actor_response_type!(InspectActorResponse, InspectActorResponse);
actor_response_type!(AddSubscriptionResponse, AddSubscriptionResponse);
actor_response_type!(RemoveSubscriptionResponse, RemoveSubscriptionResponse);
actor_response_type!(ResumeSubscriptionResponse, ResumeSubscriptionResponse);
actor_response_type!(CheckpointActorResponse, CheckpointActorResponse);

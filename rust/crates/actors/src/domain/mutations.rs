use super::*;

/// Create request after the canonical admission validator has run.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = CreateActorRequest::validate_from_proto)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/CreateActorRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct CreateActorRequest {
    #[wire(tag = 1, bytes)]

    code_sha256: CodeSha256,
    #[wire(tag = 2)]
    home_region: String,
    #[wire(tag = 3, message)]
    bindings: Vec<Binding>,
    #[wire(tag = 4, message)]
    limits: ActorLimits,
    #[wire(tag = 5, message)]
    subscriptions: Vec<SubscriptionSpec>,
    #[wire(tag = 6)]
    idempotency_key: String,
}

impl CreateActorRequest {
    /// Builds and validates a create request through the canonical admission path.
    pub fn new(
        code_sha256: CodeSha256,
        home_region: String,
        bindings: Vec<Binding>,
        limits: ActorLimits,
        subscriptions: Vec<SubscriptionSpec>,
        idempotency_key: String,
    ) -> Result<Self, DomainError> {
        let wire = wire::CreateActorRequest {
            code_sha256: code_sha256.as_bytes().to_vec(),
            home_region,
            bindings: bindings.into_iter().map(Into::into).collect(),
            limits: Some(limits.into()),
            subscriptions: subscriptions.into_iter().map(Into::into).collect(),
            idempotency_key,
        };
        Self::try_from(wire)
    }

    /// Re-runs the canonical request validator after converting untrusted wire
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        let wire: wire::CreateActorRequest = self.clone().into();
        crate::validate_create(&wire).map_err(DomainError::Contract)
    }

    /// Returns the code digest.
    pub fn code_sha256(&self) -> &CodeSha256 { &self.code_sha256 }
    /// Returns the home region.
    pub fn home_region(&self) -> &str { &self.home_region }
    /// Returns bindings in their request order.
    pub fn bindings(&self) -> &[Binding] { &self.bindings }
    /// Returns the validated resource limits.
    pub fn limits(&self) -> &ActorLimits { &self.limits }
    /// Returns subscriptions in their request order.
    pub fn subscriptions(&self) -> &[SubscriptionSpec] { &self.subscriptions }
    /// Returns the idempotency key.
    pub fn idempotency_key(&self) -> &str { &self.idempotency_key }
}

/// Update request after the canonical admission validator has run.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = UpdateActorRequest::validate_from_proto)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/UpdateActorRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct UpdateActorRequest {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
    #[wire(tag = 2, bytes)]
    code_sha256: CodeSha256,
    #[wire(tag = 3, message)]
    bindings: Vec<Binding>,
    #[wire(tag = 4, message)]
    limits: ActorLimits,
    #[ts(type = "bigint")]
    #[wire(tag = 5)]
    expected_configuration_revision: u64,
    #[wire(tag = 6)]
    idempotency_key: String,
}

impl UpdateActorRequest {
    /// Builds and validates an update request through the canonical admission path.
    pub fn new(
        actor_id: ActorId,
        code_sha256: CodeSha256,
        bindings: Vec<Binding>,
        limits: ActorLimits,
        expected_configuration_revision: u64,
        idempotency_key: String,
    ) -> Result<Self, DomainError> {
        let wire = wire::UpdateActorRequest {
            actor_id: actor_id.as_str().to_owned(),
            code_sha256: code_sha256.as_bytes().to_vec(),
            bindings: bindings.into_iter().map(Into::into).collect(),
            limits: Some(limits.into()),
            expected_configuration_revision,
            idempotency_key,
        };
        Self::try_from(wire)
    }

    /// Re-runs the canonical request validator without recursively invoking
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        let wire: wire::UpdateActorRequest = self.clone().into();
        crate::validate_update(&wire).map_err(DomainError::Contract)
    }

    /// Returns the Actor identity.
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
    /// Returns the code digest.
    pub fn code_sha256(&self) -> &CodeSha256 { &self.code_sha256 }
    /// Returns bindings in their request order.
    pub fn bindings(&self) -> &[Binding] { &self.bindings }
    /// Returns the validated resource limits.
    pub fn limits(&self) -> &ActorLimits { &self.limits }
    /// Returns the expected compare-and-replace revision.
    pub fn expected_configuration_revision(&self) -> u64 {
        self.expected_configuration_revision
    }
    /// Returns the idempotency key.
    pub fn idempotency_key(&self) -> &str { &self.idempotency_key }
}

/// Inspect currently has no additional wire validator; only the semantic
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/InspectActorRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct InspectActorRequest {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
}

impl InspectActorRequest {
    /// Creates an inspect request for a validated Actor identity.
    pub fn new(actor_id: ActorId) -> Self { Self { actor_id } }
    /// Returns the Actor identity.
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
}

/// Add subscription request after the canonical admission validator has run.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = AddSubscriptionRequest::validate_from_proto)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/AddSubscriptionRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct AddSubscriptionRequest {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
    #[wire(tag = 2, message)]
    subscription: SubscriptionSpec,
    #[wire(tag = 3)]
    idempotency_key: String,
}

impl AddSubscriptionRequest {
    /// Builds and validates an add-subscription request through the canonical path.
    pub fn new(
        actor_id: ActorId,
        subscription: SubscriptionSpec,
        idempotency_key: String,
    ) -> Result<Self, DomainError> {
        let wire = wire::AddSubscriptionRequest {
            actor_id: actor_id.as_str().to_owned(),
            subscription: Some(subscription.into()),
            idempotency_key,
        };
        Self::try_from(wire)
    }

    /// Re-runs the canonical request validator without recursively invoking
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        let wire: wire::AddSubscriptionRequest = self.clone().into();
        crate::validate_add_subscription(&wire).map_err(DomainError::Contract)
    }

    /// Returns the Actor identity.
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
    /// Returns the validated subscription.
    pub fn subscription(&self) -> &SubscriptionSpec { &self.subscription }
    /// Returns the idempotency key.
    pub fn idempotency_key(&self) -> &str { &self.idempotency_key }
}

/// Removal has no canonical validator yet, so subscription and idempotency
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/RemoveSubscriptionRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct RemoveSubscriptionRequest {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
    #[wire(tag = 2)]
    subscription_id: String,
    #[wire(tag = 3)]
    idempotency_key: String,
}

impl RemoveSubscriptionRequest {
    /// Creates a removal request; no additional canonical validator exists yet.
    pub fn new(actor_id: ActorId, subscription_id: String, idempotency_key: String) -> Self {
        Self {
            actor_id,
            subscription_id,
            idempotency_key,
        }
    }
    /// Returns the Actor identity.
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
    /// Returns the subscription identifier.
    pub fn subscription_id(&self) -> &str { &self.subscription_id }
    /// Returns the idempotency key.
    pub fn idempotency_key(&self) -> &str { &self.idempotency_key }
}

/// Resume has no canonical validator yet; the wire strings stay unbranded.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/ResumeSubscriptionRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct ResumeSubscriptionRequest {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
    #[wire(tag = 2)]
    subscription_id: String,
    #[wire(tag = 3)]
    idempotency_key: String,
}

impl ResumeSubscriptionRequest {
    /// Creates a resume request; no additional canonical validator exists yet.
    pub fn new(actor_id: ActorId, subscription_id: String, idempotency_key: String) -> Self {
        Self {
            actor_id,
            subscription_id,
            idempotency_key,
        }
    }
    /// Returns the Actor identity.
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
    /// Returns the subscription identifier.
    pub fn subscription_id(&self) -> &str { &self.subscription_id }
    /// Returns the idempotency key.
    pub fn idempotency_key(&self) -> &str { &self.idempotency_key }
}

/// Checkpoint has no canonical validator yet; only the Actor identity rule is
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/CheckpointActorRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct CheckpointActorRequest {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
    #[wire(tag = 2)]
    idempotency_key: String,
}

impl CheckpointActorRequest {
    /// Creates a checkpoint request for a validated Actor identity.
    pub fn new(actor_id: ActorId, idempotency_key: String) -> Self {
        Self {
            actor_id,
            idempotency_key,
        }
    }
    /// Returns the Actor identity.
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
    /// Returns the idempotency key.
    pub fn idempotency_key(&self) -> &str { &self.idempotency_key }
}

/// Deletion request. The canonical validator requires an Actor identity and a
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = DeleteActorRequest::validate_from_proto)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/DeleteActorRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct DeleteActorRequest {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
    #[wire(tag = 2)]
    idempotency_key: String,
}

impl DeleteActorRequest {
    /// Builds and validates a deletion request through the canonical admission path.
    pub fn new(actor_id: ActorId, idempotency_key: String) -> Result<Self, DomainError> {
        Self::try_from(wire::DeleteActorRequest {
            actor_id: actor_id.as_str().to_owned(),
            idempotency_key,
        })
    }

    /// Re-runs the canonical request validator without recursively invoking
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        let wire: wire::DeleteActorRequest = self.clone().into();
        crate::validate_delete(&wire).map_err(DomainError::Contract)
    }

    /// Returns the Actor identity.
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
    /// Returns the idempotency key.
    pub fn idempotency_key(&self) -> &str { &self.idempotency_key }
}

/// Empty deletion acknowledgement. After it, every operation on the deleted
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, Default, Eq, PartialEq, TS)]
#[ts(export_to = "actors/DeleteActorResponse.ts")]
#[ts(type = "Record<never, never>")]
pub struct DeleteActorResponse {}

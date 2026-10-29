use super::*;

/// Deployment request after the canonical admission validator has run.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = DeploymentRequest::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/DeploymentRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct DeploymentRequest {
    #[wire(tag = 1, bytes)]
    code_sha256: CodeSha256,
    #[wire(tag = 3, message)]
    bindings: Vec<Binding>,
    #[wire(tag = 4, message)]
    #[ts(optional)]
    limits: Option<ActorLimits>,
    #[wire(tag = 5, message)]
    subscriptions: Vec<SubscriptionSpec>,
    #[wire(tag = 6)]
    idempotency_key: String,
    #[wire(tag = 7, string)]
    name: ActorNamespace,
    #[wire(tag = 8, bytes)]
    #[ts(type = r#"import("../../readonly.js").ReadonlyBytes"#)]
    pub(super) account_id: Vec<u8>,
    #[wire(tag = 9)]
    jurisdiction: String,
    #[wire(tag = 10)]
    #[ts(type = "bigint")]
    expected_revision: u64,
    #[wire(tag = 11, message)]
    #[ts(optional)]
    worker_limits: Option<WorkerLimits>,
    #[wire(tag = 12, message)]
    secrets: Vec<SecretSelection>,
    #[wire(tag = 13, message)]
    #[ts(optional)]
    fetch: Option<FetchBinding>,
}

impl DeploymentRequest {
    /// Builds and validates a namespace deployment through the canonical admission path.
    pub fn new(
        name: ActorNamespace,
        code_sha256: CodeSha256,
        bindings: Vec<Binding>,
        limits: ActorLimits,
        subscriptions: Vec<SubscriptionSpec>,
        idempotency_key: String,
    ) -> Result<Self, DomainError> {
        let wire = wire::DeploymentRequest {
            worker_limits: None,
            name: name.as_str().to_owned(),
            account_id: Vec::new(),
            jurisdiction: String::new(),
            expected_revision: 0,
            code_sha256: code_sha256.as_bytes().to_vec(),
            bindings: bindings.into_iter().map(Into::into).collect(),
            secrets: Vec::new(),
            fetch: None,
            limits: Some(limits.into()),
            subscriptions: subscriptions.into_iter().map(Into::into).collect(),
            idempotency_key,
        };
        Self::try_from(wire)
    }

    /// Build a named Worker deployment with request ceilings and no actor configuration.
    pub fn worker(
        name: ActorNamespace,
        code_sha256: CodeSha256,
        bindings: Vec<Binding>,
        limits: WorkerLimits,
        idempotency_key: String,
    ) -> Result<Self, DomainError> {
        crate::addressing::validate_dns_label(name.as_str().as_bytes())
            .map_err(|_| DomainError::Contract(crate::ContractError::InvalidArgument))?;
        Self::try_from(wire::DeploymentRequest {
            code_sha256: code_sha256.as_bytes().to_vec(),
            bindings: bindings.into_iter().map(Into::into).collect(),
            secrets: Vec::new(),
            fetch: None,
            limits: None,
            worker_limits: Some(limits.into()),
            subscriptions: vec![],
            idempotency_key,
            name: name.as_str().to_owned(),
            account_id: vec![],
            jurisdiction: String::new(),
            expected_revision: 0,
        })
    }

    /// Exact deployment name; never an actor instance ID.
    pub fn name(&self) -> &ActorNamespace {
        &self.name
    }
    /// Namespace placement restriction; empty permits any jurisdiction.
    pub fn jurisdiction(&self) -> &str {
        &self.jurisdiction
    }
    /// Set a namespace restriction while retaining the authenticated account.
    pub fn with_jurisdiction(mut self, jurisdiction: String) -> Result<Self, DomainError> {
        if !matches!(jurisdiction.as_str(), "" | "eu" | "us" | "fedramp") {
            return Err(DomainError::Contract(crate::ContractError::InvalidArgument));
        }
        self.jurisdiction = jurisdiction;
        self.validate_from_proto()?;
        Ok(self)
    }
    /// Set the account, jurisdiction and compare-and-replace deployment revision.
    pub fn with_placement(
        mut self,
        account: Vec<u8>,
        jurisdiction: String,
        expected_revision: u64,
    ) -> Result<Self, DomainError> {
        crate::addressing::AccountId::new(&account)?;
        if !matches!(jurisdiction.as_str(), "" | "eu" | "us" | "fedramp") {
            return Err(DomainError::Contract(crate::ContractError::InvalidArgument));
        }
        self.account_id = account;
        self.jurisdiction = jurisdiction;
        self.expected_revision = expected_revision;
        self.validate_from_proto()?;
        Ok(self)
    }
    /// Re-runs the canonical request validator after converting untrusted wire
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        let wire: wire::DeploymentRequest = self.clone().into();
        crate::validate_deployment(&wire).map_err(DomainError::Contract)
    }

    /// Returns the code digest.
    pub fn code_sha256(&self) -> &CodeSha256 {
        &self.code_sha256
    }
    /// Explicit full replacement; an empty selection inherits nothing.
    pub fn with_secrets(mut self, secrets: Vec<SecretSelection>) -> Result<Self, DomainError> {
        self.secrets = secrets;
        self.validate_from_proto()?;
        Ok(self)
    }
    /// Ordered reference selections, never credential values.
    pub fn secrets(&self) -> &[SecretSelection] {
        &self.secrets
    }
    /// Full replacement of outbound HTTPS authority; None revokes the handle.
    pub fn with_fetch(mut self, fetch: Option<FetchBinding>) -> Result<Self, DomainError> {
        self.fetch = fetch;
        self.validate_from_proto()?;
        Ok(self)
    }
    /// The explicitly selected outbound HTTPS handle.
    pub fn fetch(&self) -> Option<&FetchBinding> {
        self.fetch.as_ref()
    }
    /// Returns bindings in their request order.
    pub fn bindings(&self) -> &[Binding] {
        &self.bindings
    }
    /// Returns the validated resource limits.
    pub fn limits(&self) -> Option<&ActorLimits> {
        self.limits.as_ref()
    }
    /// Stateless request limits, present only for Worker deployments.
    pub fn worker_limits(&self) -> Option<&WorkerLimits> {
        self.worker_limits.as_ref()
    }
    /// Returns subscriptions in their request order.
    pub fn subscriptions(&self) -> &[SubscriptionSpec] {
        &self.subscriptions
    }
    /// Returns the idempotency key.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// Update request after the canonical admission validator has run.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = UpdateNamespaceRequest::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/UpdateNamespaceRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct UpdateNamespaceRequest {
    #[wire(tag = 1, string)]
    namespace_id: ActorNamespace,
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

    #[wire(tag = 91, bytes)]
    #[ts(type = r#"import("../../readonly.js").ReadonlyBytes"#)]
    pub(super) account_id: Vec<u8>,
    #[wire(tag = 7, message)]
    secrets: Vec<SecretSelection>,
    #[wire(tag = 8, message)]
    #[ts(optional)]
    fetch: Option<FetchBinding>,
}

impl UpdateNamespaceRequest {
    /// Builds and validates an update request through the canonical admission path.
    pub fn new(
        namespace_id: ActorNamespace,
        code_sha256: CodeSha256,
        bindings: Vec<Binding>,
        limits: ActorLimits,
        expected_configuration_revision: u64,
        idempotency_key: String,
    ) -> Result<Self, DomainError> {
        let wire = wire::UpdateNamespaceRequest {
            account_id: Vec::new(),
            namespace_id: namespace_id.as_str().to_owned(),
            code_sha256: code_sha256.as_bytes().to_vec(),
            bindings: bindings.into_iter().map(Into::into).collect(),
            secrets: Vec::new(),
            fetch: None,
            limits: Some(limits.into()),
            expected_configuration_revision,
            idempotency_key,
        };
        Self::try_from(wire)
    }

    /// Re-runs the canonical request validator without recursively invoking
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        let wire: wire::UpdateNamespaceRequest = self.clone().into();
        crate::validate_update(&wire).map_err(DomainError::Contract)
    }

    /// Returns the Actor identity.
    pub fn namespace_id(&self) -> &ActorNamespace {
        &self.namespace_id
    }
    /// Returns the code digest.
    pub fn code_sha256(&self) -> &CodeSha256 {
        &self.code_sha256
    }
    /// Explicit full replacement; an empty selection inherits nothing.
    pub fn with_secrets(mut self, secrets: Vec<SecretSelection>) -> Result<Self, DomainError> {
        self.secrets = secrets;
        self.validate_from_proto()?;
        Ok(self)
    }
    /// Ordered reference selections, never credential values.
    pub fn secrets(&self) -> &[SecretSelection] {
        &self.secrets
    }
    /// Full replacement of outbound HTTPS authority; None revokes the handle.
    pub fn with_fetch(mut self, fetch: Option<FetchBinding>) -> Result<Self, DomainError> {
        self.fetch = fetch;
        self.validate_from_proto()?;
        Ok(self)
    }
    /// The explicitly selected outbound HTTPS handle.
    pub fn fetch(&self) -> Option<&FetchBinding> {
        self.fetch.as_ref()
    }
    /// Returns bindings in their request order.
    pub fn bindings(&self) -> &[Binding] {
        &self.bindings
    }
    /// Returns the validated resource limits.
    pub fn limits(&self) -> &ActorLimits {
        &self.limits
    }
    /// Returns the expected compare-and-replace revision.
    pub fn expected_configuration_revision(&self) -> u64 {
        self.expected_configuration_revision
    }
    /// Returns the idempotency key.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// Inspect currently has no additional wire validator; only the semantic
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/InspectActorRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct InspectActorRequest {
    #[wire(tag = 1, string)]
    pub(super) actor_id: ActorId,

    #[wire(tag = 90, string)]
    #[ts(optional)]
    pub(super) namespace: Option<ActorNamespace>,
    #[wire(tag = 91, bytes)]
    #[ts(type = r#"import("../../readonly.js").ReadonlyBytes"#)]
    pub(super) account_id: Vec<u8>,
}

impl InspectActorRequest {
    /// Creates an inspect request for a validated Actor identity.
    pub fn new(actor_id: ActorId) -> Self {
        Self {
            actor_id,
            namespace: None,
            account_id: Vec::new(),
        }
    }
    /// Returns the Actor identity.
    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }
}

/// Add subscription request after the canonical admission validator has run.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = AddSubscriptionRequest::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/AddSubscriptionRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct AddSubscriptionRequest {
    #[wire(tag = 1, string)]
    pub(super) actor_id: ActorId,
    #[wire(tag = 2, message)]
    subscription: SubscriptionSpec,
    #[wire(tag = 3)]
    idempotency_key: String,

    #[wire(tag = 90, string)]
    #[ts(optional)]
    pub(super) namespace: Option<ActorNamespace>,
    #[wire(tag = 91, bytes)]
    #[ts(type = r#"import("../../readonly.js").ReadonlyBytes"#)]
    pub(super) account_id: Vec<u8>,
}

impl AddSubscriptionRequest {
    /// Builds and validates an add-subscription request through the canonical path.
    pub fn new(
        actor_id: ActorId,
        subscription: SubscriptionSpec,
        idempotency_key: String,
    ) -> Result<Self, DomainError> {
        let wire = wire::AddSubscriptionRequest {
            namespace: None,
            account_id: Vec::new(),
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
    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }
    /// Returns the validated subscription.
    pub fn subscription(&self) -> &SubscriptionSpec {
        &self.subscription
    }
    /// Returns the idempotency key.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// Removal has no canonical validator yet, so subscription and idempotency
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/RemoveSubscriptionRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct RemoveSubscriptionRequest {
    #[wire(tag = 1, string)]
    pub(super) actor_id: ActorId,
    #[wire(tag = 2)]
    subscription_id: String,
    #[wire(tag = 3)]
    idempotency_key: String,

    #[wire(tag = 90, string)]
    #[ts(optional)]
    pub(super) namespace: Option<ActorNamespace>,
    #[wire(tag = 91, bytes)]
    #[ts(type = r#"import("../../readonly.js").ReadonlyBytes"#)]
    pub(super) account_id: Vec<u8>,
}

impl RemoveSubscriptionRequest {
    /// Creates a removal request; no additional canonical validator exists yet.
    pub fn new(actor_id: ActorId, subscription_id: String, idempotency_key: String) -> Self {
        Self {
            namespace: None,
            account_id: Vec::new(),
            actor_id,
            subscription_id,
            idempotency_key,
        }
    }
    /// Returns the Actor identity.
    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }
    /// Returns the subscription identifier.
    pub fn subscription_id(&self) -> &str {
        &self.subscription_id
    }
    /// Returns the idempotency key.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// Resume has no canonical validator yet; the wire strings stay unbranded.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/ResumeSubscriptionRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct ResumeSubscriptionRequest {
    #[wire(tag = 1, string)]
    pub(super) actor_id: ActorId,
    #[wire(tag = 2)]
    subscription_id: String,
    #[wire(tag = 3)]
    idempotency_key: String,

    #[wire(tag = 90, string)]
    #[ts(optional)]
    pub(super) namespace: Option<ActorNamespace>,
    #[wire(tag = 91, bytes)]
    #[ts(type = r#"import("../../readonly.js").ReadonlyBytes"#)]
    pub(super) account_id: Vec<u8>,
}

impl ResumeSubscriptionRequest {
    /// Creates a resume request; no additional canonical validator exists yet.
    pub fn new(actor_id: ActorId, subscription_id: String, idempotency_key: String) -> Self {
        Self {
            namespace: None,
            account_id: Vec::new(),
            actor_id,
            subscription_id,
            idempotency_key,
        }
    }
    /// Returns the Actor identity.
    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }
    /// Returns the subscription identifier.
    pub fn subscription_id(&self) -> &str {
        &self.subscription_id
    }
    /// Returns the idempotency key.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// Checkpoint has no canonical validator yet; only the Actor identity rule is
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/CheckpointActorRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct CheckpointActorRequest {
    #[wire(tag = 1, string)]
    pub(super) actor_id: ActorId,
    #[wire(tag = 2)]
    idempotency_key: String,

    #[wire(tag = 90, string)]
    #[ts(optional)]
    pub(super) namespace: Option<ActorNamespace>,
    #[wire(tag = 91, bytes)]
    #[ts(type = r#"import("../../readonly.js").ReadonlyBytes"#)]
    pub(super) account_id: Vec<u8>,
}

impl CheckpointActorRequest {
    /// Creates a checkpoint request for a validated Actor identity.
    pub fn new(actor_id: ActorId, idempotency_key: String) -> Self {
        Self {
            namespace: None,
            account_id: Vec::new(),
            actor_id,
            idempotency_key,
        }
    }
    /// Returns the Actor identity.
    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }
    /// Returns the idempotency key.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// Deletion request. The canonical validator requires an Actor identity and a
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = DeleteAllRequest::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/DeleteAllRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct DeleteAllRequest {
    #[wire(tag = 1, string)]
    pub(super) actor_id: ActorId,
    #[wire(tag = 2)]
    idempotency_key: String,

    #[wire(tag = 90, string)]
    #[ts(optional)]
    pub(super) namespace: Option<ActorNamespace>,
    #[wire(tag = 91, bytes)]
    #[ts(type = r#"import("../../readonly.js").ReadonlyBytes"#)]
    pub(super) account_id: Vec<u8>,
}

impl DeleteAllRequest {
    /// Builds and validates a deletion request through the canonical admission path.
    pub fn new(actor_id: ActorId, idempotency_key: String) -> Result<Self, DomainError> {
        Self::try_from(wire::DeleteAllRequest {
            namespace: None,
            account_id: Vec::new(),
            actor_id: actor_id.as_str().to_owned(),
            idempotency_key,
        })
    }

    /// Re-runs the canonical request validator without recursively invoking
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        let wire: wire::DeleteAllRequest = self.clone().into();
        crate::validate_delete(&wire).map_err(DomainError::Contract)
    }

    /// Returns the Actor identity.
    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }
    /// Returns the idempotency key.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// Empty deletion acknowledgement. After it, every operation on the deleted
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, Default, Eq, PartialEq, TS)]
#[ts(export_to = "actors/DeleteAllResponse.ts")]
#[ts(type = "Record<never, never>")]
pub struct DeleteAllResponse {}

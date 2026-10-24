use super::*;

/// Typed invocation request. Method, URL, headers, and body preserve the
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = InvokeActorRequest::validate_from_proto)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/InvokeActorRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct InvokeActorRequest {
    #[wire(tag = 1, string)]
    pub(super) actor_id: ActorId,
    #[wire(tag = 2)]
    method: String,
    #[wire(tag = 3)]
    url: String,
    #[wire(tag = 4, bytes)]
    #[ts(type = "import(\"../../readonly.js\").ReadonlyBytes")]
    body: Vec<u8>,
    #[wire(tag = 5, message)]
    headers: Vec<Header>,

    #[wire(tag = 90, string)]
    #[ts(optional)]
    pub(super) namespace: Option<ActorNamespace>,
    #[wire(tag = 91, bytes)]
    #[ts(type = r#"import("../../readonly.js").ReadonlyBytes"#)]
    pub(super) account_id: Vec<u8>,
    #[wire(tag = 92)]
    #[ts(optional)]
    pub(super) location_hint: Option<String>,
    #[wire(tag = 93)]
    #[ts(optional)]
    pub(super) binding_name: Option<String>,
    #[wire(tag = 94)]
    #[ts(optional)]
    pub(super) rpc_method: Option<String>,
}

impl InvokeActorRequest {
    /// Creates an invocation request while preserving bytes and header order.
    pub fn new(
        actor_id: ActorId,
        method: String,
        url: String,
        body: Vec<u8>,
        headers: Vec<Header>,
    ) -> Self {
        Self {
            namespace: None,
            account_id: Vec::new(),
            actor_id,
            location_hint: None,
            binding_name: None,
            rpc_method: None,
            method,
            url,
            body,
            headers,
        }
    }
    /// Returns the Actor identity.
    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }
    /// Returns the invocation method.
    pub fn method(&self) -> &str {
        &self.method
    }
    /// Returns the invocation URL.
    pub fn url(&self) -> &str {
        &self.url
    }
    /// Returns the invocation body bytes.
    pub fn body(&self) -> &[u8] {
        &self.body
    }
    /// Returns headers in their original order.
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }
}

/// Typed invocation response with byte-preserving body and headers.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/InvokeActorResponse.ts")]
#[ts(rename_all = "camelCase")]
pub struct InvokeActorResponse {
    #[wire(tag = 1)]
    status: u32,
    #[wire(tag = 2, bytes)]
    #[ts(type = "import(\"../../readonly.js\").ReadonlyBytes")]
    body: Vec<u8>,
    #[wire(tag = 3, message)]
    headers: Vec<Header>,
}

impl InvokeActorResponse {
    /// Returns the HTTP-like status code.
    pub fn status(&self) -> u32 {
        self.status
    }
    /// Returns the response body bytes.
    pub fn body(&self) -> &[u8] {
        &self.body
    }
    /// Returns headers in their original order.
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }
}

/// Typed invocation request. Method, URL, headers, and body preserve the
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = InvokeWorkerRequest::validate_from_proto)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/InvokeWorkerRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct InvokeWorkerRequest {
    #[wire(tag = 1, string)]
    pub(super) worker_name: ActorNamespace,
    #[wire(tag = 2)]
    method: String,
    #[wire(tag = 3)]
    url: String,
    #[wire(tag = 4, bytes)]
    #[ts(type = "import(\"../../readonly.js\").ReadonlyBytes")]
    body: Vec<u8>,
    #[wire(tag = 5, message)]
    headers: Vec<Header>,

    #[wire(tag = 91, bytes)]
    #[ts(type = r#"import("../../readonly.js").ReadonlyBytes"#)]
    pub(super) account_id: Vec<u8>,
    #[wire(tag = 92)]
    #[ts(optional)]
    binding_name: Option<String>,
    #[wire(tag = 94)]
    #[ts(optional)]
    rpc_method: Option<String>,
}

impl InvokeWorkerRequest {
    /// Creates an invocation request while preserving bytes and header order.
    pub fn new(
        worker_name: ActorNamespace,
        method: String,
        url: String,
        body: Vec<u8>,
        headers: Vec<Header>,
    ) -> Result<Self, DomainError> {
        let request = Self {
            account_id: Vec::new(),
            worker_name,
            binding_name: None,
            rpc_method: None,
            method,
            url,
            body,
            headers,
        };
        request.validate_from_proto()?;
        Ok(request)
    }
    /// Returns the Worker deployment name.
    pub fn worker_name(&self) -> &ActorNamespace {
        &self.worker_name
    }
    /// Returns the invocation method.
    pub fn method(&self) -> &str {
        &self.method
    }
    /// Returns the invocation URL.
    pub fn url(&self) -> &str {
        &self.url
    }
    /// Returns the invocation body bytes.
    pub fn body(&self) -> &[u8] {
        &self.body
    }
    /// Returns headers in their original order.
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }
}

impl InvokeWorkerRequest {
    /// A named binding, admitted by the host, authorizes private RPC/fetch.
    pub fn with_binding(mut self, account: &[u8], binding: String) -> Result<Self, DomainError> {
        crate::addressing::AccountId::new(account)?;
        if binding.is_empty() {
            return Err(DomainError::InvalidBinding);
        }
        self.account_id = account.to_vec();
        self.binding_name = Some(binding);
        Ok(self)
    }
}

impl InvokeActorRequest {
    /// Select a host-admitted namespace binding; the name is not a capability.
    pub fn with_binding(mut self, name: String) -> Result<Self, DomainError> {
        if name.is_empty() {
            return Err(DomainError::InvalidBinding);
        }
        self.binding_name = Some(name);
        Ok(self)
    }
    /// Target an exported RPC method instead of the fetch handler.
    pub fn with_rpc(mut self, method: String) -> Result<Self, DomainError> {
        if method.is_empty() {
            return Err(DomainError::Contract(crate::ContractError::InvalidArgument));
        }
        self.rpc_method = Some(method);
        Ok(self)
    }
}
impl InvokeWorkerRequest {
    /// Target a private service's exported RPC method.
    pub fn with_rpc(mut self, method: String) -> Result<Self, DomainError> {
        if method.is_empty() {
            return Err(DomainError::Contract(crate::ContractError::InvalidArgument));
        }
        self.rpc_method = Some(method);
        Ok(self)
    }
}

impl InvokeActorRequest {
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        if let Some(hint) = &self.location_hint {
            crate::addressing::validate_location_hint(hint)?;
        }
        if self.binding_name.as_ref().is_some_and(String::is_empty)
            || self.rpc_method.as_ref().is_some_and(String::is_empty)
        {
            return Err(DomainError::Contract(crate::ContractError::InvalidArgument));
        }
        Ok(())
    }
}

impl InvokeWorkerRequest {
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        crate::addressing::validate_dns_label(self.worker_name.as_str().as_bytes())
            .map_err(|_| DomainError::InvalidBinding)?;
        if self.binding_name.as_ref().is_some_and(String::is_empty)
            || self.rpc_method.as_ref().is_some_and(String::is_empty)
        {
            return Err(DomainError::InvalidBinding);
        }
        Ok(())
    }
}

use super::*;

/// Typed invocation request. Method, URL, headers, and body preserve the
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/InvokeActorRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct InvokeActorRequest {
    #[wire(tag = 1, string)]
    actor_id: ActorId,
    #[wire(tag = 2)]
    method: String,
    #[wire(tag = 3)]
    url: String,
    #[wire(tag = 4, bytes)]
    #[ts(type = "import(\"../../readonly.js\").ReadonlyBytes")]
    body: Vec<u8>,
    #[wire(tag = 5, message)]
    headers: Vec<Header>,
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
            actor_id,
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

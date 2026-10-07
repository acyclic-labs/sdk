//! Isolated Diplomat 0.16.1 experiment over the Actors semantic domain.
//!
//! This crate owns no Actors fields. Opaque bridge values contain the real
//! `acyclic_actors::domain` values and constructors call canonical Rust APIs.

use acyclic_actors::{domain, wire};

#[diplomat::bridge]
mod ffi {
    use super::{domain, wire};

    pub enum ErrorKind {
        EmptyActorId,
        InvalidCodeSha256,
        InvalidContract,
        InvalidSubscription,
        InvalidBinding,
        MissingMessage,
        UnknownActorState,
        UnknownSubscriptionState,
        UnknownErrorCode,
    }

    #[diplomat::opaque]
    pub struct ActorsError {
        kind: ErrorKind,
        message: String,
    }

    impl ActorsError {
        fn from_domain(error: domain::DomainError) -> Box<Self> {
            let kind = match error {
                domain::DomainError::EmptyActorId => ErrorKind::EmptyActorId,
                domain::DomainError::InvalidCodeSha256 => ErrorKind::InvalidCodeSha256,
                domain::DomainError::Contract(_) => ErrorKind::InvalidContract,
                domain::DomainError::UnknownActorState(_) => ErrorKind::UnknownActorState,
                domain::DomainError::UnknownSubscriptionState(_) => ErrorKind::UnknownSubscriptionState,
                domain::DomainError::UnknownErrorCode(_) => ErrorKind::UnknownErrorCode,
                domain::DomainError::MissingMessage => ErrorKind::MissingMessage,
                domain::DomainError::InvalidSubscription => ErrorKind::InvalidSubscription,
                domain::DomainError::InvalidBinding => ErrorKind::InvalidBinding,
            };
            Box::new(Self { kind, message: error.to_string() })
        }
        pub fn kind(&self) -> ErrorKind { self.kind }
        pub fn message<'a>(&'a self) -> &'a str { &self.message }
    }

    #[diplomat::opaque]
    pub struct ActorId { inner: domain::ActorId }
    impl ActorId {
        pub fn new(value: &str) -> Result<Box<Self>, Box<ActorsError>> {
            domain::ActorId::new(value.to_owned()).map(|inner| Box::new(Self { inner })).map_err(ActorsError::from_domain)
        }
        pub fn value<'a>(&'a self) -> &'a str { self.inner.as_str() }
    }

    #[diplomat::opaque]
    pub struct CodeSha256 { inner: domain::CodeSha256 }
    impl CodeSha256 {
        pub fn new(bytes: &[u8]) -> Result<Box<Self>, Box<ActorsError>> {
            domain::CodeSha256::new(bytes.to_vec()).map(|inner| Box::new(Self { inner })).map_err(ActorsError::from_domain)
        }
        pub fn byte_at(&self, index: usize) -> u8 { self.inner.as_bytes().get(index).copied().unwrap_or(0) }
        pub fn len(&self) -> usize { 32 }
    }

    #[diplomat::opaque]
    pub struct ActorLimits { inner: domain::ActorLimits }
    impl ActorLimits {
        pub fn new(handler_timeout_millis: u64, memory_bytes: u64, checkpoint_bytes: u64) -> Result<Box<Self>, Box<ActorsError>> {
            domain::ActorLimits::new(handler_timeout_millis, memory_bytes, checkpoint_bytes).map(|inner| Box::new(Self { inner })).map_err(ActorsError::from_domain)
        }
        pub fn handler_timeout_millis(&self) -> u64 { self.inner.handler_timeout_millis() }
        pub fn memory_bytes(&self) -> u64 { self.inner.memory_bytes() }
        pub fn checkpoint_bytes(&self) -> u64 { self.inner.checkpoint_bytes() }
    }

    #[diplomat::opaque]
    pub struct Binding { inner: domain::Binding }
    impl Binding {
        pub fn new(name: &str, capability: &str, resource: &str) -> Result<Box<Self>, Box<ActorsError>> {
            domain::Binding::new(name.to_owned(), capability.to_owned(), resource.to_owned()).map(|inner| Box::new(Self { inner })).map_err(ActorsError::from_domain)
        }
        pub fn name<'a>(&'a self) -> &'a str { self.inner.name() }
        pub fn capability<'a>(&'a self) -> &'a str { self.inner.capability() }
        pub fn resource<'a>(&'a self) -> &'a str { self.inner.resource() }
    }

    #[diplomat::opaque_mut]
    pub struct BindingList { inner: Vec<domain::Binding> }
    impl BindingList {
        pub fn new() -> Box<Self> { Box::new(Self { inner: Vec::new() }) }
        pub fn push(&mut self, binding: &Binding) { self.inner.push(binding.inner.clone()); }
        pub fn len(&self) -> usize { self.inner.len() }
    }

    #[diplomat::opaque]
    pub struct SubscriptionStart { inner: domain::SubscriptionStart }
    impl SubscriptionStart {
        pub fn cursor(cursor: u64) -> Box<Self> { Box::new(Self { inner: domain::SubscriptionStart::Cursor { cursor } }) }
        pub fn current_head() -> Box<Self> { Box::new(Self { inner: domain::SubscriptionStart::CurrentHead { current_head: true } }) }
        pub fn is_cursor(&self) -> bool { matches!(self.inner, domain::SubscriptionStart::Cursor { .. }) }
        pub fn cursor_value(&self) -> u64 { self.inner.cursor_value().unwrap_or(0) }
    }

    #[diplomat::opaque]
    pub struct SubscriptionSpec { inner: domain::SubscriptionSpec }
    impl SubscriptionSpec {
        pub fn new(subscription_id: &str, stream_path: &str, start: &SubscriptionStart, placement_anchor: bool) -> Result<Box<Self>, Box<ActorsError>> {
            domain::SubscriptionSpec::new(subscription_id.to_owned(), stream_path.to_owned(), start.inner, placement_anchor).map(|inner| Box::new(Self { inner })).map_err(ActorsError::from_domain)
        }
        pub fn subscription_id<'a>(&'a self) -> &'a str { self.inner.subscription_id() }
        pub fn stream_path<'a>(&'a self) -> &'a str { self.inner.stream_path() }
        pub fn placement_anchor(&self) -> bool { self.inner.placement_anchor() }
    }

    #[diplomat::opaque_mut]
    pub struct SubscriptionList { inner: Vec<domain::SubscriptionSpec> }
    impl SubscriptionList {
        pub fn new() -> Box<Self> { Box::new(Self { inner: Vec::new() }) }
        pub fn push(&mut self, subscription: &SubscriptionSpec) { self.inner.push(subscription.inner.clone()); }
        pub fn len(&self) -> usize { self.inner.len() }
    }

    #[diplomat::opaque]
    pub struct CreateActorRequest { inner: domain::CreateActorRequest }
    impl CreateActorRequest {
        pub fn new(code_sha256: &CodeSha256, home_region: &str, bindings: &BindingList, limits: &ActorLimits, subscriptions: &SubscriptionList, idempotency_key: &str) -> Result<Box<Self>, Box<ActorsError>> {
            domain::CreateActorRequest::new(code_sha256.inner.clone(), home_region.to_owned(), bindings.inner.clone(), limits.inner, subscriptions.inner.clone(), idempotency_key.to_owned()).map(|inner| Box::new(Self { inner })).map_err(ActorsError::from_domain)
        }
        pub fn home_region<'a>(&'a self) -> &'a str { self.inner.home_region() }
        pub fn idempotency_key<'a>(&'a self) -> &'a str { self.inner.idempotency_key() }
        pub fn binding_count(&self) -> usize { self.inner.bindings().len() }
        pub fn subscription_count(&self) -> usize { self.inner.subscriptions().len() }
        pub fn code_sha256_byte_at(&self, index: usize) -> u8 { self.inner.code_sha256().as_bytes().get(index).copied().unwrap_or(0) }
    }

    #[diplomat::opaque_mut]
    pub struct Headers { inner: Vec<wire::Header> }
    impl Headers {
        pub fn new() -> Box<Self> { Box::new(Self { inner: Vec::new() }) }
        pub fn push(&mut self, name: &str, value: &str) { self.inner.push(wire::Header { name: name.to_owned(), value: value.to_owned() }); }
        pub fn len(&self) -> usize { self.inner.len() }
        pub fn name<'a>(&'a self, index: usize) -> &'a str { self.inner.get(index).map(|h| h.name.as_str()).unwrap_or("") }
        pub fn value<'a>(&'a self, index: usize) -> &'a str { self.inner.get(index).map(|h| h.value.as_str()).unwrap_or("") }
    }

    #[diplomat::opaque]
    pub struct InvokeActorRequest { inner: domain::InvokeActorRequest }
    impl InvokeActorRequest {
        pub fn new(actor_id: &ActorId, method: &str, url: &str, body: &[u8], headers: &Headers) -> Box<Self> {
            Box::new(Self { inner: domain::InvokeActorRequest::new(actor_id.inner.clone(), method.to_owned(), url.to_owned(), body.to_vec(), headers.inner.clone()) })
        }
        pub fn actor_id<'a>(&'a self) -> &'a str { self.inner.actor_id().as_str() }
        pub fn method<'a>(&'a self) -> &'a str { self.inner.method() }
        pub fn url<'a>(&'a self) -> &'a str { self.inner.url() }
        pub fn body<'a>(&'a self) -> &'a [u8] { self.inner.body() }
        pub fn body_len(&self) -> usize { self.inner.body().len() }
        pub fn header_count(&self) -> usize { self.inner.headers().len() }
        pub fn header_name<'a>(&'a self, index: usize) -> &'a str { self.inner.headers().get(index).map(|h| h.name.as_str()).unwrap_or("") }
        pub fn header_value<'a>(&'a self, index: usize) -> &'a str { self.inner.headers().get(index).map(|h| h.value.as_str()).unwrap_or("") }
    }

    #[diplomat::opaque]
    pub struct InvokeOutput { inner: Option<domain::InvokeActorResponse> }
    impl InvokeOutput {
        pub fn absent() -> Box<Self> { Box::new(Self { inner: None }) }
        pub fn present(status: u32, body: &[u8], headers: &Headers) -> Box<Self> {
            let wire = wire::InvokeActorResponse { status, body: body.to_vec().into(), headers: headers.inner.clone() };
            Box::new(Self { inner: Some(wire.into()) })
        }
        pub fn has_response(&self) -> bool { self.inner.is_some() }
        pub fn status(&self) -> u32 { self.inner.as_ref().map(domain::InvokeActorResponse::status).unwrap_or(0) }
        pub fn body<'a>(&'a self) -> &'a [u8] { self.inner.as_ref().map(domain::InvokeActorResponse::body).unwrap_or(&[]) }
        pub fn body_len(&self) -> usize { self.body().len() }
        pub fn header_count(&self) -> usize { self.inner.as_ref().map(|r| r.headers().len()).unwrap_or(0) }
        pub fn header_name<'a>(&'a self, index: usize) -> &'a str { self.inner.as_ref().and_then(|r| r.headers().get(index)).map(|h| h.name.as_str()).unwrap_or("") }
        pub fn header_value<'a>(&'a self, index: usize) -> &'a str { self.inner.as_ref().and_then(|r| r.headers().get(index)).map(|h| h.value.as_str()).unwrap_or("") }
    }
}

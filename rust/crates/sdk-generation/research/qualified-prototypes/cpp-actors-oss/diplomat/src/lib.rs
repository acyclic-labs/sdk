//! Isolated Diplomat 0.16.1 experiment over the Actors semantic domain.
//!
//! This crate owns no Actors fields. Opaque bridge values contain the real
//! `acyclic_actors::domain` values and constructors call canonical Rust APIs.

use acyclic_actors::{client, domain, wire, ContractError};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

#[diplomat::bridge]
mod ffi {
    use super::{client, domain, wire, ContractError, Arc, Mutex, CancellationToken};
    use std::future::Future;

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
        Configuration,
        Transport,
        Service,
        Contract,
        Semantic,
        Cancelled,
    }

    #[diplomat::opaque]
    pub struct ActorsError {
        kind: ErrorKind,
        has_numeric_detail: bool,
        numeric_detail: i32,
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
            let numeric_detail = match error {
                domain::DomainError::Contract(ContractError::InvalidArgument) => 1,
                domain::DomainError::Contract(ContractError::LimitExceeded) => 2,
                domain::DomainError::Contract(ContractError::DuplicateName) => 3,
                domain::DomainError::UnknownActorState(value)
                | domain::DomainError::UnknownSubscriptionState(value)
                | domain::DomainError::UnknownErrorCode(value) => value,
                _ => 0,
            };
            let has_numeric_detail = matches!(
                error,
                domain::DomainError::Contract(_)
                    | domain::DomainError::UnknownActorState(_)
                    | domain::DomainError::UnknownSubscriptionState(_)
                    | domain::DomainError::UnknownErrorCode(_)
            );
            Box::new(Self { kind, has_numeric_detail, numeric_detail, message: error.to_string() })
        }
        pub fn kind(&self) -> ErrorKind { self.kind }
        pub fn has_numeric_detail(&self) -> bool { self.has_numeric_detail }
        pub fn numeric_detail(&self) -> i32 { self.numeric_detail }
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

    #[diplomat::opaque]
    pub struct CreateActorResponse { inner: domain::CreateActorResponse }
    impl CreateActorResponse {
        pub fn has_actor(&self) -> bool { self.inner.actor().is_some() }
        pub fn actor_id<'a>(&'a self) -> &'a str { self.inner.actor().map(|a| a.actor_id().as_str()).unwrap_or("") }
        pub fn home_region<'a>(&'a self) -> &'a str { self.inner.actor().map(|a| a.home_region()).unwrap_or("") }
        pub fn state(&self) -> i32 { self.inner.actor().map(|a| i32::from(a.state())).unwrap_or(0) }
        pub fn subscription_count(&self) -> usize { self.inner.actor().map(|a| a.subscriptions().len()).unwrap_or(0) }
        pub fn checkpoint_present(&self) -> bool { self.inner.actor().and_then(|a| a.checkpoint_unix_millis()).is_some() }
        pub fn checkpoint_unix_millis(&self) -> u64 { self.inner.actor().and_then(|a| a.checkpoint_unix_millis()).unwrap_or(0) }
        pub fn checkpoint_epoch(&self) -> u64 { self.inner.actor().map(|a| a.checkpoint_epoch()).unwrap_or(0) }
        pub fn configuration_revision(&self) -> u64 { self.inner.actor().map(|a| a.configuration_revision()).unwrap_or(0) }
        pub fn code_sha256_byte_at(&self, index: usize) -> u8 { self.inner.actor().and_then(|a| a.code_sha256().as_bytes().get(index).copied()).unwrap_or(0) }
    }

    #[diplomat::opaque]
    pub struct UpdateActorRequest { inner: domain::UpdateActorRequest }
    impl UpdateActorRequest {
        pub fn new(actor_id: &ActorId, code_sha256: &CodeSha256, bindings: &BindingList, limits: &ActorLimits, expected_configuration_revision: u64, idempotency_key: &str) -> Result<Box<Self>, Box<ActorsError>> {
            domain::UpdateActorRequest::new(actor_id.inner.clone(), code_sha256.inner.clone(), bindings.inner.clone(), limits.inner, expected_configuration_revision, idempotency_key.to_owned()).map(|inner| Box::new(Self { inner })).map_err(ActorsError::from_domain)
        }
        pub fn actor_id<'a>(&'a self) -> &'a str { self.inner.actor_id().as_str() }
        pub fn expected_configuration_revision(&self) -> u64 { self.inner.expected_configuration_revision() }
        pub fn binding_count(&self) -> usize { self.inner.bindings().len() }
        pub fn code_sha256_byte_at(&self, index: usize) -> u8 { self.inner.code_sha256().as_bytes().get(index).copied().unwrap_or(0) }
    }

    #[diplomat::opaque]
    pub struct InspectActorRequest { inner: domain::InspectActorRequest }
    impl InspectActorRequest {
        pub fn new(actor_id: &ActorId) -> Box<Self> { Box::new(Self { inner: domain::InspectActorRequest::new(actor_id.inner.clone()) }) }
        pub fn actor_id<'a>(&'a self) -> &'a str { self.inner.actor_id().as_str() }
    }

    #[diplomat::opaque]
    pub struct AddSubscriptionRequest { inner: domain::AddSubscriptionRequest }
    impl AddSubscriptionRequest {
        pub fn new(actor_id: &ActorId, subscription: &SubscriptionSpec, idempotency_key: &str) -> Result<Box<Self>, Box<ActorsError>> {
            domain::AddSubscriptionRequest::new(actor_id.inner.clone(), subscription.inner.clone(), idempotency_key.to_owned()).map(|inner| Box::new(Self { inner })).map_err(ActorsError::from_domain)
        }
        pub fn actor_id<'a>(&'a self) -> &'a str { self.inner.actor_id().as_str() }
        pub fn subscription_id<'a>(&'a self) -> &'a str { self.inner.subscription().subscription_id() }
    }

    #[diplomat::opaque]
    pub struct RemoveSubscriptionRequest { inner: domain::RemoveSubscriptionRequest }
    impl RemoveSubscriptionRequest {
        pub fn new(actor_id: &ActorId, subscription_id: &str, idempotency_key: &str) -> Box<Self> { Box::new(Self { inner: domain::RemoveSubscriptionRequest::new(actor_id.inner.clone(), subscription_id.to_owned(), idempotency_key.to_owned()) }) }
        pub fn actor_id<'a>(&'a self) -> &'a str { self.inner.actor_id().as_str() }
        pub fn subscription_id<'a>(&'a self) -> &'a str { self.inner.subscription_id() }
    }

    #[diplomat::opaque]
    pub struct ResumeSubscriptionRequest { inner: domain::ResumeSubscriptionRequest }
    impl ResumeSubscriptionRequest {
        pub fn new(actor_id: &ActorId, subscription_id: &str, idempotency_key: &str) -> Box<Self> { Box::new(Self { inner: domain::ResumeSubscriptionRequest::new(actor_id.inner.clone(), subscription_id.to_owned(), idempotency_key.to_owned()) }) }
        pub fn actor_id<'a>(&'a self) -> &'a str { self.inner.actor_id().as_str() }
        pub fn subscription_id<'a>(&'a self) -> &'a str { self.inner.subscription_id() }
    }

    #[diplomat::opaque]
    pub struct CheckpointActorRequest { inner: domain::CheckpointActorRequest }
    impl CheckpointActorRequest {
        pub fn new(actor_id: &ActorId, idempotency_key: &str) -> Box<Self> { Box::new(Self { inner: domain::CheckpointActorRequest::new(actor_id.inner.clone(), idempotency_key.to_owned()) }) }
        pub fn actor_id<'a>(&'a self) -> &'a str { self.inner.actor_id().as_str() }
    }

    #[diplomat::opaque]
    pub struct UpdateActorResponse { inner: domain::UpdateActorResponse }
    impl UpdateActorResponse {
        pub fn has_actor(&self) -> bool { self.inner.actor().is_some() }
        pub fn actor_id<'a>(&'a self) -> &'a str { self.inner.actor().map(|a| a.actor_id().as_str()).unwrap_or("") }
        pub fn home_region<'a>(&'a self) -> &'a str { self.inner.actor().map(|a| a.home_region()).unwrap_or("") }
        pub fn state(&self) -> i32 { self.inner.actor().map(|a| i32::from(a.state())).unwrap_or(0) }
        pub fn subscription_count(&self) -> usize { self.inner.actor().map(|a| a.subscriptions().len()).unwrap_or(0) }
        pub fn checkpoint_present(&self) -> bool { self.inner.actor().and_then(|a| a.checkpoint_unix_millis()).is_some() }
        pub fn checkpoint_unix_millis(&self) -> u64 { self.inner.actor().and_then(|a| a.checkpoint_unix_millis()).unwrap_or(0) }
        pub fn checkpoint_epoch(&self) -> u64 { self.inner.actor().map(|a| a.checkpoint_epoch()).unwrap_or(0) }
        pub fn configuration_revision(&self) -> u64 { self.inner.actor().map(|a| a.configuration_revision()).unwrap_or(0) }
        pub fn code_sha256_byte_at(&self, index: usize) -> u8 { self.inner.actor().and_then(|a| a.code_sha256().as_bytes().get(index).copied()).unwrap_or(0) }
    }

    #[diplomat::opaque]
    pub struct InspectActorResponse { inner: domain::InspectActorResponse }
    impl InspectActorResponse {
        pub fn has_actor(&self) -> bool { self.inner.actor().is_some() }
        pub fn actor_id<'a>(&'a self) -> &'a str { self.inner.actor().map(|a| a.actor_id().as_str()).unwrap_or("") }
        pub fn home_region<'a>(&'a self) -> &'a str { self.inner.actor().map(|a| a.home_region()).unwrap_or("") }
        pub fn state(&self) -> i32 { self.inner.actor().map(|a| i32::from(a.state())).unwrap_or(0) }
        pub fn subscription_count(&self) -> usize { self.inner.actor().map(|a| a.subscriptions().len()).unwrap_or(0) }
        pub fn checkpoint_present(&self) -> bool { self.inner.actor().and_then(|a| a.checkpoint_unix_millis()).is_some() }
        pub fn checkpoint_unix_millis(&self) -> u64 { self.inner.actor().and_then(|a| a.checkpoint_unix_millis()).unwrap_or(0) }
        pub fn checkpoint_epoch(&self) -> u64 { self.inner.actor().map(|a| a.checkpoint_epoch()).unwrap_or(0) }
        pub fn configuration_revision(&self) -> u64 { self.inner.actor().map(|a| a.configuration_revision()).unwrap_or(0) }
        pub fn code_sha256_byte_at(&self, index: usize) -> u8 { self.inner.actor().and_then(|a| a.code_sha256().as_bytes().get(index).copied()).unwrap_or(0) }
    }

    #[diplomat::opaque]
    pub struct AddSubscriptionResponse { inner: domain::AddSubscriptionResponse }
    impl AddSubscriptionResponse {
        pub fn has_actor(&self) -> bool { self.inner.actor().is_some() }
        pub fn actor_id<'a>(&'a self) -> &'a str { self.inner.actor().map(|a| a.actor_id().as_str()).unwrap_or("") }
        pub fn home_region<'a>(&'a self) -> &'a str { self.inner.actor().map(|a| a.home_region()).unwrap_or("") }
        pub fn state(&self) -> i32 { self.inner.actor().map(|a| i32::from(a.state())).unwrap_or(0) }
        pub fn subscription_count(&self) -> usize { self.inner.actor().map(|a| a.subscriptions().len()).unwrap_or(0) }
        pub fn checkpoint_present(&self) -> bool { self.inner.actor().and_then(|a| a.checkpoint_unix_millis()).is_some() }
        pub fn checkpoint_unix_millis(&self) -> u64 { self.inner.actor().and_then(|a| a.checkpoint_unix_millis()).unwrap_or(0) }
        pub fn checkpoint_epoch(&self) -> u64 { self.inner.actor().map(|a| a.checkpoint_epoch()).unwrap_or(0) }
        pub fn configuration_revision(&self) -> u64 { self.inner.actor().map(|a| a.configuration_revision()).unwrap_or(0) }
        pub fn code_sha256_byte_at(&self, index: usize) -> u8 { self.inner.actor().and_then(|a| a.code_sha256().as_bytes().get(index).copied()).unwrap_or(0) }
    }

    #[diplomat::opaque]
    pub struct RemoveSubscriptionResponse { inner: domain::RemoveSubscriptionResponse }
    impl RemoveSubscriptionResponse {
        pub fn has_actor(&self) -> bool { self.inner.actor().is_some() }
        pub fn actor_id<'a>(&'a self) -> &'a str { self.inner.actor().map(|a| a.actor_id().as_str()).unwrap_or("") }
        pub fn home_region<'a>(&'a self) -> &'a str { self.inner.actor().map(|a| a.home_region()).unwrap_or("") }
        pub fn state(&self) -> i32 { self.inner.actor().map(|a| i32::from(a.state())).unwrap_or(0) }
        pub fn subscription_count(&self) -> usize { self.inner.actor().map(|a| a.subscriptions().len()).unwrap_or(0) }
        pub fn checkpoint_present(&self) -> bool { self.inner.actor().and_then(|a| a.checkpoint_unix_millis()).is_some() }
        pub fn checkpoint_unix_millis(&self) -> u64 { self.inner.actor().and_then(|a| a.checkpoint_unix_millis()).unwrap_or(0) }
        pub fn checkpoint_epoch(&self) -> u64 { self.inner.actor().map(|a| a.checkpoint_epoch()).unwrap_or(0) }
        pub fn configuration_revision(&self) -> u64 { self.inner.actor().map(|a| a.configuration_revision()).unwrap_or(0) }
        pub fn code_sha256_byte_at(&self, index: usize) -> u8 { self.inner.actor().and_then(|a| a.code_sha256().as_bytes().get(index).copied()).unwrap_or(0) }
    }

    #[diplomat::opaque]
    pub struct ResumeSubscriptionResponse { inner: domain::ResumeSubscriptionResponse }
    impl ResumeSubscriptionResponse {
        pub fn has_actor(&self) -> bool { self.inner.actor().is_some() }
        pub fn actor_id<'a>(&'a self) -> &'a str { self.inner.actor().map(|a| a.actor_id().as_str()).unwrap_or("") }
        pub fn home_region<'a>(&'a self) -> &'a str { self.inner.actor().map(|a| a.home_region()).unwrap_or("") }
        pub fn state(&self) -> i32 { self.inner.actor().map(|a| i32::from(a.state())).unwrap_or(0) }
        pub fn subscription_count(&self) -> usize { self.inner.actor().map(|a| a.subscriptions().len()).unwrap_or(0) }
        pub fn checkpoint_present(&self) -> bool { self.inner.actor().and_then(|a| a.checkpoint_unix_millis()).is_some() }
        pub fn checkpoint_unix_millis(&self) -> u64 { self.inner.actor().and_then(|a| a.checkpoint_unix_millis()).unwrap_or(0) }
        pub fn checkpoint_epoch(&self) -> u64 { self.inner.actor().map(|a| a.checkpoint_epoch()).unwrap_or(0) }
        pub fn configuration_revision(&self) -> u64 { self.inner.actor().map(|a| a.configuration_revision()).unwrap_or(0) }
        pub fn code_sha256_byte_at(&self, index: usize) -> u8 { self.inner.actor().and_then(|a| a.code_sha256().as_bytes().get(index).copied()).unwrap_or(0) }
    }

    #[diplomat::opaque]
    pub struct CheckpointActorResponse { inner: domain::CheckpointActorResponse }
    impl CheckpointActorResponse {
        pub fn has_actor(&self) -> bool { self.inner.actor().is_some() }
        pub fn actor_id<'a>(&'a self) -> &'a str { self.inner.actor().map(|a| a.actor_id().as_str()).unwrap_or("") }
        pub fn home_region<'a>(&'a self) -> &'a str { self.inner.actor().map(|a| a.home_region()).unwrap_or("") }
        pub fn state(&self) -> i32 { self.inner.actor().map(|a| i32::from(a.state())).unwrap_or(0) }
        pub fn subscription_count(&self) -> usize { self.inner.actor().map(|a| a.subscriptions().len()).unwrap_or(0) }
        pub fn checkpoint_present(&self) -> bool { self.inner.actor().and_then(|a| a.checkpoint_unix_millis()).is_some() }
        pub fn checkpoint_unix_millis(&self) -> u64 { self.inner.actor().and_then(|a| a.checkpoint_unix_millis()).unwrap_or(0) }
        pub fn checkpoint_epoch(&self) -> u64 { self.inner.actor().map(|a| a.checkpoint_epoch()).unwrap_or(0) }
        pub fn configuration_revision(&self) -> u64 { self.inner.actor().map(|a| a.configuration_revision()).unwrap_or(0) }
        pub fn code_sha256_byte_at(&self, index: usize) -> u8 { self.inner.actor().and_then(|a| a.code_sha256().as_bytes().get(index).copied()).unwrap_or(0) }
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

    #[diplomat::opaque]
    pub struct AsyncError {
        kind: ErrorKind,
        grpc_code: i32,
        has_service_detail: bool,
        service_detail_code: i32,
        message: String,
    }

    impl AsyncError {
        fn from_client(error: client::Error) -> Box<Self> {
            let (kind, grpc_code, detail_code, has_service_detail) = match &error {
                client::Error::Configuration(_) => (ErrorKind::Configuration, 0, 0, false),
                client::Error::Transport(_) => (ErrorKind::Transport, 0, 0, false),
                client::Error::Service { grpc_code, detail } => (ErrorKind::Service, *grpc_code, detail.as_ref().map(|d| d.code).unwrap_or(0), detail.is_some()),
                client::Error::Contract(_) => (ErrorKind::Contract, 0, 0, false),
                client::Error::Semantic(_) => (ErrorKind::Semantic, 0, 0, false),
                client::Error::Cancelled => (ErrorKind::Cancelled, 0, 0, false),
            };
            Box::new(Self { kind, grpc_code, has_service_detail, service_detail_code: detail_code, message: error.to_string() })
        }
        pub fn kind(&self) -> ErrorKind { self.kind }
        pub fn grpc_code(&self) -> i32 { self.grpc_code }
        pub fn has_service_detail(&self) -> bool { self.has_service_detail }
        pub fn service_detail_code(&self) -> i32 { self.service_detail_code }
        pub fn message<'a>(&'a self) -> &'a str { &self.message }
    }

    enum AsyncResult {
        Create(Result<domain::CreateActorResponse, client::Error>),
        Update(Result<domain::UpdateActorResponse, client::Error>),
        Inspect(Result<domain::InspectActorResponse, client::Error>),
        Add(Result<domain::AddSubscriptionResponse, client::Error>),
        Remove(Result<domain::RemoveSubscriptionResponse, client::Error>),
        Resume(Result<domain::ResumeSubscriptionResponse, client::Error>),
        Checkpoint(Result<domain::CheckpointActorResponse, client::Error>),
        Invoke(Result<domain::InvokeActorResponse, client::Error>),
    }

    #[diplomat::opaque_mut]
    pub struct OperationHandle {
        cancellation: CancellationToken,
        result: Arc<Mutex<Option<AsyncResult>>>,
        callback: Option<Box<dyn Fn()>>,
        notified: bool,
    }

    impl OperationHandle {
        pub fn cancel(&self) { self.cancellation.cancel(); }
        pub fn is_cancelled(&self) -> bool { self.cancellation.is_cancelled() }
        pub fn is_done(&self) -> bool { self.result.lock().map(|slot| slot.is_some()).unwrap_or(true) }
        pub fn poll(&mut self) {
            if self.is_done() && !self.notified {
                self.notified = true;
                if let Some(callback) = self.callback.take() { callback(); }
            }
        }
        fn take_result(&mut self) -> Result<Option<AsyncResult>, Box<AsyncError>> {
            self.result.lock().map(|mut slot| slot.take()).map_err(|_| AsyncError::from_client(client::Error::Transport("operation result lock poisoned".into())))
        }

        fn wrong_operation() -> Box<AsyncError> {
            AsyncError::from_client(client::Error::Transport("operation result type does not match this accessor".into()))
        }

        pub fn take_create(&mut self) -> Result<Option<Box<CreateActorResponse>>, Box<AsyncError>> {
            let Some(AsyncResult::Create(result)) = self.take_result()? else { return Ok(None); };
            result.map(|response| Some(Box::new(CreateActorResponse { inner: response }))).map_err(AsyncError::from_client)
        }
        pub fn take_update(&mut self) -> Result<Option<Box<UpdateActorResponse>>, Box<AsyncError>> {
            let Some(AsyncResult::Update(result)) = self.take_result()? else { return Ok(None); };
            result.map(|response| Some(Box::new(UpdateActorResponse { inner: response }))).map_err(AsyncError::from_client)
        }
        pub fn take_inspect(&mut self) -> Result<Option<Box<InspectActorResponse>>, Box<AsyncError>> {
            let Some(AsyncResult::Inspect(result)) = self.take_result()? else { return Ok(None); };
            result.map(|response| Some(Box::new(InspectActorResponse { inner: response }))).map_err(AsyncError::from_client)
        }
        pub fn take_add_subscription(&mut self) -> Result<Option<Box<AddSubscriptionResponse>>, Box<AsyncError>> {
            let Some(AsyncResult::Add(result)) = self.take_result()? else { return Ok(None); };
            result.map(|response| Some(Box::new(AddSubscriptionResponse { inner: response }))).map_err(AsyncError::from_client)
        }
        pub fn take_remove_subscription(&mut self) -> Result<Option<Box<RemoveSubscriptionResponse>>, Box<AsyncError>> {
            let Some(AsyncResult::Remove(result)) = self.take_result()? else { return Ok(None); };
            result.map(|response| Some(Box::new(RemoveSubscriptionResponse { inner: response }))).map_err(AsyncError::from_client)
        }
        pub fn take_resume_subscription(&mut self) -> Result<Option<Box<ResumeSubscriptionResponse>>, Box<AsyncError>> {
            let Some(AsyncResult::Resume(result)) = self.take_result()? else { return Ok(None); };
            result.map(|response| Some(Box::new(ResumeSubscriptionResponse { inner: response }))).map_err(AsyncError::from_client)
        }
        pub fn take_checkpoint(&mut self) -> Result<Option<Box<CheckpointActorResponse>>, Box<AsyncError>> {
            let Some(AsyncResult::Checkpoint(result)) = self.take_result()? else { return Ok(None); };
            result.map(|response| Some(Box::new(CheckpointActorResponse { inner: response }))).map_err(AsyncError::from_client)
        }
        pub fn take(&mut self) -> Result<Option<Box<InvokeOutput>>, Box<AsyncError>> {
            let Some(AsyncResult::Invoke(result)) = self.take_result()? else {
                return Ok(None);
            };
            result.map(|response| Some(Box::new(InvokeOutput { inner: Some(response) }))).map_err(AsyncError::from_client)
        }
    }

    #[diplomat::opaque]
    pub struct ActorsClient {
        inner: Arc<client::Client>,
        runtime: Arc<tokio::runtime::Runtime>,
    }

    impl ActorsClient {
        fn spawn_operation<F, Fut>(&self, operation: F, callback: impl Fn() + 'static) -> Box<OperationHandle>
        where
            F: FnOnce(CancellationToken) -> Fut + Send + 'static,
            Fut: Future<Output = AsyncResult> + Send + 'static,
        {
            let cancellation = CancellationToken::new();
            let result = Arc::new(Mutex::new(None));
            let task_result = Arc::clone(&result);
            let task_cancellation = cancellation.clone();
            self.runtime.spawn(async move {
                let outcome = operation(task_cancellation).await;
                if let Ok(mut slot) = task_result.lock() { *slot = Some(outcome); }
            });
            Box::new(OperationHandle { cancellation, result, callback: Some(Box::new(callback)), notified: false })
        }

        pub fn connect(endpoint: &str, token: &str) -> Result<Box<Self>, Box<AsyncError>> {
            let runtime = tokio::runtime::Runtime::new().map_err(|error| AsyncError::from_client(client::Error::Transport(error.to_string())))?;
            let inner = runtime.block_on(client::connect(endpoint, token)).map_err(AsyncError::from_client)?;
            Ok(Box::new(Self { inner: Arc::new(inner), runtime: Arc::new(runtime) }))
        }

        pub fn connect_with_ca(endpoint: &str, token: &str, ca_certificate: &[u8]) -> Result<Box<Self>, Box<AsyncError>> {
            let runtime = tokio::runtime::Runtime::new().map_err(|error| AsyncError::from_client(client::Error::Transport(error.to_string())))?;
            let inner = runtime.block_on(client::connect_with_ca_certificate(endpoint, token, Some(ca_certificate))).map_err(AsyncError::from_client)?;
            Ok(Box::new(Self { inner: Arc::new(inner), runtime: Arc::new(runtime) }))
        }

        pub fn transport<'a>(&'a self) -> &'a str { self.inner.transport() }

        pub fn invoke_async(&self, request: &InvokeActorRequest, callback: impl Fn() + 'static) -> Box<OperationHandle> {
            let task_client = Arc::clone(&self.inner);
            let task_request = request.inner.clone();
            self.spawn_operation(move |token| async move { AsyncResult::Invoke(client::run_with_cancellation(task_client.invoke_actor(&task_request), Some(token)).await) }, callback)
        }

        pub fn create_async(&self, request: &CreateActorRequest, callback: impl Fn() + 'static) -> Box<OperationHandle> {
            let task_client = Arc::clone(&self.inner);
            let task_request = request.inner.clone();
            self.spawn_operation(move |token| async move { AsyncResult::Create(client::run_with_cancellation(task_client.create_actor(&task_request), Some(token)).await) }, callback)
        }

        pub fn update_async(&self, request: &UpdateActorRequest, callback: impl Fn() + 'static) -> Box<OperationHandle> {
            let task_client = Arc::clone(&self.inner);
            let task_request = request.inner.clone();
            self.spawn_operation(move |token| async move { AsyncResult::Update(client::run_with_cancellation(task_client.update_actor(&task_request), Some(token)).await) }, callback)
        }

        pub fn inspect_async(&self, request: &InspectActorRequest, callback: impl Fn() + 'static) -> Box<OperationHandle> {
            let task_client = Arc::clone(&self.inner);
            let task_request = request.inner.clone();
            self.spawn_operation(move |token| async move { AsyncResult::Inspect(client::run_with_cancellation(task_client.inspect_actor(&task_request), Some(token)).await) }, callback)
        }

        pub fn add_subscription_async(&self, request: &AddSubscriptionRequest, callback: impl Fn() + 'static) -> Box<OperationHandle> {
            let task_client = Arc::clone(&self.inner);
            let task_request = request.inner.clone();
            self.spawn_operation(move |token| async move { AsyncResult::Add(client::run_with_cancellation(task_client.add_subscription(&task_request), Some(token)).await) }, callback)
        }

        pub fn remove_subscription_async(&self, request: &RemoveSubscriptionRequest, callback: impl Fn() + 'static) -> Box<OperationHandle> {
            let task_client = Arc::clone(&self.inner);
            let task_request = request.inner.clone();
            self.spawn_operation(move |token| async move { AsyncResult::Remove(client::run_with_cancellation(task_client.remove_subscription(&task_request), Some(token)).await) }, callback)
        }

        pub fn resume_subscription_async(&self, request: &ResumeSubscriptionRequest, callback: impl Fn() + 'static) -> Box<OperationHandle> {
            let task_client = Arc::clone(&self.inner);
            let task_request = request.inner.clone();
            self.spawn_operation(move |token| async move { AsyncResult::Resume(client::run_with_cancellation(task_client.resume_subscription(&task_request), Some(token)).await) }, callback)
        }

        pub fn checkpoint_async(&self, request: &CheckpointActorRequest, callback: impl Fn() + 'static) -> Box<OperationHandle> {
            let task_client = Arc::clone(&self.inner);
            let task_request = request.inner.clone();
            self.spawn_operation(move |token| async move { AsyncResult::Checkpoint(client::run_with_cancellation(task_client.checkpoint_actor(&task_request), Some(token)).await) }, callback)
        }
    }
}

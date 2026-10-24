//! Private Worker service bindings, separate from public hostname authentication.
use acyclic_actors::{
    addressing::{AccountId, ResourceIdentity, validate_dns_label},
    client, domain,
};
use std::{future::Future, sync::Arc};
/// Holder's admitted transport, never supplied by a serializable address.
pub trait WorkerInvoker: Send + Sync {
    /// Invoke once through the holder's named service binding.
    fn invoke(
        &self,
        request: &domain::InvokeWorkerRequest,
    ) -> impl Future<Output = Result<domain::InvokeActorResponse, client::Error>> + Send;
}
#[cfg(not(target_arch = "wasm32"))]
impl WorkerInvoker for client::Client {
    async fn invoke(
        &self,
        request: &domain::InvokeWorkerRequest,
    ) -> Result<domain::InvokeActorResponse, client::Error> {
        self.invoke_worker(request).await
    }
}
/// Typed service binding materialized from an admitted deployment declaration.
pub struct WorkerServiceBinding<T> {
    identity: ResourceIdentity,
    authority: Arc<T>,
    binding: String,
}
impl<T: WorkerInvoker> WorkerServiceBinding<T> {
    /// Select a typed service target; opaque resource strings cannot grant access.
    pub fn from_binding(
        binding: &domain::Binding,
        authority: Arc<T>,
    ) -> Result<Self, client::Error> {
        let (account, worker) = binding
            .service_target()
            .ok_or(domain::DomainError::InvalidBinding)?;
        AccountId::new(account).map_err(domain::DomainError::from)?;
        validate_dns_label(worker.as_bytes()).map_err(|_| domain::DomainError::InvalidBinding)?;
        Ok(Self {
            identity: ResourceIdentity::new(account, worker.as_bytes())
                .map_err(domain::DomainError::from)?,
            authority,
            binding: binding.name().to_owned(),
        })
    }
    /// The public deployment address carries no binding authority.
    pub const fn identity(&self) -> &ResourceIdentity {
        &self.identity
    }
    fn request(
        &self,
        method: String,
        url: String,
        body: Vec<u8>,
        headers: Vec<domain::Header>,
    ) -> Result<domain::InvokeWorkerRequest, client::Error> {
        let name = std::str::from_utf8(self.identity.id().as_bytes())
            .map_err(|_| domain::DomainError::InvalidBinding)?;
        Ok(domain::InvokeWorkerRequest::new(
            domain::ActorNamespace::new(name.to_owned())?,
            method,
            url,
            body,
            headers,
        )?
        .with_binding(self.identity.account().as_bytes(), self.binding.clone())?)
    }
    /// Fetch privately through the binding, without a target HTTP credential.
    pub async fn fetch(
        &self,
        method: String,
        url: String,
        body: Vec<u8>,
        headers: Vec<domain::Header>,
    ) -> Result<domain::InvokeActorResponse, client::Error> {
        self.authority
            .invoke(&self.request(method, url, body, headers)?)
            .await
    }
    /// Invoke an exported RPC method using the same binding authority.
    pub async fn rpc(
        &self,
        method: String,
        payload: Vec<u8>,
    ) -> Result<domain::InvokeActorResponse, client::Error> {
        self.authority
            .invoke(
                &self
                    .request(
                        "RPC".into(),
                        "https://worker.internal/".into(),
                        payload,
                        vec![],
                    )?
                    .with_rpc(method)?,
            )
            .await
    }
}

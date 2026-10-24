//! Namespace-scoped actor IDs and capability-bound stubs. Getting a stub is local.
use crate::{
    addressing::{AccountId, ActorReference, IdentityError, NamespaceId},
    client, domain,
};
use std::{future::Future, sync::Arc};
use tokio::sync::{Mutex, watch};

/// A namespace-scoped ID with optional name-construction provenance.
#[derive(Clone, Debug)]
pub struct Id {
    reference: ActorReference,
    name: Option<String>,
}

impl PartialEq for Id {
    fn eq(&self, other: &Self) -> bool {
        self.reference == other.reference
    }
}
impl Eq for Id {}

impl Id {
    /// The plain local ID spelling, suitable for idFromString in this namespace.
    pub fn as_str(&self) -> &str {
        self.reference.id()
    }
    /// Name exists only when this ID was constructed with idFromName.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
    /// Complete transferable address. It contains no bearer or routing token.
    pub const fn reference(&self) -> &ActorReference {
        &self.reference
    }
}

/// Host-admitted invocation authority; references alone do not implement it.
pub trait Invoker: Send + Sync {
    /// Submit once through the holder's authenticated transport.
    fn invoke(
        &self,
        request: &domain::InvokeActorRequest,
    ) -> impl Future<Output = Result<domain::InvokeActorResponse, client::Error>> + Send;
}

#[cfg(not(target_arch = "wasm32"))]
impl Invoker for client::Client {
    async fn invoke(
        &self,
        request: &domain::InvokeActorRequest,
    ) -> Result<domain::InvokeActorResponse, client::Error> {
        self.invoke_actor(request).await
    }
}

/// Invalid scope, invalid identity, transport rejection or a broken stub.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Shared strict identity validation.
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// Semantic request validation.
    #[error(transparent)]
    Domain(#[from] domain::DomainError),
    /// A reference or ID belongs to another binding scope.
    #[error("actor reference is outside the namespace binding")]
    Scope,
    /// This stub's previous call failed or was cancelled; obtain a new stub.
    #[error("actor stub is broken")]
    Broken,
    /// The admitted invocation failed; it is never silently replayed here.
    #[error(transparent)]
    Invoke(#[from] client::Error),
}

/// Typed namespace binding carrying authority separately from its public scope.
pub struct Namespace<T> {
    account: AccountId,
    namespace: NamespaceId,
    authority: Arc<T>,
    binding_name: Option<String>,
}

impl<T> Clone for Namespace<T> {
    fn clone(&self) -> Self {
        Self {
            account: self.account.clone(),
            namespace: self.namespace.clone(),
            authority: self.authority.clone(),
            binding_name: self.binding_name.clone(),
        }
    }
}

impl<T: Invoker> Namespace<T> {
    /// Materialize a host-authorized binding. Cloud validates the actual grant.
    pub fn new(account: &[u8], namespace: &[u8], authority: Arc<T>) -> Result<Self, Error> {
        Ok(Self {
            account: AccountId::new(account)?,
            namespace: NamespaceId::new(namespace)?,
            authority,
            binding_name: None,
        })
    }
    /// Materialize the exact namespace selected in an admitted deployment binding.
    pub fn from_binding(binding: &domain::Binding, authority: Arc<T>) -> Result<Self, Error> {
        let (account, namespace) = binding.namespace_target().ok_or(Error::Scope)?;
        let mut value = Self::new(account, namespace.as_bytes(), authority)?;
        value.binding_name = Some(binding.name().to_owned());
        Ok(value)
    }
    fn id(&self, text: &str, named: bool) -> Result<Id, Error> {
        Ok(Id {
            reference: ActorReference::new(
                self.account.as_bytes(),
                self.namespace.as_str().as_bytes(),
                text.as_bytes(),
            )?,
            name: named.then(|| text.to_owned()),
        })
    }
    /// A name is its own plain ID. This never creates an actor.
    pub fn id_from_name(&self, name: &str) -> Result<Id, Error> {
        self.id(name, true)
    }
    /// Parse an exact local ID; parsing does not restore name provenance.
    pub fn id_from_string(&self, text: &str) -> Result<Id, Error> {
        self.id(text, false)
    }
    /// Generate 128 random bits as lowercase hex, without creating an actor.
    pub fn new_unique_id(&self) -> Result<Id, Error> {
        self.id(&random_id()?, false)
    }
    /// Reconstitute an ID only in the exact bound account and namespace.
    pub fn id_from_reference(&self, reference: ActorReference) -> Result<Id, Error> {
        if reference.account() != &self.account || reference.namespace() != &self.namespace {
            return Err(Error::Scope);
        }
        Ok(Id {
            reference,
            name: None,
        })
    }
    /// Obtain a local stub with no existence check. The first call activates it.
    pub fn get(&self, id: &Id, location_hint: Option<String>) -> Result<Stub<T>, Error> {
        self.id_from_reference(id.reference.clone())?;
        if let Some(hint) = &location_hint {
            crate::addressing::validate_location_hint(hint)?;
        }
        Ok(Stub {
            id: id.clone(),
            location_hint,
            authority: self.authority.clone(),
            binding_name: self.binding_name.clone(),
            session: random_id()?,
            sequence: Mutex::new(0),
            failed: watch::channel(false).0,
        })
    }
    /// Resolve a name and obtain its local stub in one step.
    pub fn get_by_name(&self, name: &str, location_hint: Option<String>) -> Result<Stub<T>, Error> {
        self.get(&self.id_from_name(name)?, location_hint)
    }
}

fn random_id() -> Result<String, Error> {
    // UUID-v4 masks six bits. Select eight unmasked random bytes from each
    // of two independent draws, retaining a full 128 random bits.
    let left = uuid::Uuid::new_v4();
    let right = uuid::Uuid::new_v4();
    let mut bytes = Vec::with_capacity(16);
    for value in [left.as_bytes(), right.as_bytes()] {
        for index in [0, 1, 2, 3, 4, 5, 7, 9] {
            bytes.push(*value.get(index).ok_or(Error::Scope)?);
        }
    }
    let text: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok(text)
}

/// Ordered call admission and terminal failure state belong to each stub.
pub struct Stub<T> {
    id: Id,
    location_hint: Option<String>,
    authority: Arc<T>,
    binding_name: Option<String>,
    session: String,
    sequence: Mutex<u64>,
    failed: watch::Sender<bool>,
}

struct InFlight<'a> {
    failed: &'a watch::Sender<bool>,
    completed: bool,
}
impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.failed.send_replace(true);
        }
    }
}

impl<T: Invoker> Stub<T> {
    /// Namespace-scoped ID, preserving optional name provenance.
    pub const fn id(&self) -> &Id {
        &self.id
    }
    /// Original name, when obtained through idFromName/getByName.
    pub fn name(&self) -> Option<&str> {
        self.id.name()
    }
    /// Safe address projection; serializing this cannot serialize the authority.
    pub fn reference(&self) -> &ActorReference {
        self.id.reference()
    }
    /// Submit an HTTP request once, in this stub's call order.
    pub async fn fetch(
        &self,
        method: String,
        url: String,
        body: Vec<u8>,
        headers: Vec<domain::Header>,
    ) -> Result<domain::InvokeActorResponse, Error> {
        let request = domain::InvokeActorRequest::new(
            domain::ActorId::new(self.id.as_str().to_owned())?,
            method,
            url,
            body,
            headers,
        )
        .with_reference(self.id.reference())?
        .with_location_hint(self.location_hint.clone())?;
        let request = if let Some(name) = &self.binding_name {
            request.with_binding(name.clone())?
        } else {
            request
        };
        self.call(request).await
    }
    /// Invoke an exported method through the bound namespace, preserving call order.
    pub async fn rpc(
        &self,
        method: String,
        payload: Vec<u8>,
    ) -> Result<domain::InvokeActorResponse, Error> {
        let request = domain::InvokeActorRequest::new(
            domain::ActorId::new(self.id.as_str().to_owned())?,
            "RPC".into(),
            "https://actor.internal/".into(),
            payload,
            vec![],
        )
        .with_reference(self.id.reference())?
        .with_location_hint(self.location_hint.clone())?
        .with_rpc(method)?;
        let request = if let Some(name) = &self.binding_name {
            request.with_binding(name.clone())?
        } else {
            request
        };
        self.call(request).await
    }
    async fn call(
        &self,
        request: domain::InvokeActorRequest,
    ) -> Result<domain::InvokeActorResponse, Error> {
        let mut failure = self.failed.subscribe();
        let mut flight = InFlight {
            failed: &self.failed,
            completed: false,
        };
        let sequence = {
            let mut next = self.sequence.lock().await;
            if *self.failed.borrow() {
                return Err(Error::Broken);
            }
            let sequence = *next;
            *next = next.checked_add(1).ok_or(Error::Broken)?;
            sequence
        };
        let request = request.with_invocation_order(domain::InvocationOrder::new(
            self.session.clone(),
            sequence,
        )?);
        // Cloud admits sequence N before N+1, then allows handlers/responses to interleave.
        let response = tokio::select! {
            biased;
            _ = failure.wait_for(|failed| *failed) => return Err(Error::Broken),
            response = self.authority.invoke(&request) => response?,
        };
        if *self.failed.borrow() {
            return Err(Error::Broken);
        }
        flight.completed = true;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    #[derive(Default)]
    struct Mock {
        calls: AtomicUsize,
        fail: AtomicBool,
        pending: AtomicBool,
    }
    impl Invoker for Mock {
        async fn invoke(
            &self,
            request: &domain::InvokeActorRequest,
        ) -> Result<domain::InvokeActorResponse, client::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.namespace(), "ClassOne");
            assert_eq!(
                request
                    .reference(b"wrong")
                    .map_err(domain::DomainError::from)?
                    .account()
                    .as_bytes(),
                b"account"
            );
            if self.pending.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if self.fail.load(Ordering::SeqCst) {
                return Err(client::Error::Transport("rejected".into()));
            }
            Ok(domain::InvokeActorResponse::try_from(
                crate::wire::InvokeActorResponse {
                    status: 200,
                    ..Default::default()
                },
            )?)
        }
    }
    #[tokio::test]
    async fn get_is_local_references_are_scoped_and_name_is_not_identity() -> Result<(), Error> {
        let transport = Arc::new(Mock::default());
        let namespace = Namespace::new(b"account", b"ClassOne", transport.clone())?;
        let named = namespace.id_from_name("same")?;
        let parsed = namespace.id_from_string("same")?;
        assert_eq!(named, parsed);
        assert_eq!(named.name(), Some("same"));
        assert_eq!(parsed.name(), None);
        let stub = namespace.get(&named, None)?;
        assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
        for (account, ns) in [
            (b"other".as_slice(), b"ClassOne".as_slice()),
            (b"account".as_slice(), b"ClassTwo".as_slice()),
        ] {
            assert!(matches!(
                namespace.id_from_reference(ActorReference::new(account, ns, b"same")?),
                Err(Error::Scope)
            ));
        }
        let random = namespace.new_unique_id()?;
        assert_eq!(random.as_str().len(), 32);
        assert!(
            random
                .as_str()
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        );
        assert_eq!(random.name(), None);
        stub.fetch(
            "GET".into(),
            "https://actor.internal/".into(),
            vec![],
            vec![],
        )
        .await?;
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
        Ok(())
    }
    #[tokio::test]
    async fn responses_interleave_and_cancellation_fails_every_inflight_call() -> Result<(), Error>
    {
        let transport = Arc::new(Mock::default());
        transport.pending.store(true, Ordering::SeqCst);
        let namespace = Namespace::new(b"account", b"ClassOne", transport.clone())?;
        let stub = namespace.get_by_name("same", None)?;
        assert_eq!(stub.name(), Some("same"));
        assert_eq!(stub.id().as_str(), "same");
        let mut first = Box::pin(stub.rpc("first".into(), vec![]));
        let mut second = Box::pin(stub.rpc("second".into(), vec![]));
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert!(futures::poll!(second.as_mut()).is_pending());
        // Negative control: holding the queue until the response admits only one call.
        assert_eq!(transport.calls.load(Ordering::SeqCst), 2);
        drop(first);
        assert!(matches!(second.await, Err(Error::Broken)));
        assert!(matches!(
            stub.rpc("third".into(), vec![]).await,
            Err(Error::Broken)
        ));
        assert_eq!(transport.calls.load(Ordering::SeqCst), 2);
        Ok(())
    }
    #[tokio::test]
    async fn a_failed_or_cancelled_stub_cannot_send_again() -> Result<(), Error> {
        let transport = Arc::new(Mock::default());
        let namespace = Namespace::new(b"account", b"ClassOne", transport.clone())?;
        let id = namespace.id_from_name("same")?;
        let stub = namespace.get(&id, None)?;
        transport.fail.store(true, Ordering::SeqCst);
        assert!(
            stub.fetch(
                "GET".into(),
                "https://actor.internal/".into(),
                vec![],
                vec![]
            )
            .await
            .is_err()
        );
        transport.fail.store(false, Ordering::SeqCst);
        assert!(matches!(
            stub.fetch(
                "GET".into(),
                "https://actor.internal/".into(),
                vec![],
                vec![]
            )
            .await,
            Err(Error::Broken)
        ));
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
        // Negative control: a fresh stub is allowed to reach exactly the same address.
        let fresh = namespace.get(&id, None)?;
        fresh
            .fetch(
                "GET".into(),
                "https://actor.internal/".into(),
                vec![],
                vec![],
            )
            .await?;
        transport.pending.store(true, Ordering::SeqCst);
        let mut call = Box::pin(fresh.fetch(
            "GET".into(),
            "https://actor.internal/".into(),
            vec![],
            vec![],
        ));
        assert!(futures::poll!(call.as_mut()).is_pending());
        drop(call);
        assert!(matches!(
            fresh
                .fetch(
                    "GET".into(),
                    "https://actor.internal/".into(),
                    vec![],
                    vec![]
                )
                .await,
            Err(Error::Broken)
        ));
        assert_eq!(transport.calls.load(Ordering::SeqCst), 3);
        Ok(())
    }
}

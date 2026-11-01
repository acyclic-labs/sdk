//! Projection of existing operation fields into the shared actor address.
use super::*;
use crate::addressing::{ActorReference, IdentityError};

fn namespace(value: Option<&ActorNamespace>) -> &str {
    value.map_or(
        crate::addressing::DEFAULT_ACTOR_NAMESPACE,
        ActorNamespace::as_str,
    )
}

fn reference(
    id: &ActorId,
    scope: Option<&ActorNamespace>,
    stored_account: &[u8],
    fallback_account: &[u8],
) -> Result<ActorReference, IdentityError> {
    ActorReference::new(
        if stored_account.is_empty() {
            fallback_account
        } else {
            stored_account
        },
        namespace(scope).as_bytes(),
        id.as_str().as_bytes(),
    )
}

macro_rules! actor_scope {
    ($($request:ty),* $(,)?) => {$(
        impl $request {
            /// Exact namespace, resolving only the absent legacy field.
            pub fn namespace(&self) -> &str {
                namespace(self.namespace.as_ref())
            }
            /// Recover the complete address; legacy callers supply their account field.
            pub fn reference(&self, account: &[u8]) -> Result<ActorReference, IdentityError> {
                reference(&self.actor_id, self.namespace.as_ref(), &self.account_id, account)
            }
            /// Bind an operation to a validated address, without adding authority.
            pub fn with_reference(mut self, reference: &ActorReference) -> Result<Self, DomainError> {
                self.actor_id = reference.into();
                self.namespace = Some(reference.namespace().into());
                self.account_id = reference.account().as_bytes().to_vec();
                Ok(self)
            }
        }
    )*};
}

actor_scope!(
    InspectActorRequest,
    AddSubscriptionRequest,
    RemoveSubscriptionRequest,
    ResumeSubscriptionRequest,
    CheckpointActorRequest,
    DeleteAllRequest,
    InvokeActorRequest,
    ActorObservation,
);

impl ForkActorRequest {
    /// Exact namespace of both the parent and the child.
    pub fn namespace(&self) -> &str {
        namespace(self.namespace.as_ref())
    }
    /// The parent's complete address; legacy callers supply their account.
    pub fn reference(&self, account: &[u8]) -> Result<ActorReference, IdentityError> {
        self.address(account, &self.actor_id)
    }
    /// The child's complete address, in the parent's account and namespace.
    pub fn child_reference(&self, account: &[u8]) -> Result<ActorReference, IdentityError> {
        self.address(account, &self.child_actor_id)
    }
    fn address(&self, account: &[u8], id: &ActorId) -> Result<ActorReference, IdentityError> {
        reference(id, self.namespace.as_ref(), &self.account_id, account)
    }
}

impl InvokeActorRequest {
    /// Advisory first-access placement; jurisdiction remains namespace config.
    pub fn with_location_hint(mut self, hint: Option<String>) -> Result<Self, DomainError> {
        if let Some(value) = &hint {
            crate::addressing::validate_location_hint(value)?;
        }
        self.location_hint = hint;
        Ok(self)
    }
    /// Exact advisory location hint, if supplied.
    pub fn location_hint(&self) -> Option<&str> {
        self.location_hint.as_deref()
    }
}

#[cfg(test)]
mod tests;

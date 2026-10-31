//! Projection of existing operation fields into the shared actor address.
use super::*;

impl InspectActorRequest {
    /// Exact namespace, resolving only the absent legacy field.
    pub fn namespace(&self) -> &str {
        self.namespace.as_ref().map_or(
            crate::addressing::DEFAULT_ACTOR_NAMESPACE,
            ActorNamespace::as_str,
        )
    }
    /// Recover the complete address; legacy callers supply their account field.
    pub fn reference(
        &self,
        account: &[u8],
    ) -> Result<crate::addressing::ActorReference, crate::addressing::IdentityError> {
        let account = if self.account_id.is_empty() {
            account
        } else {
            &self.account_id
        };
        crate::addressing::ActorReference::new(
            account,
            self.namespace().as_bytes(),
            self.actor_id.as_str().as_bytes(),
        )
    }
    /// Bind an operation to a validated address, without adding authority.
    pub fn with_reference(
        mut self,
        reference: &crate::addressing::ActorReference,
    ) -> Result<Self, DomainError> {
        self.actor_id = ActorId::new(reference.id().to_owned())?;
        self.namespace = Some(ActorNamespace::new(
            reference.namespace().as_str().to_owned(),
        )?);
        self.account_id = reference.account().as_bytes().to_vec();
        Ok(self)
    }
}

impl AddSubscriptionRequest {
    /// Exact namespace, resolving only the absent legacy field.
    pub fn namespace(&self) -> &str {
        self.namespace.as_ref().map_or(
            crate::addressing::DEFAULT_ACTOR_NAMESPACE,
            ActorNamespace::as_str,
        )
    }
    /// Recover the complete address; legacy callers supply their account field.
    pub fn reference(
        &self,
        account: &[u8],
    ) -> Result<crate::addressing::ActorReference, crate::addressing::IdentityError> {
        let account = if self.account_id.is_empty() {
            account
        } else {
            &self.account_id
        };
        crate::addressing::ActorReference::new(
            account,
            self.namespace().as_bytes(),
            self.actor_id.as_str().as_bytes(),
        )
    }
    /// Bind an operation to a validated address, without adding authority.
    pub fn with_reference(
        mut self,
        reference: &crate::addressing::ActorReference,
    ) -> Result<Self, DomainError> {
        self.actor_id = ActorId::new(reference.id().to_owned())?;
        self.namespace = Some(ActorNamespace::new(
            reference.namespace().as_str().to_owned(),
        )?);
        self.account_id = reference.account().as_bytes().to_vec();
        Ok(self)
    }
}

impl RemoveSubscriptionRequest {
    /// Exact namespace, resolving only the absent legacy field.
    pub fn namespace(&self) -> &str {
        self.namespace.as_ref().map_or(
            crate::addressing::DEFAULT_ACTOR_NAMESPACE,
            ActorNamespace::as_str,
        )
    }
    /// Recover the complete address; legacy callers supply their account field.
    pub fn reference(
        &self,
        account: &[u8],
    ) -> Result<crate::addressing::ActorReference, crate::addressing::IdentityError> {
        let account = if self.account_id.is_empty() {
            account
        } else {
            &self.account_id
        };
        crate::addressing::ActorReference::new(
            account,
            self.namespace().as_bytes(),
            self.actor_id.as_str().as_bytes(),
        )
    }
    /// Bind an operation to a validated address, without adding authority.
    pub fn with_reference(
        mut self,
        reference: &crate::addressing::ActorReference,
    ) -> Result<Self, DomainError> {
        self.actor_id = ActorId::new(reference.id().to_owned())?;
        self.namespace = Some(ActorNamespace::new(
            reference.namespace().as_str().to_owned(),
        )?);
        self.account_id = reference.account().as_bytes().to_vec();
        Ok(self)
    }
}

impl ResumeSubscriptionRequest {
    /// Exact namespace, resolving only the absent legacy field.
    pub fn namespace(&self) -> &str {
        self.namespace.as_ref().map_or(
            crate::addressing::DEFAULT_ACTOR_NAMESPACE,
            ActorNamespace::as_str,
        )
    }
    /// Recover the complete address; legacy callers supply their account field.
    pub fn reference(
        &self,
        account: &[u8],
    ) -> Result<crate::addressing::ActorReference, crate::addressing::IdentityError> {
        let account = if self.account_id.is_empty() {
            account
        } else {
            &self.account_id
        };
        crate::addressing::ActorReference::new(
            account,
            self.namespace().as_bytes(),
            self.actor_id.as_str().as_bytes(),
        )
    }
    /// Bind an operation to a validated address, without adding authority.
    pub fn with_reference(
        mut self,
        reference: &crate::addressing::ActorReference,
    ) -> Result<Self, DomainError> {
        self.actor_id = ActorId::new(reference.id().to_owned())?;
        self.namespace = Some(ActorNamespace::new(
            reference.namespace().as_str().to_owned(),
        )?);
        self.account_id = reference.account().as_bytes().to_vec();
        Ok(self)
    }
}

impl CheckpointActorRequest {
    /// Exact namespace, resolving only the absent legacy field.
    pub fn namespace(&self) -> &str {
        self.namespace.as_ref().map_or(
            crate::addressing::DEFAULT_ACTOR_NAMESPACE,
            ActorNamespace::as_str,
        )
    }
    /// Recover the complete address; legacy callers supply their account field.
    pub fn reference(
        &self,
        account: &[u8],
    ) -> Result<crate::addressing::ActorReference, crate::addressing::IdentityError> {
        let account = if self.account_id.is_empty() {
            account
        } else {
            &self.account_id
        };
        crate::addressing::ActorReference::new(
            account,
            self.namespace().as_bytes(),
            self.actor_id.as_str().as_bytes(),
        )
    }
    /// Bind an operation to a validated address, without adding authority.
    pub fn with_reference(
        mut self,
        reference: &crate::addressing::ActorReference,
    ) -> Result<Self, DomainError> {
        self.actor_id = ActorId::new(reference.id().to_owned())?;
        self.namespace = Some(ActorNamespace::new(
            reference.namespace().as_str().to_owned(),
        )?);
        self.account_id = reference.account().as_bytes().to_vec();
        Ok(self)
    }
}

impl ForkActorRequest {
    /// Exact namespace of both the parent and the child.
    pub fn namespace(&self) -> &str {
        self.namespace.as_ref().map_or(
            crate::addressing::DEFAULT_ACTOR_NAMESPACE,
            ActorNamespace::as_str,
        )
    }
    /// The parent's complete address; legacy callers supply their account.
    pub fn reference(
        &self,
        account: &[u8],
    ) -> Result<crate::addressing::ActorReference, crate::addressing::IdentityError> {
        self.address(account, &self.actor_id)
    }
    /// The child's complete address, in the parent's account and namespace.
    pub fn child_reference(
        &self,
        account: &[u8],
    ) -> Result<crate::addressing::ActorReference, crate::addressing::IdentityError> {
        self.address(account, &self.child_actor_id)
    }
    fn address(
        &self,
        account: &[u8],
        id: &ActorId,
    ) -> Result<crate::addressing::ActorReference, crate::addressing::IdentityError> {
        let account = if self.account_id.is_empty() {
            account
        } else {
            &self.account_id
        };
        crate::addressing::ActorReference::new(
            account,
            self.namespace().as_bytes(),
            id.as_str().as_bytes(),
        )
    }
}

impl DeleteAllRequest {
    /// Exact namespace, resolving only the absent legacy field.
    pub fn namespace(&self) -> &str {
        self.namespace.as_ref().map_or(
            crate::addressing::DEFAULT_ACTOR_NAMESPACE,
            ActorNamespace::as_str,
        )
    }
    /// Recover the complete address; legacy callers supply their account field.
    pub fn reference(
        &self,
        account: &[u8],
    ) -> Result<crate::addressing::ActorReference, crate::addressing::IdentityError> {
        let account = if self.account_id.is_empty() {
            account
        } else {
            &self.account_id
        };
        crate::addressing::ActorReference::new(
            account,
            self.namespace().as_bytes(),
            self.actor_id.as_str().as_bytes(),
        )
    }
    /// Bind an operation to a validated address, without adding authority.
    pub fn with_reference(
        mut self,
        reference: &crate::addressing::ActorReference,
    ) -> Result<Self, DomainError> {
        self.actor_id = ActorId::new(reference.id().to_owned())?;
        self.namespace = Some(ActorNamespace::new(
            reference.namespace().as_str().to_owned(),
        )?);
        self.account_id = reference.account().as_bytes().to_vec();
        Ok(self)
    }
}

impl InvokeActorRequest {
    /// Exact namespace, resolving only the absent legacy field.
    pub fn namespace(&self) -> &str {
        self.namespace.as_ref().map_or(
            crate::addressing::DEFAULT_ACTOR_NAMESPACE,
            ActorNamespace::as_str,
        )
    }
    /// Recover the complete address; legacy callers supply their account field.
    pub fn reference(
        &self,
        account: &[u8],
    ) -> Result<crate::addressing::ActorReference, crate::addressing::IdentityError> {
        let account = if self.account_id.is_empty() {
            account
        } else {
            &self.account_id
        };
        crate::addressing::ActorReference::new(
            account,
            self.namespace().as_bytes(),
            self.actor_id.as_str().as_bytes(),
        )
    }
    /// Bind an operation to a validated address, without adding authority.
    pub fn with_reference(
        mut self,
        reference: &crate::addressing::ActorReference,
    ) -> Result<Self, DomainError> {
        self.actor_id = ActorId::new(reference.id().to_owned())?;
        self.namespace = Some(ActorNamespace::new(
            reference.namespace().as_str().to_owned(),
        )?);
        self.account_id = reference.account().as_bytes().to_vec();
        Ok(self)
    }
}

impl ActorObservation {
    /// Exact namespace, resolving only the absent legacy field.
    pub fn namespace(&self) -> &str {
        self.namespace.as_ref().map_or(
            crate::addressing::DEFAULT_ACTOR_NAMESPACE,
            ActorNamespace::as_str,
        )
    }
    /// Recover the complete address; legacy callers supply their account field.
    pub fn reference(
        &self,
        account: &[u8],
    ) -> Result<crate::addressing::ActorReference, crate::addressing::IdentityError> {
        let account = if self.account_id.is_empty() {
            account
        } else {
            &self.account_id
        };
        crate::addressing::ActorReference::new(
            account,
            self.namespace().as_bytes(),
            self.actor_id.as_str().as_bytes(),
        )
    }
    /// Bind an operation to a validated address, without adding authority.
    pub fn with_reference(
        mut self,
        reference: &crate::addressing::ActorReference,
    ) -> Result<Self, DomainError> {
        self.actor_id = ActorId::new(reference.id().to_owned())?;
        self.namespace = Some(ActorNamespace::new(
            reference.namespace().as_str().to_owned(),
        )?);
        self.account_id = reference.account().as_bytes().to_vec();
        Ok(self)
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

//! Exact resource-owner contribution through the original authority port.
use super::*;
use acyclic_auth::{
    contract::{CaveatKind, GrantKind, Principal, PrincipalKind},
    verify::{TrustEvidence, VerifiedCapability},
};

/// A trusted resource producer's already-authorized selection. Request decoding
/// cannot construct one: paths and descriptive caller metadata confer no grant.
/// This value borrows original signed facts and transfers no live custody.
pub struct ResourceIssuance<'a> {
    pub(crate) parent: &'a Scope,
    pub(crate) grants: &'a [(&'a str, bool, &'a [&'a str])],
    pub(crate) caller: Option<(&'a Principal, &'a TrustEvidence)>,
    pub(crate) now: u64,
}

impl ResourceIssuance<'_> {
    /// Original selected scope, independently admitted by the resource owner.
    pub fn parent(&self) -> &Scope {
        self.parent
    }
    /// Exact resource names, prefix semantics and permitted actions.
    pub fn grants(&self) -> &[(&str, bool, &[&str])] {
        self.grants
    }
    /// Original acting Principal and complete authenticated trust evidence.
    /// The calling session retains and supervises its original live owner.
    pub fn caller(&self) -> Option<(&Principal, &TrustEvidence)> {
        self.caller
    }
    /// Resource owner's admission time. The original issuer's current trust
    /// and clock still fence the result before it can be disclosed.
    pub fn now_unix_seconds(&self) -> u64 {
        self.now
    }

    fn grants_match(&self, capability: &VerifiedCapability) -> bool {
        capability.effective().grants().any(|set| {
            set.grants.len() == self.grants.len()
                && set
                    .grants
                    .iter()
                    .zip(self.grants)
                    .all(|(grant, (name, prefix, actions))| {
                        matches!(&grant.kind, GrantKind::Resource(grant)
                        if grant.resource.name == *name && grant.resource.prefix == *prefix
                        && grant.actions.iter().map(String::as_str).eq(actions.iter().copied()))
                    })
        })
    }
}

impl AdmittedAuthority {
    pub(crate) async fn issue_resources(
        &self,
        context: &Context,
        operation: OperationId,
        request: &ResourceIssuance<'_>,
    ) -> Result<VerifiedCapability> {
        context.check().map_err(|_| Error::Cancelled)?;
        let parent = self.parent(request.parent)?;
        let issued = self
            .run(context, operation, async {
                match &self.custody {
                    Custody::Native(root) => root.resource_credential(
                        request.parent,
                        request.grants,
                        request.caller,
                        request.now,
                    ),
                    Custody::Platform(source) => {
                        source
                            .resource_credential(context, operation, request)
                            .await
                    }
                }
            })
            .await?;
        self.check_scope(request.parent)
            .map_err(|error| Self::after_dispatch(error, operation))?;
        if matches!(self.custody, Custody::Platform(_)) {
            let audience = workload::resource(&self.caller.verifier.audience)?;
            let principal = issued.effective().principal();
            let selected = match parent {
                Parent::Original(owner) => owner.caller.evidence.contributed_to(&issued),
                Parent::Descendant(parent) => parent
                    .contributed_to(&issued)
                    .map_err(workload::unauthorized)?,
            };
            let caller = match request.caller {
                Some((expected, evidence)) => {
                    principal == expected && evidence.contributed_to(&issued)
                }
                None => {
                    principal.kind == PrincipalKind::Service
                        && principal.id == request.parent.issuer()
                        && principal.lineage.is_empty()
                        && principal.groups.is_empty()
                        && principal.roles.is_empty()
                        && principal.attributes.is_empty()
                }
            };
            let recipient = issued.blocks().iter().flat_map(|block| &block.caveats).any(|caveat| {
                matches!(&caveat.kind, CaveatKind::Audience(value) if !value.prefix && value.name == audience)
            });
            if !issued.is_sealed()
                || !selected
                || !caller
                || !recipient
                || !request.grants_match(&issued)
            {
                return Err(Error::Unauthorized(
                    "platform returned a different resource contribution".into(),
                ));
            }
            // Reuse canonical current trust, validity, recipient and context
            // checks. Immutable signature admission is already owned by issued.
            issued
                .authorize(
                    &*self.caller.verifier.public.trust,
                    &acyclic_auth::authority::Request {
                        target: acyclic_auth::authority::Target::Context,
                        audience: &audience,
                        holder: None,
                        now_unix_seconds: self.caller.verifier.public.now(),
                        keys: &[],
                        attributes: &Default::default(),
                        usage: &Default::default(),
                        cpu_millis: 0,
                        memory_bytes: 0,
                    },
                    acyclic_auth::authority::PolicyDecision::Allow,
                    acyclic_auth::authority::Scope::Exact,
                )
                .map_err(workload::unauthorized)?;
        }
        context
            .check()
            .map_err(|_| Error::Indeterminate(operation))?;
        Ok(issued)
    }
}

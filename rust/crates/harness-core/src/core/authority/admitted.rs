//! Parent-bound runtime operations. Root signing material never crosses this port.
use super::*;
use crate::OperationId;
use acyclic_auth::operation::Context;
use acyclic_stream::{BoxProviderFuture, ProviderPlatform};

/// A checkpoint operation, rather than an unrestricted signing oracle.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointKind {
    /// Committed execution-index checkpoint.
    Execution,
    /// Committed working-context checkpoint.
    Working,
    /// Exact prepared model request.
    ModelRequest,
}

impl CheckpointKind {
    pub(super) fn domain(self) -> &'static [u8] {
        match self {
            Self::Execution => b"harness/v2/execution-index-checkpoint\0",
            Self::Working => b"harness/v2/working-context-checkpoint\0",
            Self::ModelRequest => b"harness/v2/model-request-checkpoint\0",
        }
    }
}

/// Exact derivation requested from the original authority custodian.
#[derive(Clone, Copy, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScopeDerivation<'a> {
    /// Ordinary attenuation never changes the acting agent or widens labels.
    Narrow {
        /// Requested labels, all contained in the parent grant.
        capabilities: &'a Capabilities,
        /// Acting identity checked by the trusted resource producer.
        agent: Option<AgentId>,
    },
    /// An original private-volume owner shares one immutable file.
    PrivateFileRead {
        /// Exact immutable file, including version and private-volume owner.
        file: &'a FileRef,
        /// Recipient agent.
        reader: AgentId,
    },
    /// An original private-volume owner shares one directory prefix.
    PrivateDirectoryRead {
        /// Original owner's private volume.
        volume: &'a crate::conversation::VolumeRef,
        /// Canonical path prefix within that volume.
        prefix: &'a str,
        /// Recipient agent.
        reader: AgentId,
    },
}

impl<'a> ScopeDerivation<'a> {
    pub(super) fn resolve(
        self,
        parent: &Scope,
    ) -> Result<(std::borrow::Cow<'a, Capabilities>, Option<AgentId>)> {
        use std::borrow::Cow;
        match self {
            Self::Narrow {
                capabilities,
                agent,
            } => {
                if !capabilities.is_subset_of(parent.capabilities()) {
                    return Err(Error::Unauthorized("child scope widens its parent".into()));
                }
                Ok((Cow::Borrowed(capabilities), agent))
            }
            Self::PrivateFileRead { file, reader } => {
                file.validate(&mut None)?;
                if is_internal_path(file.path()) {
                    return Err(Error::Unauthorized(
                        "internal storage files cannot be delegated".into(),
                    ));
                }
                Self::owner_read(parent, file.volume())?;
                Ok((
                    Cow::Owned(Capabilities::new([file.read_capability(&mut None)?])),
                    Some(reader),
                ))
            }
            Self::PrivateDirectoryRead {
                volume,
                prefix,
                reader,
            } => {
                Self::owner_read(parent, volume)?;
                Ok((
                    Cow::Owned(Capabilities::new([
                        volume.directory_read_capability(prefix, &mut None)?
                    ])),
                    Some(reader),
                ))
            }
        }
    }

    fn owner_read(parent: &Scope, volume: &crate::conversation::VolumeRef) -> Result<()> {
        volume.validate(&mut None)?;
        if !matches!(volume.owner(), VolumeOwner::Agent(owner) if Some(*owner) == parent.agent)
            || !parent
                .capabilities
                .contains(&volume.capability(VolumeOperation::Read, &mut None)?)
        {
            return Err(Error::Unauthorized(
                "private read delegation requires its original owner".into(),
            ));
        }
        Ok(())
    }
}

/// Platform-owned operations for one independently admitted caller.
///
/// The opener obtains this capability from its authenticated platform, never
/// from a request body. The implementation retains the original caller and
/// checks every request against it; the guest cannot choose another caller.
/// Dropped operation producers settle with a typed error. Uncertain completion
/// returns `Error::Indeterminate` with the supplied original operation identity;
/// neither side redispatches. The original `Context` also bounds every wait.
pub trait AuthorityOperations: ProviderPlatform {
    /// Observe original admission, this exact scope's current revocation and
    /// custody availability. An older guest trust snapshot cannot replace it.
    /// This is synchronous and starts no foreign operation.
    fn check(&self, scope: &Scope) -> Result<()>;

    /// Derive a host-attested scope from an authenticated descendant of the
    /// original admission. The platform checks the actual created resource or
    /// acting agent; request metadata alone never establishes that authority.
    /// The result is sealed public evidence, with no restricted append tip.
    fn derive<'a>(
        &'a self,
        context: &'a Context,
        operation: OperationId,
        parent: &'a Scope,
        id: &'a str,
        request: ScopeDerivation<'a>,
    ) -> BoxProviderFuture<'a, Result<Scope>>;

    /// Sign only the named checkpoint under the admitted audience. The platform
    /// supplies the issuer and audience; neither is accepted from the guest.
    fn checkpoint<'a>(
        &'a self,
        context: &'a Context,
        operation: OperationId,
        kind: CheckpointKind,
        digest: [u8; 32],
    ) -> BoxProviderFuture<'a, Result<[u8; 64]>>;
}

#[derive(Clone)]
enum Custody {
    Native(AuthorityRoot),
    Platform(Arc<dyn AuthorityOperations>),
}

enum Parent<'a> {
    Original(&'a AdmittedAuthority),
    Descendant(Arc<acyclic_auth::verify::VerifiedCapability>),
}

impl Parent<'_> {
    fn preserved_by(&self, descendant: &acyclic_auth::verify::VerifiedCapability) -> Result<bool> {
        match self {
            Self::Original(owner) => owner.caller.evidence.preserved_by(descendant),
            Self::Descendant(parent) => parent.preserved_by(descendant),
        }
        .map_err(workload::unauthorized)
    }
}

/// An opaque authority capability over one original verified caller.
///
/// Native custody stays in the host. The original expiry and cancellation
/// producer are bound once, before serving requests. This is a runtime grant
/// owner, not the customer's `acyclic_auth::contract::CallerContext` identity.
/// Guest transports use that invocation's opaque handle and public admission
/// proofs; a `Scope` containing an append secret must remain with its custodian.
#[derive(Clone)]
pub struct AdmittedAuthority {
    pub(super) caller: VerifiedScope,
    pub(super) context: Context,
    custody: Custody,
}

impl acyclic_auth::trust::ChangeSource for AdmittedAuthority {
    fn check_open(&self) -> std::result::Result<(), acyclic_auth::trust::SourceClosed> {
        self.caller.verifier.public.changes.check_open()
    }
    fn changed(&self) -> acyclic_auth::trust::Changed {
        self.caller.verifier.public.changes.changed()
    }
}

impl acyclic_auth::guest::GuestAdmissionOwner for AdmittedAuthority {
    type Error = Error;

    fn check(&self) -> Result<()> {
        AdmittedAuthority::check(self)
    }
}

impl AdmittedAuthority {
    fn after_dispatch(error: Error, operation: OperationId) -> Error {
        match error {
            Error::Cancelled => Error::Indeterminate(operation),
            error => error,
        }
    }

    /// Bind a platform-admitted operation port to its independently authenticated
    /// public configuration and sealed original caller. This belongs to the trusted
    /// opener, never to request decoding or routing. `changes` is the SAME owner's
    /// live trust stream used to construct `verifier`; source loss fences active
    /// invocations. The original operations guard observes each publication.
    pub fn from_platform(
        verifier: AuthorityVerifier,
        caller: Scope,
        context: Context,
        operations: Arc<dyn AuthorityOperations>,
        changes: Arc<dyn acyclic_auth::trust::ChangeSource>,
    ) -> Result<Self> {
        if !Arc::ptr_eq(&verifier.public.changes, &changes) {
            return Err(Error::Unauthorized(
                "platform trust change owner mismatch".into(),
            ));
        }
        Self::new(verifier, caller, context, Custody::Platform(operations))
    }

    fn new(
        verifier: AuthorityVerifier,
        caller: Scope,
        context: Context,
        custody: Custody,
    ) -> Result<Self> {
        let authenticated = workload::verify(
            &verifier,
            &verifier.audience,
            &caller,
            workload::now(),
            &mut None,
        )?;
        if matches!(custody, Custody::Platform(_)) && !authenticated.is_sealed() {
            return Err(Error::Unauthorized(
                "platform admission exports restricted append authority".into(),
            ));
        }
        let expiry = authenticated.effective().expires_at_unix_seconds();
        if u128::from(context.deadline_unix_millis()) > u128::from(expiry) * 1000 {
            return Err(Error::Invalid(
                "authority operation deadline exceeds its original admission".into(),
            ));
        }
        let caller = VerifiedScope {
            scope: caller,
            verifier,
            evidence: authenticated
                .trust_evidence()
                .map_err(workload::unauthorized)?,
        };
        let capability = Self {
            caller,
            context,
            custody,
        };
        capability.check()?;
        Ok(capability)
    }

    /// The immutable, independently verified caller shared by serving and the
    /// selected-volume factory. No request can replace this scope or audience.
    pub fn caller(&self) -> (&Authority, &Scope) {
        (&self.caller.verifier.audience, &self.caller.scope)
    }

    /// Public evidence for the original admission. Exporting it never transfers
    /// the host's restricted append tip or its runtime signing custody.
    pub fn public_scope(&self) -> Result<Scope> {
        self.check()?;
        let scope = &self.caller.scope;
        let capability = workload::verify(
            &self.caller.verifier,
            &self.caller.verifier.audience,
            scope,
            workload::now(),
            &mut None,
        )?;
        let mut encoded = capability.encode_sealed().map_err(workload::unauthorized)?;
        Ok(Scope::from_wire(
            scope.id.clone(),
            scope.capabilities.clone(),
            scope.issuer.clone(),
            scope.agent,
            scope.parent_proof,
            *scope.proof,
            std::mem::take(&mut *encoded),
        ))
    }

    /// Non-dispatching observation for synchronous admission boundaries.
    pub fn check(&self) -> Result<()> {
        self.check_scope(&self.caller.scope)
    }

    pub(super) fn check_scope(&self, scope: &Scope) -> Result<()> {
        self.context.check().map_err(|_| Error::Cancelled)?;
        self.caller
            .verifier
            .public
            .changes
            .check_open()
            .map_err(|_| Error::Cancelled)?;
        if let Custody::Platform(source) = &self.custody {
            source.check(scope)?;
        }
        self.context.check().map_err(|_| Error::Cancelled)?;
        self.caller.recheck(&mut None)?;
        Ok(())
    }

    async fn run<T>(
        &self,
        context: &Context,
        operation: OperationId,
        work: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        self.context
            .run(context.run(work))
            .await
            .map_err(|_| Error::Indeterminate(operation))?
            .map_err(|_| Error::Indeterminate(operation))?
    }

    fn parent<'a>(&'a self, parent: &Scope) -> Result<Parent<'a>> {
        self.check_scope(parent)?;
        if std::ptr::eq(parent, &self.caller.scope) {
            return Ok(Parent::Original(self));
        }
        let capability = workload::verify(
            &self.caller.verifier,
            &self.caller.verifier.audience,
            parent,
            workload::now(),
            &mut None,
        )?;
        if matches!(self.custody, Custody::Platform(_)) && !capability.is_sealed() {
            return Err(Error::Unauthorized(
                "platform request exports restricted append authority".into(),
            ));
        }
        if !self
            .caller
            .evidence
            .preserved_by(&capability)
            .map_err(workload::unauthorized)?
        {
            return Err(Error::Unauthorized(
                "scope drops original admission dependencies".into(),
            ));
        }
        Ok(Parent::Descendant(capability))
    }

    /// Derive exactly the requested host-attested child without exporting custody.
    /// No retry occurs after an uncertain outcome. Native execution creates no
    /// task, timer or boxed future; the platform branch awaits its one producer.
    pub async fn derive(
        &self,
        context: &Context,
        operation: OperationId,
        parent: &Scope,
        id: &str,
        capabilities: &Capabilities,
        agent: Option<AgentId>,
    ) -> Result<Scope> {
        self.derive_request(
            context,
            operation,
            parent,
            id,
            ScopeDerivation::Narrow {
                capabilities,
                agent,
            },
        )
        .await
    }

    /// Apply one typed derivation after authenticating its exact parent.
    pub async fn derive_request(
        &self,
        context: &Context,
        operation: OperationId,
        parent: &Scope,
        id: &str,
        request: ScopeDerivation<'_>,
    ) -> Result<Scope> {
        context.check().map_err(|_| Error::Cancelled)?;
        let authenticated = self.parent(parent)?;
        let (capabilities, agent) = request.resolve(parent)?;
        let capabilities = capabilities.as_ref();
        let issued = self
            .run(context, operation, async {
                match &self.custody {
                    Custody::Native(root) => {
                        if matches!(request, ScopeDerivation::Narrow { .. })
                            && agent != parent.agent()
                        {
                            return Err(Error::Unauthorized(
                                "delegation cannot attest a different acting agent".into(),
                            ));
                        }
                        root.issue_derived(parent, id, capabilities, agent, &mut None)
                    }
                    Custody::Platform(source) => {
                        source.derive(context, operation, parent, id, request).await
                    }
                }
            })
            .await?;
        match &self.custody {
            Custody::Native(_) => self
                .check()
                .map_err(|error| Self::after_dispatch(error, operation))?,
            Custody::Platform(_) => {
                self.check_scope(&issued)
                    .map_err(|error| Self::after_dispatch(error, operation))?;
                let capability = workload::verify(
                    &self.caller.verifier,
                    &self.caller.verifier.audience,
                    &issued,
                    workload::now(),
                    &mut None,
                )?;
                if !capability.is_sealed()
                    || issued.id() != id
                    || issued.capabilities() != capabilities
                    || issued.agent() != agent
                    || issued.issuer() != parent.issuer()
                    || issued.parent_proof != Some(*parent.proof)
                    || !authenticated.preserved_by(&capability)?
                {
                    return Err(Error::Unauthorized(
                        "platform returned a different derived authority".into(),
                    ));
                }
            }
        }
        context
            .check()
            .map_err(|_| Error::Indeterminate(operation))?;
        Ok(issued)
    }

    /// Produce a typed checkpoint, verifying the returned signature before use.
    pub async fn checkpoint(
        &self,
        context: &Context,
        operation: OperationId,
        kind: CheckpointKind,
        digest: [u8; 32],
    ) -> Result<[u8; 64]> {
        context.check().map_err(|_| Error::Cancelled)?;
        self.check()?;
        let verifier = &self.caller.verifier;
        let value = (&verifier.id, &verifier.audience, digest);
        let proof = self
            .run(context, operation, async {
                match &self.custody {
                    Custody::Native(_) => {
                        verifier.canonical_proof(kind.domain(), &value, &mut None)
                    }
                    Custody::Platform(source) => {
                        source.checkpoint(context, operation, kind, digest).await
                    }
                }
            })
            .await?;
        self.check()
            .map_err(|error| Self::after_dispatch(error, operation))?;
        if let Custody::Platform(_) = &self.custody {
            verifier.verify_canonical_proof(kind.domain(), &value, &proof, &mut None)?;
        }
        context
            .check()
            .map_err(|_| Error::Indeterminate(operation))?;
        Ok(proof)
    }
}

impl AuthorityOperations for AdmittedAuthority {
    fn check(&self, scope: &Scope) -> Result<()> {
        self.check_scope(scope)?;
        self.caller.verifier.verify(scope, &mut None)
    }
    fn derive<'a>(
        &'a self,
        context: &'a Context,
        operation: OperationId,
        parent: &'a Scope,
        id: &'a str,
        request: ScopeDerivation<'a>,
    ) -> BoxProviderFuture<'a, Result<Scope>> {
        Box::pin(async move {
            Self::derive_request(self, context, operation, parent, id, request)
                .await?
                .seal(&self.caller.verifier)
        })
    }
    fn checkpoint<'a>(
        &'a self,
        context: &'a Context,
        operation: OperationId,
        kind: CheckpointKind,
        digest: [u8; 32],
    ) -> BoxProviderFuture<'a, Result<[u8; 64]>> {
        Box::pin(Self::checkpoint(self, context, operation, kind, digest))
    }
}

impl AuthorityRoot {
    /// Admit the original caller at a native host boundary. The resulting port
    /// invokes this same issuer, retaining no separate signing implementation.
    pub fn admit(&self, caller: Scope, context: Context) -> Result<AdmittedAuthority> {
        AdmittedAuthority::new(
            self.verifier(&mut None)?,
            caller,
            context,
            Custody::Native(self.clone()),
        )
    }
}

#[cfg(test)]
mod tests;

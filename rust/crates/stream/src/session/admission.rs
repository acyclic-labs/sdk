//! The one place a session credential is verified. A session's
//! [`CredentialVerifier`] turns each credential a client presents into the
//! [`Admission`] the session's slots hold. Every call, read and follow
//! authorization is built from that admission and revalidated on every
//! trust change: the stock [`CapabilityVerifier`] for this SDK's capability
//! envelope, or a host's own verifier for the credentials its gateway issues
//! (an OAuth or JWS token, say).
use super::*;
use acyclic_auth::{
    authority::{PolicyDecision, Request, Scope},
    contract::{GrantSet, Principal},
    verify::{self, Trust, VerifiedCapability, VerifyError},
};

/// A credential its session's verifier admitted, held for as long as the
/// credential stays registered in its slot. Every authorization the session
/// grants on it rechecks it against the current trust.
pub trait Admission: Send + Sync + 'static {
    /// The issuer-attested identity, including delegation lineage.
    fn principal(&self) -> &Principal;

    /// Exclusive UTC Unix seconds after which nothing is admitted.
    fn expires_at_unix_seconds(&self) -> u64;

    /// Every grant ceiling. Every ceiling must allow a request.
    fn grants(&self) -> Box<dyn Iterator<Item = &GrantSet> + '_>;

    /// Admits `request` under `trust`, intersected with the owner's
    /// `policy`, checking the facts `scope` says it can check now.
    ///
    /// # Errors
    /// When the credential is revoked, expired, or does not admit `request`.
    fn authorize(
        &self,
        trust: &(dyn Trust + Send + Sync),
        request: &Request<'_>,
        policy: PolicyDecision,
        scope: Scope,
    ) -> Result<(), VerifyError>;

    /// `requested` intersected with every filter ceiling of the credential.
    ///
    /// # Errors
    /// When the intersection is not a valid filter.
    fn follow_filter(
        &self,
        requested: wire::SessionFilter,
    ) -> Result<crate::FollowFilter, StreamError>;
}

/// Verifies the credentials a session's clients present.
pub trait CredentialVerifier: Send + Sync + 'static {
    /// Verifies `credential` under `trust`.
    ///
    /// # Errors
    /// When `credential` is malformed, untrusted, revoked or expired.
    fn verify(
        &self,
        credential: &[u8],
        trust: &TrustSnapshot,
    ) -> Result<Arc<dyn Admission>, VerifyError>;
}

/// The stock verifier: this SDK's signed capability envelope, verified
/// against the session's trust.
pub struct CapabilityVerifier;

impl CredentialVerifier for CapabilityVerifier {
    fn verify(
        &self,
        credential: &[u8],
        trust: &TrustSnapshot,
    ) -> Result<Arc<dyn Admission>, VerifyError> {
        let capability = verify::verify(credential, trust.trust.as_ref(), trust.now_unix_seconds)?;
        Ok(Arc::new(capability))
    }
}

impl Admission for VerifiedCapability {
    fn principal(&self) -> &Principal {
        self.effective().principal()
    }

    fn expires_at_unix_seconds(&self) -> u64 {
        self.effective().expires_at_unix_seconds()
    }

    fn grants(&self) -> Box<dyn Iterator<Item = &GrantSet> + '_> {
        Box::new(self.effective().grants())
    }

    fn authorize(
        &self,
        trust: &(dyn Trust + Send + Sync),
        request: &Request<'_>,
        policy: PolicyDecision,
        scope: Scope,
    ) -> Result<(), VerifyError> {
        Self::authorize(self, trust, request, policy, scope)
    }

    fn follow_filter(
        &self,
        requested: wire::SessionFilter,
    ) -> Result<crate::FollowFilter, StreamError> {
        crate::FollowFilter::authorized(requested, self)
    }
}

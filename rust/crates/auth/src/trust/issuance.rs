//! Same-key remote certification over the original issuer's authenticated transport.
//!
//! The serving owner admits the caller and commits the request digest and result
//! atomically in its existing store. This module neither grants issuance authority
//! nor creates a journal, signer, trust follower or transport retry policy.

use crate::{
    contract::{CertificateChain, CertificateChainProto, CertifiedKey, CertifiedKeyProto},
    crypto::BlockId,
    operation::{BudgetError, Context},
    verify::{CertificateVerifier, Trust, VerifyError},
};
use prost::Message;
use sha2::{Digest, Sha256};

/// An immutable public request. Renewals use a new operation key; recovery keeps
/// this exact request, including the original leaf public key and issuer epoch.
/// No private key or bearer credential is included.
pub struct Request {
    operation_key: BlockId,
    issuer: BlockId,
    environment: String,
    claims: CertifiedKeyProto,
    digest: BlockId,
}

impl Request {
    /// Admit the existing canonical key contract once, before dispatch. The
    /// opaque operation key is supplied by the original caller, never generated
    /// during retry or recovery. `issuer` is pinned by that caller's trust owner.
    ///
    /// # Errors
    /// Rejects malformed claims, an invalid issuer, or a non-root certification.
    pub fn new(
        operation_key: BlockId,
        issuer: BlockId,
        environment: String,
        claims: CertifiedKeyProto,
    ) -> Result<Self, VerifyError> {
        crate::crypto::public_key(&issuer)?;
        let claims = CertifiedKey::try_from(claims)?;
        crate::crypto::public_key(&claims.public_key)?;
        if environment.is_empty() || claims.parent_key_id.is_some() {
            return Err(VerifyError::Malformed);
        }
        let claims = CertifiedKeyProto::from(claims);
        let encoded = claims.encode_to_vec();
        let mut hash = Sha256::new();
        hash.update(b"acyclic.auth.v1/issuance\0");
        hash.update(operation_key);
        hash.update(issuer);
        hash.update((environment.len() as u64).to_le_bytes());
        hash.update(environment.as_bytes());
        hash.update((encoded.len() as u64).to_le_bytes());
        hash.update(&encoded);
        Ok(Self {
            operation_key,
            issuer,
            environment,
            claims,
            digest: hash.finalize().into(),
        })
    }

    /// Original store key, scoped to the authenticated issuer and account.
    pub fn operation_key(&self) -> &BlockId {
        &self.operation_key
    }

    /// Exact account root; a routing change must preserve this identity.
    pub fn issuer(&self) -> &BlockId {
        &self.issuer
    }

    /// Original authenticated environment.
    pub fn environment(&self) -> &str {
        &self.environment
    }

    /// Exact public claims to sign, including the complete acting principal.
    pub fn claims(&self) -> &CertifiedKeyProto {
        &self.claims
    }

    /// Versioned commitment to the original key, issuer, environment and claims.
    /// Providers compare this before signing or replaying a retained result.
    pub fn digest(&self) -> &BlockId {
        &self.digest
    }
}

/// A definitive decision that this exact request did not commit. These values
/// must never represent a transport error or an absent recovery record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The original caller's current admission or owner policy refused issuance.
    Denied,
    /// This operation key was already bound to different request bytes.
    Conflict,
    /// The original root, account or epoch no longer matches the request.
    IssuerChanged,
}

/// Why no terminal decision can currently be established. None proves absence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unknown {
    /// The original response or authenticated serving source was lost.
    Unavailable,
    /// The original invocation stopped after dispatch could have begun.
    Stopped(BudgetError),
    /// A reply did not bind to the original request or exact requested claims.
    InvalidReply,
    /// A retained certificate fails the current canonical trust check.
    Authority(VerifyError),
}

/// The original serving owner's committed decision, before client admission.
pub enum Decision {
    /// Public certificates retained atomically with this exact request digest.
    Accepted(CertificateChainProto),
    /// Definitive no-commit decision, retained under the same operation key.
    Refused(Refusal),
    /// No terminal decision available, including a missing recovery record.
    Unknown,
}

/// Authenticated transport reply. Durable acceptance precedes its disclosure.
pub struct Reply {
    /// Exact commitment retained by the original serving transaction.
    pub request_digest: BlockId,
    /// Original transaction outcome, never inferred from HTTP status or EOF.
    pub decision: Decision,
}

/// Native transport wait, dropped when the original invocation stops.
#[cfg(not(target_arch = "wasm32"))]
pub type Pending<'a> = futures::future::BoxFuture<'a, Result<Reply, Unknown>>;
/// Browser transport wait on its original event loop.
#[cfg(target_arch = "wasm32")]
pub type Pending<'a> = futures::future::LocalBoxFuture<'a, Result<Reply, Unknown>>;

/// Adapter to the ORIGINAL authenticated issuer and its existing durable store.
///
/// Before signing, atomically bind `(issuer, account, operation_key)` to the
/// request digest. A different digest refuses with `Conflict`; an identical one
/// returns the retained terminal result without signing again. Retain the exact
/// accepted certificate or refusal before acknowledging it. Resolve an uncertain
/// original commit through that store; never mint a replacement result.
///
/// Both methods retain original caller admission and custody through dispatch.
/// Source loss must end their wait as `Unknown::Unavailable`, including before
/// EOF. They perform one operation, with no redirects, polling or automatic retry.
pub trait Provider {
    /// Issue or return the original same-key decision, after current admission.
    fn issue<'a>(&'a self, context: &'a Context, request: &'a Request) -> Pending<'a>;
    /// Recover only; this method must not sign, reserve or issue a missing key.
    fn recover<'a>(&'a self, context: &'a Context, request: &'a Request) -> Pending<'a>;
}

/// A decision admitted against the caller's current trust and original request.
pub enum Outcome {
    /// Committed exact certificate, authenticated without minting a workload.
    /// It grants no authority by itself; mint/use still checks current trust.
    Accepted(CertificateChain),
    /// Definitive no-commit decision from the original authenticated issuer.
    Refused(Refusal),
    /// Retain the original request; do not substitute a new operation or key.
    Unknown(Unknown),
}

/// One prepared issuance retains its original invocation and public request.
/// Explicit recovery shares both. Only immutable public signature evidence is
/// reused; every reply checks current trust and the original clock. Exclusive
/// borrowing needs no lock, background task or retained decision cache.
pub struct Client<P> {
    provider: P,
    context: Context,
    request: Request,
    verification: CertificateVerifier,
}

impl<P: Provider> Client<P> {
    /// Retain the supplied original operation; creates no clock or stop event.
    pub fn new(provider: P, context: Context, request: Request) -> Self {
        Self {
            provider,
            context,
            request,
            verification: CertificateVerifier::default(),
        }
    }

    /// Issue once. A pre-dispatch stop returns `Err` without invoking the
    /// provider. After dispatch, a stop can only establish `Unknown`.
    ///
    /// # Errors
    /// Refuses before dispatch if the original stop or clock is unavailable.
    pub async fn issue(&mut self, trust: &(impl Trust + ?Sized)) -> Result<Outcome, BudgetError> {
        self.execute(false, trust).await
    }

    /// One explicit lookup for the SAME request. An absent record is Unknown.
    /// This never invokes `Provider::issue`, even after a lost acknowledgement.
    ///
    /// # Errors
    /// Refuses before lookup if the original stop or clock is unavailable.
    pub async fn recover(&mut self, trust: &(impl Trust + ?Sized)) -> Result<Outcome, BudgetError> {
        self.execute(true, trust).await
    }

    async fn execute(
        &mut self,
        recover: bool,
        trust: &(impl Trust + ?Sized),
    ) -> Result<Outcome, BudgetError> {
        let Self {
            provider,
            context,
            request,
            verification,
        } = self;
        context.now_unix_millis()?;
        let work = async {
            let reply = if recover {
                provider.recover(context, request).await
            } else {
                provider.issue(context, request).await
            }?;
            if reply.request_digest != request.digest {
                return Err(Unknown::InvalidReply);
            }
            match reply.decision {
                Decision::Accepted(certificates) => {
                    if certificates.keys.len() != 1
                        || certificates.keys[0].key.as_ref() != Some(&request.claims)
                        || !certificates
                            .birth
                            .as_ref()
                            .and_then(|b| b.birth.as_ref())
                            .is_some_and(|birth| {
                                birth.environment == request.environment
                                    && birth.genesis_public_key == request.issuer
                            })
                    {
                        return Err(Unknown::InvalidReply);
                    }
                    let certificates = verification
                        .verify_certificates(
                            certificates,
                            &request.issuer,
                            trust,
                            context.now_unix_millis().map_err(Unknown::Stopped)? / 1000,
                        )
                        .map_err(Unknown::Authority)?;
                    Ok(Outcome::Accepted(certificates))
                }
                Decision::Refused(reason) => Ok(Outcome::Refused(reason)),
                Decision::Unknown => Err(Unknown::Unavailable),
            }
        };
        Ok(match context.run(work).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(reason)) => Outcome::Unknown(reason),
            Err(reason) => Outcome::Unknown(Unknown::Stopped(reason.into())),
        })
    }
}

#[cfg(test)]
mod tests;

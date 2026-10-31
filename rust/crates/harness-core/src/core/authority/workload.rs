//! Product scope constraints carried in append-only Ed25519 workload chains.
use super::*;
use acyclic_auth::{
    authority::{PolicyDecision, Request, Target},
    contract::*,
    trust::{AccountRoot, Certification, Follower, Issuer},
    verify::{self, CertifiedSigner, VerifiedCapability},
};
use ed25519_dalek::SigningKey;
mod admission;
mod retained;
mod revocation;
use retained::RetainedAuthentication;
pub(super) use revocation::revoke;

pub(super) struct AuthenticatedWorkload {
    capability: std::sync::Arc<VerifiedCapability>,
    last: Claims,
}

#[derive(Default)]
pub(super) struct WorkloadOwner {
    latest: Option<RetainedAuthentication<AuthenticatedWorkload>>,
    certificates: verify::CertificateVerifier,
}

fn authenticated(capability: VerifiedCapability) -> Result<std::sync::Arc<AuthenticatedWorkload>> {
    let last = admission::scope_facts(&capability)?;
    Ok(std::sync::Arc::new(AuthenticatedWorkload {
        capability: std::sync::Arc::new(capability),
        last,
    }))
}

fn authenticate(
    public: &runtime_signing::PublicAuthority,
    trust: &dyn verify::Trust,
    scope: &Scope,
    timestamp: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<std::sync::Arc<AuthenticatedWorkload>> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
    }
    let mut retained = public.workload.lock().map_err(unauthorized)?;
    if let Some(authenticated) = retained.latest.as_ref()
        && let Some(proof) = authenticated.get(&scope.workload, work)?
    {
        return Ok(proof);
    }
    let authenticated = authenticated(
        retained
            .certificates
            .verify(&scope.workload, trust, timestamp)
            .map_err(unauthorized)?,
    )?;
    retained.latest = RetainedAuthentication::new(&scope.workload, authenticated.clone(), work)?;
    Ok(authenticated)
}

// Scope facts, format v1: each block's label names its scope, its grants
// hold the audience and one exact grant per capability, and a lineage entry
// names a new acting agent. Chains of any other form are refused.
const SCOPE: &str = "acyclic.harness.scope.v1:";
const AGENT: &str = "acyclic.harness.agent:";
const CAPABILITIES: &str = "harness:/capabilities/";
const HOLD: &str = "harness/hold";
const ACTION: &str = "harness/execute";
const CERTIFICATE_SECONDS: u64 = 30 * 24 * 60 * 60;

/// The authenticated facts of a chain's tip block; the issuer is its account.
struct Claims {
    id: String,
    capabilities: Capabilities,
    agent: Option<AgentId>,
    parent: Option<[u8; 32]>,
}
pub(super) fn unauthorized(error: impl std::fmt::Display) -> Error {
    Error::Unauthorized(format!("workload capability rejected: {error}"))
}
pub(super) fn now() -> u64 {
    acyclic_stream::UnixMillisClock::now_unix_millis(&acyclic_stream::SystemUnixMillisClock) / 1000
}
/// The local environment every runtime-certified key is born in.
pub(super) const ENVIRONMENT: &str = "local-harness";
// Root scopes reuse one certificate until half its validity remains.
const RENEW_SECONDS: u64 = CERTIFICATE_SECONDS / 2;

/// The locally run Issuer's environment key, derived from the runtime seed.
pub(super) fn issuer(seed: &[u8; 32]) -> Issuer {
    Issuer::new(
        ENVIRONMENT,
        runtime_signing::prepare_key(&blake3::derive_key("acyclic.harness.environment.v1", seed)),
    )
}

// Prepared keys stay inside runtime custody. Issuance borrows this owner;
// neither a verifier nor an admitted workload receives its root keys. Local
// and self-hosted runs publish trust exactly as a platform Issuer does: signed
// records applied to the Follower every verifier reads.
pub(super) struct RuntimeKeys {
    issuer: Issuer,
    account: AccountRoot,
    runtime: SigningKey,
    root: Option<(std::sync::Arc<CertifiedSigner>, u64)>,
}

impl RuntimeKeys {
    /// Creates account `id` and publishes its first keyring and revocation
    /// checkpoint to `trust`, which then serves it.
    pub(super) fn genesis(
        id: &str,
        issuer: Issuer,
        seed: &[u8; 32],
        trust: &Follower,
        now: u64,
    ) -> Result<Self> {
        let key = |domain| runtime_signing::prepare_key(&blake3::derive_key(domain, seed));
        let (account, records) = issuer
            .genesis(id, key("acyclic.harness.account.v1"), now)
            .map_err(unauthorized)?;
        for record in &records {
            trust.apply(record, now).map_err(unauthorized)?;
        }
        trust.resume();
        trust.serve(id);
        Ok(Self {
            issuer,
            account,
            runtime: key("acyclic.harness.runtime.v1"),
            root: None,
        })
    }

    /// Certifies the runtime key for `grants` plus admission receipts. A
    /// `parent` bounds the certificate: it expires no later and is revoked
    /// with it.
    fn certify(
        &self,
        timestamp: u64,
        mut grants: GrantSet,
        parent: Option<&VerifiedCapability>,
    ) -> Result<CertifiedSigner> {
        grants.grants.push(Grant {
            kind: GrantKind::Resource(ResourceGrant {
                resource: ResourcePattern {
                    name: "harness:/admissions/".into(),
                    prefix: true,
                },
                actions: vec![acyclic_auth::admission::RECEIPT_ACTION.into()],
            }),
        });
        let expires = timestamp
            .checked_add(CERTIFICATE_SECONDS)
            .ok_or_else(|| unauthorized("clock overflow"))?
            .min(parent.map_or(u64::MAX, |parent| {
                parent.effective().expires_at_unix_seconds()
            }));
        let certificates = self
            .issuer
            .certify(
                &self.account,
                Certification {
                    key_id: "runtime",
                    public_key: self.runtime.verifying_key(),
                    validity: Validity {
                        not_before_unix_seconds: timestamp,
                        expires_at_unix_seconds: expires,
                    },
                    grants,
                    principal: Principal {
                        kind: PrincipalKind::Service,
                        id: self.account.id().into(),
                        lineage: vec![],
                        groups: vec![],
                        roles: vec![],
                        attributes: vec![],
                    },
                    // The parent's tip first: a root block's authenticated parent.
                    parent_revocation_ids: parent
                        .into_iter()
                        .flat_map(|parent| {
                            let tip = parent.block_ids().last().map(|tip| tip.as_slice());
                            tip.into_iter()
                                .chain(parent.revocation_ids().filter(move |id| Some(*id) != tip))
                        })
                        .map(<[u8]>::to_vec)
                        .collect(),
                },
            )
            .map_err(unauthorized)?;
        #[cfg(test)]
        acyclic_test_support::differential::record(CertificateSignature);
        CertifiedSigner::new(certificates, self.runtime.clone()).map_err(unauthorized)
    }

    /// The signer of root scopes for every `harness:/` audience, certified
    /// once and renewed (the same key, a new validity) at half its lifetime.
    fn root_signer(&mut self, timestamp: u64) -> Result<std::sync::Arc<CertifiedSigner>> {
        if let Some((signer, renew_at)) = &self.root
            && timestamp < *renew_at
        {
            return Ok(signer.clone());
        }
        let mut grants = grant("harness:/", true);
        grants.grants.push(resource_grant(CAPABILITIES, true, HOLD));
        let signer = std::sync::Arc::new(self.certify(timestamp, grants, None)?);
        let renew_at = timestamp
            .checked_add(RENEW_SECONDS)
            .ok_or_else(|| unauthorized("clock overflow"))?;
        self.root = Some((signer.clone(), renew_at));
        Ok(signer)
    }

    /// Publishes the revocation of `block` and every descendant, retained
    /// until `retain_until`, and applies it to `trust`.
    pub(super) fn revoke(
        &mut self,
        trust: &Follower,
        block: [u8; 32],
        retain_until: u64,
        now: u64,
    ) -> Result<()> {
        let record = self
            .account
            .revoke(&[(block, retain_until)], now)
            .map_err(unauthorized)?;
        trust.apply(&record, now).map_err(unauthorized)?;
        Ok(())
    }

    /// Publishes a keyring that retires every key below `epoch`; the next
    /// root scope is certified at the new epoch.
    #[cfg(test)]
    pub(super) fn raise_minimum_epoch(
        &mut self,
        trust: &Follower,
        epoch: u64,
        now: u64,
    ) -> Result<()> {
        let record = self
            .account
            .raise_minimum_epoch(epoch, now)
            .map_err(unauthorized)?;
        trust.apply(&record, now).map_err(unauthorized)?;
        self.root = None;
        Ok(())
    }
}
fn entropy() -> [u8; 32] {
    let mut bytes = [0; 32];
    bytes[..16].copy_from_slice(crate::identity::fresh_uuid().as_bytes());
    bytes[16..].copy_from_slice(crate::identity::fresh_uuid().as_bytes());
    bytes
}
pub(super) fn resource(audience: &Authority) -> Result<String> {
    audience.validate_with_work(&mut None)?;
    let kind = match audience.kind {
        AggregateKind::Agent => "agents",
        AggregateKind::Conversation => "conversations",
        AggregateKind::Session => "sessions",
        AggregateKind::Turn => "turns",
        AggregateKind::Task => "tasks",
    };
    Ok(format!("harness:/{kind}/{}", audience.id))
}
fn resource_grant(resource: &str, prefix: bool, action: &str) -> Grant {
    Grant {
        kind: GrantKind::Resource(ResourceGrant {
            resource: ResourcePattern {
                name: resource.into(),
                prefix,
            },
            actions: vec![action.into()],
        }),
    }
}
fn grant(resource: &str, prefix: bool) -> GrantSet {
    GrantSet {
        grants: vec![resource_grant(resource, prefix, ACTION)],
    }
}
/// Execution on `audience` plus one exact grant per capability.
fn scope_grants(audience: &str, capabilities: &Capabilities) -> GrantSet {
    let mut grants = grant(audience, false);
    grants
        .grants
        .extend(capabilities.0.iter().map(|capability| {
            resource_grant(
                &format!(
                    "{CAPABILITIES}{}",
                    acyclic_auth::encode_component(capability.as_bytes())
                ),
                false,
                HOLD,
            )
        }));
    grants
}
/// The typed facts of one block of `scope` for `audience`. A block names
/// its agent only when it differs from `holder`, the agent the chain names
/// so far; an acting agent is never dropped.
fn facts(scope: &Scope, audience: &str, holder: Option<AgentId>) -> Result<Vec<Caveat>> {
    let mut caveats = vec![
        Caveat {
            kind: CaveatKind::Label(format!("{SCOPE}{}", scope.id)),
        },
        Caveat {
            kind: CaveatKind::Grants(scope_grants(audience, &scope.capabilities)),
        },
    ];
    if scope.agent == holder {
        return Ok(caveats);
    }
    let Some(agent) = scope.agent else {
        return Err(unauthorized("a delegate cannot drop its acting agent"));
    };
    caveats.push(Caveat {
        kind: CaveatKind::LineageAppend(LineageAppend {
            ids: vec![format!(
                "{AGENT}{}",
                uuid::Uuid::from_bytes(agent.into_bytes()).hyphenated()
            )],
        }),
    });
    Ok(caveats)
}
fn retain(
    scope: &mut Scope,
    capability: &VerifiedCapability,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    *scope.proof = *capability
        .block_ids()
        .last()
        .ok_or_else(|| unauthorized("missing tip"))?;
    let length = capability.encoded_len() as u64;
    if let Some(io) = work.as_deref_mut() {
        io.admit_allocation(length)?;
        io.admit_owned(length)?;
        io.retain_owned(length)?;
    }
    let mut encoded = capability.encode();
    scope.workload = std::mem::take(&mut *encoded);
    Ok(())
}
pub(super) fn issue(
    issuer: &AuthorityRoot,
    scope: &mut Scope,
    parent: Option<&VerifiedCapability>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if let Some(io) = work.as_deref() {
        io.check_cancellation()?;
    }
    let timestamp = issuer.runtime.public.now();
    let name = resource(&issuer.audience)?;
    let signer = {
        let mut keys = issuer.runtime.keys()?;
        match parent {
            Some(parent) => std::sync::Arc::new(keys.certify(
                timestamp,
                scope_grants(&name, &scope.capabilities),
                Some(parent),
            )?),
            None => keys.root_signer(timestamp)?,
        }
    };
    let mut caveats = facts(scope, &name, None)?;
    caveats.push(Caveat {
        kind: CaveatKind::Audience(ResourcePattern {
            name,
            prefix: false,
        }),
    });
    let capability = signer
        .mint(
            caveats,
            SigningKey::from_bytes(&entropy()),
            &*issuer.runtime.trust,
            timestamp,
        )
        .map_err(unauthorized)?;
    retain(scope, &capability, work)
}
/// A sealed credential for exactly `grants`, each `(resource, prefix,
/// actions)`, under `issuer`'s audience at `timestamp`. Its certificate is
/// bound to `parent`: it expires no later and is revoked with it.
pub(super) fn resource_credential(
    issuer: &AuthorityRoot,
    parent: &VerifiedCapability,
    grants: &[(&str, bool, &[&str])],
    timestamp: u64,
) -> Result<(Vec<u8>, String)> {
    let grants = GrantSet {
        grants: grants
            .iter()
            .map(|(name, prefix, actions)| Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: (*name).into(),
                        prefix: *prefix,
                    },
                    actions: actions.iter().map(|action| (*action).into()).collect(),
                }),
            })
            .collect(),
    };
    let signer = issuer
        .runtime
        .keys()?
        .certify(timestamp, grants.clone(), Some(parent))?;
    let capability = signer
        .mint(
            vec![
                Caveat {
                    kind: CaveatKind::Grants(grants),
                },
                Caveat {
                    kind: CaveatKind::Audience(ResourcePattern {
                        name: resource(&issuer.audience)?,
                        prefix: false,
                    }),
                },
            ],
            SigningKey::from_bytes(&entropy()),
            &*issuer.runtime.trust,
            timestamp,
        )
        .and_then(VerifiedCapability::seal)
        .map_err(unauthorized)?;
    let principal = capability.effective().principal().id.clone();
    Ok((capability.encode().to_vec(), principal))
}
/// Appends `scope`'s block for `audience`, signed by the current holder;
/// `holder` is the agent the chain names so far.
pub(super) fn append(
    capability: &VerifiedCapability,
    scope: &mut Scope,
    audience: &Authority,
    holder: Option<AgentId>,
    expires: Option<u64>,
) -> Result<()> {
    let mut caveats = facts(scope, &resource(audience)?, holder)?;
    if let Some(expires) = expires {
        caveats.push(Caveat {
            kind: CaveatKind::ExpiresAtUnixSeconds(expires),
        });
    }
    let narrowed = capability
        .attenuate(caveats, &entropy())
        .map_err(unauthorized)?;
    retain(scope, &narrowed, &mut None)
}

fn usage() -> Limits {
    Limits {
        rps: None,
        burst: None,
        bytes_per_s: None,
        channels: None,
        concurrency: None,
        bytes: None,
        spend_micros: None,
        tokens: None,
    }
}
pub(super) fn verify(
    verifier: &AuthorityVerifier,
    audience: &Authority,
    scope: &Scope,
    timestamp: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<std::sync::Arc<VerifiedCapability>> {
    if scope.issuer != verifier.id {
        return Err(unauthorized("foreign issuer"));
    }
    verifier.admission.check(scope)?;
    let capability = {
        let trust: &dyn verify::Trust = &*verifier.public.trust;
        let authenticated = authenticate(&verifier.public, trust, scope, timestamp, work)?;
        authorize_scope(trust, audience, scope, timestamp, &authenticated, work)?
    };
    // A foreign owner is never invoked while holding the public trust lock.
    verifier.admission.check(scope)?;
    Ok(capability)
}
pub(super) fn verify_trusted(
    trust: &dyn verify::Trust,
    audience: &Authority,
    scope: &Scope,
    timestamp: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<std::sync::Arc<VerifiedCapability>> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
    }
    let authenticated =
        authenticated(verify::verify(&scope.workload, trust, timestamp).map_err(unauthorized)?)?;
    authorize_scope(trust, audience, scope, timestamp, &authenticated, work)
}

fn authorize_scope(
    trust: &dyn verify::Trust,
    audience: &Authority,
    scope: &Scope,
    timestamp: u64,
    authenticated: &AuthenticatedWorkload,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<std::sync::Arc<VerifiedCapability>> {
    if let Some(io) = work.as_deref() {
        io.check_cancellation()?;
    }
    let capability = &authenticated.capability;
    if capability.account_id().map_err(unauthorized)? != scope.issuer {
        return Err(unauthorized("foreign account"));
    }
    let name = resource(audience)?;
    let last = &authenticated.last;
    if last.id != scope.id
        || last.capabilities != scope.capabilities
        || last.agent != scope.agent
        || last.parent != scope.parent_proof
        || capability.block_ids().last() != Some(scope.proof.as_ref())
    {
        return Err(unauthorized("scope does not match its authenticated tip"));
    }
    capability
        .authorize(
            trust,
            &Request {
                target: Target::Resource {
                    action: ACTION,
                    resource: &name,
                },
                audience: &name,
                holder: Some(&scope.issuer),
                now_unix_seconds: timestamp,
                keys: &[],
                attributes: &RecordAttributes::default(),
                usage: &usage(),
                cpu_millis: 0,
                memory_bytes: 0,
            },
            PolicyDecision::Allow,
            acyclic_auth::authority::Scope::Exact,
        )
        .map_err(unauthorized)?;
    Ok(capability.clone())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[derive(Debug, PartialEq)]
struct CertificateSignature;

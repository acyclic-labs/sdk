//! Product scope constraints carried in append-only Ed25519 workload chains.
use super::*;
use acyclic_auth::{
    authority::{PolicyDecision, Request, Target},
    contract::*,
    trust::Snapshot,
    verify::{self, CertifiedSigner, VerifiedCapability},
};
use ed25519_dalek::{Signer, SigningKey};
use prost::Message;
mod admission;
mod retained;
pub(crate) use admission::AuthenticatedRecord;
pub(super) use admission::HistoricalOwner;
pub(super) use admission::{live_record, record, verify_record};
use retained::RetainedAuthentication;

pub(super) struct AuthenticatedWorkload {
    capability: std::sync::Arc<VerifiedCapability>,
    attributes: RecordAttributes,
    last: Claims,
}

#[derive(Default)]
pub(super) struct WorkloadOwner {
    latest: Option<RetainedAuthentication<AuthenticatedWorkload>>,
    certificates: verify::CertificateVerifier,
}

fn authenticated(capability: VerifiedCapability) -> Result<std::sync::Arc<AuthenticatedWorkload>> {
    let (attributes, last) = admission::scope_facts(&capability, false)?;
    Ok(std::sync::Arc::new(AuthenticatedWorkload {
        capability: std::sync::Arc::new(capability),
        attributes,
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

const CLAIM: &str = "harness.scope.";
const ACTION: &str = "harness/execute";
const CERTIFICATE_SECONDS: u64 = 30 * 24 * 60 * 60;

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Claims {
    id: String,
    capabilities: Capabilities,
    agent: Option<AgentId>,
    issuer: String,
    parent: Option<[u8; 32]>,
}
impl Claims {
    fn from_scope(scope: &Scope) -> Self {
        Self {
            id: scope.id.clone(),
            capabilities: scope.capabilities.clone(),
            agent: scope.agent,
            issuer: scope.issuer.clone(),
            parent: scope.parent_proof,
        }
    }
}
pub(super) fn unauthorized(error: impl std::fmt::Display) -> Error {
    Error::Unauthorized(format!("workload capability rejected: {error}"))
}
pub(super) fn now() -> u64 {
    acyclic_stream::UnixMillisClock::now_unix_millis(&acyclic_stream::SystemUnixMillisClock) / 1000
}
// Prepared keys stay inside runtime custody. Issuance borrows this owner;
// neither a verifier nor an admitted workload receives its root keys.
pub(super) struct RuntimeKeys {
    environment: SigningKey,
    account: SigningKey,
    runtime: SigningKey,
    receipt: std::sync::Mutex<Option<ReceiptSigner>>,
}

struct ReceiptSigner {
    account: String,
    born: u64,
    expires: u64,
    signer: std::sync::Arc<CertifiedSigner>,
}

impl RuntimeKeys {
    pub(super) fn new(seed: &[u8; 32]) -> Self {
        let key = |domain| runtime_signing::prepare_key(&blake3::derive_key(domain, seed));
        Self {
            environment: key("acyclic.harness.environment.v1"),
            account: key("acyclic.harness.account.v1"),
            runtime: key("acyclic.harness.runtime.v1"),
            receipt: Default::default(),
        }
    }

    // Runtime time attestations share one restricted certificate. Each receipt
    // binds its exact audience/payload/time; workloads never receive this signer.
    fn receipt_signer(&self, id: &str, timestamp: u64) -> Result<std::sync::Arc<CertifiedSigner>> {
        let mut retained = self.receipt.lock().map_err(unauthorized)?;
        if let Some(receipt) = retained.as_ref()
            && receipt.account == id
            && receipt.born <= timestamp
            && timestamp < receipt.expires
        {
            return Ok(receipt.signer.clone());
        }
        let expires = timestamp
            .checked_add(CERTIFICATE_SECONDS)
            .ok_or_else(|| unauthorized("clock overflow"))?;
        let signer = std::sync::Arc::new(signer(id, self, timestamp, None, None)?);
        *retained = Some(ReceiptSigner {
            account: id.into(),
            born: timestamp,
            expires,
            signer: signer.clone(),
        });
        Ok(signer)
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
pub(super) fn trust(id: &str, keys: &RuntimeKeys) -> Snapshot {
    Snapshot {
        environment: "local-harness".into(),
        environment_keys: vec![keys.environment.verifying_key().to_bytes()],
        account: id.into(),
        issuer_keys: vec![keys.account.verifying_key().to_bytes()],
        minimum_key_epoch: 1,
        revoked_key_ids: Default::default(),
        revoked_block_ids: Default::default(),
    }
}
fn grant(resource: &str) -> GrantSet {
    GrantSet {
        grants: vec![Grant {
            kind: GrantKind::Resource(ResourceGrant {
                resource: ResourcePattern {
                    name: resource.into(),
                    prefix: false,
                },
                actions: vec![ACTION.into()],
            }),
        }],
    }
}
fn signature(domain: &[u8], claims: &impl Message, signer: &SigningKey) -> Vec<u8> {
    #[cfg(test)]
    acyclic_test_support::differential::record(CertificateSignature);
    let mut message = domain.to_vec();
    message.extend_from_slice(&claims.encode_to_vec());
    signer.sign(&message).to_bytes().to_vec()
}
fn signer(
    id: &str,
    keys: &RuntimeKeys,
    timestamp: u64,
    name: Option<&str>,
    parent: Option<&VerifiedCapability>,
) -> Result<CertifiedSigner> {
    signer_with_grants(
        id,
        keys,
        timestamp,
        name.map_or(GrantSet { grants: Vec::new() }, grant),
        parent,
    )
}

pub(super) fn signer_with_grants(
    id: &str,
    keys: &RuntimeKeys,
    timestamp: u64,
    grants: GrantSet,
    parent: Option<&VerifiedCapability>,
) -> Result<CertifiedSigner> {
    let environment = &keys.environment;
    let account = &keys.account;
    let leaf = &keys.runtime;
    let birth = Birth {
        account_id: id.into(),
        environment: "local-harness".into(),
        genesis_public_key: account.verifying_key().to_bytes().to_vec(),
        issued_at_unix_seconds: timestamp,
    };
    let certified = CertifiedKey {
        account_id: id.into(),
        key_id: "runtime".into(),
        public_key: leaf.verifying_key().to_bytes().to_vec(),
        key_epoch: 1,
        validity: Validity {
            not_before_unix_seconds: timestamp,
            expires_at_unix_seconds: timestamp
                .checked_add(CERTIFICATE_SECONDS)
                .ok_or_else(|| unauthorized("clock overflow"))?
                .min(parent.map_or(u64::MAX, |parent| {
                    parent.effective().expires_at_unix_seconds()
                })),
        },
        grants: {
            let mut grants = grants;
            grants.grants.push(Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: "harness:/admissions/".into(),
                        prefix: true,
                    },
                    actions: vec![acyclic_auth::admission::RECEIPT_ACTION.into()],
                }),
            });
            grants
        },
        principal: Principal {
            kind: PrincipalKind::Service,
            id: id.into(),
            lineage: vec![],
            groups: vec![],
            roles: vec![],
            attributes: vec![],
        },
        parent_key_id: None,
        parent_revocation_ids: parent
            .into_iter()
            .flat_map(VerifiedCapability::revocation_ids)
            .map(<[u8]>::to_vec)
            .collect(),
    };
    let certificates = CertificateChain {
        birth: BirthCertificate {
            signature: signature(
                acyclic_auth::BIRTH_DOMAIN,
                &BirthProto::from(birth.clone()),
                environment,
            ),
            birth,
        },
        keys: vec![KeyCertificate {
            signature: signature(
                acyclic_auth::KEY_DOMAIN,
                &CertifiedKeyProto::from(certified.clone()),
                account,
            ),
            key: certified,
        }],
    };
    CertifiedSigner::new(certificates, leaf.clone()).map_err(unauthorized)
}
fn claims(scope: &Scope, index: usize) -> Result<Caveat> {
    Ok(Caveat {
        kind: CaveatKind::Pin(Pin {
            name: format!("{CLAIM}{index}"),
            value: PinValue::Literal(
                serde_json::to_vec(&Claims::from_scope(scope)).map_err(unauthorized)?,
            ),
        }),
    })
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
    let timestamp = now();
    let name = resource(&issuer.audience)?;
    let capability = signer(
        &issuer.id,
        &issuer.runtime.workload,
        timestamp,
        Some(&name),
        parent,
    )?
    .mint(
        vec![
            claims(scope, 0)?,
            Caveat {
                kind: CaveatKind::Grants(grant(&name)),
            },
            Caveat {
                kind: CaveatKind::Audience(ResourcePattern {
                    name,
                    prefix: false,
                }),
            },
        ],
        SigningKey::from_bytes(&entropy()),
        &issuer.public_workload_trust()?,
        timestamp,
    )
    .map_err(unauthorized)?;
    retain(scope, &capability, work)
}
/// A sealed credential for `actions` on the `fs:/` `prefix`, under
/// `issuer`'s audience. Its certificate is bound to `parent`: it expires no
/// later and is revoked with it.
pub(super) fn resource_credential(
    issuer: &AuthorityRoot,
    parent: &VerifiedCapability,
    prefix: &str,
    actions: &[&str],
) -> Result<Vec<u8>> {
    let timestamp = now();
    let grants = GrantSet {
        grants: vec![Grant {
            kind: GrantKind::Resource(ResourceGrant {
                resource: ResourcePattern {
                    name: prefix.into(),
                    prefix: true,
                },
                actions: actions.iter().map(|action| (*action).into()).collect(),
            }),
        }],
    };
    let capability = signer_with_grants(
        &issuer.id,
        &issuer.runtime.workload,
        timestamp,
        grants.clone(),
        Some(parent),
    )?
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
        &issuer.public_workload_trust()?,
        timestamp,
    )
    .and_then(VerifiedCapability::seal)
    .map_err(unauthorized)?;
    Ok(capability.encode().to_vec())
}
pub(super) fn append(
    capability: &VerifiedCapability,
    scope: &mut Scope,
    expires: Option<u64>,
) -> Result<()> {
    let index = capability.block_ids().len();
    let mut caveats = vec![claims(scope, index)?];
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
    let trust = verifier.public.trust.read().map_err(unauthorized)?;
    let authenticated = authenticate(&verifier.public, &*trust, scope, timestamp, work)?;
    authorize_scope(&*trust, audience, scope, timestamp, &authenticated, work)
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
        || last.issuer != scope.issuer
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
                attributes: &authenticated.attributes,
                usage: &usage(),
                cpu_millis: 0,
                memory_bytes: 0,
            },
            PolicyDecision::Allow,
        )
        .map_err(unauthorized)?;
    Ok(capability.clone())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[derive(Debug, PartialEq)]
struct CertificateSignature;

#[cfg(test)]
mod receipt_tests;

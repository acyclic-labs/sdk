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
fn unauthorized(error: impl std::fmt::Display) -> Error {
    Error::Unauthorized(format!("workload capability rejected: {error}"))
}
pub(super) fn now() -> u64 {
    acyclic_stream::UnixMillisClock::now_unix_millis(&acyclic_stream::SystemUnixMillisClock) / 1000
}
fn key(seed: &[u8; 32], domain: &str) -> SigningKey {
    SigningKey::from_bytes(&blake3::derive_key(domain, seed))
}
fn entropy() -> [u8; 32] {
    let mut bytes = [0; 32];
    bytes[..16].copy_from_slice(crate::identity::fresh_uuid().as_bytes());
    bytes[16..].copy_from_slice(crate::identity::fresh_uuid().as_bytes());
    bytes
}
fn resource(audience: &Authority) -> Result<String> {
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
pub(super) fn trust(id: &str, seed: &[u8; 32]) -> Snapshot {
    Snapshot {
        environment: "local-harness".into(),
        environment_keys: vec![
            key(seed, "acyclic.harness.environment.v1")
                .verifying_key()
                .to_bytes(),
        ],
        account: id.into(),
        issuer_keys: vec![
            key(seed, "acyclic.harness.account.v1")
                .verifying_key()
                .to_bytes(),
        ],
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
    let mut message = domain.to_vec();
    message.extend_from_slice(&claims.encode_to_vec());
    signer.sign(&message).to_bytes().to_vec()
}
fn signer(issuer: &AuthorityIssuer, timestamp: u64, name: &str) -> Result<CertifiedSigner> {
    let environment = key(&issuer.key, "acyclic.harness.environment.v1");
    let account = key(&issuer.key, "acyclic.harness.account.v1");
    let leaf = key(&issuer.key, "acyclic.harness.runtime.v1");
    let birth = Birth {
        account_id: issuer.id.clone(),
        environment: "local-harness".into(),
        genesis_public_key: account.verifying_key().to_bytes().to_vec(),
        issued_at_unix_seconds: timestamp,
    };
    let certified = CertifiedKey {
        account_id: issuer.id.clone(),
        key_id: "runtime".into(),
        public_key: leaf.verifying_key().to_bytes().to_vec(),
        key_epoch: 1,
        validity: Validity {
            not_before_unix_seconds: timestamp,
            expires_at_unix_seconds: timestamp
                .checked_add(CERTIFICATE_SECONDS)
                .ok_or_else(|| unauthorized("clock overflow"))?,
        },
        grants: grant(name),
        principal: Principal {
            kind: PrincipalKind::Service,
            id: issuer.id.clone(),
            lineage: vec![],
            groups: vec![],
            roles: vec![],
            attributes: vec![],
        },
        parent_key_id: None,
        parent_revocation_ids: vec![],
    };
    let certificates = CertificateChain {
        birth: BirthCertificate {
            signature: signature(
                acyclic_auth::BIRTH_DOMAIN,
                &BirthProto::from(birth.clone()),
                &environment,
            ),
            birth,
        },
        keys: vec![KeyCertificate {
            signature: signature(
                acyclic_auth::KEY_DOMAIN,
                &CertifiedKeyProto::from(certified.clone()),
                &account,
            ),
            key: certified,
        }],
    };
    CertifiedSigner::new(certificates, leaf).map_err(unauthorized)
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
    issuer: &AuthorityIssuer,
    scope: &mut Scope,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if let Some(io) = work.as_deref() {
        io.check_cancellation()?;
    }
    let timestamp = now();
    let name = resource(&issuer.audience)?;
    let capability = signer(issuer, timestamp, &name)?
        .mint(
            vec![
                claims(scope, 0)?,
                Caveat {
                    kind: CaveatKind::Audience(ResourcePattern {
                        name,
                        prefix: false,
                    }),
                },
            ],
            SigningKey::from_bytes(&entropy()),
            &trust(&issuer.id, &issuer.key),
            timestamp,
        )
        .map_err(unauthorized)?;
    retain(scope, &capability, work)
}
pub(super) fn append(capability: &VerifiedCapability, scope: &mut Scope) -> Result<()> {
    let index = capability.block_ids().len();
    let narrowed = capability
        .attenuate(vec![claims(scope, index)?], &entropy())
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
) -> Result<VerifiedCapability> {
    if scope.issuer != verifier.id {
        return Err(unauthorized("foreign issuer"));
    }
    verify_trusted(
        &trust(&verifier.id, &verifier.key),
        audience,
        scope,
        timestamp,
        work,
    )
}
pub(super) fn verify_trusted(
    trust: &dyn verify::Trust,
    audience: &Authority,
    scope: &Scope,
    timestamp: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<VerifiedCapability> {
    if let Some(io) = work.as_deref() {
        io.check_cancellation()?;
    }
    let capability = verify::verify(&scope.workload, trust, timestamp).map_err(unauthorized)?;
    if capability.account_id().map_err(unauthorized)? != scope.issuer {
        return Err(unauthorized("foreign account"));
    }
    let name = resource(audience)?;
    let mut attributes = Vec::new();
    let mut previous: Option<Claims> = None;
    for (index, scoped) in capability.effective().pins().enumerate() {
        let pin = scoped.pin();
        if scoped.declaring_block() != index || pin.name != format!("{CLAIM}{index}") {
            return Err(unauthorized("invalid scope ancestry"));
        }
        let PinValue::Literal(value) = &pin.value else {
            return Err(unauthorized("nonliteral scope"));
        };
        let current: Claims = serde_json::from_slice(value).map_err(unauthorized)?;
        if current.issuer != scope.issuer
            || previous.as_ref().is_some_and(|parent| {
                current.agent != parent.agent
                    || !current.capabilities.is_subset_of(&parent.capabilities)
                    || current.parent != capability.block_ids().get(index - 1).copied()
            })
        {
            return Err(unauthorized("scope widens its ancestor"));
        }
        previous = Some(current);
        attributes.push(Attribute {
            name: pin.name.clone(),
            value: value.clone(),
        });
    }
    if attributes.len() != capability.block_ids().len()
        || previous.as_ref() != Some(&Claims::from_scope(scope))
        || capability.block_ids().last() != Some(scope.proof.as_ref())
    {
        return Err(unauthorized("scope does not match its authenticated tip"));
    }
    let attributes = RecordAttributes::try_new(attributes).map_err(unauthorized)?;
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
                attributes: &attributes,
                usage: &usage(),
                cpu_millis: 0,
                memory_bytes: 0,
            },
            PolicyDecision::Allow,
        )
        .map_err(unauthorized)?;
    Ok(capability)
}

#[cfg(test)]
mod tests;

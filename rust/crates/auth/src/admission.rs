//! Runtime receipts separate historical evidence from live workload authority.
use crate::{
    authority::{PolicyDecision, Request, Target},
    contract::*,
    verify::{self, CertifiedSigner, Trust, VerifiedCapability, VerifyError},
};
use ed25519_dalek::SigningKey;
use sha2::{Digest, Sha256};
use std::fmt::Write;

const PAYLOAD: &str = "acyclic.admission.payload";
const TIME: &str = "acyclic.admission.time";
const AUDIENCE: &str = "acyclic.admission.audience";
const TARGET: &str = "acyclic.admission.target";
const RESERVED: [&str; 4] = [PAYLOAD, TIME, AUDIENCE, TARGET];

/// Semantic runtime permission required by the receipt signing certificate.
pub const RECEIPT_ACTION: &str = "harness/admit";

/// One trusted clock observation at the runtime's admission boundary. Only the
/// runtime custodian can supply the certified signer; workloads get its tips.
pub struct RuntimeAdmission<'a> {
    signer: &'a CertifiedSigner,
    trust: &'a dyn Trust,
    now: u64,
}
impl<'a> RuntimeAdmission<'a> {
    /// Borrows the runtime's custodied signer and authenticated current state.
    pub fn new(signer: &'a CertifiedSigner, trust: &'a dyn Trust, now: u64) -> Self {
        Self { signer, trust, now }
    }

    /// Rechecks live authority, binds one payload, discards both append tips,
    /// and authenticates the admission time independently of the workload.
    /// `entropy` must be fresh runtime-supplied cryptographic randomness.
    pub fn record(
        &self,
        workload: &VerifiedCapability,
        request: &Request<'_>,
        payload: [u8; 32],
        entropy: &[u8; 32],
    ) -> Result<AdmittedCapability, VerifyError> {
        if request.now_unix_seconds != self.now {
            return Err(VerifyError::Time);
        }
        workload.authorize(self.trust, request, PolicyDecision::Allow)?;
        let attributes = facts(request, payload, self.now)?;
        let Target::Resource { action, resource } = request.target else {
            return Err(VerifyError::Malformed);
        };
        let mut caveats = pins(&attributes);
        caveats.push(Caveat {
            kind: CaveatKind::Grants(grant(resource, action)),
        });
        caveats.push(Caveat {
            kind: CaveatKind::Audience(ResourcePattern {
                name: request.audience.into(),
                prefix: false,
            }),
        });
        let token = workload
            .attenuate(caveats, &seed(b"workload", entropy))?
            .seal()?;
        // Check the new restriction too: an ancestor pin cannot be bypassed by
        // silently signing a contradictory payload binding.
        token.authorize(
            self.trust,
            &Request {
                attributes: &attributes,
                ..*request
            },
            PolicyDecision::Allow,
        )?;
        let encoded = token.encode().to_vec();
        let name = receipt_resource(&encoded);
        let receipt_facts = receipt_facts(request, payload, self.now)?;
        let mut caveats = pins(&receipt_facts);
        caveats.push(Caveat {
            kind: CaveatKind::Grants(grant(&name, RECEIPT_ACTION)),
        });
        caveats.push(Caveat {
            kind: CaveatKind::Audience(ResourcePattern {
                name: request.audience.into(),
                prefix: false,
            }),
        });
        caveats.push(Caveat {
            kind: CaveatKind::NotBeforeUnixSeconds(self.now),
        });
        caveats.push(Caveat {
            kind: CaveatKind::ExpiresAtUnixSeconds(
                self.now.checked_add(1).ok_or(VerifyError::Time)?,
            ),
        });
        let receipt = self
            .signer
            .mint(
                caveats,
                SigningKey::from_bytes(&seed(b"receipt", entropy)),
                self.trust,
                self.now,
            )?
            .seal()?;
        if receipt.account_id()? != token.account_id()? {
            return Err(VerifyError::Trust);
        }
        receipt.authorize(
            self.trust,
            &receipt_request(request, &name, &receipt_facts, self.now),
            PolicyDecision::Allow,
        )?;
        Ok(AdmittedCapability {
            workload: encoded,
            receipt: receipt.encode().to_vec(),
            admitted_at_unix_seconds: self.now,
        })
    }
}

/// Immutable authentication evidence for one exact historical admission.
/// Inspecting its workload does not authorize a request. Every use must still
/// check the current trust snapshot and the exact payload through `authorize`.
pub struct AuthenticatedAdmission {
    workload: VerifiedCapability,
    receipt: VerifiedCapability,
    admitted: u64,
    resource: String,
}

impl AuthenticatedAdmission {
    /// Borrow the authenticated claims when deriving the product's request.
    #[must_use]
    pub fn workload(&self) -> &VerifiedCapability {
        &self.workload
    }

    /// Intersect the exact historical request with current public trust.
    ///
    /// # Errors
    /// Rejects changed bindings, future admission times and revoked trust.
    pub fn authorize_history(
        self,
        trust: &dyn Trust,
        request: &Request<'_>,
        payload: [u8; 32],
    ) -> Result<VerifiedCapability, VerifyError> {
        self.check_request(trust, request, payload)?;
        Ok(self.workload)
    }

    /// Reauthorize this immutable proof without consuming its owner's evidence.
    /// No decision is retained: current trust and every request binding are checked.
    ///
    /// # Errors
    /// Rejects changed bindings, future admission times and revoked trust.
    pub fn check_request(
        &self,
        trust: &dyn Trust,
        request: &Request<'_>,
        payload: [u8; 32],
    ) -> Result<(), VerifyError> {
        if self.admitted > request.now_unix_seconds {
            return Err(VerifyError::Time);
        }
        let receipt_facts = receipt_facts(request, payload, self.admitted)?;
        self.receipt.authorize(
            trust,
            &receipt_request(request, &self.resource, &receipt_facts, self.admitted),
            PolicyDecision::Allow,
        )?;
        let attributes = facts(request, payload, self.admitted)?;
        self.workload.authorize(
            trust,
            &Request {
                now_unix_seconds: self.admitted,
                attributes: &attributes,
                ..*request
            },
            PolicyDecision::Allow,
        )?;
        Ok(())
    }
}

/// Runtime-owned authentication of historical proofs. Only the latest workload
/// and receipt certifications are retained; every block and request is rechecked.
#[derive(Default)]
pub struct HistoryVerifier {
    workload: verify::CertificateVerifier,
    receipt: verify::CertificateVerifier,
}

impl HistoryVerifier {
    /// Authenticate exact admission bytes using this owner's certified roots.
    ///
    /// # Errors
    /// Rejects malformed, forged, revoked, wrong-trust or future evidence.
    pub fn authenticate(
        &mut self,
        admitted: &AdmittedCapability,
        trust: &dyn Trust,
        now_unix_seconds: u64,
    ) -> Result<AuthenticatedAdmission, VerifyError> {
        let timestamp = admitted.admitted_at_unix_seconds;
        if timestamp > now_unix_seconds {
            return Err(VerifyError::Time);
        }
        let receipt = self.receipt.verify(&admitted.receipt, trust, timestamp)?;
        // Only a newly runtime-signed block zero can attest the clock.
        if !receipt.is_sealed() || receipt.block_ids().len() != 1 {
            return Err(VerifyError::Signature);
        }
        let workload = self.workload.verify(&admitted.workload, trust, timestamp)?;
        if !workload.is_sealed() || receipt.account_id()? != workload.account_id()? {
            return Err(VerifyError::Signature);
        }
        Ok(AuthenticatedAdmission {
            workload,
            receipt,
            admitted: timestamp,
            resource: receipt_resource(&admitted.workload),
        })
    }
}

impl AdmittedCapability {
    /// Authenticate the immutable workload and receipt once. The result carries
    /// no saved policy decision; callers must authorize each derived request.
    ///
    /// # Errors
    /// Rejects malformed, forged, revoked or future-dated evidence.
    pub fn authenticate_history(
        &self,
        trust: &dyn Trust,
        now_unix_seconds: u64,
    ) -> Result<AuthenticatedAdmission, VerifyError> {
        HistoryVerifier::default().authenticate(self, trust, now_unix_seconds)
    }

    /// Verifies one historical payload under current public trust and revocations.
    /// The runtime receipt authenticates the old clock value; fresh admissions
    /// still check expiry against the trusted current clock. The owning product
    /// derives the semantic action, resource and payload digest, never the caller.
    pub fn verify_history(
        &self,
        trust: &dyn Trust,
        request: &Request<'_>,
        payload: [u8; 32],
    ) -> Result<VerifiedCapability, VerifyError> {
        let authenticated = self.authenticate_history(trust, request.now_unix_seconds)?;
        authenticated.authorize_history(trust, request, payload)
    }
}

fn grant(name: &str, action: &str) -> GrantSet {
    GrantSet {
        grants: vec![Grant {
            kind: GrantKind::Resource(ResourceGrant {
                resource: ResourcePattern {
                    name: name.into(),
                    prefix: false,
                },
                actions: vec![action.into()],
            }),
        }],
    }
}
fn seed(domain: &[u8], entropy: &[u8; 32]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"acyclic.auth.v1/admission-tip\0");
    hash.update(domain);
    hash.update(entropy);
    hash.finalize().into()
}
fn receipt_resource(bytes: &[u8]) -> String {
    let mut name = String::from("harness:/admissions/");
    for byte in Sha256::digest(bytes) {
        let _ = write!(name, "{byte:02x}");
    }
    name
}
fn receipt_facts(
    request: &Request<'_>,
    payload: [u8; 32],
    admitted: u64,
) -> Result<RecordAttributes, VerifyError> {
    let Target::Resource { action, resource } = request.target else {
        return Err(VerifyError::Malformed);
    };
    let mut target = Sha256::new();
    for value in [action, resource] {
        target.update((value.len() as u64).to_le_bytes());
        target.update(value.as_bytes());
    }
    RecordAttributes::try_new(vec![
        Attribute {
            name: PAYLOAD.into(),
            value: payload.to_vec(),
        },
        Attribute {
            name: TIME.into(),
            value: admitted.to_le_bytes().to_vec(),
        },
        Attribute {
            name: AUDIENCE.into(),
            value: request.audience.as_bytes().to_vec(),
        },
        Attribute {
            name: TARGET.into(),
            value: target.finalize().to_vec(),
        },
    ])
    .map_err(VerifyError::from)
}
fn facts(
    request: &Request<'_>,
    payload: [u8; 32],
    admitted: u64,
) -> Result<RecordAttributes, VerifyError> {
    if request
        .attributes
        .as_slice()
        .iter()
        .any(|attribute| RESERVED.contains(&attribute.name.as_str()))
    {
        return Err(VerifyError::Malformed);
    }
    let mut attributes = request.attributes.as_slice().to_vec();
    attributes.extend_from_slice(receipt_facts(request, payload, admitted)?.as_slice());
    RecordAttributes::try_new(attributes).map_err(VerifyError::from)
}
fn pins(attributes: &RecordAttributes) -> Vec<Caveat> {
    attributes
        .as_slice()
        .iter()
        .filter(|attribute| RESERVED.contains(&attribute.name.as_str()))
        .map(|attribute| Caveat {
            kind: CaveatKind::Pin(Pin {
                name: attribute.name.clone(),
                value: PinValue::Literal(attribute.value.clone()),
            }),
        })
        .collect()
}
fn receipt_request<'a>(
    request: &'a Request<'a>,
    name: &'a str,
    attributes: &'a RecordAttributes,
    admitted: u64,
) -> Request<'a> {
    Request {
        target: Target::Resource {
            action: RECEIPT_ACTION,
            resource: name,
        },
        attributes,
        keys: &[],
        now_unix_seconds: admitted,
        ..*request
    }
}

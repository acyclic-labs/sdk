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

/// Authenticated immutable receipt and workload belonging to one exact record.
/// Authorization still checks current trust, time and the product's bindings.
pub struct AuthenticatedAdmission<'a> {
    record: &'a AdmittedCapability,
    receipt: VerifiedCapability,
    workload: VerifiedCapability,
}

impl AdmittedCapability {
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
        self.authenticate_history(trust, request.now_unix_seconds)?
            .authorize_history(trust, request, payload)
    }

    /// Authenticates both sealed chains once, borrowing the immutable record.
    /// The product may derive scope facts from the workload before consuming
    /// this value to authorize the historical payload under fresh public trust.
    pub fn authenticate_history(
        &self,
        trust: &dyn Trust,
        now_unix_seconds: u64,
    ) -> Result<AuthenticatedAdmission<'_>, VerifyError> {
        let admitted = self.admitted_at_unix_seconds;
        if admitted > now_unix_seconds {
            return Err(VerifyError::Time);
        }
        let receipt = verify::verify(&self.receipt, trust, admitted)?;
        // A holder can append a false time to its tip. Only a newly runtime-
        // signed block zero can authenticate an admission timestamp.
        if !receipt.is_sealed() || receipt.block_ids().len() != 1 {
            return Err(VerifyError::Signature);
        }
        let workload = verify::verify(&self.workload, trust, admitted)?;
        if !workload.is_sealed() || receipt.account_id()? != workload.account_id()? {
            return Err(VerifyError::Signature);
        }
        Ok(AuthenticatedAdmission {
            record: self,
            receipt,
            workload,
        })
    }
}

impl AuthenticatedAdmission<'_> {
    /// The exact authenticated workload, available for immutable scope facts.
    #[must_use]
    pub const fn workload(&self) -> &VerifiedCapability {
        &self.workload
    }

    /// Consumes the authentication and checks both chains against current
    /// public trust and the exact historical payload, audience and action.
    pub fn authorize_history(
        self,
        trust: &dyn Trust,
        request: &Request<'_>,
        payload: [u8; 32],
    ) -> Result<VerifiedCapability, VerifyError> {
        let admitted = self.record.admitted_at_unix_seconds;
        if admitted > request.now_unix_seconds {
            return Err(VerifyError::Time);
        }
        let name = receipt_resource(&self.record.workload);
        let receipt_facts = receipt_facts(request, payload, admitted)?;
        self.receipt.authorize(
            trust,
            &receipt_request(request, &name, &receipt_facts, admitted),
            PolicyDecision::Allow,
        )?;
        let attributes = facts(request, payload, admitted)?;
        self.workload.authorize(
            trust,
            &Request {
                now_unix_seconds: admitted,
                attributes: &attributes,
                ..*request
            },
            PolicyDecision::Allow,
        )?;
        Ok(self.workload)
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

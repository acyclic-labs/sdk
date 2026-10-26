//! Sealed workload descendants plus independently authenticated runtime time.
use super::*;
use acyclic_auth::admission::{AuthenticatedAdmission, HistoryVerifier, RuntimeAdmission};
use std::sync::Arc;

#[derive(Default)]
pub(in crate::core::authority) struct HistoricalOwner {
    record: Option<RetainedAuthentication<AuthenticatedRecord>>,
    verifier: HistoryVerifier,
}

/// One exact historical proof, retained by the public authority, never a decision.
pub(crate) struct AuthenticatedRecord {
    authenticated: AuthenticatedAdmission,
    attributes: RecordAttributes,
    last: Claims,
}

impl std::ops::Deref for AuthenticatedRecord {
    type Target = VerifiedCapability;
    fn deref(&self) -> &Self::Target {
        self.authenticated.workload()
    }
}

fn authenticate_record(
    public: &runtime_signing::PublicAuthority,
    trust: &dyn verify::Trust,
    encoded: &[u8],
    timestamp: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Arc<AuthenticatedRecord>> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
    }
    let mut retained = public.historical.lock().map_err(unauthorized)?;
    if let Some(record) = retained.record.as_ref()
        && let Some(proof) = record.get(encoded, work)?
    {
        return Ok(proof);
    }
    let envelope = AdmittedCapabilityProto::decode(encoded).map_err(unauthorized)?;
    if envelope.encode_to_vec() != encoded {
        return Err(unauthorized("noncanonical admission"));
    }
    let admitted: AdmittedCapability = envelope.try_into().map_err(unauthorized)?;
    let authenticated = retained
        .verifier
        .authenticate(&admitted, trust, timestamp)
        .map_err(unauthorized)?;
    let (attributes, last) = scope_facts(authenticated.workload(), true)?;
    let record = Arc::new(AuthenticatedRecord {
        authenticated,
        attributes,
        last,
    });
    retained.record = RetainedAuthentication::new(encoded, record.clone(), work)?;
    Ok(record)
}

pub(in crate::core::authority) fn record(
    runtime: &runtime_signing::RuntimeSigner,
    id: &str,
    audience: &Authority,
    scope: &Scope,
    payload: [u8; 32],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<u8>> {
    let timestamp = now();
    let trust = runtime.public.trust.read().map_err(unauthorized)?;
    let authenticated = authenticate(&runtime.public, &*trust, scope, timestamp, work)?;
    let capability = authorize_scope(&*trust, audience, scope, timestamp, &authenticated, work)?;
    let name = resource(audience)?;
    let signer = runtime.workload.receipt_signer(id, timestamp)?;
    let receipt = RuntimeAdmission::new(&signer, &*trust, timestamp)
        .record(
            &capability,
            &Request {
                target: Target::Resource {
                    action: ACTION,
                    resource: &name,
                },
                audience: &name,
                holder: Some(id),
                now_unix_seconds: timestamp,
                keys: &[],
                attributes: &authenticated.attributes,
                usage: &usage(),
                cpu_millis: 0,
                memory_bytes: 0,
            },
            payload,
            &entropy(),
        )
        .map_err(unauthorized)?;
    let envelope = AdmittedCapabilityProto::from(receipt);
    let length = envelope.encoded_len() as u64;
    if let Some(io) = work.as_deref_mut() {
        io.admit_allocation(length)?;
        io.admit_owned(length)?;
        io.retain_owned(length)?;
    }
    Ok(envelope.encode_to_vec())
}

pub(in crate::core::authority) fn verify_record(
    verifier: &AuthorityVerifier,
    encoded: &[u8],
    payload: [u8; 32],
    expected: Option<&RecordedScope>,
    required: &[&str],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Arc<AuthenticatedRecord>> {
    let trust = verifier.public.trust.read().map_err(unauthorized)?;
    let record = authenticate_record(&verifier.public, &*trust, encoded, now(), work)?;
    if record.account_id().map_err(unauthorized)? != verifier.id {
        return Err(unauthorized("foreign admission account"));
    }
    let last = &record.last;
    if last.issuer != verifier.id
        || expected.is_some_and(|scope| {
            last.id != scope.id
                || last.agent != scope.agent
                || last.issuer != scope.issuer
                || last.capabilities != scope.capabilities
        })
        || required
            .iter()
            .any(|grant| !last.capabilities.contains(grant))
    {
        return Err(unauthorized(
            "admission scope does not authorize the payload",
        ));
    }
    let name = resource(&verifier.audience)?;
    record
        .authenticated
        .check_request(
            &*trust,
            &Request {
                target: Target::Resource {
                    action: ACTION,
                    resource: &name,
                },
                audience: &name,
                holder: Some(&verifier.id),
                now_unix_seconds: now(),
                keys: &[],
                attributes: &record.attributes,
                usage: &usage(),
                cpu_millis: 0,
                memory_bytes: 0,
            },
            payload,
        )
        .map_err(unauthorized)?;
    Ok(record)
}

pub(in crate::core::authority) fn live_record(
    verifier: &AuthorityVerifier,
    event: &Event,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Arc<AuthenticatedRecord>> {
    let capability = verifier.authenticated_event(event, work)?;
    capability
        .check(&*verifier.public.trust.read().map_err(unauthorized)?, now())
        .map_err(unauthorized)?;
    Ok(capability)
}

pub(super) fn scope_facts(
    capability: &VerifiedCapability,
    historical: bool,
) -> Result<(RecordAttributes, Claims)> {
    let mut attributes = Vec::new();
    let mut previous: Option<Claims> = None;
    for scoped in capability.effective().pins() {
        let pin = scoped.pin();
        if historical && pin.name.starts_with("acyclic.admission.") {
            if scoped.declaring_block() + 1 != capability.block_ids().len() {
                return Err(unauthorized("admission facts precede the final block"));
            }
            continue;
        }
        let index = attributes.len();
        if scoped.declaring_block() != index || pin.name != format!("{CLAIM}{index}") {
            return Err(unauthorized("invalid admission scope ancestry"));
        }
        let PinValue::Literal(value) = &pin.value else {
            return Err(unauthorized("nonliteral admission scope"));
        };
        let current: Claims = serde_json::from_slice(value).map_err(unauthorized)?;
        if previous.as_ref().is_some_and(|parent| {
            current.issuer != parent.issuer
                || current.agent != parent.agent
                || !current.capabilities.is_subset_of(&parent.capabilities)
                || current.parent != capability.block_ids().get(index - 1).copied()
        }) {
            return Err(unauthorized("admission widens its ancestor"));
        }
        previous = Some(current);
        attributes.push(Attribute {
            name: pin.name.clone(),
            value: value.clone(),
        });
    }
    if attributes.len() + usize::from(historical) != capability.block_ids().len() {
        return Err(unauthorized("admission lacks a complete scope chain"));
    }
    Ok((
        RecordAttributes::try_new(attributes).map_err(unauthorized)?,
        previous.ok_or_else(|| unauthorized("admission has no scope"))?,
    ))
}

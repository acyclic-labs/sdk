//! Sealed workload descendants plus independently authenticated runtime time.
use super::*;
use acyclic_auth::admission::RuntimeAdmission;

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
    let capability = verify_trusted(&*trust, audience, scope, timestamp, work)?;
    let (attributes, _) = scope_facts(&capability, false)?;
    let name = resource(audience)?;
    let signer = signer(id, runtime.seed(), timestamp, &name, None)?;
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
                attributes: &attributes,
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
) -> Result<()> {
    if let Some(io) = work.as_deref() {
        io.check_cancellation()?;
    }
    let envelope = AdmittedCapabilityProto::decode(encoded).map_err(unauthorized)?;
    if envelope.encode_to_vec() != encoded {
        return Err(unauthorized("noncanonical admission"));
    }
    let admitted: AdmittedCapability = envelope.try_into().map_err(unauthorized)?;
    let trust = verifier.public.trust.read().map_err(unauthorized)?;
    let capability = verify::verify(
        &admitted.workload,
        &*trust,
        admitted.admitted_at_unix_seconds,
    )
    .map_err(unauthorized)?;
    if capability.account_id().map_err(unauthorized)? != verifier.id {
        return Err(unauthorized("foreign admission account"));
    }
    let (attributes, last) = scope_facts(&capability, true)?;
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
    admitted
        .verify_history(
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
                attributes: &attributes,
                usage: &usage(),
                cpu_millis: 0,
                memory_bytes: 0,
            },
            payload,
        )
        .map_err(unauthorized)?;
    Ok(())
}

pub(in crate::core::authority) fn live_record(
    verifier: &AuthorityVerifier,
    event: &Event,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<VerifiedCapability> {
    verifier.verify_event(event, work)?;
    let envelope =
        AdmittedCapabilityProto::decode(event.attestation.as_slice()).map_err(unauthorized)?;
    let admitted: AdmittedCapability = envelope.try_into().map_err(unauthorized)?;
    verify::verify(
        &admitted.workload,
        &*verifier.public.trust.read().map_err(unauthorized)?,
        now(),
    )
    .map_err(unauthorized)
}

fn scope_facts(
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

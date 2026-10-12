//! grants fork protocol.
use super::*;
pub(in crate::fork) fn reserve_reference_bytes<'a>(
    totals: &mut BTreeMap<&'a ProviderRef, u64>,
    verifier: &'a dyn ForkSeedVerifier,
    file: &FileRef,
    owned: &mut CodecQuota<'_, '_>,
) -> Result<()> {
    let key = verifier.provider();
    if !totals.contains_key(key) { owned.slots::<(&ProviderRef, u64)>(1)?; }
    let total = totals.entry(key).or_default();
    *total = total
        .checked_add(file.descriptor().byte_length())
        .ok_or_else(|| Error::Invalid("fork reference bytes overflow".into()))?;
    if *total > verifier.maximum_reference_bytes() {
        return Err(Error::Invalid(
            "fork reference bytes exceed aggregate limit".into(),
        ));
    }
    Ok(())
}

pub(in crate::fork) fn decode_fork_manifest(
    reference: &FileRef,
    bytes: &[u8],
    owned: &mut CodecQuota<'_, '_>,
) -> Result<Vec<crate::conversation::Attachment>> {
    if owned.work().is_none() {
        return decode_attachment_manifest(reference, bytes, None, None);
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = owned.work().as_deref().and_then(|io| io.original_native_work()) {
        let io = crate::filesystem::PreparationIo::from_native_work(native)?;
        let items = crate::conversation::decode_attachment_manifest(
            reference, bytes, None, Some(&io))?;
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        owned.work().as_deref_mut().expect("original manifest observer").absorb_native_custody(custody)?;
        owned.adopt(retained)?;
        return Ok(items);
    }
    reference.validate(&mut None)?;
    if reference.descriptor().media_type() != "application/vnd.acyclic.harness.attachments+json" {
        return Err(Error::Invalid("attachment manifest metadata is invalid".into()));
    }
    if bytes.len() as u64 == reference.descriptor().byte_length()
        && let Some(observer) = owned.work().as_deref_mut()
    {
        observer.hashed_bytes(bytes.len() as u64)?;
    }
    reference.descriptor().verify(bytes, &mut None)?;
    let maximum = crate::store::default_projection_read_limits().maximum_bytes;
    if bytes.len() as u64 > maximum {
        return Err(Error::Invalid("attachment manifest exceeds finite codec allowance".into()));
    }
    let schema_bytes = (bytes.len() as u64).max(8).checked_mul(2)
        .and_then(|count| count.checked_mul(
            (std::mem::size_of::<crate::conversation::Attachment>()
                + std::mem::size_of::<String>() + 1) as u64,
        ))
        .ok_or_else(|| Error::Invalid("attachment manifest schema storage overflows".into()))?;
    owned.retain(schema_bytes)?;
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        ["Attachment", "file", "label", "FileRefWire", "volume", "path", "version",
            "descriptor", "display_name", "FileDescriptorWire", "sha256", "byte_length",
            "media_type", "VolumeRefWire", "provider", "id", "class", "owner",
            "ProviderRefWire", "namespace", "family", "project", "agent_private",
            "session_shared", "kind", "agent", "session",
            "SHA-256 digest must contain exactly 32 bytes", "provider identity is invalid"]
            .iter().map(|label| label.len() as u64 + 4).sum(),
    )?;
    let bounds = crate::contract::JsonBounds {
        maximum_bytes: maximum,
        maximum_items: maximum.saturating_add(1),
        maximum_depth: 256,
    };
    let mut observed_failure = false;
    let mut charge = |charge| {
        let result = match charge {
            crate::contract::JsonCodecCharge::Admission { .. } => {
                owned.retain(diagnostic_bytes).and_then(|()| owned.checkpoint())
            }
            _ => match owned.work().as_deref_mut() {
                Some(observer) => observer.codec_charge(charge),
                None => Ok(()),
            },
        };
        observed_failure |= result.is_err();
        result
    };
    let decoded: Result<Vec<crate::conversation::Attachment>> =
        crate::contract::json_from_slice_bounded(std::marker::PhantomData, bytes, bounds, schema_bytes, None, None, &mut charge);
    drop(charge);
    let items = decoded.map_err(|error| {
        if observed_failure { error }
        else { Error::Invalid(format!("attachment manifest is invalid: {error}")) }
    })?;
    owned.release(diagnostic_bytes)?;
    if items.len() as u64 > crate::conversation::MAX_PORTABLE_COUNT as u64 {
        return Err(Error::Invalid("attachment count exceeds limit".into()));
    }
    for item in &items {
        item.validate(&mut None)?;
    }
    let canonical = crate::contract::json_bytes_bounded(&items, bounds, None, &mut |charge| {
        match charge {
            crate::contract::JsonCodecCharge::Retain { bytes } => owned.retain(bytes),
            crate::contract::JsonCodecCharge::Release { bytes } => owned.release(bytes),
            _ => match owned.work().as_deref_mut() {
                Some(observer) => observer.codec_charge(charge),
                None => Ok(()),
            },
        }
    })?;
    owned.retain((canonical.capacity() - canonical.len()) as u64)?;
    let canonical_capacity = canonical.capacity() as u64;
    let matches = canonical == bytes;
    drop(canonical);
    owned.release(canonical_capacity)?;
    if !matches {
        return Err(Error::Invalid("attachment manifest is not canonical or complete".into()));
    }
    Ok(items)
}

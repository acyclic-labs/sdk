//! Aggregate reference limits and canonical attachment admission.
use super::*;

pub(in crate::fork) fn reserve_reference_bytes<'a>(
    totals: &mut BTreeMap<&'a ProviderRef, u64>,
    verifier: &'a dyn ForkSeedVerifier,
    file: &FileRef,
    owned: &mut CodecQuota<'_, '_>,
) -> Result<()> {
    let key = verifier.provider();
    if !totals.contains_key(key) {
        owned.slots::<(&ProviderRef, u64)>(1)?;
    }
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
    if let Some(native) = owned
        .work()
        .as_deref()
        .and_then(|io| io.original_native_work())
    {
        let io = crate::filesystem::PreparationIo::from_native_work(native)?;
        let items = decode_attachment_manifest(reference, bytes, None, Some(&io))?;
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        owned.observer()?.absorb_native_custody(custody)?;
        owned.adopt(retained)?;
        return Ok(items);
    }
    reference.validate(&mut None)?;
    if reference.descriptor().media_type() != "application/vnd.acyclic.harness.attachments+json" {
        return Err(Error::Invalid(
            "attachment manifest metadata is invalid".into(),
        ));
    }
    reference.descriptor().verify(bytes, owned.work())?;
    let maximum = crate::store::default_projection_read_limits().maximum_bytes;
    if bytes.len() as u64 > maximum {
        return Err(Error::Invalid(
            "attachment manifest exceeds finite codec allowance".into(),
        ));
    }
    let schema_bytes = (bytes.len() as u64)
        .max(8)
        .checked_mul(2)
        .and_then(|count| {
            count.checked_mul(
                (std::mem::size_of::<crate::conversation::Attachment>()
                    + std::mem::size_of::<String>()
                    + 1) as u64,
            )
        })
        .ok_or_else(|| Error::Invalid("attachment manifest schema storage overflows".into()))?;
    owned.retain(schema_bytes)?;
    let diagnostic_bytes = crate::executor::json::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        crate::executor::json::preparation_serde_diagnostic_label_bytes(),
    )?;
    let bounds = crate::executor::json::JsonBounds {
        maximum_bytes: maximum,
        maximum_items: maximum.saturating_add(1),
        maximum_depth: 256,
    };
    let items: Vec<crate::conversation::Attachment> =
        crate::executor::json::json_from_slice_bounded(
            std::marker::PhantomData,
            bytes,
            bounds,
            schema_bytes,
            None,
            None,
            &mut |charge| match charge {
                crate::executor::json::JsonCodecCharge::Admission { .. } => owned
                    .retain(diagnostic_bytes)
                    .and_then(|()| owned.checkpoint()),
                _ => owned.observer()?.codec_charge(charge),
            },
        )?;
    owned.release(diagnostic_bytes)?;
    if items.len() as u64 > crate::conversation::MAX_PORTABLE_COUNT as u64 {
        return Err(Error::Invalid("attachment count exceeds limit".into()));
    }
    for item in &items {
        item.validate(&mut None)?;
    }
    let canonical = crate::executor::json::json_bytes_bounded(
        &items,
        bounds,
        None,
        &mut |charge| match charge {
            crate::executor::json::JsonCodecCharge::Retain { bytes } => owned.retain(bytes),
            crate::executor::json::JsonCodecCharge::Release { bytes } => owned.release(bytes),
            _ => owned.observer()?.codec_charge(charge),
        },
    )?;
    let capacity = canonical.capacity() as u64;
    let matches = canonical == bytes;
    drop(canonical);
    owned.release(capacity)?;
    if !matches {
        return Err(Error::Invalid(
            "attachment manifest is not canonical or complete".into(),
        ));
    }
    Ok(items)
}

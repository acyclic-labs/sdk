use super::*;

/// Provider-owned metadata proving an immutable merge publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderJoinProof {
    /// Provider that owns and verifies this statement.
    pub provider: ProviderRef,
    /// Versioned provider-specific proof format.
    pub format: String,
    /// Bounded immutable proof payload.
    pub statement: serde_json::Value,
}

impl ProviderJoinProof {
    /// Rejects an invalid or unbounded provider proof.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        self.provider.validate(work)?;
        if self.format.is_empty()
            || self.statement.is_null()
            || !proof_numbers_are_js_safe(&self.statement, work)?
        {
            return Err(merge_boundary(work, "provider merge proof is invalid"));
        }
        Ok(())
    }
}

pub(super) fn merge_boundary(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_deref()
        .is_some_and(|io| io.original_native_work().is_some())
    {
        return Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason,
        };
    }
    let _ = work;
    Error::Invalid(reason.into())
}

fn proof_numbers_are_js_safe(
    value: &serde_json::Value,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    // FIRST: the borrowed traversal buffer drops before its original quota.
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut pending = crate::core::reserve_copy_vector::<&serde_json::Value>(1, &mut quota)?;
    pending.push(value);
    while let Some(value) = pending.pop() {
        if let Some(io) = quota.work().as_deref_mut() {
            io.check_cancellation()?;
            io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
        }
        match value {
            serde_json::Value::Number(number) => {
                if !crate::conversation::is_exact_js_integer(number) {
                    return Ok(false);
                }
            }
            serde_json::Value::Array(items) => {
                append_proof_values(&mut pending, items.iter(), items.len(), &mut quota)?
            }
            serde_json::Value::Object(fields) => {
                append_proof_values(&mut pending, fields.values(), fields.len(), &mut quota)?
            }
            _ => {}
        }
    }
    Ok(true)
}

fn append_proof_values<'a>(
    pending: &mut Vec<&'a serde_json::Value>,
    values: impl Iterator<Item = &'a serde_json::Value>,
    count: usize,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<()> {
    let required = pending
        .len()
        .checked_add(count)
        .ok_or_else(|| merge_boundary(quota.work(), "provider merge proof traversal overflows"))?;
    if required > pending.capacity() {
        let mut replacement =
            crate::core::reserve_copy_vector::<&serde_json::Value>(required, quota)?;
        let copied = (pending.len() * std::mem::size_of::<&serde_json::Value>()) as u64;
        if let Some(io) = quota.work().as_deref_mut() {
            io.admit_copy(copied)?;
            io.copy_bytes(copied)?;
        }
        replacement.extend_from_slice(pending);
        let old = std::mem::replace(pending, replacement);
        let old_bytes = (old.capacity() * std::mem::size_of::<&serde_json::Value>()) as u64;
        drop(old);
        if quota.work().is_some() {
            quota.relinquish(old_bytes)?;
        }
    }
    pending.extend(values);
    Ok(())
}

pub(super) fn provider_operation_has_nonzero(
    bytes: &[u8],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    for byte in bytes {
        if let Some(io) = work.as_deref_mut() {
            io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
        }
        if *byte != 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

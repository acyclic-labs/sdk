#[cfg(any(test, feature = "test-support"))]
use super::{AuthorityVerifier, Scope};
#[cfg(any(test, feature = "test-support"))]
use crate::Error;
use crate::Result;
#[cfg(any(test, feature = "test-support"))]
use acyclic_fs::access::Capability;

#[cfg(any(test, feature = "test-support"))]
impl AuthorityVerifier {
    /// Verify a signed scope and extract its single filesystem grant payload.
    /// Call once when binding a view, then use `acyclic_fs::access::PathTrie` for
    /// checks. Principal attributes and operation context come from the host.
    pub fn filesystem_capability(&self, scope: &Scope) -> Result<Capability> {
        self.verify(scope, &mut None)?;
        let label = filesystem_scope_label(scope.capabilities(), &mut None)?
            .ok_or_else(|| Error::Unauthorized("scope has no filesystem grant".into()))?;
        let capability = Capability::from_scope_label(label)
            .map_err(|error| Error::Unauthorized(error.to_string()))?
            .ok_or_else(|| Error::Unauthorized("scope has no filesystem grant".into()))?;
        capability
            .validate()
            .map_err(|error| Error::Unauthorized(error.to_string()))?;
        Ok(capability)
    }
}

pub(crate) fn filesystem_scope_label<'a>(
    capabilities: &'a crate::Capabilities,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Option<&'a str>> {
    let mut result = None;
    for label in capabilities.iter() {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
            io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
        }
        if label.starts_with(acyclic_fs::access::SCOPE_PREFIX) {
            if result.is_some() {
                return Err(crate::fork::boundary_error(
                    work,
                    crate::wire::ErrorCode::Unauthorized,
                    "scope has multiple filesystem grants",
                ));
            }
            result = Some(label);
        }
    }
    Ok(result)
}

// Legacy labels are request admission only; the filesystem owner still applies
// the persisted policy and every structured ceiling through PathTrie.
pub(crate) fn directory_read_granted_with_work(
    grants: &crate::Capabilities,
    volume: &crate::conversation::VolumeRef,
    path: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    use acyclic_fs::access::{DIRECTORY_READ_PREFIX, VolumeOperation};
    if volume.provider().family() == "filesystem" && filesystem_scope_label(grants, work)?.is_some()
    {
        return Ok(true);
    }
    if contains_content_grant_with_work(
        grants,
        volume,
        VolumeOperation::Read.capability_prefix(),
        work,
    )? {
        return Ok(true);
    }
    for prefix in std::iter::once("")
        .chain(
            path.match_indices('/')
                .filter_map(|(end, _)| path.get(..end)),
        )
        .chain((!path.is_empty()).then_some(path))
    {
        if !crate::conversation::is_internal_path(prefix)
            && contains_content_grant_with_work(
                grants,
                &(volume, prefix),
                DIRECTORY_READ_PREFIX,
                work,
            )?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn contains_content_grant_with_work<T: serde::Serialize + ?Sized>(
    grants: &crate::Capabilities,
    value: &T,
    prefix: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    if let Some(io) = work.as_deref_mut() {
        io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
    }
    if !grants
        .0
        .range::<str, _>((
            std::ops::Bound::Included(prefix),
            std::ops::Bound::Unbounded,
        ))
        .next()
        .is_some_and(|grant| grant.starts_with(prefix))
    {
        return Ok(false);
    }
    let digest = crate::executor::json::canonical_json_digest(
        value,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        work,
    )?;
    if let Some(io) = work.as_deref_mut() {
        io.codec_charge(crate::executor::json::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        io.copy_bytes((prefix.len() + 64) as u64)?;
    }
    Ok(crate::conversation::contains_content_label(
        grants, prefix, digest,
    ))
}

pub(crate) fn volume_write_granted_with_work(
    grants: &crate::Capabilities,
    volume: &crate::conversation::VolumeRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    if volume.provider().family() == "filesystem" && filesystem_scope_label(grants, work)?.is_some()
    {
        return Ok(true);
    }
    contains_content_grant_with_work(
        grants,
        volume,
        acyclic_fs::access::VolumeOperation::Write.capability_prefix(),
        work,
    )
}

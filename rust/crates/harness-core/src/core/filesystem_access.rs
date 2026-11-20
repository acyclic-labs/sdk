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
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
        io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
    }
    let mut result = None;
    for label in capabilities.with_prefix(acyclic_fs::access::SCOPE_PREFIX) {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
            io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
        }
        if result.is_some() {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "scope has multiple filesystem grants",
            ));
        }
        result = Some(label);
    }
    Ok(result)
}

// Legacy labels are request admission only; the filesystem owner still applies
// the persisted policy and every structured ceiling through PathTrie.
/// How a scope's grants admit a read. Every content path (exact version,
/// range, directory, local storage) decides with [`file_read_grant`] or
/// [`directory_read_grant`], so no path is stricter or looser than another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReadGrant<'p> {
    /// The structured `filesystem:` scope; the volume's persisted policy
    /// still decides each path.
    Structured,
    /// The whole volume.
    Volume,
    /// One file by its exact version.
    Exact,
    /// Every path under this directory prefix, subject to persisted policy.
    Directory(&'p str),
}

/// Whether `grants`' structured `filesystem:` scope admits `volume`, leaving
/// each path to the volume's persisted policy. Its `volume: None` stands for
/// the host's authenticated selection, so only host-held scopes carry one: a
/// workload's admission replaces it with explicit labels for the agent's own
/// bindings (`ScopeDerivation::Workload`). A workload keeps only a structured
/// label naming exact volumes, whose compiled grants reach nothing else.
pub(crate) fn structured_grant(
    grants: &crate::Capabilities,
    volume: &crate::conversation::VolumeRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    Ok(volume.provider().family() == "filesystem"
        && filesystem_scope_label(grants, work)?.is_some())
}

/// The original whole-volume, exact-file, directory or structured grant for `file`, if any.
pub(crate) fn file_read_grant<'f>(
    grants: &crate::Capabilities,
    file: &'f crate::conversation::FileRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Option<ReadGrant<'f>>> {
    if grants
        .with_prefix(acyclic_fs::access::FILE_READ_PREFIX)
        .next()
        .is_some()
        && matches_read_label(grants, work, |work| file.read_capability(work))?
    {
        return Ok(Some(ReadGrant::Exact));
    }
    if file.volume().is_agent_journal()
        && journal_input_requires_selection(file.path())
        && !grants.contains(crate::contract::capability::HARNESS_HOST)
    {
        // Staging owns the journal write. Ordinary readers only receive the
        // exact accepted input or the operation's original steering directory.
        let Some(prefix) = journal_turn_input_directory(file.path()) else {
            return Ok(None);
        };
        return Ok(matches_read_label(grants, work, |work| {
            file.volume().directory_read_capability(prefix, work)
        })?
        .then_some(ReadGrant::Directory(prefix)));
    }
    directory_read_grant(
        grants,
        file.volume(),
        file.path()
            .rsplit_once('/')
            .map_or("", |(parent, _)| parent),
        work,
    )
}

pub(crate) fn journal_input_requires_selection(path: &str) -> bool {
    path.strip_prefix("inputs/").is_some_and(|relative| {
        !relative.contains('/') || relative.starts_with("turns/") || relative.starts_with("skills/")
    })
}

pub(crate) fn journal_turn_input_directory(path: &str) -> Option<&str> {
    const ROOT: &str = "inputs/turns/";
    let (operation, _) = path.strip_prefix(ROOT)?.split_once('/')?;
    if operation.is_empty() {
        return None;
    }
    path.get(..ROOT.len() + operation.len())
}

/// The grant that admits reading every path under `path` of `volume`, if any.
pub(crate) fn directory_read_grant<'p>(
    grants: &crate::Capabilities,
    volume: &crate::conversation::VolumeRef,
    path: &'p str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Option<ReadGrant<'p>>> {
    use acyclic_fs::access::{DIRECTORY_READ_PREFIX, VolumeOperation};
    if structured_grant(grants, volume, work)? {
        return Ok(Some(ReadGrant::Structured));
    }
    if contains_content_grant_with_work(
        grants,
        volume,
        VolumeOperation::Read.capability_prefix(),
        work,
    )? {
        return Ok(Some(ReadGrant::Volume));
    }
    if grants.with_prefix(DIRECTORY_READ_PREFIX).next().is_none() {
        return Ok(None);
    }
    let digest = volume.identity_digest(
        crate::store::default_projection_read_limits().maximum_bytes,
        work,
    )?;
    for prefix in std::iter::once("")
        .chain(
            path.match_indices('/')
                .filter_map(|(end, _)| path.get(..end)),
        )
        .chain((!path.is_empty()).then_some(path))
    {
        if matches_read_label(grants, work, |work| {
            volume.directory_read_label(digest, prefix, work)
        })? {
            return Ok(Some(ReadGrant::Directory(prefix)));
        }
    }
    Ok(None)
}

fn matches_read_label(
    grants: &crate::Capabilities,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    make: impl FnOnce(&mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<String>,
) -> Result<bool> {
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let label = make(quota.work())?;
    quota.adopt(label.capacity() as u64)?;
    Ok(grants.contains(&label))
}

pub(crate) fn directory_read_granted_with_work(
    grants: &crate::Capabilities,
    volume: &crate::conversation::VolumeRef,
    path: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    Ok(directory_read_grant(grants, volume, path, work)?.is_some())
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
    if grants.with_prefix(prefix).next().is_none() {
        return Ok(false);
    }
    let digest = crate::executor::json::canonical_json_digest(
        value,
        Some(acyclic_stream::MAX_RECORD_BYTES as u64),
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

/// Whether a grant admits attempting this volume operation. A structured
/// filesystem grant still requires the bound policy check before content IO.
pub fn volume_operation_granted_with_work(
    grants: &crate::Capabilities,
    volume: &crate::conversation::VolumeRef,
    operation: crate::conversation::VolumeOperation,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    if structured_grant(grants, volume, work)? {
        return Ok(true);
    }
    contains_content_grant_with_work(grants, volume, operation.capability_prefix(), work)
}

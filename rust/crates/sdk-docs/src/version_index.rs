use super::*;

/// Canonical schema bytes held through one admission and publication.
pub(super) struct DocsSchema(Vec<u8>);

impl DocsSchema {
    pub fn new() -> Result<Self, Error> {
        Ok(Self(serde_json::to_vec_pretty(&schema_json()?)?))
    }

    pub fn bytes(&self) -> &[u8] {
        &self.0
    }
}

pub(super) fn validate_version_index(
    index: &VersionIndex,
    output_dir: &Path,
    schema: &DocsSchema,
) -> Result<(), Error> {
    let mut candidates = HashSet::new();
    for entry in &index.release_candidates {
        if entry.publication_status != PublicationStatus::Candidate
            || !entry.released_packages.is_empty()
        {
            return Err(Error::Invalid(
                "candidate catalog entry claims registry release evidence".into(),
            ));
        }
        validate_version_entry(entry, &Channel::Release, output_dir, schema)?;
        if !candidates.insert(&entry.version) {
            return Err(Error::Invalid("duplicate release candidate".into()));
        }
    }
    let mut prereleases = HashSet::new();
    for entry in &index.historical_prereleases {
        if Version::parse(&entry.version)
            .map_err(|e| Error::Invalid(e.to_string()))?
            .pre
            .is_empty()
            || entry.publication_status != PublicationStatus::RegistryReleased
            || entry.released_packages.is_empty()
        {
            return Err(Error::Invalid(
                "historical prerelease requires a released prerelease identity".into(),
            ));
        }
        validate_version_entry(entry, &Channel::Release, output_dir, schema)?;
        if !prereleases.insert(&entry.version) {
            return Err(Error::Invalid("duplicate historical prerelease".into()));
        }
    }
    let mut release_versions = HashSet::new();
    for entry in &index.releases {
        if entry.publication_status != PublicationStatus::RegistryReleased
            || entry.released_packages.is_empty()
        {
            return Err(Error::Invalid(
                "stable release catalog requires verified registry evidence".into(),
            ));
        }
        stable_version(&entry.version)?;
        validate_version_entry(entry, &Channel::Release, output_dir, schema)?;
        if !release_versions.insert(entry.version.as_str()) {
            return Err(Error::Invalid(format!(
                "version index contains duplicate release {}",
                entry.version
            )));
        }
    }
    if let Some(preview) = &index.preview {
        validate_version_entry(preview, &Channel::Preview, output_dir, schema)?;
    }
    if let Some(latest) = &index.latest {
        stable_version(&latest.version)?;
        let Some(release) = index
            .releases
            .iter()
            .find(|entry| entry.version == latest.version)
        else {
            return Err(Error::Invalid(format!(
                "version index latest release {} is absent from releases",
                latest.version
            )));
        };
        if release != latest {
            return Err(Error::Invalid(format!(
                "version index latest release {} differs from its release entry",
                latest.version
            )));
        }
    }
    validate_latest_release(index)?;
    Ok(())
}

pub(super) fn validate_latest_release(index: &VersionIndex) -> Result<(), Error> {
    let Some(maximum) = index
        .releases
        .iter()
        .filter(|entry| {
            entry.publication_status == PublicationStatus::RegistryReleased
                && !entry.released_packages.is_empty()
                && !entry.released_packages.iter().any(|package| package.yanked)
        })
        .max_by(|left, right| compare_versions(&left.version, &right.version))
    else {
        if index.latest.is_some() {
            return Err(Error::Invalid(
                "stable latest requires a verified non-yanked release".into(),
            ));
        }
        return Ok(());
    };
    match index.latest.as_ref() {
        Some(latest) if latest == maximum => Ok(()),
        Some(latest) => Err(Error::Invalid(format!(
            "version index latest release {} is not the maximum release {}",
            latest.version, maximum.version
        ))),
        None => Err(Error::Invalid(format!(
            "version index latest release is missing; maximum release is {}",
            maximum.version
        ))),
    }
}

fn validate_version_entry(
    entry: &VersionEntry,
    expected_channel: &Channel,
    output_dir: &Path,
    docs_schema: &DocsSchema,
) -> Result<(), Error> {
    if &entry.channel != expected_channel {
        return Err(Error::Invalid(format!(
            "version index entry {} has the wrong channel",
            entry.version
        )));
    }
    match entry.channel {
        Channel::Release => {
            if entry.released_packages.is_empty() {
                stable_version(&entry.version)?;
            } else {
                Version::parse(&entry.version).map_err(|e| Error::Invalid(e.to_string()))?;
            }
        }
        Channel::Preview => {
            safe_version(&entry.version)?;
        }
    }
    if entry.revision.len() < 40
        || entry.revision.len() > 64
        || !entry.revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(format!(
            "version index entry {} has an invalid source revision",
            entry.version
        )));
    }
    if entry.data_sha256.len() != 64
        || !entry
            .data_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(format!(
            "version index entry {} has an invalid data digest",
            entry.version
        )));
    }
    let channel_dir = match entry.channel {
        Channel::Release
            if entry.publication_status == PublicationStatus::Candidate
                && entry.data_file.starts_with("release-candidates/") =>
        {
            "release-candidates"
        }
        Channel::Release => "releases",
        Channel::Preview => "preview",
    };
    if !entry.data_file.ends_with("/sdk-docs-data.v1.json") {
        return Err(Error::Invalid(format!(
            "version index entry {} has an unsupported data file {}",
            entry.version, entry.data_file
        )));
    }
    let schema = DATA_SCHEMA_VERSION;
    let expected_data_file = format!(
        "{channel_dir}/{}/{}.json",
        safe_version(&entry.version)?,
        schema
    );
    if entry.data_file != expected_data_file {
        return Err(Error::Invalid(format!(
            "version index entry {} has unexpected data file {}",
            entry.version, entry.data_file
        )));
    }
    let data_path = output_dir.join(&entry.data_file);
    reject_reparse_ancestors(&data_path)?;
    let metadata = fs::symlink_metadata(&data_path).map_err(|_| {
        Error::Invalid(format!(
            "version index entry {} refers to a missing data file {}",
            entry.version,
            data_path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(Error::Invalid(format!(
            "version index entry {} data path is not a regular file {}",
            entry.version,
            data_path.display()
        )));
    }
    let data_bytes = fs::read(&data_path)?;
    if sha256_hex(&data_bytes) != entry.data_sha256 {
        return Err(Error::Invalid(format!(
            "version index entry {} data digest does not match {}",
            entry.version,
            data_path.display()
        )));
    }
    // Serde structs also accept positional arrays; the persisted format is an object.
    if data_bytes.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
        return Err(Error::Invalid(format!(
            "version index entry {} data file must be a JSON object",
            entry.version
        )));
    }
    let data: DocsData = serde_json::from_slice(&data_bytes).map_err(|error| {
        Error::Invalid(format!(
            "version index entry {} data file is not valid DocsData: {error}",
            entry.version
        ))
    })?;
    if data.schema != schema || data.schema_version != schema {
        return Err(Error::Invalid(format!(
            "version index entry {} data file has an unsupported docs schema",
            entry.version
        )));
    }
    validate_schema_sidecar(output_dir, entry, docs_schema)?;
    if data.version != entry.version {
        return Err(Error::Invalid(format!(
            "version index entry {} data version is {}",
            entry.version, data.version
        )));
    }
    if data.channel != entry.channel {
        return Err(Error::Invalid(format!(
            "version index entry {} data channel does not match the index",
            entry.version
        )));
    }
    if data.source.revision != entry.revision {
        return Err(Error::Invalid(format!(
            "version index entry {} data revision does not match the index",
            entry.version
        )));
    }
    if data.source.publication_status != entry.publication_status {
        return Err(Error::Invalid(
            "version entry publication status differs from immutable data".into(),
        ));
    }
    if data.source.captured_source != entry.captured_source {
        return Err(Error::Invalid(
            "version entry captured source differs from immutable data".into(),
        ));
    }
    if data.source.released_packages != entry.released_packages {
        return Err(Error::Invalid(
            "version index released package identities differ from immutable data".into(),
        ));
    }
    if !entry.released_packages.is_empty() {
        historical::validate_release_identity(&data)?;
    }
    validate_source_info(&data.source, &data.channel)?;
    validate_navigation(&data)?;
    Ok(())
}

fn validate_schema_sidecar(
    output_dir: &Path,
    entry: &VersionEntry,
    schema: &DocsSchema,
) -> Result<(), Error> {
    let data_path = output_dir.join(&entry.data_file);
    let sidecar_path = data_path
        .parent()
        .ok_or_else(|| Error::Invalid("data file has no version directory".into()))?
        .join("sdk-docs-data.v1.schema.json");
    reject_reparse_ancestors(&sidecar_path)?;
    let metadata = fs::symlink_metadata(&sidecar_path).map_err(|_| {
        Error::Invalid(format!(
            "version index entry {} refers to a missing schema sidecar {}",
            entry.version,
            sidecar_path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(Error::Invalid(format!(
            "version index entry {} schema sidecar is not a regular file {}",
            entry.version,
            sidecar_path.display()
        )));
    }
    let actual = fs::read(&sidecar_path)?;
    if actual != schema.bytes() {
        return Err(Error::Invalid(format!(
            "version index entry {} schema sidecar does not match the pinned schema",
            entry.version
        )));
    }
    Ok(())
}

pub(super) fn merge_version_index(
    mut index: VersionIndex,
    entry: &VersionEntry,
    mark_latest: bool,
) -> Result<VersionIndex, Error> {
    // The sole caller holds the publication lock and just loaded and validated
    // this index. Merging changes only the new entry, never the held history.
    if entry.channel == Channel::Release && entry.publication_status == PublicationStatus::Candidate
    {
        if mark_latest || !entry.released_packages.is_empty() {
            return Err(Error::Invalid(
                "release candidate cannot claim stable latest".into(),
            ));
        }
        insert_entry(
            &mut index.release_candidates,
            entry,
            || format!("refusing to rewrite release candidate {}", entry.version),
            compare_versions,
        )?;
        return Ok(index);
    }
    if entry.channel == Channel::Release
        && !Version::parse(&entry.version)
            .map_err(|e| Error::Invalid(e.to_string()))?
            .pre
            .is_empty()
    {
        if entry.released_packages.is_empty() || mark_latest {
            return Err(Error::Invalid(
                "historical prereleases cannot be stable latest".into(),
            ));
        }
        insert_entry(
            &mut index.historical_prereleases,
            entry,
            || "refusing to rewrite historical prerelease identity or digest".to_owned(),
            |left, right| left.cmp(right),
        )?;
        return Ok(index);
    }
    if entry.channel == Channel::Release {
        insert_entry(
            &mut index.releases,
            entry,
            || {
                format!(
                    "refusing to rewrite release {} with a different revision or digest",
                    entry.version
                )
            },
            compare_versions,
        )?;
        if mark_latest {
            let should_advance = match index.latest.as_ref() {
                None => true,
                Some(current) => match (
                    stable_version(&entry.version),
                    stable_version(&current.version),
                ) {
                    (Ok(entry), Ok(current)) => entry > current,
                    _ => false,
                },
            };
            if should_advance {
                index.latest = Some(entry.clone());
            }
        }
    } else if let Some(existing) = &index.preview {
        if existing.version == entry.version && existing != entry {
            return Err(Error::Invalid(
                "refusing to rewrite the preview version with a different revision or digest"
                    .into(),
            ));
        }
        index.preview = Some(entry.clone());
    } else {
        index.preview = Some(entry.clone());
    }
    Ok(index)
}

/// Order catalog versions by their stable identity, falling back to the raw
/// string for versions that are not stable. Every catalog ordering uses this
/// one rule.
fn compare_versions(left: &str, right: &str) -> std::cmp::Ordering {
    match (stable_version(left), stable_version(right)) {
        (Ok(left), Ok(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}

/// Insert a new entry, or verify that the existing entry with the same version
/// is identical. A published identity or digest is never rewritten.
fn insert_entry(
    entries: &mut Vec<VersionEntry>,
    entry: &VersionEntry,
    conflict: impl FnOnce() -> String,
    order: impl Fn(&str, &str) -> std::cmp::Ordering,
) -> Result<(), Error> {
    if let Some(existing) = entries.iter().find(|item| item.version == entry.version) {
        if existing != entry {
            return Err(Error::Invalid(conflict()));
        }
    } else {
        entries.push(entry.clone());
        entries.sort_by(|left, right| order(&left.version, &right.version));
    }
    Ok(())
}

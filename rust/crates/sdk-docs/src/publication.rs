use super::*;

/// Write data, its Schemars-generated schema, and the guarded version index.
///
/// A persistent output-directory lock serializes concurrent publications
/// across index validation, bundle writes, and index replacement.
pub fn write_bundle(data: &DocsData, output_dir: &Path, mark_latest: bool) -> Result<(), Error> {
    write_bundle_inner(data, output_dir, mark_latest, true, false)
}

/// Write immutable data without admitting it into the version catalog.
/// Producers may qualify sidecars before calling [`write_bundle`] to publish.
pub fn write_bundle_files(
    data: &DocsData,
    output_dir: &Path,
    mark_latest: bool,
) -> Result<(), Error> {
    write_bundle_inner(data, output_dir, mark_latest, false, false)
}

/// Catalog namespace shared by documentation and qualification sidecars.
pub fn publication_namespace(channel: &Channel, status: &PublicationStatus) -> &'static str {
    match channel {
        Channel::Release if *status == PublicationStatus::Candidate => "release-candidates",
        Channel::Release => "releases",
        Channel::Preview => "preview",
    }
}

pub(super) fn write_bundle_inner(
    data: &DocsData,
    output_dir: &Path,
    mark_latest: bool,
    publish: bool,
    historical: bool,
) -> Result<(), Error> {
    if data.schema != DATA_SCHEMA_VERSION || data.schema_version != DATA_SCHEMA_VERSION {
        return Err(Error::Invalid(
            "cannot publish data with an unsupported docs schema".into(),
        ));
    }
    if data.channel == Channel::Release {
        if historical {
            historical::validate_release_identity(data)?;
        } else {
            stable_version(&data.version)?;
        }
    }
    if !data.source.released_packages.is_empty() {
        historical::validate_release_identity(data)?;
    }
    if mark_latest && data.source.publication_status != PublicationStatus::RegistryReleased {
        return Err(Error::Invalid(
            "a release candidate cannot be marked latest without verified registry releases".into(),
        ));
    }
    if mark_latest && data.channel != Channel::Release {
        return Err(Error::Invalid("only a release can be marked latest".into()));
    }
    validate_source_info(&data.source, &data.channel)?;
    validate_navigation(data)?;
    reject_reparse_ancestors(output_dir)?;
    fs::create_dir_all(output_dir)?;
    let _publication_lock = lock_publication(output_dir)?;
    let index = load_version_index(output_dir)?;
    let channel_dir = publication_namespace(&data.channel, &data.source.publication_status);
    let version_dir = output_dir
        .join(channel_dir)
        .join(safe_version(&data.version)?);
    let data_file = format!(
        "{channel_dir}/{}/sdk-docs-data.v1.json",
        safe_version(&data.version)?
    );
    let data_bytes = serde_json::to_vec_pretty(data)?;
    let digest = sha256_hex(&data_bytes);
    let data_path = output_dir.join(&data_file);
    if data_path.exists() {
        let existing = fs::read(&data_path)?;
        if sha256_hex(&existing) != digest {
            return Err(Error::Invalid(format!(
                "refusing to rewrite {} with different data",
                data_path.display()
            )));
        }
    }
    let entry = VersionEntry {
        captured_source: data.source.captured_source.clone(),
        publication_status: data.source.publication_status.clone(),
        released_packages: data.source.released_packages.clone(),
        version: data.version.clone(),
        channel: data.channel.clone(),
        revision: data.source.revision.clone(),
        data_file,
        data_sha256: digest,
    };
    let updated_index = merge_version_index(index, &entry, output_dir, mark_latest)?;
    validate_latest_release(&updated_index)?;
    reject_reparse_ancestors(&version_dir)?;
    fs::create_dir_all(&version_dir)?;
    atomic_write(&data_path, &data_bytes, true)?;
    let schema = schema_json()?;
    atomic_write(
        &version_dir.join("sdk-docs-data.v1.schema.json"),
        &serde_json::to_vec_pretty(&schema)?,
        true,
    )?;
    atomic_write(
        &output_dir.join("sdk-docs-versions.v1.schema.json"),
        &serde_json::to_vec_pretty(&schemars::schema_for!(VersionIndex))?,
        true,
    )?;
    if publish {
        atomic_write(
            &output_dir.join("sdk-docs-versions.v1.json"),
            &serde_json::to_vec_pretty(&updated_index)?,
            false,
        )?;
    }
    Ok(())
}

pub(super) fn load_version_index(output_dir: &Path) -> Result<VersionIndex, Error> {
    let index_path = output_dir.join("sdk-docs-versions.v1.json");
    reject_reparse_ancestors(&index_path)?;
    let mut index = if index_path.exists() {
        let metadata = fs::symlink_metadata(&index_path)?;
        if !metadata.is_file() {
            return Err(Error::Invalid(format!(
                "version index is not a regular file: {}",
                index_path.display()
            )));
        }
        serde_json::from_slice::<VersionIndex>(&fs::read(&index_path)?)?
    } else {
        VersionIndex {
            release_candidates: Vec::new(),
            historical_prereleases: Vec::new(),
            schema: VERSION_INDEX_SCHEMA_VERSION.into(),
            latest: None,
            releases: Vec::new(),
            preview: None,
        }
    };
    if index.schema != VERSION_INDEX_SCHEMA_VERSION {
        return Err(Error::Invalid(format!(
            "unsupported version index schema: {}",
            index.schema
        )));
    }
    let mut verified = Vec::new();
    for entry in index.releases.drain(..) {
        if entry.publication_status == PublicationStatus::Candidate {
            index.release_candidates.push(entry);
        } else {
            verified.push(entry);
        }
    }
    index.releases = verified;
    if index
        .latest
        .as_ref()
        .is_some_and(|entry| entry.publication_status == PublicationStatus::Candidate)
    {
        index.latest = None;
    }
    validate_version_index(&index, output_dir)?;
    Ok(index)
}

pub(super) fn lock_publication(output_dir: &Path) -> Result<File, Error> {
    let lock_path = output_dir.join(".sdk-docs-versions.v1.lock");
    reject_reparse_ancestors(&lock_path)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    reject_reparse_ancestors(&lock_path)?;
    if !lock.metadata()?.is_file() {
        return Err(Error::Invalid(format!(
            "publication lock is not a regular file: {}",
            lock_path.display()
        )));
    }
    lock.lock()?;
    Ok(lock)
}

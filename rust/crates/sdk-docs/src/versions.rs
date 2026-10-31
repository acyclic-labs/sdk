use super::*;

pub(super) fn stable_version(version: &str) -> Result<Version, Error> {
    let parsed = Version::parse(version).map_err(|error| {
        Error::Invalid(format!(
            "release version {version:?} is not valid semver: {error}"
        ))
    })?;
    if !parsed.pre.is_empty() {
        return Err(Error::Invalid(format!(
            "release version {version:?} must not contain a pre-release identifier"
        )));
    }
    Ok(parsed)
}

pub(super) fn validate_source_digest(value: &str) -> Result<(), Error> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(Error::Invalid(
            "source manifest digest must use the sha256:<64 hex characters> form".into(),
        ));
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::Invalid(
            "source manifest digest must use the sha256:<64 hex characters> form".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_source_info(source: &SourceInfo, channel: &Channel) -> Result<(), Error> {
    if let Some(captured) = &source.captured_source {
        if captured.revision()? != source.revision {
            return Err(Error::Invalid(
                "captured source identity differs from revision".into(),
            ));
        }
        if matches!(
            captured,
            historical::CapturedSource::RegistryArchives { .. }
        ) && (source.source_state != "registry-archives"
            || source.publication_status != PublicationStatus::RegistryReleased
            || *channel != Channel::Release)
        {
            return Err(Error::Invalid(
                "registry archive source requires verified historical release scope".into(),
            ));
        }
    }
    if (source.publication_status == PublicationStatus::RegistryReleased)
        != !source.released_packages.is_empty()
    {
        return Err(Error::Invalid(
            "publication status does not match verified released package identities".into(),
        ));
    }
    if source.revision.len() < 40
        || source.revision.len() > 64
        || !source.revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(
            "source revision must be a 40- to 64-character hexadecimal identity".into(),
        ));
    }
    if !matches!(
        source.source_state.as_str(),
        "captured-snapshot" | "working-tree" | "release-tag"
    ) && !(source.source_state == "registry-archives"
        && matches!(
            &source.captured_source,
            Some(historical::CapturedSource::RegistryArchives { .. })
        ))
    {
        return Err(Error::Invalid("unsupported source state".into()));
    }
    if matches!(channel, Channel::Release) && source.source_state == "working-tree" {
        return Err(Error::Invalid(
            "release data requires captured-snapshot or release-tag source state".into(),
        ));
    }
    if let Some(source_sha256) = &source.source_sha256 {
        validate_source_digest(source_sha256)?;
    } else if source.source_state != "working-tree" {
        return Err(Error::Invalid(
            "captured snapshots and release tags require a trusted source manifest digest".into(),
        ));
    }
    if source.input_sha256.len() != 64
        || !source
            .input_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(
            "rustdoc input digest must be 64 hexadecimal characters".into(),
        ));
    }
    if source.rustdoc_format_versions.is_empty() {
        return Err(Error::Invalid(
            "rustdoc format metadata must contain at least one version".into(),
        ));
    }
    if source
        .rustdoc_format_versions
        .iter()
        .any(|format| *format != FORMAT_VERSION)
    {
        return Err(Error::Invalid(format!(
            "unsupported rustdoc format in source metadata; expected {}",
            FORMAT_VERSION
        )));
    }
    Ok(())
}

/// Projects a complete documentation version onto one portable storage key.
/// The immutable document and index retain and verify the original version.
pub fn safe_version(version: &str) -> Result<String, Error> {
    if version.is_empty() {
        return Err(Error::Invalid(format!(
            "invalid version path segment: {version:?}"
        )));
    }

    // Inline portable names and short hex encodings fit one physical native
    // component. Longer logical names use the same SHA-256 identity as other
    // immutable documentation objects; publication and reopen still verify
    // the complete original version in both the document and its index.
    if version.len() > 255 {
        return Ok(format!("~h{}", sha256_hex(version.as_bytes())));
    }
    if version.as_bytes().iter().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(*byte, b'.' | b'-' | b'_')
    }) && !version.ends_with('.')
        && !is_windows_reserved_segment(version)
    {
        return Ok(version.to_owned());
    }
    if version.len() > (255 - 1) / 2 {
        return Ok(format!("~h{}", sha256_hex(version.as_bytes())));
    }
    // `~h` is disjoint from both readable names and lowercase hexadecimal.
    Ok(hex_digest("~", version.as_bytes()))
}

pub(super) fn is_windows_reserved_segment(segment: &str) -> bool {
    let stem = segment.split('.').next().unwrap_or_default();
    matches!(
        stem.to_ascii_uppercase().as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}
pub(super) fn format_id(id: Id) -> String {
    id.0.to_string()
}
pub(super) fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}
pub(super) fn slugify(value: &str) -> String {
    value.replace('_', "-").to_ascii_lowercase()
}
pub(super) fn titleize(value: &str) -> String {
    value
        .split(['-', '_'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Return the schema generated directly from the output structs.
pub fn schema_json() -> Result<serde_json::Value, Error> {
    serde_json::to_value(schemars::schema_for!(DocsData)).map_err(Error::from)
}

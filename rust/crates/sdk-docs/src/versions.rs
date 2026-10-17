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

pub(super) fn release_version(version: &str) -> Result<Version, Error> {
    stable_version(version)
}
/// Encode a documentation version as an injective, portable path segment.
pub fn safe_version(version: &str) -> Result<String, Error> {
    if version.is_empty() {
        return Err(Error::Invalid(format!(
            "invalid version path segment: {version:?}"
        )));
    }

    // Keep ordinary lowercase names readable. Unsafe names use a literal
    // `~` plus lowercase UTF-8 hex so URL handling cannot decode them before
    // the static server performs its filesystem lookup. Excluding `~` from
    // the readable alphabet keeps this mapping injective.
    let encoded = if version
        .as_bytes()
        .iter()
        .all(|byte| is_portable_version_byte(*byte))
        && version != "."
        && version != ".."
        && !version.ends_with('.')
        && !version.ends_with(' ')
        && !is_windows_reserved_segment(version)
    {
        version.to_owned()
    } else {
        hex_version(version.as_bytes())
    };
    if encoded.len() > 255 {
        return Err(Error::Invalid(format!(
            "version path segment exceeds 255 bytes: {version:?}"
        )));
    }
    Ok(encoded)
}

pub(super) fn is_portable_version_byte(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-' | b'_')
}

pub(super) fn hex_version(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(1 + bytes.len() * 2);
    encoded.push('~');
    for byte in bytes {
        encoded.push(HEX[(*byte >> 4) as usize] as char);
        encoded.push(HEX[(*byte & 0x0f) as usize] as char);
    }
    encoded
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
    let mut schema = serde_json::to_value(schemars::schema_for!(DocsData))?;
    let object = schema
        .as_object_mut()
        .ok_or_else(|| Error::Invalid("DocsData schema must be a JSON object".into()))?;
    let required = object
        .entry("required")
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    let required = required
        .as_array_mut()
        .ok_or_else(|| Error::Invalid("DocsData schema required must be an array".into()))?;
    for field in ["packages", "search"] {
        if !required.iter().any(|value| value == field) {
            required.push(serde_json::Value::String(field.into()));
        }
    }
    let properties = object
        .get_mut("properties")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| Error::Invalid("DocsData schema properties are missing".into()))?;
    for field in ["schema", "schemaVersion"] {
        let property = properties.get_mut(field).ok_or_else(|| {
            Error::Invalid(format!("DocsData schema property {field} is missing"))
        })?;
        property["const"] = serde_json::Value::String(DATA_SCHEMA_VERSION.into());
    }
    Ok(schema)
}

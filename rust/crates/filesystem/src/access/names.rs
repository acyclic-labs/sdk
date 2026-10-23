use super::InvalidPolicy;
use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
use crate::model::{CaseSensitivity, UnicodePolicy, VolumeConfig};
use std::borrow::Cow;

pub(super) fn encoding(config: VolumeConfig) -> NameEncoding {
    NameEncoding::native_for_profile(config.profile)
}

pub(super) fn encoded_len(text: &str, config: VolumeConfig) -> usize {
    match encoding(config) {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => text.len(),
        NameEncoding::WindowsUtf16Le => text.encode_utf16().count() * 2,
    }
}

// Paths are lossless Unicode projections of the profile's native names. Native
// byte/UTF-16 keys remain exact; only UTF-8 logical names use full case folding.
pub(super) fn key(path: &str, config: VolumeConfig) -> Result<Cow<'_, str>, InvalidPolicy> {
    validate(path, config)?;
    Ok(fold(path, config))
}

pub(super) fn validate(path: &str, config: VolumeConfig) -> Result<(), InvalidPolicy> {
    let encoding = encoding(config);
    if !super::glob::valid_path(path)
        || (encoding == NameEncoding::Utf8
            && config.unicode == UnicodePolicy::RequireNfc
            && !unicode_normalization::is_nfc(path))
        || NamespacePath::validate_component_lengths(
            path.split('/')
                .filter(|part| !part.is_empty())
                .map(|part| encoded_len(part, config)),
            config.limits,
        )
        .is_err()
        || (!path.is_empty()
            && path.split('/').any(|part| {
                LogicalName::validate_unicode(encoding, part, config.limits.maximum_component_bytes)
                    .is_err()
            }))
    {
        return Err(InvalidPolicy("path violates volume name semantics"));
    }
    Ok(())
}

pub(super) fn write_key(
    text: &str,
    config: VolumeConfig,
    sink: &mut impl std::fmt::Write,
) -> std::fmt::Result {
    if config.case_sensitivity == CaseSensitivity::ProfileFolded {
        encoding(config).write_unicode_case_fold_key(text, sink)
    } else {
        sink.write_str(text)
    }
}

pub(super) fn fold(text: &str, config: VolumeConfig) -> Cow<'_, str> {
    if config.case_sensitivity == CaseSensitivity::ProfileFolded {
        encoding(config).unicode_case_fold_key(text)
    } else {
        Cow::Borrowed(text)
    }
}

pub(super) fn folds_unicode(config: VolumeConfig) -> bool {
    config.case_sensitivity == CaseSensitivity::ProfileFolded
        && encoding(config) == NameEncoding::Utf8
}

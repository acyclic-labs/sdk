use crate::kernel::{NameEncoding, NamespacePath};
use crate::{VolumeConfig, VolumeId};
use acyclic_stream::StreamError;

/// Canonical filesystem resource for a stable volume identity and Unicode path.
/// Names are encoded component by component; the owner applies volume semantics.
pub fn resource(volume: VolumeId, path: &str) -> Result<String, StreamError> {
    let mut result = format!("fs:/{}", uuid::Uuid::from_bytes(volume.into_bytes()));
    if !path.is_empty() {
        for component in path.split('/') {
            if component.is_empty() || matches!(component, "." | "..") {
                return Err(StreamError::InvalidArgument);
            }
            result.push('/');
            result.push_str(&acyclic_auth::encode_component(component.as_bytes()));
        }
    }
    acyclic_stream::wire_codec::session::validate_resource(&result)?;
    Ok(result)
}

/// Canonical full native namespace resource using the volume's case semantics.
pub fn namespace_resource(
    volume: VolumeId,
    path: &NamespacePath,
    config: VolumeConfig,
) -> Result<String, StreamError> {
    let mut result = resource(volume, "")?;
    for name in path.components() {
        let mut text = String::new();
        name.append_unicode(&mut text)
            .map_err(|_| StreamError::AccessDenied)?;
        let text = if config.case_sensitivity == crate::model::CaseSensitivity::ProfileFolded {
            NameEncoding::native_for_profile(config.profile).unicode_case_fold_key(&text)
        } else {
            std::borrow::Cow::Borrowed(text.as_str())
        };
        result.push('/');
        result.push_str(&acyclic_auth::encode_component(text.as_bytes()));
    }
    Ok(result)
}

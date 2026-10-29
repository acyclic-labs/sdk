use crate::VolumeId;
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

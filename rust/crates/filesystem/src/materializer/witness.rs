use super::*;

pub(super) const NATIVE_WITNESS_VERSION: u8 = 2;

pub(super) fn encode_native_witness(
    before: Option<[u8; 32]>,
    after: Option<[u8; 32]>,
    metadata: Option<crate::WorkspaceMetadata>,
) -> Result<Vec<u8>, NativeTreeMaterializationError> {
    let mut image = Vec::with_capacity(67);
    image.push(NATIVE_WITNESS_VERSION);
    for fingerprint in [before, after] {
        image.push(u8::from(fingerprint.is_some()));
        image.extend_from_slice(&fingerprint.unwrap_or([0; 32]));
    }
    if let Some(metadata) = metadata {
        image.extend_from_slice(
            &serde_json::to_vec(&metadata)
                .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?,
        );
    }
    Ok(image)
}

pub(super) fn decode_native_witness(
    image: &[u8],
) -> Result<NativeWitness, NativeTreeMaterializationError> {
    let header = image
        .get(..67)
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    if header[0] != NATIVE_WITNESS_VERSION {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    let decode = |field: &[u8]| {
        let fingerprint: [u8; 32] = field[1..]
            .try_into()
            .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?;
        match field[0] {
            0 if fingerprint == [0; 32] => Ok(None),
            1 => Ok(Some(fingerprint)),
            _ => Err(NativeTreeMaterializationError::InvalidLayout),
        }
    };
    Ok((decode(&header[1..34])?, decode(&header[34..67])?))
}

pub(super) fn decode_native_preimage_metadata(
    image: &[u8],
) -> Result<Option<crate::WorkspaceMetadata>, NativeTreeMaterializationError> {
    let metadata = image
        .get(67..)
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    if metadata.is_empty() {
        Ok(None)
    } else {
        serde_json::from_slice(metadata)
            .map(Some)
            .map_err(|_| NativeTreeMaterializationError::InvalidLayout)
    }
}

pub(super) fn decode_native_metadata(
    image: &[u8],
) -> Result<crate::WorkspaceMetadata, NativeTreeMaterializationError> {
    serde_json::from_slice(image).map_err(|_| NativeTreeMaterializationError::InvalidLayout)
}

#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
pub(super) fn encode_native_metadata(
    metadata: &crate::WorkspaceMetadata,
) -> Result<Vec<u8>, NativeTreeMaterializationError> {
    serde_json::to_vec(metadata).map_err(|_| NativeTreeMaterializationError::InvalidLayout)
}

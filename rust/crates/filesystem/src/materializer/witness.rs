use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(super) const NATIVE_WITNESS_VERSION: u8 = 2;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct NativeMetadataImage {
    pub(super) readonly: bool,
    pub(super) posix_mode: Option<u32>,
    pub(super) windows_attributes: Option<u32>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) type NativeFingerprint = [u8; 32];

#[cfg(not(target_arch = "wasm32"))]
pub(super) type NativeWitness = (Option<NativeFingerprint>, Option<NativeFingerprint>);

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn encode_native_witness(
    before: Option<[u8; 32]>,
    after: Option<[u8; 32]>,
    metadata: Option<NativeMetadataImage>,
    work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
    use std::io::Write as _;
    let mut header = [0; 67];
    header[0] = NATIVE_WITNESS_VERSION;
    for (index, fingerprint) in [before, after].into_iter().enumerate() {
        let offset = 1 + index * 33;
        header[offset] = u8::from(fingerprint.is_some());
        header[offset + 1..offset + 33].copy_from_slice(&fingerprint.unwrap_or([0; 32]));
    }
    let mut writer = ImageWriter {
        bytes: Vec::new(),
        allocation: work.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
        failure: None,
    };
    if let Err(error) = writer.write_all(&header) {
        return Err(writer.failure.take().unwrap_or_else(|| error.into()));
    }
    if let Some(metadata) = metadata {
        if serde_json::to_writer(&mut writer, &metadata).is_err() {
            return Err(writer
                .failure
                .take()
                .unwrap_or(NativeTreeMaterializationError::InvalidLayout));
        }
    }
    Ok((writer.bytes, writer.allocation))
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn decode_native_witness(
    image: &[u8],
) -> Result<NativeWitness, NativeTreeMaterializationError> {
    let (witness, _) = image
        .split_at_checked(67)
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    let image: &[u8; 67] = witness
        .try_into()
        .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?;
    let (version, fields) = image
        .split_first()
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    if *version != NATIVE_WITNESS_VERSION {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    let decode = |present: u8, bytes: &[u8]| {
        let fingerprint: [u8; 32] = bytes
            .try_into()
            .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?;
        match present {
            0 if fingerprint == [0; 32] => Ok(None),
            1 => Ok(Some(fingerprint)),
            _ => Err(NativeTreeMaterializationError::InvalidLayout),
        }
    };
    let (before, after) = fields
        .split_at_checked(33)
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    let (before_present, before_bytes) = before
        .split_first()
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    let (after_present, after_bytes) = after
        .split_first()
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    Ok((
        decode(*before_present, before_bytes)?,
        decode(*after_present, after_bytes)?,
    ))
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn decode_native_preimage_metadata(
    image: &[u8],
) -> Result<Option<NativeMetadataImage>, NativeTreeMaterializationError> {
    let (_, metadata) = image
        .split_at_checked(67)
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    if metadata.is_empty() {
        Ok(None)
    } else {
        serde_json::from_slice(metadata)
            .map(Some)
            .map_err(|_| NativeTreeMaterializationError::InvalidLayout)
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn decode_native_metadata(
    image: &[u8],
) -> Result<NativeMetadataImage, NativeTreeMaterializationError> {
    serde_json::from_slice(image).map_err(|_| NativeTreeMaterializationError::InvalidLayout)
}

use super::*;

pub(super) const NATIVE_WITNESS_VERSION: u8 = 2;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct NativeMetadataImage {
    pub(super) readonly: bool,
    pub(super) posix_mode: Option<u32>,
    pub(super) windows_attributes: Option<u32>,
}

pub(super) type NativeFingerprint = [u8; 32];

pub(super) type NativeWitness = (Option<NativeFingerprint>, Option<NativeFingerprint>);

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
    let mut writer = ImageWriter::new(work)?;
    if let Err(error) = writer.write_all(&header) {
        return Err(writer.failure.take().unwrap_or_else(|| error.into()));
    }
    if let Some(metadata) = metadata {
        writer.json(&metadata)?;
    }
    Ok((writer.bytes, writer.allocation))
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

pub(super) fn decode_native_metadata(
    image: &[u8],
) -> Result<NativeMetadataImage, NativeTreeMaterializationError> {
    serde_json::from_slice(image).map_err(|_| NativeTreeMaterializationError::InvalidLayout)
}

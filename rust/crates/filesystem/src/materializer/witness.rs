use super::*;

pub(super) const NATIVE_WITNESS_VERSION: u8 = 2;

pub(super) fn encode_native_witness(
    before: Option<[u8; 32]>,
    after: Option<[u8; 32]>,
    metadata: Option<crate::WorkspaceMetadata>,
    work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
    let mut header = [0; 67];
    let [version, fields @ ..] = &mut header;
    *version = NATIVE_WITNESS_VERSION;
    for ([present, data @ ..], fingerprint) in fields
        .as_chunks_mut::<33>()
        .0
        .iter_mut()
        .zip([before, after])
    {
        *present = u8::from(fingerprint.is_some());
        data.copy_from_slice(&fingerprint.unwrap_or([0; 32]));
    }
    let mut writer = ImageWriter::new(work)?;
    std::io::Write::write_all(&mut writer, &header).map_err(|_| {
        writer
            .failure
            .take()
            .unwrap_or(NativeTreeMaterializationError::InvalidLayout)
    })?;
    if let Some(metadata) = metadata {
        serde_json::to_writer(&mut writer, &metadata).map_err(|_| {
            writer
                .failure
                .take()
                .unwrap_or(NativeTreeMaterializationError::InvalidLayout)
        })?;
    }
    Ok((writer.bytes, writer.allocation))
}

pub(super) fn decode_native_witness(
    image: &[u8],
) -> Result<[Option<[u8; 32]>; 2], NativeTreeMaterializationError> {
    let header = image
        .get(..67)
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    let (version, fields) = header
        .split_first()
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    if *version != NATIVE_WITNESS_VERSION {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    let decode = |[present, fingerprint @ ..]: &[u8; 33]| match *present {
        0 if *fingerprint == [0; 32] => Ok(None),
        1 => Ok(Some(*fingerprint)),
        _ => Err(NativeTreeMaterializationError::InvalidLayout),
    };
    let [before, after] = fields.as_chunks::<33>().0 else {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    };
    Ok([decode(before)?, decode(after)?])
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
    any(test, feature = "local"),
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
pub(super) fn encode_native_metadata(
    metadata: &crate::WorkspaceMetadata,
    work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
    let mut writer = ImageWriter::new(work)?;
    serde_json::to_writer(&mut writer, metadata).map_err(|_| {
        writer
            .failure
            .take()
            .unwrap_or(NativeTreeMaterializationError::InvalidLayout)
    })?;
    Ok((writer.bytes, writer.allocation))
}

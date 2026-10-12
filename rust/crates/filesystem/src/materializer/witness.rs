use super::*;

pub(super) const NATIVE_WITNESS_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub(super) struct NativeMetadataImage {
    pub(super) readonly: bool,
    pub(super) posix_mode: Option<u32>,
    pub(super) windows_attributes: Option<u32>,
}

pub(super) fn encode_native_witness(
    before: Option<[u8; 32]>,
    after: Option<[u8; 32]>,
    metadata: Option<NativeMetadataImage>,
    work: &NativeMaterializationWork,
) -> Result<MaterializationPreimage, NativeTreeMaterializationError> {
    use std::io::Write as _;
    let mut writer = NativeImageWriter::new(work)?;
    let mut header = [0; 67];
    header[0] = NATIVE_WITNESS_VERSION;
    if let Some(before) = before {
        header[1] = 1;
        header[2..34].copy_from_slice(&before);
    }
    if let Some(after) = after {
        header[34] = 1;
        header[35..67].copy_from_slice(&after);
    }
    writer
        .write_all(&header)
        .map_err(|error| writer.failure.take().unwrap_or_else(|| error.into()))?;
    if let Some(metadata) = metadata {
        writer.metadata(&metadata)?;
    }
    Ok(MaterializationPreimage {
        image: writer.image,
        custody: Some(writer.allocation),
    })
}

pub(super) fn decode_native_witness(
    image: &[u8],
) -> Result<(Option<[u8; 32]>, Option<[u8; 32]>), NativeTreeMaterializationError> {
    if image.len() < 67 || image[0] != NATIVE_WITNESS_VERSION {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    let decode = |offset| {
        let bytes: &[u8] = &image[offset..offset + 33];
        let fingerprint: [u8; 32] = bytes[1..]
            .try_into()
            .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?;
        match bytes[0] {
            0 if fingerprint == [0; 32] => Ok(None),
            1 => Ok(Some(fingerprint)),
            _ => Err(NativeTreeMaterializationError::InvalidLayout),
        }
    };
    Ok((decode(1)?, decode(34)?))
}

pub(super) fn decode_native_metadata(
    image: &[u8],
) -> Result<NativeMetadataImage, NativeTreeMaterializationError> {
    serde_json::from_slice(image).map_err(|_| NativeTreeMaterializationError::InvalidLayout)
}

pub(super) struct NativeImageWriter<'a> {
    pub(super) image: Vec<u8>,
    pub(super) allocation: NativeAllocation,
    pub(super) failure: Option<NativeTreeMaterializationError>,
    work: &'a NativeMaterializationWork,
}
impl<'a> NativeImageWriter<'a> {
    pub(super) fn new(
        work: &'a NativeMaterializationWork,
    ) -> Result<Self, NativeTreeMaterializationError> {
        Ok(Self {
            image: Vec::new(),
            allocation: work.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
            failure: None,
            work,
        })
    }
    pub(super) fn metadata(
        &mut self,
        value: &NativeMetadataImage,
    ) -> Result<(), NativeTreeMaterializationError> {
        serde_json::to_writer(&mut *self, value).map_err(|error| {
            self.failure
                .take()
                .unwrap_or_else(|| std::io::Error::other(error).into())
        })
    }
}
impl std::io::Write for NativeImageWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let result = (|| {
            let delta = crate::WorkCounters {
                bytes_encoded: bytes.len() as u64,
                bytes_copied: bytes.len() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            self.work.admit(delta)?;
            let needed = self
                .image
                .len()
                .checked_add(bytes.len())
                .ok_or(crate::WorkError::Overflow)?;
            if needed > self.image.capacity() {
                self.allocation
                    .grow((needed - self.image.capacity()) as u64)?;
                let reserved = self.image.try_reserve_exact(needed - self.image.len());
                let reconciled = self
                    .allocation
                    .reconcile_capacity(self.image.capacity() as u64);
                reserved.map_err(NativeTreeMaterializationError::AllocationFailed)?;
                reconciled?;
            }
            self.image.extend_from_slice(bytes);
            self.work.spent(delta)?;
            Ok::<_, NativeTreeMaterializationError>(bytes.len())
        })();
        result.map_err(|error| {
            self.failure = Some(error);
            std::io::ErrorKind::Other.into()
        })
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "native-mount")]
pub(super) fn encode_native_metadata(
    metadata: &crate::WorkspaceMetadata,
    work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
    let mut writer = NativeImageWriter::new(work)?;
    writer.metadata(&NativeMetadataImage {
        readonly: metadata
            .windows_attributes
            .is_some_and(|attributes| attributes & 1 != 0)
            || metadata.posix_mode.is_some_and(|mode| mode & 0o222 == 0),
        posix_mode: metadata.posix_mode,
        windows_attributes: metadata.windows_attributes,
    })?;
    Ok((writer.image, writer.allocation))
}

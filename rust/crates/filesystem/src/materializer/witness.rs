use super::*;

pub(super) const NATIVE_WITNESS_VERSION: u8 = 2;
const METADATA_DOMAIN: &[u8] = b"acyclic.native.metadata/v";
const METADATA_VERSION: &[u8] = b"1\0";

#[cfg(feature = "test-support")]
#[path = "witness_fuzz.rs"]
/// On-demand checks of the actual witness and metadata codecs.
pub mod codec_fuzz;

fn encode_witness_header(fingerprints: [Option<[u8; 32]>; 2]) -> [u8; 67] {
    let mut header = [0; 67];
    let [version, fields @ ..] = &mut header;
    *version = NATIVE_WITNESS_VERSION;
    for ([present, data @ ..], fingerprint) in
        fields.as_chunks_mut::<33>().0.iter_mut().zip(fingerprints)
    {
        *present = u8::from(fingerprint.is_some());
        data.copy_from_slice(&fingerprint.unwrap_or([0; 32]));
    }
    header
}

pub(super) struct NativeWitness<'a> {
    pub(super) fingerprints: [Option<[u8; 32]>; 2],
    metadata: &'a [u8],
}

impl NativeWitness<'_> {
    pub(super) fn metadata(
        &self,
    ) -> Result<Option<crate::WorkspaceMetadata>, NativeTreeMaterializationError> {
        if self.metadata.is_empty() {
            Ok(None)
        } else {
            decode_metadata_payload(self.metadata).map(Some)
        }
    }
}

pub(super) fn encode_native_witness(
    before: Option<[u8; 32]>,
    after: Option<[u8; 32]>,
    metadata: Option<crate::WorkspaceMetadata>,
    work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
    let header = encode_witness_header([before, after]);
    encode_image(&header, metadata.as_ref(), work)
}

fn encode_image(
    header: &[u8],
    metadata: Option<&crate::WorkspaceMetadata>,
    work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
    let mut writer = ImageWriter::new(work)?;
    std::io::Write::write_all(&mut writer, header).map_err(|_| {
        writer
            .failure
            .take()
            .unwrap_or(NativeTreeMaterializationError::InvalidLayout)
    })?;
    if let Some(metadata) = metadata {
        serde_json::to_writer(&mut writer, metadata).map_err(|_| {
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
) -> Result<NativeWitness<'_>, NativeTreeMaterializationError> {
    image
        .first()
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    let fields = acyclic_auth::format::header(
        image,
        b"",
        &[NATIVE_WITNESS_VERSION],
        || NativeTreeMaterializationError::InvalidLayout,
        || NativeTreeMaterializationError::UnsupportedFormat,
    )?;
    let (fields, metadata) = fields
        .split_at_checked(66)
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    let decode = |[present, fingerprint @ ..]: &[u8; 33]| match *present {
        0 if *fingerprint == [0; 32] => Ok(None),
        1 => Ok(Some(*fingerprint)),
        _ => Err(NativeTreeMaterializationError::InvalidLayout),
    };
    let [before, after] = fields.as_chunks::<33>().0 else {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    };
    Ok(NativeWitness {
        fingerprints: [decode(before)?, decode(after)?],
        metadata,
    })
}

pub(super) fn decode_native_metadata(
    image: &[u8],
) -> Result<crate::WorkspaceMetadata, NativeTreeMaterializationError> {
    let image = acyclic_auth::format::header(
        image,
        METADATA_DOMAIN,
        METADATA_VERSION,
        || NativeTreeMaterializationError::UnsupportedFormat,
        || NativeTreeMaterializationError::UnsupportedFormat,
    )?;
    decode_metadata_payload(image)
}

fn decode_metadata_payload(
    image: &[u8],
) -> Result<crate::WorkspaceMetadata, NativeTreeMaterializationError> {
    serde_json::from_slice(image).map_err(|_| NativeTreeMaterializationError::InvalidLayout)
}

#[cfg(any(
    test,
    feature = "test-support",
    all(any(test, feature = "local"), feature = "native-mount")
))]
pub(super) fn encode_native_metadata(
    metadata: &crate::WorkspaceMetadata,
    work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
    encode_image(b"acyclic.native.metadata/v1\0", Some(metadata), work)
}

#[cfg(kani)]
mod proofs {
    use super::*;

    #[kani::proof]
    #[kani::unwind(68)]
    fn native_witness_header_preserves_all_fingerprint_bits() {
        let fingerprints: [Option<[u8; 32]>; 2] = kani::any();
        let encoded = encode_witness_header(fingerprints);
        let decoded = decode_native_witness(&encoded).unwrap();
        assert_eq!(decoded.fingerprints, fingerprints);
        assert!(decoded.metadata.is_empty());
    }

    #[kani::proof]
    #[kani::unwind(68)]
    fn native_witness_header_is_canonical_and_refuses_before_fields() {
        let bytes: [u8; 67] = kani::any();
        if let Ok(value) = decode_native_witness(&bytes) {
            assert_eq!(encode_witness_header(value.fingerprints), bytes);
        }
        let version: u8 = kani::any();
        if version != NATIVE_WITNESS_VERSION {
            assert!(matches!(
                decode_native_witness(&[version]),
                Err(NativeTreeMaterializationError::UnsupportedFormat)
            ));
        } else {
            let length: usize = kani::any();
            kani::assume(length > 0 && length < bytes.len());
            let mut truncated = bytes;
            truncated[0] = version;
            assert!(matches!(
                decode_native_witness(&truncated[..length]),
                Err(NativeTreeMaterializationError::InvalidLayout)
            ));
        }
    }
}

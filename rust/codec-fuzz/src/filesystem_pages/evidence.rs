//! Projected preparation facts never establish fresh closure or native custody.
use acyclic_fs::kernel::*;
use sha2::{Digest as _, Sha256};

fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(super) fn check(bytes: &[u8]) {
    // Bind the fuzz input to itself so mutations can reach the decoder. The
    // negative control below retains the original hash while changing content.
    let original = hash(bytes);
    let maximum_bytes = u64::try_from(bytes.len()).expect("input length fits the wire bound");
    let Ok(projected) =
        decode_generation_publication_evidence(bytes, &original, maximum_bytes, u64::MAX)
    else {
        return;
    };
    super::historical::publication(bytes, &projected);
    // These are only the facts decoded from original preparation evidence,
    // passed back to its canonical writer, not a new proof of the closure.
    let proof = GenerationProof {
        root: projected.root.clone(),
        generation_id: projected.manifest.generation_id,
        file_count: projected.manifest.file_count,
        logical_file_bytes: projected.logical_file_bytes,
        objects: projected.manifest.objects.clone(),
        work: Default::default(),
    };
    let encoded = encode_generation_publication_evidence(
        &projected.request,
        &proof,
        &projected.append,
        projected.manifest.config,
        maximum_bytes,
    )
    .expect("canonical projected evidence is admitted by its writer");
    assert_eq!(encoded.bytes.as_ref(), bytes);
    assert_eq!(encoded.sha256, original);
    let maximum_objects = u64::try_from(projected.manifest.objects.len())
        .expect("admitted object count fits the wire bound");
    assert_eq!(
        decode_generation_publication_evidence(bytes, &original, maximum_bytes, maximum_objects)
            .expect("exact admitted bounds retain original preparation"),
        projected,
    );
    assert!(matches!(
        decode_generation_publication_evidence(
            bytes,
            &original,
            maximum_bytes - 1,
            maximum_objects
        ),
        Err(PublicationEvidenceError::TooLarge)
    ));
    assert!(matches!(
        decode_generation_publication_evidence(
            bytes,
            &original,
            maximum_bytes,
            maximum_objects - 1
        ),
        Err(PublicationEvidenceError::Manifest(
            GenerationExportManifestError::TooManyObjects
        ))
    ));
    let mut foreign = original;
    foreign[0] ^= 1;
    assert!(matches!(
        decode_generation_publication_evidence(bytes, &foreign, u64::MAX, u64::MAX),
        Err(PublicationEvidenceError::Mismatch)
    ));
    let truncated = &bytes[..bytes.len() - 1];
    assert!(matches!(
        decode_generation_publication_evidence(truncated, &original, u64::MAX, u64::MAX),
        Err(PublicationEvidenceError::Mismatch)
    ));
    let mut trailing = bytes.to_vec();
    trailing.push(0);
    assert!(
        decode_generation_publication_evidence(&trailing, &hash(&trailing), u64::MAX, u64::MAX)
            .is_err()
    );
    super::refuses_versions(
        bytes,
        b"acyclic-fs-generation-publication-evidence-v1\0".len(),
        |header, version| {
            matches!(
                decode_generation_publication_evidence(header, &hash(header), u64::MAX, u64::MAX),
                Err(PublicationEvidenceError::Decode(CanonicalDecodeError::UnsupportedVersion { found, expected: 1 }))
                    if found == version
            )
        },
    );
}

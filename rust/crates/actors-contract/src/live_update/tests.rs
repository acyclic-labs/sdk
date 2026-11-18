use super::*;
use std::cell::Cell;

thread_local! {
    /// Schema descriptor decodes on this thread, the dominant validation cost.
    pub(super) static DESCRIPTOR_DECODES: Cell<u64> = const { Cell::new(0) };
}

fn decodes() -> u64 {
    DESCRIPTOR_DECODES.with(Cell::get)
}

fn upload(code: u8) -> Result<Publication, Error> {
    let codec = crate::guest::CodecIdentity {
        name: "live-update-proof".into(),
        version: "1".into(),
    };
    let compatibility = CompatibilityManifest {
        rpcs: BTreeMap::new(),
        messages: BTreeMap::from([(
            "proof.message".into(),
            Schema {
                descriptor_set: crate::FILE_DESCRIPTOR_SET.to_vec(),
                message: "acyclic.actors.v1.InvokeActorRequest".into(),
            },
        )]),
        peers: Vec::new(),
        handoff: HandoffRequirements {
            writes: codec.clone(),
            reads: vec![codec],
            journal_reads: Vec::new(),
            durable_await: false,
        },
        adapters: Vec::new(),
    };
    let schema = compatibility.schema_digest()?;
    Publication::new(
        DeploymentId {
            account: b"account".to_vec(),
            name: "proof".into(),
        },
        0,
        BTreeMap::from([(
            "alpha".into(),
            Artifact {
                code: [code; 32],
                configuration: [1; 32],
                schema,
            },
        )]),
        compatibility,
    )
}

#[test]
fn a_publication_validates_once_however_often_its_identity_is_read() -> Result<(), Error> {
    let before = decodes();
    let publication = upload(1)?;
    assert_eq!(decodes() - before, 1);
    let version = publication.version().clone();
    let schema = publication.compatibility().schema_digest()?;
    let after = decodes();
    for _ in 0..64 {
        assert_eq!(publication.version(), &version);
        assert_eq!(publication.schema_digest(), schema);
    }
    assert_eq!(decodes(), after);

    // Decoding is construction: it validates exactly once and fixes the same identity.
    let bytes = encode(&publication)?;
    let decoded = decode::<Publication>(&bytes)?;
    assert_eq!(decodes() - after, 1);
    assert_eq!(decoded.version(), &version);
    assert_eq!(version.digest, <[u8; 32]>::from(Sha256::digest(&bytes)));

    // No unvalidated value exists, from either constructor.
    assert!(matches!(upload(0), Err(Error::Invalid("publication"))));
    let exhausted = String::from_utf8(bytes)
        .map_err(|_| Error::Invalid("utf-8"))?
        .replace(
            r#""expectedVersion":"0""#,
            &format!(r#""expectedVersion":"{}""#, u64::MAX),
        );
    assert!(decode::<Publication>(exhausted.as_bytes()).is_err());
    Ok(())
}

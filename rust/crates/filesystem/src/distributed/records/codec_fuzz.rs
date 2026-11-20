//! Original stream record writers and readers, with independent typed inputs.
use super::*;
use crate::kernel::decoder_fuzz::records::record;

/// Named seed inputs, or why one could not be encoded.
pub type Seeds = Result<Vec<(String, Vec<u8>)>, Box<dyn std::error::Error>>;

/// The first `N` bytes of `input`, zero-padded.
fn padded<const N: usize>(input: impl IntoIterator<Item = u8>) -> [u8; N] {
    let mut input = input.into_iter();
    std::array::from_fn(|_| input.next().unwrap_or(0))
}

fn fields(input: &[u8]) -> (GenerationLocator, GenerationId, OperationId) {
    let bytes: [u8; 32] = padded(input.iter().copied());
    let mut words = bytes.into_iter();
    let lineage = padded::<8>(words.by_ref());
    let changes = padded::<8>(words.by_ref());
    let operation = padded::<16>(words);
    (
        GenerationLocator {
            lineage: u64::from_le_bytes(lineage),
            changes: u64::from_le_bytes(changes),
        },
        GenerationId::new(Digest::from_bytes(bytes)),
        OperationId::from_bytes(operation),
    )
}

/// Check exact width, tags and identities through the original decoders.
pub fn check(input: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let (locator, generation, operation) = fields(input);
    let cancellation = CancellationToken::new();
    let encode_locator = |value: &GenerationLocator| {
        value
            .encode(&mut AuthorityWork::new(
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .map(|bytes| bytes.to_vec())
    };
    record(
        input,
        &locator,
        encode_locator(&locator),
        decode_generation_locator,
        encode_locator,
    )?;
    let encode_lineage = |value: &GenerationId| {
        encode_lineage_generation(
            *value,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )
        .map(|bytes| bytes.to_vec())
    };
    let lineage = record(
        input,
        &generation,
        encode_lineage(&generation),
        decode_lineage_generation,
        encode_lineage,
    )?;
    let mut foreign = lineage;
    *foreign
        .get_mut(LINEAGE_DOMAIN.len())
        .ok_or("lineage record has no object kind")? = crate::ObjectKind::Metadata.canonical_tag();
    if !(decode_lineage_generation(&foreign).is_err()) {
        return Err("foreign lineage object kind admitted".into());
    };
    let reserved = ReservedPublication {
        operation,
        head: Head {
            epoch: Epoch::new(locator.lineage.max(1))?,
            sequence: Sequence::new(locator.changes),
            digest: generation.digest(),
        },
        epoch_tail: locator.changes.max(1),
    };
    for operation in [None, Some(reserved)] {
        let encode_gate = |value: &Option<ReservedPublication>| {
            encode_publication_gate(
                *value,
                &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
            )
            .map(|bytes| bytes.to_vec())
        };
        record(
            input,
            &operation,
            encode_gate(&operation),
            decode_publication_gate,
            encode_gate,
        )?;
    }
    Ok(())
}

/// Library encoder output with distinct fields, boundary words and gate states.
pub fn seeds() -> Seeds {
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let mut seeds = Vec::new();
    for (name, input) in [
        ("zero", [0; 32]),
        ("maximum", [255; 32]),
        ("distinct", padded(0..)),
    ] {
        let (locator, generation, operation) = fields(&input);
        seeds.push((
            format!("generation-locator-{name}"),
            locator.encode(&mut work)?.to_vec(),
        ));
        seeds.push((
            format!("generation-lineage-{name}"),
            encode_lineage_generation(generation, &mut work)?.to_vec(),
        ));
        seeds.push((
            format!("publication-gate-active-{name}"),
            encode_publication_gate(
                Some(ReservedPublication {
                    operation,
                    head: Head {
                        epoch: Epoch::new(locator.lineage.max(1))?,
                        sequence: Sequence::new(locator.changes),
                        digest: generation.digest(),
                    },
                    epoch_tail: locator.changes.max(1),
                }),
                &mut work,
            )?
            .to_vec(),
        ));
    }
    seeds.push((
        "publication-gate-free".into(),
        encode_publication_gate(None, &mut work)?.to_vec(),
    ));
    Ok(seeds)
}

//! Original stream record writers and readers, with independent typed inputs.
use super::*;
use crate::kernel::decoder_fuzz::records::record;

fn fields(input: &[u8]) -> (GenerationLocator, GenerationId, OperationId) {
    let mut bytes = [0; 32];
    let length = input.len().min(bytes.len());
    bytes[..length].copy_from_slice(&input[..length]);
    let mut lineage = [0; 8];
    lineage.copy_from_slice(&bytes[..8]);
    let mut changes = [0; 8];
    changes.copy_from_slice(&bytes[8..16]);
    let mut operation = [0; 16];
    operation.copy_from_slice(&bytes[16..]);
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
    foreign[LINEAGE_DOMAIN.len()] = crate::ObjectKind::Metadata.canonical_tag();
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
pub fn seeds() -> Result<Vec<(String, Vec<u8>)>, Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let mut seeds = Vec::new();
    for (name, input) in [
        ("zero", [0; 32]),
        ("maximum", [255; 32]),
        ("distinct", std::array::from_fn(|index| index as u8)),
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

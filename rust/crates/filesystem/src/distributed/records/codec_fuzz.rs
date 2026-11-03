//! Original stream record writers and readers, with independent typed inputs.
use super::*;
use crate::kernel::decoder_fuzz::records::record;

fn fields(input: &[u8]) -> (GenerationLocator, GenerationId, OperationId) {
    let mut bytes = [0; 32];
    let length = input.len().min(bytes.len());
    bytes[..length].copy_from_slice(&input[..length]);
    (
        GenerationLocator {
            lineage: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
            changes: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        },
        GenerationId::new(Digest::from_bytes(bytes)),
        OperationId::from_bytes(bytes[16..].try_into().unwrap()),
    )
}

/// Check exact width, tags and identities through the original decoders.
pub fn check(input: &[u8]) {
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
    );
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
    );
    let mut foreign = lineage;
    foreign[LINEAGE_DOMAIN.len()] = crate::ObjectKind::Metadata.canonical_tag();
    assert!(
        decode_lineage_generation(&foreign).is_err(),
        "foreign lineage object kind admitted"
    );
    for operation in [None, Some(operation)] {
        let encode_gate = |value: &Option<OperationId>| {
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
        );
    }
}

/// Library encoder output with distinct fields, boundary words and gate states.
pub fn seeds() -> Result<Vec<(String, Vec<u8>)>, AuthorityStoreError> {
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
            encode_publication_gate(Some(operation), &mut work)?.to_vec(),
        ));
    }
    seeds.push((
        "publication-gate-free".into(),
        encode_publication_gate(None, &mut work)?.to_vec(),
    ));
    Ok(seeds)
}

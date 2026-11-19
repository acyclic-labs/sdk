//! Generated codec inputs, never a recorded provider fixture. The same checks
//! run against records committed by the original call protocol model.
use super::*;

const KINDS: [&[u8]; 9] = [
    b"request",
    b"hold",
    b"accepted",
    b"chunk",
    b"ok",
    b"error",
    b"cancelled",
    b"indeterminate",
    b"unknown",
];

/// Exercise arbitrary payloads, invalid kinds and session paths through the
/// original decoder and fold. Refusal must leave the reader unchanged.
/// Errors report construction or re-encoding failures for admitted records.
pub fn check(input: &[u8]) -> Result<(), Error> {
    let (&tag, value) = input.split_first().unwrap_or((&0, &[]));
    let mut attributes = vec![Attribute {
        name: if tag & 64 == 0 {
            KIND
        } else {
            "acyclic.call.v1"
        }
        .into(),
        value: KINDS
            .get(usize::from(tag) % KINDS.len())
            .copied()
            .ok_or(Error::Malformed("unknown generated kind"))?
            .into(),
    }];
    if tag & 16 != 0 {
        attributes.push(Attribute {
            name: FENCE.into(),
            value: value.into(),
        });
    }
    let Ok(headers) = RecordHeaders::new(Vec::new(), attributes) else {
        // Oversized generated headers cannot represent an admitted record.
        return Ok(());
    };
    let origin = StreamPath::new("generated/call").map_err(Error::Stream)?;
    let reader = Journal::new(&origin);
    check_record(
        &Record {
            sequence: u64::from(tag & 32 != 0),
            value: value.into(),
            commit_id: CommitId::from_bytes([1; 32]),
            committed_at_micros: 0,
            headers,
            origin,
        },
        &reader,
    )
}

/// Check one provider-committed record against the codec and the current fold.
pub fn check_record(original: &Record, before: &Journal) -> Result<(), Error> {
    if let Ok(entry) = Entry::decode(original) {
        let (kind, value, fence) = match &entry {
            Entry::Request(value) => (b"request".as_slice(), value.clone(), None),
            Entry::Hold(hold) => {
                assert_eq!(hold.encode().as_ref(), original.value.as_ref());
                (b"hold".as_slice(), hold.encode().into(), None)
            }
            Entry::Accepted(accepted) => (
                b"accepted".as_slice(),
                accepted.executor.clone(),
                accepted.fence.as_ref(),
            ),
            Entry::Chunk(value) => (b"chunk".as_slice(), value.clone(), None),
            Entry::Terminal(outcome) => (outcome.kind(), outcome.body(), None),
        };
        let encoded = record(kind, value, fence)?;
        let mut roundtrip = original.clone();
        roundtrip.value = encoded.value;
        roundtrip.headers = encoded.headers;
        assert_eq!(Entry::decode(&roundtrip)?, entry);
    }
    let mut reader = before.clone();
    if reader.apply(original).is_err() {
        assert_eq!(
            &reader, before,
            "refused record changed the original reader"
        );
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn seeds() {
    for branch in [None, Some(CommitId::from_bytes([7; 32]))] {
        for remaining in [None, Some(0), Some(u64::MAX)] {
            let hold = crate::escrow::Escrow::new(remaining).in_branch(branch);
            let encoded = hold.encode();
            let mut input = vec![1]; // Select the hold kind, then its encoded value.
            input.extend_from_slice(&encoded);
            assert!(check(&input).is_ok());
            for end in 0..encoded.len() {
                assert!(crate::escrow::Escrow::decode(&encoded[..end]).is_err());
                assert!(check(&input[..end + 1]).is_ok());
            }
            let mut changed = encoded.to_vec();
            *changed.last_mut().expect("nonempty hold") ^= 1;
            assert!(check(&[&[1][..], &changed].concat()).is_ok());
            changed = encoded.to_vec();
            changed.push(0);
            assert!(crate::escrow::Escrow::decode(&changed).is_err());
            assert!(check(&[&[1][..], &changed].concat()).is_ok());
        }
    }
    for tag in 0..=u8::MAX {
        assert!(check(&[tag, 0xff, 0, b'/']).is_ok());
    }
    // The same retained target must accept inputs larger than a legal header
    // without mistaking admission refusal for a decoder failure.
    let mut oversized = vec![0; 65_537];
    for tag in [16, 16 | 64] {
        oversized[0] = tag;
        assert!(check(&oversized).is_ok());
    }
}

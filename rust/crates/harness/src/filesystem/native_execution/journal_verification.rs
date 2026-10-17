use super::journal_allocation;
use acyclic_fs::MaterializationJournal;

pub(crate) fn lexical_items(input: &[u8]) -> u64 {
    lexical_counts(input).0
}

pub(crate) fn lexical_containers(input: &[u8]) -> u64 {
    lexical_counts(input).1
}

fn lexical_counts(input: &[u8]) -> (u64, u64) {
    // Count lexical items independently of the production admission pass,
    // retaining duplicate object keys and stopping neither at invalid tokens
    // nor at unknown fields: the bound must also cover rejected documents.
    let (mut quoted, mut escaped, mut atom, mut items) = (false, false, false, 0);
    let mut containers = 0;
    for &byte in input {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else {
            match byte {
                b'"' => {
                    quoted = true;
                    atom = false;
                    items += 1;
                }
                b'[' | b'{' => {
                    containers += 1;
                    atom = false;
                    items += 1;
                }
                b']' | b'}' | b':' | b',' | b' ' | b'\n' | b'\r' | b'\t' => atom = false,
                _ => {
                    items += u64::from(!atom);
                    atom = true;
                }
            }
        }
    }
    (items, containers)
}

pub(crate) fn verify_decode(input: &[u8]) -> bool {
    let slots = lexical_items(input).min(lexical_containers(input).saturating_mul(4));
    let bound = journal_allocation::decode_owned_bytes(slots, input.len() as u64, 0).unwrap();
    let mut decoded = None;
    let measured = acyclic_test_support::measure(|| {
        decoded = Some(serde_json::from_slice::<MaterializationJournal>(input));
    });
    assert!(measured.bytes_max as u64 <= bound, "{measured:?} > {bound}");
    let decoded = decoded.unwrap();
    let accepted = decoded.is_ok();
    if let Ok(journal) = decoded {
        let compact = serde_json::to_vec(&journal).unwrap();
        let reread: MaterializationJournal = serde_json::from_slice(&compact).unwrap();
        assert_eq!(journal, reread);
        let mut value = serde_json::to_value(&journal).unwrap();
        value.sort_all_objects();
        let legacy = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            serde_json::to_vec(&super::journal_encoding::Canonical(&journal)).unwrap(),
            legacy
        );
        let reread: MaterializationJournal = serde_json::from_slice(&legacy).unwrap();
        assert_eq!(journal, reread);
    }
    accepted
}

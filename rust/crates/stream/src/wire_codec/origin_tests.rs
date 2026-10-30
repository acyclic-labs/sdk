use super::*;
use prost::Message as _;

fn stored(origin: &str) -> Record {
    Record {
        sequence: 3,
        value: crate::OwnedBytes::from_static(b"value"),
        commit_id: CommitId::from_bytes([4; 32]),
        committed_at_micros: 5,
        headers: Default::default(),
        origin: StreamPath::new(origin).unwrap(),
    }
}

#[test]
fn only_an_inherited_origin_is_spelled_and_each_record_has_one_encoding() {
    let (source, fork) = (
        StreamPath::new("source").unwrap(),
        StreamPath::new("fork").unwrap(),
    );
    // A record of the followed path costs no byte for its origin.
    let own = record_wire(stored("fork"), Some(&fork));
    let mut unspelled = own.clone();
    unspelled.origin = "fork".into();
    assert_eq!(own.origin, "");
    assert_eq!(
        own.encoded_len() + "fork".len() + 2,
        unspelled.encoded_len()
    );
    assert_eq!(record(own, Some(&fork)), Ok(stored("fork")));
    // An inherited record names its segment and round-trips exactly.
    let inherited = record_wire(stored("source"), Some(&fork));
    assert_eq!(inherited.origin, "source");
    assert_eq!(record(inherited.clone(), Some(&fork)), Ok(stored("source")));
    // Spelling the followed path, or omitting an origin with no followed
    // path, is a decode error rather than a second encoding.
    assert_eq!(
        record(inherited.clone(), Some(&source)),
        Err(StreamError::InvalidArgument)
    );
    assert_eq!(
        record(record_wire(stored("source"), Some(&source)), None),
        Err(StreamError::InvalidArgument)
    );
    let mut malformed = inherited;
    malformed.origin = "/not//canonical".into();
    assert!(record(malformed, Some(&fork)).is_err());
}

#[test]
fn a_committed_mutation_owns_its_records_and_spells_no_origin() {
    let path = StreamPath::new("source").unwrap();
    let committed = |records| wire::CommittedMutation {
        mutation: Some(wire::committed_mutation::Mutation::Append(
            wire::CommittedAppend {
                path: path.to_string(),
                start: 3,
                end: 4,
                tail: 4,
                records,
            },
        )),
    };
    let own = record_wire(stored("source"), Some(&path));
    assert!(matches!(
        committed_mutation(committed(vec![own])),
        Ok(CommittedMutation::Append(append)) if append.records == [stored("source")]
    ));
    let foreign = record_wire(stored("other"), Some(&path));
    assert_eq!(
        committed_mutation(committed(vec![foreign])).err(),
        Some(StreamError::InvalidArgument)
    );
}

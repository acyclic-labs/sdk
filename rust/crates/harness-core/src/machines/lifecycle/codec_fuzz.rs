//! Generated-input oracles for the same plan/binding admission used at first use.
//! These do not authenticate the selected path or certify physical placement.
use super::*;

/// Exercise both original decoders, their canonical fixed points and source
/// association refusals. Generated envelope metadata is not a captured receipt;
/// positive corpus values must come from the original stream producer.
pub fn check(bytes: &[u8]) {
    let record = acyclic_stream::Record {
        sequence: 0,
        value: bytes.to_vec().into(),
        commit_id: Default::default(),
        committed_at_micros: 0,
        headers: Default::default(),
        origin: acyclic_stream::StreamPath::new("machine-codec-input".to_owned())
            .expect("fixed generated-input path"),
    };
    if admitted(&record, |record| ProvisionPlan::decode("machine", record)) {
        assert!(ProvisionPlan::decode("foreign", &record).is_err());
    }
    let mut binding = record;
    binding.sequence = 1;
    if admitted(&binding, |record| MachineBinding::decode("machine", record)) {
        assert!(MachineBinding::decode("foreign", &binding).is_err());
    }
}

/// Require a positive check with the original producer envelope. Unlike an
/// arbitrary fuzz input, a captured plan or binding must actually be admitted.
pub fn check_record(target: &str, record: &acyclic_stream::Record) {
    let foreign = if target.is_empty() { "foreign" } else { "" };
    let accepted = match record.sequence {
        0 => {
            assert!(ProvisionPlan::decode(foreign, record).is_err());
            admitted(record, |record| ProvisionPlan::decode(target, record))
        }
        1 => {
            assert!(MachineBinding::decode(foreign, record).is_err());
            admitted(record, |record| MachineBinding::decode(target, record))
        }
        _ => false,
    };
    assert!(
        accepted,
        "original Machine producer record was not admitted"
    );
}

fn admitted<T: Serialize>(
    record: &acyclic_stream::Record,
    decode: impl Fn(&acyclic_stream::Record) -> Result<T>,
) -> bool {
    let Ok(value) = decode(record) else {
        return false;
    };
    let encoded = serde_json::to_vec(&value).expect("admitted Machine record encodes");
    let mut restored = record.clone();
    restored.value = encoded.clone().into();
    let value = decode(&restored).expect("canonical Machine record retains admission");
    assert_eq!(
        serde_json::to_vec(&value).expect("canonical Machine record encodes"),
        encoded
    );
    for sequence in [0, 1, u64::MAX] {
        if sequence != record.sequence {
            restored.sequence = sequence;
            assert!(decode(&restored).is_err());
        }
    }
    true
}

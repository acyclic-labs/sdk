//! Query inputs use their canonical serde types, without a second codec.
use acyclic_harness::{
    client::QueryViewLimits,
    task_volume::{Component, Cursor, FileKey, Generation, Query},
};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

fn round_trip<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: T) {
    assert_eq!(
        serde_json::from_slice::<T>(&serde_json::to_vec(&value).unwrap()).unwrap(),
        value
    );
}

pub fn check_query(query: &Query, bytes: &[u8]) {
    assert_eq!(
        &serde_json::from_slice::<Query>(bytes).expect("valid generated query must decode"),
        query
    );
}

pub fn exercise(bytes: &[u8]) {
    // Arbitrary JSON reaches every public input's actual serde implementation.
    if let Ok(value) = serde_json::from_slice::<Query>(bytes) {
        round_trip(value);
    }
    if let Ok(value) = serde_json::from_slice::<Cursor>(bytes) {
        round_trip(value);
    }
    if let Ok(value) = serde_json::from_slice::<Generation>(bytes) {
        round_trip(value);
    }
    if let Ok(value) = serde_json::from_slice::<QueryViewLimits>(bytes) {
        round_trip(value);
    }
    if let Ok(key) = serde_json::from_slice::<FileKey>(bytes) {
        assert_eq!(FileKey::parse(&key.path()).unwrap(), key);
        round_trip(key);
    }
    // Generate success on every iteration; an always-rejecting decoder fails.
    let mut number = [0; 8];
    for (target, value) in number.iter_mut().zip(bytes) {
        *target = *value;
    }
    let turn = u64::from_le_bytes(number);
    let task: Component = "task".to_owned().try_into().unwrap();
    for query in [
        Query::Root,
        Query::Inputs,
        Query::Task(task.clone()),
        Query::Turn {
            task: task.clone(),
            turn,
        },
        Query::File(FileKey::parse(&format!(".tasks/task/turns/{turn}/text.md")).unwrap()),
    ] {
        let encoded = serde_json::to_vec(&query).unwrap();
        check_query(&query, &encoded);
    }
    round_trip(QueryViewLimits {
        files: bytes.len(),
        segments: bytes.len(),
        bytes: turn,
    });
}

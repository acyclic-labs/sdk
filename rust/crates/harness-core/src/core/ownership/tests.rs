//! Allocation measurements are independent of the structural accounting walk.
use super::OwnedBytes;
use crate::{Error, Result, conversation::*, core::*, interaction::*};
use acyclic_test_support::measure;
use proptest::prelude::*;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    mem::size_of,
};

fn covers<T: OwnedBytes>(value: T) -> Result<(u64, u64)> {
    let mut charged = Ok(0);
    let walk = measure(|| charged = value.owned_heap_bytes());
    assert_eq!(walk.count_total, 0, "accounting allocated");
    let charged = charged?;
    let freed = measure(|| drop(value));
    assert!(freed.bytes_current <= 0);
    let actual = freed.bytes_current.unsigned_abs();
    assert!(
        charged >= actual,
        "{}: walk {charged}, allocator {actual}",
        std::any::type_name::<T>()
    );
    Ok((charged, actual))
}
fn fixture<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let mut value: Value =
        serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    if let Some(object) = value.as_object_mut() {
        object.remove("contract");
    }
    serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))
}
macro_rules! fixture {
    ($ty:ty, $file:literal) => {
        fixture::<$ty>(include_bytes!(concat!(
            "../../../../harness/fixtures/v2/",
            $file,
            ".json"
        )))
    };
}

#[repr(align(256))]
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct AlignedKey(u64);
impl OwnedBytes for AlignedKey {
    fn owned_heap_bytes(&self) -> Result<u64> {
        Ok(0)
    }
}

#[test]
fn over_aligned_tree_storage_includes_node_padding() -> Result<()> {
    let mut map = BTreeMap::new();
    let mut set = BTreeSet::new();
    map.insert(AlignedKey(1), ());
    set.insert(AlignedKey(1));
    covers(map)?;
    covers(set)?;
    Ok(())
}

fn json_values() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|n| json!(n)),
        "[a-z]{0,128}".prop_map(Value::String),
    ]
    .prop_recursive(3, 64, 8, |value| {
        prop_oneof![
            prop::collection::vec(value.clone(), 0..8).prop_map(Value::Array),
            prop::collection::btree_map("[a-z]{0,16}", value, 0..8)
                .prop_map(|map| Value::Object(map.into_iter().collect())),
        ]
    })
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn boxed_inline_storage_is_counted(value in any::<u64>()) {
        let (charged, _) = covers(Box::new(value)).map_err(|error| TestCaseError::fail(error.to_string()))?;
        // GlobalAlloc permits allocation elision. covers still checks that the
        // charge covers observed storage; the layout must be charged even when
        // the optimizer removes the physical allocation.
        prop_assert_eq!(charged, size_of::<u64>() as u64);
    }
    #[test]
    fn boxed_text_storage_is_counted(text in "[a-z]{0,128}") {
        let expected = text.len() as u64;
        let (charged, _) = covers(text.into_boxed_str()).map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(charged, expected);
    }
    #[test]
    fn boxed_slice_storage_is_counted(values in prop::collection::vec(any::<u64>(), 0..128)) {
        let expected = (values.len() * size_of::<u64>()) as u64;
        let (charged, _) = covers(values.into_boxed_slice()).map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(charged, expected);
    }
    #[test]
    fn spare_capacity_is_counted(text in "[a-z]{0,32}", spare in 0..4096usize) {
        let mut text = text;
        text.reserve_exact(spare);
        let mut values = Vec::with_capacity(spare);
        values.push(Some(Box::new(text)));
        let mut queue = VecDeque::with_capacity(spare);
        queue.push_back(values);
        covers(queue).map_err(|error| TestCaseError::fail(error.to_string()))?;
        covers(Vec::<String>::with_capacity(spare)).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
    #[test]
    fn sparse_tree_nodes_are_counted(entries in 1..96usize, retained in 0..96usize) {
        let mut map = BTreeMap::new();
        let mut set = BTreeSet::new();
        for index in 0..entries {
            map.insert(format!("key-{index}"), Vec::<String>::with_capacity(index));
            set.insert(format!("key-{index}"));
        }
        while map.len() > retained { map.pop_first(); }
        while set.len() > retained { set.pop_last(); }
        covers(map).map_err(|error| TestCaseError::fail(error.to_string()))?;
        covers(set).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
    #[test]
    fn json_storage_is_counted(value in json_values()) { covers(value).map_err(|error| TestCaseError::fail(error.to_string()))?; }
    #[test]
    fn typed_dto_storage_is_counted(payload in json_values(), spare in 0..128usize) {
        for value in dto_corpus(payload, spare).map_err(|error| TestCaseError::fail(error.to_string()))? { covers(value).map_err(|error| TestCaseError::fail(error.to_string()))?; }
    }
}

fn dto_corpus(payload: Value, spare: usize) -> Result<Vec<Box<dyn OwnedBytes>>> {
    let file = fixture!(FileRef, "file-ref")?;
    let effect = EffectState {
        provider: "provider".into(),
        guarantee: EffectGuarantee::ExactlyOnce,
        effect_kind: "send".into(),
        request: Box::new(file.clone()),
        request_digest: [0; 32],
        result_schema: file,
        result_schema_digest: [0; 32],
        status: EffectStatus::Planned,
        planning: None,
        attempts: Vec::with_capacity(spare),
    };
    let output: Vec<Box<dyn OwnedBytes>> = vec![
        Box::new(effect),
        Box::new(payload),
        Box::new(fixture!(ConversationMessage, "conversation-message")?),
        Box::new(fixture!(InteractionResolution, "interaction-resolution")?),
        Box::new(fixture!(ExtensionRecord, "extension-record")?),
        Box::new(fixture!(
            ExtensionStateMigration,
            "extension-state-migration"
        )?),
        Box::new(fixture!(ExtensionConfiguration, "extension-configuration")?),
        Box::new(fixture!(ExtensionAdmission, "extension-admission")?),
    ];
    Ok(output)
}
#[test]
fn json_ownership_preserves_the_codec_depth_limit() {
    let mut value = Value::Null;
    for _ in 0..256 {
        value = Value::Array(vec![value]);
    }
    assert!(value.owned_heap_bytes().is_err());
}

use super::*;
use crate::conversation::{
    BoundedContentFuture, FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef,
};
use crate::{AgentId, resources::ProviderRef};
use proptest::prelude::*;
use serde_json::{Value, json};

struct Resolver(Vec<u8>);
impl ContentResidencyVerifier for Resolver {
    fn read<'a>(
        &'a self,
        _: &'a FileRef,
        _: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        BoundedContentFuture::new(io, || async {
            Ok(acyclic_stream::OwnedBytes::copy_from_slice(&self.0))
        })
    }
}

fn reference(bytes: &[u8]) -> FileRef {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("fixture".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            &mut None,
        )
        .unwrap(),
        "invocation.json".into(),
        "v1".into(),
        FileDescriptor::from_bytes(bytes, "application/json", &mut None).unwrap(),
        "fixture".into(),
        &mut None,
    )
    .unwrap()
}

fn check_bytes(bytes: Vec<u8>, must_reject: bool) {
    let reference = reference(&bytes);
    let resolver = Resolver(bytes);
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(2_000_000, 4 * 1024 * 1024),
        &acyclic_fs::CancellationToken::new(),
    )
    .unwrap();
    let io = PreparationIo::from_native_work(&work).unwrap();
    let baseline = work.live_allocation_bytes();
    let result = futures::executor::block_on(artifacts::read_tool_call(
        &resolver,
        &reference,
        4096,
        Some(&io),
    ));
    if must_reject {
        assert!(result.is_err());
    }
    match result {
        Ok(call) => {
            let custody = io
                .take_native_custody(call.payload_bytes().unwrap())
                .unwrap();
            drop(call);
            drop(custody);
        }
        Err(error) => drop(error),
    }
    let result = futures::executor::block_on(artifacts::read_reasoning(
        &resolver,
        &reference,
        4096,
        Some(&io),
    ));
    match result {
        Ok(block) => {
            let custody = io
                .take_native_custody(block.payload_bytes().unwrap())
                .unwrap();
            drop(block);
            drop(custody);
        }
        Err(error) => drop(error),
    }
    // Check while the I/O scope is still alive: scope destruction must not hide a leak.
    assert_eq!(work.live_allocation_bytes(), baseline);
    assert_eq!(io.owned_bytes().unwrap(), 0);
}

fn check_rejected(value: Value) {
    check_bytes(serde_json::to_vec(&value).unwrap(), true);
}

fn arbitrary_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|n| json!(n)),
        ".{0,32}".prop_map(Value::String)
    ]
    .prop_recursive(4, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map(".{0,16}", inner, 0..4)
                .prop_map(|map| Value::Object(map.into_iter().collect())),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn every_rejected_record_releases_all_source_and_field_custody(
        text in ".{0,128}", malformed in 0usize..8, nesting in 0usize..8,
    ) {
        let mut value = json!({"invocation":{"call_id":"call", "name":"echo", "arguments":{"payload":text}},"metadata":{"signature":text}});
        match malformed {
            0 => { value = json!([value]); },
            1 => {value.as_object_mut().unwrap().remove("metadata");},
            2 => {value.as_object_mut().unwrap().remove("invocation");},
            3 => {value["invocation"] = json!([text]);},
            4 => {value["invocation"]["call_id"] = Value::Null;},
            5 => {value["invocation"]["name"] = json!(7);},
            6 => {value["invocation"].as_object_mut().unwrap().remove("arguments");},
            _ => {value["metadata"]["unsafe"] = json!(u64::MAX);},
        }
        for _ in 0..nesting {value = json!({"wrapper":value});}
        check_rejected(value);
    }
}

#[test]
fn removed_metadata_rejection_does_not_leave_the_decoded_record_retained() {
    check_rejected(
        json!({"invocation":{"call_id":"call", "name":"echo", "arguments":{"payload":"x".repeat(1000)}}, "extra":"y".repeat(1000)}),
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn guarded_joins_conserve_the_original_ledger(bytes in 0u64..8192, additional in 0u64..8192) {
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(100_000, 4*1024*1024),
            &acyclic_fs::CancellationToken::new(),
        ).unwrap();
        let io = PreparationIo::from_native_work(&work).unwrap();
        let baseline = work.live_allocation_bytes();
        let mut hold = io.reserve_decode(bytes).unwrap();
        let incoming = io.reserve_decode(additional).unwrap();
        prop_assert_eq!(hold.bytes() + incoming.bytes(), bytes + additional);
        prop_assert_eq!(work.live_allocation_bytes() - baseline, bytes + additional);
        hold.absorb(incoming).unwrap();
        prop_assert_eq!(hold.bytes(), bytes + additional);
        prop_assert!(hold.substitute(bytes + additional + 1).is_err());
        prop_assert_eq!(hold.bytes(), bytes + additional);
        drop(hold);
        prop_assert_eq!(work.live_allocation_bytes(), baseline);
    }
}

#[test]
fn premature_retention_negative_control_is_visible_before_io_scope_drop() {
    let bytes = br#"{"invocation":{"arguments":{"payload":"charged"}}}"#;
    let file = reference(bytes);
    let resolver = Resolver(bytes.to_vec());
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(100_000, 4 * 1024 * 1024),
        &acyclic_fs::CancellationToken::new(),
    )
    .unwrap();
    let io = PreparationIo::from_native_work(&work).unwrap();
    let baseline = work.live_allocation_bytes();
    let (value, hold) =
        futures::executor::block_on(artifacts::read_value(&resolver, &file, 4096, Some(&io)))
            .unwrap();
    // Broken original ordering: release the guard before discovering missing metadata.
    hold.unwrap().into_retained().unwrap();
    assert!(value.get("metadata").is_none());
    drop(value);
    assert_ne!(work.live_allocation_bytes(), baseline);
    io.release(io.owned_bytes().unwrap()).unwrap();
    assert_eq!(work.live_allocation_bytes(), baseline);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn arbitrary_wire_inputs_leave_no_custody_after_their_final_reader(bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
        check_bytes(bytes, false);
    }

    #[test]
    fn arbitrary_saved_field_trees_conserve_custody_on_success_or_rejection(
        arguments in arbitrary_value(), metadata in arbitrary_value(),
        field in prop::option::of(0usize..6),
    ) {
        let mut value = json!({"invocation":{"call_id":"call", "name":"echo", "arguments":arguments}, "metadata":metadata});
        if let Some(field) = field {
            match field {
                0 => {value["invocation"]["call_id"] = value["metadata"].clone();},
                1 => {value["invocation"]["name"] = value["metadata"].clone();},
                2 => {value["invocation"].as_object_mut().unwrap().remove("arguments");},
                3 => {value["invocation"] = value["metadata"].clone();},
                4 => {value.as_object_mut().unwrap().remove("metadata");},
                _ => {value = json!([value]);},
            }
        }
        check_bytes(serde_json::to_vec(&value).unwrap(), false);
    }
}

#[test]
fn canonical_reasoning_keeps_only_its_field_custody() {
    use crate::model::{
        ModelEvent, ReasoningBlock, ReasoningKind, ReasoningOrigin, ReasoningOutcome,
    };
    let event = ModelEvent::ReasoningBlock {
        block: ReasoningBlock {
            kind: ReasoningKind::Encrypted,
            text: Some("summary".into()),
            origin: ReasoningOrigin {
                provider: "responses".into(),
                model: "fixture".into(),
                credential_scope: Some([7; 32]),
            },
            continuity: b"{ \"type\": \"reasoning\", \"encrypted_content\": \"opaque\" }".to_vec(),
            outcome: ReasoningOutcome::PreservedOpaque,
        },
    };
    check_bytes(serde_json::to_vec(&event).unwrap(), true);
}

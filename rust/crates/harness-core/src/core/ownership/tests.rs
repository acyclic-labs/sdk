//! Allocation measurements are independent of the structural accounting walk.
use super::OwnedBytes;
use crate::{
    Error, Result, conversation::*, core::*, fork::*, interaction::*, resource_preparation::*,
    runtime::TaskAdmissionRecord,
};
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

#[test]
fn every_admission_root_and_its_fields_have_accounting() {
    fn covered<T: OwnedBytes>() {}
    macro_rules! covered { ($($ty:ty),* $(,)?) => {$(covered::<$ty>();)*}; }
    covered!(
        Authority, Scope, RecordedScope, EventReference, Event, EventPayload,
        Command, Action, EffectStatus, EffectAttestation, EffectState,
        ExtensionRecord, ExtensionStateMigration, ExtensionDependency,
        ExtensionConfiguration, ExtensionAdmission, crate::Capabilities,
        ForkSeed, ForkRequest, ForkReport, ForkPreparation, ForkSelection,
        ForkOmission, CapturedResource, Capture, ResourceRevision,
        ForkHistoryPolicy, SummaryForkSelection, SummaryForkCapture,
        SharedGrant, ReferenceGrant, AttestedBoundary,
        crate::merge::ProjectMergeReceipt, crate::merge::ProviderJoinProof,
        ConversationMessage, FileRef, FileDescriptor, VolumeRef, VolumeOwner,
        ReferencedAttachments, Attachment, ModelContextSelection,
        InteractionTicket, InteractionKind, ApprovalBinding,
        InteractionResolution, InteractionOutcome,
        TaskAdmissionRecord, crate::runtime::ExecutionPlacement,
        crate::runtime::TaskRunLimits, ResourcePreparationRequest,
        ResourceCreationProofRef, ResourceCreationOrigin,
        crate::source_authorization::ProjectSourceAuthorization,
        crate::registry::ComponentIdentity, crate::workflow::MachineIdentity,
        crate::workflow::WorkflowCommand, crate::scheduler::LeaseFence,
        crate::resources::ProviderRef, crate::resources::ResourceRef,
        crate::resources::GenerationRef, crate::resources::StreamRef,
        crate::resources::ContextRef, crate::resources::ArtifactRef,
        crate::resources::CheckpointRef, crate::resources::SandboxRef,
        crate::store::HistoryReadLimits, Limits, Value,
        BTreeMap<crate::EffectId, EffectState>,
        BTreeMap<Authority, ForkSeed>, BTreeSet<(String, Vec<u8>)>,
        VecDeque<Event>, Vec<(ExtensionDependency, [u8;32])>,
    );
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

#[test]
fn tree_insertion_bounds_cover_root_and_cascading_splits() -> Result<()> {
    fn insert<K: Ord, V>(map: &mut BTreeMap<K, V>, key: K, value: V) -> Result<()> {
        let reserved = super::walk::tree_insertion_bytes::<(K, V)>(map.len())?;
        let actual = measure(|| assert!(map.insert(key, value).is_none())).bytes_total;
        assert!(
            reserved >= actual,
            "tree insertion: reserved {reserved}, allocated {actual}"
        );
        Ok(())
    }
    let seed = fixture!(ForkSeed, "fork-seed")?;
    let mut forks = BTreeMap::new();
    let mut aligned = BTreeMap::new();
    let mut set = BTreeSet::new();
    for index in 0..256_u64 {
        let child = Authority {
            kind: AggregateKind::Conversation,
            id: format!("child-{index:04}"),
        };
        insert(&mut forks, child, seed.clone())?;
        insert(&mut aligned, AlignedKey(index), [0_u8; 32])?;
        let reserved = super::walk::tree_insertion_bytes::<u64>(set.len())?;
        let actual = measure(|| assert!(set.insert(index))).bytes_total;
        assert!(
            reserved >= actual,
            "set insertion: reserved {reserved}, allocated {actual}"
        );
    }
    // Sparse trees can have a different height and split pattern at the same len.
    while forks.len() > 12 {
        forks.pop_first();
    }
    for index in 256..512 {
        let child = Authority {
            kind: AggregateKind::Conversation,
            id: format!("child-{index:04}"),
        };
        insert(&mut forks, child, seed.clone())?;
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn fork_application_funds_its_map_node_before_inserting() -> Result<()> {
    let seed = fixture!(ForkSeed, "fork-seed")?;
    let old_reservation = super::owned_sum([
        seed.owned_heap_bytes()?,
        seed.child.id.len() as u64,
        size_of::<(Authority, ForkSeed)>() as u64,
    ])?;
    let payload = EventPayload::ForkPublished {
        seed: Box::new(seed),
    };
    let mut reducer = crate::core::tests::reducer_fixture();
    let cancellation = acyclic_fs::CancellationToken::new();
    let limited = crate::filesystem::PreparationIo::from_budget(
        acyclic_fs::performance::WorkBudget {
            peak_allocation_bytes: old_reservation,
            ..acyclic_fs::performance::BOUNDARY_BUDGET
        },
        &cancellation,
    )?;
    let mut observer = &limited;
    assert!(
        reducer
            .apply_payload_in_quota(
                &payload,
                1,
                &mut CodecQuota::new(&mut Some(&mut observer), 0)?
            )
            .is_err()
    );
    assert!(reducer.projection.forks.is_empty());
    let io = crate::filesystem::PreparationIo::from_budget(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &cancellation,
    )?;
    let mut observer = &io;
    let mut work: Option<&mut dyn HistoryIo> = Some(&mut observer);
    let mut quota = CodecQuota::new(&mut work, 0)?;
    reducer.apply_payload_in_quota(&payload, 1, &mut quota)?;
    quota.transfer();
    drop(quota);
    assert_eq!(reducer.projection.forks.len(), 1);
    let forks = std::mem::take(&mut reducer.projection.forks);
    let actual = measure(|| drop(forks)).bytes_current.unsigned_abs();
    assert!(
        io.owned_bytes()? >= actual,
        "fork map: reserved {}, allocated {actual}",
        io.owned_bytes()?
    );
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
    let planning = Event {
        revision: 1,
        operation_id: crate::OperationId::from_bytes([1; 16]),
        intent_digest: [0; 32],
        scope: RecordedScope::from_wire(
            "planning".into(),
            crate::Capabilities::default(),
            "issuer".into(),
            None,
        ),
        attestation: Vec::with_capacity(spare),
        causal_parent: None,
        payload: EventPayload::EffectDispatched {
            effect_id: crate::EffectId::from_bytes([1; 16]),
            attempt_id: crate::EffectAttemptId::from_bytes([2; 16]),
        },
    };
    let effect = EffectState {
        provider: "provider".into(),
        guarantee: EffectGuarantee::ExactlyOnce,
        effect_kind: "send".into(),
        request: Box::new(file.clone()),
        request_digest: [0; 32],
        result_schema: file.clone(),
        result_schema_digest: [0; 32],
        status: EffectStatus::Planned,
        planning: Some(Box::new(planning)),
        attempts: Vec::with_capacity(spare),
    };
    let mut admission = fixture!(TaskAdmissionRecord, "task-admission")?;
    admission.input = payload.clone();
    admission.resource_proofs.reserve_exact(spare);
    let request = ResourcePreparationRequest {
        provider: file.volume().provider().clone(),
        child: admission,
        creation_grant: String::with_capacity(spare),
        maximum_resources: 1,
        maximum_work: 1,
        maximum_bytes: 1,
        payload,
        command: Some(crate::workflow::WorkflowCommand {
            operation_id: crate::OperationId::from_bytes([1; 16]),
            kind: "prepare".into(),
            payload: file.clone(),
        }),
        source_checkpoint: Some(file.clone()),
        input_files: vec![file],
        read_authorization: None,
    };
    let mut output: Vec<Box<dyn OwnedBytes>> = vec![
        Box::new(effect),
        Box::new(request),
        Box::new(fixture!(ForkSeed, "fork-seed")?),
        Box::new(fixture!(ForkRequest, "fork-request")?),
        Box::new(fixture!(ResourceRevision, "resource-revision")?),
        Box::new(fixture!(ReferenceGrant, "reference-grant")?),
        Box::new(fixture!(
            crate::merge::ProjectMergeReceipt,
            "project-merge-receipt"
        )?),
        Box::new(fixture!(ConversationMessage, "conversation-message")?),
        Box::new(fixture!(InteractionResolution, "interaction-resolution")?),
        Box::new(fixture!(ExtensionRecord, "extension-record")?),
        Box::new(fixture!(
            ExtensionStateMigration,
            "extension-state-migration"
        )?),
        Box::new(fixture!(ExtensionConfiguration, "extension-configuration")?),
        Box::new(fixture!(ExtensionAdmission, "extension-admission")?),
        Box::new(fixture!(
            crate::runtime::ExecutionPlacement,
            "execution-placement"
        )?),
    ];
    for payload in event_payloads()? {
        output.push(Box::new(
            serde_json::from_value::<EventPayload>(payload)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        ));
    }
    for action in actions()? {
        let action = serde_json::from_value::<Action>(action)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        output.push(Box::new(crate::core::tests::command(
            crate::OperationId::from_bytes([1; 16]),
            0,
            action,
        )));
    }
    Ok(output)
}
fn wire(file: &str) -> Result<Value> {
    match file {
        "file" => fixture!(Value, "file-ref"),
        "seed" => fixture!(Value, "fork-seed"),
        "merge" => fixture!(Value, "project-merge-receipt"),
        "message" => fixture!(Value, "conversation-message"),
        "record" => fixture!(Value, "extension-record"),
        "migration" => fixture!(Value, "extension-state-migration"),
        "configuration" => fixture!(Value, "extension-configuration"),
        "resolution" => fixture!(Value, "interaction-resolution"),
        _ => Err(Error::Invalid("unknown fixture".into())),
    }
}
fn observation() -> Result<Value> {
    Ok(
        json!({"provider":"provider", "effect_id":"11111111-1111-1111-1111-111111111111", "attempt_id":"11111111-1111-1111-1111-111111111111", "request_digest":([1_u8;32]), "guarantee":"exactly_once", "result_schema_digest":([1_u8;32]), "status":{"state":"succeeded","result":wire("file")?}, "proof":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="}),
    )
}
fn ticket() -> Result<Value> {
    Ok(
        json!({"id":"11111111-1111-1111-1111-111111111111", "kind":"question", "request":wire("file")?, "deadline_unix_ms":null, "approval":null}),
    )
}
fn event_payloads() -> Result<Vec<Value>> {
    let id = "11111111-1111-1111-1111-111111111111";
    Ok(vec![
        json!({"kind":"lifecycle_transitioned","from":"pending","to":"active","reason":"reason"}),
        json!({"kind":"custom","record":wire("record")?}),
        json!({"kind":"extension_state_migrated","migration":wire("migration")?}),
        json!({"kind":"extensions_selected","previous":[],"selected":[],"configurations":[wire("configuration")?]}),
        json!({"kind":"extension_configured","previous":wire("configuration")?,"record":wire("configuration")?}),
        json!({"kind":"effect_planned","effect_id":id,"provider":"provider","guarantee":"exactly_once","effect_kind":"send","request":wire("file")?,"request_digest":([1_u8;32]),"result_schema":wire("file")?,"result_schema_digest":([1_u8;32])}),
        json!({"kind":"effect_dispatched","effect_id":id,"attempt_id":id}),
        json!({"kind":"effect_resolved","observation":observation()?}),
        json!({"kind":"fork_published","seed":wire("seed")?}),
        json!({"kind":"project_merge_published","receipt":wire("merge")?}),
        json!({"kind":"conversation_bound","agent":id}),
        json!({"kind":"conversation_message_appended","message":wire("message")?}),
        json!({"kind":"model_context_selected","selection":{"conversation_revision":1,"message_ids":[id],"checkpoint":wire("file")?}}),
        json!({"kind":"interaction_opened","ticket":ticket()?}),
        json!({"kind":"interaction_resolved","resolution":wire("resolution")?}),
    ])
}
fn actions() -> Result<Vec<Value>> {
    let id = "11111111-1111-1111-1111-111111111111";
    Ok(vec![
        json!({"kind":"transition_lifecycle","to":"active","reason":"reason"}),
        json!({"kind":"append_custom","schema":"test.schema","version":1,"content":wire("file")?}),
        json!({"kind":"migrate_extension_state","name":"test.schema","previous":{"authority":{"kind":"agent","id":"agent"},"revision":1},"to_version":2,"content":wire("file")?}),
        json!({"kind":"select_extensions","roots":[]}),
        json!({"kind":"configure_extension","extension":{"name":"test.schema","version":1},"content":wire("file")?}),
        json!({"kind":"plan_effect","effect_id":id,"provider":"provider","guarantee":"exactly_once","effect_kind":"send","request":wire("file")?,"result_schema":wire("file")?}),
        json!({"kind":"mark_effect_dispatched","effect_id":id,"attempt_id":id}),
        json!({"kind":"resolve_effect","observation":observation()?}),
        json!({"kind":"publish_fork","seed":wire("seed")?}),
        json!({"kind":"publish_project_merge","receipt":wire("merge")?}),
        json!({"kind":"bind_conversation","agent":id}),
        json!({"kind":"append_conversation_message","message":wire("message")?}),
        json!({"kind":"select_model_context","selection":{"conversation_revision":1,"message_ids":[id],"checkpoint":wire("file")?}}),
        json!({"kind":"open_interaction","ticket":ticket()?}),
        json!({"kind":"resolve_interaction","resolution":wire("resolution")?}),
    ])
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn checkpoint_copy_fits_the_eight_mib_peak_bound() -> Result<()> {
    for count in [8, 64] {
        let mut reducer = crate::core::tests::reducer_fixture();
        for revision in 0..count {
            reducer.apply(&crate::core::tests::command(
                crate::core::tests::operation(revision),
                u64::from(revision),
                Action::AppendCustom {
                    schema: "example.message".into(),
                    version: 1,
                    content: Box::new(crate::core::tests::request_file(br#"{"text":"hello"}"#)?),
                },
            ))?;
        }
        let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
        let io = crate::filesystem::PreparationIo::from_budget(
            acyclic_fs::performance::WorkBudget {
                peak_allocation_bytes: 8 * 1024 * 1024,
                ..acyclic_fs::performance::BOUNDARY_BUDGET
            },
            &acyclic_fs::CancellationToken::new(),
        )?;
        let mut observer = &io;
        let copied = snapshot.copy(&mut Some(&mut observer))?;
        assert_eq!(copied, snapshot);
        let actual = measure(|| drop(copied)).bytes_current.unsigned_abs();
        assert!(io.owned_bytes()? >= actual);
        assert!(io.owned_bytes()? < 8 * 1024 * 1024);
        println!(
            "checkpoint events={count}, bytes={}, retained bytes={}, allocator bytes={actual}",
            serde_json::to_vec(&snapshot)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .len(),
            io.owned_bytes()?
        );
    }
    Ok(())
}

#[test]
fn json_ownership_preserves_the_codec_depth_limit() {
    let mut value = Value::Null;
    for _ in 0..256 {
        value = Value::Array(vec![value]);
    }
    assert!(value.owned_heap_bytes().is_err());
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn temporary_effect_projection_releases_map_and_planning_custody() -> Result<()> {
    let effect_id = crate::EffectId::from_bytes([9; 16]);
    let mut reducer = crate::core::tests::reducer_fixture();
    reducer.apply(&crate::core::tests::command(
        crate::core::tests::operation(1),
        0,
        Action::PlanEffect {
            effect_id,
            provider: "example.provider".into(),
            guarantee: EffectGuarantee::ExactlyOnce,
            effect_kind: "example.send".into(),
            request: Box::new(crate::core::tests::request_file(br#"{"value":1}"#)?),
            result_schema: Box::new(crate::core::tests::schema_file(&json!({"type":"object"}))?),
        },
    ))?;
    let events = reducer.events_after(0, 1)?;
    let io = crate::filesystem::PreparationIo::from_budget(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &acyclic_fs::CancellationToken::new(),
    )?;
    let mut observer = &io;
    let mut work: Option<&mut dyn HistoryIo> = Some(&mut observer);
    for invalid in [false, false, true, false] {
        let authority = work
            .as_deref_mut()
            .unwrap()
            .clone_authority(&reducer.authority)?;
        let verifier = reducer.authority_verifier.copy(&mut work)?;
        let history = if invalid {
            vec![events[0].clone(), events[0].clone()]
        } else {
            events.clone()
        };
        let projected = Reducer::project_effect_with_retained_quota(
            authority, verifier, effect_id, &history, &mut work,
        );
        if invalid {
            assert!(projected.is_err());
        } else {
            let (effect, retained) = projected?;
            assert_eq!(Some(&effect), reducer.effect(effect_id));
            assert_eq!(io.owned_bytes()?, retained, "temporary storage leaked");
            let actual = measure(|| drop(effect)).bytes_current.unsigned_abs();
            assert!(retained >= actual);
            work.as_deref_mut().unwrap().release_owned(retained)?;
        }
        assert_eq!(
            io.owned_bytes()?,
            0,
            "projection custody outlived its owner"
        );
    }
    Ok(())
}

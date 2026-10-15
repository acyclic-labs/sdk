use super::*;
use crate::{
    AgentId, TaskId,
    conversation::{VolumeClass, VolumeOwner},
    resources::ProviderRef,
};
use proptest::prelude::*;

fn intent(labels: &[String], key: &[u8], owner: u8) -> Intent {
    let provider =
        ProviderRef::new("test".into(), "filesystem".into(), "1".into(), &mut None).unwrap();
    Intent {
        task: TaskId::from_bytes([1; 16]),
        original_fence: crate::scheduler::LeaseFence {
            reservation_id: "reservation".into(),
            placement: "worker".into(),
        },
        operation_directory: "/operations".into(),
        manifest: NativeViewManifest {
            options: NativeViewOptions {
                root: "/root".into(),
                maximum_volumes: 16,
                work_per_volume: acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
            },
            volumes: labels
                .iter()
                .map(|label| NativeVolumeMapping {
                    volume: VolumeRef::new(
                        provider.clone(),
                        "volume".into(),
                        VolumeClass::Project,
                        match owner % 3 {
                            0 => VolumeOwner::Project("project".into()),
                            1 => VolumeOwner::Agent(AgentId::from_bytes([2; 16])),
                            _ => VolumeOwner::Session("session".into()),
                        },
                        &mut None,
                    )
                    .unwrap(),
                    generation: GenerationRef::new(&provider, key, Some("version"), None)
                        .unwrap()
                        .0,
                    namespace: NativeVolumeNamespace::MaterializedDirectory,
                    path: label.into(),
                    writable: true,
                    authority_revision: [3; 32],
                })
                .collect(),
        },
    }
}

fn verify(input: &[u8]) {
    let items = super::super::journal_verification::lexical_items(input);
    let bound = intent_decode_owned_bytes(items, input.len() as u64, 0).unwrap();
    let mut result = None;
    let measured = allocation_counter::measure(|| {
        result = Some(serde_json::from_slice::<Intent>(input));
    });
    assert!(measured.bytes_max as u64 <= bound, "{measured:?} > {bound}");
    if let Ok(decoded) = result.unwrap() {
        assert!(decoded.decoded_owned_bytes().unwrap().unwrap() <= bound);
        let encoded = serde_json::to_vec(&decoded).unwrap();
        let again: Intent = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            serde_json::to_value(&decoded).unwrap(),
            serde_json::to_value(&again).unwrap()
        );
    }
}

proptest! {
    // Property: the closed Intent allowance covers actual decoder peak heap
    // and retained capacities, including rejected owner content and truncation.
    #[test]
    fn intent_decode_bound_covers_owned_fields_and_owner_tag_scratch(
        labels in prop::collection::vec(".{0,64}", 0..16),
        key in prop::collection::vec(any::<u8>(), 1..128),
        owner in any::<u8>(),
        cut in any::<usize>(),
        scratch in prop::collection::vec(prop::collection::vec(".{0,32}", 0..16), 0..16),
    ) {
        let intent = intent(&labels, &key, owner);
        let encoded = serde_json::to_vec(&intent).unwrap();
        verify(&encoded);
        verify(&encoded[..cut % (encoded.len() + 1)]);
        let mut malformed = serde_json::to_value(&intent).unwrap();
        for mapping in malformed["manifest"]["volumes"].as_array_mut().unwrap() {
            // Sorted keys put id before kind, forcing the real adjacent-tag
            // decoder to buffer nested Content before rejecting its type.
            mapping["volume"]["owner"] = serde_json::json!({"id": scratch, "kind": "project"});
        }
        verify(&serde_json::to_vec(&malformed).unwrap());
    }
}

#[test]
fn intent_fits_existing_one_megabyte_ledger() -> Result<()> {
    let expected = intent(&["/root/volume".into()], &[1; 32], 0);
    let bytes = serde_json::to_vec(&expected).unwrap();
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let (decoded, custody) = crate::filesystem::native_decoded(io.decode(
        std::marker::PhantomData::<Intent>,
        &bytes,
        crate::wire::ErrorCode::Storage,
    )?)?;
    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    assert!(custody.work().shares_allocation_context(&work));
    assert!(work.receipt().peak_allocation_bytes <= 1_000_000);
    Ok(())
}

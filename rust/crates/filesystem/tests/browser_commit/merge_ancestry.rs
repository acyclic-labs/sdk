use super::*;
use acyclic_fs::kernel::{FileMetadata, NamespacePath};
use acyclic_fs::model::{
    AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, MutationMode, VolumeLimits,
};
use acyclic_fs::{
    AuthoredMutation, ContentChange, ContentTimes, FsError, LivePublicationObservation,
    OperationId, OperationReceipt,
};
use acyclic_stream::OwnedBytes;

async fn allow(_: WorkBudget) -> acyclic_fs::MeasuredResult<OperationReceipt<()>, FsError> {
    Ok(OperationReceipt {
        value: (),
        work: WorkCounters::UNCHARGED,
    })
}

#[wasm_bindgen_test]
async fn browser_prepared_merge_preserves_ancestry_after_composition() -> Result<(), JsValue> {
    for stale in [false, true] {
        let name = if stale {
            "acyclic-fs-browser-merge-stale"
        } else {
            "acyclic-fs-browser-merge-batch"
        };
        delete_database(name).await?;
        let authority = open_clean_authority(name).await?;
        let objects = IndexedDbObjectStore::open(name, 65_536)
            .await
            .map_err(js_error)?;
        let fs = acyclic_fs::Fs::new(
            authority,
            objects,
            acyclic_fs::EmbeddedCapabilities { durable: true },
        );
        let cancel = CancellationToken::new();
        let budget = WorkBudget::UNBOUNDED;
        let workspace = fs
            .create_workspace("merge", budget, &cancel)
            .await
            .map_err(js_error)?
            .value;
        let mode = CheckoutMode {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::Manual,
            mutations: MutationMode::PrivateOverlay,
        };
        let mut seed = workspace
            .checkout(GenerationSelector::Head, mode, budget, &cancel)
            .await
            .map_err(js_error)?
            .value;
        for name in ["/a", "/b"] {
            seed.create(
                AuthoredMutation::CreateFile {
                    path: NamespacePath::from_portable(
                        &acyclic_fs::path::PortablePath::parse(name, VolumeLimits::default())
                            .map_err(js_error)?,
                        VolumeLimits::default(),
                    )
                    .map_err(js_error)?,
                    bytes: OwnedBytes::from_static(b"old"),
                    metadata: FileMetadata::default(),
                },
                budget,
                &cancel,
            )
            .await
            .map_err(js_error)?;
        }
        seed.commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            budget,
            &cancel,
            allow,
        )
        .await
        .map_err(js_error)?;
        let mut merge = workspace
            .checkout(GenerationSelector::Head, mode, budget, &cancel)
            .await
            .map_err(js_error)?
            .value;
        let mut other = workspace
            .checkout(GenerationSelector::Head, mode, budget, &cancel)
            .await
            .map_err(js_error)?
            .value;
        for (checkout, name) in [(&mut merge, "/a"), (&mut other, "/b")] {
            checkout
                .change_content(
                    NamespacePath::from_portable(
                        &acyclic_fs::path::PortablePath::parse(name, VolumeLimits::default())
                            .map_err(js_error)?,
                        VolumeLimits::default(),
                    )
                    .map_err(js_error)?,
                    ContentChange::Write {
                        offset: 0,
                        bytes: OwnedBytes::from_static(b"new"),
                    },
                    ContentTimes::Preserve,
                    budget,
                    &cancel,
                )
                .await
                .map_err(js_error)?;
        }
        assert!(matches!(
            merge
                .prepare_merge(seed.generation_id(), 32, 8, budget, &cancel)
                .await
                .map_err(js_error)?
                .value,
            acyclic_fs::MergePreparation::Prepared { .. }
        ));
        let prepared = merge.generation_id();
        let parents = merge.root().parents.clone();
        assert_eq!(parents.len(), 2);
        if stale {
            other
                .commit(
                    OperationId::new(),
                    None,
                    None,
                    None,
                    PublicationPermit::Unrestricted,
                    budget,
                    &cancel,
                    allow,
                )
                .await
                .map_err(js_error)?;
        }
        let publish = merge.commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            budget,
            &cancel,
            allow,
        );
        let outcome = if stale {
            publish.await.map_err(js_error)?
        } else {
            let (first, second) = futures::join!(
                other.commit(
                    OperationId::new(),
                    None,
                    None,
                    None,
                    PublicationPermit::Unrestricted,
                    budget,
                    &cancel,
                    allow
                ),
                publish
            );
            assert!(matches!(
                first.map_err(js_error)?.value,
                LivePublicationObservation::Committed { .. }
            ));
            second.map_err(js_error)?
        };
        assert!(matches!(
            outcome.value,
            LivePublicationObservation::Committed { .. }
        ));
        if merge.generation_id() == prepared {
            // IndexedDB completions can make either concurrent proposal the
            // leader. An unchanged merge root already retains its ancestry.
            assert!(!stale, "stale proposal bypassed composition");
            assert_eq!(merge.root().parents, parents);
        } else {
            assert_eq!(
                merge.root().parents.as_slice(),
                &[other.generation_id(), prepared],
                "composition discarded the original prepared merge ancestry"
            );
        }
        let original = workspace
            .checkout(
                GenerationSelector::Exact(prepared),
                CheckoutMode::read_only_pinned(),
                budget,
                &cancel,
            )
            .await
            .map_err(js_error)?
            .value;
        assert_eq!(original.root().parents, parents);
    }
    Ok(())
}

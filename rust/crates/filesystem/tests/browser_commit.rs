//! Browser host conformance through the public canonical volume commit API.
#![cfg(target_arch = "wasm32")]
use acyclic_fs::browser::IndexedDbObjectStore;
use acyclic_fs::{CancellationToken, PublicationPermit, WorkBudget, WorkCounters};
use indexed_db_futures::database::Database;
use indexed_db_futures::prelude::*;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;
wasm_bindgen_test_configure!(run_in_browser);
#[path = "browser_commit/merge_ancestry.rs"]
mod merge_ancestry;
#[path = "browser_commit/storage.rs"]
mod storage;

fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}
async fn delete_database(name: &str) -> Result<(), JsValue> {
    Database::delete_by_name(name)
        .map_err(js_error)?
        .await
        .map_err(js_error)
}
async fn open_clean_authority(
    name: &str,
) -> Result<acyclic_fs::StreamAuthorityStore<dyn acyclic_stream::StreamProvider>, JsValue> {
    delete_database(&format!("acyclic-fs-authority/{name}")).await?;
    acyclic_fs::browser::open_authority(name, acyclic_stream::BrowserStreamLimits::default())
        .await
        .map_err(js_error)
}

#[wasm_bindgen_test]
async fn browser_disjoint_transactions_use_the_canonical_group_and_original_receipts()
-> Result<(), JsValue> {
    use futures::TryStreamExt;
    const NAME: &str = "acyclic-fs-browser-group";
    delete_database(NAME).await?;
    let authority = open_clean_authority(NAME).await?;
    let objects = IndexedDbObjectStore::open(NAME, 65_536)
        .await
        .map_err(js_error)?;
    let fs = acyclic_fs::Fs::new(
        authority,
        objects,
        acyclic_fs::EmbeddedCapabilities { durable: true },
    );
    let cancellation = CancellationToken::new();
    let workspace = fs
        .create_workspace("batch", WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(js_error)?
        .value;
    let mut seed = workspace
        .begin_transaction(
            acyclic_fs::IdempotencyKey::new(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(js_error)?
        .value;
    seed.write_text("/a", "old").await.map_err(js_error)?;
    seed.write_text("/b", "old").await.map_err(js_error)?;
    assert!(matches!(
        seed.commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            allow_browser_publication
        )
        .await
        .map_err(js_error)?
        .value,
        acyclic_fs::TransactionCommit::Committed(_)
    ));
    let (provider, path) = fs
        .authority_records(workspace.id().volume_id())
        .map_err(js_error)?;
    let before = provider.tail(path.clone()).await.map_err(js_error)?;
    let keys = [
        acyclic_fs::IdempotencyKey::new(),
        acyclic_fs::IdempotencyKey::new(),
    ];
    let mut left = workspace
        .begin_transaction(keys[0], WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(js_error)?
        .value;
    let mut right = workspace
        .begin_transaction(keys[1], WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(js_error)?
        .value;
    left.write_text("/a", "left").await.map_err(js_error)?;
    right.write_text("/b", "right").await.map_err(js_error)?;
    let (left, right) = futures::join!(
        left.commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            allow_browser_publication
        ),
        right.commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            allow_browser_publication
        ),
    );
    for outcome in [left, right] {
        assert!(matches!(
            outcome.map_err(js_error)?.value,
            acyclic_fs::TransactionCommit::Committed(_)
        ));
    }
    let records: Vec<_> = provider
        .read(acyclic_stream::ReadRequest {
            path,
            from: before,
            limit: 2,
        })
        .await
        .map_err(js_error)?
        .try_collect()
        .await
        .map_err(js_error)?;
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[0].commit_id, records[1].commit_id,
        "browser bypassed group commit"
    );
    for (key, (path, expected)) in keys
        .into_iter()
        .zip([("/a", b"left".as_slice()), ("/b", b"right".as_slice())])
    {
        assert!(
            workspace
                .operation_generation(key, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(js_error)?
                .value
                .is_some()
        );
        assert_eq!(
            workspace
                .read(path, 16, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(js_error)?
                .value
                .as_ref(),
            expected
        );
    }
    Ok(())
}

fn allow_browser_publication(
    _: WorkBudget,
) -> std::future::Ready<
    acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, acyclic_fs::WorkspaceError>,
> {
    std::future::ready(Ok(acyclic_fs::OperationReceipt {
        value: (),
        work: WorkCounters::UNCHARGED,
    }))
}

#[wasm_bindgen_test]
async fn incompatible_authority_schema_is_rejected_without_replacing_persisted_state()
-> Result<(), JsValue> {
    const NAME: &str = "acyclic-fs-prior-authority-schema";
    delete_database(NAME).await?;
    let old = Database::open(NAME)
        .with_version(1_u32)
        .with_on_upgrade_needed(|_, database| {
            database.create_object_store("persisted-state").build()?;
            Ok(())
        })
        .await
        .map_err(js_error)?;
    old.close();
    assert!(
        acyclic_fs::browser::open_authority(NAME, acyclic_stream::BrowserStreamLimits::default())
            .await
            .is_err()
    );
    assert!(IndexedDbObjectStore::open(NAME, 1024).await.is_err());
    let retained = Database::open(NAME)
        .with_version(1_u32)
        .await
        .map_err(js_error)?;
    assert_eq!(retained.version(), 1.0);
    assert!(retained.transaction("persisted-state").build().is_ok());
    retained.close();
    delete_database(NAME).await?;
    Ok(())
}

use super::*;
use js_sys::{Array, BigInt, Reflect, Uint8Array};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

fn js<T: Tsify + Serialize>(value: T) -> Result<JsValue, JsValue> {
    ts(value).map(Into::into)
}

fn field<T: JsCast>(value: &JsValue, name: &str) -> Result<T, JsValue> {
    Ok(Reflect::get(value, &name.into())?.dyn_into::<T>()?)
}

fn decimal(value: &JsValue, name: &str) -> Result<Option<String>, JsValue> {
    Ok(field::<BigInt>(value, name)?.to_string(10)?.as_string())
}

fn text(value: &JsValue, name: &str) -> Result<Option<String>, JsValue> {
    Ok(Reflect::get(value, &name.into())?.as_string())
}

fn work() -> BrowserWorkCounters {
    browser_work(acyclic_fs::WorkCounters {
        bytes_copied: 17,
        ..acyclic_fs::WorkCounters::default()
    })
}

#[wasm_bindgen_test]
fn transaction_conflicts_preserve_full_width_integers_bytes_and_undefined() -> Result<(), JsValue> {
    for offset in [3, 9_007_199_254_740_993, u64::MAX] {
        let result = js(BrowserTransactionRebase {
            status: BrowserTransactionRebaseStatus::Conflicted,
            generation_id: None,
            conflicts: vec![browser_transaction_conflict(RebaseConflict {
                region: DependencyRegion::ContentRange {
                    file_id: FileId::from_bytes([7; 16]),
                    offset,
                    length: u64::MAX,
                },
                usage: DependencyUse::ObservationAndMutation,
                expected: DependencyState::Absent,
                actual: DependencyState::Absent,
            })],
            truncated: false,
        })?;
        assert!(Reflect::get(&result, &"generationId".into())?.is_undefined());
        let conflict = field::<Array>(&result, "conflicts")?.get(0);
        assert_eq!(decimal(&conflict, "offset")?, Some(offset.to_string()));
        assert_eq!(decimal(&conflict, "length")?, Some(u64::MAX.to_string()));
        assert_eq!(
            field::<Uint8Array>(&conflict, "fileId")?.to_vec(),
            vec![7; 16]
        );
        for absent in ["directoryId", "expected", "actual"] {
            assert!(Reflect::get(&conflict, &absent.into())?.is_undefined());
        }
        assert_eq!(text(&conflict, "region")?.as_deref(), Some("content-range"));
        assert_eq!(
            text(&conflict, "usage")?.as_deref(),
            Some("observation-and-mutation")
        );
    }
    Ok(())
}

#[wasm_bindgen_test]
fn result_dtos_preserve_bytes_bigints_and_work_counters() -> Result<(), JsValue> {
    let plan = js(BrowserWorkspaceExtentPlan {
        spans: vec![BrowserWorkspaceExtentSpan {
            offset: 9_007_199_254_740_993,
            length: 1,
            source_end: 9_007_199_254_740_994,
            kind: "content",
        }],
    })?;
    let span = field::<Array>(&plan, "spans")?.get(0);
    assert_eq!(
        decimal(&span, "offset")?.as_deref(),
        Some("9007199254740993")
    );

    let mutation = js(BrowserMutationResult {
        file_id: Some(vec![7; 16].into()),
        work: work(),
    })?;
    assert_eq!(field::<Uint8Array>(&mutation, "fileId")?.length(), 16);

    let metadata = js(super::metadata::BrowserMetadataResult {
        canonical_bytes: vec![0, 255, 1],
        work: work(),
    })?;
    assert_eq!(
        field::<Uint8Array>(&metadata, "canonicalBytes")?.to_vec(),
        vec![0, 255, 1]
    );

    let commit = js(BrowserCommitResult {
        status: "committed",
        generation_id: Some(vec![5; 32].into()),
        epoch: Some(9_007_199_254_740_993),
        sequence: None,
        committed_fingerprint: None,
        work: work(),
    })?;
    assert_eq!(
        decimal(&commit, "epoch")?.as_deref(),
        Some("9007199254740993")
    );
    assert!(Reflect::get(&commit, &"sequence".into())?.is_undefined());
    let work = Reflect::get(&commit, &"work".into())?;
    assert_eq!(decimal(&work, "bytesCopied")?.as_deref(), Some("17"));

    let transfer = js(GenerationTransferBatchResult {
        first_object: 9_007_199_254_740_993,
        next_object: None,
        objects: vec![vec![1, 2, 3].into()],
        work: self::work(),
    })?;
    assert_eq!(
        decimal(&transfer, "firstObject")?.as_deref(),
        Some("9007199254740993")
    );
    let object = field::<Array>(&transfer, "objects")?
        .get(0)
        .dyn_into::<Uint8Array>()?;
    assert_eq!(object.to_vec(), vec![1, 2, 3]);
    Ok(())
}

#[wasm_bindgen_test]
fn oversized_transaction_array_is_rejected_before_decode() {
    let operations = Array::new();
    operations.set_length(2_049);
    let config = VolumeConfig {
        profile: FilesystemProfile::Posix,
        ..VolumeConfig::portable(Lifecycle::Ephemeral)
    };
    assert!(decode_authored_transactions(operations.into(), config).is_err());
}

#[wasm_bindgen_test(async)]
async fn transaction_results_preserve_commits_and_conflicting_rebases() -> Result<(), JsValue> {
    let cache = ObjectCacheOptions {
        maximum_entries: 8,
        maximum_bytes: 1024,
        maximum_in_flight: 2,
        maximum_waiters_per_object: 2,
    };
    let fs = browser_fs(
        memory_engine(1024, 64 * 1024 * 1024, cache)?,
        "memory",
        "memory",
    );
    let workspace = fs.create_workspace("transactions".to_owned()).await?;
    workspace.write("/file".to_owned(), vec![1]).await?;
    let mut winner = workspace.begin_transaction(Some(vec![31; 16])).await?;
    let mut loser = workspace.begin_transaction(Some(vec![32; 16])).await?;
    winner.write("/file".to_owned(), vec![2]).await?;
    loser.write("/file".to_owned(), vec![3]).await?;
    let committed: JsValue = winner.commit(None).await?.into();
    assert_eq!(text(&committed, "status")?.as_deref(), Some("committed"));
    assert_eq!(
        field::<Uint8Array>(&committed, "generationId")?.length(),
        32
    );
    let conflict: JsValue = loser.commit(None).await?.into();
    assert_eq!(text(&conflict, "status")?.as_deref(), Some("conflict"));
    let rebased: JsValue = loser.rebase(JsValue::from(16)).await?.into();
    assert_eq!(text(&rebased, "status")?.as_deref(), Some("conflicted"));
    assert!(Reflect::get(&rebased, &"generationId".into())?.is_undefined());
    assert!(field::<Array>(&rebased, "conflicts")?.length() > 0);
    assert_eq!(workspace.read("/file".to_owned(), 1).await?, vec![2]);
    Ok(())
}

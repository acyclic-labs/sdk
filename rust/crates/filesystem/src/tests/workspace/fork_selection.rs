use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn invalid_fork_prefixes_stop_before_directory_rewrites() -> Result<(), Box<dyn Error>> {
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let mut writes = Vec::new();
    for path in ["/missing", "/file/child"] {
        let fs = Fs::memory();
        let parent = unbounded!(fs, create_workspace("invalid-selection"))?;
        parent.write_text("/file", "body").await?;
        let source = unbounded!(parent, head())?;
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes().unwrap();
        let result = source
            .matches_fork_selection(
                &source,
                Some(vec![path.to_owned()]),
                &original,
                &cancellation,
            )
            .await;
        if path == "/missing" {
            assert!(matches!(result, Err(WorkspaceError::NotFound)));
        } else {
            assert!(matches!(result, Err(WorkspaceError::NotDirectory)));
        }
        writes.push(original.receipt().unwrap().page_writes);
        drop((source, parent, fs));
        assert_eq!(original.live_allocation_bytes().unwrap(), baseline);
    }
    // Both invalid inputs stop after the same shared empty-tree preparation.
    // A known non-directory must not produce a pruned parent before refusing.
    assert_eq!(writes[1], writes[0]);
    Ok(())
}

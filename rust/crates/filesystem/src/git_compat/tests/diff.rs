use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    // Count eligible files exactly, excluding directory records and SDK roots.
    // Similar prefixes stay visible; explicitly tracked files retain Git semantics.
    #[test]
    fn reserved_roots_do_not_enter_file_diffs(
        visible in 1usize..4, hidden in 1usize..4,
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let workspace = memory_workspace("diff-reserved-roots").await;
            let before = workspace.head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await.unwrap().value;
            let mut transaction = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x71;16])).await;
            for directory in ["/src", "/.system", "/.volumes", "/.systematic"] {
                transaction.create_dir_all(directory, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await.unwrap();
            }
            for index in 0..visible {
                transaction.write_text(&format!("/src/{index}.txt"), "coding file").await.unwrap();
            }
            transaction.write_text("/.systematic/visible", "ordinary project file").await.unwrap();
            for directory in [".system", ".volumes"] {
                for index in 0..hidden {
                    transaction.write_text(&format!("/{directory}/{index}.txt"), "SDK metadata").await.unwrap();
                }
            }
            let after = publish_fixture(transaction).await;
            let cancellation = CancellationToken::new();
            let tracked = BTreeSet::new();
            let all = git_compatible_diff_counts(&before, &after, &tracked, &[], 64, &cancellation).await.unwrap();
            assert_eq!(all.file_changes, visible + 1 + 2 * hidden);
            assert_eq!(all.binding_changes, all.file_changes);
            let filtered = git_compatible_diff_counts(&before, &after, &tracked,
                &[".system", ".volumes"], 64, &cancellation).await.unwrap();
            assert_eq!(filtered.file_changes, visible + 1);
            assert_eq!(filtered.binding_changes, visible + 1);
            let tracked = BTreeSet::from([".system/0.txt".into()]);
            let filtered = git_compatible_diff_counts(&before, &after, &tracked,
                &[".system", ".volumes"], 64, &cancellation).await.unwrap();
            assert_eq!(filtered.file_changes, visible + 2);
            assert_eq!(filtered.binding_changes, visible + 2);
            assert_eq!(workspace.head(WorkBudget::UNBOUNDED, &cancellation).await.unwrap().value.id(), after.id());
        });
    }
}

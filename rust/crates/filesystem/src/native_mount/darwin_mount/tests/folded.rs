use super::*;
use proptest::prelude::*;

// The real Darwin pager must reach every unambiguous entry and EOF, regardless
// of how many empty filtered source pages precede them. Every page still
// charges the original native work.
fn filtered_pages(pages: usize, visible: bool) -> TestResult {
    let names = (0..pages * DIRECTORY_PAGE_SIZE as usize / 2)
        .flat_map(|index| [format!("A{index:05}"), format!("a{index:05}")])
        .chain(visible.then(|| "z-visible".to_owned()));
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let checkout = runtime.block_on(crate::facade::test_fixtures::folded_checkout(names))?;
    let config = checkout.volume_config();
    let source = Arc::new(
        crate::native_mount::adapter::tests::access::bind_test_access(
            MemorySource::new(
                Arc::new(crate::native_mount::SharedCheckout::new(checkout)),
                config,
            )?,
            vec![],
            None,
        )?,
    );
    let work = work(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let context = test_context(&source, &work)?;
    let root = MountPath::root();
    let mut directory = DirectoryRead::new(clone_path(&root, &work)?, context.cache_epochs(&work)?);
    ensure_directory_page(&context, &mut directory, &work)?;
    assert_eq!(directory.pop_entry(), visible);
    ensure_directory_page(&context, &mut directory, &work)?;
    assert!(
        !directory.pop_entry(),
        "only the unrelated entry may be disclosed"
    );
    Ok(())
}

#[test]
fn folded_filtered_pages_reach_visible_entries_and_eof() -> TestResult {
    for visible in [false, true] {
        filtered_pages(9, visible)?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]

    #[test]
    fn folded_filtered_pager_preserves_visibility_and_eof(
        pages in 0..11,
        visible in any::<bool>(),
    ) {
        filtered_pages(pages, visible).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

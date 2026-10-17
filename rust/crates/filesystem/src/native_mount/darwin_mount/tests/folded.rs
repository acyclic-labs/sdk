use super::*;
use proptest::prelude::*;

// The real Darwin pager must reach every unambiguous entry and EOF, regardless
// of how many empty filtered source pages precede them. Only changed epochs
// spend the bounded restart allowance; every page still charges native work.
fn filtered_pages(pages: usize, visible: bool) -> TestResult {
    let names = (0..pages * DIRECTORY_PAGE_SIZE as usize / 2)
        .flat_map(|index| [format!("A{index:05}"), format!("a{index:05}")])
        .chain(visible.then(|| "z-visible".to_owned()));
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let checkout = runtime.block_on(crate::facade::test_fixtures::folded_checkout(names))?;
    let config = checkout.volume_config();
    let source = Arc::new(MemorySource::new(
        Arc::new(crate::native_mount::SharedCheckout::new(checkout)),
        config,
    )?);
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
fn folded_filtered_pages_do_not_exhaust_the_restart_allowance() -> TestResult {
    for visible in [false, true] {
        filtered_pages(MAXIMUM_LISTING_RESTARTS + 1, visible)?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]

    #[test]
    fn folded_filtered_pager_preserves_visibility_and_eof(
        pages in 0..MAXIMUM_LISTING_RESTARTS + 3,
        visible in any::<bool>(),
    ) {
        filtered_pages(pages, visible).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

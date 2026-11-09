use acyclic_fs::native_mount::{MountDirectoryCursor, MountDirectoryPage};
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};

// This module is compiled outside the library: custom sources must not need
// crate-private cursor fields to retain independent listings and their credit.
#[test]
fn custom_directory_continuations_retain_independent_state_and_original_credit()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = work.live_allocation_bytes();
    let make_page = |directory, position| {
        MountDirectoryCursor::from_source_state((directory, position), &work)
            .map(|cursor| MountDirectoryPage::new(Vec::new(), Some(cursor), None))
    };
    let first = make_page(11_u64, 37_u64)?;
    let second = make_page(22_u64, 19_u64)?;
    let bytes = std::mem::size_of::<(u64, u64)>() as u64;
    assert_eq!(work.live_allocation_bytes(), baseline + 2 * bytes);
    assert_eq!(
        first
            .next_cursor()
            .and_then(MountDirectoryCursor::source_state::<(u64, u64)>),
        Some(&(11, 37))
    );
    drop(second);
    assert_eq!(work.live_allocation_bytes(), baseline + bytes);
    assert_eq!(
        first
            .next_cursor()
            .and_then(MountDirectoryCursor::source_state::<(u64, u64)>),
        Some(&(11, 37))
    );
    assert!(
        first
            .next_cursor()
            .and_then(MountDirectoryCursor::source_state::<String>)
            .is_none()
    );
    drop(first);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

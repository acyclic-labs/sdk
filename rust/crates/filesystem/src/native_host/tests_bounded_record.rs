use super::*;
#[cfg(unix)]
#[test]
fn held_canonical_directory_observes_renamed_object_not_replaced_spelling()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let original = temporary.path().join("original");
    let moved = temporary.path().join("moved");
    std::fs::create_dir(&original)?;
    let directory = Dir::open_ambient_dir(&original, cap_std::ambient_authority())?;
    std::fs::rename(&original, &moved)?;
    std::fs::create_dir(&original)?;
    let expected = std::fs::canonicalize(&moved)?;
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
    let (actual, allocation) = canonical_directory_path_held(directory, &mut work)?;
    assert_eq!(actual, expected);
    assert_ne!(actual, std::fs::canonicalize(&original)?);
    assert_eq!(work.live_allocation_bytes(), actual.capacity() as u64);
    drop(actual);
    work.release(allocation)?;
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}
#[cfg(windows)]
#[test]
fn canonical_wide_directory_retains_native_capacity_without_scalar_conversion()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::windows::ffi::{OsStrExt as _, OsStringExt as _};
    let temporary = tempfile::tempdir()?;
    let input = temporary.path().join(OsString::from_wide(&[0xD800, 120]));
    std::fs::create_dir(&input)?;
    let expected = std::fs::canonicalize(&input)?;
    let expected: Vec<u16> = expected.as_os_str().encode_wide().collect();
    let directory = Dir::open_ambient_dir(&input, cap_std::ambient_authority())?;
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
    let (actual, allocation) = canonical_directory_wide(&directory, &mut work)?;
    assert_eq!(actual, expected);
    assert_ne!(actual.last(), Some(&0));
    assert_eq!(work.live_allocation_bytes(), actual.capacity() as u64 * 2);
    drop(actual);
    work.release(allocation)?;
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}
#[test]
fn owned_child_uses_held_parent_after_lexical_replacement() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let original = temporary.path().join("original");
    std::fs::create_dir(&original)?;
    std::fs::create_dir(original.join("child"))?;
    let parent = Dir::open_ambient_dir(&original, cap_std::ambient_authority())?;
    let child = Dir::open_ambient_dir(original.join("child"), cap_std::ambient_authority())?;
    let expected = held_root_identity(&child)?;
    std::fs::rename(&original, temporary.path().join("moved"))?;
    std::fs::create_dir(&original)?;
    std::fs::create_dir(original.join("child"))?;
    #[cfg(unix)]
    let name = OsStr::new("child");
    #[cfg(windows)]
    let name = &[99_u16, 104, 105, 108, 100][..];
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
    let actual = open_owned_child_directory(&parent, name, &mut work)?;
    assert_eq!(held_root_identity(&actual)?, expected);
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}
#[cfg(unix)]
#[test]
fn owned_child_refuses_symlinks_and_nonleaf_names() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    std::fs::create_dir(temporary.path().join("child"))?;
    std::os::unix::fs::symlink("child", temporary.path().join("link"))?;
    let parent = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
    for name in ["link", "..", "child/../child"] {
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
        assert!(open_owned_child_directory(&parent, OsStr::new(name), &mut work).is_err());
        assert_eq!(work.live_allocation_bytes(), 0);
    }
    Ok(())
}
const ACCOUNTING_STATE_BYTES: u64 = NativeMaterializationWork::accounting_state_bytes();
#[cfg(unix)]
#[test]
fn held_ancestry_survives_aliases_and_lexical_parent_replacement()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let parent_path = temporary.path().join("parent");
    std::fs::create_dir(&parent_path)?;
    std::fs::create_dir(parent_path.join("child"))?;
    std::os::unix::fs::symlink(&parent_path, temporary.path().join("alias"))?;
    let parent = Dir::open_ambient_dir(&parent_path, cap_std::ambient_authority())?;
    let child = Dir::open_ambient_dir(
        temporary.path().join("alias/child"),
        cap_std::ambient_authority(),
    )?;
    let parent_identity = held_root_identity(&parent)?;
    let child_identity = held_root_identity(&child)?;
    std::fs::rename(&parent_path, temporary.path().join("moved"))?;
    std::fs::create_dir(&parent_path)?;
    let replacement = Dir::open_ambient_dir(&parent_path, cap_std::ambient_authority())?;
    let replacement_identity = held_root_identity(&replacement)?;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = 0;
    let mut work = DemandWork::new(budget, &crate::CancellationToken::new());
    assert!(directory_has_ancestor(&child, child_identity, &mut work)?);
    assert!(directory_has_ancestor(&child, parent_identity, &mut work)?);
    assert!(!directory_has_ancestor(
        &child,
        replacement_identity,
        &mut work
    )?);
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}
struct CancelAfterName<'a> {
    work: &'a NativeMaterializationWork,
    cancellation: &'a crate::CancellationToken,
}
impl NativeHostWork for CancelAfterName<'_> {
    type Error = NativeTreeMaterializationError;
    type Allocation = NativeAllocation;
    type CleanupWrite = NativeCleanupWrite;
    fn io_error_kind(error: &Self::Error) -> Option<io::ErrorKind> {
        <&NativeMaterializationWork as NativeHostWork>::io_error_kind(error)
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error {
        NativeTreeMaterializationError::AllocationFailed(error)
    }
    fn check(&self) -> Result<(), Self::Error> {
        self.work.admit(WorkCounters::UNCHARGED)
    }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.work.admit(delta)
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.work.record_admitted(delta)?;
        if delta.bytes_copied != 0 {
            self.cancellation.cancel();
        }
        Ok(())
    }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.work.charge(delta)
    }
    fn claim(&mut self, bytes: u64) -> Result<Self::Allocation, Self::Error> {
        self.work
            .claim(bytes, u64::from((bytes) != 0), WorkCounters::UNCHARGED)
    }
    fn reserve_opaque_decode(&mut self, logical_max: u64) -> Result<Self::Allocation, Self::Error> {
        self.work.claim(logical_max, 0, WorkCounters::UNCHARGED)
    }
    fn reconcile_allocation(
        &mut self,
        allocation: &mut Self::Allocation,
        actual: u64,
    ) -> Result<(), Self::Error> {
        allocation.reconcile_capacity(actual)
    }
    fn release(&mut self, allocation: Self::Allocation) -> Result<(), Self::Error> {
        drop(allocation);
        Ok(())
    }
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error> {
        self.work.reserve_cleanup_write()
    }
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite) {
        drop(reservation);
    }
    fn attempt_cleanup_write<T>(
        &mut self,
        reservation: Self::CleanupWrite,
        operation: impl FnOnce() -> T,
    ) -> (T, Result<(), Self::Error>) {
        reservation.attempt(operation)
    }
}
#[test]
fn cancellation_after_native_name_copy_releases_it_without_consuming_the_record()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("kept"), b"data")?;
    let directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
    let cancellation = crate::CancellationToken::new();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes =
        ACCOUNTING_STATE_BYTES + HostStatReader::BUFFER_BYTES as u64 + 32;
    let work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(cause, _)| cause)?;
    let mut boundary = CancelAfterName {
        work: &work,
        cancellation: &cancellation,
    };
    let (mut reader, allocation) = read_dir_held(&directory, &mut boundary)?;
    #[cfg(unix)]
    let failure = reader.next(&mut boundary);
    #[cfg(windows)]
    let failure = reader.next_wide(&mut boundary);
    assert!(matches!(
        failure,
        Err(NativeTreeMaterializationError::Cancelled)
    ));
    assert_eq!(
        materialization_live_bytes(&work)?,
        ACCOUNTING_STATE_BYTES + HostStatReader::BUFFER_BYTES as u64
    );
    #[cfg(unix)]
    let repeated = reader.next(&mut boundary);
    #[cfg(windows)]
    let repeated = reader.next_wide(&mut boundary);
    assert!(matches!(
        repeated,
        Err(NativeTreeMaterializationError::Cancelled)
    ));
    drop((reader, allocation));
    assert_eq!(materialization_live_bytes(&work)?, ACCOUNTING_STATE_BYTES);
    Ok(())
}
fn materialization_live_bytes(
    work: &NativeMaterializationWork,
) -> Result<u64, NativeTreeMaterializationError> {
    let (bytes, _empty_allocation) = work.allocations(|ledger, _, _, _| {
        Ok::<_, NativeTreeMaterializationError>(ledger.live_bytes())
    })?;
    Ok(bytes)
}
#[test]
fn held_scanners_keep_independent_cursors_and_scoped_native_allocations()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("kept"), b"data")?;
    let directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes =
        ACCOUNTING_STATE_BYTES + 2 * HostStatReader::BUFFER_BYTES as u64 + 64;
    let work = NativeMaterializationWork::new(budget, &crate::CancellationToken::new())
        .map_err(|(cause, _)| cause)?;
    let (mut first, first_allocation) = read_dir_held(&directory, &mut &work)?;
    let (mut second, second_allocation) = read_dir_held(&directory, &mut &work)?;
    assert_eq!(
        first_allocation.bytes(),
        HostStatReader::BUFFER_BYTES as u64
    );
    assert_eq!(
        materialization_live_bytes(&work)?,
        ACCOUNTING_STATE_BYTES + 2 * HostStatReader::BUFFER_BYTES as u64
    );
    #[cfg(unix)]
    let (first_entry, first_name) = first
        .next(&mut &work)?
        .ok_or_else(|| io::Error::other("first cursor lost its entry"))?;
    #[cfg(windows)]
    let (first_entry, first_name) = first
        .next_wide(&mut &work)?
        .ok_or_else(|| io::Error::other("first cursor lost its entry"))?;
    #[cfg(unix)]
    let (second_entry, second_name) = second
        .next(&mut &work)?
        .ok_or_else(|| io::Error::other("second cursor shared the first cursor's position"))?;
    #[cfg(windows)]
    let (second_entry, second_name) = second
        .next_wide(&mut &work)?
        .ok_or_else(|| io::Error::other("second cursor shared the first cursor's position"))?;
    assert_eq!(first_entry.name, second_entry.name);
    assert_eq!(first_name.bytes(), first_entry.logical_owned_bytes());
    assert_eq!(second_name.bytes(), second_entry.logical_owned_bytes());
    assert!(first_entry.stat.is_some());
    assert!(second_entry.stat.is_some());
    assert_eq!(
        materialization_live_bytes(&work)?,
        ACCOUNTING_STATE_BYTES
            + 2 * HostStatReader::BUFFER_BYTES as u64
            + first_name.bytes()
            + second_name.bytes()
    );
    drop((first_entry, first_name, second_entry, second_name));
    assert_eq!(
        materialization_live_bytes(&work)?,
        ACCOUNTING_STATE_BYTES + 2 * HostStatReader::BUFFER_BYTES as u64
    );
    drop((first, first_allocation, second, second_allocation));
    assert_eq!(materialization_live_bytes(&work)?, ACCOUNTING_STATE_BYTES);
    Ok(())
}
#[test]
fn a_record_survives_metadata_admission_failure_without_another_refill()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("kept"), b"data")?;
    let root = HostRoot::open(temporary.path(), &mut ordinary_work()).map_err(ordinary_error)?;
    let mut first = ordinary_work();
    let (mut reader, _) = root.read_dir_stats(Path::new(""), &mut first)?;
    first.budget.backend_read_operations = first.work.backend_read_operations + 1;
    #[cfg(unix)]
    let failure = reader.next(&mut first);
    #[cfg(windows)]
    let failure = reader.next_wide(&mut first);
    assert!(matches!(
        failure,
        Err(DemandError::Work(crate::WorkError::BudgetExceeded {
            counter: "backend_read_operations",
            ..
        }))
    ));
    let mut retry = ordinary_work();
    retry.adopt(reader.logical_owned_bytes())?;
    #[cfg(unix)]
    let entry = reader.next(&mut retry)?;
    #[cfg(windows)]
    let entry = reader.next_wide(&mut retry)?;
    let (entry, _) = entry.ok_or_else(|| io::Error::other("denied native record was lost"))?;
    #[cfg(unix)]
    assert_eq!(entry.name, OsStr::new("kept"));
    #[cfg(windows)]
    assert_eq!(entry.name, [107, 101, 112, 116]);
    assert!(entry.stat.is_some());
    Ok(())
}

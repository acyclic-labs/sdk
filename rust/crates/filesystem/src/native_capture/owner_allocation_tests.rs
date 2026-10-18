use super::*;
use acyclic_native_runtime::{NativeFile, ReadAllocation};
use acyclic_test_support::{AllocationInfo, measure};
use std::{borrow::Borrow, cell::Cell, ops::Deref, sync::Arc};

thread_local! {
    static OLD_OWNER: Cell<bool> = const { Cell::new(false) };
}

pub(super) enum Owner {
    Stack(NativeFile),
    Old {
        file: Arc<NativeFile>,
        _allocation: ReadAllocation,
    },
}

impl Borrow<NativeFile> for Owner {
    fn borrow(&self) -> &NativeFile {
        match self {
            Self::Stack(file) => file,
            Self::Old { file, .. } => file,
        }
    }
}

impl Deref for Owner {
    type Target = NativeFile;
    fn deref(&self) -> &NativeFile {
        self.borrow()
    }
}

pub(super) fn owner(file: NativeFile, work: &NativeMaterializationWork) -> Owner {
    if OLD_OWNER.get() {
        let allocation =
            ReadAllocation::new(&work.runtime_meter(), std::mem::size_of::<NativeFile>())
                .expect("the old-owner control has an unbounded original budget");
        Owner::Old {
            file: Arc::new(file),
            _allocation: allocation,
        }
    } else {
        Owner::Stack(file)
    }
}

struct OldOwner(bool);
pub(super) fn restore_owner() -> impl Drop {
    OldOwner(OLD_OWNER.replace(true))
}
impl Drop for OldOwner {
    fn drop(&mut self) {
        OLD_OWNER.set(self.0);
    }
}

// Warm one-byte staging with live shared health: these counts exclude fixture setup.
const OWNER_FREE_ALLOCATIONS: u64 = if cfg!(windows) { 43 } else { 46 };

fn no_owner_allocation(measured: AllocationInfo) -> Result<(), String> {
    let ceiling = OWNER_FREE_ALLOCATIONS;
    if measured.count_total != ceiling {
        return Err(format!(
            "capture allocations: expected {ceiling}, got {measured:?}"
        ));
    }
    Ok(())
}

#[test]
fn production_capture_has_no_outer_file_owner_allocation() -> Result<(), Box<dyn std::error::Error>>
{
    let _subscriber =
        tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    acyclic_native_runtime::initialize_native_io_services()?;
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let setup =
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).unwrap();
    let checkout = runtime.block_on(super::capture_policy_tests::native_checkout(&setup))?;
    let source = tempfile::tempdir()?;
    let file = source.path().join("file");
    std::fs::write(&file, [0x5a])?;
    let root = HostRoot::open(source.path(), &mut crate::native_host::ordinary_work())
        .map_err(|error| std::io::Error::other(format!("{error:?}")))?;
    let path = test_host_path(Path::new("file"));
    let snapshot = test_host_stat(&root, Path::new("file"))?;
    // Pin shared health so unrelated parallel imports cannot change the measured
    // allocation count by sweeping or growing the global weak-entry registry.
    let _health =
        NativeFile::from_file(std::fs::File::open(&file)?, None).map_err(|(error, _)| error)?;
    let run = |old| {
        let _owner = OldOwner(OLD_OWNER.replace(old));
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .unwrap();
        let scope = CaptureWorkScope::new(&work).unwrap();
        let mut result = None;
        let measured = measure(|| {
            let mut bytes = 0;
            let staged = runtime.block_on(stage_regular_body(
                &checkout,
                &root,
                &path,
                &snapshot,
                8,
                &scope.native,
                &mut bytes,
            ));
            result = Some((staged, bytes));
        });
        let (staged, bytes) = result.unwrap();
        let staged = staged.unwrap();
        assert_eq!(bytes, 1);
        assert_eq!(staged.len(), 1);
        assert_eq!(staged[0].1.logical_bytes(), 1);
        drop(staged);
        drop(scope);
        measured
    };
    // Warm the native handle path, executor, source codec and store before counting.
    run(false);
    run(true);
    let borrowed = run(false);
    let old = run(true);
    let repeated = run(false);
    eprintln!("CAPTURE_ALLOC borrowed={borrowed:?} old={old:?} repeated={repeated:?}");
    assert_eq!(borrowed.count_total, repeated.count_total);
    assert_eq!(old.count_total, borrowed.count_total + 1);
    assert_eq!(
        old.bytes_total - borrowed.bytes_total,
        (std::mem::size_of::<NativeFile>() + 2 * std::mem::size_of::<usize>()) as u64
    );
    no_owner_allocation(borrowed).unwrap();
    no_owner_allocation(repeated).unwrap();
    assert!(no_owner_allocation(old).is_err());
    Ok(())
}

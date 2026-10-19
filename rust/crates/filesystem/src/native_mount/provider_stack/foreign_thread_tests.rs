//! ProjFS invokes the provider on Windows-owned threads, whose TLS destructor
//! path differs from a thread created by Rust's test/runtime machinery.
use super::run;
use std::cell::RefCell;
use std::ffi::c_void;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use windows::Win32::Foundation::{HANDLE, WAIT_OBJECT_0};
use windows::Win32::System::Threading::{CreateThread, GetExitCodeThread, WaitForSingleObject};

struct Marker(Arc<AtomicUsize>);
impl Drop for Marker {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

thread_local! {
    static MARKER: RefCell<Option<Marker>> = const { RefCell::new(None) };
}

type Job = Box<dyn FnOnce() + Send>;

unsafe extern "system" fn foreign_entry(parameter: *mut c_void) -> u32 {
    // SAFETY: successful CreateThread transfers this one outer Box to its
    // entry point. Catch every panic before crossing the Windows ABI.
    let job = unsafe { Box::from_raw(parameter.cast::<Job>()) };
    u32::from(std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)).is_err())
}

fn foreign_thread(job: impl FnOnce() + Send + 'static) -> windows::core::Result<()> {
    let parameter = Box::into_raw(Box::new(Box::new(job) as Job));
    // SAFETY: foreign_entry owns the parameter after successful creation; the
    // default stack reserve belongs to Windows and is not changed by this test.
    let handle = match unsafe {
        CreateThread(
            None,
            0,
            Some(foreign_entry),
            Some(parameter.cast()),
            Default::default(),
            None,
        )
    } {
        Ok(handle) => {
            // SAFETY: CreateThread returned a uniquely owned thread handle.
            unsafe { OwnedHandle::from_raw_handle(handle.0) }
        }
        Err(error) => {
            // SAFETY: no entry point can use the parameter after failed creation.
            drop(unsafe { Box::from_raw(parameter) });
            return Err(error);
        }
    };
    let raw = HANDLE(handle.as_raw_handle());
    // SAFETY: the owned thread handle remains live through both queries. A
    // hung regression fails inside the externally bounded runtime suite.
    assert_eq!(unsafe { WaitForSingleObject(raw, 5_000) }, WAIT_OBJECT_0);
    let mut code = 1;
    // SAFETY: code is a writable local and the thread is physically finished.
    unsafe { GetExitCodeThread(raw, &raw mut code) }?;
    assert_eq!(code, 0, "foreign callback thread failed");
    Ok(())
}

#[test]
fn windows_owned_callback_threads_retire_once_and_preserve_other_tls() -> windows::core::Result<()>
{
    for initialize_inside in [false, true] {
        let drops = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&drops);
        foreign_thread(move || {
            let initialize = || {
                MARKER.with(|slot| {
                    assert!(slot.borrow().is_none());
                    *slot.borrow_mut() = Some(Marker(Arc::clone(&observed)));
                })
            };
            if !initialize_inside {
                initialize();
            }
            run(|| {
                run(|| {
                    if initialize_inside {
                        initialize();
                    }
                    assert_eq!(observed.load(Ordering::SeqCst), 0);
                })
                .expect("nested callback preserves its caller fiber");
                let panic =
                    std::panic::catch_unwind(|| run(|| panic!("nested callback panic control")));
                assert!(panic.is_err());
                assert_eq!(observed.load(Ordering::SeqCst), 0);
            })
            .expect("first callback");
            assert_eq!(
                observed.load(Ordering::SeqCst),
                0,
                "fiber retired thread TLS"
            );
            run(|| MARKER.with(|slot| assert!(slot.borrow().is_some())))
                .expect("next callback retains thread state");
            let panic = std::panic::catch_unwind(|| run(|| panic!("callback panic control")));
            assert!(panic.is_err());
            run(|| ()).expect("callback after panic");
            assert_eq!(observed.load(Ordering::SeqCst), 0);
        })?;
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "TLS retired more than once"
        );
    }
    Ok(())
}

#[test]
fn windows_owned_refused_callback_never_bootstraps_runtime() -> windows::core::Result<()> {
    foreign_thread(|| {
        let payload = crate::NativeMaterializationWork::accounting_state_bytes();
        let work = crate::NativeMaterializationWork::new(
            crate::WorkBudget {
                allocation_operations: 1,
                peak_allocation_bytes: payload,
                ..crate::WorkBudget::UNBOUNDED
            },
            &crate::CancellationToken::new(),
        )
        .expect("original accounting state is admitted");
        let before = work.receipt();
        let bootstrapped = std::cell::Cell::new(false);
        let result = super::run_with_runtime_bootstrap(
            Some(&work),
            || {
                bootstrapped.set(true);
                std::thread::current()
            },
            || panic!("refused callback ran"),
        );
        assert!(matches!(result, Err(super::ProviderFailure::Work(_))));
        assert!(!bootstrapped.get(), "refused provider bootstrapped runtime");
        assert_eq!(work.receipt(), before);
    })
}

//! QuickJS's allocator boundary keeps engine bytes on the caller's native ledger.
use rquickjs::allocator::Allocator;
use std::{
    alloc::{Layout, alloc, alloc_zeroed, dealloc, realloc},
    ptr,
};

#[repr(C, align(16))]
struct Header {
    size: usize,
    #[cfg(feature = "filesystem")]
    custody: Option<acyclic_fs::NativeAllocation>,
}

#[derive(Clone)]
pub(super) struct EngineAllocator {
    #[cfg(feature = "filesystem")]
    failure: std::sync::Arc<std::sync::Mutex<Option<acyclic_fs::NativeTreeMaterializationError>>>,
    #[cfg(feature = "filesystem")]
    work: Option<acyclic_fs::NativeMaterializationWork>,
}

// The buffer retires before the ledger credit, including across the RPC channel.
pub(super) struct BridgeText {
    pub(super) value: String,
    _custody: crate::NativeCustody,
}

impl EngineAllocator {
    pub(super) fn copy_text(&self, text: &rquickjs::String<'_>) -> rquickjs::Result<BridgeText> {
        // QuickJS funds the temporary UTF-8 view through this same allocator.
        let text = text.clone().to_cstring()?;
        #[cfg(feature = "filesystem")]
        let custody = self
            .work
            .as_ref()
            .map(|work| work.claim(text.len() as u64, 1, acyclic_fs::WorkCounters::UNCHARGED))
            .transpose()
            .map_err(|error| {
                self.rejected(error);
                rquickjs::Error::Unknown
            })?;
        #[cfg(not(feature = "filesystem"))]
        let custody = None;
        let mut value = String::new();
        value
            .try_reserve_exact(text.len())
            .map_err(|_| rquickjs::Error::Unknown)?;
        value.push_str(text.as_str());
        Ok(BridgeText {
            value,
            _custody: custody,
        })
    }
    pub(super) fn new(work: crate::NativeWork<'_>) -> Self {
        let _ = work;
        Self {
            #[cfg(feature = "filesystem")]
            failure: Default::default(),
            #[cfg(feature = "filesystem")]
            work: work.cloned(),
        }
    }

    #[cfg(feature = "filesystem")]
    pub(super) fn failure(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<Option<acyclic_fs::NativeTreeMaterializationError>>> {
        self.failure.clone()
    }

    #[cfg(feature = "filesystem")]
    fn rejected(&self, error: acyclic_fs::NativeTreeMaterializationError) {
        let mut failure = self
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if failure.is_none() {
            *failure = Some(error);
        }
    }

    fn layout(size: usize) -> Option<Layout> {
        Layout::from_size_align(size.checked_add(size_of::<Header>())?, align_of::<Header>()).ok()
    }

    #[allow(
        unsafe_code,
        reason = "QuickJS allocator FFI: initialize an aligned header and return its disjoint payload"
    )]
    fn allocate(&self, size: usize, zeroed: bool) -> *mut u8 {
        let Some(layout) = Self::layout(size) else {
            return ptr::null_mut();
        };
        #[cfg(feature = "filesystem")]
        let custody = match self
            .work
            .as_ref()
            .map(|work| work.claim(layout.size() as u64, 1, acyclic_fs::WorkCounters::UNCHARGED))
            .transpose()
        {
            Ok(custody) => custody,
            Err(error) => {
                self.rejected(error);
                return ptr::null_mut();
            }
        };
        // SAFETY: layout is nonzero, valid and sufficiently aligned for Header.
        let pointer = unsafe {
            if zeroed {
                alloc_zeroed(layout)
            } else {
                alloc(layout)
            }
        };
        if pointer.is_null() {
            return pointer;
        }
        unsafe {
            pointer.cast::<Header>().write(Header {
                size,
                #[cfg(feature = "filesystem")]
                custody,
            });
            pointer.add(size_of::<Header>())
        }
    }
}

#[allow(
    unsafe_code,
    reason = "QuickJS allocator FFI: every pointer carries this allocator's aligned header and live layout; realloc preserves it and dealloc retires its custody"
)]
unsafe impl Allocator for EngineAllocator {
    fn alloc(&mut self, size: usize) -> *mut u8 {
        self.allocate(size, false)
    }

    fn calloc(&mut self, count: usize, size: usize) -> *mut u8 {
        count
            .checked_mul(size)
            .map_or(ptr::null_mut(), |size| self.allocate(size, true))
    }

    unsafe fn dealloc(&mut self, pointer: *mut u8) {
        let base = unsafe { pointer.sub(size_of::<Header>()) };
        let header = unsafe { base.cast::<Header>().read() };
        if let Some(layout) = Self::layout(header.size) {
            unsafe {
                dealloc(base, layout);
            }
        }
        drop(header);
    }

    unsafe fn realloc(&mut self, pointer: *mut u8, size: usize) -> *mut u8 {
        let base = unsafe { pointer.sub(size_of::<Header>()) };
        let header = unsafe { &mut *base.cast::<Header>() };
        let Some(old_layout) = Self::layout(header.size) else {
            return ptr::null_mut();
        };
        let Some(new_layout) = Self::layout(size) else {
            return ptr::null_mut();
        };
        #[cfg(feature = "filesystem")]
        if let Some(custody) = &mut header.custody
            && size > header.size
            && let Err(error) = custody.grow((size - header.size) as u64)
        {
            self.rejected(error);
            return ptr::null_mut();
        }
        let old_size = header.size;
        let _ = old_size;
        let moved = unsafe { realloc(base, old_layout, new_layout.size()) };
        if moved.is_null() {
            #[cfg(feature = "filesystem")]
            if let Some(custody) = &mut header.custody
                && size > old_size
            {
                if let Err(error) = custody.release((size - old_size) as u64) {
                    self.rejected(error);
                }
            }
            return moved;
        }
        let header = unsafe { &mut *moved.cast::<Header>() };
        #[cfg(feature = "filesystem")]
        if let Some(custody) = &mut header.custody
            && size < old_size
        {
            if let Err(error) = custody.release((old_size - size) as u64) {
                self.rejected(error);
            }
        }
        header.size = size;
        unsafe { moved.add(size_of::<Header>()) }
    }

    unsafe fn usable_size(pointer: *mut u8) -> usize {
        unsafe { (*pointer.sub(size_of::<Header>()).cast::<Header>()).size }
    }
}

#[cfg(all(test, feature = "filesystem"))]
mod tests {
    use super::*;

    fn work(deny_allocation: bool) -> crate::Result<acyclic_fs::NativeMaterializationWork> {
        let mut budget = acyclic_fs::WorkBudget::UNBOUNDED;
        if deny_allocation {
            // Fund the ledger itself, leaving no credit for engine or bridge bytes.
            budget.peak_allocation_bytes =
                acyclic_fs::NativeMaterializationWork::accounting_state_bytes();
        }
        work_with_budget(budget)
    }

    fn work_with_budget(
        budget: acyclic_fs::WorkBudget,
    ) -> crate::Result<acyclic_fs::NativeMaterializationWork> {
        acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new())
            .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })
    }

    #[tokio::test]
    async fn real_engine_releases_native_memory_and_preserves_ledger_refusal() -> crate::Result<()>
    {
        let original = work(false)?;
        let baseline = original.live_allocation_bytes();
        let ledger_operations = original.receipt().allocation_operations;
        let runtime = rquickjs::AsyncRuntime::new_with_alloc(EngineAllocator::new(Some(&original)))
            .expect("funded runtime construction");
        let startup_operations = original.receipt().allocation_operations - ledger_operations;
        drop(runtime);
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert!(startup_operations > 0);
        for admitted in 0..startup_operations {
            let original = work_with_budget(acyclic_fs::WorkBudget {
                allocation_operations: ledger_operations + admitted,
                ..acyclic_fs::WorkBudget::UNBOUNDED
            })?;
            let baseline = original.live_allocation_bytes();
            let allocator = EngineAllocator::new(Some(&original));
            let failure = allocator.failure();
            let runtime = rquickjs::AsyncRuntime::new_with_alloc(allocator);
            // QuickJS can recover an optional allocation failure. Both a
            // returned runtime and a refused startup must retire their bytes.
            let refused = runtime.is_err();
            drop(runtime);
            assert_eq!(
                original.live_allocation_bytes(),
                baseline,
                "runtime allocation prefix {admitted}/{startup_operations}"
            );
            if refused {
                assert!(matches!(
                    failure.lock().unwrap().as_ref(),
                    Some(acyclic_fs::NativeTreeMaterializationError::Work(
                        acyclic_fs::WorkError::BudgetExceeded {
                            counter: "allocation_operations",
                            ..
                        }
                    ))
                ));
            }
        }
        for deny_allocation in [true, false] {
            let work = work(deny_allocation)?;
            let baseline = work.live_allocation_bytes();
            let allocator = EngineAllocator::new(Some(&work));
            let failure = allocator.failure();
            let (send, _receive) = tokio::sync::mpsc::channel(1);
            let result = super::super::interpreter::run(
                "Array.from({length: 1000}, (_, i) => ({i})).length".into(),
                acyclic_stream::OwnedBytes::from_static(b"[]"),
                send,
                tokio_util::sync::CancellationToken::new(),
                4096,
                allocator,
                super::super::interpreter::INTERRUPT_BUDGET,
            )
            .await;
            if deny_allocation {
                assert!(result.is_err());
                assert!(failure.lock().unwrap().is_some());
            } else {
                assert_eq!(result.unwrap(), serde_json::json!(1000));
                assert!(failure.lock().unwrap().is_none());
            }
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        Ok(())
    }
}

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
    failure: std::sync::Arc<std::sync::Mutex<Option<crate::Error>>>,
    #[cfg(feature = "filesystem")]
    work: Option<acyclic_fs::NativeMaterializationWork>,
}

// The buffer retires before the ledger credit, including across the RPC channel.
pub(super) struct BridgeText {
    pub(super) value: String,
    _custody: crate::NativeCustody,
}

// The decoded tree retires before its original allocation credit.
#[derive(Debug)]
pub(super) struct EngineResult {
    pub(super) value: serde_json::Value,
    pub(super) custody: crate::NativeCustody,
}

impl EngineAllocator {
    pub(super) fn decode_result(&self, bytes: &[u8]) -> rquickjs::Result<EngineResult> {
        #[cfg(feature = "filesystem")]
        if let Some(work) = &self.work {
            let decode = || {
                let io = crate::filesystem::PreparationIo::from_native_work(work)?;
                let decoded = io.decode(
                    std::marker::PhantomData::<serde_json::Value>,
                    bytes,
                    crate::wire::ErrorCode::Invalid,
                )?;
                crate::filesystem::native_decoded(decoded)
            };
            return match decode() {
                Ok((value, custody)) => Ok(EngineResult {
                    value,
                    custody: Some(custody),
                }),
                Err(error) => {
                    self.rejected(error);
                    Err(rquickjs::Error::Unknown)
                }
            };
        }
        crate::executor::json::json_from_slice_bounded(
            std::marker::PhantomData::<serde_json::Value>,
            bytes,
            crate::executor::json::JsonBounds {
                maximum_bytes: bytes.len() as u64,
                maximum_items: u64::MAX,
                maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
            },
            None,
            Some(crate::executor::json::JsonValueSchema::Root),
            &mut |_| Ok(()),
        )
        .map(|value| EngineResult {
            value,
            custody: None,
        })
        .map_err(|_| rquickjs::Error::Unknown)
    }

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
    pub(super) fn failure(&self) -> std::sync::Arc<std::sync::Mutex<Option<crate::Error>>> {
        self.failure.clone()
    }

    #[cfg(feature = "filesystem")]
    fn rejected(&self, error: impl Into<crate::Error>) {
        let mut failure = self
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if failure.is_none() {
            *failure = Some(error.into());
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

    fn native_work(bytes: u64) -> crate::Result<acyclic_fs::NativeMaterializationWork> {
        // The ledger owns its own Arc before the allocation under test starts.
        let maximum = acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + bytes;
        acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, maximum),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })
    }

    #[tokio::test]
    async fn bridge_text_keeps_original_credit_and_refuses_an_unfunded_copy() -> crate::Result<()> {
        let runtime = rquickjs::AsyncRuntime::new().unwrap();
        let context = rquickjs::AsyncContext::full(&runtime).await.unwrap();
        for bytes in [1, 8192] {
            let work = native_work(bytes)?;
            let baseline = work.live_allocation_bytes();
            let allocator = EngineAllocator::new(Some(&work));
            context
                .with(|ctx| {
                    let text = rquickjs::String::from_str(ctx, &"x".repeat(4096)).unwrap();
                    let copy = allocator.copy_text(&text);
                    if bytes == 1 {
                        assert!(copy.is_err());
                        assert!(allocator.failure().lock().unwrap().is_some());
                    } else {
                        let copy = copy.unwrap();
                        assert_eq!(copy.value.len(), 4096);
                        assert_eq!(work.live_allocation_bytes(), baseline + 4096);
                        drop(copy);
                    }
                    assert_eq!(work.live_allocation_bytes(), baseline);
                })
                .await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn real_engine_releases_native_memory_and_preserves_ledger_refusal() -> crate::Result<()>
    {
        for bytes in [1, 64 * 1024 * 1024] {
            let work = native_work(bytes)?;
            let baseline = work.live_allocation_bytes();
            let allocator = EngineAllocator::new(Some(&work));
            let failure = allocator.failure();
            let (send, _receive) = tokio::sync::mpsc::channel(1);
            let result = super::super::interpreter::run(
                "export default Array.from({length: 1000}, (_, i) => ({i})).length".into(),
                acyclic_stream::OwnedBytes::from_static(b"[]"),
                send,
                tokio_util::sync::CancellationToken::new(),
                4096,
                allocator,
                super::super::interpreter::INTERRUPT_BUDGET,
            )
            .await;
            if bytes == 1 {
                assert!(result.is_err());
                assert!(matches!(
                    failure.lock().unwrap().as_ref(),
                    Some(crate::Error::NativeWork {
                        cause: acyclic_fs::NativeTreeMaterializationError::Work(
                            acyclic_fs::WorkError::BudgetExceeded {
                                counter: "peak_allocation_bytes",
                                maximum,
                                ..
                            }
                        ), ..
                    }) if *maximum == baseline + bytes
                ));
            } else {
                assert_eq!(result.unwrap().value, serde_json::json!(1000));
                assert!(failure.lock().unwrap().is_none());
            }
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        Ok(())
    }

    #[tokio::test]
    async fn engine_results_retain_original_credit_and_refuse_unfunded_decoding()
    -> crate::Result<()> {
        let runtime = rquickjs::AsyncRuntime::new().unwrap();
        let context = rquickjs::AsyncContext::full(&runtime).await.unwrap();
        for (bytes, cancelled) in [(1, false), (1024 * 1024, false), (1024 * 1024, true)] {
            let work = native_work(bytes)?;
            let baseline = work.live_allocation_bytes();
            let allocator = EngineAllocator::new(Some(&work));
            if cancelled {
                work.cancellation().cancel();
            }
            context.with(|ctx| {
                // Capture the actual engine encoding, including escaped and
                // multi-byte text, rather than supplying a second JSON encoder.
                let json: rquickjs::String = ctx.eval(
                    r#"JSON.stringify({text:'β\\n'.repeat(1024), values:Array.from({length:32}, (_, i) => ({i}))})"#
                ).unwrap();
                let json = json.to_cstring().unwrap();
                let result = allocator.decode_result(json.as_str().as_bytes());
                if cancelled {
                    assert!(result.is_err());
                    assert!(matches!(allocator.failure().lock().unwrap().as_ref(),
                        Some(crate::Error::NativeWork {
                            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
                        })
                    ));
                } else if bytes == 1 {
                    assert!(result.is_err());
                    assert!(matches!(allocator.failure().lock().unwrap().as_ref(),
                        Some(crate::Error::NativeWork {
                            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                                acyclic_fs::WorkError::BudgetExceeded {
                                    counter: "peak_allocation_bytes", ..
                                }
                            ), ..
                        })
                    ));
                } else {
                    let result = result.unwrap();
                    assert_eq!(result.value["text"].as_str().unwrap(), "β\\n".repeat(1024));
                    assert_eq!(result.value["values"][31]["i"], 31);
                    let retained = result.custody.as_ref().unwrap().bytes();
                    assert!(retained > 0);
                    assert_eq!(work.live_allocation_bytes(), baseline + retained);
                    assert!(allocator.failure().lock().unwrap().is_none());
                    drop(result);
                }
                assert_eq!(work.live_allocation_bytes(), baseline);
            }).await;
        }
        // The production interpreter must transfer that same custody out of
        // the runtime; every engine allocation is already retired on return.
        let work = native_work(8 * 1024 * 1024)?;
        let baseline = work.live_allocation_bytes();
        let allocator = EngineAllocator::new(Some(&work));
        let failure = allocator.failure();
        let (send, _receive) = tokio::sync::mpsc::channel(1);
        let result = super::super::interpreter::run(
            "export default {text:'x'.repeat(4096)}".into(),
            acyclic_stream::OwnedBytes::from_static(b"[]"),
            send,
            tokio_util::sync::CancellationToken::new(),
            8192,
            allocator,
            super::super::interpreter::INTERRUPT_BUDGET,
        )
        .await
        .unwrap();
        assert_eq!(result.value["text"].as_str().unwrap().len(), 4096);
        assert_eq!(
            work.live_allocation_bytes(),
            baseline + result.custody.as_ref().unwrap().bytes()
        );
        assert!(failure.lock().unwrap().is_none());
        drop(result);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }
}

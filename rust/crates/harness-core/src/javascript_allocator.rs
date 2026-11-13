//! QuickJS's allocator boundary keeps engine bytes on the caller's native ledger.
use rquickjs::allocator::Allocator;
use std::{
    alloc::{Layout, alloc, alloc_zeroed, dealloc, realloc},
    ptr,
};

#[repr(C, align(16))]
struct Header {
    size: usize,
    custody: Option<acyclic_fs::NativeAllocation>,
}

#[derive(Clone)]
pub(crate) struct EngineAllocator {
    failure: std::sync::Arc<std::sync::Mutex<Option<crate::Error>>>,

    work: Option<acyclic_fs::NativeMaterializationWork>,
}

// The buffer retires before the ledger credit, including across the RPC channel.
pub(crate) struct BridgeText {
    pub(crate) value: String,
    _custody: acyclic_fs::NativeCustody,
}

// The decoded tree retires before its original allocation credit.
#[derive(Debug)]
pub(crate) struct EngineResult<T = serde_json::Value> {
    pub(crate) value: T,
    pub(crate) custody: acyclic_fs::NativeCustody,
}

pub(crate) fn transfer<T>(
    custody: &mut acyclic_fs::NativeCustody,
    bytes: u64,
    target: &mut EngineResult<T>,
) -> crate::Result<()> {
    if let Some(custody) = custody {
        match &mut target.custody {
            Some(target) => custody.transfer_to(target, bytes)?,
            None => target.custody = Some(custody.split_off(bytes)?),
        }
    }
    Ok(())
}
impl<T> EngineResult<T> {
    pub(crate) fn absorb(&mut self, mut other: acyclic_fs::NativeCustody) -> crate::Result<()> {
        if let (Some(custody), Some(incoming)) = (&mut self.custody, &mut other) {
            if let Err(cause) = custody.absorb(incoming) {
                return Err(crate::Error::NativeWork {
                    cause,
                    custody: other,
                });
            }
        } else if self.custody.is_none() {
            self.custody = other;
        }
        Ok(())
    }

    pub(crate) fn share<R>(
        mut self,
        tag: R,
        work: acyclic_fs::NativeWork<'_>,
    ) -> crate::Result<std::sync::Arc<(Self, R)>> {
        if let Some(work) = work {
            let bytes =
                acyclic_fs::NativeMaterializationWork::shared_allocation_bytes::<(Self, R)>()?
                    - size_of::<T>() as u64;
            let mut metadata = work.claim(bytes, 1, acyclic_fs::WorkCounters::UNCHARGED)?;
            match &mut self.custody {
                Some(custody) => custody.absorb(&mut metadata)?,
                None => self.custody = Some(metadata),
            }
        }
        Ok(std::sync::Arc::new((self, tag)))
    }
}

impl EngineAllocator {
    pub(crate) fn decode_result(&self, bytes: &[u8]) -> rquickjs::Result<EngineResult> {
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
        let decoded = crate::executor::json::json_from_slice_bounded(
            std::marker::PhantomData::<serde_json::Value>,
            bytes,
            crate::executor::json::JsonBounds {
                maximum_bytes: Some(bytes.len() as u64),
                maximum_items: None,
                maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
            },
            Some(crate::wire::ErrorCode::Invalid),
            Some(&crate::executor::json::JsonValueSchema::Root),
            &mut |_| Ok(()),
        )
        .map(|value| EngineResult {
            value,
            custody: None,
        });
        decoded.map_err(|error| {
            self.rejected(error);
            rquickjs::Error::Unknown
        })
    }

    pub(crate) fn copy_text(&self, text: &rquickjs::String<'_>) -> rquickjs::Result<BridgeText> {
        // QuickJS funds the temporary UTF-8 view through this same allocator.
        let text = text.clone().to_cstring()?;
        let custody = self
            .work
            .as_ref()
            .map(|work| work.claim(text.len() as u64, 1, acyclic_fs::WorkCounters::UNCHARGED))
            .transpose()
            .map_err(|error| {
                self.rejected(error);
                rquickjs::Error::Unknown
            })?;
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
    pub(crate) fn new(work: acyclic_fs::NativeWork<'_>) -> Self {
        let _ = work;
        Self {
            failure: Default::default(),
            work: work.cloned(),
        }
    }

    pub(crate) fn failure(&self) -> std::sync::Arc<std::sync::Mutex<Option<crate::Error>>> {
        self.failure.clone()
    }

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
            pointer.cast::<Header>().write(Header { size, custody });
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

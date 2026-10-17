use acyclic_fs::{NativeAllocation, WorkCounters};
use acyclic_harness::Result;
use bytes::Bytes;

struct Owned<T> {
    bytes: T,
    _custody: NativeAllocation,
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for Owned<T> {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

/// Attaches paid payload custody without copying its bytes. The Bytes owner
/// control allocation is admitted before Bytes constructs that allocation.
pub(crate) fn owned<T: AsRef<[u8]> + Send + 'static>(
    bytes: T,
    mut custody: NativeAllocation,
) -> Result<Bytes> {
    let (layout, _) = std::alloc::Layout::new::<std::sync::atomic::AtomicUsize>()
        .extend(std::alloc::Layout::new::<Owned<T>>())
        .map_err(|_| acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "provider Bytes owner size overflows",
        })?;
    let mut control = custody.work().claim(
        layout.pad_to_align().size() as u64,
        1,
        WorkCounters::UNCHARGED,
    )?;
    custody.absorb(&mut control)?;
    Ok(Bytes::from_owner(Owned {
        bytes,
        _custody: custody,
    }))
}

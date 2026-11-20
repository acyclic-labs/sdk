//! One admitted body reader for bounded JSON exchanges.
use crate::boundary;
use acyclic_harness_core::Result;
use futures::StreamExt as _;
use zeroize::Zeroizing;

/// A body read under an admitted byte budget, with the custody that paid for it.
pub struct Body {
    /// The bytes read, scrubbed when dropped.
    pub bytes: Zeroizing<Vec<u8>>,
    /// The native allocation hold the read charged.
    #[cfg(not(target_arch = "wasm32"))]
    pub custody: acyclic_fs::NativeCustody,
}

/// Reads BODY to its end, refusing past MAXIMUM bytes and charging WORK for every copy.
pub async fn read(
    mut body: acyclic_stream::BoxProviderStream<'static, Result<bytes::Bytes>>,
    maximum: impl Into<Option<usize>>,
    work: acyclic_fs::NativeWork<'_>,
) -> Result<Body> {
    let maximum = maximum.into();
    #[cfg(not(target_arch = "wasm32"))]
    let mut custody = work
        .map(|work| work.claim(64, 1, acyclic_fs_work::performance::WorkCounters::UNCHARGED))
        .transpose()?;
    let mut bytes = Zeroizing::new(Vec::new());
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        let next = bytes
            .len()
            .checked_add(chunk.len())
            .filter(|n| maximum.is_none_or(|maximum| *n <= maximum))
            .ok_or_else(|| boundary(work, "HTTP JSON response exceeds byte limit"))?;
        if next > bytes.capacity() {
            let previous = bytes.capacity();
            let growth = next.max(previous.saturating_mul(2));
            let capacity = maximum.map_or(growth, |maximum| growth.min(maximum));
            #[cfg(not(target_arch = "wasm32"))]
            if let (Some(work), Some(hold)) = (work, custody.as_mut()) {
                let mut growth = work.claim(
                    capacity as u64,
                    1,
                    acyclic_fs_work::performance::WorkCounters {
                        bytes_copied: bytes.len() as u64,
                        ..Default::default()
                    },
                )?;
                hold.absorb(&mut growth)?;
            }
            acyclic_auth::operation::reserve_protected_bytes(&mut bytes, capacity).map_err(
                |cause| {
                    #[cfg(not(target_arch = "wasm32"))]
                    if work.is_some() {
                        return acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause)
                            .into();
                    }
                    let _ = cause;
                    boundary(work, "cannot allocate HTTP JSON response")
                },
            )?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(hold) = custody.as_mut() {
                // The replacement's full claim precedes allocation. Scrub the
                // original backing before releasing its allocation credit.
                hold.release(previous as u64)?;
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            work.charge(acyclic_fs_work::performance::WorkCounters {
                bytes_copied: chunk.len() as u64,
                items_examined: 1,
                ..Default::default()
            })?;
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(Body {
        bytes,
        #[cfg(not(target_arch = "wasm32"))]
        custody,
    })
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "bounded_tests.rs"]
mod tests;

//! One admitted body reader for bounded JSON exchanges.
use acyclic_harness_core::{Error, NativeWork, Result};
use futures::StreamExt as _;
use zeroize::Zeroizing;

pub(crate) struct Body {
    pub bytes: Zeroizing<Vec<u8>>,
    #[cfg(not(target_arch = "wasm32"))]
    pub custody: acyclic_harness_core::NativeCustody,
}

fn boundary(work: NativeWork<'_>, reason: &'static str) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if work.is_some() {
        return Error::NativeBoundary {
            code: acyclic_harness_core::wire::ErrorCode::Invalid,
            reason,
        };
    }
    let _ = work;
    crate::invalid(reason)
}

pub(crate) async fn read(
    mut body: acyclic_stream::BoxProviderStream<'static, Result<bytes::Bytes>>,
    maximum: Option<usize>,
    work: NativeWork<'_>,
) -> Result<Body> {
    #[cfg(not(target_arch = "wasm32"))]
    let mut custody = work
        .map(|work| work.claim(64, 1, acyclic_fs::WorkCounters::UNCHARGED))
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
            let capacity = next.max(previous.saturating_mul(2));
            let capacity = maximum.map_or(capacity, |maximum| capacity.min(maximum));
            #[cfg(not(target_arch = "wasm32"))]
            if let (Some(work), Some(hold)) = (work, custody.as_mut()) {
                let mut growth = work.claim(
                    capacity as u64,
                    1,
                    acyclic_fs::WorkCounters {
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
            work.charge(acyclic_fs::WorkCounters {
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

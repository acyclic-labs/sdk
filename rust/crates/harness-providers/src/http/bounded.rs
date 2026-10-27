//! One admitted body reader for bounded JSON exchanges.
use acyclic_harness_core::{Error, NativeWork, Result};
use futures::StreamExt as _;

pub(crate) struct Body {
    pub bytes: Vec<u8>,
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
    maximum: usize,
    work: NativeWork<'_>,
) -> Result<Body> {
    #[cfg(not(target_arch = "wasm32"))]
    let mut custody = work
        .map(|work| work.claim(64, 1, acyclic_fs::WorkCounters::UNCHARGED))
        .transpose()?;
    let mut bytes = Vec::new();
    let mut chunks = 0usize;
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        chunks = chunks
            .checked_add(1)
            .filter(|count| *count <= maximum)
            .ok_or_else(|| boundary(work, "HTTP JSON response exceeds item limit"))?;
        let next = bytes
            .len()
            .checked_add(chunk.len())
            .filter(|n| *n <= maximum)
            .ok_or_else(|| boundary(work, "HTTP JSON response exceeds byte limit"))?;
        if next > bytes.capacity() {
            let capacity = next
                .checked_next_power_of_two()
                .unwrap_or(maximum)
                .min(maximum);
            #[cfg(not(target_arch = "wasm32"))]
            if let (Some(work), Some(hold)) = (work, custody.as_mut()) {
                let mut growth = work.claim(
                    (capacity - bytes.capacity()) as u64,
                    1,
                    acyclic_fs::WorkCounters {
                        bytes_copied: bytes.len() as u64,
                        ..Default::default()
                    },
                )?;
                hold.absorb(&mut growth)?;
            }
            bytes.reserve_exact(capacity - bytes.len());
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

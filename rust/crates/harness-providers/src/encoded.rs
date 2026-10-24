//! One immutable wire projection for counting and generation, scoped to its request.
use crate::{ResolvedModel, files::Files, projection};
use acyclic_harness_core::{
    NativeCustody, NativeWork, Result,
    model::{PreparedModelRequest, ReasoningOrigin},
};
use bytes::Bytes;
use std::sync::Arc;

// Native waiters live inside their already funded futures; there is no heap
// waiter table whose allocation could escape the original work ledger.
#[cfg(not(target_arch = "wasm32"))]
type Mutex<T> = tokio::sync::Mutex<T>;
#[cfg(target_arch = "wasm32")]
type Mutex<T> = futures::lock::Mutex<T>;

#[cfg(test)]
std::thread_local! {
    static PROJECTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "encoded_tests.rs"]
mod tests;

#[derive(Clone)]
pub(crate) struct Bodies {
    pub generation: Bytes,
    pub count: Bytes,
    pub verified: Arc<Verification>,
}

pub(crate) struct Verification {
    pub tokens: Mutex<Option<u64>>,
    _custody: NativeCustody,
}

struct Encoding {
    config: Arc<crate::ProviderSpec>,
    files: Files,
    origin: ReasoningOrigin,
    key: Option<Arc<str>>,
    transport: Arc<crate::deadline::Bounded>,
    #[cfg(not(target_arch = "wasm32"))]
    work: Option<acyclic_fs::NativeMaterializationWork>,
    bodies: Bodies,
    _custody: NativeCustody,
}

struct Memo {
    encoding: Mutex<Option<Encoding>>,
    _custody: NativeCustody,
}

impl Encoding {
    fn matches(
        &self,
        provider: &ResolvedModel,
        origin: &ReasoningOrigin,
        work: NativeWork<'_>,
    ) -> bool {
        let reader = match (&self.files.reader, &provider.files.reader) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };
        #[cfg(not(target_arch = "wasm32"))]
        let scope = match (&self.work, work) {
            (Some(a), Some(b)) => a.shares_materialization_scope(b),
            (None, None) => true,
            _ => false,
        };
        #[cfg(target_arch = "wasm32")]
        let scope = work.is_none();
        Arc::ptr_eq(&self.config, &provider.config)
            && reader
            && Arc::ptr_eq(&self.transport, &provider.transport)
            && match (&self.key, &provider.key) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            }
            && self.files.maximum_bytes == provider.files.maximum_bytes
            && &self.origin == origin
            && scope
    }
}

pub(crate) async fn bodies(
    provider: &ResolvedModel,
    request: &PreparedModelRequest,
    names: &crate::tool_names::Names<'_>,
    origin: &ReasoningOrigin,
    work: NativeWork<'_>,
) -> Result<Bodies> {
    provider.validate(&request.request().model)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = work {
        work.remaining()?;
    }
    let memo = request.provider_memo(|| {
        // Pay for the box before constructing it. Native mutex waiters are intrusive.
        #[cfg(not(target_arch = "wasm32"))]
        let custody = work
            .map(|work| {
                work.claim(
                    std::mem::size_of::<Memo>() as u64,
                    1,
                    acyclic_fs::WorkCounters::UNCHARGED,
                )
            })
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let custody = None;
        Ok(Memo {
            encoding: Mutex::new(None),
            _custody: custody,
        })
    })?;
    // File resolution is inside the single flight as well as wire projection.
    let mut encoding = memo.encoding.lock().await;
    if let Some(value) = encoding
        .as_ref()
        .filter(|value| value.matches(provider, origin, work))
    {
        return Ok(value.bodies.clone());
    }
    let value = project(provider, request, names, origin, work).await?;
    let bodies = value.bodies.clone();
    if encoding.is_none() {
        *encoding = Some(value);
    }
    // A foreign binding re-verifies its content and retains no second cache entry.
    Ok(bodies)
}

async fn project(
    provider: &ResolvedModel,
    request: &PreparedModelRequest,
    names: &crate::tool_names::Names<'_>,
    origin: &ReasoningOrigin,
    work: NativeWork<'_>,
) -> Result<Encoding> {
    #[cfg(test)]
    PROJECTIONS.with(|count| count.set(count.get() + 1));
    #[cfg(not(target_arch = "wasm32"))]
    let mut custody = work
        .map(|work| {
            let (mut bytes, nodes) = crate::native::shape(request.bytes());
            for message in &request.request().messages {
                bytes = bytes
                    .checked_add(crate::files::extra_bytes(
                        &message.content,
                        provider.files.maximum_bytes,
                    )? as u64)
                    .ok_or_else(|| crate::invalid("model file size overflow"))?;
            }
            crate::native::reserve(work, bytes, nodes, 0)
        })
        .transpose()?;
    #[cfg(not(target_arch = "wasm32"))]
    let io = work
        .map(acyclic_harness_core::filesystem::PreparationIo::from_native_work)
        .transpose()?;
    #[cfg(target_arch = "wasm32")]
    let io = None;
    #[cfg(target_arch = "wasm32")]
    let mut custody = None;
    let projected = async {
        let mut files =
            crate::files::resolve(&provider.files, request.request(), io.as_ref()).await?;
        let body = projection::body_named(provider, request.request(), names, origin, &mut files)?;
        let generation = provider.prefix_cache.encode(body, work, &mut custody)?;
        let count = match &provider.config.token_counter {
            Some(spec) => crate::counting::wire::encoded_body(
                &generation,
                &provider.model.name,
                spec.adapter,
            )?,
            None => Vec::new(),
        };
        Ok::<_, acyclic_harness_core::Error>((generation, count))
    }
    .await;
    #[cfg(not(target_arch = "wasm32"))]
    let projected = match custody.as_mut() {
        Some(hold) => crate::native::diagnostic(projected, hold),
        None => projected,
    };
    let (generation, count) = projected?;
    let bodies = own_bodies(generation, count, work, &mut custody)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(hold) = custody.as_mut() {
        crate::native::retain(
            hold,
            (std::mem::size_of::<Encoding>() + origin.provider.capacity() + origin.model.capacity())
                as u64,
        )?;
    }
    #[cfg(target_arch = "wasm32")]
    let custody = None;
    Ok(Encoding {
        config: provider.config.clone(),
        files: provider.files.clone(),
        origin: origin.clone(),
        key: provider.key.clone(),
        transport: provider.transport.clone(),
        #[cfg(not(target_arch = "wasm32"))]
        work: work.cloned(),
        bodies,
        _custody: custody,
    })
}

fn own_bodies(
    generation: Vec<u8>,
    count: Vec<u8>,
    work: NativeWork<'_>,
    custody: &mut NativeCustody,
) -> Result<Bodies> {
    #[cfg(not(target_arch = "wasm32"))]
    let verification_custody = work
        .map(|work| {
            work.claim(
                (std::mem::size_of::<Verification>() + 2 * std::mem::size_of::<usize>()) as u64,
                1,
                acyclic_fs::WorkCounters::UNCHARGED,
            )
        })
        .transpose()?;
    #[cfg(target_arch = "wasm32")]
    let verification_custody = None;
    let verified = Arc::new(Verification {
        tokens: Mutex::new(None),
        _custody: verification_custody,
    });
    #[cfg(not(target_arch = "wasm32"))]
    let bodies = match custody.as_mut() {
        Some(hold) => {
            let generation_hold = hold.split_off(generation.capacity() as u64)?;
            let count_hold = hold.split_off(count.capacity() as u64)?;
            Bodies {
                generation: crate::http::owned_bytes::owned(generation, generation_hold)?,
                count: crate::http::owned_bytes::owned(count, count_hold)?,
                verified,
            }
        }
        None => Bodies {
            generation: generation.into(),
            count: count.into(),
            verified,
        },
    };
    #[cfg(target_arch = "wasm32")]
    let bodies = Bodies {
        generation: generation.into(),
        count: count.into(),
        verified,
    };
    #[cfg(target_arch = "wasm32")]
    let _ = (work, custody);
    Ok(bodies)
}

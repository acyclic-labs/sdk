//! Pinned, bounded reads from the existing authoritative aggregate Stream.

use crate::{
    Error, Result,
    core::{Authority, AuthorityVerifier, Event},
};

use acyclic_stream::{StreamPath, StreamProvider};

use serde::{Deserialize, Serialize};

#[cfg(feature = "filesystem")]
mod measured;

#[cfg(feature = "filesystem")]
pub use measured::HistoryWork;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use native::{AcceptedCompletionIo, adopt_native_sdk_output, native_sdk_result};

/// The native branch owns its original pre-funded frame outside the boxed future.
pub enum HistoryContentFuture<'a, T> {
    Ordinary {
        future: acyclic_stream::BoxProviderFuture<'a, Result<T>>,
        frame_bytes: u64,
    },
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Native(crate::NativeFutureBundle<'a, T>),
}

impl<T> Future for HistoryContentFuture<'_, T> {
    type Output = Result<T>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.get_mut() {
            Self::Ordinary { future, .. } => future.as_mut().poll(cx),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::Native(future) => std::pin::Pin::new(future).poll(cx),
        }
    }
}

impl<T> HistoryContentFuture<'_, T> {
    fn ordinary_frame_bytes(&self) -> u64 {
        match self {
            Self::Ordinary { frame_bytes, .. } => *frame_bytes,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::Native(_) => 0,
        }
    }
}

pub(crate) async fn verify_content_with_work(
    observer: &mut dyn HistoryIo,
    verifier: &dyn crate::conversation::ContentResidencyVerifier,
    reference: &crate::conversation::FileRef,
) -> Result<()> {
    observer.verify_content(verifier, reference)?.await
}

pub(crate) async fn read_content_with_work(
    observer: &mut dyn HistoryIo,
    verifier: &dyn crate::conversation::ContentResidencyVerifier,
    reference: &crate::conversation::FileRef,
) -> Result<(acyclic_stream::OwnedBytes, u64)> {
    let bytes = observer.read_content(verifier, reference)?.await?;
    let held = if super::operations::original_native_decode_mode(observer) {
        0
    } else {
        bytes.retained_capacity().unwrap_or(0) as u64
    };
    if let Err(error) = observer.retain_owned(held) {
        drop(bytes);
        return Err(error);
    }
    Ok((bytes, held))
}

#[doc(hidden)]
/// Observes bounded aggregate work.
pub trait HistoryIo: Send {
    /// Check cancellation.
    fn check_cancellation(&self) -> Result<()>;
    /// Begin call.
    fn begin_call(&mut self) -> Result<()>;
    /// Begin write.
    fn begin_write(&mut self) -> Result<()>;
    /// Receive record.
    fn receive_record(&mut self, bytes: u64) -> Result<()>;
    /// Records the SDK-returned authority prefix when the genuine native SDK already owns raw buffers and accounts its source IO and item visits.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn receive_native_record(&mut self, bytes: u64) -> Result<()> {
        let Some(original) = self.original_native_work() else {
            return self.receive_record(bytes);
        };
        original
            .spent(acyclic_fs::WorkCounters {
                authority_records_read: 1,
                authority_bytes_read: bytes,
                ..acyclic_fs::WorkCounters::default()
            })
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
        original.remaining().map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
        Ok(())
    }
    /// Retain owned.
    fn retain_owned(&mut self, bytes: u64) -> Result<()>;
    /// Admit owned.
    fn admit_owned(&self, bytes: u64) -> Result<()>;
    /// Release owned.
    fn release_owned(&mut self, bytes: u64) -> Result<()>;
    /// Allocation failure.
    fn allocation_failure(&self, cause: std::collections::TryReserveError) -> Error {
        Error::Storage(cause.to_string())
    }
    /// Clone authority.
    fn clone_authority(&mut self, authority: &Authority) -> Result<Authority>;
    /// Clone text.
    fn clone_text(&mut self, value: &str) -> Result<String>;

    /// Reserve text.
    fn reserve_text(&mut self, text: &mut String, capacity: usize) -> Result<()>;
    /// Records the attempt after its storage has been admitted.
    fn allocation_attempt(&mut self, bytes: u64) -> Result<()>;
    /// Admit allocation.
    fn admit_allocation(&mut self, bytes: u64) -> Result<()> {
        self.admit_owned(bytes)?;
        self.allocation_attempt(bytes)
    }
    /// Admit copy.
    fn admit_copy(&self, bytes: u64) -> Result<()>;
    /// Copy bytes.
    fn copy_bytes(&mut self, bytes: u64) -> Result<()>;
    /// Encoded bytes.
    fn encoded_bytes(&mut self, bytes: u64) -> Result<()>;
    /// Admit encoding.
    fn admit_encoding(&self, bytes: u64) -> Result<()>;
    /// Codec charge.
    fn codec_charge(&mut self, charge: crate::executor::json::JsonCodecCharge) -> Result<()>;
    /// Decode json value.
    fn decode_json_value(&mut self, _bytes: &[u8]) -> Result<(serde_json::Value, u64)> {
        Err(Error::Unsupported(
            "this history observer has no guarded JSON value decoder".into(),
        ))
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Original native work.
    fn original_native_work(&self) -> Option<&acyclic_fs::NativeMaterializationWork> {
        None
    }
    /// Identity alone cannot dispatch another native call for an accepted completion; its caller must supply the sealed pre-action SDK token.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn requires_prepared_native_dispatch(&self) -> bool {
        false
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Absorb native custody.
    fn absorb_native_custody(&mut self, incoming: acyclic_fs::NativeAllocation) -> Result<()> {
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            custody: Some(incoming),
        })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Take native custody.
    fn take_native_custody(&mut self, _bytes: u64) -> Result<acyclic_fs::NativeAllocation> {
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            custody: None,
        })
    }
    /// Hashed bytes.
    fn hashed_bytes(&mut self, bytes: u64) -> Result<()>;
    /// Admit acknowledgment.
    fn admit_acknowledgment(&self, bytes: u64, count: u64) -> Result<()>;
    /// Acknowledged.
    fn acknowledged(&mut self, bytes: u64, count: u64) -> Result<()>;
    /// Admit output.
    fn admit_output(&self, bytes: u64, count: u64) -> Result<()>;
    /// Stage output.
    fn stage_output(&mut self, bytes: u64, count: u64) -> Result<()>;
    /// Return output.
    fn return_output(&mut self, bytes: u64, count: u64) -> Result<()>;
    /// Verify content.
    fn verify_content<'a>(
        &'a mut self,
        verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>>;
    /// Read content.
    fn read_content<'a>(
        &'a mut self,
        verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>>;
    #[cfg(feature = "filesystem")]
    /// Remaining.
    fn remaining(&self) -> Result<acyclic_fs::WorkBudget>;
    #[cfg(feature = "filesystem")]
    /// Cancellation.
    fn cancellation(&self) -> &acyclic_fs::CancellationToken;
    #[cfg(feature = "filesystem")]
    /// Receive work.
    fn receive_work(&mut self, work: acyclic_fs::WorkCounters) -> Result<()>;
}

pub(crate) fn reserve<T>(
    values: &mut Vec<T>,
    capacity: usize,
    observer: &mut dyn HistoryIo,
) -> Result<()> {
    let old = values.capacity();
    if capacity <= old {
        return Ok(());
    }
    let extra = (capacity - old)
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| Error::Invalid("history vector storage overflows".into()))?
        as u64;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if observer.requires_prepared_native_dispatch() {
        return observer.allocation_attempt(extra);
    }
    observer.retain_owned(extra)?;
    let reserved = observer.allocation_attempt(extra).and_then(|()| {
        values
            .try_reserve_exact(capacity - values.len())
            .map_err(|cause| observer.allocation_failure(cause))
    });
    if let Err(error) = reserved {
        observer.release_owned(extra)?;
        return Err(error);
    }
    let actual = (values.capacity() - old)
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| Error::Invalid("history vector capacity overflows".into()))?
        as u64;
    if actual > extra {
        observer.retain_owned(actual - extra)
    } else {
        observer.release_owned(extra - actual)
    }
}

pub(crate) fn ensure_slot<T>(
    values: &mut Vec<T>,
    maximum: usize,
    observer: &mut dyn HistoryIo,
) -> Result<()> {
    if values.len() < values.capacity() {
        return Ok(());
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if observer.requires_prepared_native_dispatch() {
        return observer.allocation_attempt(1);
    }
    let capacity = values
        .capacity()
        .checked_mul(2)
        .unwrap_or(maximum)
        .max(4)
        .min(maximum);
    if capacity <= values.capacity() {
        return Err(Error::Invalid(
            "history vector exceeds declared maximum".into(),
        ));
    }
    reserve(values, capacity, observer)
}

/// Explicit archival position. Revisions are inclusive; later appends are excluded.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCursor {
    /// Aggregate whose admission attestations must verify.
    pub authority: Authority,
    /// Last event already consumed (zero starts at the first event).
    pub after_revision: u64,
    /// Fixed committed boundary for this traversal.
    pub through_revision: u64,
}

/// Finite work and output bounds for one history read.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct HistoryReadLimits {
    /// Maximum number of decoded events.
    pub maximum_events: u32,
    /// Maximum total encoded record bytes, checked before decoding each record.
    #[cfg_attr(feature = "wasm", tsify(type = "bigint"))]
    pub maximum_bytes: u64,
}

/// One verified page and its resumable position. An empty page is terminal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryPage {
    /// Ordered, gapless events attested by this aggregate's owner.
    pub events: Vec<Event>,
    /// Same pinned boundary, advanced only over returned events.
    pub cursor: HistoryCursor,
}

/// An owner-bound Stream handle, with no reducer, history cache or replay on open.
pub struct HistoryReader<P> {
    client: std::sync::Arc<P>,
    pub(in crate::store) path: StreamPath,
    pub(in crate::store) verifier: AuthorityVerifier,
}

pub(crate) fn verify_history_record_with_retained_quota(
    verifier: &AuthorityVerifier,
    authority: &Authority,
    sequence: u64,
    record: &acyclic_stream::Record,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(Event, u64)> {
    if record.sequence != sequence || record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(super::guarded_boundary(
            work,
            crate::wire::ErrorCode::Storage,
            "history record position or byte bound is invalid",
        ));
    }
    let ((record_authority, event), owned_event) =
        crate::wire_codec::decode_event(&record.value, work)?;
    let verification = (|| {
        let revision = sequence.checked_add(1).ok_or_else(|| {
            super::guarded_boundary(
                work,
                crate::wire::ErrorCode::Storage,
                "history event revision overflows",
            )
        })?;
        if &record_authority != authority || event.revision != revision {
            return Err(super::guarded_boundary(
                work,
                crate::wire::ErrorCode::Storage,
                "history event binding is invalid",
            ));
        }
        verifier.verify_audience(authority, &mut None)?;
        verifier.verify_event(&event, work)
    })();
    let authority_bytes = record_authority.id.capacity() as u64;
    drop(record_authority);
    if let Err(error) = verification {
        drop(event);
        if let Some(work) = work.as_deref_mut() {
            let _ = work.release_owned(authority_bytes + owned_event);
        }
        return Err(error);
    }
    if let Some(work) = work.as_deref_mut() {
        work.release_owned(authority_bytes)?;
    }
    Ok((event, owned_event))
}

/// The original canonical codec, admitting logical storage before growth.
pub(crate) fn canonical_bytes_with_work<T: Serialize + ?Sized>(
    value: &T,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<u8>> {
    if work.is_none() {
        return crate::contract::canonical_json_bytes(value);
    }
    if maximum_bytes == 0 || maximum_bytes == u64::MAX {
        return Err(super::guarded_boundary(
            work,
            crate::wire::ErrorCode::Invalid,
            "canonical encoder allowance must be finite and positive",
        ));
    }
    canonical_bytes_for_bounds_with_work(
        value,
        crate::executor::json::JsonBounds {
            maximum_bytes,
            maximum_items: maximum_bytes.saturating_add(1),
            maximum_depth: 256,
        },
        work,
    )
}

pub(crate) fn canonical_bytes_for_bounds_with_work<T: Serialize + ?Sized>(
    value: &T,
    bounds: crate::executor::json::JsonBounds,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<u8>> {
    let Some(work) = work.as_deref_mut() else {
        return crate::contract::canonical_json_bytes(value);
    };
    let code = super::operations::original_native_decode_mode(work)
        .then_some(crate::wire::ErrorCode::Invalid);
    let bytes =
        crate::executor::json::canonical_json_bytes_bounded(value, bounds, code, &mut |charge| {
            work.codec_charge(charge)
        })?;
    if let Err(error) = work.retain_owned((bytes.capacity() - bytes.len()) as u64) {
        let size = bytes.len() as u64;
        drop(bytes);
        let _ = work.release_owned(size);
        return Err(error);
    }
    Ok(bytes)
}

mod conversation;
mod forks;
mod pages;

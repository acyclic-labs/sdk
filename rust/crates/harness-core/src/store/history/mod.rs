//! Pinned, bounded reads from the existing authoritative aggregate Stream.

use crate::{Error, Result, core::Authority};

use serde::{Deserialize, Serialize};

#[cfg(feature = "filesystem")]
mod measured;

#[cfg(feature = "filesystem")]
pub use measured::HistoryWork;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use native::{adopt_native_sdk_output, native_sdk_result};

/// The native branch owns its original pre-funded frame outside the boxed future.
pub enum HistoryContentFuture<'a, T> {
    /// An observed ordinary callback.
    Ordinary {
        /// Callback frame.
        future: acyclic_stream::BoxProviderFuture<'a, Result<T>>,
        /// Retained frame size.
        frame_bytes: u64,
    },
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// An originally funded native callback.
    Native(crate::NativeFutureBundle<'a, T>),
}

impl<'a, T> HistoryContentFuture<'a, T> {}

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
    /// Transfers retained output ownership, or declines retention on this observer.
    #[cfg(feature = "filesystem")]
    fn take_owned_custody(
        &mut self,
        bytes: u64,
    ) -> Result<Option<crate::filesystem::PreparationCustody>> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.original_native_work().is_some() {
            return self
                .take_native_custody(bytes)
                .map(crate::filesystem::PreparationCustody::Native)
                .map(Some);
        }
        let _ = bytes;
        Ok(None)
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
    /// Remaining.
    fn remaining(&self) -> Result<acyclic_fs::WorkBudget>;
    /// Cancellation.
    fn cancellation(&self) -> &acyclic_fs::CancellationToken;
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

/// Finite work and output bounds for one history read.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct HistoryReadLimits {
    /// Maximum number of decoded events.
    pub maximum_events: u32,
    /// Maximum total encoded record bytes, checked before decoding each record.
    #[cfg_attr(feature = "wasm", tsify(type = "bigint"))]
    pub maximum_bytes: u64,
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
    // The encoder returns its buffer with the whole capacity already retained;
    // callers release exactly `capacity()`.
    crate::executor::json::canonical_json_bytes_bounded(value, bounds, code, &mut |charge| {
        work.codec_charge(charge)
    })
}

crate::core::ownership::owned_struct!(HistoryReadLimits {
    maximum_events,
    maximum_bytes
});

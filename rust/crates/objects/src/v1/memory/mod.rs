use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex};
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

use super::Guarded;
use super::{Error, Object, ObjectsProvider, request, wire};
#[cfg(not(target_arch = "wasm32"))]
use super::{
    NativeObjectsCause, NativeObjectsError, NativeObjectsSharedFailure, ObjectsBatch,
    ObjectsNativeFuture, ObjectsNativeRequest, ObjectsPutReceipt, native_future,
    native_input_meter, native_preparation_error,
};
use crate::body::{BodyReadError, BodyReadMode, StoredBody};
use crate::obs;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
use acyclic_stream::OwnedBytes;
use imbl::OrdMap;
use prost::Message;
use wire::ErrorCode::{
    AlreadyExists, IdempotencyMismatch, InvalidArgument, NotFound, NotModified, PreconditionFailed,
    QuotaExceeded, Unavailable,
};
#[cfg(feature = "local")]
pub(super) mod persistence;

// Publish a key only after the entropy source fills the complete candidate.
fn fresh_cursor_key(
    fill: impl FnOnce(&mut [u8]) -> Result<(), getrandom::Error>,
) -> Result<[u8; 32], wire::ErrorCode> {
    let mut key = [0; 32];
    fill(&mut key).map_err(|_| Unavailable)?;
    Ok(key)
}

/// Allocation and cardinality limits for the in-process reference provider.
#[derive(Clone, Copy, Debug)]
pub struct MemoryOptions {
    /// Maximum combined bytes in current objects and staged multipart parts.
    pub maximum_bytes: usize,
    /// Maximum number of buckets, current objects, staged uploads, and retry receipts.
    pub maximum_entries: usize,
}
impl Default for MemoryOptions {
    fn default() -> Self {
        Self {
            maximum_bytes: 64 * 1024 * 1024,
            maximum_entries: 10_000,
        }
    }
}

/// Atomic reference provider with no public version history or captured listings.
#[derive(Clone)]
pub struct MemoryObjects {
    state: Arc<Mutex<State>>,
    options: MemoryOptions,
    token_key: Arc<Mutex<Option<[u8; 32]>>>,
    #[cfg(feature = "local")]
    journal: Option<Arc<persistence::Journal>>,
    #[cfg(feature = "local")]
    defer_local: bool,
}

impl MemoryObjects {
    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, State>, Error> {
        let state = self.state.lock().map_err(|_| Error::from(Unavailable))?;
        #[cfg(feature = "local")]
        if let Some(journal) = &self.journal {
            journal.check()?;
        }
        Ok(state)
    }

    /// Creates a provider with an existing default bucket.
    pub fn with_default_bucket() -> (Self, wire::BucketRef) {
        let bucket = wire::BucketRef {
            name: "default".to_owned(),
        };
        let mut state = State::default();
        state.buckets.insert(
            bucket.name.clone().into(),
            Bucket {
                info: wire::Bucket {
                    bucket: Some(bucket.clone()),
                    created_at: Some(prost_types::Timestamp::default()),
                }
                .into(),
                objects: OrdMap::new(),
            },
        );
        (
            Self {
                state: Arc::new(Mutex::new(state)),
                options: MemoryOptions {
                    maximum_entries: usize::MAX,
                    ..MemoryOptions::default()
                },
                token_key: Arc::new(Mutex::new(None)),
                #[cfg(feature = "local")]
                journal: None,
                #[cfg(feature = "local")]
                defer_local: false,
            },
            bucket,
        )
    }

    /// Creates a provider with one validated bucket.
    pub fn with_bucket(
        name: impl Into<String>,
        options: MemoryOptions,
    ) -> Result<(Self, wire::BucketRef), Error> {
        let name = name.into();
        request::bucket_name(&name)?;
        let provider = Self::new(options)?;
        let bucket = wire::BucketRef { name: name.clone() };
        provider.mutate([0; 32], &None, |state| {
            let info = wire::Bucket {
                bucket: Some(bucket.clone()),
                created_at: Some(timestamp()?),
            };
            state.buckets.insert(
                name.into(),
                Bucket {
                    info: info.clone().into(),
                    objects: OrdMap::new(),
                },
            );
            Ok(info)
        })?;
        Ok((provider, bucket))
    }

    /// Creates a provider with explicit limits.
    pub fn new(options: MemoryOptions) -> Result<Self, Error> {
        if options.maximum_entries == 0 {
            return Err(InvalidArgument.into());
        }
        let token_key = fresh_cursor_key(getrandom::fill)?;
        Ok(Self {
            state: Arc::default(),
            options,
            token_key: Arc::new(Mutex::new(Some(token_key))),
            #[cfg(feature = "local")]
            journal: None,
            #[cfg(feature = "local")]
            defer_local: false,
        })
    }

    fn cursor_key_with(
        &self,
        fill: impl FnOnce(&mut [u8]) -> Result<(), getrandom::Error>,
    ) -> Result<[u8; 32], Error> {
        let mut key = self
            .token_key
            .lock()
            .map_err(|_| Error::from(Unavailable))?;
        if let Some(key) = *key {
            return Ok(key);
        }
        let candidate = fresh_cursor_key(fill)?;
        *key = Some(candidate);
        Ok(candidate)
    }

    fn cursor(&self, query: &wire::ListObjectsRequest, key: &str) -> Result<String, Error> {
        let payload = key.as_bytes();
        let token_key = self.cursor_key_with(getrandom::fill)?;
        let mut hash = blake3::Hasher::new_keyed(&token_key);
        let mut bound = query.clone();
        bound.continuation_token.clear();
        let encoded = bound.encode_to_vec();
        hash.update(&(encoded.len() as u64).to_le_bytes());
        hash.update(&encoded);
        hash.update(payload);
        Ok(format!(
            "{}.{}",
            hex::encode(payload),
            hash.finalize().to_hex()
        ))
    }

    fn decode_cursor(&self, query: &wire::ListObjectsRequest) -> Result<Option<String>, Error> {
        if query.continuation_token.is_empty() {
            return Ok(None);
        }
        if query.continuation_token.len() > 2200 {
            return Err(InvalidArgument.into());
        }
        let (payload, _) = query
            .continuation_token
            .split_once('.')
            .ok_or(Error::from(InvalidArgument))?;
        let key =
            String::from_utf8(hex::decode(payload).map_err(|_| Error::from(InvalidArgument))?)
                .map_err(|_| Error::from(InvalidArgument))?;
        if self.cursor(query, &key)? != query.continuation_token {
            return Err(InvalidArgument.into());
        }
        Ok(Some(key))
    }
}

fn successor(prefix: &str) -> Option<String> {
    let mut next = prefix.to_owned();
    while let Some(last) = next.pop() {
        if let Some(bumped) = (last..=char::MAX).nth(1) {
            next.push(bumped);
            return Some(next);
        }
    }
    None
}

#[cfg(not(target_arch = "wasm32"))]
fn timestamp() -> Result<prost_types::Timestamp, Error> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::from(Unavailable))?;
    Ok(prost_types::Timestamp {
        seconds: i64::try_from(duration.as_secs()).map_err(|_| Error::from(Unavailable))?,
        nanos: i32::try_from(duration.subsec_nanos()).map_err(|_| Error::from(Unavailable))?,
    })
}
#[cfg(target_arch = "wasm32")]
#[allow(
    clippy::cast_possible_truncation,
    reason = "finite milliseconds are checked within the exact integer range before conversion"
)]
fn timestamp() -> Result<prost_types::Timestamp, Error> {
    let now = js_sys::Date::now();
    if !now.is_finite() || !(-62_167_219_200_000.0..=253_402_300_799_999.0).contains(&now) {
        return Err(Unavailable.into());
    }
    let millis = now as i64;
    Ok(prost_types::Timestamp {
        seconds: millis.div_euclid(1000),
        nanos: i32::try_from(millis.rem_euclid(1000)).map_err(|_| Error::from(Unavailable))?
            * 1_000_000,
    })
}

#[cfg(not(target_arch = "wasm32"))]
mod native;
mod provider;
mod publication;
mod read;
mod state;
mod transaction;

#[cfg(not(target_arch = "wasm32"))]
pub(super) use native::clone_native_text;
#[cfg(not(target_arch = "wasm32"))]
use native::*;
use publication::*;
use read::*;
pub(super) use state::State;
use state::{Bucket, Receipt, SharedInfo, Stored, Upload, stored_bytes};

#[cfg(test)]
mod bootstrap_tests;

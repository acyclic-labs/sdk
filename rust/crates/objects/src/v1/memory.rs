//! Bounded logical Objects provider; native publication meters the same atomic engine.
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex};
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

use super::{Error, NativeBatchObjects, Object, ObjectsProvider, request, wire};
use crate::body::{BodyParts, BodyReadError, BodyReadMode, StoredBody};
use crate::obs;
use acyclic_stream::OwnedBytes;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
#[cfg(not(target_arch = "wasm32"))]
use super::{NativeObjectsCause, NativeObjectsError, ObjectsBatch, ObjectsNativeFuture, ObjectsNativeRequest, ObjectsPutReceipt};
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
#[derive(Clone, Default)]
pub(super) struct State {
    buckets: OrdMap<SharedInfo<String>, Bucket>,
    uploads: OrdMap<SharedInfo<String>, Upload>,
    receipts: OrdMap<SharedInfo<String>, Receipt>,
    sequence: u64,
    /// Body bytes of every stored object, maintained by [`stored_bytes`].
    object_bytes: usize,
}
#[derive(Clone)]
struct Bucket {
    info: SharedInfo<wire::Bucket>,
    objects: OrdMap<SharedInfo<String>, Stored>,
}
// Equality supports persistent subtree diffs.
#[derive(Clone, PartialEq)]
struct Stored {
    info: SharedInfo,
    body: StoredBody,
}
#[derive(Clone)]
struct Upload {
    bucket: SharedInfo<String>,
    key: SharedInfo<String>,
    metadata: SharedInfo<Option<wire::ObjectMetadata>>,
    parts: OrdMap<u32, (SharedInfo<wire::UploadedPart>, StoredBody)>,
}
#[derive(Clone, PartialEq)]
struct Receipt {
    digest: [u8; 32],
    response: OwnedBytes,
    put_info: Option<SharedInfo>,
    #[cfg(feature = "local")]
    kind: u32,
}

#[derive(Clone)]
struct SharedInfo<T = wire::ObjectInfo>(Arc<InfoOwner<T>>);
struct InfoOwner<T = wire::ObjectInfo> {
    info: T,
    #[cfg(not(target_arch = "wasm32"))]
    _allocation: Option<ReadAllocation>,
}
impl<T> std::ops::Deref for SharedInfo<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.0.info }
}
impl<T: PartialEq> PartialEq for SharedInfo<T> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.0.info == other.0.info
    }
}
impl<T> From<T> for SharedInfo<T> {
    fn from(info: T) -> Self {
        Self(Arc::new(InfoOwner {
            info,
            #[cfg(not(target_arch = "wasm32"))]
            _allocation: None,
        }))
    }
}

/// Updates `next`'s object byte total from `before`'s by walking only the object subtrees the two states do not share, so quota admission costs the change, not the store size.
fn stored_bytes(before: &State, next: &mut State) -> Option<usize> {
    let sum = |bucket: &Bucket| {
        bucket.objects.values().try_fold(0_usize, |total, object| {
            total.checked_add(object.body.len())
        })
    };
    let (mut added, mut removed) = (0_usize, 0_usize);
    for (name, bucket) in &next.buckets {
        let Some(old) = before.buckets.get(name) else {
            added = added.checked_add(sum(bucket)?)?;
            continue;
        };
        for item in old.objects.diff(&bucket.objects) {
            let (old, new) = match item {
                imbl::ordmap::DiffItem::Add(_, new) => (0, new.body.len()),
                imbl::ordmap::DiffItem::Update {
                    old: (_, old),
                    new: (_, new),
                } => (old.body.len(), new.body.len()),
                imbl::ordmap::DiffItem::Remove(_, old) => (old.body.len(), 0),
            };
            added = added.checked_add(new)?;
            removed = removed.checked_add(old)?;
        }
    }
    for (name, bucket) in &before.buckets {
        if !next.buckets.contains_key(name) {
            removed = removed.checked_add(sum(bucket)?)?;
        }
    }
    next.object_bytes = before
        .object_bytes
        .checked_add(added)?
        .checked_sub(removed)?;
    next.uploads
        .values()
        .flat_map(|upload| upload.parts.values())
        .try_fold(next.object_bytes, |total, (_, body)| {
            total.checked_add(body.len())
        })
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
    /// Creates the infrastructure-free SDK composition with an existing `default` bucket.
    #[must_use]
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
                }.into(),
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

    /// Synchronously admits one bucket for a native SDK composition.
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

    /// Creates a provider with explicit limits and independently authenticated cursors.
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

    // Failed mutations roll back publication and receipt together.
    fn mutate<R: Message + Default + 'static>(
        &self,
        digest: [u8; 32],
        identity: &Option<wire::MutationIdentity>,
        action: impl FnOnce(&mut State) -> Result<R, Error>,
    ) -> Result<R, Error> {
        request::identity(identity)?;
        let mut guard = self.lock_state()?;
        self.mutate_locked(&mut guard, digest, identity, action)
    }

    fn mutate_locked<R: Message + Default + 'static>(
        &self,
        guard: &mut State,
        digest: [u8; 32],
        identity: &Option<wire::MutationIdentity>,
        action: impl FnOnce(&mut State) -> Result<R, Error>,
    ) -> Result<R, Error> {
        self.mutate_locked_engine(
            guard, digest, identity,
            |receipt| R::decode(receipt.response.as_slice()).map_err(|_| Unavailable.into()),
            action,
            |response, identity| {
                let encoded: OwnedBytes = response.encode_to_vec().into();
                // Promote ordinary byte controls before native aliasing.
                drop(encoded.clone());
                Ok((
                    identity.idempotency_key.clone().into(),
                    Receipt {
                        digest,
                        response: encoded,
                        put_info: (response as &dyn std::any::Any)
                            .downcast_ref::<wire::ObjectInfo>().cloned().map(Into::into),
                        #[cfg(feature = "local")]
                        kind: persistence::response_kind::<R>()?,
                    },
                ))
            },
            |before, next| {
                #[cfg(feature = "local")]
                if let Some(journal) = &self.journal {
                    if self.defer_local { journal.validate_bodies(next)?; }
                    else { journal.commit(before, next, None).map_err(persistence::service_error)?; }
                }
                #[cfg(not(feature = "local"))]
                let _ = (before, next);
                Ok(())
            },
        )
    }

    // Retry, receipt and state publication are one transaction.
    fn mutate_locked_engine<R, E: From<Error>>(
        &self,
        guard: &mut State,
        digest: [u8; 32],
        identity: &Option<wire::MutationIdentity>,
        replay: impl FnOnce(&Receipt) -> Result<R, E>,
        action: impl FnOnce(&mut State) -> Result<R, E>,
        receipt: impl FnOnce(&R, &wire::MutationIdentity) -> Result<(SharedInfo<String>, Receipt), E>,
        commit: impl FnOnce(&State, &mut State) -> Result<(), E>,
    ) -> Result<R, E> {
        #[cfg(feature = "local")]
        if let Some(journal) = &self.journal { journal.check()?; }
        request::identity(identity)?;
        if let Some(identity) = identity
            && let Some(receipt) = guard.receipts.get(identity.idempotency_key.as_str())
        {
            if receipt.digest != digest { return Err(Error::from(IdempotencyMismatch).into()); }
            return replay(receipt);
        }
        let mut next = guard.clone();
        let response = action(&mut next)?;
        if let Some(identity) = identity {
            let (key, receipt) = receipt(&response, identity)?;
            next.receipts.insert(key, receipt);
        }
        let count = next.buckets.len()
            + next.uploads.len()
            + next.receipts.len()
            + next.buckets.values().map(|bucket| bucket.objects.len()).sum::<usize>()
            + next.uploads.values().map(|upload| upload.parts.len()).sum::<usize>();
        let bytes = stored_bytes(guard, &mut next).ok_or(Error::from(QuotaExceeded))?;
        if count > self.options.maximum_entries || bytes > self.options.maximum_bytes {
            return Err(Error::from(QuotaExceeded).into());
        }
        commit(guard, &mut next)?;
        *guard = next;
        Ok(response)
    }

    #[cfg(feature = "local")]
    fn put_durable_batch_locked(
        &self,
        state: &mut State,
        requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        let mut staged = self.clone();
        staged.defer_local = true;
        let mut input = requests.into_iter();
        let mut results = Vec::with_capacity(input.len());
        loop {
            let Some(count) = put_group_sizes(input.as_slice()).next() else { break; };
            let mut next = state.clone();
            let start = results.len();
            for (query, body) in input.by_ref().take(count) {
                results.push(staged.put_locked(&mut next, &query, body));
            }
            if next.sequence != state.sequence {
                match self.journal.as_ref().ok_or(Error::from(Unavailable))
                    .and_then(|journal| journal.commit(state, &mut next, None).map_err(persistence::service_error))
                {
                    Ok(()) => *state = next,
                    Err(error) => {
                        for result in &mut results[start..] {
                            if result.is_ok() { *result = Err(error); }
                        }
                    }
                }
            }
        }
        results
    }

    fn put_locked(
        &self,
        state: &mut State,
        query: &wire::PutObjectHeader,
        body: OwnedBytes,
    ) -> Result<wire::ObjectInfo, Error> {
        let digest = request::put_digest(query, &body)?;
        let name = request::bucket(&query.bucket)?;
        self.mutate_locked(state, digest, &query.mutation, |state| {
            publish(state, name, &query.object_key, query.metadata.clone(),
                &query.preconditions, StoredBody::memory(body))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn put_locked_native(
        &self,
        state: &mut State,
        mut query: wire::PutObjectHeader,
        body: OwnedBytes,
        meter: &Arc<dyn NativeReadMeter>,
    ) -> Result<ObjectsPutReceipt, NativeObjectsError> {
        request::put(&query)?;
        request::upload_length(body.len() as u64)?;
        let digest = put_digest_measured(&mut query, &body, meter)?;
        let name = query.bucket.as_ref().expect("validated put bucket").name.as_str();
        self.mutate_locked_engine(
                state, digest, &query.mutation,
                |receipt| {
                    let info = receipt.put_info.as_ref().ok_or(Error::from(Unavailable))?;
                    let mut allocation = ReadAllocation::empty(meter);
                    let info = clone_native_info(info, meter, &mut allocation)?;
                    Ok(ObjectsPutReceipt::new(info, Some(allocation)))
                },
                |state| publish_engine(state, name, &query.object_key, &query.preconditions,
                    StoredBody::memory(body),
                    |state, size| {
                        let mut stored_allocation = ReadAllocation::empty(meter);
                        let metadata = query.metadata.as_ref().map(|metadata| clone_native_metadata(metadata, meter, &mut stored_allocation)).transpose()?;
                        let info = wire::ObjectInfo {
                            etag: next_id_measured(state, meter, &mut stored_allocation)?,
                            size,
                            metadata,
                            last_modified: Some(timestamp()?),
                        };
                        let info = SharedInfo::new_measured(info, stored_allocation, meter)?;
                        let mut returned_allocation = ReadAllocation::empty(meter);
                        let returned = clone_native_info(&info, meter, &mut returned_allocation)?;
                        Ok((info, ObjectsPutReceipt::new(returned, Some(returned_allocation))))
                    },
                    |key| SharedInfo::<String>::text(key, meter)),
                |response, identity| {
                    let encoded = encode_measured(response.info(), meter)?;
                    let mut allocation = ReadAllocation::empty(meter);
                    let info = clone_native_info(response.info(), meter, &mut allocation)?;
                    let info = SharedInfo::new_measured(info, allocation, meter)?;
                    Ok((SharedInfo::<String>::text(&identity.idempotency_key, meter)?, Receipt {
                        digest,
                        response: encoded,
                        put_info: Some(info),
                        #[cfg(feature = "local")]
                        kind: persistence::response_kind::<wire::ObjectInfo>()?,
                    }))
                },
                |before, next| {
                    meter.check()?;
                    #[cfg(feature = "local")]
                    if let Some(journal) = &self.journal {
                        if self.defer_local { journal.validate_bodies(next)?; }
                        else { journal.commit(before, next, Some(Arc::clone(meter)))?; }
                    }
                    #[cfg(not(feature = "local"))]
                    let _ = (before, next);
                    if self.native_table_backend() { meter.admit_write(0)?; }
                    Ok(())
                },
        )
    }

    // Local cache staging adds no backend operation.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_table_backend(&self) -> bool {
        #[cfg(feature = "local")]
        { self.journal.is_none() }
        #[cfg(not(feature = "local"))]
        { true }
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

/// The least string above every string that starts with `prefix`, if any.
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
fn next_id(state: &mut State) -> Result<String, Error> {
    state.sequence = state
        .sequence
        .checked_add(1)
        .ok_or(Error::from(QuotaExceeded))?;
    // Opaque receipt identity; not a version selector or placement identifier.
    Ok(format!(
        "\"{}\"",
        blake3::hash(&state.sequence.to_le_bytes()).to_hex()
    ))
}
fn check_condition(
    value: &Option<wire::Preconditions>,
    current: Option<&Stored>,
) -> Result<(), Error> {
    request::preconditions(value)?;
    match value.as_ref().and_then(|value| value.condition.as_ref()) {
        Some(wire::preconditions::Condition::IfAbsent(true)) if current.is_some() => {
            Err(PreconditionFailed.into())
        }
        Some(wire::preconditions::Condition::IfMatch(etag))
            if current.is_none_or(|value| value.info.etag != *etag) =>
        {
            Err(PreconditionFailed.into())
        }
        _ => Ok(()),
    }
}
fn read_condition(
    info: &wire::ObjectInfo,
    if_match: &str,
    if_none_match: &str,
) -> Result<(), Error> {
    request::read_filters(if_match, if_none_match)?;
    if !if_match.is_empty() && if_match != "*" && if_match != info.etag {
        return Err(PreconditionFailed.into());
    }
    if !if_none_match.is_empty() && (if_none_match == "*" || if_none_match == info.etag) {
        return Err(NotModified.into());
    }
    Ok(())
}
fn publish(
    state: &mut State,
    bucket: &str,
    key: &str,
    metadata: Option<wire::ObjectMetadata>,
    condition: &Option<wire::Preconditions>,
    body: StoredBody,
) -> Result<wire::ObjectInfo, Error> {
    publish_engine(state, bucket, key, condition, body,
        |state, size| {
            let info = wire::ObjectInfo {
                etag: next_id(state)?, size, metadata, last_modified: Some(timestamp()?),
            };
            Ok((info.clone().into(), info))
        },
        |key| Ok(key.to_owned().into()))
}

fn publish_engine<R, E: From<Error>>(
    state: &mut State,
    bucket: &str,
    key: &str,
    condition: &Option<wire::Preconditions>,
    body: StoredBody,
    info: impl FnOnce(&mut State, u64) -> Result<(SharedInfo, R), E>,
    copy_key: impl FnOnce(&str) -> Result<SharedInfo<String>, E>,
) -> Result<R, E> {
    let current = state.buckets.get(bucket).ok_or(Error::from(NotFound))?.objects.get(key);
    check_condition(condition, current)?;
    let key = copy_key(key)?;
    let (info, response) = info(state, body.len() as u64)?;
    state.buckets.get_mut(bucket).ok_or(Error::from(NotFound))?.objects
        .insert(key, Stored { info, body });
    Ok(response)
}
fn upload<'a>(state: &'a State, bucket: &str, key: &str, id: &str) -> Result<&'a Upload, Error> {
    request::upload_id(id)?;
    let value = state.uploads.get(id).ok_or(Error::from(NotFound))?;
    if value.bucket.as_str() != bucket || value.key.as_str() != key {
        return Err(NotFound.into());
    }
    Ok(value)
}

struct ResolvedGet {
    header: wire::GetObjectHeader,
    body: StoredBody,
    start: usize,
    end: usize,
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<ReadAllocation>,
}

impl ResolvedGet {
    async fn read(self, mode: BodyReadMode) -> Result<Object, BodyReadError> {
        #[cfg(not(target_arch = "wasm32"))]
        let allocation = self.allocation;
        let Self { header, body, start, end, .. } = self;
        let body = body.read(start, end, mode).await?;
        Ok(Object {
            header, body,
            #[cfg(not(target_arch = "wasm32"))]
            _header_custody: allocation,
        })
    }
}

fn get_locked(
    state: &State,
    query: &wire::GetObjectRequest,
    maximum_bytes: u64,
) -> Result<ResolvedGet, Error> {
    let name = request::get(query)?;
    let (value, range, start, end) = get_selected_locked(state, name, query, maximum_bytes)?;
    Ok(ResolvedGet {
        header: wire::GetObjectHeader { object: Some((*value.info).clone()), content_range: range },
        body: value.body.clone(), start, end,
        #[cfg(not(target_arch = "wasm32"))]
        allocation: None,
    })
}

fn get_selected_locked<'a>(
    state: &'a State,
    name: &str,
    query: &wire::GetObjectRequest,
    maximum_bytes: u64,
) -> Result<(&'a Stored, Option<wire::ContentRange>, usize, usize), Error> {
    let value = state
        .buckets
        .get(name)
        .and_then(|bucket| bucket.objects.get(query.object_key.as_str()))
        .ok_or(Error::from(NotFound))?;
    read_condition(&value.info, &query.if_match, &query.if_none_match)?;
    let range = request::range(&query.range, value.info.size)?;
    let (start, end) = match &range {
        None => (0, value.body.len()),
        Some(range) => (
            usize::try_from(range.start).map_err(|_| Error::from(QuotaExceeded))?,
            usize::try_from(range.end + 1).map_err(|_| Error::from(QuotaExceeded))?,
        ),
    };
    if (end - start) as u64 > maximum_bytes {
        return Err(Error::from(QuotaExceeded).into());
    }
    Ok((value, range, start, end))
}

#[cfg(not(target_arch = "wasm32"))]
fn clone_native_text(
    value: &str,
    meter: &Arc<dyn NativeReadMeter>,
    retained: &mut ReadAllocation,
) -> Result<String, NativeObjectsError> {
    let mut allocation = ReadAllocation::new(meter, value.len())?;
    let mut text = String::new();
    text.try_reserve_exact(value.len())
        .map_err(|error| meter.allocation_failed(error))?;
    allocation.reconcile_capacity(text.capacity())?;
    meter.admit_codec(0, value.len() as u64, 0)?;
    text.push_str(value);
    meter.record_codec(0, value.len() as u64, 0)?;
    retained.absorb(&mut allocation)?;
    Ok(text)
}

#[cfg(not(target_arch = "wasm32"))]
fn clone_native_info(
    info: &wire::ObjectInfo,
    meter: &Arc<dyn NativeReadMeter>,
    retained: &mut ReadAllocation,
) -> Result<wire::ObjectInfo, NativeObjectsError> {
    Ok(wire::ObjectInfo {
        etag: clone_native_text(&info.etag, meter, retained)?,
        size: info.size,
        metadata: info.metadata.as_ref().map(|metadata| clone_native_metadata(metadata, meter, retained)).transpose()?,
        last_modified: info.last_modified,
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn clone_native_metadata(
    metadata: &wire::ObjectMetadata,
    meter: &Arc<dyn NativeReadMeter>,
    retained: &mut ReadAllocation,
) -> Result<wire::ObjectMetadata, NativeObjectsError> {
    let mut copied = wire::ObjectMetadata {
        content_type: clone_native_text(&metadata.content_type, meter, retained)?,
        content_encoding: clone_native_text(&metadata.content_encoding, meter, retained)?,
        cache_control: clone_native_text(&metadata.cache_control, meter, retained)?,
        content_disposition: clone_native_text(&metadata.content_disposition, meter, retained)?,
        content_language: clone_native_text(&metadata.content_language, meter, retained)?,
        expires_unix_seconds: metadata.expires_unix_seconds,
        user: BTreeMap::new(),
    };
    for (key, value) in &metadata.user {
        let key = clone_native_text(key, meter, retained)?;
        let value = clone_native_text(value, meter, retained)?;
        // Public retained entry payload; private BTree node layout is excluded.
        let bytes = std::mem::size_of::<(String, String)>();
        let mut entry = ReadAllocation::adopt(meter, bytes)?;
        meter.admit_codec(0, bytes as u64, 0)?;
        retained.absorb(&mut entry)?;
        copied.user.insert(key, value);
        meter.record_codec(0, bytes as u64, 0)?;
    }
    Ok(copied)
}

impl std::borrow::Borrow<str> for SharedInfo<String> {
    fn borrow(&self) -> &str { self.as_str() }
}
impl std::fmt::Display for SharedInfo<String> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.info.fmt(f) }
}
impl<T: Eq> Eq for SharedInfo<T> {}
impl<T: Ord> PartialOrd for SharedInfo<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl<T: Ord> Ord for SharedInfo<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.0.info.cmp(&other.0.info) }
}

#[cfg(not(target_arch = "wasm32"))]
impl SharedInfo<String> {
    fn text(value: &str, meter: &Arc<dyn NativeReadMeter>) -> Result<Self, NativeObjectsError> {
        let mut allocation = ReadAllocation::empty(meter);
        let text = clone_native_text(value, meter, &mut allocation)?;
        Self::new_measured(text, allocation, meter)
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl<T> SharedInfo<T> {
    fn new_measured(info: T, mut allocation: ReadAllocation, meter: &Arc<dyn NativeReadMeter>) -> Result<Self, NativeObjectsError> {
        let control = std::mem::size_of::<InfoOwner<T>>();
        meter.admit_codec(0, control as u64, 0)?;
        allocation.absorb(&mut ReadAllocation::new(meter, control)?)?;
        let result = Self(Arc::new(InfoOwner { info, _allocation: Some(allocation) }));
        meter.record_codec(0, control as u64, 0)?;
        Ok(result)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn next_id_measured(
    state: &mut State,
    meter: &Arc<dyn NativeReadMeter>,
    retained: &mut ReadAllocation,
) -> Result<String, NativeObjectsError> {
    state.sequence = state.sequence.checked_add(1).ok_or(Error::from(QuotaExceeded))?;
    meter.admit_codec(64, 0, 8)?;
    let hash = blake3::hash(&state.sequence.to_le_bytes()).to_hex();
    meter.record_codec(64, 0, 8)?;
    let mut bytes = [b'"'; 66];
    bytes[1..65].copy_from_slice(hash.as_bytes());
    clone_native_text(std::str::from_utf8(&bytes).map_err(|_| Error::from(Unavailable))?, meter, retained)
}

#[cfg(not(target_arch = "wasm32"))]
fn encode_measured(
    message: &impl Message,
    meter: &Arc<dyn NativeReadMeter>,
) -> Result<OwnedBytes, NativeObjectsError> {
    let length = message.encoded_len();
    let mut allocation = ReadAllocation::new(meter, length)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(|error| meter.allocation_failed(error))?;
    allocation.reconcile_capacity(bytes.capacity())?;
    meter.admit_codec(length as u64, 0, 0)?;
    message.encode(&mut bytes).map_err(|_| Error::from(Unavailable))?;
    meter.record_codec(bytes.len() as u64, 0, 0)?;
    OwnedBytes::from_vec_with_guard(bytes, allocation, Arc::clone(meter))
        .map_err(|failure| super::native_preparation_error(failure.into_error()))
}

#[cfg(not(target_arch = "wasm32"))]
fn put_digest_measured(
    query: &mut wire::PutObjectHeader,
    body: &[u8],
    meter: &Arc<dyn NativeReadMeter>,
) -> Result<[u8; 32], NativeObjectsError> {
    let mutation = query.mutation.take();
    let encoded = encode_measured(query, meter);
    query.mutation = mutation;
    let encoded = encoded?;
    let domain = b"acyclic.objects.v1.mutation\0";
    let hash_bytes = body.len().checked_add(domain.len() + 8 + 3 + 8 + 8 + 32)
        .and_then(|bytes| bytes.checked_add(encoded.len()))
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
    meter.admit_codec(0, 0, hash_bytes as u64)?;
    let body_hash = blake3::hash(body);
    let digest = request::finish_digest("put", &encoded, body.len() as u64, *body_hash.as_bytes());
    meter.record_codec(0, 0, hash_bytes as u64)?;
    drop(encoded);
    Ok(digest)
}


#[cfg(not(target_arch = "wasm32"))]
fn native_vec<T>(count: usize, meter: &Arc<dyn NativeReadMeter>) -> Result<(Vec<T>, ReadAllocation), NativeObjectsError> {
    let bytes = count.checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
    let mut allocation = ReadAllocation::new(meter, bytes)?;
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|error| meter.allocation_failed(error))?;
    allocation.reconcile_capacity(values.capacity() * std::mem::size_of::<T>())?;
    Ok((values, allocation))
}

#[cfg(not(target_arch = "wasm32"))]
fn get_native_locked(
    state: &State,
    query: &wire::GetObjectRequest,
    maximum_bytes: u64,
    meter: &Arc<dyn NativeReadMeter>,
    table_backend: bool,
) -> Result<ResolvedGet, NativeObjectsError> {
    meter.check()?;
    let name = request::get(query)?;
    if table_backend { meter.admit_read(0)?; }
    let (value, range, start, end) = get_selected_locked(state, name, query, maximum_bytes)?;
    let mut retained = ReadAllocation::empty(meter);
    let info = clone_native_info(&value.info, meter, &mut retained)?;
    Ok(ResolvedGet {
        header: wire::GetObjectHeader { object: Some(info), content_range: range },
        body: value.body.clone(), start, end, allocation: Some(retained),
    })
}

#[async_trait::async_trait]
impl NativeBatchObjects for MemoryObjects {
    async fn put_batch(
        &self,
        requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.put_batch",
                batch_len = requests.len()
            ),
            async {
                if requests.is_empty() {
                    return Vec::new();
                }
                let Ok(mut state) = self.lock_state() else {
                    return vec![Err(Unavailable.into()); requests.len()];
                };
                #[cfg(feature = "local")]
                if self.journal.is_some() {
                    return self.put_durable_batch_locked(&mut state, requests);
                }
                requests
                    .into_iter()
                    .map(|(query, body)| self.put_locked(&mut state, &query, body))
                    .collect()
            },
        )
        .await
    }

    async fn get_batch(
        &self,
        requests: Vec<(wire::GetObjectRequest, u64)>,
    ) -> Vec<Result<Object, Error>> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.get_batch",
                batch_len = requests.len()
            ),
            async {
                if requests.is_empty() {
                    return Vec::new();
                }
                let selected = {
                    let Ok(state) = self.lock_state() else {
                        return (0..requests.len()).map(|_| Err(Unavailable.into())).collect();
                    };
                    requests
                        .into_iter()
                        .map(|(query, maximum)| get_locked(&state, &query, maximum))
                        .collect::<Vec<_>>()
                };
                let mut results = Vec::with_capacity(selected.len());
                for value in selected {
                    results.push(match value {
                        Ok(value) => value.read(BodyReadMode::Ordinary).await.map_err(|_| Error::from(Unavailable)),
                        Err(error) => Err(error),
                    });
                }
                results
            },
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn get_batch_native(
        &self,
        input: ObjectsNativeRequest<Vec<(wire::GetObjectRequest, u64)>>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<Object>>, NativeObjectsError> {
        super::native_input_meter(&meter, &input)?;
        super::native_future(Arc::clone(&meter), async move {
            let input = input;
            obs::traced(obs::span!(INFO, "acyclic.objects.get_batch", batch_len = input.data().len()), async {
                meter.check()?;
                let count = input.data().len();
                if count == 0 {
                    return ObjectsBatch::new(Vec::new(), ReadAllocation::empty(&meter));
                }
                let mut selected = native_vec(count, &meter)?;
                let mut results = native_vec(count, &meter)?;
                {
                    let state = self.lock_state()?;
                    for (query, maximum) in input.data() {
                        selected.0.push(get_native_locked(&state, query, *maximum, &meter,
                            self.native_table_backend()));
                    }
                }
                for value in selected.0.drain(..) {
                    results.0.push(match value {
                        Ok(value) => value.read(BodyReadMode::Native(Arc::clone(&meter))).await,
                        Err(error) => Err(error),
                    });
                }
                drop(selected);
                let (values, allocation) = results;
                ObjectsBatch::new(values, allocation)
            }).await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn put_batch_native(
        &self,
        input: ObjectsNativeRequest<Vec<(wire::PutObjectHeader, OwnedBytes)>>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<ObjectsPutReceipt>>, NativeObjectsError> {
        super::native_input_meter(&meter, &input)?;
        super::native_future(Arc::clone(&meter), async move {
            let mut input = input;
            obs::traced(obs::span!(INFO, "acyclic.objects.put_batch", batch_len = input.data().len()), async {
                meter.check()?;
                let mut results = native_vec(input.data().len(), &meter)?;
                if input.data().is_empty() {
                    let (values, allocation) = results;
                    return ObjectsBatch::new(values, allocation);
                }
                {
                    let mut state = self.lock_state()?;
                    #[cfg(feature = "local")]
                    if let Some(journal) = &self.journal {
                        // Prepare group failure owners before mutation or I/O.
                        let mut plans = native_vec(put_group_sizes(input.data()).count(), &meter)?;
                        for count in put_group_sizes(input.data()) {
                            plans.0.push((count, super::NativeObjectsSharedFailure::prepare(&meter)?));
                        }
                        let mut staged = self.clone();
                        staged.defer_local = true;
                        let mut requests = input.data.drain(..);
                        for (count, failure) in plans.0.drain(..) {
                            let start = results.0.len();
                            let mut next = match meter.check().map(|()| state.clone()).map_err(NativeObjectsError::from) {
                                Ok(next) => next,
                                Err(error) => {
                                    failure.error.set(error)?;
                                    for _ in requests.by_ref().take(count) {
                                        results.0.push(Err(NativeObjectsError::from(NativeObjectsCause::SharedFailure(
                                            Arc::clone(&failure)))));
                                    }
                                    continue;
                                }
                            };
                            for (query, body) in requests.by_ref().take(count) {
                                results.0.push(staged.put_locked_native(&mut next, query, body, &meter));
                            }
                            if next.sequence != state.sequence {
                                match journal.commit(&state, &mut next, Some(Arc::clone(&meter))) {
                                    Ok(()) => *state = next,
                                    Err(error) => {
                                        failure.error.set(error)?;
                                        for result in &mut results.0[start..] {
                                            if result.is_ok() {
                                                *result = Err(NativeObjectsError::from(NativeObjectsCause::SharedFailure(
                                                    Arc::clone(&failure))));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        let (values, allocation) = results;
                        return ObjectsBatch::new(values, allocation);
                    }
                    for (query, body) in input.data.drain(..) {
                        results.0.push(self.put_locked_native(&mut state, query, body, &meter));
                    }
                }
                let (values, allocation) = results;
                ObjectsBatch::new(values, allocation)
            }).await
        })
    }
}

#[cfg(feature = "local")]
fn put_group_sizes(
    requests: &[(wire::PutObjectHeader, OwnedBytes)],
) -> impl Iterator<Item = usize> + '_ {
    let mut requests = requests.iter().peekable();
    std::iter::from_fn(move || {
        requests.peek()?;
        let mut bytes = 0_usize;
        let mut count = 0_usize;
        while requests.next_if(|(query, body)| {
            let total = bytes.saturating_add(persistence::put_record_bytes(query, body));
            let fits = bytes == 0 || total <= persistence::RECORD_LIMIT;
            if fits { bytes = total; }
            fits
        }).is_some() {
            count += 1;
        }
        Some(count)
    })
}

#[async_trait::async_trait]
impl ObjectsProvider for MemoryObjects {
    async fn create_bucket(&self, query: wire::CreateBucketRequest) -> Result<wire::Bucket, Error> {
        obs::traced(obs::span!(INFO, "acyclic.objects.create_bucket"), async {
            request::bucket_name(&query.name)?;
            self.mutate(
                request::create_bucket_digest(&query)?,
                &query.mutation,
                |state| {
                    if state.buckets.contains_key(query.name.as_str()) {
                        return Err(AlreadyExists.into());
                    }
                    let info = wire::Bucket {
                        bucket: Some(wire::BucketRef {
                            name: query.name.clone(),
                        }),
                        created_at: Some(timestamp()?),
                    };
                    state.buckets.insert(
                        query.name.clone().into(),
                        Bucket {
                            info: info.clone().into(),
                            objects: OrdMap::new(),
                        },
                    );
                    Ok(info)
                },
            )
        })
        .await
    }
    async fn head_bucket(&self, query: wire::HeadBucketRequest) -> Result<wire::Bucket, Error> {
        obs::traced(obs::span!(INFO, "acyclic.objects.head_bucket"), async {
            let name = request::bucket(&query.bucket)?;
            Ok((*self.lock_state()?.buckets.get(name).ok_or(Error::from(NotFound))?.info).clone())
        })
        .await
    }
    async fn delete_bucket(
        &self,
        query: wire::DeleteBucketRequest,
    ) -> Result<wire::DeleteBucketResponse, Error> {
        obs::traced(obs::span!(INFO, "acyclic.objects.delete_bucket"), async {
            let name = request::bucket(&query.bucket)?;
            self.mutate(
                request::delete_bucket_digest(&query)?,
                &query.mutation,
                |state| {
                    if state
                        .buckets
                        .get(name)
                        .is_some_and(|bucket| !bucket.objects.is_empty())
                        || state.uploads.values().any(|upload| upload.bucket.as_str() == name)
                    {
                        return Err(PreconditionFailed.into());
                    }
                    Ok(wire::DeleteBucketResponse {
                        existed: state.buckets.remove(name).is_some(),
                    })
                },
            )
        })
        .await
    }
    async fn put(
        &self,
        query: wire::PutObjectHeader,
        body: OwnedBytes,
    ) -> Result<wire::ObjectInfo, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.put", bytes = body.len()),
            async {
                let mut state = self.lock_state()?;
                self.put_locked(&mut state, &query, body)
            },
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn put_native(
        &self,
        input: ObjectsNativeRequest<(wire::PutObjectHeader, OwnedBytes)>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsPutReceipt>, NativeObjectsError> {
        super::native_input_meter(&meter, &input)?;
        super::native_future(Arc::clone(&meter), async move {
            let mut input = input;
            obs::traced(obs::span!(INFO, "acyclic.objects.put", bytes = input.data().1.len()), async {
                meter.check()?;
                let mut state = self.lock_state()?;
                let (query, body) = std::mem::take(&mut input.data);
                self.put_locked_native(&mut state, query, body, &meter)
            }).await
        })
    }
    async fn get(
        &self,
        query: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.get", bytes = obs::Empty),
            async {
                let selected = {
                    let state = self.lock_state()?;
                    get_locked(&state, &query, maximum_bytes)?
                };
                let object = selected.read(BodyReadMode::Ordinary).await.map_err(|_| Error::from(Unavailable))?;
                obs::record!("bytes" = object.body().len());
                Ok(object)
            },
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn get_native(
        &self,
        input: ObjectsNativeRequest<wire::GetObjectRequest>,
        maximum_bytes: u64,
        meter: Arc<dyn NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, Object>, NativeObjectsError> {
        super::native_input_meter(&meter, &input)?;
        super::native_future(Arc::clone(&meter), async move {
            let input = input;
            obs::traced(obs::span!(INFO, "acyclic.objects.get", bytes = obs::Empty), async {
                let selected = {
                    let state = self.lock_state()?;
                    get_native_locked(&state, input.data(), maximum_bytes, &meter,
                        self.native_table_backend())?
                };
                let result = selected.read(BodyReadMode::Native(Arc::clone(&meter))).await?;
                obs::record!("bytes" = result.body().len());
                Ok(result)
            }).await
        })
    }
    async fn head(
        &self,
        query: wire::HeadObjectRequest,
    ) -> Result<wire::HeadObjectResponse, Error> {
        obs::traced(obs::span!(INFO, "acyclic.objects.head"), async {
            request::validate_binary("objects/head", &query.encode_to_vec(), 0)?;
            let name = request::bucket(&query.bucket)?;
            request::key(&query.object_key)?;
            let state = self.lock_state()?;
            let value = state
                .buckets
                .get(name)
                .and_then(|bucket| bucket.objects.get(query.object_key.as_str()))
                .ok_or(Error::from(NotFound))?;
            read_condition(&value.info, &query.if_match, &query.if_none_match)?;
            Ok(wire::HeadObjectResponse {
                object: Some((*value.info).clone()),
            })
        })
        .await
    }
    async fn delete(
        &self,
        query: wire::DeleteObjectRequest,
    ) -> Result<wire::DeleteObjectResponse, Error> {
        obs::traced(obs::span!(INFO, "acyclic.objects.delete"), async {
            let name = request::bucket(&query.bucket)?;
            request::key(&query.object_key)?;
            request::preconditions(&query.preconditions)?;
            self.mutate(request::delete_digest(&query)?, &query.mutation, |state| {
                let bucket = state.buckets.get_mut(name).ok_or(Error::from(NotFound))?;
                check_condition(&query.preconditions, bucket.objects.get(query.object_key.as_str()))?;
                Ok(wire::DeleteObjectResponse {
                    existed: bucket.objects.remove(query.object_key.as_str()).is_some(),
                })
            })
        })
        .await
    }
    async fn list(
        &self,
        query: wire::ListObjectsRequest,
    ) -> Result<wire::ListObjectsResponse, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.list", items = obs::Empty),
            async {
                request::validate_binary("objects/list", &query.encode_to_vec(), 0)?;
                let name = request::bucket(&query.bucket)?;
                let limit = request::page_size(query.page_size)?;
                if query.prefix.len() > 1024
                    || query.delimiter.len() > 1024
                    || query.prefix.contains('\0')
                    || query.delimiter.contains('\0')
                {
                    return Err(InvalidArgument.into());
                }
                let cursor = self.decode_cursor(&query)?;
                let state = self.lock_state()?;
                let bucket = state.buckets.get(name).ok_or(Error::from(NotFound))?;
                // Scan strictly after the cursor until one page is full.
                let mut lower = match &cursor {
                    Some(cursor) if *cursor >= query.prefix => Bound::Excluded(cursor.clone()),
                    _ => Bound::Included(query.prefix.clone()),
                };
                let mut page = Vec::new();
                'scan: loop {
                    let mut next = None;
                    for (key, value) in bucket
                        .objects
                        .range::<_, str>((lower.as_ref().map(String::as_str), Bound::Unbounded))
                    {
                        let Some(suffix) = key.strip_prefix(&query.prefix) else {
                            break 'scan;
                        };
                        if !query.delimiter.is_empty()
                            && let Some(index) = suffix.find(&query.delimiter)
                        {
                            let prefix = key
                                .get(..query.prefix.len() + index + query.delimiter.len())
                                .ok_or(Error::from(InvalidArgument))?;
                            next = successor(prefix);
                            if cursor.as_deref().is_none_or(|cursor| prefix > cursor) {
                                page.push((prefix.to_owned(), None));
                            }
                            break;
                        }
                        page.push((key.to_string(), Some((*value.info).clone())));
                        if page.len() > limit {
                            break 'scan;
                        }
                    }
                    match next {
                        Some(next) if page.len() <= limit => lower = Bound::Included(next),
                        _ => break,
                    }
                }
                let is_truncated = page.len() > limit;
                page.truncate(limit);
                let continuation_token = if is_truncated {
                    page.last()
                        .map(|(key, _)| self.cursor(&query, key))
                        .transpose()?
                        .unwrap_or_default()
                } else {
                    String::new()
                };
                let mut response = wire::ListObjectsResponse {
                    is_truncated,
                    continuation_token,
                    ..Default::default()
                };
                for (key, info) in page {
                    if let Some(object) = info {
                        response.entries.push(wire::ListEntry {
                            object_key: key,
                            object: Some(object),
                        });
                    } else {
                        response.common_prefixes.push(key);
                    }
                }
                obs::record!("items" = response.entries.len() + response.common_prefixes.len());
                Ok(response)
            },
        )
        .await
    }
    async fn create_multipart(
        &self,
        query: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartUpload, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.create_multipart"),
            async {
                let name = request::bucket(&query.bucket)?;
                request::key(&query.object_key)?;
                request::metadata(&query.metadata)?;
                self.mutate(
                    request::create_multipart_digest(&query)?,
                    &query.mutation,
                    |state| {
                        if !state.buckets.contains_key(name) {
                            return Err(NotFound.into());
                        }
                        let id = next_id(state)?;
                        state.uploads.insert(
                            id.clone().into(),
                            Upload {
                                bucket: name.to_owned().into(),
                                key: query.object_key.clone().into(),
                                metadata: query.metadata.clone().into(),
                                parts: OrdMap::new(),
                            },
                        );
                        Ok(wire::MultipartUpload { upload_id: id })
                    },
                )
            },
        )
        .await
    }
    async fn upload_part(
        &self,
        query: wire::UploadPartHeader,
        body: OwnedBytes,
    ) -> Result<wire::UploadedPart, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.upload_part", bytes = body.len()),
            async {
                let name = request::bucket(&query.bucket)?;
                request::key(&query.object_key)?;
                if !(1..=10_000).contains(&query.part_number)
                    || body.len() as u64 > 5 * 1024 * 1024 * 1024
                {
                    return Err(InvalidArgument.into());
                }
                self.mutate(
                    request::upload_part_digest(&query, &body)?,
                    &query.mutation,
                    |state| {
                        upload(state, name, &query.object_key, &query.upload_id)?;
                        let receipt = wire::UploadedPart {
                            part_number: query.part_number,
                            etag: next_id(state)?,
                            size: body.len() as u64,
                        };
                        state
                            .uploads
                            .get_mut(query.upload_id.as_str())
                            .ok_or(Error::from(NotFound))?
                            .parts
                            .insert(
                                query.part_number,
                                (receipt.clone().into(), StoredBody::memory(body.clone())),
                            );
                        Ok(receipt)
                    },
                )
            },
        )
        .await
    }
    async fn list_parts(
        &self,
        query: wire::ListPartsRequest,
    ) -> Result<wire::ListPartsResponse, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.list_parts", items = obs::Empty),
            async {
                request::validate_binary("multipart/list-parts", &query.encode_to_vec(), 0)?;
                let name = request::bucket(&query.bucket)?;
                request::key(&query.object_key)?;
                let limit = request::page_size(query.page_size)?;
                if query.after_part_number > 10_000 {
                    return Err(InvalidArgument.into());
                }
                let state = self.lock_state()?;
                let value = upload(&state, name, &query.object_key, &query.upload_id)?;
                let mut parts = value
                    .parts
                    .range(query.after_part_number + 1..)
                    .map(|(_, part)| part);
                let page: Vec<_> = parts
                    .by_ref()
                    .take(limit)
                    .map(|(part, _)| (**part).clone())
                    .collect();
                let truncated = parts.next().is_some();
                obs::record!("items" = page.len());
                Ok(wire::ListPartsResponse {
                    next_part_number: if truncated {
                        page.last().map_or(0, |part| part.part_number)
                    } else {
                        0
                    },
                    parts: page,
                    is_truncated: truncated,
                })
            },
        )
        .await
    }
    async fn complete_multipart(
        &self,
        query: wire::CompleteMultipartRequest,
    ) -> Result<wire::ObjectInfo, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.complete_multipart"),
            async {
                let name = request::bucket(&query.bucket)?;
                request::key(&query.object_key)?;
                request::preconditions(&query.preconditions)?;
                if query.parts.is_empty() || query.parts.len() > 10_000 {
                    return Err(InvalidArgument.into());
                }
                self.mutate(
                    request::complete_multipart_digest(&query)?,
                    &query.mutation,
                    |state| {
                        let value = upload(state, name, &query.object_key, &query.upload_id)?;
                        let mut size = 0_usize;
                        let mut previous = 0;
                        for (index, selected) in query.parts.iter().enumerate() {
                            if selected.part_number <= previous {
                                return Err(InvalidArgument.into());
                            }
                            let (receipt, body) = value
                                .parts
                                .get(&selected.part_number)
                                .ok_or(Error::from(PreconditionFailed))?;
                            if selected != &**receipt
                                || (index + 1 < query.parts.len() && body.len() < 5 * 1024 * 1024)
                            {
                                return Err(PreconditionFailed.into());
                            }
                            size = size
                                .checked_add(body.len())
                                .ok_or(Error::from(QuotaExceeded))?;
                            previous = selected.part_number;
                        }
                        if size > self.options.maximum_bytes {
                            return Err(QuotaExceeded.into());
                        }
                        let metadata = (*value.metadata).clone();
                        let mut bodies = Vec::with_capacity(query.parts.len());
                        for selected in &query.parts {
                            bodies.push(
                                value
                                    .parts
                                    .get(&selected.part_number)
                                    .ok_or(Error::from(PreconditionFailed))?
                                    .1
                                    .clone(),
                            );
                        }
                        let info = publish(
                            state,
                            name,
                            &query.object_key,
                            metadata,
                            &query.preconditions,
                            StoredBody::Composite {
                                parts: Arc::new(BodyParts {
                                    values: bodies,
                                    #[cfg(not(target_arch = "wasm32"))]
                                    _allocation: None,
                                }),
                                length: size,
                            },
                        )?;
                        state.uploads.remove(query.upload_id.as_str());
                        Ok(info)
                    },
                )
            },
        )
        .await
    }
    async fn abort_multipart(
        &self,
        query: wire::AbortMultipartRequest,
    ) -> Result<wire::AbortMultipartResponse, Error> {
        obs::traced(obs::span!(INFO, "acyclic.objects.abort_multipart"), async {
            let name = request::bucket(&query.bucket)?;
            request::key(&query.object_key)?;
            self.mutate(
                request::abort_multipart_digest(&query)?,
                &query.mutation,
                |state| {
                    if state.uploads.contains_key(query.upload_id.as_str()) {
                        upload(state, name, &query.object_key, &query.upload_id)?;
                    }
                    Ok(wire::AbortMultipartResponse {
                        existed: state.uploads.remove(query.upload_id.as_str()).is_some(),
                    })
                },
            )
        })
        .await
    }
}

#[cfg(test)]
mod bootstrap_tests {
    use super::*;

    #[test]
    fn cursor_entropy_failure_does_not_install_a_key_and_clones_share_initialization()
    -> Result<(), Error> {
        let (provider, _) = MemoryObjects::with_default_bucket();
        let failed = provider.cursor_key_with(|key| {
            key.fill(1);
            Err(getrandom::Error::UNSUPPORTED)
        });
        assert_eq!(failed.map_err(|error| error.code), Err(Unavailable));
        let clone = provider.clone();
        let key = clone.cursor_key_with(|key| {
            key.fill(2);
            Ok(())
        })?;
        drop(clone);
        assert_eq!(key, [2; 32]);
        assert_eq!(
            provider.cursor_key_with(|_| Err(getrandom::Error::UNSUPPORTED))?,
            key
        );
        Ok(())
    }

    #[tokio::test]
    async fn bootstrap_pagination_is_bound_to_provider_and_query() -> Result<(), Error> {
        let (provider, bucket) = MemoryObjects::with_default_bucket();
        for key in ["a", "b"] {
            provider
                .put(
                    wire::PutObjectHeader {
                        bucket: Some(bucket.clone()),
                        object_key: key.to_owned(),
                        ..Default::default()
                    },
                    OwnedBytes::from_static(b"body"),
                )
                .await?;
        }
        let mut query = wire::ListObjectsRequest {
            bucket: Some(bucket),
            page_size: 1,
            ..Default::default()
        };
        let first = provider.list(query.clone()).await?;
        assert!(first.is_truncated);
        assert_eq!(
            first.entries.first().map(|entry| entry.object_key.as_str()),
            Some("a")
        );
        query.continuation_token = first.continuation_token;
        let second = provider.clone().list(query.clone()).await?;
        assert_eq!(
            second
                .entries
                .first()
                .map(|entry| entry.object_key.as_str()),
            Some("b")
        );
        assert!(!second.is_truncated);
        let (other, _) = MemoryObjects::with_default_bucket();
        assert_eq!(
            other.list(query.clone()).await.map_err(|error| error.code),
            Err(InvalidArgument)
        );
        query.prefix = "a".to_owned();
        assert_eq!(
            provider.list(query).await.map_err(|error| error.code),
            Err(InvalidArgument)
        );
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn put_input_bytes(header: &wire::PutObjectHeader) -> usize {
        let mut bytes = header.object_key.capacity()
            + header.bucket.as_ref().map_or(0, |bucket| bucket.name.capacity())
            + header.mutation.as_ref().map_or(0, |identity| identity.idempotency_key.capacity());
        if let Some(metadata) = &header.metadata {
            bytes += metadata.content_type.capacity() + metadata.content_encoding.capacity()
                + metadata.cache_control.capacity() + metadata.content_disposition.capacity()
                + metadata.content_language.capacity();
            bytes += metadata.user.len() * std::mem::size_of::<(String, String)>();
            bytes += metadata.user.iter().map(|(key, value)| key.capacity() + value.capacity()).sum::<usize>();
        }
        if let Some(wire::Preconditions {
            condition: Some(wire::preconditions::Condition::IfMatch(etag)),
        }) = &header.preconditions { bytes += etag.capacity(); }
        bytes
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn get_input_bytes(query: &wire::GetObjectRequest) -> usize {
        query.object_key.capacity() + query.if_match.capacity() + query.if_none_match.capacity()
            + query.bucket.as_ref().map_or(0, |bucket| bucket.name.capacity())
    }

    #[cfg(not(target_arch = "wasm32"))]
    use super::super::tests::NativeTransportMeter;

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_publication_reuses_retry_engine_and_retires_shared_state_claims()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::sync::atomic::Ordering::SeqCst;
        let observed = Arc::new(NativeTransportMeter::new_table());
        let meter: Arc<dyn NativeReadMeter> = observed.clone();
        let (provider, bucket) = MemoryObjects::with_default_bucket();
        let header = wire::PutObjectHeader {
            bucket: Some(bucket.clone()),
            object_key: "native-publication".into(),
            metadata: Some(wire::ObjectMetadata {
                content_type: "application/native".into(),
                user: BTreeMap::from([("custody".into(), "original".into())]),
                ..Default::default()
            }),
            mutation: Some(wire::MutationIdentity { idempotency_key: "native-retry".into() }),
            ..Default::default()
        };
        let first_input = header.clone();
        let custody = ReadAllocation::adopt(&meter, put_input_bytes(&first_input))?;
        let input = ObjectsNativeRequest::new((first_input, OwnedBytes::from_static(b"actual")), custody, &meter)?;
        let first = provider.put_native(input, Arc::clone(&meter))?.await?;
        let retry_input = header.clone();
        let custody = ReadAllocation::adopt(&meter, put_input_bytes(&retry_input))?;
        let input = ObjectsNativeRequest::new((retry_input, OwnedBytes::from_static(b"actual")), custody, &meter)?;
        let retried = provider.put_native(input, Arc::clone(&meter))?.await?;
        assert_eq!(first.info(), retried.info());
        let mut changed = header;
        changed.metadata = None;
        let custody = ReadAllocation::adopt(&meter, put_input_bytes(&changed))?;
        let input = ObjectsNativeRequest::new((changed, OwnedBytes::from_static(b"actual")), custody, &meter)?;
        let mismatch = provider.put_native(input, Arc::clone(&meter))?.await
            .err().ok_or("retry accepted changed metadata")?;
        assert!(matches!(mismatch, NativeObjectsError { cause: NativeObjectsCause::Service(Error { code: IdempotencyMismatch }), .. }));
        let query = wire::GetObjectRequest {
            bucket: Some(bucket.clone()), object_key: "native-publication".into(), ..Default::default()
        };
        let custody = ReadAllocation::adopt(&meter, get_input_bytes(&query))?;
        let input = ObjectsNativeRequest::new(query, custody, &meter)?;
        let native_read = provider.get_native(input, 6, Arc::clone(&meter))?.await?;
        assert_eq!(native_read.body().as_slice(), b"actual");
        assert_eq!(native_read.header().object.as_ref(), Some(first.info()));
        drop(native_read);
        let read = provider.get(wire::GetObjectRequest {
            bucket: Some(bucket), object_key: "native-publication".into(), ..Default::default()
        }, 6).await?;
        assert_eq!(read.body().as_slice(), b"actual");
        assert_eq!(read.header().object.as_ref(), Some(first.info()));
        drop(read);
        drop(first);
        drop(retried);
        assert!(observed.memory.load(SeqCst) > 0, "state retains its physical metadata and retry bytes");
        drop(provider);
        assert_eq!(observed.memory.load(SeqCst), 0);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_publication_uses_current_authority_not_body_payer()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::sync::atomic::Ordering::SeqCst;
        let original = Arc::new(NativeTransportMeter::new_table());
        let original_meter: Arc<dyn NativeReadMeter> = original.clone();
        let allocation = ReadAllocation::new(&original_meter, 4)?;
        let body = OwnedBytes::from_vec_with_guard(vec![1, 2, 3, 4], allocation, original_meter)
            .map_err(|failure| super::super::native_preparation_error(failure.into_error()))?;
        let alias = body.clone();
        let current = Arc::new(NativeTransportMeter::new_table());
        let meter: Arc<dyn NativeReadMeter> = current.clone();
        let (provider, bucket) = MemoryObjects::with_default_bucket();
        let header = wire::PutObjectHeader {
            bucket: Some(bucket), object_key: "current".into(), ..Default::default()
        };
        let custody = ReadAllocation::adopt(&meter, put_input_bytes(&header))?;
        let input = ObjectsNativeRequest::new((header, body), custody, &meter)?;
        current.cancelled.store(true, SeqCst);
        let failure = provider.put_native(input, Arc::clone(&meter))
            .err().ok_or("body payer bypassed current cancellation")?;
        assert!(matches!(&failure.cause, NativeObjectsCause::Admission(error)
            if error.kind() == std::io::ErrorKind::Interrupted));
        assert_eq!(current.memory.load(SeqCst), 0);
        assert!(original.memory.load(SeqCst) >= 4);
        drop(alias);
        assert_eq!(original.memory.load(SeqCst), 0);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_batch_iterator_retains_its_actual_result_vector_until_drop()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::sync::atomic::Ordering::SeqCst;
        let observed = Arc::new(NativeTransportMeter::new_table());
        let meter: Arc<dyn NativeReadMeter> = observed.clone();
        let (provider, bucket) = MemoryObjects::with_default_bucket();
        for key in ["a", "b"] {
            provider.put(wire::PutObjectHeader {
                bucket: Some(bucket.clone()), object_key: key.into(), ..Default::default()
            }, OwnedBytes::from_static(b"body")).await?;
        }
        let queries = ["a", "b"].into_iter().map(|key| (wire::GetObjectRequest {
            bucket: Some(bucket.clone()), object_key: key.into(), ..Default::default()
        }, 4)).collect::<Vec<_>>();
        let input_bytes = queries.capacity() * std::mem::size_of::<(wire::GetObjectRequest, u64)>()
            + queries.iter().map(|(query, _)| get_input_bytes(query)).sum::<usize>();
        let custody = ReadAllocation::adopt(&meter, input_bytes)?;
        let input = ObjectsNativeRequest::new(queries, custody, &meter)?;
        let batch = provider.get_batch_native(input, Arc::clone(&meter))?.await?;
        let mut results = batch;
        drop(results.next().ok_or("missing first native result")??);
        drop(results.next().ok_or("missing second native result")??);
        assert!(results.next().is_none());
        assert!(observed.memory.load(SeqCst) > 0, "exhaustion does not free vector storage");
        drop(results);
        assert_eq!(observed.memory.load(SeqCst), 0);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_write_batch_keeps_individual_rollback_and_ordered_receipts()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::sync::atomic::Ordering::SeqCst;
        let observed = Arc::new(NativeTransportMeter::new_table());
        let meter: Arc<dyn NativeReadMeter> = observed.clone();
        let (mut provider, bucket) = MemoryObjects::with_default_bucket();
        provider.options.maximum_bytes = 8;
        let requests = [("a", b"four".as_slice()), ("b", b"too-large".as_slice()), ("c", b"four".as_slice())]
            .into_iter().map(|(key, body)| (wire::PutObjectHeader {
                bucket: Some(bucket.clone()), object_key: key.into(), ..Default::default()
            }, OwnedBytes::from_static(body))).collect::<Vec<_>>();
        let input_bytes = requests.capacity() * std::mem::size_of::<(wire::PutObjectHeader, OwnedBytes)>()
            + requests.iter().map(|(header, _)| put_input_bytes(header)).sum::<usize>();
        let custody = ReadAllocation::adopt(&meter, input_bytes)?;
        let input = ObjectsNativeRequest::new(requests, custody, &meter)?;
        let results = provider.put_batch_native(input, Arc::clone(&meter))?.await?;
        assert_eq!(results.results().len(), 3);
        assert!(results.results()[0].is_ok());
        assert!(matches!(&results.results()[1],
            Err(NativeObjectsError { cause: NativeObjectsCause::Service(Error { code: QuotaExceeded }), .. })));
        assert!(results.results()[2].is_ok());
        for key in ["a", "c"] {
            let object = provider.get(wire::GetObjectRequest {
                bucket: Some(bucket.clone()), object_key: key.into(), ..Default::default()
            }, 4).await?;
            assert_eq!(object.body().as_slice(), b"four");
        }
        assert!(matches!(provider.get(wire::GetObjectRequest {
            bucket: Some(bucket), object_key: "b".into(), ..Default::default()
        }, 9).await, Err(Error { code: NotFound })));
        drop(results);
        drop(provider);
        assert_eq!(observed.memory.load(SeqCst), 0);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_table_events_precede_lookup_and_publication_without_late_fabrication()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::sync::atomic::Ordering::SeqCst;
        let observed = Arc::new(NativeTransportMeter::new_table());
        let meter: Arc<dyn NativeReadMeter> = observed.clone();
        let (provider, bucket) = MemoryObjects::with_default_bucket();
        provider.put(wire::PutObjectHeader {
            bucket: Some(bucket.clone()), object_key: "existing".into(), ..Default::default()
        }, OwnedBytes::from_static(b"data")).await?;
        let query = || wire::GetObjectRequest {
            bucket: Some(bucket.clone()), object_key: "existing".into(), ..Default::default()
        };
        observed.read_limit.store(0, SeqCst);
        let request = query();
        let custody = ReadAllocation::adopt(&meter, get_input_bytes(&request))?;
        let input = ObjectsNativeRequest::new(request, custody, &meter)?;
        let denied = provider.get_native(input, 4, Arc::clone(&meter))?.await
            .err().ok_or("lookup bypassed original backend admission")?;
        assert!(matches!(&denied.cause, NativeObjectsCause::Admission(error)
            if error.kind() == std::io::ErrorKind::PermissionDenied));
        assert_eq!(observed.memory.load(SeqCst), 0);

        observed.read_limit.store(u64::MAX, SeqCst);
        let request = query();
        let custody = ReadAllocation::adopt(&meter, get_input_bytes(&request))?;
        let input = ObjectsNativeRequest::new(request, custody, &meter)?;
        let future = provider.get_native(input, 4, Arc::clone(&meter))?;
        // Deny response allocation after the funded lookup.
        observed.limit.store(observed.memory.load(SeqCst), SeqCst);
        let copy_denied = future.await.err().ok_or("header copy bypassed allocation admission")?;
        assert!(matches!(&copy_denied.cause, NativeObjectsCause::Admission(error)
            if error.kind() == std::io::ErrorKind::OutOfMemory));
        assert_eq!(observed.memory.load(SeqCst), 0);

        observed.limit.store(u64::MAX, SeqCst);
        observed.write_limit.store(0, SeqCst);
        let header = wire::PutObjectHeader {
            bucket: Some(bucket.clone()), object_key: "unpublished".into(), ..Default::default()
        };
        let custody = ReadAllocation::adopt(&meter, put_input_bytes(&header))?;
        let input = ObjectsNativeRequest::new((header, OwnedBytes::from_static(b"data")), custody, &meter)?;
        let denied = provider.put_native(input, Arc::clone(&meter))?.await
            .err().ok_or("publication bypassed original backend admission")?;
        assert!(matches!(&denied.cause, NativeObjectsCause::Admission(error)
            if error.kind() == std::io::ErrorKind::PermissionDenied));
        assert!(matches!(provider.get(wire::GetObjectRequest {
            bucket: Some(bucket), object_key: "unpublished".into(), ..Default::default()
        }, 4).await, Err(Error { code: NotFound })));
        assert_eq!(observed.memory.load(SeqCst), 0);

        let input = ObjectsNativeRequest::new(Vec::new(), ReadAllocation::empty(&meter), &meter)?;
        drop(provider.get_batch_native(input, Arc::clone(&meter))?.await?);
        let input = ObjectsNativeRequest::new(Vec::new(), ReadAllocation::empty(&meter), &meter)?;
        drop(provider.put_batch_native(input, Arc::clone(&meter))?.await?);
        assert_eq!(observed.memory.load(SeqCst), 0);
        Ok(())
    }
}

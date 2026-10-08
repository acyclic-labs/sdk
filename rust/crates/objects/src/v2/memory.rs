//! Bounded logical Objects reference provider. Listings traverse current keys.
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex};
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

use super::{Error, NativeBatchObjects, Object, ObjectsProvider, request, wire};
use crate::body::StoredBody;
use crate::obs;
use bytes::Bytes;
use imbl::OrdMap;
use prost::Message;
use wire::ErrorCode::{
    AlreadyExists, IdempotencyMismatch, InvalidArgument, NotFound, NotModified, PreconditionFailed,
    QuotaExceeded, Unavailable,
};
#[cfg(feature = "local")]
pub(super) mod persistence;

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
struct State {
    buckets: BTreeMap<String, Bucket>,
    uploads: BTreeMap<String, Upload>,
    receipts: OrdMap<String, Receipt>,
    sequence: u64,
    /// Body bytes of every stored object, maintained by [`stored_bytes`].
    object_bytes: usize,
}
#[derive(Clone)]
struct Bucket {
    info: wire::Bucket,
    objects: OrdMap<String, Stored>,
}
// Equality lets a commit find what changed by walking only the subtrees two
// persistent maps do not share (`OrdMap::diff`).
#[derive(Clone, PartialEq)]
struct Stored {
    info: wire::ObjectInfo,
    body: StoredBody,
}
#[derive(Clone)]
struct Upload {
    bucket: String,
    key: String,
    metadata: Option<wire::ObjectMetadata>,
    parts: BTreeMap<u32, (wire::UploadedPart, StoredBody)>,
}
#[derive(Clone, PartialEq)]
struct Receipt {
    digest: [u8; 32],
    response: Vec<u8>,
    #[cfg(feature = "local")]
    kind: u32,
}

/// Updates `next`'s object byte total from `before`'s by walking only the
/// object subtrees the two states do not share, so quota admission costs the
/// change, not the store size. Returns that total plus in-progress upload parts.
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
    ///
    /// The bootstrap bucket has an epoch creation time. Cursor authentication entropy is
    /// obtained on first pagination; failure returns `Unavailable`, never an unsigned token.
    /// No cardinality limit is added to the composition's existing 64 MiB byte limit.
    #[must_use]
    pub fn with_default_bucket() -> (Self, wire::BucketRef) {
        let bucket = wire::BucketRef {
            name: "default".to_owned(),
        };
        let mut state = State::default();
        state.buckets.insert(
            bucket.name.clone(),
            Bucket {
                info: wire::Bucket {
                    bucket: Some(bucket.clone()),
                    created_at: Some(prost_types::Timestamp::default()),
                },
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
    ///
    /// # Errors
    /// Rejects invalid names, unavailable entropy/time or insufficient entry capacity.
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
                name,
                Bucket {
                    info: info.clone(),
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
        let mut token_key = [0; 32];
        getrandom::fill(&mut token_key).map_err(|_| Error::from(Unavailable))?;
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

    // A failed mutation rolls back both publication and its receipt. Persistent ordered
    // maps share unchanged entries, so admission does not copy the entire object store
    // and retained retry inventory. Only the committed state owns current representations.
    fn mutate<R: Message + Default>(
        &self,
        digest: [u8; 32],
        identity: &Option<wire::MutationIdentity>,
        action: impl FnOnce(&mut State) -> Result<R, Error>,
    ) -> Result<R, Error> {
        request::identity(identity)?;
        let mut guard = self.lock_state()?;
        self.mutate_locked(&mut guard, digest, identity, action)
    }

    fn mutate_locked<R: Message + Default>(
        &self,
        guard: &mut State,
        digest: [u8; 32],
        identity: &Option<wire::MutationIdentity>,
        action: impl FnOnce(&mut State) -> Result<R, Error>,
    ) -> Result<R, Error> {
        #[cfg(feature = "local")]
        if let Some(journal) = &self.journal {
            journal.check()?;
        }
        request::identity(identity)?;
        if let Some(identity) = identity
            && let Some(receipt) = guard.receipts.get(&identity.idempotency_key)
        {
            if receipt.digest != digest {
                return Err(IdempotencyMismatch.into());
            }
            return R::decode(receipt.response.as_slice()).map_err(|_| Unavailable.into());
        }
        let mut next = guard.clone();
        let response = action(&mut next)?;
        if let Some(identity) = identity {
            next.receipts.insert(
                identity.idempotency_key.clone(),
                Receipt {
                    digest,
                    response: response.encode_to_vec(),
                    #[cfg(feature = "local")]
                    kind: persistence::response_kind::<R>()?,
                },
            );
        }
        let count = next.buckets.len()
            + next.uploads.len()
            + next.receipts.len()
            + next
                .buckets
                .values()
                .map(|bucket| bucket.objects.len())
                .sum::<usize>()
            + next
                .uploads
                .values()
                .map(|upload| upload.parts.len())
                .sum::<usize>();
        let bytes = stored_bytes(guard, &mut next).ok_or(Error::from(QuotaExceeded))?;
        if count > self.options.maximum_entries || bytes > self.options.maximum_bytes {
            return Err(QuotaExceeded.into());
        }
        #[cfg(feature = "local")]
        if let Some(journal) = &self.journal {
            if self.defer_local {
                journal.validate_bodies(&next)?;
            } else {
                journal.commit(guard, &mut next)?;
            }
        }
        *guard = next;
        Ok(response)
    }

    #[cfg(feature = "local")]
    fn put_durable_batch_locked(
        &self,
        state: &mut State,
        requests: Vec<(wire::PutObjectHeader, Bytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        let mut staged = self.clone();
        staged.defer_local = true;
        let mut input = requests.into_iter().peekable();
        let mut results = Vec::with_capacity(input.len());
        while input.peek().is_some() {
            // Group puts while their bounded record bytes fit one private
            // record, so a typical small publish costs one synchronization.
            let mut next = state.clone();
            let mut bytes = 0_usize;
            let mut batch = Vec::new();
            while let Some((query, body)) = input.next_if(|(query, body)| {
                let total = bytes.saturating_add(persistence::put_record_bytes(query, body));
                let fits = bytes == 0 || total <= persistence::RECORD_LIMIT;
                if fits {
                    bytes = total;
                }
                fits
            }) {
                batch.push(staged.put_locked(&mut next, &query, body));
            }
            if next.sequence == state.sequence {
                results.extend(batch);
                continue;
            }
            let committed = self
                .journal
                .as_ref()
                .ok_or(Error::from(Unavailable))
                .and_then(|journal| journal.commit(state, &mut next));
            match committed {
                Ok(()) => {
                    *state = next;
                    results.extend(batch);
                }
                Err(error) => {
                    results.extend(batch.into_iter().map(|result| result.and(Err(error))));
                }
            }
        }
        results
    }

    fn put_locked(
        &self,
        state: &mut State,
        query: &wire::PutObjectHeader,
        body: Bytes,
    ) -> Result<wire::ObjectInfo, Error> {
        let name = request::bucket(&query.bucket)?;
        request::key(&query.object_key)?;
        request::metadata(&query.metadata)?;
        request::preconditions(&query.preconditions)?;
        request::upload_length(body.len() as u64)?;
        self.mutate_locked(
            state,
            request::put_digest(query, &body)?,
            &query.mutation,
            |state| {
                publish(
                    state,
                    name,
                    &query.object_key,
                    query.metadata.clone(),
                    &query.preconditions,
                    StoredBody::memory(body),
                )
            },
        )
    }

    fn cursor_key_with(
        &self,
        fill: impl FnOnce(&mut [u8; 32]) -> Result<(), Error>,
    ) -> Result<[u8; 32], Error> {
        let mut key = self
            .token_key
            .lock()
            .map_err(|_| Error::from(Unavailable))?;
        if let Some(key) = *key {
            return Ok(key);
        }
        let mut candidate = [0; 32];
        fill(&mut candidate)?;
        *key = Some(candidate);
        Ok(candidate)
    }

    fn cursor(&self, query: &wire::ListObjectsRequest, key: &str) -> Result<String, Error> {
        let payload = key.as_bytes();
        let token_key =
            self.cursor_key_with(|key| getrandom::fill(key).map_err(|_| Error::from(Unavailable)))?;
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
    let current = state
        .buckets
        .get(bucket)
        .ok_or(Error::from(NotFound))?
        .objects
        .get(key);
    check_condition(condition, current)?;
    let info = wire::ObjectInfo {
        etag: next_id(state)?,
        size: body.len() as u64,
        metadata,
        last_modified: Some(timestamp()?),
    };
    state
        .buckets
        .get_mut(bucket)
        .ok_or(Error::from(NotFound))?
        .objects
        .insert(
            key.to_owned(),
            Stored {
                info: info.clone(),
                body,
            },
        );
    Ok(info)
}
fn upload<'a>(state: &'a State, bucket: &str, key: &str, id: &str) -> Result<&'a Upload, Error> {
    request::upload_id(id)?;
    let value = state.uploads.get(id).ok_or(Error::from(NotFound))?;
    if value.bucket != bucket || value.key != key {
        return Err(NotFound.into());
    }
    Ok(value)
}

struct ResolvedGet {
    header: wire::GetObjectHeader,
    body: StoredBody,
    start: usize,
    end: usize,
}

impl ResolvedGet {
    async fn read(self) -> Result<Object, Error> {
        Ok(Object {
            header: self.header,
            body: self
                .body
                .read_async(self.start, self.end)
                .await
                .map_err(|_| Error::from(Unavailable))?,
        })
    }
}

fn get_locked(
    state: &State,
    query: &wire::GetObjectRequest,
    maximum_bytes: u64,
) -> Result<ResolvedGet, Error> {
    request::validate_binary("objects/get", &query.encode_to_vec(), 0)?;
    let name = request::bucket(&query.bucket)?;
    request::key(&query.object_key)?;
    let value = state
        .buckets
        .get(name)
        .and_then(|bucket| bucket.objects.get(&query.object_key))
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
        return Err(QuotaExceeded.into());
    }
    Ok(ResolvedGet {
        header: wire::GetObjectHeader {
            object: Some(value.info.clone()),
            content_range: range,
        },
        body: value.body.clone(),
        start,
        end,
    })
}

#[async_trait::async_trait]
impl NativeBatchObjects for MemoryObjects {
    async fn put_batch(
        &self,
        requests: Vec<(wire::PutObjectHeader, Bytes)>,
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
                        return vec![Err(Unavailable.into()); requests.len()];
                    };
                    requests
                        .into_iter()
                        .map(|(query, maximum)| get_locked(&state, &query, maximum))
                        .collect::<Vec<_>>()
                };
                let mut results = Vec::with_capacity(selected.len());
                for value in selected {
                    results.push(match value {
                        Ok(value) => value.read().await,
                        Err(error) => Err(error),
                    });
                }
                results
            },
        )
        .await
    }
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
                    if state.buckets.contains_key(&query.name) {
                        return Err(AlreadyExists.into());
                    }
                    let info = wire::Bucket {
                        bucket: Some(wire::BucketRef {
                            name: query.name.clone(),
                        }),
                        created_at: Some(timestamp()?),
                    };
                    state.buckets.insert(
                        query.name.clone(),
                        Bucket {
                            info: info.clone(),
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
            Ok(self
                .lock_state()?
                .buckets
                .get(name)
                .ok_or(Error::from(NotFound))?
                .info
                .clone())
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
                        || state.uploads.values().any(|upload| upload.bucket == name)
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
        body: Bytes,
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
                let object = selected.read().await?;
                obs::record!("bytes" = object.body.len());
                Ok(object)
            },
        )
        .await
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
                .and_then(|bucket| bucket.objects.get(&query.object_key))
                .ok_or(Error::from(NotFound))?;
            read_condition(&value.info, &query.if_match, &query.if_none_match)?;
            Ok(wire::HeadObjectResponse {
                object: Some(value.info.clone()),
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
                check_condition(&query.preconditions, bucket.objects.get(&query.object_key))?;
                Ok(wire::DeleteObjectResponse {
                    existed: bucket.objects.remove(&query.object_key).is_some(),
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
                // Logical entries (keys and common prefixes) never sort below the keys
                // that produce them, so the scan starts strictly after the cursor and
                // stops at `limit + 1` entries, skipping each common-prefix group by
                // seeking to its successor.
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
                        page.push((key.clone(), Some(value.info.clone())));
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
                            id.clone(),
                            Upload {
                                bucket: name.to_owned(),
                                key: query.object_key.clone(),
                                metadata: query.metadata.clone(),
                                parts: BTreeMap::new(),
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
        body: Bytes,
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
                            .get_mut(&query.upload_id)
                            .ok_or(Error::from(NotFound))?
                            .parts
                            .insert(
                                query.part_number,
                                (receipt.clone(), StoredBody::memory(body.clone())),
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
                    .map(|(part, _)| part.clone())
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
                            if selected != receipt
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
                        let metadata = value.metadata.clone();
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
                                parts: bodies.into(),
                                length: size,
                            },
                        )?;
                        state.uploads.remove(&query.upload_id);
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
                    if state.uploads.contains_key(&query.upload_id) {
                        upload(state, name, &query.object_key, &query.upload_id)?;
                    }
                    Ok(wire::AbortMultipartResponse {
                        existed: state.uploads.remove(&query.upload_id).is_some(),
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
            Err(Unavailable.into())
        });
        assert_eq!(failed.map_err(|error| error.code), Err(Unavailable));
        let clone = provider.clone();
        let key = clone.cursor_key_with(|key| {
            key.fill(2);
            Ok(())
        })?;
        drop(clone);
        assert_eq!(key, [2; 32]);
        assert_eq!(provider.cursor_key_with(|_| Err(Unavailable.into()))?, key);
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
                    Bytes::from_static(b"body"),
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
}

//! Strongly consistent S3-shaped view over one canonical workspace.
//!
//! This module owns no object namespace or mutable state. Reads pin one
//! immutable generation and writes use the ordinary workspace transaction.

use crate::kernel::{FileKind, FilePayload, LogicalName, NameEncoding, NamespacePath};
use crate::model::{CheckoutMode, GenerationSelector};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, ByteRange, CancellationToken, IdempotencyKey,
    TransactionCommit, WorkBudget, Workspace, WorkspaceError,
};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use thiserror::Error;

const LIST_PAGE_ENTRIES: u32 = 256;
const S3_ETAG_DOMAIN: &[u8] = b"acyclic-fs-s3-etag-v1\0";

/// Bounded stable `ListObjectsV2` request semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3ListOptions {
    /// Exact bytewise UTF-8 key prefix.
    pub prefix: String,
    /// Either no delimiter or the S3 path delimiter `/`.
    pub delimiter: Option<char>,
    /// Initial exclusive key marker. Ignored after a continuation is supplied.
    pub start_after: Option<String>,
    /// Opaque immutable-generation continuation returned by a prior page.
    /// Callers should reuse the token returned by a prior page.
    pub continuation: Option<S3ListCursor>,
    /// Maximum combined objects and common prefixes returned.
    pub maximum_keys: u32,
    /// Hard authenticated namespace entries examined by this request.
    pub maximum_entries_examined: u32,
}

impl Default for S3ListOptions {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            delimiter: Some('/'),
            start_after: None,
            continuation: None,
            maximum_keys: 1_000,
            maximum_entries_examined: 100_000,
        }
    }
}

/// Complete metadata for one S3-visible regular file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3ObjectHead {
    /// Relative workspace key without a leading slash.
    pub key: String,
    /// Exact logical file length.
    pub content_length: u64,
    /// Strong validator bound to the immutable generation and key.
    pub etag: String,
}

/// One listed regular-file object.
pub type S3Object = S3ObjectHead;

/// One stable bounded listing page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3List {
    /// Regular files selected in bytewise key order.
    pub objects: Vec<S3Object>,
    /// Delimiter-collapsed prefixes selected in bytewise key order.
    pub common_prefixes: Vec<String>,
    /// Immutable-generation continuation when more matching results exist.
    pub next_continuation: Option<S3ListCursor>,
    /// Exact authenticated namespace entries examined.
    pub entries_examined: u32,
}

/// Opaque stable S3 listing continuation bound to one generation and query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3ListCursor {
    generation: crate::GenerationId,
    after: String,
    after_prefix: bool,
    query: crate::Digest,
}

impl S3ListCursor {
    /// Encodes this cursor for an S3 continuation-token field.
    #[must_use]
    pub fn encode(&self) -> String {
        format!(
            "v2.{}.{}.{}.{}",
            hex::encode(self.generation.digest().into_bytes()),
            hex::encode(self.query.into_bytes()),
            if self.after_prefix { "p" } else { "o" },
            hex::encode(self.after.as_bytes())
        )
    }

    /// Decodes one bounded canonical continuation token. This checks syntax,
    /// not the token's origin; the listing operation checks generation/query.
    ///
    /// # Errors
    ///
    /// Rejects malformed versions, identities, UTF-8, and oversized markers.
    pub fn decode(token: &str, maximum_key_bytes: u32) -> Result<Self, S3Error> {
        let mut fields = token.split('.');
        let version = fields.next().ok_or(S3Error::InvalidContinuation)?;
        if version != "v2" {
            return Err(S3Error::InvalidContinuation);
        }
        let generation = decode_fixed::<32>(fields.next())?;
        let query = decode_fixed::<32>(fields.next())?;
        let after_prefix = match fields.next() {
            Some("o") => false,
            Some("p") => true,
            _ => return Err(S3Error::InvalidContinuation),
        };
        let after = fields.next().ok_or(S3Error::InvalidContinuation)?;
        if fields.next().is_some() || after.len() > maximum_key_bytes as usize * 2 {
            return Err(S3Error::InvalidContinuation);
        }
        let after = hex::decode(after).map_err(|_| S3Error::InvalidContinuation)?;
        let after = String::from_utf8(after).map_err(|_| S3Error::InvalidContinuation)?;
        if after.len() > maximum_key_bytes as usize {
            return Err(S3Error::InvalidContinuation);
        }
        Ok(Self {
            generation: crate::GenerationId::new(crate::Digest::from_bytes(generation)),
            after,
            after_prefix,
            query: crate::Digest::from_bytes(query),
        })
    }
}

/// A zero-state S3 mapping over one workspace.
pub struct S3Workspace<A, O> {
    workspace: Workspace<A, O>,
}

/// Hard multipart staging bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct S3MultipartOptions {
    /// Maximum distinct uploaded part numbers.
    pub maximum_parts: u32,
    /// Maximum bytes admitted for any one part.
    pub maximum_part_bytes: u64,
}

impl Default for S3MultipartOptions {
    fn default() -> Self {
        Self {
            maximum_parts: 10_000,
            maximum_part_bytes: 5 * 1024 * 1024 * 1024,
        }
    }
}

/// One unpublished multipart candidate. Dropping it aborts without authority
/// mutation; staged immutable objects remain safe collectible orphans.
pub struct S3MultipartUpload<A, O> {
    transaction: crate::Transaction<A, O>,
    key: String,
    options: S3MultipartOptions,
    parts: BTreeMap<u32, crate::StagedContent>,
}

impl<A, O> Clone for S3Workspace<A, O> {
    fn clone(&self) -> Self {
        Self {
            workspace: self.workspace.clone(),
        }
    }
}

impl<A, O> Workspace<A, O> {
    /// Returns the protocol-neutral S3 view of this workspace.
    #[must_use]
    pub fn s3(&self) -> S3Workspace<A, O> {
        S3Workspace {
            workspace: self.clone(),
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> S3Workspace<A, O> {
    /// Starts one unpublished multipart object transaction.
    ///
    /// # Errors
    ///
    /// Rejects invalid keys or zero staging bounds.
    pub async fn create_multipart_upload(
        &self,
        key: &str,
        options: S3MultipartOptions,
        idempotency_key: IdempotencyKey,
    ) -> Result<S3MultipartUpload<A, O>, S3Error> {
        validate_key(key)?;
        if options.maximum_parts == 0 || options.maximum_part_bytes == 0 {
            return Err(S3Error::InvalidRequest("multipart bounds must be positive"));
        }
        Ok(S3MultipartUpload {
            transaction: self.workspace.begin_transaction(idempotency_key).await?,
            key: key.to_owned(),
            options,
            parts: BTreeMap::new(),
        })
    }
    /// Reads object metadata from one pinned generation.
    ///
    /// # Errors
    ///
    /// Rejects invalid keys, absent/non-regular paths, and backend failures.
    pub async fn head_object(&self, key: &str) -> Result<S3ObjectHead, S3Error> {
        let generation = self.workspace.head().await?;
        self.head_object_in(generation, key).await
    }

    /// Reads object metadata from one exact immutable generation.
    ///
    /// # Errors
    ///
    /// Rejects a foreign generation, invalid key, absent/non-regular path, or backend failure.
    pub async fn head_object_at(
        &self,
        generation: &crate::Generation<A, O>,
        key: &str,
    ) -> Result<S3ObjectHead, S3Error> {
        if generation.workspace_id() != self.workspace.id() {
            return Err(S3Error::ForeignGeneration);
        }
        self.head_object_in(generation.clone(), key).await
    }

    async fn head_object_in(
        &self,
        generation: crate::Generation<A, O>,
        key: &str,
    ) -> Result<S3ObjectHead, S3Error> {
        let mut checkout = self
            .workspace
            .engine_checkout(
                GenerationSelector::Exact(generation.id()),
                CheckoutMode::read_only_pinned(),
            )
            .await?;
        let path = key_path(key, checkout.volume_config())?;
        let record = checkout
            .lookup_no_follow(&path, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(WorkspaceError::engine)?
            .value
            .record
            .ok_or(S3Error::NotFound)?;
        let content_length = regular_bytes(&record.payload)?;
        Ok(S3ObjectHead {
            key: key.to_owned(),
            content_length,
            etag: etag(generation.id().digest().as_bytes(), key),
        })
    }

    /// Reads one complete object under a caller byte bound.
    ///
    /// # Errors
    ///
    /// Returns key, kind, size, authentication, or storage failures.
    pub async fn get_object(&self, key: &str, maximum_bytes: u64) -> Result<Bytes, S3Error> {
        validate_key(key)?;
        self.workspace
            .read(&absolute_key(key), maximum_bytes)
            .await
            .map_err(Into::into)
    }

    /// Reads one exact object byte range without materializing the rest.
    ///
    /// # Errors
    ///
    /// Returns key, range, kind, authentication, or storage failures.
    pub async fn get_object_range(&self, key: &str, range: ByteRange) -> Result<Bytes, S3Error> {
        let generation = self.workspace.head().await?;
        self.get_object_range_in(generation, key, range).await
    }

    /// Reads one exact byte range from one immutable generation.
    ///
    /// # Errors
    ///
    /// Rejects a foreign generation, invalid key/range, or authenticated storage failure.
    pub async fn get_object_range_at(
        &self,
        generation: &crate::Generation<A, O>,
        key: &str,
        range: ByteRange,
    ) -> Result<Bytes, S3Error> {
        if generation.workspace_id() != self.workspace.id() {
            return Err(S3Error::ForeignGeneration);
        }
        self.get_object_range_in(generation.clone(), key, range)
            .await
    }

    async fn get_object_range_in(
        &self,
        generation: crate::Generation<A, O>,
        key: &str,
        range: ByteRange,
    ) -> Result<Bytes, S3Error> {
        let mut checkout = self
            .workspace
            .engine_checkout(
                GenerationSelector::Exact(generation.id()),
                CheckoutMode::read_only_pinned(),
            )
            .await?;
        let path = key_path(key, checkout.volume_config())?;
        checkout
            .read_file_range(
                &path,
                range,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value.bytes)
            .map_err(|failure| S3Error::Workspace(WorkspaceError::engine(failure)))
    }

    /// Atomically creates or replaces one object.
    ///
    /// # Errors
    ///
    /// Returns key, parent, transaction, or publication failures.
    pub async fn put_object(
        &self,
        key: &str,
        bytes: Bytes,
        idempotency_key: IdempotencyKey,
    ) -> Result<TransactionCommit<A, O>, S3Error> {
        let mut transaction = self.workspace.begin_transaction(idempotency_key).await?;
        create_parent_directories(&mut transaction, key).await?;
        transaction.write(&absolute_key(key), bytes).await?;
        transaction.commit().await.map_err(Into::into)
    }

    /// Prepares one S3 object from already staged immutable content in the
    /// caller's transaction. The caller can validate its streaming checksums
    /// before invoking this method and commits the transaction afterward.
    ///
    /// # Errors
    ///
    /// Rejects a foreign transaction, malformed key or staged content, or a
    /// Filesystem mutation failure. No content is published on error.
    pub async fn write_staged(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        key: &str,
        parts: &[crate::StagedContent],
    ) -> Result<(), S3Error> {
        if transaction.workspace_id() != self.workspace.id() {
            return Err(S3Error::ForeignTransaction);
        }
        create_parent_directories(transaction, key).await?;
        transaction.write_staged(&absolute_key(key), parts).await?;
        Ok(())
    }

    /// Atomically removes one object.
    ///
    /// # Errors
    ///
    /// Returns key, transaction, or publication failures. An absent key is a
    /// successful no-op, matching S3 delete semantics.
    pub async fn delete_object(
        &self,
        key: &str,
        idempotency_key: IdempotencyKey,
    ) -> Result<(), S3Error> {
        validate_key(key)?;
        let mut transaction = self.workspace.begin_transaction(idempotency_key).await?;
        let mutated = match transaction.remove(&absolute_key(key)).await {
            Ok(()) => true,
            Err(WorkspaceError::NotFound) => false,
            Err(error) => return Err(error.into()),
        };
        if mutated {
            complete_delete(&transaction.commit().await?)
        } else {
            Ok(())
        }
    }

    /// Atomically removes several objects through one workspace transaction.
    ///
    /// # Errors
    ///
    /// Returns request, key, transaction, or publication failures. Absent keys
    /// are successful no-ops, including repeated bulk deletion.
    pub async fn delete_objects(
        &self,
        keys: &[String],
        idempotency_key: IdempotencyKey,
    ) -> Result<(), S3Error> {
        if keys.is_empty() {
            return Err(S3Error::InvalidRequest("delete set is empty"));
        }
        let mut transaction = self.workspace.begin_transaction(idempotency_key).await?;
        let mut mutated = false;
        for key in keys {
            validate_key(key)?;
            match transaction.remove(&absolute_key(key)).await {
                Ok(()) => mutated = true,
                Err(WorkspaceError::NotFound) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if mutated {
            complete_delete(&transaction.commit().await?)
        } else {
            Ok(())
        }
    }

    /// Copies one object through immutable extent references.
    ///
    /// # Errors
    ///
    /// Returns key, kind, transaction, or publication failures.
    pub async fn copy_object(
        &self,
        source: &str,
        destination: &str,
        idempotency_key: IdempotencyKey,
    ) -> Result<TransactionCommit<A, O>, S3Error> {
        validate_key(source)?;
        let mut transaction = self.workspace.begin_transaction(idempotency_key).await?;
        create_parent_directories(&mut transaction, destination).await?;
        transaction
            .copy(&absolute_key(source), &absolute_key(destination))
            .await?;
        transaction.commit().await.map_err(Into::into)
    }

    /// Lists regular files and delimiter prefixes from one pinned generation.
    ///
    /// # Errors
    ///
    /// Returns malformed options, unsupported names, work exhaustion, or
    /// authenticated storage failures.
    pub async fn list_objects(&self, options: S3ListOptions) -> Result<S3List, S3Error> {
        self.list_objects_in(None, options).await
    }

    /// Lists from one exact immutable generation with stable continuation.
    ///
    /// # Errors
    ///
    /// Rejects a foreign generation, malformed options/cursors, exhausted work, or storage failure.
    pub async fn list_objects_at(
        &self,
        generation: &crate::Generation<A, O>,
        options: S3ListOptions,
    ) -> Result<S3List, S3Error> {
        if generation.workspace_id() != self.workspace.id() {
            return Err(S3Error::ForeignGeneration);
        }
        self.list_objects_in(Some(generation.clone()), options)
            .await
    }

    async fn list_objects_in(
        &self,
        selected: Option<crate::Generation<A, O>>,
        options: S3ListOptions,
    ) -> Result<S3List, S3Error> {
        validate_list_options(&options)?;
        let query = list_query_digest(&options);
        let generation = match options.continuation.as_ref() {
            Some(cursor) => {
                if cursor.query != query {
                    return Err(S3Error::InvalidContinuation);
                }
                self.workspace.generation(cursor.generation).await?
            }
            None => match selected {
                Some(generation) => generation,
                None => self.workspace.head().await?,
            },
        };
        let mut checkout = self
            .workspace
            .engine_checkout(
                GenerationSelector::Exact(generation.id()),
                CheckoutMode::read_only_pinned(),
            )
            .await?;
        let limits = checkout.volume_config().limits;
        let (frontier, frontier_key, frontier_name_prefix) =
            listing_frontier(&options.prefix, checkout.volume_config())?;
        if !frontier.components().is_empty() {
            let record = checkout
                .lookup_no_follow(&frontier, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(|failure| S3Error::Workspace(WorkspaceError::engine(failure)))?
                .value
                .record;
            if !record.is_some_and(|record| record.kind == FileKind::Directory) {
                return Ok(empty_list());
            }
        }
        list_objects_lazy(
            &mut checkout,
            &options,
            limits,
            generation.id(),
            query,
            frontier,
            frontier_key,
            (!frontier_name_prefix.is_empty()).then_some(frontier_name_prefix),
        )
        .await
    }
}

enum ListingTask {
    Directory {
        path: NamespacePath,
        key_prefix: String,
        name_prefix: Option<String>,
        after: Option<LogicalName>,
        inclusive: bool,
    },
    Object {
        key: String,
        payload: FilePayload,
    },
}

fn queue_listing_task(
    pending: &mut BTreeMap<(String, u64), ListingTask>,
    sequence: &mut u64,
    lower_key: String,
    task: ListingTask,
) -> Result<(), S3Error> {
    let next = sequence.checked_add(1).ok_or(S3Error::ListLimit)?;
    pending.insert((lower_key, *sequence), task);
    *sequence = next;
    Ok(())
}

/// Seek only when every earlier child directory has a flattened `name/`
/// bound below the marker. A marker such as `a.` is ambiguous because `a/x`
/// sorts after it, so that case keeps the ordered walk.
fn direct_child_seek(
    options: &S3ListOptions,
    limits: crate::model::VolumeLimits,
    frontier_key: &str,
) -> Option<(LogicalName, bool)> {
    let Some(cursor) = &options.continuation else {
        return None;
    };
    let relative = cursor.after.strip_prefix(frontier_key)?;
    let name = if !cursor.after_prefix && !relative.contains('/') {
        relative
    } else if cursor.after_prefix && options.delimiter == Some('/') {
        let name = relative.strip_suffix('/')?;
        if name.contains('/') {
            return None;
        }
        name
    } else {
        return None;
    };
    if name
        .chars()
        .skip(1)
        .any(|character| (character as u32) < u32::from(b'/'))
    {
        return None;
    }
    LogicalName::new(
        NameEncoding::Utf8,
        name.as_bytes().to_vec(),
        limits.maximum_component_bytes,
    )
    .ok()
    .map(|name| (name, !cursor.after_prefix))
}

/// Merges directory-page lower bounds and object keys in flattened UTF-8
/// order. A directory's `name/` bound is distinct from its component name:
/// sibling `a.` must precede a descendant `a/x`.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn list_objects_lazy<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut crate::Checkout<A, O>,
    options: &S3ListOptions,
    limits: crate::model::VolumeLimits,
    generation: crate::GenerationId,
    query: crate::Digest,
    frontier: NamespacePath,
    frontier_key: String,
    frontier_name_prefix: Option<String>,
) -> Result<S3List, S3Error> {
    let mut pending = BTreeMap::new();
    let mut sequence = 0_u64;
    let seek = direct_child_seek(options, limits, &frontier_key);
    queue_listing_task(
        &mut pending,
        &mut sequence,
        frontier_key.clone(),
        ListingTask::Directory {
            path: frontier,
            key_prefix: frontier_key,
            name_prefix: frontier_name_prefix,
            after: seek.as_ref().map(|(name, _)| name.clone()),
            inclusive: seek.as_ref().is_some_and(|(_, inclusive)| *inclusive),
        },
    )?;
    let maximum = usize::try_from(options.maximum_keys).map_err(|_| S3Error::ListLimit)?;
    let mut result = empty_list();
    let mut last: Option<(String, bool)> = None;
    let mut examined = 0_u32;
    while let Some((_, task)) = pending.pop_first() {
        match task {
            ListingTask::Directory {
                path,
                key_prefix,
                name_prefix,
                after,
                inclusive,
            } => {
                let remaining = options.maximum_entries_examined - examined;
                if remaining == 0 {
                    return Err(S3Error::ListLimit);
                }
                let page_limit = LIST_PAGE_ENTRIES
                    .min(remaining)
                    .min(options.maximum_keys.saturating_add(1));
                let page = if inclusive {
                    checkout
                        .list_directory_records_at_or_after(
                            &path,
                            after.as_ref().ok_or(S3Error::InvalidContinuation)?,
                            page_limit,
                            WorkBudget::UNBOUNDED,
                            &CancellationToken::new(),
                        )
                        .await
                } else {
                    checkout
                        .list_directory_records(
                            &path,
                            after.as_ref(),
                            page_limit,
                            WorkBudget::UNBOUNDED,
                            &CancellationToken::new(),
                        )
                        .await
                }
                .map_err(|failure| S3Error::Workspace(WorkspaceError::engine(failure)))?
                .value;
                examined = examined
                    .checked_add(u32::try_from(page.entries.len()).map_err(|_| S3Error::ListLimit)?)
                    .ok_or(S3Error::ListLimit)?;
                let continuation = if page.has_more {
                    let last_name = page
                        .entries
                        .last()
                        .ok_or(S3Error::InvalidObject)?
                        .name
                        .clone();
                    let lower_key = format!("{key_prefix}{}", utf8_name(&last_name)?);
                    Some((lower_key, last_name))
                } else {
                    None
                };
                for entry in page.entries {
                    let name = utf8_name(&entry.name)?;
                    if name_prefix
                        .as_ref()
                        .is_some_and(|prefix| !name.starts_with(prefix))
                    {
                        continue;
                    }
                    let key = format!("{key_prefix}{name}");
                    match entry.record.kind {
                        FileKind::Directory => {
                            let mut components = path.components().to_vec();
                            components.push(entry.name);
                            let child = NamespacePath::new(components, limits)
                                .map_err(WorkspaceError::path)?;
                            let child_key = format!("{key}/");
                            queue_listing_task(
                                &mut pending,
                                &mut sequence,
                                child_key.clone(),
                                ListingTask::Directory {
                                    path: child,
                                    key_prefix: child_key,
                                    name_prefix: None,
                                    after: None,
                                    inclusive: false,
                                },
                            )?;
                        }
                        FileKind::Regular => queue_listing_task(
                            &mut pending,
                            &mut sequence,
                            key.clone(),
                            ListingTask::Object {
                                key,
                                payload: entry.record.payload,
                            },
                        )?,
                        _ => {}
                    }
                }
                if let Some((lower_key, last_name)) = continuation {
                    queue_listing_task(
                        &mut pending,
                        &mut sequence,
                        lower_key,
                        ListingTask::Directory {
                            path,
                            key_prefix,
                            name_prefix,
                            after: Some(last_name),
                            inclusive: false,
                        },
                    )?;
                }
            }
            ListingTask::Object { key, payload } => {
                let Some(selected) = select_key(&key, options) else {
                    continue;
                };
                let (selected_key, is_prefix) = match &selected {
                    SelectedKey::Object => (key.as_str(), false),
                    SelectedKey::Prefix(prefix) => (prefix.as_str(), true),
                };
                if is_prefix
                    && last.as_ref().is_some_and(|(last_key, last_prefix)| {
                        *last_prefix && last_key == selected_key
                    })
                {
                    continue;
                }
                if result.objects.len() + result.common_prefixes.len() == maximum {
                    result.next_continuation = last.map(|(after, after_prefix)| S3ListCursor {
                        generation,
                        after,
                        after_prefix,
                        query,
                    });
                    break;
                }
                let selected_identity = (selected_key.to_owned(), is_prefix);
                match selected {
                    SelectedKey::Object => result.objects.push(S3Object {
                        key: key.clone(),
                        content_length: regular_bytes(&payload)?,
                        etag: etag(generation.digest().as_bytes(), &key),
                    }),
                    SelectedKey::Prefix(prefix) => result.common_prefixes.push(prefix),
                }
                last = Some(selected_identity);
            }
        }
    }
    result.entries_examined = examined;
    Ok(result)
}

fn empty_list() -> S3List {
    S3List {
        objects: Vec::new(),
        common_prefixes: Vec::new(),
        next_continuation: None,
        entries_examined: 0,
    }
}

fn listing_frontier(
    prefix: &str,
    config: crate::model::VolumeConfig,
) -> Result<(NamespacePath, String, String), S3Error> {
    let limits = config.limits;
    let Some(separator) = prefix.rfind('/') else {
        return Ok((
            NamespacePath::new(Vec::new(), limits).map_err(WorkspaceError::path)?,
            String::new(),
            prefix.to_owned(),
        ));
    };
    let directory = prefix.get(..separator).ok_or(S3Error::InvalidRequest(
        "listing prefix split point is invalid",
    ))?;
    let path = if directory.is_empty() {
        NamespacePath::new(Vec::new(), limits).map_err(WorkspaceError::path)?
    } else {
        key_path(directory, config)?
    };
    Ok((
        path,
        prefix
            .get(..=separator)
            .ok_or(S3Error::InvalidRequest(
                "listing prefix split point is invalid",
            ))?
            .to_owned(),
        prefix
            .get(separator + 1..)
            .ok_or(S3Error::InvalidRequest(
                "listing prefix split point is invalid",
            ))?
            .to_owned(),
    ))
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> S3MultipartUpload<A, O> {
    /// Stages or replaces one positive part number without publishing it.
    ///
    /// # Errors
    ///
    /// Rejects zero numbers, part/count bounds, or storage failures.
    pub async fn upload_part(&mut self, part_number: u32, bytes: Bytes) -> Result<(), S3Error> {
        if part_number == 0 {
            return Err(S3Error::InvalidRequest("part number must be positive"));
        }
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > self.options.maximum_part_bytes {
            return Err(S3Error::MultipartLimit);
        }
        if !self.parts.contains_key(&part_number)
            && u32::try_from(self.parts.len()).unwrap_or(u32::MAX) >= self.options.maximum_parts
        {
            return Err(S3Error::MultipartLimit);
        }
        let maximum = u64::try_from(bytes.len())
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let mut source = Cursor::new(bytes);
        let staged = self.transaction.stage_content(&mut source, maximum).await?;
        self.parts.insert(part_number, staged);
        Ok(())
    }

    /// Publishes exactly the caller-selected ordered part set as one object.
    ///
    /// # Errors
    ///
    /// Rejects empty, duplicate, or missing part identities and returns the
    /// ordinary workspace publication outcome.
    pub async fn complete(
        &mut self,
        ordered_parts: &[u32],
    ) -> Result<TransactionCommit<A, O>, S3Error> {
        if ordered_parts.is_empty() {
            return Err(S3Error::InvalidRequest("multipart completion is empty"));
        }
        let mut seen = BTreeSet::new();
        let mut staged = Vec::new();
        staged
            .try_reserve_exact(ordered_parts.len())
            .map_err(|_| S3Error::MultipartLimit)?;
        for part_number in ordered_parts {
            if !seen.insert(*part_number) {
                return Err(S3Error::InvalidRequest("multipart part is duplicated"));
            }
            staged.push(
                *self
                    .parts
                    .get(part_number)
                    .ok_or(S3Error::MissingPart(*part_number))?,
            );
        }
        create_parent_directories(&mut self.transaction, &self.key).await?;
        self.transaction
            .write_staged(&absolute_key(&self.key), &staged)
            .await?;
        self.transaction.commit().await.map_err(Into::into)
    }

    /// Aborts without authority mutation. Staged objects are collectible.
    pub fn abort(self) {}
}

enum SelectedKey {
    Object,
    Prefix(String),
}

fn select_key(key: &str, options: &S3ListOptions) -> Option<SelectedKey> {
    let remainder = key.strip_prefix(options.prefix.as_str())?;
    let selected = if options.delimiter == Some('/')
        && let Some(index) = remainder.find('/')
    {
        SelectedKey::Prefix(format!("{}{}", options.prefix, remainder.get(..=index)?))
    } else {
        SelectedKey::Object
    };
    let (selected_key, selected_prefix) = match &selected {
        SelectedKey::Object => (key, false),
        SelectedKey::Prefix(prefix) => (prefix.as_str(), true),
    };
    if let Some(cursor) = &options.continuation {
        if (selected_key, selected_prefix) <= (cursor.after.as_str(), cursor.after_prefix) {
            return None;
        }
    } else if options
        .start_after
        .as_ref()
        .is_some_and(|after| selected_key <= after.as_str())
    {
        return None;
    }
    Some(selected)
}

fn list_query_digest(options: &S3ListOptions) -> crate::Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-s3-list-query-v1\0");
    hasher.update(&(options.prefix.len() as u64).to_le_bytes());
    hasher.update(options.prefix.as_bytes());
    hasher.update(&[options.delimiter.map_or(0, |value| value as u8)]);
    crate::Digest::from_bytes(*hasher.finalize().as_bytes())
}

fn decode_fixed<const N: usize>(value: Option<&str>) -> Result<[u8; N], S3Error> {
    let value = value.ok_or(S3Error::InvalidContinuation)?;
    if value.len() != N * 2 {
        return Err(S3Error::InvalidContinuation);
    }
    let decoded = hex::decode(value).map_err(|_| S3Error::InvalidContinuation)?;
    decoded.try_into().map_err(|_| S3Error::InvalidContinuation)
}

fn validate_list_options(options: &S3ListOptions) -> Result<(), S3Error> {
    if options.maximum_keys == 0 || options.maximum_entries_examined == 0 {
        return Err(S3Error::InvalidRequest("list bounds must be positive"));
    }
    if !matches!(options.delimiter, None | Some('/')) {
        return Err(S3Error::InvalidRequest("only '/' is a supported delimiter"));
    }
    if options.prefix.starts_with('/') || options.prefix.contains("//") {
        return Err(S3Error::InvalidKey);
    }
    if options
        .start_after
        .as_ref()
        .is_some_and(|value| value.len() > 32 * 1024)
    {
        return Err(S3Error::InvalidRequest("start marker is too large"));
    }
    Ok(())
}

pub(crate) async fn create_parent_directories<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut crate::Transaction<A, O>,
    key: &str,
) -> Result<(), S3Error> {
    validate_key(key)?;
    if let Some((parent, _)) = key.rsplit_once('/')
        && !parent.is_empty()
    {
        transaction.create_dir_all(&format!("/{parent}")).await?;
    }
    Ok(())
}

fn key_path(key: &str, config: crate::model::VolumeConfig) -> Result<NamespacePath, S3Error> {
    validate_key(key)?;
    crate::workspace::customer_path(&absolute_key(key), config).map_err(Into::into)
}

fn validate_key(key: &str) -> Result<(), S3Error> {
    if key.is_empty() || key.starts_with('/') || key.ends_with('/') || key.contains("//") {
        return Err(S3Error::InvalidKey);
    }
    Ok(())
}

fn absolute_key(key: &str) -> String {
    format!("/{key}")
}

fn complete_delete<A, O>(outcome: &TransactionCommit<A, O>) -> Result<(), S3Error> {
    match outcome {
        TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_) => Ok(()),
        TransactionCommit::Conflict { .. } | TransactionCommit::Fenced => {
            Err(S3Error::WriteConflict)
        }
        TransactionCommit::IdempotencyConflict => Err(S3Error::IdempotencyConflict),
    }
}

fn utf8_name(name: &LogicalName) -> Result<&str, S3Error> {
    if name.encoding() != NameEncoding::Utf8 {
        return Err(S3Error::UnsupportedNamespace);
    }
    std::str::from_utf8(name.as_bytes()).map_err(|_| S3Error::UnsupportedNamespace)
}

fn regular_bytes(payload: &FilePayload) -> Result<u64, S3Error> {
    match payload {
        FilePayload::InlineRegular(bytes) => {
            u64::try_from(bytes.as_bytes().len()).map_err(|_| S3Error::InvalidObject)
        }
        FilePayload::Regular { logical_bytes, .. } => Ok(*logical_bytes),
        _ => Err(S3Error::NotRegularFile),
    }
}

fn etag(generation: &[u8; 32], key: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(S3_ETAG_DOMAIN);
    hasher.update(generation);
    hasher.update(key.as_bytes());
    format!("\"{}\"", hasher.finalize().to_hex())
}

/// Typed S3 mapping failures.
#[derive(Debug, Error)]
pub enum S3Error {
    /// Workspace operation failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Key is not one canonical relative file path.
    #[error("invalid S3 object key")]
    InvalidKey,
    /// Request options are unsupported or contradictory.
    #[error("invalid S3 request: {0}")]
    InvalidRequest(&'static str),
    /// Object is absent.
    #[error("S3 object does not exist")]
    NotFound,
    /// Selected path is not a regular file.
    #[error("S3 path is not a regular file")]
    NotRegularFile,
    /// Namespace cannot be represented as S3 UTF-8 keys.
    #[error("workspace namespace is not representable by the S3 view")]
    UnsupportedNamespace,
    /// Authenticated object shape is invalid.
    #[error("authenticated S3 object shape is invalid")]
    InvalidObject,
    /// Listing exhausted its caller-supplied work bound.
    #[error("S3 listing exceeded its authenticated entry bound")]
    ListLimit,
    /// Multipart staging exceeded an explicit count or byte bound.
    #[error("S3 multipart upload exceeded its bound")]
    MultipartLimit,
    /// Multipart completion referenced an absent part.
    #[error("S3 multipart part {0} is absent")]
    MissingPart(u32),
    /// A concurrent writer or authority epoch won before publication.
    #[error("S3 mutation conflicted with a concurrent writer")]
    WriteConflict,
    /// A retry identity was previously bound to different input.
    #[error("S3 idempotency key was reused for different input")]
    IdempotencyConflict,
    /// An exact-generation view was supplied for another workspace.
    #[error("S3 generation belongs to another workspace")]
    ForeignGeneration,
    /// A staged write transaction belongs to another workspace.
    #[error("S3 transaction belongs to another workspace")]
    ForeignTransaction,
    /// Listing continuation is malformed or belongs to another query.
    #[error("invalid S3 listing continuation")]
    InvalidContinuation,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Fs;

    #[test]
    fn listing_cursor_retains_object_prefix_identity() -> Result<(), S3Error> {
        let generation = crate::GenerationId::new(crate::Digest::from_bytes([1; 32]));
        let query = crate::Digest::from_bytes([2; 32]);
        let token = S3ListCursor {
            generation,
            after: "a/".to_owned(),
            after_prefix: false,
            query,
        }
        .encode();
        let cursor = S3ListCursor::decode(&token, 1_024)?;
        let options = S3ListOptions {
            continuation: Some(cursor),
            ..S3ListOptions::default()
        };
        assert!(matches!(
            select_key("a/b", &options),
            Some(SelectedKey::Prefix(prefix)) if prefix == "a/"
        ));
        assert!(matches!(
            select_key("a//b", &options),
            Some(SelectedKey::Prefix(prefix)) if prefix == "a/"
        ));
        assert!(matches!(
            S3ListCursor::decode(
                &format!(
                    "v1.{}.{}.{}",
                    "01".repeat(32),
                    "02".repeat(32),
                    hex::encode("a/")
                ),
                1_024
            ),
            Err(S3Error::InvalidContinuation)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn s3_view_is_the_same_generation_and_copy_is_body_free()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let workspace = fs.create_workspace("s3").await?;
        let s3 = workspace.s3();
        let other = fs.create_workspace("another-s3-workspace").await?;
        let mut foreign = other.begin_transaction(IdempotencyKey::new()).await?;
        assert!(matches!(
            s3.write_staged(&mut foreign, "foreign", &[]).await,
            Err(S3Error::ForeignTransaction)
        ));
        s3.put_object(
            "src/a.bin",
            Bytes::from_static(b"abcdef"),
            IdempotencyKey::new(),
        )
        .await?;
        s3.put_object(
            "root.bin",
            Bytes::from_static(b"root"),
            IdempotencyKey::new(),
        )
        .await?;
        assert_eq!(
            s3.get_object_range(
                "src/a.bin",
                ByteRange {
                    offset: 2,
                    length: 3
                }
            )
            .await?,
            b"cde".as_slice()
        );
        s3.copy_object("src/a.bin", "copy/a.bin", IdempotencyKey::new())
            .await?;
        assert_eq!(
            workspace.read("/copy/a.bin", 16).await?,
            b"abcdef".as_slice()
        );

        let listing = Box::pin(s3.list_objects(S3ListOptions::default())).await?;
        assert_eq!(listing.common_prefixes, vec!["copy/", "src/"]);
        assert_eq!(
            listing
                .objects
                .iter()
                .map(|object| object.key.as_str())
                .collect::<Vec<_>>(),
            vec!["root.bin"]
        );
        let flat = Box::pin(s3.list_objects(S3ListOptions {
            delimiter: None,
            ..S3ListOptions::default()
        }))
        .await?;
        assert_eq!(
            flat.objects
                .iter()
                .map(|object| object.key.as_str())
                .collect::<Vec<_>>(),
            vec!["copy/a.bin", "root.bin", "src/a.bin"]
        );
        s3.delete_objects(
            &["copy/a.bin".to_owned(), "root.bin".to_owned()],
            IdempotencyKey::new(),
        )
        .await?;
        assert!(matches!(
            s3.head_object("root.bin").await,
            Err(S3Error::NotFound)
        ));
        s3.delete_objects(
            &["copy/a.bin".to_owned(), "root.bin".to_owned()],
            IdempotencyKey::new(),
        )
        .await?;
        s3.delete_object("never-existed", IdempotencyKey::new())
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn listing_is_bounded_and_pagination_is_stable() -> Result<(), Box<dyn std::error::Error>>
    {
        let fs = Fs::memory();
        let workspace = fs.create_workspace("pages").await?;
        let s3 = workspace.s3();
        for key in ["a", "b", "c"] {
            s3.put_object(key, Bytes::from_static(b"x"), IdempotencyKey::new())
                .await?;
        }
        let first = Box::pin(s3.list_objects(S3ListOptions {
            maximum_keys: 2,
            ..S3ListOptions::default()
        }))
        .await?;
        assert_eq!(
            first
                .next_continuation
                .as_ref()
                .map(|cursor| cursor.after.as_str()),
            Some("b")
        );
        let token = first
            .next_continuation
            .as_ref()
            .ok_or("first listing page has no continuation")?
            .encode();
        let continuation = S3ListCursor::decode(&token, 32 * 1024)?;
        s3.put_object("aa", Bytes::from_static(b"new"), IdempotencyKey::new())
            .await?;
        let second = Box::pin(s3.list_objects(S3ListOptions {
            continuation: Some(continuation),
            ..S3ListOptions::default()
        }))
        .await?;
        assert_eq!(
            second
                .objects
                .iter()
                .map(|object| object.key.as_str())
                .collect::<Vec<_>>(),
            vec!["c"]
        );
        assert!(matches!(
            S3ListCursor::decode("v1.invalid", 32 * 1024),
            Err(S3Error::InvalidContinuation)
        ));
        assert!(matches!(
            Box::pin(s3.list_objects(S3ListOptions {
                prefix: "different".to_owned(),
                continuation: first.next_continuation,
                ..S3ListOptions::default()
            }))
            .await,
            Err(S3Error::InvalidContinuation)
        ));
        assert!(matches!(
            Box::pin(s3.list_objects(S3ListOptions {
                maximum_entries_examined: 1,
                ..S3ListOptions::default()
            }))
            .await,
            Err(S3Error::ListLimit)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn delimiter_pages_preserve_order_and_exact_resume()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let workspace = fs.create_workspace("delimiter-pages").await?;
        let s3 = workspace.s3();
        for key in ["a/one", "a/two", "b/one", "c"] {
            s3.put_object(key, Bytes::from_static(b"x"), IdempotencyKey::new())
                .await?;
        }
        let mut options = S3ListOptions {
            delimiter: Some('/'),
            maximum_keys: 1,
            ..S3ListOptions::default()
        };
        let mut entries = Vec::new();
        let mut complete = false;
        for _ in 0..4 {
            let page = s3.list_objects(options.clone()).await?;
            entries.extend(page.objects.into_iter().map(|object| (object.key, false)));
            entries.extend(
                page.common_prefixes
                    .into_iter()
                    .map(|prefix| (prefix, true)),
            );
            let Some(cursor) = page.next_continuation else {
                complete = true;
                break;
            };
            options.continuation = Some(S3ListCursor::decode(&cursor.encode(), 1_024)?);
        }
        assert!(complete, "listing did not terminate within four pages");
        assert_eq!(
            entries,
            [
                ("a/".to_owned(), true),
                ("b/".to_owned(), true),
                ("c".to_owned(), false)
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn listing_pages_use_flat_key_order_without_scanning_every_entry()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let workspace = fs.create_workspace("lazy-listing").await?;
        let s3 = workspace.s3();
        for key in ["a/x", "a-", "a.", "a0", "b", "z"] {
            s3.put_object(key, Bytes::from_static(b"x"), IdempotencyKey::new())
                .await?;
        }
        for index in 0..32 {
            s3.put_object(
                &format!("zz{index:02}"),
                Bytes::from_static(b"x"),
                IdempotencyKey::new(),
            )
            .await?;
        }
        for (delimiter, expected) in [
            (None, ["a-", "a.", "a/x", "a0", "b", "z"]),
            (Some('/'), ["a-", "a.", "a/", "a0", "b", "z"]),
        ] {
            let mut options = S3ListOptions {
                delimiter,
                maximum_keys: 1,
                ..S3ListOptions::default()
            };
            let mut observed = Vec::new();
            for page_index in 0..6 {
                let page = s3.list_objects(options.clone()).await?;
                if page_index == 0 {
                    assert!(page.entries_examined < 38);
                }
                observed.extend(page.objects.into_iter().map(|object| object.key));
                observed.extend(page.common_prefixes);
                options.continuation = page.next_continuation;
            }
            assert_eq!(observed, expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn listing_matches_an_independent_key_model() -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let workspace = fs.create_workspace("listing-model").await?;
        let s3 = workspace.s3();
        let keys = [
            "a/x",
            "a.",
            "a-",
            "a0",
            "b",
            "z",
            "zz/one",
            "zz/two",
            "ünicode/α",
        ];
        for key in keys {
            s3.put_object(key, Bytes::from_static(b"x"), IdempotencyKey::new())
                .await?;
        }
        for prefix in ["", "a", "a/", "zz", "ü"] {
            for delimiter in [None, Some('/')] {
                let expected = keys
                    .iter()
                    .filter_map(|key| {
                        let suffix = key.strip_prefix(prefix)?;
                        Some(match (delimiter, suffix.split_once('/')) {
                            (Some('/'), Some((head, _))) => (format!("{prefix}{head}/"), true),
                            _ => ((*key).to_owned(), false),
                        })
                    })
                    .collect::<BTreeSet<_>>();
                for maximum_keys in [1, 2, 4] {
                    let mut options = S3ListOptions {
                        prefix: prefix.to_owned(),
                        delimiter,
                        maximum_keys,
                        ..S3ListOptions::default()
                    };
                    let mut actual = Vec::new();
                    for _ in 0..=expected.len() {
                        let page = s3.list_objects(options.clone()).await?;
                        let mut entries = page
                            .objects
                            .into_iter()
                            .map(|object| (object.key, false))
                            .chain(page.common_prefixes.into_iter().map(|name| (name, true)))
                            .collect::<Vec<_>>();
                        entries.sort();
                        actual.extend(entries);
                        let Some(cursor) = page.next_continuation else {
                            break;
                        };
                        options.continuation = Some(S3ListCursor::decode(&cursor.encode(), 1_024)?);
                    }
                    assert_eq!(actual, expected.iter().cloned().collect::<Vec<_>>());
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn listing_uses_fetched_witness_before_requesting_another_page()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let workspace = fs.create_workspace("listing-witness").await?;
        let s3 = workspace.s3();
        for key in ["a", "b", "c", "d", "p/x"] {
            s3.put_object(key, Bytes::from_static(b"x"), IdempotencyKey::new())
                .await?;
        }
        let first = s3
            .list_objects(S3ListOptions {
                delimiter: None,
                maximum_keys: 1,
                maximum_entries_examined: 2,
                ..S3ListOptions::default()
            })
            .await?;
        assert_eq!(
            first
                .objects
                .iter()
                .map(|item| item.key.as_str())
                .collect::<Vec<_>>(),
            ["a"]
        );
        assert_eq!(first.entries_examined, 2);
        assert!(first.next_continuation.is_some());
        let two = s3
            .list_objects(S3ListOptions {
                delimiter: Some('/'),
                maximum_keys: 2,
                maximum_entries_examined: 3,
                ..S3ListOptions::default()
            })
            .await?;
        assert_eq!(
            two.objects
                .iter()
                .map(|item| item.key.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(two.entries_examined, 3);
        assert!(two.next_continuation.is_some());

        let workspace = fs.create_workspace("listing-prefix-witness").await?;
        let s3 = workspace.s3();
        for key in ["a", "b/x", "c", "d"] {
            s3.put_object(key, Bytes::from_static(b"x"), IdempotencyKey::new())
                .await?;
        }
        let with_prefix = s3
            .list_objects(S3ListOptions {
                maximum_keys: 2,
                maximum_entries_examined: 4,
                ..S3ListOptions::default()
            })
            .await?;
        assert_eq!(
            with_prefix
                .objects
                .iter()
                .map(|item| item.key.as_str())
                .collect::<Vec<_>>(),
            ["a"]
        );
        assert_eq!(with_prefix.common_prefixes, ["b/"]);
        assert_eq!(with_prefix.entries_examined, 4);
        assert!(with_prefix.next_continuation.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn flat_continuations_seek_without_replaying_earlier_pages()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let workspace = fs.create_workspace("flat-seek").await?;
        let s3 = workspace.s3();
        for index in 0..32 {
            s3.put_object(
                &format!("key{index:03}"),
                Bytes::from_static(b"x"),
                IdempotencyKey::new(),
            )
            .await?;
        }
        let mut options = S3ListOptions {
            delimiter: None,
            maximum_keys: 1,
            maximum_entries_examined: 3,
            ..S3ListOptions::default()
        };
        for index in 0..32 {
            let page = s3.list_objects(options.clone()).await?;
            assert!(page.entries_examined <= 3);
            assert_eq!(page.objects.len(), 1);
            assert_eq!(page.objects[0].key, format!("key{index:03}"));
            options.continuation = page.next_continuation;
        }
        assert!(options.continuation.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn object_cursor_seeks_past_dense_predecessor_keys()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let workspace = fs.create_workspace("dense-list-cursor").await?;
        let s3 = workspace.s3();
        for index in 0..32 {
            s3.put_object(
                &format!("a9{index:03}"),
                Bytes::from_static(b"x"),
                IdempotencyKey::new(),
            )
            .await?;
        }
        for key in ["a:", "a;"] {
            s3.put_object(key, Bytes::from_static(b"x"), IdempotencyKey::new())
                .await?;
        }
        let first = s3
            .list_objects(S3ListOptions {
                delimiter: None,
                maximum_keys: 33,
                maximum_entries_examined: 34,
                ..S3ListOptions::default()
            })
            .await?;
        assert_eq!(
            first.objects.last().map(|object| object.key.as_str()),
            Some("a:")
        );
        let page = s3
            .list_objects(S3ListOptions {
                delimiter: None,
                maximum_keys: 1,
                maximum_entries_examined: 2,
                continuation: first.next_continuation,
                ..S3ListOptions::default()
            })
            .await?;
        assert_eq!(page.objects[0].key, "a;");
        assert!(page.entries_examined <= 2);
        Ok(())
    }

    #[tokio::test]
    async fn object_cursor_at_a_directory_includes_its_descendants()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let workspace = fs.create_workspace("directory-marker").await?;
        let s3 = workspace.s3();
        s3.put_object("a/x", Bytes::from_static(b"x"), IdempotencyKey::new())
            .await?;
        for delimiter in [None, Some('/')] {
            let mut options = S3ListOptions {
                delimiter,
                maximum_keys: 1,
                maximum_entries_examined: 2,
                ..S3ListOptions::default()
            };
            options.continuation = Some(S3ListCursor {
                generation: workspace.head().await?.id(),
                after: "a".to_owned(),
                after_prefix: false,
                query: list_query_digest(&options),
            });
            let page = s3.list_objects(options).await?;
            if delimiter.is_some() {
                assert_eq!(page.common_prefixes, ["a/"]);
            } else {
                assert_eq!(page.objects[0].key, "a/x");
            }
            assert!(page.entries_examined <= 2);
        }
        Ok(())
    }

    #[tokio::test]
    async fn listing_descends_to_the_exact_prefix_frontier_without_unrelated_subtrees()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let workspace = fs.create_workspace("prefix-frontier").await?;
        let s3 = workspace.s3();
        for index in 0..32 {
            s3.put_object(
                &format!("unrelated-{index:02}/value"),
                Bytes::from_static(b"x"),
                IdempotencyKey::new(),
            )
            .await?;
        }
        s3.put_object(
            "target/value",
            Bytes::from_static(b"wanted"),
            IdempotencyKey::new(),
        )
        .await?;

        let selected = Box::pin(s3.list_objects(S3ListOptions {
            prefix: "target/".to_owned(),
            delimiter: None,
            maximum_entries_examined: 1,
            ..S3ListOptions::default()
        }))
        .await?;
        assert_eq!(selected.entries_examined, 1);
        assert_eq!(selected.objects.len(), 1);
        assert_eq!(selected.objects[0].key, "target/value");

        let absent = Box::pin(s3.list_objects(S3ListOptions {
            prefix: "absent/".to_owned(),
            maximum_entries_examined: 1,
            ..S3ListOptions::default()
        }))
        .await?;
        assert_eq!(absent.entries_examined, 0);
        assert!(absent.objects.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn multipart_stages_off_authority_and_publishes_one_ordered_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let workspace = fs.create_workspace("multipart").await?;
        let before = workspace.head().await?.id();
        let mut upload = workspace
            .s3()
            .create_multipart_upload(
                "large/value.bin",
                S3MultipartOptions::default(),
                IdempotencyKey::new(),
            )
            .await?;
        upload.upload_part(2, Bytes::from_static(b"second")).await?;
        upload.upload_part(1, Bytes::from_static(b"first-")).await?;
        assert_eq!(workspace.head().await?.id(), before);
        assert!(matches!(
            upload.complete(&[1, 2]).await?,
            TransactionCommit::Committed(_)
        ));
        assert!(matches!(
            upload.complete(&[1, 2]).await?,
            TransactionCommit::AlreadyCommitted(_)
        ));
        assert_eq!(
            workspace.read("/large/value.bin", 64).await?,
            b"first-second".as_slice()
        );

        let mut invalid = workspace
            .s3()
            .create_multipart_upload(
                "invalid.bin",
                S3MultipartOptions::default(),
                IdempotencyKey::new(),
            )
            .await?;
        invalid.upload_part(1, Bytes::from_static(b"part")).await?;
        assert!(matches!(
            invalid.complete(&[2]).await,
            Err(S3Error::MissingPart(2))
        ));
        assert!(matches!(
            workspace.s3().head_object("invalid.bin").await,
            Err(S3Error::NotFound)
        ));
        Ok(())
    }
}

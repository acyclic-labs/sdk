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
    /// Return keys strictly after this prior key or common prefix.
    pub after: Option<String>,
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
            after: None,
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
    /// Last returned key/prefix when more matching results exist.
    pub next_after: Option<String>,
    /// Exact authenticated namespace entries examined.
    pub entries_examined: u32,
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
        let mut checkout = self
            .workspace
            .engine_checkout(
                GenerationSelector::Exact(generation.id()),
                CheckoutMode::read_only_pinned(),
            )
            .await?;
        let path = key_path(key, checkout.volume_config().limits)?;
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
        let mut checkout = self
            .workspace
            .engine_checkout(
                GenerationSelector::Exact(generation.id()),
                CheckoutMode::read_only_pinned(),
            )
            .await?;
        let path = key_path(key, checkout.volume_config().limits)?;
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

    /// Atomically removes one object.
    ///
    /// # Errors
    ///
    /// Returns key, absence, transaction, or publication failures.
    pub async fn delete_object(
        &self,
        key: &str,
        idempotency_key: IdempotencyKey,
    ) -> Result<TransactionCommit<A, O>, S3Error> {
        validate_key(key)?;
        let mut transaction = self.workspace.begin_transaction(idempotency_key).await?;
        transaction.remove(&absolute_key(key)).await?;
        transaction.commit().await.map_err(Into::into)
    }

    /// Atomically removes several objects through one workspace transaction.
    ///
    /// # Errors
    ///
    /// Returns request, key, absence, transaction, or publication failures.
    pub async fn delete_objects(
        &self,
        keys: &[String],
        idempotency_key: IdempotencyKey,
    ) -> Result<TransactionCommit<A, O>, S3Error> {
        if keys.is_empty() {
            return Err(S3Error::InvalidRequest("delete set is empty"));
        }
        let mut transaction = self.workspace.begin_transaction(idempotency_key).await?;
        for key in keys {
            validate_key(key)?;
            transaction.remove(&absolute_key(key)).await?;
        }
        transaction.commit().await.map_err(Into::into)
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
        validate_list_options(&options)?;
        let generation = self.workspace.head().await?;
        let mut checkout = self
            .workspace
            .engine_checkout(
                GenerationSelector::Exact(generation.id()),
                CheckoutMode::read_only_pinned(),
            )
            .await?;
        let limits = checkout.volume_config().limits;
        let (frontier, frontier_key, frontier_name_prefix) =
            listing_frontier(&options.prefix, limits)?;
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
        let mut pending = vec![(
            frontier,
            frontier_key,
            (!frontier_name_prefix.is_empty()).then_some(frontier_name_prefix),
        )];
        let mut objects = BTreeMap::new();
        let mut prefixes = BTreeSet::new();
        let mut examined = 0_u32;
        while let Some((directory, directory_key, name_prefix)) = pending.pop() {
            let mut after: Option<LogicalName> = None;
            loop {
                let page = checkout
                    .list_directory_records(
                        &directory,
                        after.as_ref(),
                        LIST_PAGE_ENTRIES,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| S3Error::Workspace(WorkspaceError::engine(failure)))?
                    .value;
                for entry in &page.entries {
                    examined = examined.checked_add(1).ok_or(S3Error::ListLimit)?;
                    if examined > options.maximum_entries_examined {
                        return Err(S3Error::ListLimit);
                    }
                    let name = utf8_name(&entry.name)?;
                    if name_prefix
                        .as_ref()
                        .is_some_and(|prefix| !name.starts_with(prefix))
                    {
                        continue;
                    }
                    let key = format!("{directory_key}{name}");
                    match entry.record.kind {
                        FileKind::Directory => {
                            let mut components = directory.components().to_vec();
                            components.push(entry.name.clone());
                            pending.push((
                                NamespacePath::new(components, limits)
                                    .map_err(WorkspaceError::path)?,
                                format!("{key}/"),
                                None,
                            ));
                        }
                        FileKind::Regular => match select_key(&key, &options) {
                            Some(SelectedKey::Object) => {
                                objects.insert(
                                    key.clone(),
                                    S3Object {
                                        key: key.clone(),
                                        content_length: regular_bytes(&entry.record.payload)?,
                                        etag: etag(generation.id().digest().as_bytes(), &key),
                                    },
                                );
                            }
                            Some(SelectedKey::Prefix(prefix)) => {
                                prefixes.insert(prefix);
                            }
                            None => {}
                        },
                        _ => {}
                    }
                }
                after = page.entries.last().map(|entry| entry.name.clone());
                if !page.has_more {
                    break;
                }
            }
        }
        finish_list(objects, prefixes, options.maximum_keys, examined)
    }
}

fn empty_list() -> S3List {
    S3List {
        objects: Vec::new(),
        common_prefixes: Vec::new(),
        next_after: None,
        entries_examined: 0,
    }
}

fn finish_list(
    mut objects: BTreeMap<String, S3Object>,
    mut prefixes: BTreeSet<String>,
    maximum_keys: u32,
    entries_examined: u32,
) -> Result<S3List, S3Error> {
    let mut combined = objects
        .keys()
        .map(|key| (key.clone(), false))
        .chain(prefixes.iter().map(|prefix| (prefix.clone(), true)))
        .collect::<Vec<_>>();
    combined.sort_unstable();
    let maximum = usize::try_from(maximum_keys).map_err(|_| S3Error::ListLimit)?;
    let has_more = combined.len() > maximum;
    combined.truncate(maximum);
    let selected = combined.iter().map(|(key, _)| key).collect::<BTreeSet<_>>();
    objects.retain(|key, _| selected.contains(key));
    prefixes.retain(|key| selected.contains(key));
    Ok(S3List {
        objects: objects.into_values().collect(),
        common_prefixes: prefixes.into_iter().collect(),
        next_after: has_more
            .then(|| combined.last().map(|(key, _)| key.clone()))
            .flatten(),
        entries_examined,
    })
}

fn listing_frontier(
    prefix: &str,
    limits: crate::model::VolumeLimits,
) -> Result<(NamespacePath, String, String), S3Error> {
    let Some(separator) = prefix.rfind('/') else {
        return Ok((
            NamespacePath::new(Vec::new(), limits).map_err(WorkspaceError::path)?,
            String::new(),
            prefix.to_owned(),
        ));
    };
    let directory = &prefix[..separator];
    let path = if directory.is_empty() {
        NamespacePath::new(Vec::new(), limits).map_err(WorkspaceError::path)?
    } else {
        key_path(directory, limits)?
    };
    Ok((
        path,
        prefix[..=separator].to_owned(),
        prefix[separator + 1..].to_owned(),
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
    if !key.starts_with(&options.prefix)
        || options
            .after
            .as_ref()
            .is_some_and(|after| key <= after.as_str())
    {
        return None;
    }
    let remainder = &key[options.prefix.len()..];
    if options.delimiter == Some('/')
        && let Some(index) = remainder.find('/')
    {
        return Some(SelectedKey::Prefix(format!(
            "{}{}",
            options.prefix,
            &remainder[..=index]
        )));
    }
    Some(SelectedKey::Object)
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
    Ok(())
}

async fn create_parent_directories<A: AsyncAuthorityStore, O: AsyncObjectStore>(
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

fn key_path(key: &str, limits: crate::model::VolumeLimits) -> Result<NamespacePath, S3Error> {
    validate_key(key)?;
    crate::workspace::customer_path(&absolute_key(key), limits).map_err(Into::into)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Fs;

    #[tokio::test]
    async fn s3_view_is_the_same_generation_and_copy_is_body_free()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let workspace = fs.create_workspace("s3").await?;
        let s3 = workspace.s3();
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
        assert_eq!(first.next_after.as_deref(), Some("b"));
        let second = Box::pin(s3.list_objects(S3ListOptions {
            after: first.next_after,
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

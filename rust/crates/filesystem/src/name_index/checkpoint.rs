//! Restart records are a stream of JSON lines in bounded volume files. A
//! record may span files; neither the manifest nor a write buffer grows with
//! the number of directories. Unchanged chunks retain their existing binding.
use super::*;

const CHUNK_BYTES: usize = 64 * 1024;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredManifest {
    pub(super) schema: u32,
    cut: NameCut,
    names_cut: NameCut,
    observed_generation: GenerationId,
    pages: u64,
    pending_directories: usize,
    complete: bool,
    checkpoint_chunks: u64,
    frontier_blocks: usize,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyFile {
    chunks: u64,
}

impl StoredManifest {
    pub(super) async fn restore<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        self,
        generation: &Generation<A, O>,
    ) -> Result<NameIndexManifest, SearchError> {
        let stop = CancellationToken::new();
        let mut checkpoint: Option<NameCheckpoint> = None;
        let mut record = Vec::new();
        for ordinal in 0..self.checkpoint_chunks {
            let bytes = generation
                .read(
                    &chunk_path(ordinal),
                    CHUNK_BYTES as u64,
                    WorkBudget::UNBOUNDED,
                    &stop,
                )
                .await
                .map_err(|failure| failure.error)?
                .value;
            for part in bytes.split_inclusive(|byte| *byte == b'\n') {
                record.extend_from_slice(part);
                if record.last() == Some(&b'\n') {
                    if let Some(checkpoint) = &mut checkpoint {
                        checkpoint.restore_record(&record)?;
                    } else {
                        checkpoint = Some(NameCheckpoint::from_header(&record)?);
                    }
                    record.clear();
                }
            }
        }
        if !record.is_empty() {
            return Err(SearchError::Invalid("truncated pathname checkpoint".into()));
        }
        let mut checkpoint =
            checkpoint.ok_or_else(|| SearchError::Invalid("empty pathname checkpoint".into()))?;
        for ordinal in 0..self.frontier_blocks {
            let path = frontier_path(ordinal);
            let header = read_policy_header(generation, &path).await?;
            for chunk in 0..header.chunks {
                let bytes = generation
                    .read(
                        &policy_chunk(&path, chunk),
                        CHUNK_BYTES as u64,
                        WorkBudget::UNBOUNDED,
                        &stop,
                    )
                    .await
                    .map_err(|failure| failure.error)?
                    .value;
                for part in bytes.split_inclusive(|byte| *byte == b'\n') {
                    record.extend_from_slice(part);
                    if record.len() as u64 > MAXIMUM_INDEX_FILE_BYTES {
                        return Err(SearchError::Invalid(
                            "oversized directory continuation".into(),
                        ));
                    }
                    if record.last() == Some(&b'\n') {
                        checkpoint.restore_frontier_record(&record)?;
                        record.clear();
                    }
                }
            }
            if !record.is_empty() {
                return Err(SearchError::Invalid("truncated frontier block".into()));
            }
        }
        let root = policy_root();
        let mut after = None;
        loop {
            let page = generation
                .list_directory(&root, after.as_ref(), 128, WorkBudget::UNBOUNDED, &stop)
                .await
                .map_err(|failure| failure.error)?
                .value
                .0;
            for entry in &page.entries {
                let name = entry
                    .name
                    .unicode_text(WorkBudget::UNBOUNDED, &stop)
                    .map_err(|failure| failure.error)?
                    .value
                    .to_string();
                let Some(hash) = name.strip_suffix(".json") else {
                    continue;
                };
                if hash.len() != 64 {
                    continue;
                }
                if !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    return Err(SearchError::Invalid(
                        "invalid checkpoint policy filename".into(),
                    ));
                }
                let path = format!("{root}/{hash}");
                let header = read_policy_header(generation, &path).await?;
                let mut record = Vec::new();
                for ordinal in 0..header.chunks {
                    let bytes = generation
                        .read(
                            &policy_chunk(&path, ordinal),
                            CHUNK_BYTES as u64,
                            WorkBudget::UNBOUNDED,
                            &stop,
                        )
                        .await
                        .map_err(|failure| failure.error)?
                        .value;
                    record.extend_from_slice(&bytes);
                }
                checkpoint.restore_policy_record(&record, hash)?;
            }
            if !page.has_more {
                break;
            }
            after = page.entries.last().map(|entry| entry.name.clone());
        }
        if checkpoint.pending_directories() != self.pending_directories {
            return Err(SearchError::Invalid(
                "pathname checkpoint progress mismatch".into(),
            ));
        }
        let root = directory_root();
        let mut after = None;
        loop {
            let page = generation
                .list_directory(&root, after.as_ref(), 128, WorkBudget::UNBOUNDED, &stop)
                .await
                .map_err(|failure| failure.error)?
                .value
                .0;
            for entry in &page.entries {
                let name = entry
                    .name
                    .unicode_text(WorkBudget::UNBOUNDED, &stop)
                    .map_err(|failure| failure.error)?
                    .value
                    .to_string();
                let hash = name
                    .strip_suffix(".json")
                    .filter(|hash| {
                        hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
                    })
                    .ok_or_else(|| {
                        SearchError::Invalid("invalid directory token filename".into())
                    })?;
                let bytes = generation
                    .read(
                        &format!("{root}/{name}"),
                        MAXIMUM_INDEX_FILE_BYTES,
                        WorkBudget::UNBOUNDED,
                        &stop,
                    )
                    .await
                    .map_err(|failure| failure.error)?
                    .value;
                checkpoint.restore_directory_record(&bytes, hash)?;
            }
            if !page.has_more {
                break;
            }
            after = page.entries.last().map(|entry| entry.name.clone());
        }
        Ok(NameIndexManifest {
            schema: self.schema,
            cut: self.cut,
            names_cut: self.names_cut,
            observed_generation: self.observed_generation,
            pages: self.pages,
            pending_directories: self.pending_directories,
            complete: self.complete,
            checkpoint,
        })
    }
}

pub(super) async fn write_manifest<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut crate::Transaction<A, O>,
    expected: &Generation<A, O>,
    manifest: &NameIndexManifest,
    previous: Option<&NameIndex<A, O>>,
) -> Result<(usize, usize, usize), SearchError> {
    let mut buffer = Vec::with_capacity(CHUNK_BYTES);
    let mut chunks = 0;
    for record in manifest.checkpoint.records() {
        let mut record = record?;
        record.push(b'\n');
        let mut remaining = record.as_slice();
        while !remaining.is_empty() {
            let count = remaining.len().min(CHUNK_BYTES - buffer.len());
            buffer.extend_from_slice(&remaining[..count]);
            remaining = &remaining[count..];
            if buffer.len() == CHUNK_BYTES {
                write_chunk(transaction, expected, chunks, &buffer).await?;
                chunks += 1;
                buffer.clear();
            }
        }
    }
    if !buffer.is_empty() {
        write_chunk(transaction, expected, chunks, &buffer).await?;
        chunks += 1;
    }
    // The prior snapshot must still name this policy namespace at the captured
    // head. A competing publication cannot make a stale diff authoritative.
    let previous = if let Some(previous) = previous {
        let bytes = expected
            .read(
                MANIFEST,
                MAXIMUM_INDEX_FILE_BYTES,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let prior = previous
            .generation
            .read(
                MANIFEST,
                MAXIMUM_INDEX_FILE_BYTES,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        (bytes == prior).then_some(previous)
    } else {
        None
    };
    let frontier_root = format!("{ROOT}/frontier");
    if previous.is_none() {
        clear_directory(transaction, expected, &frontier_root).await?;
    }
    transaction
        .write_batch(
            &[&frontier_root],
            &[],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    let mut frontier_records = 0;
    for (ordinal, records) in manifest
        .checkpoint
        .frontier_records_since(previous.map(|previous| &previous.manifest.checkpoint))
    {
        let path = frontier_path(ordinal);
        let old_chunks = if previous.is_some() {
            match read_policy_header(expected, &path).await {
                Ok(header) => header.chunks,
                Err(SearchError::Workspace(WorkspaceError::NotFound)) => 0,
                Err(error) => return Err(error),
            }
        } else {
            0
        };
        let mut chunks = 0;
        if let Some(records) = records {
            buffer.clear();
            for record in records {
                frontier_records += 1;
                let mut record = record?;
                record.push(b'\n');
                if record.len() as u64 > MAXIMUM_INDEX_FILE_BYTES {
                    return Err(SearchError::Invalid(
                        "oversized directory continuation".into(),
                    ));
                }
                let mut remaining = record.as_slice();
                while !remaining.is_empty() {
                    let count = remaining.len().min(CHUNK_BYTES - buffer.len());
                    buffer.extend_from_slice(&remaining[..count]);
                    remaining = &remaining[count..];
                    if buffer.len() == CHUNK_BYTES {
                        write_piece(transaction, expected, &policy_chunk(&path, chunks), &buffer)
                            .await?;
                        chunks += 1;
                        buffer.clear();
                    }
                }
            }
            if !buffer.is_empty() {
                write_piece(transaction, expected, &policy_chunk(&path, chunks), &buffer).await?;
                chunks += 1;
            }
            transaction
                .write(
                    &format!("{path}.json"),
                    serde_json::to_vec(&PolicyFile { chunks })?,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)?;
        } else {
            transaction
                .remove(
                    &format!("{path}.json"),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)?;
        }
        for chunk in chunks..old_chunks {
            transaction
                .remove(
                    &policy_chunk(&path, chunk),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)?;
        }
    }
    let root = policy_root();
    if previous.is_none() {
        clear_directory(transaction, expected, &root).await?;
    }
    transaction
        .write_batch(
            &[root.as_str()],
            &[],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    let mut policy_records = 0;
    for record in manifest
        .checkpoint
        .policy_records_since(previous.map(|previous| &previous.manifest.checkpoint))
    {
        let (directory, record) = record?;
        policy_records += 1;
        let path = format!("{root}/{}", blake3::hash(directory.as_bytes()).to_hex());
        let old_chunks = if previous.is_some() {
            match read_policy_header(expected, &path).await {
                Ok(header) => header.chunks,
                Err(SearchError::Workspace(WorkspaceError::NotFound)) => 0,
                Err(error) => return Err(error),
            }
        } else {
            0
        };
        let chunks = if let Some(record) = record {
            let chunks = record.len().div_ceil(CHUNK_BYTES) as u64;
            for (ordinal, bytes) in record.chunks(CHUNK_BYTES).enumerate() {
                transaction
                    .write(
                        &policy_chunk(&path, ordinal as u64),
                        bytes.to_vec(),
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| failure.error)?;
            }
            transaction
                .write(
                    &format!("{path}.json"),
                    serde_json::to_vec(&PolicyFile { chunks })?,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)?;
            chunks
        } else {
            transaction
                .remove(
                    &format!("{path}.json"),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)?;
            0
        };
        for ordinal in chunks..old_chunks {
            transaction
                .remove(
                    &policy_chunk(&path, ordinal),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)?;
        }
    }
    let directory_root = directory_root();
    if previous.is_none() {
        clear_directory(transaction, expected, &directory_root).await?;
    }
    transaction
        .write_batch(
            &[&directory_root],
            &[],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    let mut metadata_records = 0;
    for record in manifest
        .checkpoint
        .directory_records_since(previous.map(|previous| &previous.manifest.checkpoint))
    {
        let (directory, record) = record?;
        metadata_records += 1;
        let path = format!(
            "{directory_root}/{}.json",
            blake3::hash(directory.as_bytes()).to_hex()
        );
        if let Some(bytes) = record {
            if bytes.len() as u64 > MAXIMUM_INDEX_FILE_BYTES {
                return Err(SearchError::Invalid(
                    "directory token exceeds its file budget".into(),
                ));
            }
            transaction
                .write(
                    &path,
                    bytes,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)?;
        } else {
            transaction
                .remove(&path, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(|failure| failure.error)?;
        }
    }
    let stored = StoredManifest {
        schema: crate::NameIndexManifest::SCHEMA,
        cut: manifest.cut.clone(),
        names_cut: manifest.names_cut.clone(),
        observed_generation: manifest.observed_generation,
        pages: manifest.pages,
        pending_directories: manifest.pending_directories,
        complete: manifest.complete,
        checkpoint_chunks: chunks,
        frontier_blocks: manifest.checkpoint.frontier_blocks(),
    };
    transaction
        .write(
            MANIFEST,
            serde_json::to_vec(&stored)?,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    Ok((policy_records, metadata_records, frontier_records))
}

fn frontier_path(ordinal: usize) -> String {
    format!("{ROOT}/frontier/{ordinal:016x}")
}

fn policy_root() -> String {
    format!("{ROOT}/policies")
}

fn directory_root() -> String {
    format!("{ROOT}/directories")
}

async fn clear_directory<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut crate::Transaction<A, O>,
    expected: &Generation<A, O>,
    root: &str,
) -> Result<(), SearchError> {
    let stop = CancellationToken::new();
    let mut after = None;
    loop {
        let page = match expected
            .list_directory(root, after.as_ref(), 128, WorkBudget::UNBOUNDED, &stop)
            .await
        {
            Ok(receipt) => receipt.value.0,
            Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => return Ok(()),
            Err(failure) => return Err(failure.error.into()),
        };
        for entry in &page.entries {
            let name = entry
                .name
                .unicode_text(WorkBudget::UNBOUNDED, &stop)
                .map_err(|failure| failure.error)?
                .value;
            transaction
                .remove(&format!("{root}/{name}"), WorkBudget::UNBOUNDED, &stop)
                .await
                .map_err(|failure| failure.error)?;
        }
        if !page.has_more {
            return Ok(());
        }
        after = page.entries.last().map(|entry| entry.name.clone());
    }
}

fn policy_chunk(path: &str, ordinal: u64) -> String {
    format!("{path}-{ordinal:016x}.jsonl")
}

async fn read_policy_header<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &Generation<A, O>,
    path: &str,
) -> Result<PolicyFile, SearchError> {
    let bytes = generation
        .read(
            &format!("{path}.json"),
            128,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn write_chunk<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut crate::Transaction<A, O>,
    expected: &Generation<A, O>,
    ordinal: u64,
    bytes: &[u8],
) -> Result<(), SearchError> {
    write_piece(transaction, expected, &chunk_path(ordinal), bytes).await
}

async fn write_piece<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut crate::Transaction<A, O>,
    expected: &Generation<A, O>,
    path: &str,
    bytes: &[u8],
) -> Result<(), SearchError> {
    let stop = CancellationToken::new();
    match expected
        .read(path, CHUNK_BYTES as u64, WorkBudget::UNBOUNDED, &stop)
        .await
    {
        Ok(receipt) if receipt.value.as_ref() == bytes => return Ok(()),
        Ok(_) => {}
        Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => {}
        Err(failure) => return Err(failure.error.into()),
    }
    transaction
        .write(path, bytes.to_vec(), WorkBudget::UNBOUNDED, &stop)
        .await
        .map_err(|failure| failure.error)?;
    Ok(())
}

fn chunk_path(ordinal: u64) -> String {
    format!("{ROOT}/checkpoint-{ordinal:016x}.jsonl")
}

#[cfg(all(test, feature = "memory", feature = "native-mount"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn name_search_checkpoint_persists_only_changed_policies_and_metadata() {
        let fs = crate::Fs::memory();
        let stop = CancellationToken::new();
        let workspace = fs
            .create_workspace("policy-deltas", WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        let expected = workspace
            .head(WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        let mut walk = crate::NameWalk::<
            _,
            _,
            crate::demand::native::NativeDemandSource,
            crate::MemoryLazyWorkspaceStore,
        >::exact(expected.clone(), None, crate::GitIgnorePolicy::default())
        .await
        .unwrap();
        while walk.next_page().await.unwrap().is_some() {}
        let mut checkpoint = walk.checkpoint().unwrap();
        for ordinal in 0..66 {
            let policy = if ordinal == 1 {
                crate::GitIgnorePolicy::parse(&"*.ignored\n".repeat(2000))
            } else {
                crate::GitIgnorePolicy::parse("*.ignored")
            };
            let record = serde_json::to_vec(
                &serde_json::json!({"Policy": [format!("d{ordinal:02}"), policy]}),
            )
            .unwrap();
            checkpoint.restore_record(&record).unwrap();
            let metadata = serde_json::to_vec(
                &serde_json::json!({"Directory": [format!("d{ordinal:02}"), {"version": null, "directory_identity": null}]}),
            )
            .unwrap();
            checkpoint.restore_record(&metadata).unwrap();
        }
        let manifest = NameIndexManifest {
            schema: crate::NameIndexManifest::SCHEMA,
            cut: walk.cut(),
            names_cut: walk.cut(),
            observed_generation: expected.id(),
            pages: 0,
            pending_directories: 0,
            complete: true,
            checkpoint,
        };
        let initial = NameIndex::publish(&workspace, &expected, manifest, None, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(initial.checkpoint_policy_records, 66);
        assert_eq!(initial.checkpoint_metadata_records, 66);
        let mut manifest = initial.manifest.clone();
        manifest.checkpoint.forget_subtree("d00");
        let record = serde_json::to_vec(
            &serde_json::json!({"Policy": ["d01", crate::GitIgnorePolicy::parse("*.changed")]}),
        )
        .unwrap();
        manifest.checkpoint.restore_record(&record).unwrap();
        let metadata = serde_json::to_vec(&serde_json::json!({"Directory": [
            "d01", {"version": crate::demand::SourceVersion([2; 32]), "directory_identity": null}
        ]}))
        .unwrap();
        manifest.checkpoint.restore_record(&metadata).unwrap();
        assert_eq!(
            manifest
                .checkpoint
                .policy_records_since(Some(&initial.manifest.checkpoint))
                .count(),
            2
        );
        // Negative control: a complete rewrite serializes every remaining policy.
        assert_eq!(manifest.checkpoint.policy_records_since(None).count(), 65);
        assert_eq!(
            manifest
                .checkpoint
                .directory_records_since(Some(&initial.manifest.checkpoint))
                .count(),
            2
        );
        assert_eq!(
            manifest.checkpoint.directory_records_since(None).count(),
            65
        );
        let updated = NameIndex::publish(
            &workspace,
            &initial.generation(),
            manifest,
            None,
            Some(&initial),
        )
        .await
        .unwrap()
        .unwrap();
        let reopened = NameIndex::open(&workspace).await.unwrap().unwrap();
        assert_eq!(updated.checkpoint_policy_records, 2);
        assert_eq!(updated.checkpoint_metadata_records, 2);
        assert_eq!(
            serde_json::to_vec(&updated.manifest.checkpoint).unwrap(),
            serde_json::to_vec(&reopened.manifest.checkpoint).unwrap()
        );
        let changed = format!("{}/{}", policy_root(), blake3::hash(b"d01").to_hex());
        let old_chunks = read_policy_header(&initial.generation(), &changed)
            .await
            .unwrap()
            .chunks;
        let new_chunks = read_policy_header(&updated.generation(), &changed)
            .await
            .unwrap()
            .chunks;
        assert!(old_chunks > new_chunks);
        for ordinal in new_chunks..old_chunks {
            assert!(
                matches!(updated.generation().read(&policy_chunk(&changed, ordinal), CHUNK_BYTES as u64, WorkBudget::UNBOUNDED, &stop).await,
                Err(failure) if matches!(failure.error, WorkspaceError::NotFound))
            );
        }
        let removed = format!("{}/{}", policy_root(), blake3::hash(b"d00").to_hex());
        assert!(matches!(
            read_policy_header(&updated.generation(), &removed).await,
            Err(SearchError::Workspace(WorkspaceError::NotFound))
        ));
        assert!(
            matches!(updated.generation().read(&policy_chunk(&removed, 0), CHUNK_BYTES as u64, WorkBudget::UNBOUNDED, &stop).await,
            Err(failure) if matches!(failure.error, WorkspaceError::NotFound))
        );
        for directory in ["d02", "d31", "d63"] {
            let path = format!(
                "{}/{}",
                policy_root(),
                blake3::hash(directory.as_bytes()).to_hex()
            );
            for path in [
                format!("{path}.json"),
                policy_chunk(&path, 0),
                format!(
                    "{}/{}.json",
                    directory_root(),
                    blake3::hash(directory.as_bytes()).to_hex()
                ),
            ] {
                let mut ctx =
                    crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &stop);
                let (path, _) = crate::kernel::NamespacePath::parse_in_profile(
                    &path,
                    workspace.profile(),
                    workspace.limits(),
                    &mut ctx,
                    &stop,
                )
                .unwrap();
                let paths = [path];
                let before = initial
                    .generation()
                    .lookup_paths(&paths, WorkBudget::UNBOUNDED, &stop)
                    .await
                    .unwrap()
                    .value;
                let after = updated
                    .generation()
                    .lookup_paths(&paths, WorkBudget::UNBOUNDED, &stop)
                    .await
                    .unwrap()
                    .value;
                assert!(before[0].is_some());
                assert_eq!(before, after, "rewrote unchanged checkpoint metadata");
            }
        }
        // A fresh traversal replaces the current policy set. Older snapshots
        // retain their files through generation pins; the live tree retains no
        // obsolete policy namespace or historical delta chain.
        let mut empty = updated.manifest.clone();
        for ordinal in 0..66 {
            empty.checkpoint.forget_subtree(&format!("d{ordinal:02}"));
        }
        let reset = NameIndex::publish(&workspace, &updated.generation(), empty, None, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reset.checkpoint_policy_records, 0);
        assert_eq!(reset.checkpoint_metadata_records, 0);
        let files = reset
            .generation()
            .list_directory(&policy_root(), None, 128, WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value
            .0;
        assert!(files.entries.is_empty());
        assert_eq!(
            NameIndex::open(&workspace)
                .await
                .unwrap()
                .unwrap()
                .manifest
                .checkpoint
                .policy_records_since(None)
                .count(),
            0
        );
        assert!(
            !initial
                .manifest
                .checkpoint
                .eligible(&crate::GitIgnorePolicy::default(), "d00/file.ignored")
        );
    }

    #[tokio::test]
    async fn name_search_checkpoint_pages_large_frontier_and_reuses_unchanged_chunks() {
        let fs = crate::Fs::memory();
        let stop = CancellationToken::new();
        let workspace = fs
            .create_workspace("checkpoint-pages", WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        let expected = workspace
            .head(WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        let mut walk = crate::NameWalk::<
            _,
            _,
            crate::demand::native::NativeDemandSource,
            crate::MemoryLazyWorkspaceStore,
        >::exact(expected.clone(), None, crate::GitIgnorePolicy::default())
        .await
        .unwrap();
        while walk.next_page().await.unwrap().is_some() {}
        let mut checkpoint = walk.checkpoint().unwrap();
        for ordinal in 0..5000 {
            checkpoint.discover_directory(
                &format!("{}/{}/{ordinal:05}", "d".repeat(250), "e".repeat(250)),
                true,
            );
        }
        let policy = crate::GitIgnorePolicy::parse(&"*.ignored\n".repeat(150));
        for ordinal in 0..32 {
            let record = serde_json::to_vec(
                &serde_json::json!({"Policy": [format!("policy-{ordinal}"), policy]}),
            )
            .unwrap();
            checkpoint.restore_record(&record).unwrap();
        }
        let manifest = NameIndexManifest {
            schema: crate::NameIndexManifest::SCHEMA,
            cut: walk.cut(),
            names_cut: walk.cut(),
            observed_generation: expected.id(),
            pages: 0,
            pending_directories: 5000,
            complete: false,
            checkpoint,
        };
        // Negative control: the former inline checkpoint cannot fit the
        // unchanged manifest file budget, despite containing no candidates.
        assert!(serde_json::to_vec(&manifest).unwrap().len() as u64 > MAXIMUM_INDEX_FILE_BYTES);
        let published = NameIndex::publish(&workspace, &expected, manifest, None, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(published.checkpoint_frontier_records, 5000);
        let opened = NameIndex::open(&workspace).await.unwrap().unwrap();
        assert_eq!(opened.manifest.pending_directories, 5000);
        assert_eq!(
            serde_json::to_vec(&published.manifest.checkpoint).unwrap(),
            serde_json::to_vec(&opened.manifest.checkpoint).unwrap()
        );
        let generation = opened.generation();
        let root = generation
            .read(MANIFEST, 4096, WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        let stored: StoredManifest = serde_json::from_slice(&root).unwrap();
        assert_eq!(stored.checkpoint_chunks, 1);
        assert!(stored.frontier_blocks > 32);
        let mut checkpoint_paths = Vec::new();
        for ordinal in 0..stored.checkpoint_chunks {
            checkpoint_paths.push(chunk_path(ordinal));
            let chunk = generation
                .read(
                    &chunk_path(ordinal),
                    CHUNK_BYTES as u64,
                    WorkBudget::UNBOUNDED,
                    &stop,
                )
                .await
                .unwrap()
                .value;
            assert!(chunk.len() <= CHUNK_BYTES);
        }
        for ordinal in 0..stored.frontier_blocks {
            let path = frontier_path(ordinal);
            checkpoint_paths.push(format!("{path}.json"));
            let header = read_policy_header(&generation, &path).await.unwrap();
            for chunk in 0..header.chunks {
                let path = policy_chunk(&path, chunk);
                let bytes = generation
                    .read(&path, CHUNK_BYTES as u64, WorkBudget::UNBOUNDED, &stop)
                    .await
                    .unwrap()
                    .value;
                assert!(bytes.len() <= CHUNK_BYTES);
                checkpoint_paths.push(path);
            }
        }
        let resumed = crate::NameWalk::<
            _,
            _,
            crate::demand::native::NativeDemandSource,
            crate::MemoryLazyWorkspaceStore,
        >::resume_exact(
            &workspace,
            opened.manifest.checkpoint.clone(),
            crate::GitIgnorePolicy::default(),
        )
        .await
        .unwrap();
        assert_eq!(resumed.pending_directories(), 5000);
        let republished = NameIndex::publish(
            &workspace,
            &generation,
            opened.manifest.clone(),
            None,
            Some(&opened),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(republished.checkpoint_policy_records, 0);
        assert_eq!(republished.checkpoint_frontier_records, 0);
        // Inspect each checkpoint binding directly. A whole-generation diff
        // spends its join budget traversing unrelated bindings as well.
        for path in &checkpoint_paths {
            let mut ctx = crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &stop);
            let (path, _) = crate::kernel::NamespacePath::parse_in_profile(
                path,
                workspace.profile(),
                workspace.limits(),
                &mut ctx,
                &stop,
            )
            .unwrap();
            let paths = [path];
            let before = generation
                .lookup_paths(&paths, WorkBudget::UNBOUNDED, &stop)
                .await
                .unwrap()
                .value;
            let after = republished
                .generation()
                .lookup_paths(&paths, WorkBudget::UNBOUNDED, &stop)
                .await
                .unwrap()
                .value;
            assert!(before[0].is_some());
            assert_eq!(before, after);
        }
        let mut extended = republished.manifest.clone();
        extended
            .checkpoint
            .discover_directory("new-directory", true);
        extended.pending_directories += 1;
        let updated = NameIndex::publish(
            &workspace,
            &republished.generation(),
            extended,
            None,
            Some(&republished),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(updated.checkpoint_frontier_records <= 32);
        assert!(updated.checkpoint_frontier_records > 0);
        // Negative control: the old inline continuation stream visits 5001
        // rows after this single append. The counter above sits at encoding.
        assert_eq!(updated.manifest.checkpoint.pending_directories(), 5001);
        assert_eq!(
            serde_json::to_vec(&updated.manifest.checkpoint).unwrap(),
            serde_json::to_vec(
                &NameIndex::open(&workspace)
                    .await
                    .unwrap()
                    .unwrap()
                    .manifest
                    .checkpoint
            )
            .unwrap()
        );
        // A fresh traversal retires all continuation blocks in the live cut;
        // the earlier generation keeps its pinned restart frontier.
        let mut reset = updated.manifest.clone();
        reset.checkpoint = walk.checkpoint().unwrap();
        reset.pending_directories = 0;
        reset.complete = true;
        let reset = NameIndex::publish(&workspace, &updated.generation(), reset, None, None)
            .await
            .unwrap()
            .unwrap();
        let frontier_files = reset
            .generation()
            .list_directory(
                &format!("{ROOT}/frontier"),
                None,
                128,
                WorkBudget::UNBOUNDED,
                &stop,
            )
            .await
            .unwrap()
            .value
            .0;
        assert!(frontier_files.entries.is_empty());
        assert_eq!(reset.checkpoint_frontier_records, 0);
        assert_eq!(
            NameIndex::open(&workspace)
                .await
                .unwrap()
                .unwrap()
                .manifest
                .pending_directories,
            0
        );
        assert_eq!(opened.manifest.pending_directories, 5000);
    }
}

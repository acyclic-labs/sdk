//! Offline, copy-only capacity increases. The destination is never activated here.

use super::*;

impl LocalStream {
    /// Copies a closed store into a new, private directory with increased capacity.
    ///
    /// The source must exist and is exclusively locked until validation finishes. Its
    /// journal and snapshot are never changed. Recovery (including torn-tail repair)
    /// happens only on the copy. Every configured bound must stay equal or increase.
    /// The returned provider holds the destination lock and has replayed the migrated
    /// store successfully. Drop it before activating the destination externally.
    ///
    /// The destination must not exist. On error or cancellation it may contain a partial
    /// copy: do not activate it. Keep the source as a rollback copy and only switch the
    /// application after successful completion. This migrates Streams, not sibling object
    /// stores; applications must separately back up and preserve those stores.
    pub async fn copy_with_increased_capacity(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        previous: LocalStreamLimits,
        increased: LocalStreamLimits,
    ) -> Result<Self, LocalStreamError> {
        validate_limits(previous)?;
        validate_limits(increased)?;
        let old = encode_limits(previous)?;
        let new = encode_limits(increased)?;
        for (old, new) in old.as_chunks::<8>().0.iter().zip(new.as_chunks::<8>().0) {
            let old = u64::from_le_bytes(*old);
            let new = u64::from_le_bytes(*new);
            if new < old {
                return Err(LocalStreamError::InvalidLimits);
            }
        }
        let source = source.as_ref().to_path_buf();
        let destination = destination.as_ref().to_path_buf();
        // Keep ownership through blocking work even when the requesting task is dropped.
        run_owned_initialization(async move {
            let copy_root = destination.clone();
            let source_lock = tokio::task::spawn_blocking(move || {
                // Never create a missing source (which would look like an empty migration).
                std::fs::metadata(source.join(JOURNAL_FILE))?;
                let source_lock = lock_root(&source, previous)?;
                let mut header = [0_u8; HEADER_BYTES];
                File::open(source.join(JOURNAL_FILE))?.read_exact(&mut header)?;
                decode_header(&header, previous).ok_or(LocalStreamError::Corrupt)?;
                #[cfg(unix)]
                std::os::unix::fs::DirBuilderExt::mode(&mut std::fs::DirBuilder::new(), 0o700)
                    .create(&copy_root)?;
                #[cfg(not(unix))]
                std::fs::DirBuilder::new().create(&copy_root)?;
                std::fs::copy(source.join(JOURNAL_FILE), copy_root.join(JOURNAL_FILE))?;
                match std::fs::copy(source.join(SNAPSHOT_FILE), copy_root.join(SNAPSHOT_FILE)) {
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                Ok::<_, LocalStreamError>(source_lock)
            })
            .await
            .map_err(|_| LocalStreamError::Executor)??;

            // Validate original limits and recover only the disposable copy.
            let recovered = Self::open(&destination, previous).await?;
            let state = recovered.inner.provider.encode_state().await;
            let store_time = recovered.inner.clock.now_unix_millis();
            drop(recovered);
            let write_root = destination.clone();
            tokio::task::spawn_blocking(move || {
                let _destination_lock = lock_root(&write_root, increased)?;
                for (name, bytes) in [
                    (
                        SNAPSHOT_FILE,
                        encode_snapshot(increased, 0, store_time, &state)?,
                    ),
                    (JOURNAL_FILE, encode_header(increased, 0)?),
                ] {
                    let mut file = File::create(write_root.join(name))?;
                    file.write_all(&bytes)?;
                    sync_file(&file, increased.durability)?;
                }
                sync_directory(&write_root, increased.durability)?;
                // Persist the directory entry in its parent as well.
                if let Some(parent) = write_root.parent() {
                    sync_directory(parent, increased.durability)?;
                }
                Ok::<_, LocalStreamError>(())
            })
            .await
            .map_err(|_| LocalStreamError::Executor)??;
            let migrated = Self::open(destination, increased).await?;
            drop(source_lock);
            Ok(migrated)
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An operator-supplied offline copy is never checked into the repository.
    #[tokio::test]
    #[ignore = "requires ACYCLIC_STREAM_OFFLINE_COPY pointing to a private closed store"]
    async fn capacity_copy_operator_fixture() -> Result<(), Box<dyn std::error::Error>> {
        let source = std::env::var("ACYCLIC_STREAM_OFFLINE_COPY")?;
        let directory = tempfile::tempdir()?;
        let previous = LocalStreamLimits::default();
        let provider = LocalStream::open(&source, previous).await?;
        let expected = provider.inner.provider.encode_state().await;
        drop(provider);
        let increased = LocalStreamLimits {
            memory: MemoryLimits {
                paths: 500_000,
                ..previous.memory
            },
            ..previous
        };
        let migrated = LocalStream::copy_with_increased_capacity(
            &source,
            directory.path().join("migrated"),
            previous,
            increased,
        )
        .await?;
        assert_eq!(migrated.inner.provider.encode_state().await, expected);
        migrated
            .append(AppendRequest {
                path: StreamPath::new(
                    "capacity-verification/new/hierarchy/with/more/than/four/paths",
                )?,
                records: vec![Bytes::from_static(b"verified")],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        println!("Complete recovered state preserved; new hierarchy allocation succeeded.");
        Ok(())
    }

    #[tokio::test]
    async fn capacity_copy_preserves_history_retries_and_source()
    -> Result<(), Box<dyn std::error::Error>> {
        for compact in [false, true] {
            let directory = tempfile::tempdir()?;
            let source = directory.path().join("source");
            let destination = directory.path().join("destination");
            let previous = LocalStreamLimits {
                memory: MemoryLimits {
                    paths: 1,
                    ..MemoryLimits::default()
                },
                journal_operations: if compact { 8 } else { 1_000_000 },
                ..LocalStreamLimits::default()
            };
            let provider = LocalStream::open(&source, previous).await?;
            let mut outcomes = Vec::new();
            for index in 0..21 {
                outcomes.push(super::super::tests::append_keyed(&provider, index).await?);
            }
            let expected = provider.inner.provider.encode_state().await;
            let increased = LocalStreamLimits {
                memory: MemoryLimits {
                    paths: 10,
                    ..previous.memory
                },
                ..previous
            };
            assert!(matches!(
                LocalStream::copy_with_increased_capacity(
                    &source,
                    &destination,
                    previous,
                    increased
                )
                .await,
                Err(LocalStreamError::AlreadyOpen)
            ));
            assert!(!destination.exists());
            drop(provider);
            let original_journal = std::fs::read(source.join(JOURNAL_FILE))?;
            let original_snapshot = std::fs::read(source.join(SNAPSHOT_FILE)).ok();
            let migrated = LocalStream::copy_with_increased_capacity(
                &source,
                &destination,
                previous,
                increased,
            )
            .await?;
            assert_eq!(migrated.inner.provider.encode_state().await, expected);
            for (index, outcome) in (0..).zip(&outcomes) {
                assert_eq!(
                    &super::super::tests::append_keyed(&migrated, index).await?,
                    outcome
                );
            }
            migrated
                .append(AppendRequest {
                    path: StreamPath::new("new-capacity")?,
                    records: vec![Bytes::from_static(b"works")],
                    if_tail: Some(0),
                    idempotency_key: None,
                })
                .await?;
            drop(migrated);
            let reopened = LocalStream::open(&destination, increased).await?;
            assert_eq!(reopened.tail(StreamPath::new("new-capacity")?).await?, 1);
            assert_eq!(std::fs::read(source.join(JOURNAL_FILE))?, original_journal);
            assert_eq!(
                std::fs::read(source.join(SNAPSHOT_FILE)).ok(),
                original_snapshot
            );
            let rollback = LocalStream::open(&source, previous).await?;
            assert_eq!(rollback.inner.provider.encode_state().await, expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn capacity_copy_rejects_shrink_existing_destination_and_corruption()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        let destination = directory.path().join("destination");
        let limits = LocalStreamLimits::default();
        drop(LocalStream::open(&source, limits).await?);
        let smaller = LocalStreamLimits {
            memory: MemoryLimits {
                paths: 1,
                ..limits.memory
            },
            ..limits
        };
        assert!(matches!(
            LocalStream::copy_with_increased_capacity(&source, &destination, limits, smaller).await,
            Err(LocalStreamError::InvalidLimits)
        ));
        assert!(!destination.exists());
        std::fs::create_dir(&destination)?;
        assert!(
            LocalStream::copy_with_increased_capacity(&source, &destination, limits, limits)
                .await
                .is_err()
        );
        std::fs::write(source.join(JOURNAL_FILE), b"corrupt")?;
        assert!(
            LocalStream::copy_with_increased_capacity(
                &source,
                directory.path().join("bad"),
                limits,
                limits
            )
            .await
            .is_err()
        );
        assert_eq!(std::fs::read(source.join(JOURNAL_FILE))?, b"corrupt");
        Ok(())
    }
}

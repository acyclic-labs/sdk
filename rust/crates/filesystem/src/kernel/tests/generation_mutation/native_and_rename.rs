use super::*;

#[cfg(not(target_arch = "wasm32"))]
struct NoGenerationCalls;

#[cfg(not(target_arch = "wasm32"))]
#[allow(
    clippy::panic,
    reason = "The fixture must fail if denied work reaches the provider."
)]
impl AsyncObjectStore for NoGenerationCalls {
    fn decoded_cache_get(
        &self,
        _: crate::async_storage::DecodedCacheKey,
    ) -> Result<Option<crate::async_storage::DecodedCacheValue>, crate::ObjectStoreError> {
        panic!("denied generation entered provider cache");
    }

    async fn put(
        &self,
        _: ObjectId,
        _: OwnedBytes,
        _: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        panic!("denied generation entered provider write");
    }

    async fn read(
        &self,
        _: ObjectId,
        _: u64,
        _: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        panic!("denied generation entered provider read");
    }

    async fn read_many(
        &self,
        _: &[crate::storage::ObjectReadRequest],
        _: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        panic!("denied generation entered provider batch");
    }

    async fn contains(
        &self,
        _: ObjectId,
        _: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        panic!("denied generation entered provider probe");
    }

    async fn put_hashed(
        &self,
        _: crate::storage::HashedObject,
        _: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        panic!("denied generation entered provider write");
    }
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn original_generation_denies_identity_scratch_before_provider_calls()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let source = [Mutation::File {
        file_id: FileId::from_bytes([2; 16]),
        mutation: FileMutation::ValidateRegular,
    }];
    let (operations, input_guard) = original_identity_operations(&source, &mut accounting)?;
    let limited = original.native_volume_work(WorkBudget {
        allocation_operations: 0,
        ..WorkBudget::UNBOUNDED
    })?;
    let mut accounting = KernelAccounting::native(&limited, &cancellation);
    let before = original.receipt();
    let error = (apply_generation_mutations_retaining_with_accounting(
        &NoGenerationCalls,
        &fixture.generation,
        operations,
        config(),
        &mut accounting,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("exhausted original allocated identity scratch")?;
    assert!(matches!(
        error,
        crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(
            WorkError::BudgetExceeded { .. }
        ))
    ));
    assert_eq!(original.receipt(), before);
    drop(error);
    drop(input_guard);
    drop(limited);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn original_root_clone_denial_releases_parents_without_copy()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let mut root = fixture(&store).await?.generation;
    root.parents
        .push(GenerationId::new(Digest::from_bytes([17; 32])));
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            bytes_copied: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let error = clone_generation_root_with_accounting(&root, &mut accounting)
        .err()
        .ok_or("zero original copy budget cloned parents")?;
    assert!(matches!(
        error,
        crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(
            WorkError::BudgetExceeded { .. }
        ))
    ));
    assert_eq!(original.receipt().bytes_copied, 0);
    assert_eq!(original.receipt().backend_read_operations, 0);
    assert_eq!(original.receipt().backend_write_operations, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn original_identity_operations(
    source: &[Mutation],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<Mutation>, KernelOwnedAllocation), crate::FsError> {
    assert!(source.iter().all(|operation| matches!(
        operation,
        Mutation::File { .. } | Mutation::CloneFileRange { .. }
    )));
    let mut operations = reserve_owned::<Mutation>(source.len(), accounting)?;
    let delta = WorkCounters {
        bytes_copied: usize_to_u64(source.len())
            .checked_mul(size_of::<Mutation>() as u64)
            .ok_or(WorkError::Overflow)?,
        ..WorkCounters::default()
    };
    accounting.admit(delta)?;
    operations.value.extend(source.iter().cloned());
    accounting.record_completed(delta)?;
    Ok((operations.value, operations.allocation))
}

/// One kind a rename endpoint can name in [`rename_follows_the_posix_kind_rules`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RenameEndpoint {
    Absent,
    Regular,
    SymbolicLink,
    EmptyDirectory,
    FullDirectory,
}

impl RenameEndpoint {
    const ALL: [Self; 5] = [
        Self::Absent,
        Self::Regular,
        Self::SymbolicLink,
        Self::EmptyDirectory,
        Self::FullDirectory,
    ];

    const fn is_directory(self) -> bool {
        matches!(self, Self::EmptyDirectory | Self::FullDirectory)
    }

    /// The mutations that bind `name` to this kind, under fresh identities
    /// starting at `identity`.
    async fn create(
        self,
        store: &MemoryObjectStore,
        fixture: &Fixture,
        name: &str,
        identity: u8,
    ) -> Result<Vec<Mutation>, Box<dyn std::error::Error>> {
        let directory = |file_id| FileRecord {
            file_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata: fixture.metadata,
            payload: FilePayload::Directory {
                entries: fixture.empty_tree,
            },
        };
        let file_id = FileId::from_bytes([identity; 16]);
        let record = match self {
            Self::Absent => return Ok(Vec::new()),
            Self::Regular => regular(file_id, fixture.metadata)?,
            Self::SymbolicLink => FileRecord {
                file_id,
                kind: FileKind::SymbolicLink,
                link_count: 1,
                metadata: fixture.metadata,
                payload: FilePayload::SymbolicLink {
                    target_bytes: 6,
                    target: blob(store, b"target").await?,
                },
            },
            Self::EmptyDirectory | Self::FullDirectory => directory(file_id),
        };
        let mut mutations = vec![Mutation::Create {
            path: path(&[name])?,
            record,
        }];
        if self == Self::FullDirectory {
            mutations.push(Mutation::Create {
                path: path(&[name, "child"])?,
                record: regular(FileId::from_bytes([identity + 1; 16]), fixture.metadata)?,
            });
        }
        Ok(mutations)
    }
}

/// What POSIX `rename(2)` (Linux `renameat2` with `RENAME_NOREPLACE` when
/// `replace` is false) does with these endpoints: `None` succeeds.
fn posix_rename(
    source: RenameEndpoint,
    destination: RenameEndpoint,
    same_name: bool,
    replace: bool,
) -> Option<&'static str> {
    if source == RenameEndpoint::Absent {
        return Some("ENOENT");
    }
    if destination == RenameEndpoint::Absent {
        return None;
    }
    if !replace {
        return Some("EEXIST");
    }
    if same_name {
        return None;
    }
    match (source.is_directory(), destination.is_directory()) {
        (true, false) => Some("ENOTDIR"),
        (false, true) => Some("EISDIR"),
        _ if destination == RenameEndpoint::FullDirectory => Some("ENOTEMPTY"),
        _ => None,
    }
}

fn errno_of(error: &GenerationMutationError) -> &'static str {
    match error {
        GenerationMutationError::MissingSource => "ENOENT",
        GenerationMutationError::AlreadyExists => "EEXIST",
        GenerationMutationError::NotDirectory => "ENOTDIR",
        GenerationMutationError::IsDirectory => "EISDIR",
        GenerationMutationError::DirectoryNotEmpty => "ENOTEMPTY",
        _ => "unexpected",
    }
}

/// Every pair of endpoint kinds, both replacement policies, and a name
/// renamed onto itself, against the POSIX reference above: a refusal
/// publishes nothing, and a success moves the source's identity whole.
#[tokio::test]
async fn rename_follows_the_posix_kind_rules() -> Result<(), Box<dyn std::error::Error>> {
    let mut cases = Vec::new();
    for source in RenameEndpoint::ALL {
        for replace in [true, false] {
            cases.push((source, source, true, replace));
            for destination in RenameEndpoint::ALL {
                cases.push((source, destination, false, replace));
            }
        }
    }
    for (source, destination, same_name, replace) in cases {
        let case =
            format!("{source:?} -> {destination:?}, same name {same_name}, replace {replace}");
        let store = MemoryObjectStore::default();
        let fixture = fixture(&store).await?;
        let mut setup = source.create(&store, &fixture, "source", 10).await?;
        if !same_name {
            setup.extend(
                destination
                    .create(&store, &fixture, "destination", 20)
                    .await?,
            );
        }
        let base = if setup.is_empty() {
            fixture.generation.clone()
        } else {
            apply_generation_mutations(
                &store,
                &fixture.generation,
                setup,
                config(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?
            .root
        };
        let destination_name = if same_name { "source" } else { "destination" };
        let renamed = apply_generation_mutations(
            &store,
            &base,
            vec![Mutation::Rename {
                source: path(&["source"])?,
                destination: path(&[destination_name])?,
                replace,
            }],
            config(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await;
        async fn lookup(
            store: &MemoryObjectStore,
            generation: &GenerationRoot,
            name: &str,
        ) -> Result<Option<FileId>, Box<dyn std::error::Error>> {
            Ok(lookup_path(
                store,
                generation,
                &path(&[name])?,
                config(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?
            .record
            .map(|record| record.file_id))
        }
        match (
            posix_rename(source, destination, same_name, replace),
            renamed,
        ) {
            (None, Ok(receipt)) => {
                assert_eq!(
                    lookup(&store, &receipt.root, destination_name).await?,
                    Some(FileId::from_bytes([10; 16])),
                    "{case}: the destination names the source's identity"
                );
                if !same_name {
                    assert_eq!(
                        lookup(&store, &receipt.root, "source").await?,
                        None,
                        "{case}"
                    );
                }
            }
            (Some(expected), Err(failure)) => {
                assert_eq!(
                    errno_of(&failure.error),
                    expected,
                    "{case}: {}",
                    failure.error
                );
                // Only emptiness needs the candidate view built.
                if expected != "ENOTEMPTY" {
                    assert_eq!(failure.work.backend_write_operations, 0, "{case}");
                }
            }
            (expected, Ok(_)) => panic!("{case}: succeeded, POSIX gives {expected:?}"),
            (None, Err(failure)) => panic!("{case}: POSIX succeeds, got {}", failure.error),
        }
    }
    Ok(())
}

/// A name never replaces a directory above it: that directory holds the
/// name, so it is not empty (Linux `ENOTEMPTY`), whatever the kinds.
#[tokio::test]
async fn rename_over_an_ancestor_is_refused() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
    let directory = |identity| FileRecord {
        file_id: FileId::from_bytes([identity; 16]),
        kind: FileKind::Directory,
        link_count: 1,
        metadata: fixture.metadata,
        payload: FilePayload::Directory {
            entries: fixture.empty_tree,
        },
    };
    let base = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![
            Mutation::Create {
                path: path(&["dir", "e"])?,
                record: directory(10),
            },
            Mutation::Create {
                path: path(&["dir", "e", "a"])?,
                record: directory(11),
            },
            Mutation::Create {
                path: path(&["dir", "e", "f"])?,
                record: regular(FileId::from_bytes([12; 16]), fixture.metadata)?,
            },
        ],
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .root;
    for (source, destination) in [
        (&["dir", "e", "a"][..], &["dir", "e"][..]),
        (&["dir", "e", "a"][..], &["dir"][..]),
        (&["dir", "e", "f"][..], &["dir", "e"][..]),
    ] {
        let failure = apply_generation_mutations(
            &store,
            &base,
            vec![Mutation::Rename {
                source: path(source)?,
                destination: path(destination)?,
                replace: true,
            }],
            config(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or_else(|| format!("{source:?} replaced its ancestor {destination:?}"))?;
        assert!(
            matches!(failure.error, GenerationMutationError::DirectoryNotEmpty),
            "{source:?} -> {destination:?}: {}",
            failure.error
        );
        assert_eq!(failure.work.backend_write_operations, 0);
    }
    Ok(())
}

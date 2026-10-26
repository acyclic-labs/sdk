use super::*;
use acyclic_fs::{MemoryAuthorityBackend, MemoryObjectBackend};
use proptest::prelude::*;

type Host = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))
}

async fn fixture() -> Result<(Arc<Host>, WorkspaceRef)> {
    let filesystem = Fs::memory();
    filesystem
        .create_workspace(
            "shared-bytes",
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| map_error(failure.error))?;
    let provider = ProviderRef::new("test".into(), "filesystem".into(), "1".into(), &mut None)?;
    let reference = workspace_ref(provider.clone(), "shared-bytes")?;
    Ok((FilesystemHost::new(filesystem, &provider, None)?, reference))
}

fn rejected(
    runtime: &tokio::runtime::Runtime,
    host: &Host,
    reference: &WorkspaceRef,
    bytes: acyclic_stream::OwnedBytes,
    io: Option<&PreparationIo>,
) -> Result<(acyclic_test_support::AllocationInfo, std::time::Duration)> {
    let mutations = [WorkspaceMutation::PutFile {
        path: "/../invalid".into(),
        bytes,
    }];
    let key = IdempotencyKey::new("rejected-write")?;
    let mut result = None;
    let started = std::time::Instant::now();
    let allocations = acyclic_test_support::measure(|| {
        result = Some(runtime.block_on(async {
            host.apply_generation(reference, None, &mutations, key, io)?
                .await
        }));
    });
    let elapsed = started.elapsed();
    let error = match result {
        Some(Err(error)) => error,
        _ => return Err(Error::Invalid("invalid path was not rejected".into())),
    };
    #[cfg(not(target_arch = "wasm32"))]
    let error = match error {
        Error::NativeWorkspace { source, .. } => map_error(*source),
        error => error,
    };
    assert!(matches!(error, Error::Invalid(_)), "{error:?}");
    Ok((allocations, elapsed))
}

#[test]
fn rejected_writes_do_not_allocate_or_copy_payload_storage() -> Result<()> {
    let runtime = runtime()?;
    let (host, reference) = runtime.block_on(fixture())?;
    let (empty, _) = rejected(
        &runtime,
        &host,
        &reference,
        acyclic_stream::OwnedBytes::new(),
        None,
    )?;
    for length in [1 << 20, 4 << 20] {
        let bytes: acyclic_stream::OwnedBytes = vec![123; length + 32].into();
        let slice = bytes.slice(16..length + 16);
        let (large, _) = rejected(&runtime, &host, &reference, slice, None)?;
        assert_eq!(large.count_total, empty.count_total, "{length} bytes");
        assert_eq!(large.bytes_total, empty.bytes_total, "{length} bytes");
        assert_eq!(large.bytes_max, empty.bytes_max, "{length} bytes");
    }
    let (unique, _) = rejected(&runtime, &host, &reference, vec![123; 32].into(), None)?;
    for length in [1 << 20, 4 << 20] {
        let (large, _) = rejected(&runtime, &host, &reference, vec![123; length].into(), None)?;
        assert_eq!(
            large.count_total, unique.count_total,
            "unique {length} bytes"
        );
        assert_eq!(
            large.bytes_total, unique.bytes_total,
            "unique {length} bytes"
        );
        assert_eq!(large.bytes_max, unique.bytes_max, "unique {length} bytes");
    }
    Ok(())
}

#[test]
fn rejected_without_subscriber() -> Result<()> {
    profile(false, false)
}
#[test]
fn rejected_with_tracing() -> Result<()> {
    profile(true, false)
}
#[test]
fn rejected_with_accounting() -> Result<()> {
    profile(false, true)
}

fn profile(tracing: bool, accounting: bool) -> Result<()> {
    let dispatch = if tracing {
        tracing::Dispatch::new(tracing_subscriber::registry())
    } else {
        tracing::Dispatch::none()
    };
    let _dispatch = tracing::dispatcher::set_default(&dispatch);
    let runtime = runtime()?;
    let (host, reference) = runtime.block_on(fixture())?;
    let bytes: acyclic_stream::OwnedBytes = vec![123; 4 << 20].into();
    // Promote ordinary Bytes storage before measuring shared-owner clones.
    let shared = bytes.clone();
    let mut times = Vec::with_capacity(32);
    let mut allocations = Vec::with_capacity(32);
    for _ in 0..32 {
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(100_000, 16_000_000),
            &cancellation,
        )?;
        let (allocation, elapsed) = rejected(
            &runtime,
            &host,
            &reference,
            shared.clone(),
            accounting.then_some(&io),
        )?;
        allocations.push((
            allocation.count_total,
            allocation.bytes_total,
            allocation.bytes_max,
        ));
        times.push(elapsed);
        if accounting {
            eprintln!("receipt={:?}", io.receipt());
        }
    }
    times.sort_unstable();
    eprintln!(
        "tracing={tracing} accounting={accounting} median={:?}; allocations={:?}..{:?}",
        times.get(16),
        allocations.iter().min(),
        allocations.iter().max()
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // Sharing an arbitrary visible range must preserve bytes through publication,
    // mutation retirement, replacement and the accounted/unaccounted paths.
    #[test]
    fn publication_preserves_visible_bytes(
        payload in prop::collection::vec(any::<u8>(), 0..4096),
        prefix in 0..32_usize, suffix in 0..32_usize,
    ) {
        let runtime = runtime().map_err(|error| TestCaseError::fail(error.to_string()))?;
        let result = runtime.block_on(publication_case(&payload, prefix, suffix));
        prop_assert!(result.is_ok(), "{result:?}");
    }
}

async fn publication_case(payload: &[u8], prefix: usize, suffix: usize) -> Result<()> {
    let (host, reference) = fixture().await?;
    let mut backing = vec![42; prefix];
    backing.extend_from_slice(payload);
    backing.resize(backing.len() + suffix + 1, 231);
    let bytes: acyclic_stream::OwnedBytes = backing.into();
    for accounted in [false, true] {
        let mut expected = payload.to_vec();
        if accounted {
            expected.push(231);
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(100_000, 16_000_000),
            &cancellation,
        )?;
        let mutations = [WorkspaceMutation::PutFile {
            path: "/content".into(),
            bytes: bytes.slice(prefix..prefix + expected.len()),
        }];
        let key = IdempotencyKey::new(if accounted { "accounted" } else { "ordinary" })?;
        let generation = host
            .apply_generation(&reference, None, &mutations, key, accounted.then_some(&io))?
            .await?;
        drop(mutations);
        let generation = host.generation_ref_id(generation.id(), None)?;
        let actual = host
            .read(&reference, Some(&generation), "/content", 4096, None)
            .await?;
        assert_eq!(actual.as_ref(), expected.as_slice());
        assert_eq!(
            bytes.get(prefix..prefix + expected.len()),
            Some(expected.as_slice())
        );
    }
    Ok(())
}

fn retained_operation_bytes<Args, T>(_: impl FnOnce(Args) -> T) -> usize {
    std::mem::size_of::<T>()
}

#[test]
fn publication_boundaries_retain_only_owned_polling_handles() {
    type Transaction = acyclic_fs::Transaction<MemoryAuthorityBackend, MemoryObjectBackend>;
    let staging = retained_operation_bytes(
        |(transaction, cancellation): (
            &'static mut Transaction,
            &'static acyclic_fs::CancellationToken,
        )| {
            Host::stage_unmeasured(
                transaction,
                &[],
                false,
                acyclic_fs::WorkBudget::UNBOUNDED,
                cancellation,
            )
        },
    );
    let publication = retained_operation_bytes(
        |(host, workspace, key): (
            &'static Host,
            &'static WorkspaceRef,
            &'static IdempotencyKey,
        )| {
            host.apply_generation_unmeasured(
                workspace,
                None,
                &[],
                key,
                false,
                FilePublication::Replace(None),
            )
        },
    );
    let content = retained_operation_bytes(
        |(host, volume, grant, key): (
            &'static Host,
            &'static VolumeRef,
            &'static ContentGrant,
            &'static IdempotencyKey,
        )| {
            let input: &'static [u8] = b"";
            host.put_content_impl(
                volume,
                grant,
                "content",
                input,
                "text/plain",
                "content",
                0,
                key,
                None,
                None,
                None,
                None,
            )
        },
    );
    eprintln!(
        "publication retained bytes: staging={staging} publication={publication} content={content}"
    );
    assert!(
        [staging, publication, content]
            .into_iter()
            .all(|bytes| bytes <= 512),
        "publication embeds a concrete child operation in its caller"
    );
}
#[test]
fn transaction_operations_retain_only_owned_polling_handles() {
    type Transaction = acyclic_fs::Transaction<MemoryAuthorityBackend, MemoryObjectBackend>;
    type Input = (
        &'static mut Transaction,
        &'static acyclic_fs::CancellationToken,
    );
    let directory = retained_operation_bytes(|(transaction, cancellation): Input| {
        unmeasured_workspace_operation(|| {
            transaction.create_dir_all("/dir", acyclic_fs::WorkBudget::UNBOUNDED, cancellation)
        })
    });
    let write = retained_operation_bytes(|(transaction, cancellation): Input| {
        unmeasured_workspace_operation(|| {
            transaction.write(
                "/file",
                acyclic_stream::OwnedBytes::new(),
                acyclic_fs::WorkBudget::UNBOUNDED,
                cancellation,
            )
        })
    });
    let create = retained_operation_bytes(|(transaction, cancellation): Input| {
        unmeasured_workspace_operation(|| {
            transaction.create_file(
                "/file",
                acyclic_stream::OwnedBytes::new(),
                acyclic_fs::kernel::FileMetadata::default(),
                acyclic_fs::WorkBudget::UNBOUNDED,
                cancellation,
            )
        })
    });
    let remove = retained_operation_bytes(|(transaction, cancellation): Input| {
        unmeasured_workspace_operation(|| {
            transaction.remove("/file", acyclic_fs::WorkBudget::UNBOUNDED, cancellation)
        })
    });
    #[cfg(not(target_arch = "wasm32"))]
    let batch = retained_operation_bytes(|(transaction, cancellation): Input| {
        unmeasured_workspace_operation(|| {
            transaction.write_batch(&[], &[], acyclic_fs::WorkBudget::UNBOUNDED, cancellation)
        })
    });
    #[cfg(not(target_arch = "wasm32"))]
    let sizes = [directory, write, create, remove, batch];
    #[cfg(target_arch = "wasm32")]
    let sizes = [directory, write, create, remove];
    eprintln!("transaction retained bytes: {sizes:?}");
    assert!(
        sizes.into_iter().all(|bytes| bytes <= 512),
        "transaction staging embeds concrete SDK operations in its poll frame"
    );
}

#[test]
fn publication_children_retain_only_owned_polling_handles() {
    type Workspace = acyclic_fs::Workspace<MemoryAuthorityBackend, MemoryObjectBackend>;
    type Generation = acyclic_fs::Generation<MemoryAuthorityBackend, MemoryObjectBackend>;
    type Transaction = acyclic_fs::Transaction<MemoryAuthorityBackend, MemoryObjectBackend>;
    let authorization = retained_operation_bytes(
        |(host, workspace, grant): (&'static Host, &'static Workspace, &'static ContentGrant)| {
            host.authorize_content_write(workspace, grant, "file", false, None, None)
        },
    );
    let retained_read = retained_operation_bytes(
        |(host, generation, file): (&'static Host, &'static Generation, &'static FileRef)| {
            host.read_retained_file(generation, file, 64, None)
        },
    );
    let begin = retained_operation_bytes(
        |(workspace, key, cancellation): (
            &'static Workspace,
            FilesystemKey,
            &'static acyclic_fs::CancellationToken,
        )| {
            unmeasured_workspace_operation(|| {
                workspace.begin_transaction(key, acyclic_fs::WorkBudget::UNBOUNDED, cancellation)
            })
        },
    );
    let begin_at = retained_operation_bytes(
        |(workspace, generation, key, cancellation): (
            &'static Workspace,
            &'static Generation,
            FilesystemKey,
            &'static acyclic_fs::CancellationToken,
        )| {
            unmeasured_workspace_operation(|| {
                workspace.begin_transaction_at(
                    generation,
                    key,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    cancellation,
                )
            })
        },
    );
    let rebase = retained_operation_bytes(
        |(transaction, cancellation): (
            &'static mut Transaction,
            &'static acyclic_fs::CancellationToken,
        )| {
            unmeasured_workspace_operation(|| {
                transaction.rebase(1, acyclic_fs::WorkBudget::UNBOUNDED, cancellation)
            })
        },
    );
    let commit = retained_operation_bytes(
        |(transaction, cancellation): (
            &'static mut Transaction,
            &'static acyclic_fs::CancellationToken,
        )| {
            unmeasured_workspace_operation(|| {
                transaction.commit(
                    None,
                    acyclic_fs::PublicationPermit::Unrestricted,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    cancellation,
                    |_| async {
                        Ok::<_, acyclic_fs::OperationFailure<Error>>(acyclic_fs::OperationReceipt {
                            value: (),
                            work: acyclic_fs::WorkCounters::default(),
                        })
                    },
                )
            })
        },
    );
    let sizes = [
        authorization,
        retained_read,
        begin,
        begin_at,
        rebase,
        commit,
    ];
    eprintln!("publication child retained bytes: {sizes:?}");
    assert!(
        sizes.into_iter().all(|bytes| bytes <= 512),
        "publication embeds concrete validation or transaction lifecycle futures"
    );
}

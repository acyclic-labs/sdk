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
            host.apply_generation(reference, None, &mutations, key, io, None)?
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // A stale base may merge or conflict, but cannot erase a newer sibling.
    #[test]
    fn stale_publication_preserves_newer_files(payload in prop::collection::vec(any::<u8>(), 0..256)) {
        runtime().unwrap().block_on(async move {
            for accounted in [false, true] {
                let (host, reference) = fixture().await?;
                let old = host.resolve(&reference, None).await?.generation;
                let fresh = [WorkspaceMutation::PutFile {
                    path:"/newer".into(), bytes:b"newer".to_vec().into(),
                }];
                host.apply_generation(&reference, None, &fresh, IdempotencyKey::new("fresh")?, None, None)?.await?;
                let cancellation = acyclic_fs::CancellationToken::new();
                let io = PreparationIo::from_budget(
                    acyclic_fs::performance::work_budget(100_000, 16_000_000), &cancellation,
                )?;
                let stale = [WorkspaceMutation::PutFile {
                    path:"/late".into(), bytes:payload.clone().into(),
                }];
                let result = host.apply_generation(&reference, Some(&old), &stale,
                    IdempotencyKey::new("stale")?, accounted.then_some(&io), None)?.await;
                if let Err(error) = &result { assert!(matches!(error, Error::Conflict(_)), "{error:?}"); }
                assert_eq!(&*host.read(&reference, None, "/newer", 4096, None).await?, b"newer");
                if result.is_ok() {
                    assert_eq!(&*host.read(&reference, None, "/late", 4096, None).await?, &payload);
                }
            }
            Ok::<_, Error>(())
        }).unwrap();
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
            .apply_generation(
                &reference,
                None,
                &mutations,
                key,
                accounted.then_some(&io),
                None,
            )?
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

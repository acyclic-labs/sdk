use super::*;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn compiled() -> TestResult<CompiledPolicy> {
    let mut work = usize::MAX;
    Ok(CompiledPolicy::compile_with_work(
        serde_json::from_slice(crate::access::INITIAL_POLICY)?,
        VolumeConfig::portable(crate::Lifecycle::Ephemeral),
        &mut work,
        |_| Ok(()),
    )?)
}
fn revision(value: u8) -> Revision {
    Revision::Value(ObjectId {
        kind: crate::ObjectKind::TreePage,
        digest: Digest::from_bytes([value; 32]),
    })
}
fn insert(cache: &PolicyCache, value: u8, capacity: u64) -> TestResult<Arc<CompiledPolicy>> {
    cache.initialize(capacity + PolicyCache::accounting_bytes()?)?;
    let volume = VolumeId::from_bytes([1; 16]);
    assert!(cache.get(volume, revision(value)).is_none());
    let mut compiled = compiled()?;
    compiled.cache_charge = Some(cache.charge(compiled.logical_owned_bytes())?);
    let compiled = Arc::new(compiled);
    cache.insert(volume, revision(value), Arc::clone(&compiled))?;
    Ok(compiled)
}
fn live(cache: &PolicyCache) -> TestResult<u64> {
    let state = cache.state.lock().map_err(|error| error.to_string())?;
    let memory = state.memory.as_ref().ok_or("missing cache memory")?;
    let bytes = *memory.live.lock().map_err(|error| error.to_string())?;
    Ok(bytes)
}

#[test]
fn equal_policy_revisions_in_different_volumes_do_not_alias() -> TestResult {
    let cache = PolicyCache::default();
    let bytes = compiled()?.logical_owned_bytes();
    let first = insert(&cache, 1, bytes * 2)?;
    assert!(
        cache
            .get(VolumeId::from_bytes([2; 16]), revision(1))
            .is_none()
    );
    assert!(Arc::ptr_eq(
        &first,
        &cache
            .get(VolumeId::from_bytes([1; 16]), revision(1))
            .ok_or("missing volume")?
    ));
    Ok(())
}

#[test]
fn byte_pressure_evicts_the_least_recent_revision_and_recompiles_on_demand() -> TestResult {
    let cache = PolicyCache::default();
    let volume = VolumeId::from_bytes([1; 16]);
    let bytes = compiled()?.logical_owned_bytes();
    drop(insert(&cache, 1, bytes * 2)?);
    let second = insert(&cache, 2, bytes * 2)?;
    let original = Arc::downgrade(&second);
    drop(second);
    drop(
        cache
            .get(volume, revision(1))
            .ok_or("missing first revision")?,
    );
    drop(insert(&cache, 3, bytes * 2)?);
    assert!(cache.get(volume, revision(2)).is_none());
    assert!(original.upgrade().is_none());
    drop(insert(&cache, 2, bytes * 2)?);
    assert!(cache.get(volume, revision(2)).is_some());
    assert_eq!(live(&cache)?, bytes * 2);
    Ok(())
}

#[test]
fn evicted_open_views_keep_their_exact_charge_until_the_last_handle_drops() -> TestResult {
    let cache = PolicyCache::default();
    let bytes = compiled()?.logical_owned_bytes();
    let held = insert(&cache, 1, bytes)?;
    assert!(matches!(
        cache.charge(bytes),
        Err(FsError::AccessPolicyCacheCapacity)
    ));
    assert_eq!(live(&cache)?, bytes);
    assert!(
        cache
            .get(VolumeId::from_bytes([1; 16]), revision(1))
            .is_none()
    );
    drop(held);
    let charge = cache.charge(bytes)?;
    assert_eq!(live(&cache)?, bytes);
    // Negative control: releasing the charge at eviction would admit a second
    // entry while the original physical policy was still alive.
    assert!(matches!(
        cache.charge(1),
        Err(FsError::AccessPolicyCacheCapacity)
    ));
    drop(charge);
    assert!(cache.charge(bytes).is_ok());
    Ok(())
}

#[cfg(all(feature = "memory", not(target_arch = "wasm32")))]
async fn load(
    owner: &crate::Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectStore>,
    work: &crate::NativeMaterializationWork,
    cancel: &CancellationToken,
) -> TestResult<Arc<CompiledPolicy>> {
    let mut checkout = owner
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            cancel,
        )
        .await?
        .value;
    // Return only the shared program; metadata observations belong to checkout.
    Ok(checkout.compiled_access_policy(work, cancel).await?)
}

#[cfg(all(feature = "memory", not(target_arch = "wasm32")))]
#[tokio::test]
async fn shared_policy_bytes_have_cache_custody_independent_of_invocation_ledgers()
-> Result<(), Box<dyn std::error::Error>> {
    // Use the ordinary uncached object provider so this assertion measures only
    // policy ownership. LogicalObjectStore independently retains decoded pages
    // under the reading invocation's native custody (402 bytes in this fixture).
    let owner = Fs::new(
        crate::distributed::StreamAuthorityStore::new(Arc::new(
            acyclic_stream::MemoryStream::default(),
        )),
        crate::MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    )
    .create_workspace_with_operation::<_, (), crate::WorkspaceError>(
        "cache-host",
        crate::VolumeInitialization {
            config: VolumeConfig::portable(crate::Lifecycle::Ephemeral),
            access_policy: Some(crate::access::INITIAL_POLICY),
        },
        crate::OperationId::new(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
        None,
    )
    .await?
    .value
    .workspace();
    let cancel = CancellationToken::new();
    let original_cancel = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &original_cancel)
        .map_err(|(cause, _)| cause)?;
    let baseline = original.live_allocation_bytes();
    let first = load(&owner, &original, &cancel).await?;
    let cache = &owner.volume.fs.inner.policy_cache;
    assert_eq!(live(cache)?, first.logical_owned_bytes());
    assert_eq!(original.live_allocation_bytes(), baseline);
    original_cancel.cancel();
    let other = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
        .map_err(|(cause, _)| cause)?;
    let other_baseline = other.live_allocation_bytes();
    let second = load(&owner, &other, &cancel).await?;
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(other.live_allocation_bytes(), other_baseline);
    assert_eq!(original.live_allocation_bytes(), baseline);
    drop(second);
    crate::access::fixtures::publish_policy(
        &owner,
        &crate::access::fixtures::denied(crate::access::Action::Read, "secret"),
    )
    .await?;
    let changed = load(&owner, &other, &cancel).await?;
    assert!(!Arc::ptr_eq(&first, &changed));
    let retained = first.logical_owned_bytes() + changed.logical_owned_bytes();
    assert_eq!(other.live_allocation_bytes(), other_baseline);
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert_eq!(live(cache)?, retained);
    let memory = SharedArc::clone(
        cache
            .state
            .lock()
            .map_err(|error| error.to_string())?
            .memory
            .as_ref()
            .ok_or("missing cache budget")?,
    );
    drop(owner);
    assert_eq!(
        *memory.live.lock().map_err(|error| error.to_string())?,
        retained
    );
    drop(first);
    assert_eq!(
        *memory.live.lock().map_err(|error| error.to_string())?,
        changed.logical_owned_bytes()
    );
    drop(changed);
    assert_eq!(*memory.live.lock().map_err(|error| error.to_string())?, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

fn check_data_heads(key_on_head: bool) -> TestResult {
    let cache = PolicyCache::default();
    let bytes = compiled()?.logical_owned_bytes();
    let volume = VolumeId::from_bytes([1; 16]);
    let mut compiles = 0;
    for head in 1..=8 {
        let key = revision(if key_on_head { head } else { 1 });
        if cache.get(volume, key).is_none() {
            drop(insert(
                &cache,
                if key_on_head { head } else { 1 },
                bytes * 2,
            )?);
            compiles += 1;
        }
    }
    assert_eq!(compiles, 1, "data writes must preserve compiled policy");
    Ok(())
}

#[test]
fn data_head_changes_do_not_recompile_the_policy_record() -> TestResult {
    check_data_heads(false)
}

#[test]
#[should_panic(expected = "data writes must preserve compiled policy")]
fn negative_control_head_keys_recompile_on_every_write() {
    assert!(check_data_heads(true).is_ok());
}

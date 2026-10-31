use super::*;

fn same_actor(a: &RetainedArc<Volume>, b: &RetainedArc<Volume>) -> bool {
    std::ptr::eq(&**a, &**b)
}

fn key(index: u32) -> [u8; 32] {
    let mut key = [0; 32];
    key[28..].copy_from_slice(&index.to_be_bytes());
    key
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn dormant_volume_history_retires_actors_and_index_nodes_on_the_original_ledger()
-> Result<(), crate::FsError> {
    let cancel = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
            .map_err(|(error, _)| error)?;
    let mut accounting = KernelAccounting::native(&original, &cancel);
    let baseline = original.live_allocation_bytes();
    let registry = Registry::new(&mut accounting)?;
    for index in 0..256 {
        drop(registry.actor(key(index), &mut accounting)?);
    }
    let full = original.live_allocation_bytes();
    for round in 1..8 {
        for index in round * 256..(round + 1) * 256 {
            drop(registry.actor(key(index), &mut accounting)?);
        }
        assert_eq!(original.live_allocation_bytes(), full);
        let entries = registry.entries.lock().unwrap();
        for index in 0..(round * 256) {
            assert!(entries.live.get(key(index), &mut accounting)?.is_none());
        }
    }
    let survivors: Vec<_> = registry
        .entries
        .lock()
        .unwrap()
        .idle
        .iter()
        .flatten()
        .map(RetainedArc::downgrade)
        .collect();
    drop(registry);
    assert!(survivors.iter().all(|weak| weak.upgrade().is_none()));
    assert!(
        original.live_allocation_bytes() > baseline,
        "dead weak backing lost its funding"
    );
    drop(survivors);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[test]
fn cache_collisions_never_replace_live_actor_identity_or_refuse_concurrency()
-> Result<(), crate::FsError> {
    let cancel = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    let registry = Registry::new(&mut accounting)?;
    let keys: Vec<_> = (0..600_u32)
        .map(|index| {
            let mut key = [0; 32];
            key[24..28].copy_from_slice(&index.to_be_bytes());
            let prefix = key[..31]
                .iter()
                .fold(0_u8, |slot, byte| slot.rotate_left(1).wrapping_add(*byte));
            key[31] = 0_u8.wrapping_sub(prefix.rotate_left(1));
            key
        })
        .collect();
    let mut live = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        let volume = registry.actor(*key, &mut accounting)?;
        volume
            .revision
            .store(index as u64 + 1, std::sync::atomic::Ordering::Relaxed);
        live.push(volume);
    }
    for (index, key) in keys.iter().enumerate() {
        let again = registry.actor(*key, &mut accounting)?;
        assert!(same_actor(&again, &live[index]));
        assert_eq!(
            again.revision.load(std::sync::atomic::Ordering::Relaxed),
            index as u64 + 1
        );
    }
    assert_eq!(
        registry
            .entries
            .lock()
            .unwrap()
            .idle
            .iter()
            .flatten()
            .count(),
        1
    );
    drop(live);
    let entries = registry.entries.lock().unwrap();
    for key in &keys[..keys.len() - 1] {
        assert!(entries.live.get(*key, &mut accounting)?.is_none());
    }
    Ok(())
}

#[test]
fn delayed_retirement_cannot_unregister_a_new_actor_for_the_same_key() -> Result<(), crate::FsError>
{
    let cancel = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    let registry = Registry::new(&mut accounting)?;
    let key = key(0);
    drop(registry.actor(key, &mut accounting)?);
    let mut entries = registry.entries.lock().unwrap();
    let retired = entries.idle[0].take().unwrap();
    let weak = retired.downgrade();
    let dropping = std::thread::spawn(move || drop(retired));
    // The real last-owner destructor blocks on this held registry lock after
    // the Arc becomes non-upgradeable, exactly the replacement race.
    while weak.strong_count() != 0 {
        std::thread::yield_now();
    }
    let replacement = Volume::new(&mut accounting)?;
    *entries.live.get_or_try_insert_with(
        key,
        &mut accounting,
        |_| Ok(RetainedWeak::default()),
    )? = replacement.downgrade();
    replacement
        .registration
        .set(Registration {
            entries: registry.entries.downgrade(),
            key,
        })
        .unwrap_or_else(|_| unreachable!("new replacement"));
    entries.idle[0] = Some(replacement.clone());
    drop(entries);
    dropping.join().unwrap();
    assert!(same_actor(
        &replacement,
        &registry.actor(key, &mut accounting)?
    ));
    Ok(())
}

#[test]
fn refused_new_actor_keeps_idle_identity_and_scope_boundaries() -> Result<(), crate::FsError> {
    let cancel = CancellationToken::new();
    let owner = acyclic_stream::OperationClaims::default();
    let other_owner = acyclic_stream::OperationClaims::default();
    let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    let id = AuthorityId::from_bytes([7; 16]);
    let first = authority_actor(&owner, [1; 16], id, &mut accounting)?;
    let other_scope = authority_actor(&owner, [2; 16], id, &mut accounting)?;
    let other = authority_actor(&other_owner, [1; 16], id, &mut accounting)?;
    assert!(!same_actor(&first, &other_scope));
    assert!(!same_actor(&first, &other));
    let mut denied = KernelAccounting::standalone(
        WorkBudget {
            allocation_operations: 0,
            ..crate::performance::BOUNDARY_BUDGET
        },
        &cancel,
    );
    assert!(
        authority_actor(
            &owner,
            [1; 16],
            AuthorityId::from_bytes([8; 16]),
            &mut denied
        )
        .is_err()
    );
    assert!(same_actor(
        &first,
        &authority_actor(&owner, [1; 16], id, &mut denied)?
    ));
    cancel.cancel();
    assert!(authority_actor(&owner, [1; 16], id, &mut accounting).is_err());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn refused_index_admission_retires_the_unregistered_actor_before_its_funding()
-> Result<(), crate::FsError> {
    let cancel = CancellationToken::new();
    let mut owner = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    let registry = Registry::new(&mut owner)?;
    let first = registry.actor(key(0), &mut owner)?;
    // Native control + retained volume backing fit; the first index node does not. The new
    // actor has no registration until insertion succeeds, so its destructor
    // must not attempt to reacquire the registry lock during this refusal.
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: 3,
            ..crate::performance::BOUNDARY_BUDGET
        },
        &cancel,
    )
    .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    assert!(
        registry
            .actor(key(1), &mut KernelAccounting::native(&original, &cancel))
            .is_err()
    );
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert!(
        registry
            .entries
            .lock()
            .unwrap()
            .live
            .get(key(1), &mut owner)?
            .is_none()
    );
    assert!(same_actor(&first, &registry.actor(key(0), &mut owner)?));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn retained_actor_and_dead_registry_backing_stay_funded_after_owner_retirement()
-> Result<(), crate::FsError> {
    let cancel = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
            .map_err(|(error, _)| error)?;
    let mut accounting = KernelAccounting::native(&original, &cancel);
    let baseline = original.live_allocation_bytes();
    let registry = Registry::new(&mut accounting)?;
    let actor = registry.actor(key(0), &mut accounting)?;
    let actor_backing = actor.downgrade();
    let registry_backing = registry.entries.downgrade();
    drop(registry);
    assert!(registry_backing.upgrade().is_none());
    assert_eq!(actor_backing.strong_count(), 1);
    let actor_and_dead_registry = original.live_allocation_bytes();
    assert!(actor_and_dead_registry > baseline);
    cancel.cancel();
    drop(accounting);
    drop(actor);
    assert!(actor_backing.upgrade().is_none());
    // Dead weak handles keep both actual allocations resident and funded.
    // Cancellation cannot suppress their eventual cleanup.
    assert_eq!(original.live_allocation_bytes(), actor_and_dead_registry);
    drop(registry_backing);
    let actor_only = original.live_allocation_bytes();
    assert!(actor_only > baseline && actor_only < actor_and_dead_registry);
    drop(actor_backing);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

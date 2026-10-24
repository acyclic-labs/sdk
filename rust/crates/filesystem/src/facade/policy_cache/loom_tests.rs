use super::*;
use crate::sync::atomic::{AtomicUsize, Ordering};

fn compile_once(
    cache: &PolicyCache,
    compiles: &AtomicUsize,
    single_flight: bool,
) -> Result<(), String> {
    let volume = VolumeId::from_bytes([1; 16]);
    let revision = Revision::Unavailable;
    if cache.get(volume, revision).is_some() {
        return Ok(());
    }
    let _flight = if single_flight {
        Some(
            cache
                .compilation
                .lock()
                .map_err(|error| error.to_string())?,
        )
    } else {
        None
    };
    if single_flight && cache.get(volume, revision).is_some() {
        return Ok(());
    }
    compiles.fetch_add(1, Ordering::SeqCst);
    let mut work = usize::MAX;
    let mut compiled = CompiledPolicy::compile_with_work(
        &serde_json::from_slice(crate::access::INITIAL_POLICY)
            .map_err(|error| error.to_string())?,
        VolumeConfig::portable(crate::Lifecycle::Ephemeral),
        &mut work,
        |_| Ok(()),
    )
    .map_err(|error| error.to_string())?;
    compiled.cache_charge = Some(
        cache
            .charge(compiled.logical_owned_bytes())
            .map_err(|error| error.to_string())?,
    );
    cache
        .insert(volume, revision, Arc::new(compiled))
        .map_err(|error| error.to_string())
}

fn model(single_flight: bool) {
    loom::model(move || {
        let cache = SharedArc::new(PolicyCache::default());
        assert!(cache.initialize(1_000_000).is_ok());
        let compiles = SharedArc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let cache = SharedArc::clone(&cache);
            let compiles = SharedArc::clone(&compiles);
            threads.push(loom::thread::spawn(move || {
                compile_once(&cache, &compiles, single_flight)
            }));
        }
        for thread in threads {
            assert!(matches!(thread.join(), Ok(Ok(()))));
        }
        assert_eq!(
            compiles.load(Ordering::SeqCst),
            1,
            "one compilation per revision"
        );
    });
}

#[test]
fn concurrent_misses_compile_one_shared_revision() {
    model(true);
}

#[test]
#[should_panic(expected = "one compilation per revision")]
fn negative_control_without_single_flight_compiles_the_same_revision_twice() {
    model(false);
}

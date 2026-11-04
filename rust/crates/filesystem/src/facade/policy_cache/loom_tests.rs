use super::*;
use crate::sync::atomic::{AtomicUsize, Ordering};

fn compile_once(
    cache: &PolicyCache,
    compiles: &AtomicUsize,
) -> Result<Arc<CompiledPolicy>, FsError> {
    let volume = VolumeId::from_bytes([1; 16]);
    let revision = Revision::Unavailable;
    if let Some(view) = cache.get(volume, revision) {
        return Ok(view);
    }
    cache.get_or_compile(volume, revision, || {
        compiles.fetch_add(1, Ordering::SeqCst);
        let mut work = usize::MAX;
        let mut compiled = CompiledPolicy::compile_with_work(
            &serde_json::from_slice(crate::access::INITIAL_POLICY)
                .map_err(|_| FsError::InvalidAccessPolicy)?,
            VolumeConfig::portable(crate::Lifecycle::Ephemeral),
            &mut work,
            |_| Ok(()),
        )
        .map_err(|_| FsError::InvalidAccessPolicy)?;
        compiled.cache_charge = Some(cache.charge(compiled.logical_owned_bytes())?);
        Ok(Arc::new(compiled))
    })
}

#[test]
fn concurrent_misses_compile_one_shared_revision() {
    loom::model(|| {
        let cache = SharedArc::new(PolicyCache::default());
        assert!(cache.initialize(1_000_000).is_ok());
        let compiles = SharedArc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let cache = SharedArc::clone(&cache);
            let compiles = SharedArc::clone(&compiles);
            threads.push(loom::thread::spawn(move || compile_once(&cache, &compiles)));
        }
        let views: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap().unwrap())
            .collect();
        assert_eq!(
            compiles.load(Ordering::SeqCst),
            1,
            "one compilation per revision"
        );
        assert!(Arc::ptr_eq(&views[0], &views[1]));
    });
}

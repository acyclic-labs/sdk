//! Synthetic source-cohort and warm admission costs, without credential output.
use acyclic_auth::credential::*;
use futures::{FutureExt, channel::oneshot, executor::block_on, future::join_all};
use std::{
    hint::black_box,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

fn cohort(
    cache: &CredentialCache,
    callers: usize,
    release: oneshot::Sender<()>,
) -> Result<f64, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut requests = Box::pin(join_all((0..callers).map(|_| cache.resolve())));
    // Drive the real source to its held event before releasing the cohort.
    assert!(requests.as_mut().now_or_never().is_none());
    release
        .send(())
        .map_err(|_| "credential source event lost its receiver")?;
    let leases = block_on(requests)
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let elapsed = started.elapsed().as_secs_f64() * 1e6;
    for lease in &leases {
        assert!(Arc::ptr_eq(&leases[0], lease));
        cache.check(lease)?;
    }
    Ok(elapsed)
}

fn measure() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "callers,cold_cohort_us,warm_resolve_ns,check_ns,expiry_cohort_us,source_calls,raw_calls"
    );
    for callers in [1, 100, 1000] {
        let clock = Arc::new(AtomicU64::new(1));
        let calls = Arc::new(AtomicUsize::new(0));
        let (release, receive) = oneshot::channel();
        let gate = Arc::new(Mutex::new(Some(receive)));
        let cache = CredentialCache::new(
            {
                let clock = clock.clone();
                let calls = calls.clone();
                let gate = gate.clone();
                move |_| {
                    let receive = gate.lock().unwrap().take();
                    let generation = calls.fetch_add(1, Ordering::Relaxed);
                    let expires = clock.load(Ordering::Relaxed) + 100;
                    async move {
                        receive
                            .ok_or(CredentialError::Invalid)?
                            .await
                            .map_err(|_| CredentialError::Invalid)?;
                        CredentialSnapshot::new(
                            format!("synthetic-{generation}"),
                            [1; 32].into(),
                            expires,
                        )
                    }
                }
            },
            {
                let clock = clock.clone();
                move || CredentialValidity {
                    now_unix_seconds: clock.load(Ordering::Relaxed),
                    revision: Some([1; 32].into()),
                }
            },
        );
        let cold = cohort(&cache, callers, release)?;
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let lease = block_on(cache.resolve())?;
        let iterations = 16384;
        let started = Instant::now();
        for _ in 0..iterations {
            black_box(block_on(cache.resolve())?);
        }
        let warm = started.elapsed().as_secs_f64() * 1e9 / f64::from(iterations);
        let started = Instant::now();
        for _ in 0..iterations {
            cache.check(black_box(&lease))?;
        }
        let check = started.elapsed().as_secs_f64() * 1e9 / f64::from(iterations);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        clock.store(101, Ordering::Relaxed);
        assert!(cache.check(&lease).is_err());
        let (release, receive) = oneshot::channel();
        *gate.lock().unwrap() = Some(receive);
        let expiry = cohort(&cache, callers, release)?;
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        // Negative control: raw callbacks have no trusted validity or reuse.
        let raw_calls = Arc::new(AtomicUsize::new(0));
        let raw = CredentialProvider::new({
            let raw_calls = raw_calls.clone();
            move || {
                raw_calls.fetch_add(1, Ordering::Relaxed);
                std::future::ready(Ok("synthetic-raw".into()))
            }
        });
        for result in block_on(join_all((0..callers).map(|_| raw.resolve()))) {
            black_box(result?);
        }
        assert_eq!(raw_calls.load(Ordering::Relaxed), callers);
        println!("{callers},{cold:.2},{warm:.2},{check:.2},{expiry:.2},2,{callers}");
    }
    Ok(())
}

fn main() -> std::process::ExitCode {
    match measure() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "STEP_FAILED credential-benchmark: {error}; recovery: cargo bench --locked -p acyclic-auth --bench credential"
            );
            std::process::ExitCode::FAILURE
        }
    }
}

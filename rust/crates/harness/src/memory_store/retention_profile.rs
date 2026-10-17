use super::*;

// The original complete validation path, including its optional accounting.
fn original(store: &MemoryStore, path: &str, io: Option<&PreparationIo>) -> Result<()> {
    for existing in store.paths.keys() {
        if let Some(io) = io {
            let examine = WorkCounters {
                items_examined: 1,
                source_bytes_read: (existing.len() + path.len()) as u64,
                ..Default::default()
            };
            io.preflight(examine)?;
            io.charge(examine)?;
        }
        if paths_conflict(existing, path) {
            return Err(Error::Invalid(
                "file path conflicts with an existing directory".into(),
            ));
        }
    }
    Ok(())
}

#[test]
fn without_subscriber() -> Result<()> {
    profile(false)
}

#[test]
fn with_tracing() -> Result<()> {
    profile(true)
}

fn profile(tracing: bool) -> Result<()> {
    let dispatch = if tracing {
        tracing::Dispatch::new(tracing_subscriber::registry())
    } else {
        tracing::Dispatch::none()
    };
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let syscalls = std::env::var_os("HARNESS_PATH_SYSCALLS").is_some();
    let mut counts = vec![
        0, 1, 2, 3, 4, 5, 6, 8, 10, 11, 12, 16, 24, 32, 48, 64, 65, 96, 128, 256, 4096,
    ];
    counts.extend([39, 40, 41, 80]);
    counts.sort_unstable();
    counts.dedup();
    for count in counts {
        if syscalls && count != 4096 {
            continue;
        }
        let store = fixture(count)?;
        for (label, query) in [
            ("after-last", "files/new".into()),
            (
                "last-rewrite",
                format!("files/{:08}", count.saturating_sub(1)),
            ),
            ("middle-gap", format!("files/{:08}-", count / 2)),
        ] {
            measure(&store, &query, label, tracing, syscalls)?;
        }
        if count == 4096 {
            for (label, query) in [
                ("existing-ancestor", "files/00002048/child"),
                ("first-ancestor", "files/00000000/child"),
                ("existing-descendant", "files"),
                ("missing-root", "missing"),
                ("before-first-root", "aaa"),
            ] {
                measure(&store, query, label, tracing, syscalls)?;
            }
        }
    }
    if !syscalls {
        for depth in [4, 16] {
            let prefix = std::iter::repeat_n("d", depth)
                .collect::<Vec<_>>()
                .join("/");
            let mut store = super::super::super::tests::store(1, 41)?;
            store.stage("a", b"x", "text/plain", "x", true)?;
            for index in 1..=40 {
                store.stage(
                    &format!("{prefix}/{index:08}"),
                    b"x",
                    "text/plain",
                    "x",
                    true,
                )?;
            }
            for (label, query) in [
                (format!("depth-{depth}-gap"), format!("{prefix}/00000020-")),
                (
                    format!("depth-{depth}-ancestor"),
                    format!("{prefix}/00000005/child"),
                ),
                (
                    format!("depth-{depth}-after-last"),
                    format!("{prefix}/missing"),
                ),
                (format!("depth-{depth}-directory"), prefix.clone()),
            ] {
                measure(&store, &query, &label, tracing, false)?;
            }
        }
    }
    if !syscalls {
        for length in [64, 4000] {
            let query = "\u{e9}".repeat(length / 2);
            let mut store = super::super::super::tests::store(1, 41)?;
            for index in 0..40 {
                store.stage(
                    &format!("{query}-{index:08}"),
                    b"x",
                    "text/plain",
                    "x",
                    true,
                )?;
            }
            store.stage(&format!("{query}/child"), b"x", "text/plain", "x", true)?;
            measure(
                &store,
                &query,
                &format!("prefix-{length}-punctuation"),
                tracing,
                false,
            )?;
        }
    }
    let mut punctuation = super::super::super::tests::store(1, 4097)?;
    for index in 0..4096 {
        punctuation.stage(&format!("foo-{index:08}"), b"x", "text/plain", "x", true)?;
    }
    punctuation.stage("foo/child", b"x", "text/plain", "x", true)?;
    measure(&punctuation, "foo", "punctuation", tracing, syscalls)
}

fn measure(
    store: &MemoryStore,
    query: &str,
    label: &str,
    tracing: bool,
    syscalls: bool,
) -> Result<()> {
    let before = original(store, query, None);
    let after = store.validate_retained_path(query, None);
    assert_eq!(
        before.err().map(|e| e.to_string()),
        after.err().map(|e| e.to_string())
    );
    let implementation = std::env::var("HARNESS_PATH_PROFILE").ok();
    let samples = if syscalls { 1 } else { 64 };
    let batch = if cfg!(debug_assertions) || syscalls {
        1
    } else {
        32
    };
    let mut times = [Vec::with_capacity(samples), Vec::with_capacity(samples)];
    let operation = |legacy: bool| {
        let store = std::hint::black_box(store);
        let query = std::hint::black_box(query);
        if legacy {
            original(store, query, None)
        } else {
            store.validate_retained_path(query, None)
        }
    };
    for sample in 0..samples {
        // Alternate pair order to avoid assigning warm-up or clock drift to one side.
        for legacy in if sample % 2 == 0 {
            [true, false]
        } else {
            [false, true]
        } {
            if implementation
                .as_deref()
                .is_some_and(|name| (name == "scan") != legacy)
            {
                continue;
            }
            if syscalls {
                eprintln!("PATH_BEGIN");
            }
            let started = std::time::Instant::now();
            for _ in 0..batch {
                let _ = std::hint::black_box(operation(legacy));
            }
            let elapsed = started.elapsed() / batch;
            if syscalls {
                eprintln!("PATH_END");
            }
            times[usize::from(!legacy)].push(elapsed);
        }
    }
    for (legacy, times) in [true, false].into_iter().zip(times.iter_mut()) {
        if times.is_empty() {
            continue;
        }
        times.sort_unstable();
        let allocation = acyclic_test_support::measure(|| {
            let _ = std::hint::black_box(operation(legacy));
        });
        eprintln!(
            "retained tracing={tracing} legacy={legacy} rows={} case={label} median={:?} alloc={} bytes={} peak={}",
            store.paths.len(),
            times.get(samples / 2),
            allocation.count_total,
            allocation.bytes_total,
            allocation.bytes_max
        );
    }
    Ok(())
}

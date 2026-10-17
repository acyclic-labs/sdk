//! Held-file demand reads: an independent baseline, exact work, reader reuse
//! under roots that are not local, native error cleanup that frees storage
//! before refunding its ledger, and a source gate against zero-fill.
use super::native::{NativeDemandSource, demand_read_work};
use super::*;
use crate::model::{FilesystemProfile, VolumeLimits};
use acyclic_test_support::differential;
use proptest::prelude::*;
use std::error::Error;

type Work = (&'static str, u64, u64);
#[derive(Debug, PartialEq, Eq)]
struct ReadError {
    details: String,
    io: Option<(std::io::ErrorKind, Option<i32>)>,
}
type Outcome = Result<Vec<u8>, ReadError>;
fn failure(error: DemandError) -> ReadError {
    ReadError {
        details: format!("{error:?}"),
        io: match &error {
            DemandError::Io(error) => Some((error.kind(), error.raw_os_error())),
            _ => None,
        },
    }
}

const MAXIMUM_READ: u64 = 1024 * 1024;

fn path(value: &str) -> NamespacePath {
    let portable =
        crate::path::PortablePath::parse(value, VolumeLimits::default()).expect("test path parses");
    NamespacePath::from_portable(&portable, VolumeLimits::default()).expect("test path is valid")
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime starts")
}

/// A source over a directory holding `contents` as `/file`, and that file held open.
pub(super) fn held(
    runtime: &tokio::runtime::Runtime,
    contents: &[u8],
    remote: bool,
) -> Result<(tempfile::TempDir, NativeDemandSource, Box<dyn DemandFile>), Box<dyn Error>> {
    held_mode(runtime, contents, remote, false)
}
fn held_mode(
    runtime: &tokio::runtime::Runtime,
    contents: &[u8],
    remote: bool,
    old: bool,
) -> Result<(tempfile::TempDir, NativeDemandSource, Box<dyn DemandFile>), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("file"), contents)?;
    runtime.block_on(async {
        let source = NativeDemandSource::open(
            std::borrow::Cow::Borrowed(directory.path()),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            None,
            None,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
        let source = if remote { source.remote() } else { source };
        let reference = source.reference();
        let cancellation = CancellationToken::new();
        let node = source
            .lookup(
                reference,
                &path("/file"),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value
            .ok_or("file absent")?;
        if old {
            let file = super::native::baseline_file(source.clone(), &path("/file"), node.version)?;
            return Ok((directory, source, file));
        }
        let file = source
            .open_file(
                reference,
                &path("/file"),
                node.version,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        Ok::<_, Box<dyn Error>>((directory, source, file))
    })
}

fn outcome(result: Result<acyclic_stream::OwnedBytes, DemandError>) -> Outcome {
    result.map(|bytes| bytes.as_ref().to_vec()).map_err(failure)
}

/// One read, its outcome, the work it spent, and its trace on this thread.
fn production_read(
    file: &dyn DemandFile,
    offset: u64,
    length: u64,
) -> (Outcome, WorkCounters, Vec<Work>) {
    read_mode(file, offset, length, false)
}
fn read_mode(
    file: &dyn DemandFile,
    offset: u64,
    length: u64,
    native: bool,
) -> (Outcome, WorkCounters, Vec<Work>) {
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let ((result, spent), trace) = differential::observe::<_, _, Work>(
        || (),
        |()| {
            let mut accounting = if native {
                crate::kernel::KernelAccounting::native(&original, &cancellation)
            } else {
                crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation)
            };
            let result = file.read_range(offset, length, &mut accounting);
            (
                result.map(|chunk| chunk.bytes),
                if native {
                    original.receipt()
                } else {
                    accounting.spent()
                },
            )
        },
    );
    (outcome(result), spent, trace)
}

// Storage initialization intentionally differs; all read and authentication events
// must match, in order. Allocation-lifetime events have their own assertions.
fn observer(trace: &[Work]) -> Vec<Work> {
    trace
        .iter()
        .copied()
        .filter(|(op, ..)| !matches!(*op, "storage" | "free" | "wait"))
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    /// Inline and remote held-file reads return exactly the baseline's bytes
    /// or failure, across sizes, ranges past the end, oversized requests and edits.
    #[test]
    fn held_file_reads_match_independent_baseline(
        size in prop_oneof![0_usize..300, 70_000_usize..80_000],
        remote in any::<bool>(),
        offset in prop_oneof![0_u64..90_000, Just(u64::MAX - 1)],
        length in prop_oneof![0_u64..90_000, Just(MAXIMUM_READ), Just(MAXIMUM_READ + 1)],
        edited in any::<bool>(),
        native in any::<bool>(),
    ) {
        let runtime = runtime();
        let contents: Vec<u8> = (0..size).map(|byte| (byte * 31 + 7) as u8).collect();
        let (directory, _source, file) = held(&runtime, &contents, remote).unwrap();
        let (old_directory, _old_source, old_file) = held_mode(&runtime, &contents, remote, true).unwrap();
        if edited {
            let edit: Vec<u8> = contents.iter().map(|byte| byte ^ 0x5a).chain([1]).collect();
            std::fs::write(directory.path().join("file"), &edit).unwrap();
            std::fs::write(old_directory.path().join("file"), &edit).unwrap();
        }
        let (expected, _, expected_trace) = read_mode(old_file.as_ref(), offset, length, native);
        let (actual, _, actual_trace) = read_mode(file.as_ref(), offset, length, native);
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(observer(&actual_trace), observer(&expected_trace));
    }
}

/// Inline reads fill uninitialized capacity: no zero-filled bytes, no copy
/// charge for served bytes, one positional read per call that the file can satisfy.
#[test]
fn held_file_reads_do_exact_uninitialized_work() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let contents: Vec<u8> = (0..100_000_u32).map(|byte| (byte * 13) as u8).collect();
    let (directory, _source, file) = held(&runtime, &contents, false)?;
    for native in [false, true] {
        for (offset, length, served) in [
            (100_000_u64, 64_u64, 0_u64),
            (10, 4096, 4096),
            (99_000, 4096, 1000),
        ] {
            let expected = expected_work(directory.path(), native, served, 0);
            let (bytes, spent, trace) = read_mode(file.as_ref(), offset, length, native);
            let start = offset as usize;
            assert_eq!(bytes, Ok(contents[start..start + served as usize].to_vec()));
            assert_eq!(
                spent,
                expected_work(directory.path(), native, served, wait_count(&trace))
            );
            assert!(trace.contains(&("storage", 0, served)));
            let (_, extra, extra_trace) = demand_read_work::adding_allocation(|| {
                read_mode(file.as_ref(), offset, length, native)
            });
            assert_ne!(
                extra,
                expected_work(directory.path(), native, served, wait_count(&extra_trace)),
                "extra allocation escaped"
            );
            assert_eq!(
                extra.allocation_operations,
                expected.allocation_operations + 1
            );
            assert_eq!(
                extra.peak_allocation_bytes,
                expected.peak_allocation_bytes + 17
            );
            if served > 0 {
                let (_, _, broken) = demand_read_work::restoring_old_work(|| {
                    read_mode(file.as_ref(), offset, length, native)
                });
                assert!(
                    !broken.contains(&("storage", 0, served)),
                    "zero-fill escaped"
                );
            }
        }
    }
    Ok(())
}

fn wait_count(trace: &[Work]) -> u64 {
    trace.iter().filter(|(op, ..)| *op == "wait").count() as u64
}
fn expected_work(root: &std::path::Path, native: bool, served: u64, waits: u64) -> WorkCounters {
    let ledger = if native {
        crate::NativeMaterializationWork::accounting_state_bytes()
    } else {
        0
    };
    // OwnedBytes' vector guard contains these three fields. No observed counter
    // supplies an expected allocation size or count.
    let header = if native {
        std::mem::size_of::<(
            Vec<u8>,
            acyclic_native_runtime::ReadAllocation,
            Option<crate::NativeAllocation>,
        )>() as u64
    } else {
        0
    };
    #[cfg(unix)]
    let (metadata_reads, path_bytes, path_items, path_allocations, path_capacity, encoded) = {
        use std::os::unix::ffi::OsStrExt;
        let n = root.as_os_str().as_bytes().len() as u64;
        (3, 4 + n, 4 + n, 2, (n + 1).max(5), 0)
    };
    #[cfg(windows)]
    let (metadata_reads, path_bytes, path_items, path_allocations, path_capacity, encoded) = {
        use std::os::windows::ffi::OsStrExt;
        let units: Vec<_> = root.as_os_str().encode_wide().collect();
        let n = units.len() as u64;
        if units.starts_with(&[92, 92, 63, 92]) || units.starts_with(&[92, 63, 63, 92]) {
            (5, 0, 4 + 2 * n, 1, 2 * (n + 1), 2 * n)
        } else {
            (5, 8, 6 + 2 * n, 2, 4 * n + 20, 2 * n)
        }
    };
    WorkCounters {
        backend_read_operations: metadata_reads
            + u64::from(served > 0) * if cfg!(windows) { 2 } else { 1 },
        source_bytes_read: served,
        bytes_copied: path_bytes,
        bytes_encoded: encoded,
        items_examined: path_items + waits,
        output_bytes: served,
        allocation_operations: path_allocations + u64::from(served > 0) + 2 * u64::from(native),
        peak_allocation_bytes: ledger + served + header.max(path_capacity),
        ..WorkCounters::UNCHARGED
    }
}

#[test]
fn exact_errors_and_observers_detect_mutations() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let (_dir, _source, file) = held(&runtime, b"contents", false)?;
    let (_old_dir, _old_source, old) = held_mode(&runtime, b"contents", false, true)?;
    for errno in [5, 13] {
        let (expected, _, old_trace) =
            demand_read_work::with_errno(errno, || production_read(old.as_ref(), 0, 4));
        let (actual, _, trace) =
            demand_read_work::with_errno(errno, || production_read(file.as_ref(), 0, 4));
        assert_eq!(actual, expected);
        assert_eq!(observer(&trace), observer(&old_trace));
        let (changed, _, _) =
            demand_read_work::with_errno(errno + 1, || production_read(file.as_ref(), 0, 4));
        assert_ne!(changed, expected, "changed errno escaped");
        let mut changed = observer(&trace);
        changed.push(("path-stat", 0, 0));
        assert_ne!(changed, observer(&old_trace), "extra observation escaped");
    }
    Ok(())
}

/// Reads under a root that is not local reuse one reader instead of
/// starting a thread for each read.
#[test]
fn remote_reads_reuse_one_reader() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let contents = b"remote contents".to_vec();
    let (_directory, source, file) = held(&runtime, &contents, true)?;
    for offset in 0..8_u64 {
        let (bytes, _, _) = production_read(file.as_ref(), offset, 4);
        let start = offset as usize;
        assert_eq!(bytes, Ok(contents[start..start + 4].to_vec()));
    }
    assert_eq!(source.remote_readers_started(), 1);
    Ok(())
}

/// One native read, with exact return to the ledger's initial live bytes.
fn native_read(
    file: Box<dyn DemandFile>,
    offset: u64,
    length: u64,
) -> Result<(Outcome, u64, Vec<Work>), Box<dyn Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let (result, trace) = differential::observe::<_, _, Work>(
        || (),
        |()| {
            let mut accounting = crate::kernel::KernelAccounting::native(&original, &cancellation);
            outcome(
                file.read_range(offset, length, &mut accounting)
                    .map(|chunk| chunk.bytes),
            )
        },
    );
    drop(file);
    let live = original.live_allocation_bytes();
    assert_eq!(live, baseline, "the read leaked capacity");
    Ok((result, baseline, trace))
}

/// Frees of charged storage at which the ledger held less than the baseline
/// plus the storage still live.
fn refunded_while_live(baseline: u64, trace: &[Work]) -> Vec<Work> {
    trace
        .iter()
        .copied()
        .filter(|&(operation, live, charged)| operation == "free" && live < baseline + charged)
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    /// Native stale-file and I/O errors release storage before its refund.
    #[test]
    fn native_read_errors_free_storage_before_refunding(
        size in 1_usize..5_000,
        offset in 0_u64..6_000,
        length in 1_u64..6_000,
        edited in any::<bool>(),
        fail in any::<bool>(),
    ) {
        let runtime = runtime();
        let contents: Vec<u8> = (0..size).map(|byte| (byte * 7 + 3) as u8).collect();
        let (directory, _source, file) = held(&runtime, &contents, false).unwrap();
        let (old_directory, _old_source, old) = held_mode(&runtime, &contents, false, true).unwrap();
        if edited {
            let edit: Vec<u8> = contents.iter().map(|byte| byte ^ 0x33).chain([2]).collect();
            std::fs::write(directory.path().join("file"), &edit).unwrap();
            std::fs::write(old_directory.path().join("file"), &edit).unwrap();
        }
        let run = || {
            let (expected, _, _) = production_read(old.as_ref(), offset, length);
            let (actual, baseline, trace) = native_read(file, offset, length).unwrap();
            prop_assert_eq!(actual, expected);
            prop_assert_eq!(refunded_while_live(baseline, &trace), Vec::<Work>::new());
            if (fail || edited) && offset < size as u64 {
                prop_assert!(trace.iter().any(|&(operation, ..)| operation == "free"));
            }
            Ok(())
        };
        if fail { demand_read_work::with_errno(5, run)?; } else { run()?; }
    }
}

#[test]
fn refunding_before_freeing_is_detected() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let (_directory, _source, file) = held(&runtime, &[7_u8; 4096], false)?;
    let (actual, baseline, trace) = crate::native_host::charged::refunding_first(|| {
        demand_read_work::with_errno(5, || native_read(file, 0, 4096))
    })?;
    assert_eq!(
        actual,
        Err(failure(DemandError::Io(std::io::Error::from_raw_os_error(
            5
        ))))
    );
    assert_ne!(
        refunded_while_live(baseline, &trace),
        Vec::<Work>::new(),
        "an early refund was not detected"
    );
    Ok(())
}

/// Counts forbidden identifiers in every production item of a source file,
/// skipping only nodes compiled solely for tests.
fn production_uses(source: &str, forbidden: &str) -> usize {
    use syn::visit::Visit;
    fn test_only(attributes: &[syn::Attribute]) -> bool {
        attributes.iter().any(|attribute| {
            attribute.path().is_ident("cfg")
                && attribute.meta.require_list().is_ok_and(|list| {
                    let tokens = list.tokens.to_string();
                    tokens == "test" || tokens.starts_with("all (test ,")
                })
        })
    }
    fn expression_attributes(expression: &syn::Expr) -> &[syn::Attribute] {
        match expression {
            syn::Expr::Block(block) => &block.attrs,
            syn::Expr::Call(call) => &call.attrs,
            syn::Expr::MethodCall(call) => &call.attrs,
            syn::Expr::If(branch) => &branch.attrs,
            syn::Expr::Macro(invocation) => &invocation.attrs,
            _ => &[],
        }
    }
    fn item_attributes(item: &syn::Item) -> &[syn::Attribute] {
        match item {
            syn::Item::Fn(item) => &item.attrs,
            syn::Item::Mod(item) => &item.attrs,
            syn::Item::Impl(item) => &item.attrs,
            syn::Item::Use(item) => &item.attrs,
            syn::Item::Const(item) => &item.attrs,
            syn::Item::Static(item) => &item.attrs,
            syn::Item::Struct(item) => &item.attrs,
            syn::Item::Enum(item) => &item.attrs,
            syn::Item::Type(item) => &item.attrs,
            _ => &[],
        }
    }
    struct Production<'a> {
        forbidden: &'a str,
        uses: usize,
    }
    impl<'ast> Visit<'ast> for Production<'_> {
        fn visit_item(&mut self, item: &'ast syn::Item) {
            if !test_only(item_attributes(item)) {
                syn::visit::visit_item(self, item);
            }
        }
        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            if !test_only(&item.attrs) {
                syn::visit::visit_impl_item_fn(self, item);
            }
        }
        fn visit_stmt(&mut self, statement: &'ast syn::Stmt) {
            let attributes = match statement {
                syn::Stmt::Local(local) => &local.attrs[..],
                syn::Stmt::Macro(invocation) => &invocation.attrs[..],
                syn::Stmt::Expr(expression, _) => expression_attributes(expression),
                syn::Stmt::Item(item) => item_attributes(item),
            };
            if !test_only(attributes) {
                syn::visit::visit_stmt(self, statement);
            }
        }
        fn visit_expr(&mut self, expression: &'ast syn::Expr) {
            if !test_only(expression_attributes(expression)) {
                syn::visit::visit_expr(self, expression);
            }
        }
        fn visit_ident(&mut self, ident: &'ast syn::Ident) {
            if ident == self.forbidden {
                self.uses += 1;
            }
        }
    }
    let file = syn::parse_file(source).expect("production source parses");
    let mut production = Production { forbidden, uses: 0 };
    production.visit_file(&file);
    production.uses
}

const READ_SOURCES: [(&str, &str); 3] = [
    ("demand/native/file.rs", include_str!("native/file.rs")),
    (
        "native_host/yielding.rs",
        include_str!("../native_host/yielding.rs"),
    ),
    (
        "native_host/spare.rs",
        include_str!("../native_host/spare.rs"),
    ),
];

fn read_gate_violations(sources: &[(&str, String)]) -> Vec<String> {
    let mut violations = Vec::new();
    for (name, source) in sources {
        for forbidden in ["resize", "PreparedOwnedBytes", "spawn", "read_at"] {
            let uses = production_uses(source, forbidden);
            if uses != 0 {
                violations.push(format!("{name} uses {forbidden} {uses} times"));
            }
        }
    }
    violations
}

/// Held-file reads never zero-fill, never read into initialized slices, and
/// never start a thread per read: only the spare-capacity reads and the
/// remote reader pool may supply their bytes.
#[test]
fn held_file_reads_have_one_uninitialized_read() {
    let sources: Vec<_> = READ_SOURCES
        .iter()
        .map(|(name, source)| (*name, (*source).to_owned()))
        .collect();
    assert_eq!(read_gate_violations(&sources), Vec::<String>::new());
    // Negative control: the removed zero-fill is caught anywhere in production.
    let anchor = "    fn fill(\n";
    let zeroing =
        "    fn zero(data: &mut Vec<u8>) {\n        data.resize(data.capacity(), 0);\n    }\n";
    let broken: Vec<_> = sources
        .iter()
        .map(|(name, source)| {
            (
                *name,
                source.replacen(anchor, &format!("{zeroing}{anchor}"), 1),
            )
        })
        .collect();
    assert_eq!(
        read_gate_violations(&broken),
        vec!["demand/native/file.rs uses resize 1 times".to_owned()]
    );
}

/// Paired measurement, not a gate: `cargo test --release -p acyclic-fs --lib
/// demand::native_read_tests::measure -- --ignored --nocapture`.
#[test]
#[ignore = "measurement"]
fn measure_held_file_reads() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let contents: Vec<u8> = (0..2_000_000_u32).map(|byte| (byte * 7) as u8).collect();
    let (_inline_directory, _inline, inline) = held(&runtime, &contents, false)?;
    let (_remote_directory, _remote, remote) = held(&runtime, &contents, true)?;
    let (_old_dir, _old_source, old_inline) = held_mode(&runtime, &contents, false, true)?;
    let (_old_remote_dir, _old_remote_source, old_remote) =
        held_mode(&runtime, &contents, true, true)?;
    let time = |rounds: u32, read: &dyn Fn(u32)| {
        for round in 0..rounds / 10 {
            read(round);
        }
        let started = std::time::Instant::now();
        for round in 0..rounds {
            read(round);
        }
        started.elapsed().as_nanos() / u128::from(rounds)
    };
    for length in [4096_u64, 65_536, MAXIMUM_READ] {
        let rounds = if length == MAXIMUM_READ { 400 } else { 4000 };
        let read = |file: &dyn DemandFile, round: u32| {
            let offset = u64::from(round % 16) * 4096;
            let (bytes, _, _) = production_read(file, offset, length);
            assert_eq!(bytes.map(|bytes| bytes.len() as u64), Ok(length));
        };
        let old = time(rounds, &|round| {
            read(old_inline.as_ref(), round);
        });
        let new = time(rounds, &|round| read(inline.as_ref(), round));
        eprintln!("inline {length} B: zero-filled {old} ns, uninitialized {new} ns");
    }
    let spawned = time(2000, &|round| {
        let (bytes, _, _) = production_read(old_remote.as_ref(), u64::from(round % 16), 4096);
        assert!(bytes.is_ok());
    });
    let pooled = time(2000, &|round| {
        let (bytes, _, _) = production_read(remote.as_ref(), u64::from(round % 16), 4096);
        assert!(bytes.is_ok());
    });
    eprintln!("remote 4096 B: thread per read {spawned} ns, reused reader {pooled} ns");
    Ok(())
}

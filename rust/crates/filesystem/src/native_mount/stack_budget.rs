//! The stack each kind of native callback may use.
//!
//! Every driver serves a callback by polling its source operation to
//! completion on the thread the callback arrived on, so the stack an
//! operation needs is the stack its driver's thread must have: a fuser
//! session thread's 2 MiB, a fuse-t worker's 4 MiB, or the 32 MiB of the
//! provider's own `ProjFS` stack. These tests measure the deepest stack each
//! kind of callback reaches through the two sources mounts serve, a lazy
//! workspace view and a checkout, over local stores, and fail when a kind
//! exceeds its budget. Every budget stays a small fraction of the smallest
//! of those stacks.
#![allow(unsafe_code, clippy::expect_used)]

use super::adapter::{CheckoutMountSource, SharedCheckout};
use super::lazy::LazyMountSource;
use super::{MountFilesystem, MountPath, MountPublication, MountSourceError};
use crate::demand::native::NativeDemandSource;
use crate::facade::{LocalAuthorityBackend, LocalObjectBackend};
use crate::kernel::FileMetadata;
use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
use crate::{Fs, LazyWorkspace, LocalCoreStateStore, LocalOptions};
use bytes::Bytes;
use std::sync::Arc;

type Lazy = LazyWorkspace<
    LocalAuthorityBackend,
    LocalObjectBackend,
    NativeDemandSource,
    LocalCoreStateStore,
>;
type Authored = CheckoutMountSource<LocalAuthorityBackend, LocalObjectBackend>;
type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Stack painted below a measured callback: more than any callback uses.
const PAINTED: usize = 8 << 20;
const PATTERN: u8 = 0xA5;
const CONTENT: u32 = 64 * 1024;

/// Fills `PAINTED` bytes of this thread's stack, directly below the caller's
/// frame, with `PATTERN`, and returns the lowest painted address.
#[inline(never)]
fn paint() -> usize {
    let mut region = [PATTERN; PAINTED];
    std::hint::black_box(&mut region);
    region.as_ptr() as usize
}

/// Runs `callback` on a thread of its own, outside any runtime, as a driver
/// thread runs one, and returns its result with the deepest stack it
/// reached below its caller.
fn deepest_stack<T: Send>(callback: impl FnOnce() -> T + Send) -> (T, usize) {
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .stack_size(2 * PAINTED)
            .spawn_scoped(scope, || {
                let lowest = paint();
                let result = callback();
                // The callback's frames reuse the painted region from its
                // top down; the lowest byte it changed marks its depth.
                let untouched = (0..PAINTED)
                    .take_while(|offset| {
                        // SAFETY: the region is this thread's own stack below
                        // every live frame, still mapped, and written by
                        // nothing else; it is only read, byte by byte.
                        let byte =
                            unsafe { std::ptr::read_volatile((lowest + offset) as *const u8) };
                        byte == PATTERN
                    })
                    .count();
                (result, PAINTED - untouched)
            })
            .expect("spawn the measured callback thread")
            .join()
            .expect("the measured callback completes")
    })
}

/// A kind of callback and the most stack, in KiB, it may use in an
/// unoptimized and an optimized build.
struct Budget {
    kind: &'static str,
    unoptimized: usize,
    optimized: usize,
}

impl Budget {
    const fn new(kind: &'static str, unoptimized: usize, optimized: usize) -> Self {
        Self {
            kind,
            unoptimized,
            optimized,
        }
    }

    const fn kib(&self) -> usize {
        if cfg!(debug_assertions) {
            self.unoptimized
        } else {
            self.optimized
        }
    }
}

/// Measures callbacks against their budgets and reports every kind at once.
struct Measurements {
    source: &'static str,
    budgets: &'static [Budget],
    measured: Vec<(&'static str, usize, usize)>,
}

impl Measurements {
    fn new(source: &'static str, budgets: &'static [Budget]) -> Self {
        Self {
            source,
            budgets,
            measured: Vec::new(),
        }
    }

    fn measure<T: Send>(
        &mut self,
        kind: &'static str,
        callback: impl FnOnce() -> Result<T, MountSourceError> + Send,
    ) -> Result<T, MountSourceError> {
        let budget = self
            .budgets
            .iter()
            .find(|budget| budget.kind == kind)
            .expect("every measured kind has a budget");
        let (result, used) = tokio::task::block_in_place(|| deepest_stack(callback));
        self.measured
            .push((kind, used.div_ceil(1024), budget.kib()));
        result
    }

    fn assert_within_budgets(self) {
        let profile = if cfg!(debug_assertions) {
            "unoptimized"
        } else {
            "optimized"
        };
        for (kind, used, budget) in &self.measured {
            println!(
                "stack {} {profile} {} {kind}: {used} KiB of {budget} KiB",
                std::env::consts::OS,
                self.source
            );
        }
        assert_eq!(
            self.measured.len(),
            self.budgets.len(),
            "every budgeted kind is measured"
        );
        let over = self
            .measured
            .iter()
            .filter(|(_, used, budget)| used > budget)
            .collect::<Vec<_>>();
        assert!(
            over.is_empty(),
            "{} callbacks exceed their stack budget (kind, KiB used, KiB budget): {over:?}",
            self.source
        );
    }
}

fn path(name: &str) -> MountPath {
    name.split('/').fold(MountPath::root(), |path, component| {
        path.child(if cfg!(windows) {
            component
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect()
        } else {
            component.as_bytes().to_vec()
        })
    })
}

/// Directories a path-depth kind resolves through: enough that a cost per
/// component would show next to the shallow kinds.
const DEPTH: usize = 16;

fn deep(name: &str) -> String {
    let mut path = (1..=DEPTH)
        .map(|level| format!("d{level}"))
        .collect::<Vec<_>>();
    path.push(name.to_owned());
    path.join("/")
}

/// Every file the measured callbacks find already present.
fn seeded_files() -> Vec<String> {
    [
        "read.txt",
        "write.txt",
        "attributes.txt",
        "rename.txt",
        "remove.txt",
        "dir/nested.txt",
    ]
    .into_iter()
    .map(str::to_owned)
    .chain([deep("read.txt"), deep("write.txt")])
    .collect()
}

/// A lazy workspace over a source tree, with its authored checkout.
async fn lazy_workspace(
    root: &std::path::Path,
) -> Result<(Arc<Lazy>, Arc<Authored>), Box<dyn std::error::Error>> {
    let source_root = root.join("source");
    for name in seeded_files() {
        let file = source_root.join(&name);
        std::fs::create_dir_all(file.parent().expect("a seeded file has a parent"))?;
        std::fs::write(file, vec![7_u8; CONTENT as usize])?;
    }
    let profile = if cfg!(windows) {
        FilesystemProfile::Windows
    } else {
        FilesystemProfile::Posix
    };
    let demand = NativeDemandSource::open(&source_root, profile, VolumeLimits::default()).await?;
    let engine = Fs::local(LocalOptions::new(root.join("state"))).await?;
    let lazy = Arc::new(
        LazyWorkspace::attach(
            &engine,
            "stack-budget",
            Arc::new(demand),
            LocalCoreStateStore::open_owned(root.join("core"))?,
        )
        .await?,
    );
    let checkout = lazy
        .workspace()
        .engine_checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
        )
        .await?;
    let config = checkout.volume_config();
    let authored = Arc::new(CheckoutMountSource::new(
        Arc::new(SharedCheckout::with_publication(
            checkout,
            MountPublication::Manual,
        )),
        config,
    )?);
    Ok((lazy, authored))
}

/// Measures the callbacks every source serves. Every seeded file must
/// already be visible through `view`.
fn measure_common(view: &dyn MountFilesystem, measurements: &mut Measurements) -> TestResult {
    let read = path("read.txt");
    measurements.measure("readdir-root", || {
        view.read_directory(&MountPath::root(), None, 256)
    })?;
    measurements.measure("readdir", || view.read_directory(&path("dir"), None, 256))?;
    measurements.measure("lookup", || view.lookup(&read))?;
    let pinned = measurements.measure("lookup-pinned", || view.lookup_pinned(&read))?;
    let opened = measurements.measure("open", || view.open_file(&read))?;
    measurements.measure("getattr", || opened.lookup())?;
    measurements.measure("read", || opened.read_range(0, CONTENT))?;
    if let Some((_, Some(pin))) = pinned {
        measurements.measure("read-pinned", || {
            view.read_pinned(&read, pin, 0, CONTENT.into(), 16 * 1024, &mut |_, _| Ok(()))
        })?;
    } else {
        measurements.measure("read-pinned", || view.read_range(&read, 0, CONTENT))?;
    }
    measurements.measure("create", || {
        view.create_file(&path("created.txt"), FileMetadata::default())
    })?;
    let created = view.open_file(&path("created.txt"))?;
    measurements.measure("write", || {
        created.write_range(0, Bytes::from_static(b"written"))
    })?;
    measurements.measure("mkdir", || {
        view.create_directory(&path("made"), FileMetadata::default())
    })?;
    measurements.measure("rename", || {
        view.rename(&path("created.txt"), &path("made/created.txt"), false)
    })?;
    measurements.measure("write-existing", || {
        view.open_file(&path("write.txt"))?
            .write_range(0, Bytes::from_static(b"written"))
    })?;
    let attributes = path("attributes.txt");
    let metadata = view
        .lookup(&attributes)?
        .ok_or(MountSourceError::NotFound)?
        .metadata;
    measurements.measure("setattr-existing", || {
        view.set_attributes(&attributes, metadata, Some(1))
    })?;
    measurements.measure("rename-existing", || {
        view.rename(&path("rename.txt"), &path("renamed.txt"), false)
    })?;
    measurements.measure("remove-existing", || view.remove(&path("remove.txt"), None))?;
    measurements.measure("lookup-deep", || view.lookup(&path(&deep("read.txt"))))?;
    measurements.measure("write-deep", || {
        view.open_file(&path(&deep("write.txt")))?
            .write_range(0, Bytes::from_static(b"written"))
    })?;
    measurements.measure("flush", || view.flush())?;
    Ok(())
}

/// The smallest stack a driver serves callbacks on, in KiB: a fuser session
/// thread's, Rust's default for a spawned thread.
const SMALLEST_DRIVER_STACK_KIB: usize = 2048;

/// How many times its budget each kind leaves of the smallest driver stack.
const MARGIN: usize = 4;

// Every budget leaves `MARGIN` times itself of the smallest driver stack.
const _: () = {
    let mut remaining = BUDGETS;
    while let [budget, rest @ ..] = remaining {
        assert!(budget.optimized <= budget.unoptimized);
        assert!(budget.unoptimized * MARGIN <= SMALLEST_DRIVER_STACK_KIB);
        remaining = rest;
    }
};

/// The kinds every source serves, with each kind's budget in KiB: about a
/// quarter above the most it measured on Linux, macOS, and Windows.
const BUDGETS: &[Budget] = &[
    Budget::new("readdir-root", 384, 96),
    Budget::new("readdir", 368, 96),
    Budget::new("lookup", 352, 96),
    Budget::new("lookup-pinned", 208, 80),
    Budget::new("open", 176, 80),
    Budget::new("getattr", 160, 48),
    Budget::new("read", 192, 48),
    Budget::new("read-pinned", 352, 96),
    Budget::new("create", 336, 80),
    Budget::new("write", 336, 80),
    Budget::new("mkdir", 320, 80),
    Budget::new("rename", 336, 96),
    Budget::new("write-existing", 480, 96),
    Budget::new("setattr-existing", 496, 112),
    Budget::new("rename-existing", 512, 112),
    Budget::new("remove-existing", 320, 96),
    Budget::new("lookup-deep", 352, 96),
    Budget::new("write-deep", 480, 96),
    Budget::new("flush", 48, 32),
];

/// A lazy view serves source files until a change promotes them.
#[tokio::test(flavor = "multi_thread")]
async fn lazy_view_callbacks_fit_their_stack_budgets() -> TestResult {
    let root = tempfile::tempdir()?;
    let (lazy, authored) = lazy_workspace(root.path()).await?;
    let view = LazyMountSource::new(lazy, authored, "/".to_owned())?;
    let mut measurements = Measurements::new("lazy", BUDGETS);
    measure_common(&view, &mut measurements)?;
    measurements.assert_within_budgets();
    Ok(())
}

/// A checkout mount serves authored files only.
#[tokio::test(flavor = "multi_thread")]
async fn checkout_callbacks_fit_their_stack_budgets() -> TestResult {
    let root = tempfile::tempdir()?;
    // A lazy workspace's authored checkout is an ordinary checkout; mounted
    // alone, it serves none of the source's files.
    let (_lazy, authored) = lazy_workspace(root.path()).await?;
    for name in seeded_files() {
        let file = path(&name);
        let mut directory = MountPath::root();
        let parent = file.parent().expect("a seeded file has a parent");
        for component in parent.components() {
            directory = directory.child(component.clone());
            if authored.lookup(&directory)?.is_none() {
                authored.create_directory(&directory, FileMetadata::default())?;
            }
        }
        authored.create_file(&file, FileMetadata::default())?;
        authored
            .open_file(&file)?
            .write_range(0, Bytes::from(vec![7_u8; CONTENT as usize]))?;
    }
    let mut measurements = Measurements::new("checkout", BUDGETS);
    measure_common(authored.as_ref(), &mut measurements)?;
    measurements.assert_within_budgets();
    Ok(())
}

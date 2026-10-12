//! Native callback stack limits.
#![allow(
    unsafe_code,
    reason = "probes the callback thread's stack with volatile reads below the stack pointer"
)]

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

const PAINTED: usize = 8 << 20;
const PATTERN: u8 = 0xA5;
const CONTENT: u32 = 64 * 1024;

#[inline(never)]
fn paint() -> usize {
    let mut region = [PATTERN; PAINTED];
    std::hint::black_box(&mut region);
    region.as_ptr() as usize
}

fn deepest_stack<T: Send>(callback: impl FnOnce() -> T + Send) -> (T, usize) {
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .stack_size(2 * PAINTED)
            .spawn_scoped(scope, || {
                let lowest = paint();
                let result = callback();
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

fn measure<T: Send>(
    measured: &mut Vec<(&'static str, usize, usize)>,
    kind: &'static str,
    callback: impl FnOnce() -> Result<T, MountSourceError> + Send,
) -> Result<T, MountSourceError> {
    let (_, unoptimized, optimized) = BUDGETS
        .iter()
        .find(|(name, _, _)| *name == kind)
        .expect("every measured kind has a budget");
    let budget = if cfg!(debug_assertions) {
        *unoptimized
    } else {
        *optimized
    };
    let (result, used) = tokio::task::block_in_place(|| deepest_stack(callback));
    measured.push((kind, used.div_ceil(1024), budget));
    result
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

const DEPTH: usize = 16;

fn deep(name: &str) -> String {
    let mut path = (1..=DEPTH)
        .map(|level| format!("d{level}"))
        .collect::<Vec<_>>();
    path.push(name.to_owned());
    path.join("/")
}

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
    let demand = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(&source_root).to_path_buf()),
        profile,
        VolumeLimits::default(),
        None,
        None,
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
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
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
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

fn measure_common(source: &str, view: &dyn MountFilesystem) -> TestResult {
    let work = crate::NativeMaterializationWork::new(
        crate::performance::BOUNDARY_BUDGET,
        &crate::CancellationToken::new(),
    )
    .map_err(|(error, _)| MountSourceError::NativeWork(error))?;
    let mut measured = Vec::new();
    let read = path("read.txt");
    measure(&mut measured, "readdir-root", || {
        view.read_directory(&MountPath::root(), None, 256)
    })?;
    measure(&mut measured, "readdir", || {
        view.read_directory(&path("dir"), None, 256)
    })?;
    measure(&mut measured, "lookup", || view.lookup(&read))?;
    let pinned = measure(&mut measured, "lookup-pinned", || view.lookup_pinned(&read))?;
    let opened = measure(&mut measured, "open", || view.open_file(&read))?;
    measure(&mut measured, "getattr", || opened.lookup())?;
    measure(&mut measured, "read", || opened.read_range(0, CONTENT))?;
    if let Some((_, Some(pin))) = pinned {
        measure(&mut measured, "read-pinned", || {
            view.read_pinned(&read, pin, 0, CONTENT.into(), 16 * 1024, &mut |_, _| Ok(()))
        })?;
    } else {
        measure(&mut measured, "read-pinned", || {
            view.read_range(&read, 0, CONTENT)
        })?;
    }
    measure(&mut measured, "create", || {
        view.create_file(&path("created.txt"), FileMetadata::default(), None, &work)
    })?;
    let created = view.open_file(&path("created.txt"))?;
    measure(&mut measured, "write", || {
        created.write_range(0, Bytes::from_static(b"written"))
    })?;
    measure(&mut measured, "mkdir", || {
        view.create_directory(&path("made"), FileMetadata::default(), &work)
    })?;
    measure(&mut measured, "rename", || {
        view.rename(
            &path("created.txt"),
            &path("made/created.txt"),
            false,
            &work,
        )
    })?;
    measure(&mut measured, "write-existing", || {
        view.open_file(&path("write.txt"))?
            .write_range(0, Bytes::from_static(b"written"))
    })?;
    let attributes = path("attributes.txt");
    let metadata = view
        .lookup(&attributes)?
        .ok_or(MountSourceError::NotFound)?
        .metadata;
    measure(&mut measured, "setattr-existing", || {
        view.set_attributes(&attributes, metadata, Some(1), &work)
    })?;
    measure(&mut measured, "rename-existing", || {
        view.rename(&path("rename.txt"), &path("renamed.txt"), false, &work)
    })?;
    measure(&mut measured, "remove-existing", || {
        view.remove(&path("remove.txt"), None, &work)
    })?;
    measure(&mut measured, "lookup-deep", || {
        view.lookup(&path(&deep("read.txt")))
    })?;
    measure(&mut measured, "write-deep", || {
        view.open_file(&path(&deep("write.txt")))?
            .write_range(0, Bytes::from_static(b"written"))
    })?;
    measure(&mut measured, "flush", || view.flush(&work))?;
    let profile = if cfg!(debug_assertions) {
        "unoptimized"
    } else {
        "optimized"
    };
    for (kind, used, budget) in &measured {
        println!(
            "stack {} {profile} {source} {kind}: {used} KiB of {budget} KiB",
            std::env::consts::OS
        );
    }
    assert_eq!(
        measured.len(),
        BUDGETS.len(),
        "every budgeted kind is measured"
    );
    let over = measured
        .iter()
        .filter(|(_, used, budget)| used > budget)
        .collect::<Vec<_>>();
    assert!(
        over.is_empty(),
        "{source} callbacks exceed their stack budget (kind, KiB used, KiB budget): {over:?}"
    );
    Ok(())
}

const SMALLEST_DRIVER_STACK_KIB: usize = 2048;

const MARGIN: usize = 4;

// Every budget leaves `MARGIN` times itself of the smallest driver stack.
const _: () = {
    let mut remaining = BUDGETS;
    while let [(_, unoptimized, optimized), rest @ ..] = remaining {
        assert!(*optimized <= *unoptimized);
        assert!(*unoptimized * MARGIN <= SMALLEST_DRIVER_STACK_KIB);
        remaining = rest;
    }
};

const BUDGETS: &[(&str, usize, usize)] = &[
    ("readdir-root", 384, 96),
    ("readdir", 368, 96),
    ("lookup", 352, 96),
    ("lookup-pinned", 208, 80),
    ("open", 176, 80),
    ("getattr", 160, 48),
    ("read", 192, 48),
    ("read-pinned", 352, 96),
    ("create", 336, 80),
    ("write", 336, 80),
    ("mkdir", 320, 80),
    ("rename", 336, 96),
    ("write-existing", 480, 96),
    ("setattr-existing", 496, 112),
    ("rename-existing", 512, 112),
    ("remove-existing", 320, 96),
    ("lookup-deep", 352, 96),
    ("write-deep", 480, 96),
    ("flush", 48, 32),
];

#[tokio::test(flavor = "multi_thread")]
async fn lazy_view_callbacks_fit_their_stack_budgets() -> TestResult {
    let root = tempfile::tempdir()?;
    let (lazy, authored) = lazy_workspace(root.path()).await?;
    let view = {
        super::adapter::CallbackRuntime::create()?;
        let cancellation = authored.cancellation().clone();
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        LazyMountSource::new(
            lazy,
            authored,
            "/".to_owned(),
            super::adapter::NativeDemandCarrier::Borrowed(&mut work),
            None,
        )
    }?;
    measure_common("lazy", &view)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn checkout_callbacks_fit_their_stack_budgets() -> TestResult {
    let root = tempfile::tempdir()?;
    let (_lazy, authored) = lazy_workspace(root.path()).await?;
    for name in seeded_files() {
        let file = path(&name);
        let mut directory = MountPath::root();
        let parent = file.parent().expect("a seeded file has a parent");
        for component in parent.components() {
            directory = directory.child(component.clone());
            if authored.lookup(&directory)?.is_none() {
                super::adapter::boundary(authored.cancellation(), |work| {
                    authored.create_directory(&directory, FileMetadata::default(), work)
                })?;
            }
        }
        crate::native_mount::adapter::boundary(authored.cancellation(), |work| {
            authored.create_file(&file, FileMetadata::default(), None, work)
        })?;
        authored
            .open_file(&file)?
            .write_range(0, Bytes::from(vec![7_u8; CONTENT as usize]))?;
    }
    measure_common("checkout", authored.as_ref())?;
    Ok(())
}

//! Views of one shared FUSE instance, against the live kernel.

use super::*;
use std::process::{Command, Stdio};

const CHILD: &str = "ACYCLIC_FS_SHARED_FUSE_CHILD";
/// Negative control for the cost test: one mount per view, as before.
const UNSHARED: &str = "ACYCLIC_FS_SHARED_FUSE_UNSHARED";

fn tracking_views(count: usize) -> Result<Vec<MemorySource>, Box<dyn std::error::Error>> {
    let config = posix_config();
    memory_checkout(
        config,
        crate::model::CheckoutMode::tracking_transaction(),
        count,
    )?
    .into_iter()
    .map(|checkout| {
        MemorySource::new(
            Arc::new(crate::native_mount::SharedCheckout::new(checkout)),
            config,
        )
        .map_err(Into::into)
    })
    .collect()
}

fn view(
    source: MemorySource,
    destination: &Path,
) -> Result<crate::NativeMountSession, crate::NativeMountError> {
    mount_view(source, destination, true)
}

fn mount_view(
    source: MemorySource,
    destination: &Path,
    shared: bool,
) -> Result<crate::NativeMountSession, crate::NativeMountError> {
    let volume_id = source.volume_id()?;
    crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id,
            destination: destination.to_path_buf(),
            writable: true,
            shared,
        },
        Arc::new(source),
    )
}

/// Mount points of this process's namespace at exactly `root`.
fn mounts_at(root: &Path) -> std::io::Result<usize> {
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let table = std::fs::read_to_string("/proc/self/mountinfo")?;
    Ok(table
        .lines()
        .filter_map(|line| line.split(' ').nth(4))
        .filter(|point| Path::new(point) == root)
        .count())
}

fn thread_names() -> std::collections::BTreeMap<String, usize> {
    let mut names = std::collections::BTreeMap::new();
    for task in std::fs::read_dir("/proc/self/task")
        .into_iter()
        .flatten()
        .flatten()
    {
        let name = std::fs::read_to_string(task.path().join("comm")).unwrap_or_default();
        *names.entry(name.trim().to_owned()).or_default() += 1;
    }
    names
}

fn entries(path: &str) -> std::io::Result<usize> {
    Ok(std::fs::read_dir(path)?.count())
}

/// Runs one exact test of this binary in a child process, so process-wide
/// counts see only that test and a kill there is a real crash.
fn child(test: &str, root: &Path) -> Command {
    let mut command = Command::new(std::env::current_exe().expect("test binary"));
    command
        .args([
            test,
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, root)
        .env_remove(UNSHARED)
        .stdin(Stdio::null());
    command
}

#[test]
#[ignore = "requires a live Linux FUSE mount"]
pub(super) fn linux_shared_views_are_independent_namespaces_of_one_mount()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("views");
    std::fs::create_dir(&root)?;
    let mut sources = tracking_views(2)?.into_iter();
    let (first, second) = (sources.next().ok_or("one")?, sources.next().ok_or("two")?);
    let mut a = view(first, &root.join("a"))?;
    let mut b = view(second, &root.join("b"))?;
    assert_eq!(mounts_at(&root)?, 1, "both views share one mount");
    assert_eq!(listing(&root)?, ["a", "b"]);

    std::fs::write(root.join("a/file"), b"only in a")?;
    assert_eq!(std::fs::read(root.join("a/file"))?, b"only in a");
    assert!(!root.join("b/file").exists(), "views are independent");
    std::fs::create_dir(root.join("b/directory"))?;
    assert_eq!(listing(&root.join("b"))?, ["directory"]);

    let crossed = std::fs::rename(root.join("a/file"), root.join("b/file"))
        .expect_err("a rename across views");
    assert_eq!(crossed.raw_os_error(), Some(libc::EXDEV));
    let crossed = std::fs::hard_link(root.join("a/file"), root.join("b/linked"))
        .expect_err("a link across views");
    assert_eq!(crossed.raw_os_error(), Some(libc::EXDEV));
    let refused = std::fs::create_dir(root.join("c")).expect_err("the root lists views only");
    assert_eq!(refused.raw_os_error(), Some(libc::EACCES));

    // A descriptor held inside a retired view never reaches another view.
    let held = std::fs::File::open(root.join("a"))?;
    assert!(a.stop()?);
    assert_eq!(listing(&root)?, ["b"]);
    assert!(!root.join("a").exists());
    let late = rustix::fs::openat(
        &held,
        "file",
        rustix::fs::OFlags::RDONLY,
        rustix::fs::Mode::empty(),
    )
    .expect_err("a lookup in a retired view");
    assert_eq!(late.raw_os_error(), libc::ESTALE);
    drop(held);
    assert_eq!(listing(&root.join("b"))?, ["directory"]);
    assert_eq!(mounts_at(&root)?, 1, "the other view keeps the mount");

    assert!(b.stop()?);
    assert_eq!(mounts_at(&root)?, 0, "the last view's retirement unmounts");
    let [fence, ..] = crate::native_mount_control_paths(&root.canonicalize()?)?;
    assert!(!fence.exists(), "the unmounted root releases its fence");
    Ok(())
}

#[test]
#[ignore = "requires a live Linux FUSE mount"]
pub(super) fn linux_changes_around_a_shared_view_reach_the_kernel()
-> Result<(), Box<dyn std::error::Error>> {
    let (mounted, around) = shared_sources(posix_config())?;
    let file = name("file");
    crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.create_file(&file, FileMetadata::default(), None, work)
    })?;
    around.write_range(&file, 0, Bytes::from_static(b"first"))?;
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("views");
    std::fs::create_dir(&root)?;
    let (_, idle) = shared_sources(posix_config())?;
    // A second view shares the invalidator and must not hold up the first.
    let mut other = view(idle, &root.join("other"))?;
    let mut session = view(mounted, &root.join("view"))?;
    let path = root.join("view/file");
    assert_eq!(std::fs::read(&path)?, b"first");
    around.write_range(&file, 0, Bytes::from_static(b"other"))?;
    session.revalidate(&fuse_fixture_work())?;
    assert_eq!(std::fs::read(&path)?, b"other");
    crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.create_file(&name("added"), FileMetadata::default(), None, work)
    })?;
    session.revalidate(&fuse_fixture_work())?;
    assert_eq!(listing(&root.join("view"))?, ["added", "file"]);
    assert!(session.stop()?);
    assert!(other.stop()?);
    Ok(())
}

#[test]
#[ignore = "requires a live Linux FUSE mount"]
pub(super) fn linux_shared_views_cost_no_thread_descriptor_or_mount()
-> Result<(), Box<dyn std::error::Error>> {
    for unshared in [false, true] {
        let temporary = tempfile::tempdir()?;
        let mut command = child(
            "native_mount::fuse::tests::shared::shared_view_costs_in_this_process",
            temporary.path(),
        );
        if unshared {
            command.env(UNSHARED, "1");
        }
        let output = command.output()?;
        let text = String::from_utf8_lossy(&output.stdout);
        let expected = if unshared {
            "unshared view costs: grow"
        } else {
            "shared view costs: ok"
        };
        assert!(
            output.status.success() && text.contains(expected),
            "child failed: {text}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

/// Child half of `linux_shared_views_cost_no_thread_descriptor_or_mount`.
#[test]
#[ignore = "runs only as the child of its parent test"]
pub(super) fn shared_view_costs_in_this_process() -> Result<(), Box<dyn std::error::Error>> {
    let Some(base) = std::env::var_os(CHILD) else {
        return Ok(());
    };
    let root = Path::new(&base).join("views");
    std::fs::create_dir(&root)?;
    let root = root.canonicalize()?;
    let shared = std::env::var_os(UNSHARED).is_none();
    let start = |source, index: usize| -> Result<_, Box<dyn std::error::Error>> {
        let destination = root.join(format!("{index}"));
        if !shared {
            std::fs::create_dir(&destination)?;
        }
        Ok(mount_view(source, &destination, shared)?)
    };
    let mut sources = tracking_views(9)?.into_iter();
    let mut views = Vec::new();
    // The first view starts the instance; measure views after it, at N and 2N.
    views.push(start(sources.next().ok_or("source")?, 0)?);
    let measure = || -> std::io::Result<(usize, usize, usize)> {
        // Only this test's mounts: other processes share the namespace.
        let table = std::fs::read_to_string("/proc/self/mountinfo")?;
        let mounts = table
            .lines()
            .filter_map(|line| line.split(' ').nth(4))
            .filter(|point| Path::new(point).starts_with(&root))
            .count();
        Ok((
            entries("/proc/self/task")?,
            entries("/proc/self/fd")?,
            mounts,
        ))
    };
    let base = measure()?;
    for (index, source) in sources.by_ref().take(4).enumerate() {
        views.push(start(source, index + 1)?);
    }
    let at_n = measure()?;
    for (index, source) in sources.enumerate() {
        views.push(start(source, index + 5)?);
    }
    let at_2n = measure()?;
    assert_eq!(views.len(), 9);
    let views_listed = listing(&root)?
        .iter()
        .filter(|name| !name.as_encoded_bytes().starts_with(b"."))
        .count();
    assert_eq!(views_listed, 9);
    if !shared {
        // Each view's own mount costs threads, a descriptor and a mount.
        assert!(
            at_n.0 > base.0 && at_n.1 > base.1 && at_n.2 > base.2,
            "{base:?} {at_n:?}"
        );
        assert!(
            at_2n.0 > at_n.0 && at_2n.1 > at_n.1 && at_2n.2 > at_n.2,
            "{at_n:?} {at_2n:?}"
        );
        for mut view in views {
            assert!(view.stop()?);
        }
        println!("unshared view costs: grow");
        return Ok(());
    }
    // Process-wide pools and the instance's threads all exist once the
    // first view is up; further views add nothing.
    assert_eq!(at_n, base, "4 more views: {:?}", thread_names());
    assert_eq!(at_2n, base, "8 more views: {:?}", thread_names());
    assert_eq!(base.2, 1);
    for mut view in views {
        assert!(view.stop()?);
    }
    assert_eq!(mounts_at(&root)?, 0);
    println!("shared view costs: ok");
    Ok(())
}

#[test]
#[ignore = "requires a live Linux FUSE mount"]
pub(super) fn linux_a_crashed_owner_leaves_a_root_the_next_start_reaps()
-> Result<(), Box<dyn std::error::Error>> {
    use std::io::{BufRead as _, BufReader};

    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("views");
    let mut owner = child(
        "native_mount::fuse::tests::shared::shared_view_owner_until_killed",
        temporary.path(),
    )
    .stdout(Stdio::piped())
    .stderr(Stdio::inherit())
    .spawn()?;
    let mut lines = BufReader::new(owner.stdout.take().ok_or("child stdout")?).lines();
    loop {
        let line = lines
            .next()
            .ok_or("the owner exited before its view was live")??;
        if line.contains("shared view owner: ready") {
            break;
        }
    }
    owner.kill()?;
    owner.wait()?;
    // The kill is the leak: a mount nobody serves, and its fence.
    assert_eq!(
        mounts_at(&root)?,
        1,
        "a killed owner leaves its root mounted"
    );
    let [fence, ..] = crate::native_mount_control_paths(&root)?;
    assert!(fence.exists(), "and fenced");

    // Negative control: without the reaper the root cannot be reused.
    super::super::shared_instance::SKIP_REAP.with(|skip| skip.set(true));
    let (refused_source, _) = shared_sources(posix_config())?;
    let refused = view(refused_source, &root.join("refused"));
    super::super::shared_instance::SKIP_REAP.with(|skip| skip.set(false));
    assert!(
        refused.is_err(),
        "a crash-left root is unusable without the reaper"
    );
    assert_eq!(mounts_at(&root)?, 1);

    let (source, _) = shared_sources(posix_config())?;
    let mut session = view(source, &root.join("after"))?;
    assert_eq!(mounts_at(&root)?, 1, "the reaper replaced the dead mount");
    assert_eq!(listing(&root)?, ["after"]);
    assert!(session.stop()?);
    assert_eq!(mounts_at(&root)?, 0);
    assert!(!fence.exists());
    Ok(())
}

/// Child half of `linux_a_crashed_owner_leaves_a_root_the_next_start_reaps`:
/// holds a view until its parent kills it.
#[test]
#[ignore = "runs only as the child of its parent test"]
pub(super) fn shared_view_owner_until_killed() -> Result<(), Box<dyn std::error::Error>> {
    let Some(base) = std::env::var_os(CHILD) else {
        return Ok(());
    };
    let root = Path::new(&base).join("views");
    std::fs::create_dir(&root)?;
    let (source, _) = shared_sources(posix_config())?;
    let _session = view(source, &root.join("crashed"))?;
    println!("shared view owner: ready");
    loop {
        std::thread::park();
    }
}

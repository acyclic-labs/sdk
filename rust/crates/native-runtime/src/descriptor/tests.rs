use super::cache::{Access, Cache, Cost, Entry};
use super::*;
use std::fs::OpenOptions;

/// A cache of its own, so the bound a test sets binds only its files.
fn private_cache(capacity: usize) -> Arc<Cache<Native>> {
    Arc::new(Cache::new(Native, capacity))
}

fn create(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
}

fn cached(cache: &Arc<Cache<Native>>, path: &Path) -> io::Result<Arc<Entry<Native>>> {
    Entry::new(cache, create(path)?, path, Access::Write, true, &mut |_| {
        Ok(())
    })
}

/// Descriptors this process holds on files under `directory`.
fn descriptors_under(directory: &Path) -> io::Result<usize> {
    Ok(crate::resources::descriptors()?
        .into_iter()
        .filter(|descriptor| {
            matches!(descriptor, crate::resources::Descriptor::File(path) if path.starts_with(directory))
        })
        .count())
}

fn write_at(entry: &Arc<Entry<Native>>, offset: u64, bytes: &[u8]) -> io::Result<()> {
    entry
        .lease(&mut |_| Ok(()))?
        .write(|file| crate::write_all_at(file, offset, bytes, None))
}

fn read(entry: &Arc<Entry<Native>>, length: usize) -> io::Result<Vec<u8>> {
    let mut bytes = vec![0; length];
    let lease = entry.lease(&mut |_| Ok(()))?;
    let read = crate::read_at(lease.file(), 0, &mut bytes)?;
    bytes.truncate(read);
    Ok(bytes)
}

#[test]
fn idle_files_hold_at_most_the_capacity_in_descriptors() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let directory_path = std::fs::canonicalize(directory.path())?;
    let cache = private_cache(4);
    let files = (0..64)
        .map(|index| cached(&cache, &directory_path.join(format!("{index}.log"))))
        .collect::<io::Result<Vec<_>>>()?;
    for (index, file) in files.iter().enumerate() {
        write_at(file, 0, format!("file {index}").as_bytes())?;
    }
    assert!(cache.cached() <= 4, "{} cached", cache.cached());
    assert!(descriptors_under(&directory_path)? <= 4);
    // Every file reopens with its contents.
    for (index, file) in files.iter().enumerate() {
        assert_eq!(read(file, 64)?, format!("file {index}").into_bytes());
    }
    assert!(cache.cached() <= 4);
    Ok(())
}

#[test]
fn a_file_replaced_or_removed_while_closed_is_refused() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let cache = private_cache(1);
    let replaced_path = directory.path().join("replaced.log");
    let removed_path = directory.path().join("removed.log");
    let replaced = cached(&cache, &replaced_path)?;
    let removed = cached(&cache, &removed_path)?;
    // Admitting `removed` closed `replaced`; another file takes its name.
    let other = directory.path().join("other.log");
    std::fs::write(&other, b"another")?;
    std::fs::rename(&other, &replaced_path)?;
    let refusal = replaced.lease(&mut |_| Ok(())).map(drop);
    assert_eq!(
        refusal.map_err(|error| error.kind()),
        Err(io::ErrorKind::InvalidData)
    );
    // That reopen failed, so `removed` keeps its descriptor; close it.
    let _ = cached(&cache, &directory.path().join("third.log"))?;
    std::fs::remove_file(&removed_path)?;
    assert_eq!(
        removed
            .lease(&mut |_| Ok(()))
            .map(drop)
            .map_err(|error| error.kind()),
        Err(io::ErrorKind::NotFound)
    );
    Ok(())
}

/// An eviction skips files in use, so leases held together leave the cache
/// over capacity; their release brings it back. A written descriptor waits
/// for the next lease that lists a file, which pays for its flush.
#[test]
fn released_leases_return_the_cache_to_capacity() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let cache = private_cache(1);
    let first = cached(&cache, &directory.path().join("first"))?;
    let second = cached(&cache, &directory.path().join("second"))?;
    let held = first.lease(&mut |_| Ok(()))?;
    let other = second.lease(&mut |_| Ok(()))?;
    assert_eq!(cache.cached(), 2, "both in use");
    drop(held);
    drop(other);
    assert_eq!(cache.cached(), 1, "released");

    let held = first.lease(&mut |_| Ok(()))?;
    let other = second.lease(&mut |_| Ok(()))?;
    held.write(|file| crate::write_all_at(file, 0, b"held", None))?;
    other.write(|file| crate::write_all_at(file, 0, b"other", None))?;
    drop(held);
    drop(other);
    assert_eq!(cache.cached(), 2, "a release flushes nothing");
    let path = directory.path().join("third");
    let (third, costs) =
        charged(|charge| Entry::new(&cache, create(&path)?, &path, Access::Write, true, charge))?;
    assert!(costs.contains(&Cost::Flush), "{costs:?}");
    assert_eq!(cache.cached(), 1, "the listing paid");
    drop(third);
    Ok(())
}

#[test]
fn the_default_capacity_is_a_quarter_of_the_descriptor_limit() {
    let capacity = default_capacity();
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    {
        let limit = match descriptor_limit() {
            0 => 1024,
            limit => limit,
        };
        let quarter = usize::try_from(limit / 4).unwrap_or(usize::MAX);
        assert_eq!(capacity, quarter.clamp(16, 1 << 16));
    }
    assert!((16..=1 << 16).contains(&capacity));
}

/// Every cost a lease or a new file charges, in order.
fn charged<T>(
    run: impl FnOnce(&mut dyn FnMut(Cost) -> io::Result<()>) -> io::Result<T>,
) -> io::Result<(T, Vec<Cost>)> {
    let mut costs = Vec::new();
    let value = run(&mut |cost| {
        costs.push(cost);
        Ok(())
    })?;
    Ok((value, costs))
}

#[test]
fn a_lease_charges_the_cache_work_it_causes() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let cache = private_cache(1);
    let path = directory.path().join("log");
    let (file, costs) =
        charged(|charge| Entry::new(&cache, create(&path)?, &path, Access::Write, true, charge))?;
    // The entry, its failure slot, its descriptor and its path: a ring
    // within its capacity never grows.
    assert_eq!(costs.len(), 4, "{costs:?}");
    assert!(costs.iter().all(|cost| matches!(cost, Cost::Allocation(_))));
    // An open file: nothing to charge.
    let (lease, costs) = charged(|charge| file.lease(charge))?;
    drop(lease);
    assert_eq!(costs, []);
    // Another file, written and left idle, takes the only slot.
    let other = cached(&cache, &directory.path().join("other"))?;
    write_at(&other, 0, b"unflushed")?;
    let (lease, costs) = charged(|charge| file.lease(charge))?;
    drop(lease);
    assert!(
        matches!(
            costs.as_slice(),
            [Cost::Reopen, Cost::Allocation(_), Cost::Flush]
        ),
        "the reopen, its descriptor, then the evicted file's flush: {costs:?}"
    );
    assert_eq!(cache.cached(), 1);
    // A flush the caller cannot pay for keeps that file's descriptor.
    write_at(&other, 0, b"unflushed")?;
    let mut refused = |cost| match cost {
        Cost::Flush => Err(io::Error::from(io::ErrorKind::QuotaExceeded)),
        _ => Ok(()),
    };
    let lease = file.lease(&mut refused)?;
    assert_eq!(cache.cached(), 2);
    drop(lease);
    Ok(())
}

/// A file its owner drops leaves the ring at once, rather than when an
/// eviction next passes it, so that the ring holds only wanted files.
#[test]
fn a_forgotten_file_leaves_the_ring() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let cache = private_cache(4);
    let first = cached(&cache, &directory.path().join("first"))?;
    let second = cached(&cache, &directory.path().join("second"))?;
    let third = cached(&cache, &directory.path().join("third"))?;
    assert_eq!(cache.listed(), 3);
    // The first file's slot goes to the last, which must still find it.
    first.forget();
    drop(first);
    assert_eq!(cache.listed(), 2);
    third.forget();
    second.forget();
    assert_eq!(cache.listed(), 0);
    // A file listed again after it was forgotten leaves again.
    drop(second.lease(&mut |_| Ok(()))?);
    assert_eq!(cache.listed(), 1);
    second.forget();
    assert_eq!(cache.listed(), 0);
    Ok(())
}

#[test]
fn a_metered_lease_charges_its_meter() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let cache = private_cache(1);
    let file = cached(&cache, &directory.path().join("log"))?;
    let _ = cached(&cache, &directory.path().join("other"))?;
    let meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    drop(file.lease(&mut charge(Some(&meter)))?);
    let load =
        |counter: &std::sync::atomic::AtomicU64| counter.load(std::sync::atomic::Ordering::Acquire);
    assert_eq!(
        load(&meter.used_operations),
        2,
        "the open and identity reads"
    );
    assert_eq!(load(&meter.used_allocations), 1, "the descriptor");
    assert_eq!(load(&meter.memory), 0, "the cache keeps what it allocates");
    Ok(())
}

fn open_directory(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    directory_options(&mut options);
    options.open(path)
}

fn cached_directory(cache: &Arc<Cache<Native>>, path: &Path) -> io::Result<Arc<Entry<Native>>> {
    Entry::new(
        cache,
        open_directory(path)?,
        path,
        Access::Directory,
        false,
        &mut |_| Ok(()),
    )
}

#[test]
fn a_cached_directory_reopens_only_as_the_very_directory() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let root = std::fs::canonicalize(root.path())?;
    let [replaced_path, kept_path, third_path, moved_path] =
        ["replaced", "kept", "third", "moved"].map(|name| root.join(name));
    for path in [&replaced_path, &kept_path, &third_path] {
        std::fs::create_dir(path)?;
    }
    let cache = private_cache(1);
    let replaced = cached_directory(&cache, &replaced_path)?;
    let original = crate::file_identity(replaced.lease(&mut |_| Ok(()))?.file())?;
    // Admitting `kept` closes `replaced`; another directory takes its name.
    let kept = cached_directory(&cache, &kept_path)?;
    std::fs::rename(&replaced_path, &moved_path)?;
    std::fs::create_dir(&replaced_path)?;
    assert_eq!(
        replaced
            .lease(&mut |_| Ok(()))
            .map(drop)
            .map_err(|error| error.kind()),
        Err(io::ErrorKind::InvalidData)
    );
    // Admitting `third` closes `kept`, which reopens as itself.
    let _third = cached_directory(&cache, &third_path)?;
    let identity = crate::file_identity(&open_directory(&kept_path)?)?;
    let lease = kept.lease(&mut |_| Ok(()))?;
    assert!(lease.file().metadata()?.is_dir());
    assert_eq!(crate::file_identity(lease.file())?, identity);
    drop(lease);
    #[cfg(unix)]
    {
        // A symbolic link to the very directory takes its name. Opened
        // plainly, the link would pass the identity check; the cache never
        // follows it.
        std::fs::remove_dir(&replaced_path)?;
        std::os::unix::fs::symlink(&moved_path, &replaced_path)?;
        assert_eq!(
            crate::file_identity(&File::open(&replaced_path)?)?,
            original
        );
        assert!(replaced.lease(&mut |_| Ok(())).is_err());
    }
    #[cfg(not(unix))]
    let _ = original;
    Ok(())
}

/// Work beyond the allowance waits for admission until held descriptors are
/// returned, and work wanting more than the whole allowance is admitted alone
/// rather than never.
#[test]
fn admission_waits_while_the_allowance_is_held() -> io::Result<()> {
    use std::task::{Context, Poll, Waker};
    let mut context = Context::from_waker(Waker::noop());
    let capacity = admitted_descriptor_capacity();
    let mut whole = std::pin::pin!(admit_descriptors(capacity + 1));
    let Poll::Ready(whole) = whole.as_mut().poll(&mut context) else {
        return Err(io::Error::other(
            "an oversized admission must be admitted alone",
        ));
    };
    let held = whole?;
    let mut waiting = std::pin::pin!(admit_descriptors(1));
    assert!(
        waiting.as_mut().poll(&mut context).is_pending(),
        "admission beyond the allowance must wait"
    );
    drop(held);
    let Poll::Ready(admitted) = waiting.as_mut().poll(&mut context) else {
        return Err(io::Error::other(
            "returned descriptors must admit waiting work",
        ));
    };
    drop(admitted?);
    Ok(())
}

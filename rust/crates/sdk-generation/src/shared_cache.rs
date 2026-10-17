use std::io;

#[cfg(acyclic_sdk_docs_loom)]
pub type Lock = loom::sync::Arc<loom::sync::Mutex<()>>;
#[cfg(not(acyclic_sdk_docs_loom))]
pub type Lock = std::fs::File;

#[cfg(acyclic_sdk_docs_loom)]
loom::thread_local! { static ENTERED: std::cell::Cell<bool> = std::cell::Cell::new(false); }
#[cfg(not(acyclic_sdk_docs_loom))]
std::thread_local! { static ENTERED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

struct Entered;
impl Drop for Entered {
    fn drop(&mut self) {
        ENTERED.with(|entered| entered.set(false));
    }
}

#[cfg(not(acyclic_sdk_docs_loom))]
struct Held<'a>(&'a Lock);
#[cfg(not(acyclic_sdk_docs_loom))]
impl Drop for Held<'_> {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

// Lock order on misses: one identity lock, then storage I/O. Validated hits
// need only a record read. Nested lookups are forbidden on either path.
pub fn get_or_compute<T>(
    open_lock: impl FnOnce() -> io::Result<Lock>,
    read: impl FnMut() -> io::Result<Option<T>>,
    compute: impl FnOnce() -> io::Result<T>,
    publish: impl FnOnce(&T) -> io::Result<()>,
) -> io::Result<T> {
    cached::<true, T>(open_lock, read, compute, publish)
}

fn cached<const FAST: bool, T>(
    open_lock: impl FnOnce() -> io::Result<Lock>,
    mut read: impl FnMut() -> io::Result<Option<T>>,
    compute: impl FnOnce() -> io::Result<T>,
    publish: impl FnOnce(&T) -> io::Result<()>,
) -> io::Result<T> {
    ENTERED.with(|entered| assert!(!entered.replace(true), "nested identity cache lock"));
    let _entered = Entered;
    if FAST {
        if let Some(value) = read()? {
            return Ok(value);
        }
    }
    let lock = match open_lock() {
        Ok(lock) => lock,
        Err(_) => return compute(),
    };
    #[cfg(acyclic_sdk_docs_loom)]
    let _held = lock.lock().unwrap();
    #[cfg(not(acyclic_sdk_docs_loom))]
    let _held = {
        if lock.lock().is_err() {
            return compute();
        }
        Held(&lock)
    };
    lookup(read, compute, publish)
}

fn lookup<T>(
    read: impl FnOnce() -> io::Result<Option<T>>,
    compute: impl FnOnce() -> io::Result<T>,
    publish: impl FnOnce(&T) -> io::Result<()>,
) -> io::Result<T> {
    if let Some(value) = read()? {
        return Ok(value);
    }
    let value = compute()?;
    // Cache publication is optional; a failed write never invalidates a fresh hash.
    let _ = publish(&value);
    Ok(value)
}

#[cfg(acyclic_sdk_docs_loom)]
pub fn without_lock<T>(
    read: impl FnOnce() -> io::Result<Option<T>>,
    compute: impl FnOnce() -> io::Result<T>,
    publish: impl FnOnce(&T) -> io::Result<()>,
) -> io::Result<T> {
    lookup(read, compute, publish)
}

#[cfg(acyclic_sdk_docs_loom)]
pub fn without_fast_read<T>(
    open_lock: impl FnOnce() -> io::Result<Lock>,
    read: impl FnMut() -> io::Result<Option<T>>,
    compute: impl FnOnce() -> io::Result<T>,
    publish: impl FnOnce(&T) -> io::Result<()>,
) -> io::Result<T> {
    cached::<false, T>(open_lock, read, compute, publish)
}

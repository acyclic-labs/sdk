//! The collection epoch a publication retains its closure under.
//!
//! Every publication retains its closure under the current epoch's root. A
//! collection that must reclaim retained objects opens epoch `e + 1` while no
//! publication is in flight, marks what is live, retains it under `e + 1`,
//! and only then releases `e`. A publication either finished under `e`
//! before the switch, so the mark covers it, or retains under `e + 1`
//! itself. No live object is ever covered only by a released root.
//!
//! The current epoch survives a restart before its collection retains
//! anything under it, together with the oldest epoch that may still hold
//! batches. Interrupted collections can leave any number of epochs between
//! the two unreleased, so a switch releases all of them.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// The current epoch, shared by every clone of the store.
#[derive(Default)]
pub struct Epoch {
    current: AtomicU64,
    /// The oldest epoch that may still hold batches.
    unreleased: AtomicU64,
    /// Where the current epoch survives a restart, for a store that collects.
    #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
    pointer: Option<PathBuf>,
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) hooks: hooks::Hooks,
}

const ROOT: &[u8] = b"fs-epoch/";

impl Epoch {
    /// Resumes the epoch recorded at `pointer`, or the first.
    #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
    pub(crate) fn open(pointer: PathBuf) -> std::io::Result<Self> {
        let (current, unreleased) = match std::fs::read(&pointer) {
            Ok(bytes) => {
                let bytes: [u8; 16] = bytes.try_into().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "malformed epoch pointer")
                })?;
                let (current, unreleased) = bytes.split_at(8);
                let word = |half: &[u8]| u64::from_le_bytes(half.try_into().unwrap_or_default());
                (word(current), word(unreleased))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (0, 0),
            Err(error) => return Err(error),
        };
        Ok(Self {
            current: AtomicU64::new(current),
            unreleased: AtomicU64::new(unreleased),
            pointer: Some(pointer),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            hooks: hooks::Hooks::default(),
        })
    }

    /// The epoch new publications retain under.
    pub fn current(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }

    /// The root that epoch's publications retain under.
    pub fn root(epoch: u64) -> acyclic_objects::RootId {
        let mut bytes = Vec::with_capacity(ROOT.len() + 8);
        bytes.extend_from_slice(ROOT);
        bytes.extend_from_slice(&epoch.to_be_bytes());
        acyclic_objects::RootId::new(bytes).expect("a fixed-width epoch root is a valid root")
    }

    #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
    /// The oldest epoch that may still hold batches.
    pub(crate) fn unreleased(&self) -> u64 {
        self.unreleased.load(Ordering::Acquire)
    }

    #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
    /// Records that every epoch before `epoch` is released. A restart learns
    /// it at the next [`Epoch::persist`]; until then it releases them again,
    /// which finds nothing.
    pub(crate) fn released_before(&self, epoch: u64) {
        self.unreleased.store(epoch, Ordering::Release);
    }

    /// Durably records `next` as the epoch a restart resumes, before anything
    /// retains under it, with the oldest epoch still to release: a restart
    /// then never retains under an epoch a later collection would skip
    /// releasing.
    #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
    pub(crate) async fn persist(&self, next: u64) -> std::io::Result<()> {
        let Some(pointer) = self.pointer.clone() else {
            return Ok(());
        };
        let mut bytes = [0_u8; 16];
        let (current, unreleased) = bytes.split_at_mut(8);
        current.copy_from_slice(&next.to_le_bytes());
        unreleased.copy_from_slice(&self.unreleased().to_le_bytes());
        tokio::task::spawn_blocking(move || {
            let staged = pointer.with_extension("next");
            {
                let file = std::fs::File::create(&staged)?;
                std::io::Write::write_all(&mut &file, &bytes)?;
                file.sync_all()?;
            }
            std::fs::rename(&staged, &pointer)?;
            acyclic_native_runtime::sync_parent(&pointer, acyclic_native_runtime::Durability::Full)
        })
        .await
        .map_err(std::io::Error::other)?
    }

    #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
    /// Opens `next` for new publications. The caller holds every publication
    /// out while it does.
    pub(crate) fn switch(&self, next: u64) {
        self.current.store(next, Ordering::Release);
    }
}

/// A step of a collection that opens an epoch, after which a crash leaves
/// its own durable state.
#[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Step {
    Persisted,
    Switched,
    Retained,
    Released,
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod hooks {
    use super::Step;
    use std::sync::{Arc, Mutex, PoisonError};

    #[derive(Default)]
    pub(crate) struct Hooks {
        pub(crate) crash_after: Mutex<Option<Step>>,
        /// A publication that retained waits here before its record: the
        /// countdown names which retain after `pause_at` waits.
        pub(crate) pause: Mutex<Option<(usize, Arc<tokio::sync::Mutex<()>>)>>,
        pub(crate) retained: tokio::sync::Notify,
        pub(crate) released: tokio::sync::Notify,
    }

    impl Hooks {
        pub(crate) fn crashes(&self, step: Step) -> bool {
            *self
                .crash_after
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                == Some(step)
        }

        /// Holds the next publication that retains until the guard drops.
        pub(crate) fn pause(&self) -> tokio::sync::OwnedMutexGuard<()> {
            self.pause_at(1)
        }

        /// Holds the `nth` retain from now until the guard drops.
        pub(crate) fn pause_at(&self, nth: usize) -> tokio::sync::OwnedMutexGuard<()> {
            let gate = Arc::new(tokio::sync::Mutex::new(()));
            let guard = Arc::clone(&gate)
                .try_lock_owned()
                .expect("a fresh gate is free");
            *self.pause.lock().unwrap_or_else(PoisonError::into_inner) = Some((nth, gate));
            guard
        }

        /// Holds the retain `pause_at` named.
        pub(crate) async fn retained(&self) {
            let gate = {
                let mut pause = self.pause.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some((nth, _)) = pause.as_mut()
                    && *nth > 1
                {
                    *nth -= 1;
                    None
                } else {
                    pause.take().map(|(_, gate)| gate)
                }
            };
            if let Some(gate) = gate {
                self.retained.notify_one();
                drop(gate.lock().await);
            }
        }
    }
}

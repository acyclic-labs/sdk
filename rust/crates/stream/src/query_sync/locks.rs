use loom::sync::Mutex as LoomMutex;
use std::{
    future::poll_fn,
    ops::{Deref, DerefMut},
    task::{Poll, Waker},
};

// The Loom gate owns scheduling, exclusion and waiter registration. Tokio only
// supplies safe Send data guards: its acquisition is always an immediate try.
// Lock order: caller's outer lock -> gate; no gate guard crosses a poll boundary.
#[derive(Default)]
struct Gate(LoomMutex<Access>);

#[derive(Default)]
struct Access {
    readers: usize,
    writer: bool,
    waiters: Vec<Waker>,
}

impl Gate {
    fn take(&self, write: bool, waker: Option<&Waker>) -> Option<Permit<'_>> {
        let mut access = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !access.writer && (!write || access.readers == 0) {
            if write {
                access.writer = true;
            } else {
                access.readers += 1;
            }
            Some(Permit { gate: self, write })
        } else {
            if let Some(waker) = waker
                && !access.waiters.iter().any(|old| old.will_wake(waker))
            {
                access.waiters.push(waker.clone());
            }
            None
        }
    }

    async fn acquire(&self, write: bool) -> Permit<'_> {
        poll_fn(|cx| match self.take(write, Some(cx.waker())) {
            Some(permit) => Poll::Ready(permit),
            None => Poll::Pending,
        })
        .await
    }
}

struct Permit<'a> {
    gate: &'a Gate,
    write: bool,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        let waiters = {
            let mut access = self
                .gate
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.write {
                access.writer = false;
            } else {
                access.readers -= 1;
            }
            std::mem::take(&mut access.waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }
}

/// Data guard whose Tokio guard drops before its modeled ownership permit.
pub struct Guard<'a, G> {
    data: G,
    _permit: Permit<'a>,
}

impl<G: Deref> Deref for Guard<'_, G> {
    type Target = G::Target;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl<G: DerefMut> DerefMut for Guard<'_, G> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

/// Async mutex whose exclusion and wakeups are scheduled by Loom.
pub struct Mutex<T> {
    data: tokio::sync::Mutex<T>,
    gate: Gate,
}

impl<T> Mutex<T> {
    /// Constructs an unlocked value.
    pub fn new(value: T) -> Self {
        Self {
            data: tokio::sync::Mutex::new(value),
            gate: Gate::default(),
        }
    }
    /// Acquires exclusive modeled ownership before borrowing the value.
    pub async fn lock(&self) -> Guard<'_, tokio::sync::MutexGuard<'_, T>> {
        let permit = self.gate.acquire(true).await;
        let data = self
            .data
            .try_lock()
            .unwrap_or_else(|_| unreachable!("modeled mutex ownership disagrees"));
        Guard {
            data,
            _permit: permit,
        }
    }
    /// Consumes an unborrowed mutex.
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }
}

/// Async read/write lock whose ownership and wakeups are scheduled by Loom.
pub struct RwLock<T> {
    data: tokio::sync::RwLock<T>,
    gate: Gate,
}

impl<T> RwLock<T> {
    /// Constructs an unlocked value.
    pub fn new(value: T) -> Self {
        Self {
            data: tokio::sync::RwLock::new(value),
            gate: Gate::default(),
        }
    }
    /// Acquires shared modeled ownership.
    pub async fn read(&self) -> Guard<'_, tokio::sync::RwLockReadGuard<'_, T>> {
        let permit = self.gate.acquire(false).await;
        let data = self
            .data
            .try_read()
            .unwrap_or_else(|_| unreachable!("modeled read ownership disagrees"));
        Guard {
            data,
            _permit: permit,
        }
    }
    /// Acquires exclusive modeled ownership.
    pub async fn write(&self) -> Guard<'_, tokio::sync::RwLockWriteGuard<'_, T>> {
        let permit = self.gate.acquire(true).await;
        let data = self
            .data
            .try_write()
            .unwrap_or_else(|_| unreachable!("modeled write ownership disagrees"));
        Guard {
            data,
            _permit: permit,
        }
    }
    /// Attempts exclusive modeled ownership without waiting.
    pub fn try_write(
        &self,
    ) -> Result<Guard<'_, tokio::sync::RwLockWriteGuard<'_, T>>, std::sync::TryLockError<()>> {
        let permit = self
            .gate
            .take(true, None)
            .ok_or(std::sync::TryLockError::WouldBlock)?;
        let data = self
            .data
            .try_write()
            .unwrap_or_else(|_| unreachable!("modeled try-write ownership disagrees"));
        Ok(Guard {
            data,
            _permit: permit,
        })
    }
}

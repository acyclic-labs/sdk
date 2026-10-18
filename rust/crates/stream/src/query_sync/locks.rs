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
    next: usize,
    granted: Vec<(usize, bool)>,
    waiters: std::collections::VecDeque<Waiter>,
    stall_granted_readers: bool,
}

struct Waiter {
    id: usize,
    write: bool,
    waker: Waker,
}

impl Access {
    fn available(&self, write: bool) -> bool {
        self.waiters.is_empty()
            && !self.writer
            && if write {
                self.readers == 0
            } else {
                !self.stall_granted_readers || self.granted.is_empty()
            }
    }
    // Reserve ownership before waking, including for a waiter never polled
    // again. FIFO writers block later readers, exactly as Tokio's semaphore.
    fn grant(&mut self) -> Vec<Waker> {
        let mut wakes = Vec::new();
        while let Some(waiter) = self.waiters.front() {
            if self.writer || (waiter.write && self.readers != 0) {
                break;
            }
            let waiter = self.waiters.pop_front().unwrap();
            if waiter.write {
                self.writer = true;
            } else {
                self.readers += 1;
            }
            self.granted.push((waiter.id, waiter.write));
            wakes.push(waiter.waker);
        }
        wakes
    }

    fn release(&mut self, write: bool) {
        if write {
            self.writer = false;
        } else {
            self.readers -= 1;
        }
    }
}

impl Gate {
    fn take(&self, write: bool) -> Option<Permit<'_>> {
        let mut access = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if access.available(write) {
            if write {
                access.writer = true;
            } else {
                access.readers += 1;
            }
            Some(Permit { gate: self, write })
        } else {
            None
        }
    }

    async fn acquire(&self, write: bool) -> Permit<'_> {
        let mut registration = Registration {
            gate: self,
            id: None,
        };
        poll_fn(|cx| {
            let mut access = self.0.lock().unwrap();
            if let Some(id) = registration.id {
                if let Some(position) = access.granted.iter().position(|entry| entry.0 == id) {
                    access.granted.remove(position);
                    registration.id = None;
                    return Poll::Ready(Permit { gate: self, write });
                }
                let waiter = access
                    .waiters
                    .iter_mut()
                    .find(|waiter| waiter.id == id)
                    .unwrap();
                waiter.waker = cx.waker().clone();
            } else if access.available(write) {
                if write {
                    access.writer = true;
                } else {
                    access.readers += 1;
                }
                return Poll::Ready(Permit { gate: self, write });
            } else {
                let id = access.next;
                access.next += 1;
                registration.id = Some(id);
                access.waiters.push_back(Waiter {
                    id,
                    write,
                    waker: cx.waker().clone(),
                });
            }
            Poll::Pending
        })
        .await
    }
}

struct Registration<'a> {
    gate: &'a Gate,
    id: Option<usize>,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        let Some(id) = self.id else {
            return;
        };
        let wakes = {
            let mut access = self.gate.0.lock().unwrap();
            if let Some(position) = access.waiters.iter().position(|waiter| waiter.id == id) {
                access.waiters.remove(position);
            } else {
                let position = access
                    .granted
                    .iter()
                    .position(|entry| entry.0 == id)
                    .unwrap();
                let (_, write) = access.granted.remove(position);
                access.release(write);
            }
            access.grant()
        };
        for waker in wakes {
            waker.wake();
        }
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
            access.release(self.write);
            access.grant()
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
    pub(crate) fn stalled_reader_control(value: T) -> Self {
        Self {
            data: tokio::sync::RwLock::new(value),
            gate: Gate(LoomMutex::new(Access {
                stall_granted_readers: true,
                ..Access::default()
            })),
        }
    }
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
            .take(true)
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

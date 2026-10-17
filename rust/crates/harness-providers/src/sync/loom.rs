pub use loom::sync::*;

/// The async mutex contract, with acquisition/release visible to Loom.
pub struct AsyncMutex<T>(Mutex<T>);

impl<T> AsyncMutex<T> {
    pub fn new(value: T) -> Self {
        Self(Mutex::new(value))
    }

    pub async fn lock(&self) -> MutexGuard<'_, T> {
        self.0.lock().unwrap()
    }
}

use crate::sync::owned::{Arc, Mutex, OwnedMutexGuard, lock_owned};

/// One owner per journal, shared across replacement catalog registrations.
#[derive(Clone)]
pub(crate) struct SkillMaterializer(Arc<Mutex<()>>);

impl SkillMaterializer {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(())))
    }

    pub(crate) async fn enter(&self) -> OwnedMutexGuard<()> {
        lock_owned(self.0.clone()).await
    }
}

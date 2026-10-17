use crate::sync::Mutex;
use std::future::Future;

// This is the only staging phase lock. No guard spans an await or an external
// authorization/store/backend call; those owners never nest below this lock.
pub(super) struct StageState<J, C>(Mutex<(Option<J>, Option<C>, bool)>);

impl<J, C> StageState<J, C> {
    pub(super) fn new(journal: J) -> Self {
        Self(Mutex::new((Some(journal), None, false)))
    }

    pub(super) async fn before<E, A, F, Fut>(
        &self,
        authorization: A,
        accept: F,
        invalid: impl Fn() -> E,
    ) -> Result<(), E>
    where
        A: Future<Output = Result<(), E>>,
        F: FnOnce(J) -> Fut,
        Fut: Future<Output = Result<C, E>>,
    {
        authorization.await?;
        let journal = self.0.lock().unwrap().0.take().ok_or_else(&invalid)?;
        let completion = accept(journal).await?;
        *self.0.lock().unwrap() = (None, Some(completion), true);
        Ok(())
    }

    pub(super) async fn completed<E, F, Fut>(
        &self,
        finish: F,
        invalid: impl Fn() -> E,
    ) -> Result<(), E>
    where
        F: FnOnce(C) -> Fut,
        Fut: Future<Output = Result<J, E>>,
    {
        let completion = self.0.lock().unwrap().1.take().ok_or_else(invalid)?;
        let journal = finish(completion).await?;
        *self.0.lock().unwrap() = (Some(journal), None, false);
        Ok(())
    }

    pub(super) fn into_inner(self) -> (Option<J>, Option<C>, bool) {
        self.0.into_inner().unwrap()
    }
}
